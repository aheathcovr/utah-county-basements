"""
test_scraper.py — Utah County Basement Lead Scraper

Queries the Utah County GIS ArcGIS REST API to find residential properties
in American Fork, UT with unfinished basements.

Two-phase approach:
  Phase 1 — Identify major development districts (NBHD_DISTRICT clusters with
             many recent builds 2019+). These are the production-builder
             neighborhoods where every house has the same floor plan.
  Phase 2 — Fetch all qualifying leads (unfinished basement), enrich with
             builder inference, development rank, and updated priority score.

Priority order:
  1. Properties in major recent development districts (production builder tracts)
  2. 100% unfinished basements over partially finished
  3. Production builder quality grade (Average / Good) over custom (Very Good+)
  4. Larger unfinished sqft

Data source:
  https://maps.utahcounty.gov/arcgis/rest/services/Assessor/TaxParcelAll_NoLabel/MapServer/0

Run: python3.12 test_scraper.py
"""

import csv
import logging
import os
import sys
import warnings
from collections import Counter, defaultdict
from datetime import datetime

import requests

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Constants ────────────────────────────────────────────────────────────────

GIS_URL = (
    "https://maps.utahcounty.gov/arcgis/rest/services"
    "/Assessor/TaxParcelAll_NoLabel/MapServer/0/query"
)

TARGET_CITY = "American Fork"
RECENT_BUILD_CUTOFF = 2019      # "last 5 years" development projects
OUTPUT_DIR = "output"
LOG_DIR = "logs"

FINISHED_VALUE_PER_SQFT = 150
FINISHING_COST_PER_SQFT = 50

# Quality grades that indicate a production/tract builder (not custom)
PRODUCTION_QUALITY_GRADES = {
    "Low", "Fair", "Average", "Average Plus", "Good", "Good Plus",
}

# Known Utah County production builder name fragments → display name
# Checked against OWNER_NAME for unsold inventory; inferred for sold homes
# via NBHD_DISTRICT clustering
BUILDER_LOOKUP: list[tuple[str, str]] = [
    ("D R HORTON", "D.R. Horton"),
    ("DR HORTON", "D.R. Horton"),
    ("IVORY HOMES", "Ivory Homes"),
    ("FIELDSTONE HOMES", "Fieldstone Homes"),
    ("FIELDSTONE", "Fieldstone Homes"),
    ("HAMLET HOMES", "Hamlet Homes"),
    ("HAMLET", "Hamlet Homes"),
    ("EDGE HOMES", "Edge Homes"),
    ("TOLL BROTHERS", "Toll Brothers"),
    ("LENNAR", "Lennar"),
    ("RICHMOND AMERICAN", "Richmond American"),
    ("WOODSIDE HOMES", "Woodside Homes"),
    ("DAVID WEEKLEY", "David Weekley Homes"),
    ("CENTURY COMMUNITIES", "Century Communities"),
    ("KB HOME", "KB Homes"),
    ("MERITAGE HOMES", "Meritage Homes"),
    ("HOLMES HOMES", "Holmes Homes"),
    ("PERRY HOMES", "Perry Homes"),
    ("GARBETT HOMES", "Garbett Homes"),
    ("GARBETT", "Garbett Homes"),
    ("DESTINATION HOMES", "Destination Homes"),
    ("VISIONARY HOMES", "Visionary Homes"),
    ("SYMPHONY HOMES", "Symphony Homes"),
    ("CHRISTENSEN HOMES", "Christensen Homes"),
    ("CRAFTSMAN HOMES", "Craftsman Homes"),
    ("WATSON HOMES", "Watson Homes"),
    ("PARADE HOMES", "Parade Homes"),
    ("PATTERSON CONSTRUCTION", "Patterson Homes"),
    ("PATTERSON HOMES", "Patterson Homes"),
    ("GCH AMERICAN FORK", "GCH Builders"),
    ("RASBAND CONSTRUCTION", "Rasband Construction"),
]

GIS_FIELDS = [
    "PARCEL_NO", "PARCELID", "OWNER_NAME", "SITE_FULL_ADDRESS", "TAX_CITY",
    "GLA_WEIGHTED_YRBLT", "YEARBLT_RES", "TOTAL_ABOVE_GRADE_AREA",
    "TOTAL_BASEMENT", "TOTAL_BSMT_FINISH",
    "BASEMENT_RES", "BSMT_FINISH_RES",
    "BATHROOMS_RES", "GLA_BEDROOMS_RES",
    "PROP_TYPE_DESCR", "SPC_PROP_TYP_DESCR",
    "MKT_CUR_VALUE", "ACCOUNT_TYPE",
    "QUALITY_DESCR_RES", "STYLE_DESCR_RES", "COST_GROUP_RES",
    "NBHD_DISTRICT", "NEIGHBORHOOD",
]

CSV_FIELDS = [
    "priority_rank", "parcel_id", "owner_name", "property_address",
    "city", "state", "zip_code",
    "year_built", "above_grade_sqft",
    "total_bsmt_sqft", "finished_bsmt_sqft", "unfinished_bsmt_sqft",
    "pct_unfinished", "bedrooms", "bathrooms",
    "property_type", "quality_grade", "build_style",
    "is_production_builder", "builder_name", "builder_source",
    "nbhd_district", "development_rank", "development_size",
    "market_value", "estimated_value_add", "finish_cost_low", "finish_cost_high",
    "priority_score", "postcard_text", "detail_url",
]

# ── Logging ──────────────────────────────────────────────────────────────────

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/scrape_log.txt"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ── Builder detection ─────────────────────────────────────────────────────────

def detect_builder(owner_name: str | None) -> str | None:
    """Return display builder name if owner_name matches a known builder."""
    if not owner_name:
        return None
    upper = owner_name.upper()
    for fragment, display in BUILDER_LOOKUP:
        if fragment in upper:
            return display
    return None


# ── GIS queries ───────────────────────────────────────────────────────────────

def _paginate(where: str, out_fields: list[str], order_by: str = "") -> list[dict]:
    """Fetch all records matching a where clause, handling ArcGIS pagination."""
    all_records: list[dict] = []
    offset = 0
    page_size = 1000

    while True:
        params = {
            "where": where,
            "outFields": ",".join(out_fields),
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "f": "json",
        }
        if order_by:
            params["orderByFields"] = order_by

        r = requests.get(GIS_URL, params=params, verify=False, timeout=30)
        r.raise_for_status()
        data = r.json()

        features = data.get("features", [])
        if not features:
            break

        all_records.extend(f["attributes"] for f in features)

        if len(features) < page_size or not data.get("exceededTransferLimit"):
            break
        offset += page_size

    return all_records


def find_major_developments(city: str, cutoff_year: int) -> dict[str, dict]:
    """
    Phase 1 — Query all recent residential builds and group by NBHD_DISTRICT.

    Returns a dict keyed by NBHD_DISTRICT with:
      - count: number of recent residential builds in that district
      - rank: 1 = largest development
      - builder: inferred builder name (from OWNER_NAME of unsold units), or None
    """
    log.info(f"Phase 1: Finding major development districts (built {cutoff_year}+)...")

    where = (
        f"TAX_CITY='{city}'"
        f" AND ACCOUNT_TYPE='RESIDENTIAL'"
        f" AND YEARBLT_RES >= {cutoff_year}"
    )
    records = _paginate(where, ["NBHD_DISTRICT", "OWNER_NAME", "YEARBLT_RES"])

    # Count builds per district and collect any builder names seen
    district_counts: Counter = Counter()
    district_builders: dict[str, set] = defaultdict(set)

    for rec in records:
        district = rec.get("NBHD_DISTRICT")
        if district is None:
            continue
        district = str(district)
        district_counts[district] += 1

        builder = detect_builder(rec.get("OWNER_NAME"))
        if builder:
            district_builders[district].add(builder)

    # Build ranked development map
    developments: dict[str, dict] = {}
    for rank, (district, count) in enumerate(district_counts.most_common(), start=1):
        builder_list = sorted(district_builders.get(district, set()))
        developments[district] = {
            "count": count,
            "rank": rank,
            "builder": ", ".join(builder_list) if builder_list else None,
        }

    total_recent = sum(district_counts.values())
    log.info(f"  Found {len(developments)} districts with {total_recent:,} recent builds")
    log.info(f"  Top 5 development districts:")
    for district, info in list(developments.items())[:5]:
        builder_str = f" ({info['builder']})" if info['builder'] else ""
        log.info(f"    District {district}: {info['count']} homes{builder_str}")

    return developments


def fetch_qualifying_leads(city: str) -> list[dict]:
    """Phase 2 — Fetch all qualifying leads (unfinished basement)."""
    log.info("Phase 2: Fetching all qualifying leads...")

    where = (
        f"TAX_CITY='{city}'"
        " AND ACCOUNT_TYPE='RESIDENTIAL'"
        " AND TOTAL_BASEMENT > 0"
        " AND TOTAL_BASEMENT > TOTAL_BSMT_FINISH"
    )

    count_r = requests.get(GIS_URL, params={
        "where": where, "returnCountOnly": "true", "f": "json",
    }, verify=False, timeout=30)
    total = count_r.json().get("count", 0)
    log.info(f"  Total qualifying parcels: {total:,}")

    records = _paginate(where, GIS_FIELDS, order_by="TOTAL_BASEMENT DESC")
    log.info(f"  Fetched {len(records):,} records")
    return records


# ── Transform ─────────────────────────────────────────────────────────────────

def transform(raw: dict, developments: dict[str, dict]) -> dict:
    parcel_id = raw.get("PARCELID") or str(raw.get("PARCEL_NO", ""))
    total_bsmt = raw.get("TOTAL_BASEMENT") or 0
    finished_bsmt = raw.get("TOTAL_BSMT_FINISH") or 0
    unfinished_bsmt = total_bsmt - finished_bsmt
    pct_unfinished = round((unfinished_bsmt / total_bsmt * 100), 1) if total_bsmt else 0

    estimated_value_add = unfinished_bsmt * (FINISHED_VALUE_PER_SQFT - FINISHING_COST_PER_SQFT)
    finish_cost_low = unfinished_bsmt * 35
    finish_cost_high = unfinished_bsmt * 65

    year_built = raw.get("YEARBLT_RES") or raw.get("GLA_WEIGHTED_YRBLT")
    years_old = (2026 - year_built) if year_built else 0

    # Development info
    district = str(raw.get("NBHD_DISTRICT", "")) if raw.get("NBHD_DISTRICT") else None
    dev_info = developments.get(district, {}) if district else {}
    dev_rank = dev_info.get("rank")
    dev_size = dev_info.get("count", 0)
    is_major_development = dev_rank is not None and dev_rank <= 20

    # Builder detection
    # Direct: builder still owns the property (unsold inventory)
    direct_builder = detect_builder(raw.get("OWNER_NAME"))
    # Inferred: another unit in same district was sold by a known builder
    inferred_builder = dev_info.get("builder") if not direct_builder else None

    builder_name = direct_builder or inferred_builder
    builder_source = (
        "direct (still builder-owned)"
        if direct_builder else
        "inferred from neighborhood district"
        if inferred_builder else
        None
    )

    # Production builder flag: quality grade + not custom
    quality = raw.get("QUALITY_DESCR_RES") or ""
    is_production = quality in PRODUCTION_QUALITY_GRADES

    # ── Priority score ───────────────────────────────────────────────────────
    # Weights are designed so the ordering is:
    #   1. Major recent development (production tract) with 100% unfinished
    #   2. Any property with 100% unfinished basement
    #   3. Production quality, recent build
    #   4. Raw basement sqft

    score = 0.0

    # Completely unfinished basement — top signal
    if pct_unfinished == 100:
        score += 10.0
    else:
        score += pct_unfinished / 20.0       # partial credit up to ~5

    # In a major recent development tract — the "neighborhood campaign" signal
    if is_major_development:
        score += 8.0
        # Extra credit if it's a very large development (top 5)
        if dev_rank and dev_rank <= 5:
            score += 2.0

    # Production builder quality
    if is_production:
        score += 3.0

    # Recent build (likely still deciding whether to finish)
    if year_built and year_built >= RECENT_BUILD_CUTOFF:
        score += 3.0

    # Raw sqft — tiebreaker within same priority tier
    score += unfinished_bsmt / 500.0

    score = round(score, 2)

    # ── Output fields ────────────────────────────────────────────────────────
    address = raw.get("SITE_FULL_ADDRESS") or ""
    parts = address.split()
    zip_code = parts[-1] if parts and len(parts[-1]) == 5 and parts[-1].isdigit() else None

    owner = raw.get("OWNER_NAME") or "Homeowner"
    val_str = f"${int(estimated_value_add):,}" if estimated_value_add else "significant equity"
    yr_str = str(int(year_built)) if year_built else "your"
    postcard = (
        f"Hi {owner} — Your {yr_str} home at {address} has approximately "
        f"{int(unfinished_bsmt)} sqft of unfinished basement space. "
        f"Finishing it could add {val_str} in home equity. "
        f"Free estimate available this week."
    )

    detail_url = (
        f"https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp"
        f"?avParcelId={str(raw.get('PARCEL_NO', '')).zfill(9)}"
    )

    return {
        "priority_rank": None,          # filled in after sorting
        "parcel_id": parcel_id,
        "owner_name": raw.get("OWNER_NAME"),
        "property_address": address,
        "city": TARGET_CITY,
        "state": "UT",
        "zip_code": zip_code,
        "year_built": year_built,
        "above_grade_sqft": raw.get("TOTAL_ABOVE_GRADE_AREA"),
        "total_bsmt_sqft": total_bsmt,
        "finished_bsmt_sqft": finished_bsmt,
        "unfinished_bsmt_sqft": unfinished_bsmt,
        "pct_unfinished": pct_unfinished,
        "bedrooms": raw.get("GLA_BEDROOMS_RES"),
        "bathrooms": raw.get("BATHROOMS_RES"),
        "property_type": raw.get("PROP_TYPE_DESCR"),
        "quality_grade": quality,
        "build_style": raw.get("STYLE_DESCR_RES"),
        "is_production_builder": is_production,
        "builder_name": builder_name,
        "builder_source": builder_source,
        "nbhd_district": district,
        "development_rank": dev_rank,
        "development_size": dev_size,
        "market_value": raw.get("MKT_CUR_VALUE"),
        "estimated_value_add": int(estimated_value_add),
        "finish_cost_low": int(finish_cost_low),
        "finish_cost_high": int(finish_cost_high),
        "priority_score": score,
        "postcard_text": postcard,
        "detail_url": detail_url,
    }


# ── CSV writer ────────────────────────────────────────────────────────────────

def write_csv(leads: list[dict], filename: str) -> str:
    filepath = f"{OUTPUT_DIR}/{filename}"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    return filepath


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info(f"Starting Utah County basement lead scraper — {datetime.now()}")
    log.info(f"City: {TARGET_CITY} | Recent build cutoff: {RECENT_BUILD_CUTOFF}+")

    # Phase 1: find major development districts
    developments = find_major_developments(TARGET_CITY, RECENT_BUILD_CUTOFF)

    # Phase 2: fetch qualifying leads
    raw_records = fetch_qualifying_leads(TARGET_CITY)
    if not raw_records:
        log.error("No records returned from GIS API.")
        return

    # Transform and enrich
    leads = [transform(r, developments) for r in raw_records]

    # Sort: highest priority score first
    leads.sort(key=lambda x: x["priority_score"], reverse=True)

    # Assign final rank
    for i, lead in enumerate(leads, start=1):
        lead["priority_rank"] = i

    filepath = write_csv(leads, "american_fork_leads.csv")

    # Summary stats
    total = len(leads)
    fully_unfinished = sum(1 for l in leads if l["pct_unfinished"] == 100)
    production = sum(1 for l in leads if l["is_production_builder"])
    with_builder = sum(1 for l in leads if l["builder_name"])
    recent = sum(1 for l in leads if (l["year_built"] or 0) >= RECENT_BUILD_CUTOFF)
    in_major_dev = sum(1 for l in leads if l["development_rank"] and l["development_rank"] <= 20)

    log.info(f"\n{'=' * 60}")
    log.info(f"COMPLETE — {total:,} leads written to {filepath}")
    log.info(f"  100% unfinished basement:   {fully_unfinished:,}  ({fully_unfinished/total*100:.0f}%)")
    log.info(f"  Production builder quality: {production:,}  ({production/total*100:.0f}%)")
    log.info(f"  Builder identified:         {with_builder:,}  ({with_builder/total*100:.0f}%)")
    log.info(f"  Built {RECENT_BUILD_CUTOFF}+:                {recent:,}  ({recent/total*100:.0f}%)")
    log.info(f"  In major development:       {in_major_dev:,}  ({in_major_dev/total*100:.0f}%)")
    log.info(f"{'=' * 60}")

    print(f"\nTop 15 leads:")
    print(f"{'#':>3}  {'Address':<40} {'Unfin':>6}  {'100%':>4}  {'Builder':<22}  {'Dev Rank':>8}  {'Score':>6}")
    print("-" * 102)
    for lead in leads[:15]:
        addr = (lead["property_address"] or "")[:39]
        sqft = lead["unfinished_bsmt_sqft"]
        full = "YES" if lead["pct_unfinished"] == 100 else f"{lead['pct_unfinished']}%"
        builder = (lead["builder_name"] or "unknown")[:21]
        dev = f"#{lead['development_rank']}" if lead["development_rank"] else "—"
        score = lead["priority_score"]
        rank = lead["priority_rank"]
        print(f"{rank:>3}  {addr:<40} {sqft:>6}  {full:>4}  {builder:<22}  {dev:>8}  {score:>6.2f}")


if __name__ == "__main__":
    main()
