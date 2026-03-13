"""
test_scraper.py — Utah County Basement Lead Scraper

Queries the Utah County GIS ArcGIS REST API directly to find residential
properties in American Fork, UT with unfinished basements.

No web scraping needed — all CAMA improvement data (basement sqft, year built,
bedrooms, bathrooms, valuations) is available in the county's public GIS layer.

Data source:
  https://maps.utahcounty.gov/arcgis/rest/services/Assessor/TaxParcelAll_NoLabel/MapServer/0

Run: python3.12 test_scraper.py
"""

import csv
import logging
import os
import sys
import warnings
from datetime import datetime

import requests

warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# ── Constants ────────────────────────────────────────────────────────────────

GIS_URL = (
    "https://maps.utahcounty.gov/arcgis/rest/services"
    "/Assessor/TaxParcelAll_NoLabel/MapServer/0/query"
)

TARGET_CITY = "American Fork"
TARGET_LEADS = 10           # For test run; set to None to fetch all qualifying
OUTPUT_DIR = "output"
LOG_DIR = "logs"

# Value add estimate constants
FINISHED_VALUE_PER_SQFT = 150   # Estimated $/sqft added to market value
FINISHING_COST_PER_SQFT = 50    # Estimated $/sqft to finish

GIS_FIELDS = [
    "PARCEL_NO", "PARCELID", "OWNER_NAME", "SITE_FULL_ADDRESS", "TAX_CITY",
    "GLA_WEIGHTED_YRBLT", "TOTAL_ABOVE_GRADE_AREA",
    "TOTAL_BASEMENT", "TOTAL_BSMT_FINISH",
    "BASEMENT_RES", "BSMT_FINISH_RES",
    "BATHROOMS_RES", "GLA_BEDROOMS_RES",
    "PROP_TYPE_DESCR", "SPC_PROP_TYP_DESCR",
    "MKT_CUR_VALUE", "ACCOUNT_TYPE",
    "QUALITY_DESCR_RES", "STYLE_DESCR_RES",
]

CSV_FIELDS = [
    "parcel_id", "owner_name", "property_address", "city", "state", "zip_code",
    "year_built", "above_grade_sqft",
    "total_bsmt_sqft", "finished_bsmt_sqft", "unfinished_bsmt_sqft",
    "pct_unfinished", "bedrooms", "bathrooms",
    "property_type", "market_value",
    "estimated_value_add", "finish_cost_low", "finish_cost_high",
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

# ── GIS Query ────────────────────────────────────────────────────────────────

def query_gis(city: str, limit: int | None = None) -> list[dict]:
    """
    Query the Utah County ArcGIS parcel layer for residential properties
    in the given city that have unfinished basement square footage.
    """
    where = (
        f"TAX_CITY='{city}'"
        " AND ACCOUNT_TYPE='RESIDENTIAL'"
        " AND TOTAL_BASEMENT > 0"
        " AND TOTAL_BASEMENT > TOTAL_BSMT_FINISH"
    )

    # Get total count first
    count_r = requests.get(GIS_URL, params={
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }, verify=False, timeout=30)
    count_r.raise_for_status()
    total = count_r.json().get("count", 0)
    log.info(f"Total qualifying parcels in {city}: {total:,}")

    # Fetch records (ArcGIS default max is 1000 per request — paginate if needed)
    page_size = min(limit or 1000, 1000)
    all_records: list[dict] = []
    offset = 0

    while True:
        r = requests.get(GIS_URL, params={
            "where": where,
            "outFields": ",".join(GIS_FIELDS),
            "resultOffset": offset,
            "resultRecordCount": page_size,
            "orderByFields": "TOTAL_BASEMENT DESC",   # largest unfinished basements first
            "f": "json",
        }, verify=False, timeout=30)
        r.raise_for_status()
        data = r.json()

        features = data.get("features", [])
        if not features:
            break

        all_records.extend(f["attributes"] for f in features)
        log.info(f"  Fetched {len(all_records):,} records...")

        # Stop if we hit our limit or there are no more pages
        if limit and len(all_records) >= limit:
            all_records = all_records[:limit]
            break
        if len(features) < page_size or not data.get("exceededTransferLimit"):
            break

        offset += page_size

    return all_records


# ── Transform raw GIS record → clean lead dict ────────────────────────────────

def transform(raw: dict) -> dict:
    parcel_id = raw.get("PARCELID") or str(raw.get("PARCEL_NO", ""))
    total_bsmt = raw.get("TOTAL_BASEMENT") or 0
    finished_bsmt = raw.get("TOTAL_BSMT_FINISH") or 0
    unfinished_bsmt = total_bsmt - finished_bsmt
    pct_unfinished = round((unfinished_bsmt / total_bsmt * 100), 1) if total_bsmt else 0

    estimated_value_add = unfinished_bsmt * (FINISHED_VALUE_PER_SQFT - FINISHING_COST_PER_SQFT)
    finish_cost_low = unfinished_bsmt * 35
    finish_cost_high = unfinished_bsmt * 65

    year_built = raw.get("GLA_WEIGHTED_YRBLT")
    years_old = (2026 - year_built) if year_built else 0

    priority_score = round(
        (unfinished_bsmt / 100)
        + (min(years_old, 20) / 5)
        + (pct_unfinished / 50),
        2,
    )

    address = raw.get("SITE_FULL_ADDRESS") or ""
    # Parse zip from "97 S 200 EAST, AMERICAN FORK, UT 84003" if present
    parts = address.split()
    zip_code = parts[-1] if parts and len(parts[-1]) == 5 and parts[-1].isdigit() else None

    owner = raw.get("OWNER_NAME") or "Homeowner"
    sqft = int(unfinished_bsmt)
    val_str = f"${int(estimated_value_add):,}" if estimated_value_add else "significant equity"
    yr_str = str(int(year_built)) if year_built else "your"
    postcard = (
        f"Hi {owner} — Your {yr_str} home at {address} has approximately "
        f"{sqft} sqft of unfinished basement space. Finishing it could add "
        f"{val_str} in home equity. Free estimate available this week."
    )

    detail_url = (
        f"https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp"
        f"?avParcelId={str(raw.get('PARCEL_NO', '')).zfill(9)}"
    )

    return {
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
        "market_value": raw.get("MKT_CUR_VALUE"),
        "estimated_value_add": int(estimated_value_add),
        "finish_cost_low": int(finish_cost_low),
        "finish_cost_high": int(finish_cost_high),
        "priority_score": priority_score,
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
    log.info(f"Data source: Utah County GIS ArcGIS REST API")
    log.info(f"Target: {TARGET_LEADS or 'ALL'} qualifying leads in {TARGET_CITY}")

    raw_records = query_gis(TARGET_CITY, limit=TARGET_LEADS)

    if not raw_records:
        log.error("No records returned from GIS API.")
        return

    leads = [transform(r) for r in raw_records]

    # Sort by priority score descending
    leads.sort(key=lambda x: x["priority_score"], reverse=True)

    filepath = write_csv(leads, "american_fork_leads_test.csv")

    log.info(f"\n{'=' * 55}")
    log.info(f"COMPLETE — {len(leads)} leads written to {filepath}")
    log.info(f"{'=' * 55}")

    print(f"\nTop {min(10, len(leads))} leads by priority score:")
    print(f"{'Address':<45} {'Unfinished':>10} {'Est. Value Add':>14} {'Score':>6}")
    print("-" * 80)
    for lead in leads[:10]:
        addr = (lead["property_address"] or "N/A")[:44]
        sqft = lead["unfinished_bsmt_sqft"]
        val = lead["estimated_value_add"]
        score = lead["priority_score"]
        print(f"{addr:<45} {sqft:>8} sqft  ${val:>11,}  {score:>5.2f}")


if __name__ == "__main__":
    main()
