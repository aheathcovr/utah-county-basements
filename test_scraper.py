"""
test_scraper.py — Utah County Basement Lead Scraper (Test Run)

Finds 10 residential properties in American Fork, UT with unfinished basements.
Outputs results to output/american_fork_leads_test.csv.

Run: python3.12 test_scraper.py
"""

import asyncio
import csv
import logging
import os
import re
import sys
from datetime import datetime

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL = "https://www.utahcounty.gov/LandRecords"
SEARCH_URL = f"{BASE_URL}/SearchAppraisal.asp"
DETAIL_URL = f"{BASE_URL}/AppraisalInfo.asp"
TEST_PARCEL_ID = "513200002"      # Known good parcel for parser validation
TARGET_CITY = "AMERICAN FORK"
TARGET_LEADS = 10                 # Stop after this many qualifying leads
DELAY_BETWEEN_REQUESTS = 1.5     # Seconds — be polite to a government server
MAX_SEARCH_PAGES = 10            # Max pages to pull from search results
OUTPUT_DIR = "output"
LOG_DIR = "logs"

CSV_FIELDS = [
    "parcel_id", "owner_name", "property_address", "mailing_address",
    "city", "state", "zip_code", "year_built",
    "above_grade_sqft", "total_bsmt_sqft", "finished_bsmt_sqft",
    "unfinished_bsmt_sqft", "bedrooms", "bathrooms",
    "property_type", "assessed_value", "estimated_value_add", "detail_url",
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

# ── Shared crawl4ai configs ───────────────────────────────────────────────────

BROWSER_CFG = BrowserConfig(
    browser_type="chromium",
    headless=True,
    verbose=False,
)

# Config for search/detail pages — bypass cache so we always get fresh data
FETCH_CFG = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    wait_until="domcontentloaded",
    page_timeout=30_000,
)

# Config for search pages — wait a bit longer for table to populate
SEARCH_CFG = CrawlerRunConfig(
    cache_mode=CacheMode.BYPASS,
    wait_until="domcontentloaded",
    wait_for="table",           # Wait for a <table> element to appear
    page_timeout=30_000,
)

# ── HTML Parser ───────────────────────────────────────────────────────────────

def parse_parcel_html(html: str, parcel_id: str) -> dict | None:
    """
    Parse a Utah County Assessor property detail page.
    Returns a data dict if the property qualifies (has unfinished basement),
    or None if it should be skipped.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Build a lowercased label→value map from adjacent <td> pairs
    label_map: dict[str, str] = {}
    cells = soup.find_all("td")
    for i, cell in enumerate(cells):
        label = cell.get_text(strip=True).lower()
        if label and i + 1 < len(cells):
            value = cells[i + 1].get_text(strip=True)
            label_map[label] = value

    def get(keys: list[str]) -> str | None:
        for k in keys:
            if k in label_map:
                return label_map[k]
        return None

    def to_int(s: str | None) -> int | None:
        if s is None:
            return None
        try:
            return int(s.replace(",", "").replace("$", "").split(".")[0])
        except (ValueError, AttributeError):
            return None

    def to_float(s: str | None) -> float | None:
        if s is None:
            return None
        try:
            return float(s.replace(",", "").replace("$", ""))
        except (ValueError, AttributeError):
            return None

    # Property type — skip obvious non-residential
    prop_type = get(["property type", "prop type", "use code", "improvement type", "class"])
    if prop_type:
        non_res = ["commercial", "industrial", "condo", "apartment", "multi", "vacant", "land only"]
        if any(kw in prop_type.lower() for kw in non_res):
            return None

    total_bsmt = to_int(get([
        "bsmt sq ft", "basement sq ft", "basement sqft", "bsmt sqft", "total basement",
    ]))
    finished_bsmt = to_int(get([
        "bsmt sq ft finished", "basement finished", "finished basement",
        "bsmt finished sq ft", "basement sq ft finished",
    ]))

    # Must have a basement with unfinished space to qualify
    if not total_bsmt or total_bsmt == 0:
        return None
    unfinished_bsmt = total_bsmt - (finished_bsmt or 0)
    if unfinished_bsmt <= 0:
        return None

    # Value add estimate ($150/sqft finished value, $50/sqft finishing cost)
    estimated_value_add = unfinished_bsmt * (150 - 50)

    return {
        "parcel_id": parcel_id,
        "owner_name": get(["owner name", "owner", "taxpayer name", "taxpayer"]),
        "property_address": get(["property address", "situs address", "location address", "address"]),
        "mailing_address": get(["mailing address", "mail address", "owner address"]),
        "city": "American Fork",
        "state": "UT",
        "zip_code": get(["zip", "zip code", "postal code"]),
        "year_built": to_int(get(["year built", "yr built", "built"])),
        "above_grade_sqft": to_int(get([
            "above grade sq ft", "living area", "finished sq ft",
            "heated sq ft", "bldg sq ft", "gross sq ft",
        ])),
        "total_bsmt_sqft": total_bsmt,
        "finished_bsmt_sqft": finished_bsmt or 0,
        "unfinished_bsmt_sqft": unfinished_bsmt,
        "bedrooms": to_int(get(["bedrooms", "beds", "bedroom count", "no. bedrooms"])),
        "bathrooms": to_float(get(["bathrooms", "baths", "bathroom count", "no. bathrooms", "full baths"])),
        "property_type": prop_type,
        "assessed_value": to_int(get(["market value", "assessed value", "total value", "appraised value"])),
        "estimated_value_add": estimated_value_add,
        "detail_url": f"{DETAIL_URL}?avParcelId={parcel_id}",
    }


# ── Phase 0: robots.txt check ─────────────────────────────────────────────────

async def check_robots(crawler: AsyncWebCrawler) -> None:
    log.info("=== Phase 0: Checking robots.txt ===")
    try:
        result = await crawler.arun(
            url="https://www.utahcounty.gov/robots.txt",
            config=FETCH_CFG,
        )
        print("\n--- robots.txt ---")
        print(result.markdown[:2000] if result.markdown else "(empty)")
        print("--- end robots.txt ---\n")
    except Exception as e:
        log.warning(f"Could not fetch robots.txt: {e}")


# ── Phase 1: Explore a known parcel ──────────────────────────────────────────

async def explore_test_parcel(crawler: AsyncWebCrawler) -> None:
    log.info(f"=== Phase 1: Exploring known test parcel {TEST_PARCEL_ID} ===")
    url = f"{DETAIL_URL}?avParcelId={TEST_PARCEL_ID}"
    try:
        result = await crawler.arun(url=url, config=FETCH_CFG)
        if not result or not result.html or len(result.html) < 500:
            log.warning("Received empty/short response for test parcel")
            return

        print(f"\n--- HTML of parcel {TEST_PARCEL_ID} (first 6000 chars) ---")
        print(result.html[:6000])
        print("--- end HTML ---\n")

        # Try to parse it and show what we extracted
        data = parse_parcel_html(result.html, TEST_PARCEL_ID)
        if data:
            log.info(f"Parser extracted: {data}")
        else:
            log.info("Test parcel did not qualify (no unfinished basement, or not residential)")

    except Exception as e:
        log.error(f"Failed to fetch test parcel: {e}")

    await asyncio.sleep(DELAY_BETWEEN_REQUESTS)


# ── Phase 2: Collect parcel IDs from search results ───────────────────────────

async def collect_parcel_ids(crawler: AsyncWebCrawler) -> list[str]:
    log.info(f"=== Phase 2: Collecting parcel IDs for {TARGET_CITY} ===")
    parcel_ids: list[str] = []
    seen: set[str] = set()

    for page in range(1, MAX_SEARCH_PAGES + 1):
        # GET-based search — avStart offsets results (25 per page is typical)
        search_url = (
            f"{SEARCH_URL}"
            f"?avCity={TARGET_CITY.replace(' ', '+')}"
            f"&avType=RES"
            f"&avStart={((page - 1) * 25) + 1}"
        )

        log.info(f"  Fetching search page {page}: {search_url}")
        try:
            result = await crawler.arun(url=search_url, config=SEARCH_CFG)
            if not result or not result.html or len(result.html) < 500:
                log.warning(f"  Empty response on search page {page}, stopping pagination")
                break

            found = re.findall(r"avParcelId=(\d+)", result.html, re.IGNORECASE)
            new_ids = [pid for pid in found if pid not in seen]
            seen.update(new_ids)
            parcel_ids.extend(new_ids)

            log.info(f"  Page {page}: {len(new_ids)} new IDs (total: {len(parcel_ids)})")

            # For the test run, 200 IDs is more than enough
            if len(parcel_ids) >= 200:
                log.info("  200+ IDs collected — stopping early for test run")
                break

            # No new IDs = end of results
            if not new_ids:
                log.info("  No new IDs — search exhausted")
                break

            # Heuristic: if fewer than 20 results came back, likely the last page
            if len(found) < 20:
                log.info("  Partial page — likely last page of results")
                break

        except Exception as e:
            log.error(f"  Error on search page {page}: {e}")
            break

        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    log.info(f"Total parcel IDs collected: {len(parcel_ids)}")
    return parcel_ids


# ── Phase 3: Scrape detail pages with early exit ──────────────────────────────

async def scrape_until_target(
    crawler: AsyncWebCrawler, parcel_ids: list[str]
) -> list[dict]:
    log.info(f"=== Phase 3: Scraping detail pages (target: {TARGET_LEADS} leads) ===")
    leads: list[dict] = []
    processed = 0

    for parcel_id in parcel_ids:
        if len(leads) >= TARGET_LEADS:
            break

        url = f"{DETAIL_URL}?avParcelId={parcel_id}"
        log.info(f"  [{processed + 1}] Parcel {parcel_id} ...")

        try:
            result = await crawler.arun(url=url, config=FETCH_CFG)

            if not result or not result.html or len(result.html) < 500:
                log.debug(f"    Empty response — skipping")
                processed += 1
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)
                continue

            data = parse_parcel_html(result.html, parcel_id)

            if data:
                leads.append(data)
                log.info(
                    f"    LEAD #{len(leads)}: {data.get('property_address') or 'N/A'} "
                    f"— {data['unfinished_bsmt_sqft']} sqft unfinished"
                )
            else:
                log.debug(f"    Not qualifying")

        except Exception as e:
            log.error(f"    Error on parcel {parcel_id}: {e}")

        processed += 1
        await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

    log.info(f"Processed {processed} parcels, found {len(leads)} qualifying leads")
    return leads


# ── Phase 4: Write CSV ────────────────────────────────────────────────────────

def write_csv(leads: list[dict]) -> str:
    filepath = f"{OUTPUT_DIR}/american_fork_leads_test.csv"
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(leads)
    return filepath


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log.info(f"Starting Utah County basement scraper — {datetime.now()}")
    log.info(f"Target: {TARGET_LEADS} qualifying leads in {TARGET_CITY}")

    async with AsyncWebCrawler(config=BROWSER_CFG) as crawler:

        await check_robots(crawler)
        await explore_test_parcel(crawler)

        parcel_ids = await collect_parcel_ids(crawler)

        if not parcel_ids:
            log.error(
                "No parcel IDs found. Check Phase 1 HTML output to identify "
                "the correct search URL/form parameters for this portal."
            )
            return

        leads = await scrape_until_target(crawler, parcel_ids)

    if leads:
        filepath = write_csv(leads)
        log.info(f"\n{'=' * 55}")
        log.info(f"TEST RUN COMPLETE — {len(leads)} qualifying leads")
        log.info(f"Output: {filepath}")
        log.info(f"{'=' * 55}")
        print("\nTop leads:")
        for i, lead in enumerate(leads[:5], 1):
            addr = lead.get("property_address") or "N/A"
            sqft = lead["unfinished_bsmt_sqft"]
            val = lead["estimated_value_add"]
            print(f"  {i}. {addr} — {sqft} sqft unfinished (est. ${val:,} value add)")
    else:
        log.warning("No qualifying leads found.")
        log.info("Next steps:")
        log.info("  1. Review Phase 1 HTML — verify the exact label text for basement fields")
        log.info("  2. Review Phase 2 output — confirm parcel IDs were found in search results")
        log.info("  3. Adjust label_map keys in parse_parcel_html() to match actual HTML labels")


if __name__ == "__main__":
    asyncio.run(main())
