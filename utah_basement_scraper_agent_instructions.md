# Agent Instructions: Utah County Unfinished Basement Lead Scraper
## Using crawl4ai — American Fork, UT Test Run

---

## MISSION SUMMARY

You are building a Python script that scrapes **publicly available property assessment data** from Utah County's online parcel search portal to identify **single-family homes in American Fork, UT with unfinished or partially-finished basements**. This data is used to generate a direct-mail and door-to-door lead list for a general contractor specializing in basement finishing.

The final output is a **clean CSV file** with one row per qualifying property, containing all fields needed to write a personalized outreach piece (owner name, mailing address, basement square footage, finished vs. unfinished breakdown, estimated value add, year built, etc.).

You must use the **`crawl4ai`** Python package as the core scraping engine. Do not use Selenium, Playwright directly, or requests/BeautifulSoup as the primary scraper — use crawl4ai's `AsyncWebCrawler` interface, which wraps Playwright under the hood and handles JavaScript-rendered pages.

---

## CONTEXT: WHY THIS WORKS

Utah County's Assessor maintains a Computer Assisted Mass Appraisal (CAMA) system that tracks the following fields for every residential improvement:

- `Bsmt Sq Ft` — total basement square footage
- `Bsmt Sq Ft Finished` — finished portion of basement
- Derived: `Bsmt Sq Ft Unfinished` = `Bsmt Sq Ft` minus `Bsmt Sq Ft Finished`
- Year Built, Quality Grade, Above-Grade SqFt, Bedrooms, Bathrooms, Owner Name, Mailing Address

The public parcel search portal is:
```
https://www.utahcounty.gov/LandRecords/Index.asp
```

Individual property detail pages follow the pattern:
```
https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp?avParcelId=XXXXXXXXX
```

The portal also supports address-based and city-based searches. You will use city-based filtering to scope the test run to **American Fork only**.

A qualifying lead is any parcel where:
- Property type is single-family residential
- `Bsmt Sq Ft > 0`
- `Bsmt Sq Ft Finished < Bsmt Sq Ft` (i.e., there is unfinished space remaining)

---

## PHASE 0: ENVIRONMENT SETUP

### Install dependencies

```bash
pip install crawl4ai pandas asyncio aiohttp
playwright install chromium
```

crawl4ai requires Playwright's Chromium browser to be installed separately. Run the playwright install command after pip install.

Verify crawl4ai version >= 0.3.0 (async interface required). Import pattern:

```python
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy, NoExtractionStrategy
```

### File structure to create

```
utah_basement_scraper/
├── scraper.py           # Main script (build this)
├── config.py            # Constants and settings
├── parser.py            # HTML parsing helpers
├── output/
│   └── american_fork_leads.csv   # Final output
└── logs/
    └── scrape_log.txt   # Run log
```

---

## PHASE 1: UNDERSTAND THE TARGET PORTAL

Before writing the main scraper, your first task is to **inspect the portal structure**. Write a quick exploration script using crawl4ai to fetch and print the raw HTML of the search page so you can identify:

1. The form fields and their `name` attributes for city-based search
2. The structure of search results (table? list? pagination?)
3. The URL pattern for individual parcel detail pages
4. Whether the site uses JavaScript rendering (crawl4ai handles this, but note if `wait_for` selectors are needed)

```python
# exploration.py — run this FIRST before building the main scraper
import asyncio
from crawl4ai import AsyncWebCrawler

async def explore():
    async with AsyncWebCrawler(verbose=True) as crawler:
        # Step 1: Fetch the main search page
        result = await crawler.arun(
            url="https://www.utahcounty.gov/LandRecords/Index.asp",
            bypass_cache=True
        )
        print("=== SEARCH PAGE HTML (first 5000 chars) ===")
        print(result.html[:5000])
        print("\n=== EXTRACTED TEXT ===")
        print(result.markdown[:3000])

asyncio.run(explore())
```

**What to look for in the HTML output:**
- `<form>` action URL — this is where search POSTs go
- `<select>` or `<input>` for city filtering — look for a dropdown named something like `avCity`, `city`, `avMunicip`
- `<input>` for parcel ID search — likely named `avParcelId` or similar
- Search result table structure — note the column headers (Parcel ID, Owner, Address, etc.)
- Any `<a>` href patterns linking to detail pages

**Alternative entry point to also explore:**
```
https://www.utahcounty.gov/LandRecords/SearchAppraisal.asp
```

---

## PHASE 2: DISCOVER THE SEARCH MECHANISM

After exploring the HTML, determine whether the portal uses:

**Option A: GET request with query parameters**
- Example: `?avCity=AMERICAN+FORK&avType=RES`
- In this case, construct URL strings directly

**Option B: POST form submission**
- Use crawl4ai's `js_code` parameter to simulate form fill + submit
- Example approach:

```python
result = await crawler.arun(
    url="https://www.utahcounty.gov/LandRecords/SearchAppraisal.asp",
    js_code="""
        document.querySelector('select[name="avCity"]').value = 'AMERICAN FORK';
        document.querySelector('input[name="avType"]').value = 'RES';
        document.querySelector('form').submit();
    """,
    wait_for="table.results",  # CSS selector to wait for after form submit
    bypass_cache=True
)
```

**Option C: Direct parcel ID range iteration**
- American Fork parcels in Utah County typically begin with a specific prefix (e.g., `14` or `15` based on the county's APN schema)
- This is the nuclear option — iterate through a known APN range and scrape each detail page
- Only use this if the search portal proves too difficult to automate

**Recommended approach:** Try Option A first (direct URL construction with city parameter), then fall back to B, then C.

---

## PHASE 3: BUILD THE SEARCH RESULTS SCRAPER

Once you understand the search mechanism, write the `scrape_search_results()` function. This function should:

1. Submit a search for all residential parcels in American Fork
2. Collect all parcel IDs (or direct links to detail pages) from the results
3. Handle pagination — the portal likely shows 25–50 results per page, and American Fork has thousands of parcels

```python
async def scrape_search_results(crawler: AsyncWebCrawler, city: str = "AMERICAN FORK") -> list[str]:
    """
    Returns a list of parcel IDs or detail page URLs for all residential
    properties in the specified city.
    """
    parcel_ids = []
    page = 1

    while True:
        # Construct the search URL — adjust parameter names based on Phase 1 exploration
        search_url = (
            f"https://www.utahcounty.gov/LandRecords/SearchAppraisal.asp"
            f"?avCity={city.replace(' ', '+')}"
            f"&avType=RES"          # Residential only
            f"&avPage={page}"       # Pagination — adjust param name if different
        )

        result = await crawler.arun(
            url=search_url,
            bypass_cache=True,
            wait_for="table",       # Wait for results table to load
        )

        # Parse parcel IDs from the result HTML
        # Look for links like: /LandRecords/AppraisalInfo.asp?avParcelId=XXXXXXXXX
        import re
        found_ids = re.findall(r'avParcelId=(\d+)', result.html)

        if not found_ids:
            break  # No more results — exit pagination loop

        parcel_ids.extend(found_ids)
        print(f"Page {page}: found {len(found_ids)} parcels (total: {len(parcel_ids)})")

        # Check if there's a "Next" page link — if not, we're done
        if 'next page' not in result.markdown.lower() and 'next' not in result.html.lower():
            break

        page += 1
        await asyncio.sleep(1)  # Be polite — 1 second delay between pages

    # Deduplicate
    return list(set(parcel_ids))
```

**Important parsing notes:**
- Parcel IDs in Utah County are typically 9–12 digit numeric strings
- The HTML anchor tags linking to detail pages will contain `avParcelId=` in the href
- Use regex on `result.html` rather than trying to parse with crawl4ai's extraction strategies for this step, since the table structure may vary
- Log the total count found before proceeding — American Fork should have roughly 15,000–25,000 total parcels, of which maybe 8,000–12,000 are single-family residential

---

## PHASE 4: BUILD THE DETAIL PAGE SCRAPER

This is the core of the scraper. For each parcel ID collected in Phase 3, fetch the detail page and extract the CAMA data fields.

```python
async def scrape_parcel_detail(crawler: AsyncWebCrawler, parcel_id: str) -> dict | None:
    """
    Fetches a single parcel detail page and extracts all relevant fields.
    Returns a dict of property data, or None if not a qualifying lead.
    """
    url = f"https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp?avParcelId={parcel_id}"

    result = await crawler.arun(
        url=url,
        bypass_cache=True,
    )

    return parse_parcel_html(result.html, parcel_id)
```

---

## PHASE 5: BUILD THE HTML PARSER

This is the most critical function to get right. The Utah County assessor detail page displays data in an HTML table format. Your parser needs to extract specific labeled fields.

```python
def parse_parcel_html(html: str, parcel_id: str) -> dict | None:
    """
    Parses a Utah County Assessor property detail page.

    The page typically has sections:
    - Property/Parcel Info (owner name, mailing address, property address, APN)
    - Land Info (lot size, zoning)
    - Improvement Info (year built, quality, sqft, basement data)
    - Tax Info (assessed value, market value)

    Returns None if:
    - Property type is not single-family residential
    - No basement square footage recorded
    - Basement is already 100% finished
    """
    from bs4 import BeautifulSoup
    import re

    soup = BeautifulSoup(html, 'html.parser')

    # Initialize data dict
    data = {
        'parcel_id': parcel_id,
        'owner_name': None,
        'property_address': None,
        'mailing_address': None,
        'city': 'American Fork',
        'state': 'UT',
        'zip_code': None,
        'year_built': None,
        'above_grade_sqft': None,
        'total_bsmt_sqft': None,
        'finished_bsmt_sqft': None,
        'unfinished_bsmt_sqft': None,
        'bedrooms': None,
        'bathrooms': None,
        'property_type': None,
        'assessed_value': None,
        'estimated_value_add': None,
        'detail_url': f"https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp?avParcelId={parcel_id}"
    }

    # STRATEGY: Find all table cells that act as label:value pairs
    # The assessor page uses <td> elements where one cell is the label
    # and the adjacent cell is the value
    # Example HTML pattern:
    #   <td class="label">Bsmt Sq Ft</td><td class="value">1200</td>

    # Build a label->value mapping from the entire page
    label_value_map = {}
    cells = soup.find_all('td')
    for i, cell in enumerate(cells):
        text = cell.get_text(strip=True)
        if text and i + 1 < len(cells):
            next_cell = cells[i + 1]
            next_text = next_cell.get_text(strip=True)
            label_value_map[text.lower()] = next_text

    # --- Extract fields using fuzzy label matching ---
    # Property type check — skip non-residential
    prop_type_keys = ['property type', 'prop type', 'use code', 'improvement type', 'class']
    for key in prop_type_keys:
        if key in label_value_map:
            data['property_type'] = label_value_map[key]
            break

    # If property type is found and is clearly not single-family, skip
    if data['property_type']:
        non_residential_keywords = ['commercial', 'industrial', 'condo', 'apartment', 'multi', 'vacant', 'land only']
        if any(kw in data['property_type'].lower() for kw in non_residential_keywords):
            return None

    # Owner name
    for key in ['owner name', 'owner', 'taxpayer name', 'taxpayer']:
        if key in label_value_map:
            data['owner_name'] = label_value_map[key]
            break

    # Property address
    for key in ['property address', 'situs address', 'location address', 'address']:
        if key in label_value_map:
            data['property_address'] = label_value_map[key]
            break

    # Mailing address (may span multiple cells — capture all parts)
    for key in ['mailing address', 'mail address', 'owner address']:
        if key in label_value_map:
            data['mailing_address'] = label_value_map[key]
            break

    # Year built
    for key in ['year built', 'yr built', 'built']:
        if key in label_value_map:
            try:
                data['year_built'] = int(label_value_map[key])
            except (ValueError, TypeError):
                pass
            break

    # Above-grade square footage
    for key in ['above grade sq ft', 'living area', 'finished sq ft', 'heated sq ft', 'bldg sq ft', 'gross sq ft']:
        if key in label_value_map:
            try:
                data['above_grade_sqft'] = int(label_value_map[key].replace(',', ''))
            except (ValueError, TypeError):
                pass
            break

    # CRITICAL FIELDS: Basement square footage
    for key in ['bsmt sq ft', 'basement sq ft', 'basement sqft', 'bsmt sqft', 'total basement']:
        if key in label_value_map:
            try:
                data['total_bsmt_sqft'] = int(label_value_map[key].replace(',', ''))
            except (ValueError, TypeError):
                pass
            break

    for key in ['bsmt sq ft finished', 'basement finished', 'finished basement', 'bsmt finished sq ft']:
        if key in label_value_map:
            try:
                data['finished_bsmt_sqft'] = int(label_value_map[key].replace(',', ''))
            except (ValueError, TypeError):
                pass
            break

    # If we found total basement sqft, calculate unfinished
    if data['total_bsmt_sqft'] is not None:
        finished = data['finished_bsmt_sqft'] or 0
        data['unfinished_bsmt_sqft'] = data['total_bsmt_sqft'] - finished

    # Bedrooms and bathrooms
    for key in ['bedrooms', 'beds', 'bedroom count', 'no. bedrooms']:
        if key in label_value_map:
            try:
                data['bedrooms'] = int(label_value_map[key])
            except (ValueError, TypeError):
                pass
            break

    for key in ['bathrooms', 'baths', 'bathroom count', 'no. bathrooms', 'full baths']:
        if key in label_value_map:
            try:
                data['bathrooms'] = float(label_value_map[key])
            except (ValueError, TypeError):
                pass
            break

    # Assessed / market value
    for key in ['market value', 'assessed value', 'total value', 'appraised value']:
        if key in label_value_map:
            try:
                val_str = label_value_map[key].replace('$', '').replace(',', '')
                data['assessed_value'] = int(float(val_str))
            except (ValueError, TypeError):
                pass
            break

    # --- QUALIFY THE LEAD ---
    # Must have basement sqft > 0 and unfinished sqft > 0
    if not data['total_bsmt_sqft'] or data['total_bsmt_sqft'] == 0:
        return None  # No basement recorded — skip
    if not data['unfinished_bsmt_sqft'] or data['unfinished_bsmt_sqft'] <= 0:
        return None  # Basement already fully finished — skip

    # --- CALCULATE ESTIMATED VALUE ADD ---
    # Utah County finished basement value: ~50-60% of above-grade $/sqft
    # Approximate above-grade value in American Fork: ~$270/sqft (2025 estimate)
    # Finished basement value: ~$150/sqft
    # Finishing cost: ~$35-65/sqft (use $50 midpoint)
    FINISHED_BSMT_VALUE_PER_SQFT = 150
    FINISHING_COST_PER_SQFT = 50

    unfinished = data['unfinished_bsmt_sqft']
    gross_value_add = unfinished * FINISHED_BSMT_VALUE_PER_SQFT
    net_value_add = gross_value_add - (unfinished * FINISHING_COST_PER_SQFT)
    data['estimated_value_add'] = net_value_add

    return data
```

**IMPORTANT PARSING CAVEAT:** The exact label text on the Utah County assessor page must be verified during Phase 1 exploration. The labels above (`Bsmt Sq Ft`, `Bsmt Sq Ft Finished`, etc.) are based on known Utah county CAMA conventions but may appear differently on this specific portal. When you run the exploration script, print the full HTML of a known property detail page and adjust the label matching accordingly.

A known test parcel to verify your parser against:
```
https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp?avParcelId=513200002
```
Fetch this page first, print its HTML, and confirm which labels map to which fields before running at scale.

---

## PHASE 6: BUILD THE MAIN ORCHESTRATOR WITH RATE LIMITING

Crawling thousands of pages requires careful rate limiting to avoid being blocked. Utah County's portal is a government site — be respectful.

```python
import asyncio
import csv
import logging
from datetime import datetime
from crawl4ai import AsyncWebCrawler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/scrape_log.txt'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ---- RATE LIMITING CONSTANTS ----
DELAY_BETWEEN_REQUESTS = 1.5    # seconds between individual parcel fetches
DELAY_BETWEEN_PAGES = 2.0       # seconds between search result pages
BATCH_SIZE = 20                 # process this many parcels before writing to CSV
MAX_CONCURRENT = 1              # keep at 1 for government sites — no parallelism
TEST_MODE_LIMIT = 50            # for the test run, stop after this many parcels

async def main():
    log.info(f"Starting American Fork basement scraper — {datetime.now()}")
    all_leads = []
    processed = 0
    qualified = 0

    async with AsyncWebCrawler(
        verbose=False,
        headless=True,           # Run browser headlessly
        browser_type="chromium", # Use Chromium (default)
        sleep_on_close=False,
    ) as crawler:

        # PHASE 3: Get all parcel IDs
        log.info("Fetching search results for American Fork residential parcels...")
        parcel_ids = await scrape_search_results(crawler, city="AMERICAN FORK")
        log.info(f"Found {len(parcel_ids)} total parcel IDs to process")

        # For the TEST RUN: limit to first N parcels
        if TEST_MODE_LIMIT:
            parcel_ids = parcel_ids[:TEST_MODE_LIMIT]
            log.info(f"TEST MODE: limiting to {TEST_MODE_LIMIT} parcels")

        # PHASE 4 & 5: Scrape and parse each parcel
        for i, parcel_id in enumerate(parcel_ids):
            try:
                log.info(f"[{i+1}/{len(parcel_ids)}] Scraping parcel {parcel_id}...")
                data = await scrape_parcel_detail(crawler, parcel_id)

                if data:
                    all_leads.append(data)
                    qualified += 1
                    log.info(f"  ✓ QUALIFIED LEAD: {data.get('property_address')} — "
                             f"{data.get('unfinished_bsmt_sqft')} sqft unfinished")
                else:
                    log.debug(f"  ✗ Not a qualifying lead")

                processed += 1

                # Write to CSV every BATCH_SIZE records
                if len(all_leads) >= BATCH_SIZE:
                    write_csv(all_leads, mode='a')
                    log.info(f"  → Wrote batch of {len(all_leads)} leads to CSV")
                    all_leads = []

                # Rate limiting
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

            except Exception as e:
                log.error(f"  ERROR on parcel {parcel_id}: {e}")
                continue

        # Write any remaining leads
        if all_leads:
            write_csv(all_leads, mode='a')

    log.info(f"\n{'='*50}")
    log.info(f"SCRAPE COMPLETE")
    log.info(f"Total parcels processed: {processed}")
    log.info(f"Qualifying leads found: {qualified}")
    log.info(f"Output: output/american_fork_leads.csv")


def write_csv(leads: list[dict], mode: str = 'w'):
    """Write leads to CSV. mode='w' to overwrite, 'a' to append."""
    import os
    os.makedirs('output', exist_ok=True)

    filepath = 'output/american_fork_leads.csv'
    fieldnames = [
        'parcel_id', 'owner_name', 'property_address', 'mailing_address',
        'city', 'state', 'zip_code', 'year_built',
        'above_grade_sqft', 'total_bsmt_sqft', 'finished_bsmt_sqft',
        'unfinished_bsmt_sqft', 'bedrooms', 'bathrooms',
        'property_type', 'assessed_value', 'estimated_value_add', 'detail_url'
    ]

    # Write header only on first write
    write_header = not os.path.exists(filepath) or mode == 'w'

    with open(filepath, mode=mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(leads)


if __name__ == "__main__":
    asyncio.run(main())
```

---

## PHASE 7: ALTERNATIVE DATA SOURCE — UTAH OPEN DATA PORTAL

If the assessor portal search proves too difficult to automate (anti-bot protection, CAPTCHA, session requirements), fall back to the **Utah Open Data Portal** which provides bulk parcel downloads via the Socrata API — no scraping required.

```python
# alternative_download.py — use this if portal scraping fails
import requests
import pandas as pd

def download_utah_county_parcels():
    """
    Download Utah County parcel data from the Utah Open Data Portal via Socrata API.
    Filter for American Fork and check for basement fields.

    Socrata API docs: https://dev.socrata.com/docs/queries/
    Dataset: https://opendata.utah.gov/dataset/Tax-Parcels/essh-4bab
    """

    # Socrata API endpoint for the Utah Tax Parcels dataset
    # The dataset ID 'essh-4bab' is the Utah statewide tax parcels dataset
    base_url = "https://opendata.utah.gov/resource/essh-4bab.json"

    # Filter for Utah County + American Fork
    # Note: column names depend on the actual schema — verify at opendata.utah.gov
    params = {
        '$where': "county_name='UTAH' AND city='AMERICAN FORK'",
        '$limit': 50000,
        '$offset': 0,
    }

    all_records = []
    while True:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        records = response.json()

        if not records:
            break

        all_records.extend(records)
        print(f"Downloaded {len(all_records)} records so far...")

        if len(records) < 50000:
            break
        params['$offset'] += 50000

    df = pd.DataFrame(all_records)
    print(f"\nTotal records: {len(df)}")
    print(f"Columns available: {list(df.columns)}")

    # Look for basement-related columns
    basement_cols = [c for c in df.columns if 'bsmt' in c.lower() or 'basement' in c.lower()]
    print(f"Basement columns found: {basement_cols}")

    # If basement columns exist, filter for unfinished basements
    if basement_cols:
        # Adjust column names based on actual schema
        total_bsmt_col = next((c for c in basement_cols if 'total' in c.lower() or 'sqft' in c.lower()), None)
        finished_bsmt_col = next((c for c in basement_cols if 'finish' in c.lower()), None)

        if total_bsmt_col and finished_bsmt_col:
            df[total_bsmt_col] = pd.to_numeric(df[total_bsmt_col], errors='coerce').fillna(0)
            df[finished_bsmt_col] = pd.to_numeric(df[finished_bsmt_col], errors='coerce').fillna(0)
            df['unfinished_sqft'] = df[total_bsmt_col] - df[finished_bsmt_col]

            leads = df[(df[total_bsmt_col] > 0) & (df['unfinished_sqft'] > 0)]
            print(f"\nQualifying leads (unfinished basement): {len(leads)}")
            leads.to_csv('output/american_fork_leads_opendata.csv', index=False)
        else:
            print("Could not identify basement sqft columns — saving full dataset for manual inspection")
            df.to_csv('output/american_fork_all_parcels.csv', index=False)
    else:
        print("WARNING: No basement columns found in this dataset.")
        print("The statewide parcel dataset may not include CAMA improvement data.")
        print("Falling back to scraping the assessor portal directly is required.")
        df.to_csv('output/american_fork_all_parcels.csv', index=False)

    return df

# Run it
download_utah_county_parcels()
```

**Important note about the Open Data Portal dataset:** The statewide parcel layer distributed through UGRC/opendata.utah.gov may only include basic parcel geometry + land info (APN, owner, address, land value) but **not** the full CAMA improvement data (basement sqft fields). If the downloaded CSV lacks basement columns, you must either:
1. Submit a GRAMA public records request to Utah County Assessor for the CAMA data export (email: assessor@utahcounty.gov) — they are legally required to provide this
2. Fall back to scraping the portal directly using the main scraper

---

## PHASE 8: OUTPUT FORMAT AND POST-PROCESSING

After scraping, run this post-processing step to clean the data, calculate personalization fields, and sort leads by priority:

```python
import pandas as pd

def post_process(filepath: str = 'output/american_fork_leads.csv'):
    df = pd.read_csv(filepath)

    # Clean numeric fields
    for col in ['total_bsmt_sqft', 'finished_bsmt_sqft', 'unfinished_bsmt_sqft',
                'above_grade_sqft', 'assessed_value', 'year_built']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Recalculate unfinished sqft (in case of parsing inconsistencies)
    df['unfinished_bsmt_sqft'] = df['total_bsmt_sqft'] - df['finished_bsmt_sqft'].fillna(0)

    # Percent unfinished
    df['pct_unfinished'] = ((df['unfinished_bsmt_sqft'] / df['total_bsmt_sqft']) * 100).round(1)

    # Estimated value add (recalculate with constants)
    df['estimated_value_add'] = (df['unfinished_bsmt_sqft'] * 150) - (df['unfinished_bsmt_sqft'] * 50)

    # Estimated finishing cost (low/high range)
    df['finish_cost_low'] = (df['unfinished_bsmt_sqft'] * 35).astype(int)
    df['finish_cost_high'] = (df['unfinished_bsmt_sqft'] * 65).astype(int)

    # Years since built (opportunity age signal)
    df['years_since_built'] = 2026 - df['year_built']

    # Priority score (higher = better lead)
    # Prioritize: large unfinished sqft + older home (more likely ready for renovation)
    df['priority_score'] = (
        (df['unfinished_bsmt_sqft'] / 100) +           # Weight sqft heavily
        (df['years_since_built'].clip(0, 20) / 5) +    # Up to 20 years old = higher priority
        (df['pct_unfinished'] / 50)                    # Fully unfinished = higher priority
    ).round(2)

    # Generate personalized postcard text field
    def make_postcard_text(row):
        name = row['owner_name'] or 'Homeowner'
        addr = row['property_address'] or 'your home'
        sqft = int(row['unfinished_bsmt_sqft']) if pd.notna(row['unfinished_bsmt_sqft']) else '???'
        val = f"${int(row['estimated_value_add']):,}" if pd.notna(row['estimated_value_add']) else 'significant equity'
        yr = int(row['year_built']) if pd.notna(row['year_built']) else 'your'
        return (
            f"Hi {name} — Your {yr} home at {addr} has approximately {sqft} sqft "
            f"of unfinished basement space. Finishing it could add {val} in home equity. "
            f"Free estimate available this week."
        )

    df['postcard_text'] = df.apply(make_postcard_text, axis=1)

    # Sort by priority score descending
    df = df.sort_values('priority_score', ascending=False)

    # Save final output
    df.to_csv('output/american_fork_leads_final.csv', index=False)
    print(f"Post-processing complete: {len(df)} qualified leads")
    print(f"Top lead: {df.iloc[0]['property_address']} — {df.iloc[0]['unfinished_bsmt_sqft']} sqft unfinished")
    print(f"Average unfinished sqft: {df['unfinished_bsmt_sqft'].mean():.0f}")
    print(f"Estimated value add range: ${df['estimated_value_add'].min():,.0f} – ${df['estimated_value_add'].max():,.0f}")

    return df

post_process()
```

---

## EXPECTED OUTPUT COLUMNS (final CSV)

| Column | Description | Example |
|---|---|---|
| `parcel_id` | Utah County APN | `140250010` |
| `owner_name` | Property owner as listed on tax record | `SMITH JOHN D` |
| `property_address` | Street address of the property | `437 N 300 W` |
| `mailing_address` | Owner's mailing address (may differ) | `437 N 300 W American Fork UT 84003` |
| `city` | City | `American Fork` |
| `state` | State | `UT` |
| `zip_code` | ZIP code | `84003` |
| `year_built` | Year of construction | `2008` |
| `above_grade_sqft` | Finished above-grade living area | `2100` |
| `total_bsmt_sqft` | Total basement footprint | `1400` |
| `finished_bsmt_sqft` | Portion already finished | `0` |
| `unfinished_bsmt_sqft` | Opportunity (what contractor would finish) | `1400` |
| `bedrooms` | Current bedroom count | `4` |
| `bathrooms` | Current bathroom count | `2.5` |
| `property_type` | Assessor classification | `Single Family` |
| `assessed_value` | County assessed market value | `$485,000` |
| `estimated_value_add` | Net equity gain from finishing basement | `$140,000` |
| `finish_cost_low` | Low-end finishing cost estimate | `$49,000` |
| `finish_cost_high` | High-end finishing cost estimate | `$91,000` |
| `pct_unfinished` | Percent of basement still unfinished | `100.0` |
| `priority_score` | Calculated lead quality score | `18.4` |
| `postcard_text` | Pre-written personalized outreach text | `Hi SMITH JOHN D — Your 2008 home at...` |
| `detail_url` | Link back to assessor record | `https://...` |

---

## ERROR HANDLING REQUIREMENTS

Your scraper must handle the following error conditions gracefully (log and continue, never crash):

1. **HTTP errors (403, 404, 500):** Log the parcel ID and error code, skip to next
2. **Empty HTML response:** Log and skip
3. **Missing fields in HTML:** Use `None` for the field, still include the record if basement fields are present
4. **Network timeouts:** Implement retry logic with 3 attempts and exponential backoff (2s, 4s, 8s)
5. **Parsing exceptions:** Wrap `parse_parcel_html()` in try/except, log the parcel ID and exception
6. **Rate limit detection:** If you receive 429 or see a CAPTCHA in the HTML, pause for 60 seconds then resume

```python
async def scrape_with_retry(crawler, parcel_id, max_retries=3):
    for attempt in range(max_retries):
        try:
            result = await crawler.arun(
                url=f"https://www.utahcounty.gov/LandRecords/AppraisalInfo.asp?avParcelId={parcel_id}",
                bypass_cache=True,
            )
            if result and result.html and len(result.html) > 500:
                return result
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for {parcel_id}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
    return None
```

---

## TEST RUN CHECKLIST

Before running at scale, validate these milestones in order:

- [ ] **Milestone 1:** Run `exploration.py` — successfully fetch and print the search page HTML. Confirm the form field names for city search.
- [ ] **Milestone 2:** Successfully fetch ONE known parcel detail page (use `avParcelId=513200002`) and print its full HTML. Verify basement fields are present and identify exact label text.
- [ ] **Milestone 3:** Run `parse_parcel_html()` on that one page and confirm all fields extract correctly. Adjust label matching code as needed.
- [ ] **Milestone 4:** Run `scrape_search_results()` for American Fork and confirm you get at least 100+ parcel IDs back. Log the count.
- [ ] **Milestone 5:** Run the main scraper with `TEST_MODE_LIMIT = 20` (just 20 parcels). Verify the CSV output has correct data.
- [ ] **Milestone 6:** Run with `TEST_MODE_LIMIT = 200` and validate lead quality — spot check 5 leads against the assessor website manually.
- [ ] **Full run:** Remove the test mode limit and run against all American Fork parcels.

---

## EXPECTED RESULTS FOR AMERICAN FORK

Based on market research, expect approximately:

- Total residential parcels in American Fork: ~12,000–18,000
- Parcels with basements: ~9,000–15,000 (roughly 80–85%)
- Parcels with fully unfinished basements: ~4,000–7,000
- Parcels with partially finished basements: ~2,000–4,000
- **Total qualifying leads (any unfinished space): ~6,000–11,000**

For the monthly pipeline, take the top 300–500 sorted by priority score.

---

## IMPORTANT NOTES FOR THE AGENT

1. **crawl4ai version matters.** Use `AsyncWebCrawler` (async API), not the older synchronous `WebCrawler`. The async version is the current standard.

2. **The portal may require session cookies.** If requests return blank pages, use crawl4ai's `session_id` parameter to maintain browser state across requests:
   ```python
   result = await crawler.arun(url=url, session_id="utah_county_session")
   ```

3. **JavaScript rendering.** The Utah County assessor portal likely renders content via server-side HTML (not JS), so crawl4ai's default behavior should work. However, if pages appear blank, add `wait_for="table"` or a specific CSS selector to wait for content.

4. **User agent.** crawl4ai uses a real Chromium browser, so the default user agent is realistic and should not be blocked. Do not override it with a fake one.

5. **Respect robots.txt.** Check `https://www.utahcounty.gov/robots.txt` before running. If the `/LandRecords/` path is disallowed, use the Open Data Portal alternative instead.

6. **Public records are legal to scrape.** This is government-published public data. There are no legal concerns with accessing and downloading it programmatically, as long as you do not overload the server (hence the rate limiting).

7. **The BeautifulSoup dependency.** crawl4ai includes BeautifulSoup internally, but import it explicitly from `bs4` in your parser to be safe: `from bs4 import BeautifulSoup`.

8. **Output verification.** After the test run completes, manually verify 5–10 leads by visiting their `detail_url` links and confirming the extracted data matches what's on screen.
