#!/usr/bin/env python3
"""
radar.py — Dubai Distress Radar
A single-file version of the complete pipeline.

Usage:
    python radar.py                    # Full run: fetch + detect + feed
    python radar.py --fetch            # Just fetch listings from Bayut
    python radar.py --detect          # Just detect price drops
    python radar.py --feed            # Just regenerate feed from existing data
    python radar.py --report          # Print summary report
    python radar.py --export csv      # Export drops to CSV
    python radar.py --export json     # Export drops to JSON
    python radar.py --html            # Generate HTML feed
    python radar.py --import-dld FILE # Import DLD transaction CSV
    python radar.py --benchmarks      # Recompute area benchmarks
    python radar.py --init             # Initialize the database
    python radar.py --test            # Test API connection
"""

import sqlite3
import requests
import csv
import json
import re
import time
import hashlib
import argparse
import os
from datetime import datetime, date
from collections import defaultdict

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────

FIRECRAWL_URL = "http://localhost:3002"
FIRECRAWL_TIMEOUT = 60
RAPIDAPI_KEY = "ae80ad0eadmsh7bd5c05c9317b15p18b5cdjsn09e37c1ca6f1"
RAPIDAPI_HOST = "bayut14.p.rapidapi.com"
BAYUT_BASE_URL = f"https://{RAPIDAPI_HOST}"

BAYUT_SEARCH_URLS = [
    "https://www.bayut.com/for-sale/apartments/dubai/business-bay/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/dubai-marina/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/downtown-dubai/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/jumeirah-village-circle-jvc/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/palm-jumeirah/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/dubai-hills-estate/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/jumeirah-lake-towers-jlt/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/arjan/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/sobha-hartland/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/dubai-creek-harbour/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/meydan-city/?sort=date_desc",
    "https://www.bayut.com/for-sale/apartments/dubai/al-furjan/?sort=date_desc",
    "https://www.bayut.com/for-sale/villas/dubai/dubai-hills-estate/?sort=date_desc",
    "https://www.bayut.com/for-sale/villas/dubai/arabian-ranches/?sort=date_desc",
    "https://www.bayut.com/for-sale/villas/dubai/palm-jumeirah/?sort=date_desc",
]

MAX_PAGES_PER_URL = 2
DB_PATH = "radar.db"

AREA_IDS = {
    "downtown-dubai": "6901",
    "business-bay": "6573",
    "dubai-marina": "5003",
    "palm-jumeirah": "5548",
    "jumeirah-village-circle": "8143",
    "dubai-hills-estate": "12695",
    "jumeirah-lake-towers": "5006",
    "arjan": "8472",
    "dubai-creek-harbour": "12700",
    "motor-city": "5236",
    "sobha-hartland": "12096",
    "al-furjan": "8329",
    "dubai-sports-city": "5200",
    "meydan-city": "7918",
    "dubai-production-city": "5305",
    "dubai-silicon-oasis": "5178",
    "jumeirah-village-triangle": "8144",
    "town-square": "14611",
    "arabian-ranches": "5095",
    "damac-hills": "10822",
}

MIN_DROP_PCT = 2.0
MIN_DROP_AED = 10000
CAPITULATION_THRESHOLD = 15.0
MULTI_CUT_WINDOW_DAYS = 90
STALE_LISTING_DAYS = 120
API_DELAY_SECONDS = 1.5
MAX_API_CALLS_PER_RUN = 100
PAGES_PER_AREA = 3

AREA_ALIASES = {
    "business bay": "BUSINESS BAY",
    "dubai marina": "DUBAI MARINA",
    "downtown dubai": "BURJ KHALIFA",
    "jumeirah village circle": "JUMEIRAH VILLAGE CIRCLE",
    "jvc": "JUMEIRAH VILLAGE CIRCLE",
    "palm jumeirah": "PALM JUMEIRAH",
    "dubai hills estate": "DUBAI HILLS",
    "dubai hills": "DUBAI HILLS",
    "jumeirah lake towers": "JUMEIRAH LAKES TOWERS",
    "jlt": "JUMEIRAH LAKES TOWERS",
    "arjan": "ARJAN",
    "sobha hartland": "SOBHA HEARTLAND",
    "sobha heartland": "SOBHA HEARTLAND",
    "dubai creek harbour": "DUBAI CREEK HARBOUR",
    "meydan city": "MEYDAN ONE",
    "motor city": "MOTOR CITY",
    "al furjan": "AL FURJAN",
    "city walk": "CITY WALK",
    "arabian ranches": "ARABIAN RANCHES",
    "dubai sports city": "DUBAI SPORTS CITY",
    "dubai production city": "DUBAI PRODUCTION CITY",
    "dubai land residence complex": "DUBAI LAND RESIDENCE COMPLEX",
    "madinat al mataar": "Madinat Al Mataar",
    "majan": "MAJAN",
    "dubai studio city": "DUBAI STUDIO CITY",
    "the greens": "THE GREENS",
    "dubai investment park": "DUBAI INVESTMENT PARK FIRST",
    "jumeirah beach residence": "JUMEIRAH BEACH RESIDENCE",
    "jbr": "JUMEIRAH BEACH RESIDENCE",
    "dubai silicon oasis": "DUBAI SILICON OASIS",
    "jumeirah village triangle": "JUMEIRAH VILLAGE TRIANGLE",
    "town square": "TOWN SQUARE",
    "damac hills": "DAMAC HILLS",
    "remraam": "REMRAAM",
    "international city": "INTERNATIONAL CITY",
    "dubai south": "DUBAI SOUTH",
    "dubai harbour": "DUBAI HARBOUR",
}

# ─────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Initialize database from schema if it doesn't exist."""
    if not os.path.exists(DB_PATH):
        print("Initializing database...")
        conn = sqlite3.connect(DB_PATH)
        with open("schema.sql", "r") as f:
            conn.executescript(f.read())
        conn.close()
        print(f"   Database created: {DB_PATH}")
        return True
    return False


# ─────────────────────────────────────────────────────────────────
# BAYUT API
# ─────────────────────────────────────────────────────────────────

API_HEADERS = {
    "Content-Type": "application/json",
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}


def api_call(endpoint, params=None, call_counter=None):
    """Make a rate-limited call to the Bayut14 API."""
    if call_counter and call_counter[0] >= MAX_API_CALLS_PER_RUN:
        print(f"  API call limit ({MAX_API_CALLS_PER_RUN}) reached")
        return None

    url = f"{BAYUT_BASE_URL}/{endpoint}"

    try:
        resp = requests.get(url, headers=API_HEADERS, params=params, timeout=30)
        if call_counter:
            call_counter[0] += 1

        if resp.status_code == 429:
            print("  Rate limited — waiting 60s")
            time.sleep(60)
            return api_call(endpoint, params, call_counter)

        if resp.status_code == 403:
            print("  403 Forbidden — check your API key and subscription")
            return None

        if resp.status_code != 200:
            print(f"  API returned {resp.status_code}: {resp.text[:200]}")
            return None

        time.sleep(API_DELAY_SECONDS)
        return resp.json()

    except requests.exceptions.Timeout:
        print("  Request timed out")
        return None
    except Exception as e:
        print(f"  API error: {e}")
        return None


def fetch_properties(location_id, purpose="for-sale", prop_type="apartments", page=1, call_counter=None):
    """Fetch property listings from Bayut14 search-property endpoint."""
    params = {
        "location_ids": location_id,
        "purpose": purpose,
        "property_type": prop_type,
        "page": page,
        "langs": "en",
        "sort_order": "latest",
    }
    data = api_call("search-property", params, call_counter)
    if not data or not data.get("success"):
        return [], 0
    properties = data.get("data", {}).get("properties", [])
    total = data.get("data", {}).get("total", 0)
    return properties, total


def parse_property(raw):
    """Extract structured data from a Bayut14 API property response."""
    locations = raw.get("location", [])
    area_name = ""
    building_name = ""
    location_parts = []
    for loc in locations:
        name = loc.get("name", "")
        location_parts.append(name)
        level = loc.get("level", -1)
        if level == 2:
            area_name = name
        elif level == 3:
            if not area_name:
                area_name = name
        if loc.get("type") == "condo-building":
            building_name = name

    categories = raw.get("category", [])
    prop_type = "apartment"
    for cat in categories:
        name_lower = (cat.get("nameSingular") or cat.get("name") or "").lower()
        if "villa" in name_lower:
            prop_type = "villa"
        elif "penthouse" in name_lower:
            prop_type = "penthouse"
        elif "townhouse" in name_lower:
            prop_type = "townhouse"
        elif "apartment" in name_lower:
            prop_type = "apartment"

    area_sqm = raw.get("area") or 0
    area_sqft = round(area_sqm * 10.7639, 2) if area_sqm else None

    slug = raw.get("slug", {})
    slug_en = slug.get("en", "") if isinstance(slug, dict) else str(slug)
    url = f"https://www.bayut.com/property/details-{raw.get('externalID', '')}.html"

    cover = raw.get("coverPhoto", {}) or {}
    photo_id = cover.get("externalID", cover.get("id", ""))
    photo_url = f"https://bayut-production.s3.eu-central-1.amazonaws.com/image/{photo_id}" if photo_id else ""

    title = raw.get("title", {})
    title_en = title.get("en", "") if isinstance(title, dict) else str(title)

    return {
        "listing_id": f"bayut_{raw.get('externalID', raw.get('id', ''))}",
        "source": "bayut",
        "title": title_en,
        "purpose": raw.get("purpose", "for-sale"),
        "category": "residential",
        "property_type": prop_type,
        "bedrooms": raw.get("rooms"),
        "bathrooms": raw.get("baths"),
        "area_sqft": area_sqft,
        "area_sqm": round(area_sqm, 2) if area_sqm else None,
        "location_name": " > ".join(location_parts),
        "area_name": area_name,
        "building_name": building_name,
        "latitude": None,
        "longitude": None,
        "furnishing": raw.get("furnishingStatus"),
        "completion": raw.get("completionStatus"),
        "agent_name": "",
        "agency_name": "",
        "permit_number": raw.get("permitNumber", ""),
        "url": url,
        "photo_url": photo_url,
        "price": raw.get("price", 0),
    }


def upsert_listing(conn, listing):
    """Insert or update a listing, record price snapshot. Returns (is_new, prev_price, price)."""
    now = datetime.now().isoformat(timespec="seconds")
    today = date.today().isoformat()
    lid = listing["listing_id"]
    price = listing["price"]

    existing = conn.execute("SELECT listing_id FROM listings WHERE listing_id = ?", (lid,)).fetchone()

    if existing:
        conn.execute("""
            UPDATE listings SET title=?, area_sqft=?, area_sqm=?, area_name=?,
                building_name=?, agent_name=?, agency_name=?, photo_url=?,
                last_seen=?, is_active=1
            WHERE listing_id=?
        """, (listing["title"], listing["area_sqft"], listing["area_sqm"],
              listing["area_name"], listing["building_name"], listing["agent_name"],
              listing["agency_name"], listing["photo_url"], now, lid))
        is_new = False
    else:
        conn.execute("""
            INSERT INTO listings (listing_id, source, title, purpose, category,
                property_type, bedrooms, bathrooms, area_sqft, area_sqm,
                location_name, area_name, building_name, latitude, longitude,
                furnishing, completion, agent_name, agency_name, permit_number,
                url, photo_url, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (lid, listing["source"], listing["title"], listing["purpose"],
              listing["category"], listing["property_type"], listing["bedrooms"],
              listing["bathrooms"], listing["area_sqft"], listing["area_sqm"],
              listing["location_name"], listing["area_name"], listing["building_name"],
              listing["latitude"], listing["longitude"], listing["furnishing"],
              listing["completion"], listing["agent_name"], listing["agency_name"],
              listing["permit_number"], listing["url"], listing["photo_url"], now, now))
        is_new = True

    existing_price = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) = ?
    """, (lid, today)).fetchone()

    if not existing_price:
        conn.execute("INSERT INTO price_history (listing_id, price, recorded_at) VALUES (?,?,?)",
                     (lid, price, now))

    prev = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) < ?
        ORDER BY recorded_at DESC LIMIT 1
    """, (lid, today)).fetchone()

    return is_new, prev["price"] if prev else None, price


def run_fetch(areas=None):
    """Main fetch routine."""
    init_db()
    conn = get_db()
    start = time.time()
    call_counter = [0]
    stats = {"fetched": 0, "new": 0, "price_changes": 0}

    target_areas = areas if areas else list(AREA_IDS.keys())

    print(f"Dubai Distress Radar — Fetching {len(target_areas)} areas")
    print(f"   API: bayut14.p.rapidapi.com")
    print(f"   Budget: {MAX_API_CALLS_PER_RUN} calls | Delay: {API_DELAY_SECONDS}s\n")

    for area_slug in target_areas:
        loc_id = AREA_IDS.get(area_slug)
        if not loc_id:
            print(f"  Unknown area: {area_slug} — skipping")
            continue

        for prop_type in ["apartments", "villas"]:
            for page in range(1, PAGES_PER_AREA + 1):
                print(f"  {area_slug} | {prop_type} | page {page}", end="")

                properties, total = fetch_properties(loc_id, "for-sale", prop_type, page, call_counter)
                if not properties:
                    print(" — no results")
                    break

                page_new = 0
                page_changes = 0
                for raw in properties:
                    parsed = parse_property(raw)
                    if not parsed["listing_id"] or not parsed["price"]:
                        continue
                    is_new, prev_price, curr_price = upsert_listing(conn, parsed)
                    stats["fetched"] += 1
                    if is_new:
                        stats["new"] += 1
                        page_new += 1
                    if prev_price and prev_price != curr_price:
                        stats["price_changes"] += 1
                        page_changes += 1

                print(f" — {len(properties)} listings ({page_new} new, {page_changes} changes)")

                if call_counter[0] >= MAX_API_CALLS_PER_RUN:
                    break
            if call_counter[0] >= MAX_API_CALLS_PER_RUN:
                break
        if call_counter[0] >= MAX_API_CALLS_PER_RUN:
            print(f"\nAPI call budget exhausted ({call_counter[0]} calls)")
            break

    conn.execute(f"""
        UPDATE listings SET is_active = 0
        WHERE is_active = 1 AND julianday('now') - julianday(last_seen) > {STALE_LISTING_DAYS}
    """)

    duration = round(time.time() - start, 1)
    conn.execute("""
        INSERT INTO run_log (listings_fetched, new_listings, price_changes, api_calls_used, duration_sec)
        VALUES (?, ?, ?, ?, ?)
    """, (stats["fetched"], stats["new"], stats["price_changes"], call_counter[0], duration))

    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"Done in {duration}s | {call_counter[0]} API calls")
    print(f"   {stats['fetched']} listings | {stats['new']} new | {stats['price_changes']} price changes")


# ─────────────────────────────────────────────────────────────────
# FIRECRAWL / LISTING CRAWLER
# ─────────────────────────────────────────────────────────────────

def firecrawl_health():
    """Check if Firecrawl is running."""
    try:
        r = requests.get(f"{FIRECRAWL_URL}/v1/health", timeout=5)
        return r.status_code == 200
    except:
        return False


def firecrawl_scrape(url, formats=None, extract_schema=None):
    """Scrape a single URL via Firecrawl."""
    payload = {"url": url, "formats": formats or ["markdown", "html"]}

    if extract_schema:
        payload["formats"] = ["extract"]
        payload["extract"] = {"schema": extract_schema}

    try:
        r = requests.post(
            f"{FIRECRAWL_URL}/v1/scrape",
            json=payload,
            timeout=FIRECRAWL_TIMEOUT,
        )
        if r.status_code == 200:
            return r.json().get("data", {})
        else:
            print(f"  Firecrawl returned {r.status_code}: {r.text[:200]}")
            return None
    except requests.exceptions.Timeout:
        print(f"  Firecrawl timeout for {url}")
        return None
    except Exception as e:
        print(f"  Firecrawl error: {e}")
        return None


def parse_listings_from_markdown(markdown_text, source_url=""):
    """Parse property listings from Firecrawl markdown output."""
    listings = []
    if not markdown_text:
        return listings

    blocks = re.split(r'\n{2,}', markdown_text)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        price_match = re.search(r'AED\s*([\d,]+(?:\.\d+)?)', block, re.IGNORECASE)
        if not price_match:
            price_match = re.search(r'([\d,]+)\s*AED', block, re.IGNORECASE)

        bed_match = re.search(r'(\d+)\s*(?:Bed(?:room)?s?|BR|B/R)', block, re.IGNORECASE)
        studio_match = re.search(r'\bStudio\b', block, re.IGNORECASE)
        bath_match = re.search(r'(\d+)\s*(?:Bath(?:room)?s?)', block, re.IGNORECASE)
        sqft_match = re.search(r'([\d,]+(?:\.\d+)?)\s*(?:sq\.?\s*ft|sqft|sq ft)', block, re.IGNORECASE)

        link_match = re.findall(r'\[([^\]]*)\]\((https?://[^\)]+)\)', block)
        bayut_link = None
        for text, url in link_match:
            if 'bayut.com' in url and ('/property/' in url or '/for-sale/' in url or '/for-rent/' in url):
                if re.search(r'\d{5,}', url):
                    bayut_link = url
                    break

        prop_type = "apartment"
        if re.search(r'\bvilla\b', block, re.IGNORECASE):
            prop_type = "villa"
        elif re.search(r'\bpenthouse\b', block, re.IGNORECASE):
            prop_type = "penthouse"
        elif re.search(r'\btownhouse\b', block, re.IGNORECASE):
            prop_type = "townhouse"
        elif re.search(r'\bstudio\b', block, re.IGNORECASE):
            prop_type = "apartment"

        if price_match and (bed_match or studio_match or sqft_match):
            price_str = price_match.group(1).replace(',', '')
            price = float(price_str) if price_str else 0

            bedrooms = 0
            if studio_match and not bed_match:
                bedrooms = 0
            elif bed_match:
                bedrooms = int(bed_match.group(1))

            bathrooms = int(bath_match.group(1)) if bath_match else None
            sqft = float(sqft_match.group(1).replace(',', '')) if sqft_match else None

            area_name = extract_area_from_url(source_url)
            listing_id = generate_listing_id(bayut_link, block, price, bedrooms, sqft)

            title = block.split('\n')[0][:200].strip('#*[] ')
            if link_match:
                title = link_match[0][0][:200] if link_match[0][0] else title

            if price > 50000:
                listings.append({
                    "listing_id": listing_id,
                    "source": "bayut",
                    "title": title,
                    "purpose": "for-sale",
                    "property_type": prop_type,
                    "bedrooms": bedrooms,
                    "bathrooms": bathrooms,
                    "area_sqft": sqft,
                    "area_sqm": round(sqft * 0.092903, 2) if sqft else None,
                    "area_name": area_name,
                    "url": bayut_link or source_url,
                    "price": price,
                })

    return listings


def parse_listings_from_html(html_text, source_url=""):
    """Fallback parser: extract from raw HTML returned by Firecrawl."""
    listings = []
    if not html_text:
        return listings

    jsonld_matches = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html_text, re.DOTALL)
    for match in jsonld_matches:
        try:
            data = json.loads(match)
            items = data if isinstance(data, list) else [data]
            for item in items:
                if item.get("@type") in ("Product", "Offer", "RealEstateListing", "Residence"):
                    price = None
                    if "offers" in item:
                        offers = item["offers"] if isinstance(item["offers"], list) else [item["offers"]]
                        for o in offers:
                            if "price" in o:
                                price = float(str(o["price"]).replace(",", ""))
                                break
                    if price and price > 50000:
                        listings.append({
                            "listing_id": hashlib.md5(json.dumps(item, sort_keys=True).encode()).hexdigest()[:16],
                            "source": "bayut",
                            "title": item.get("name", "")[:200],
                            "purpose": "for-sale",
                            "property_type": "apartment",
                            "bedrooms": None,
                            "bathrooms": None,
                            "area_sqft": None,
                            "area_sqm": None,
                            "area_name": extract_area_from_url(source_url),
                            "url": item.get("url", source_url),
                            "price": price,
                        })
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    return listings


def extract_area_from_url(url):
    """Extract area name from a Bayut/listing URL."""
    match = re.search(r'/dubai/([^/?#]+)', url)
    if match:
        slug = match.group(1).rstrip('/')
        return slug.replace('-', ' ').title().replace('Jvc', 'JVC').replace('Jlt', 'JLT')
    return ""


def generate_listing_id(url, text, price, beds, sqft):
    """Generate a stable ID for a listing."""
    if url:
        num_match = re.search(r'(\d{6,})', url)
        if num_match:
            return f"bayut_{num_match.group(1)}"
    content = f"{price}_{beds}_{sqft}_{text[:100]}"
    return f"crawl_{hashlib.md5(content.encode()).hexdigest()[:12]}"


def store_listing(conn, listing):
    """Store a crawled listing and record its price. Returns (is_new, prev_price, price)."""
    now = datetime.now().isoformat(timespec="seconds")
    today = date.today().isoformat()
    lid = listing["listing_id"]
    price = listing["price"]

    existing = conn.execute("SELECT listing_id FROM listings WHERE listing_id = ?", (lid,)).fetchone()

    if existing:
        conn.execute("""
            UPDATE listings SET title=?, area_sqft=?, area_sqm=?, area_name=?,
                last_seen=?, is_active=1
            WHERE listing_id=?
        """, (listing["title"], listing["area_sqft"], listing["area_sqm"],
              listing["area_name"], now, lid))
        is_new = False
    else:
        conn.execute("""
            INSERT INTO listings (listing_id, source, title, purpose, category,
                property_type, bedrooms, bathrooms, area_sqft, area_sqm,
                location_name, area_name, building_name, url, first_seen, last_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (lid, listing["source"], listing["title"], listing["purpose"],
              "residential", listing["property_type"], listing["bedrooms"],
              listing["bathrooms"], listing["area_sqft"], listing["area_sqm"],
              "", listing["area_name"], "", listing["url"], now, now))
        is_new = True

    existing_today = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) = ?
    """, (lid, today)).fetchone()

    if not existing_today:
        conn.execute("INSERT INTO price_history (listing_id, price, recorded_at) VALUES (?,?,?)",
                     (lid, price, now))
    elif existing_today["price"] != price:
        conn.execute("""
            UPDATE price_history SET price = ?, recorded_at = ?
            WHERE listing_id = ? AND DATE(recorded_at) = ?
        """, (price, now, lid, today))

    prev = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) < ?
        ORDER BY recorded_at DESC LIMIT 1
    """, (lid, today)).fetchone()

    return is_new, prev["price"] if prev else None, price


def crawl_url(url, conn, stats):
    """Crawl a single search page URL and store listings."""
    print(f"\n  {url}")

    data = firecrawl_scrape(url, formats=["markdown"])
    if not data:
        print("     No data returned")
        return

    markdown = data.get("markdown", "")
    html = data.get("html", "")

    listings = parse_listings_from_markdown(markdown, url)

    if not listings and html:
        listings = parse_listings_from_html(html, url)

    if not listings:
        print(f"     No listings parsed (got {len(markdown)} chars of markdown)")
        debug_file = f"debug_crawl_{hashlib.md5(url.encode()).hexdigest()[:8]}.md"
        with open(debug_file, "w") as f:
            f.write(f"# Source: {url}\n\n{markdown[:5000]}")
        print(f"     Saved debug output to {debug_file}")
        return

    new_count = 0
    change_count = 0
    for listing in listings:
        is_new, prev_price, curr_price = store_listing(conn, listing)
        if is_new:
            new_count += 1
        if prev_price and prev_price != curr_price:
            change_count += 1

    stats["pages"] += 1
    stats["listings"] += len(listings)
    stats["new"] += new_count
    stats["price_changes"] += change_count

    print(f"     {len(listings)} listings ({new_count} new, {change_count} price changes)")


def crawl_all(urls=None):
    """Crawl all configured URLs or a provided list."""
    init_db()

    if not firecrawl_health():
        print(f"Firecrawl is not running at {FIRECRAWL_URL}")
        print("  Start it with: cd firecrawl && docker compose up -d")
        return None

    conn = get_db()
    target_urls = urls or BAYUT_SEARCH_URLS
    stats = {"pages": 0, "listings": 0, "new": 0, "price_changes": 0}

    print(f"Crawling {len(target_urls)} search pages via Firecrawl")
    print(f"   Firecrawl: {FIRECRAWL_URL}")

    for url in target_urls:
        crawl_url(url, conn, stats)

        for page in range(2, MAX_PAGES_PER_URL + 1):
            sep = "&" if "?" in url else "?"
            page_url = f"{url}{sep}page={page}"
            crawl_url(page_url, conn, stats)

    conn.commit()
    conn.close()
    return stats


# ─────────────────────────────────────────────────────────────────
# DLD DATA IMPORT
# ─────────────────────────────────────────────────────────────────

def import_dld_csv(csv_path):
    """Import DLD transaction CSV (supports both old and new column names)."""
    init_db()
    conn = get_db()
    count = 0
    skipped = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            txn_id = row.get("TRANSACTION_NUMBER", row.get("Transaction Number", "")).strip()
            if not txn_id:
                skipped += 1
                continue

            amount = float(row.get("TRANS_VALUE", row.get("Amount", "0")).replace(",", "") or 0)
            area_sqm = float(row.get("ACTUAL_AREA", row.get("Property Size (sq.m)", "0")).replace(",", "") or 0)
            price_sqm = amount / area_sqm if area_sqm > 0 else 0

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO transactions
                        (txn_id, txn_date, txn_type, txn_subtype, registration, freehold,
                         usage, area_name, property_type, property_subtype, amount,
                         actual_area_sqm, rooms, project, price_per_sqm)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    txn_id,
                    row.get("INSTANCE_DATE", row.get("Transaction Date", "")),
                    row.get("GROUP_EN", row.get("Transaction Type", "")),
                    row.get("PROCEDURE_EN", row.get("Transaction sub type", "")),
                    row.get("IS_OFFPLAN_EN", row.get("Registration type", "")),
                    row.get("IS_FREE_HOLD_EN", row.get("Is Free Hold?", "")),
                    row.get("USAGE_EN", row.get("Usage", "")),
                    row.get("AREA_EN", row.get("Area", "")),
                    row.get("PROP_TYPE_EN", row.get("Property Type", "")),
                    row.get("PROP_SB_TYPE_EN", row.get("Property Sub Type", "")),
                    amount, area_sqm,
                    row.get("ROOMS_EN", row.get("Room(s)", "")),
                    row.get("PROJECT_EN", row.get("Project", "")),
                    price_sqm
                ))
                count += 1
            except Exception as e:
                skipped += 1

    conn.commit()
    conn.close()
    print(f"Imported {count} transactions from {csv_path} ({skipped} skipped)")


# ─────────────────────────────────────────────────────────────────
# DROP DETECTION
# ─────────────────────────────────────────────────────────────────

def compute_area_benchmarks():
    """Compute area-level price benchmarks from DLD transactions and listings."""
    init_db()
    conn = get_db()
    today = date.today().isoformat()

    txn_areas = conn.execute("""
        SELECT area_name,
               COUNT(*) AS cnt,
               AVG(price_per_sqm) AS mean_psqm
        FROM transactions
        WHERE price_per_sqm > 500 AND price_per_sqm < 200000
          AND actual_area_sqm > 10
        GROUP BY area_name
        HAVING cnt >= 10
    """).fetchall()

    for row in txn_areas:
        area = row["area_name"]
        prices = [r["price_per_sqm"] for r in conn.execute("""
            SELECT price_per_sqm FROM transactions
            WHERE area_name = ? AND price_per_sqm > 500 AND price_per_sqm < 200000
            ORDER BY price_per_sqm
        """, (area,)).fetchall()]

        if len(prices) < 10:
            continue

        n = len(prices)
        median_psqm = prices[n // 2]
        q25_psqm = prices[n // 4]
        q10_psqm = prices[n // 10]

        median_txn = None
        txn_prices = [r[0] for r in conn.execute(
            "SELECT amount FROM transactions WHERE area_name = ? AND amount > 0 ORDER BY amount",
            (area,)).fetchall()]
        if txn_prices:
            median_txn = txn_prices[len(txn_prices) // 2]

        listing_prices = [r[0] for r in conn.execute("""
            SELECT ph.price FROM price_history ph
            JOIN listings l ON ph.listing_id = l.listing_id
            WHERE l.area_name = ? AND l.is_active = 1
            AND ph.recorded_at = (SELECT MAX(recorded_at) FROM price_history WHERE listing_id = ph.listing_id)
            ORDER BY ph.price
        """, (area,)).fetchall()]
        median_listing = listing_prices[len(listing_prices) // 2] if listing_prices else None

        gap_pct = None
        if median_listing and median_txn and median_txn > 0:
            gap_pct = round((median_listing - median_txn) / median_txn * 100, 1)

        conn.execute("""
            INSERT OR REPLACE INTO area_benchmarks
                (area_name, computed_at, txn_count, median_price_sqm, mean_price_sqm,
                 q25_price_sqm, q10_price_sqm, median_listing_price, median_txn_price,
                 asking_vs_sold_gap_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (area, today, len(prices), median_psqm, row["mean_psqm"],
              q25_psqm, q10_psqm, median_listing, median_txn, gap_pct))

    conn.commit()
    count = len(txn_areas)
    conn.close()
    print(f"Computed benchmarks for {count} areas")
    return count


def detect_drops():
    """Scan all listings for price drops and classify them."""
    init_db()
    conn = get_db()
    today = date.today().isoformat()
    new_drops = 0

    listings = conn.execute("""
        SELECT DISTINCT ph.listing_id
        FROM price_history ph
        JOIN listings l ON ph.listing_id = l.listing_id
        WHERE l.is_active = 1
        GROUP BY ph.listing_id
        HAVING COUNT(*) >= 2
    """).fetchall()

    for row in listings:
        lid = row["listing_id"]

        prices = conn.execute("""
            SELECT price, recorded_at FROM price_history
            WHERE listing_id = ? ORDER BY recorded_at ASC
        """, (lid,)).fetchall()

        if len(prices) < 2:
            continue

        latest = prices[-1]
        previous = prices[-2]

        if latest["price"] >= previous["price"]:
            continue

        drop_amount = previous["price"] - latest["price"]
        drop_pct = (drop_amount / previous["price"]) * 100

        if drop_pct < MIN_DROP_PCT or drop_amount < MIN_DROP_AED:
            continue

        existing = conn.execute("""
            SELECT id FROM drops
            WHERE listing_id = ? AND new_price = ? AND previous_price = ?
        """, (lid, latest["price"], previous["price"])).fetchone()

        if existing:
            continue

        prev_drops = conn.execute(
            "SELECT COUNT(*) AS cnt FROM drops WHERE listing_id = ?", (lid,)
        ).fetchone()["cnt"]

        drop_number = prev_drops + 1

        listing_info = conn.execute(
            "SELECT first_seen, area_name FROM listings WHERE listing_id = ?", (lid,)
        ).fetchone()

        days_listed = None
        if listing_info and listing_info["first_seen"]:
            try:
                first = datetime.fromisoformat(listing_info["first_seen"])
                days_listed = (datetime.now() - first).days
            except:
                pass

        days_since_last = None
        if prev_drops > 0:
            last_drop = conn.execute("""
                SELECT detected_at FROM drops WHERE listing_id = ?
                ORDER BY detected_at DESC LIMIT 1
            """, (lid,)).fetchone()
            if last_drop:
                try:
                    last_dt = datetime.fromisoformat(last_drop["detected_at"])
                    days_since_last = (datetime.now() - last_dt).days
                except:
                    pass

        drop_type = classify_drop(
            conn, lid, drop_pct, drop_number, days_listed,
            days_since_last, latest["price"], listing_info
        )

        conn.execute("""
            INSERT INTO drops (listing_id, previous_price, new_price, drop_amount,
                drop_pct, drop_number, drop_type, days_since_listed, days_since_last_drop)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (lid, previous["price"], latest["price"], drop_amount,
              round(drop_pct, 2), drop_number, drop_type, days_listed, days_since_last))

        new_drops += 1

    conn.commit()
    update_area_stats(conn)
    conn.commit()
    conn.close()

    print(f"Detected {new_drops} new price drops")
    return new_drops


def classify_drop(conn, listing_id, drop_pct, drop_number, days_listed,
                  days_since_last, new_price, listing_info):
    """Classify a drop into one of three types."""
    area_name = listing_info["area_name"] if listing_info else ""

    if area_name:
        bench = conn.execute("""
            SELECT median_txn_price FROM area_benchmarks
            WHERE area_name = ? ORDER BY computed_at DESC LIMIT 1
        """, (area_name,)).fetchone()

        if bench and bench["median_txn_price"] and new_price < bench["median_txn_price"]:
            return "capitulation"

    if drop_number >= 2:
        if days_since_last is not None and days_since_last <= MULTI_CUT_WINDOW_DAYS:
            return "multi_cut"
        return "multi_cut"

    if drop_pct >= CAPITULATION_THRESHOLD:
        return "capitulation"

    return "mania_normal"


def update_area_stats(conn):
    """Compute daily area-level drop statistics."""
    today = date.today().isoformat()

    areas = conn.execute("""
        SELECT DISTINCT l.area_name
        FROM drops d JOIN listings l ON d.listing_id = l.listing_id
        WHERE DATE(d.detected_at) = ?
    """, (today,)).fetchall()

    for row in areas:
        area = row["area_name"]
        if not area:
            continue

        stats = conn.execute("""
            SELECT
                COUNT(*) AS drops_today,
                AVG(d.drop_pct) AS avg_drop_pct,
                SUM(d.drop_amount) AS total_drop_aed
            FROM drops d
            JOIN listings l ON d.listing_id = l.listing_id
            WHERE l.area_name = ? AND DATE(d.detected_at) = ?
        """, (area, today)).fetchone()

        active = conn.execute(
            "SELECT COUNT(*) AS cnt FROM listings WHERE area_name = ? AND is_active = 1",
            (area,)
        ).fetchone()["cnt"]

        multi = conn.execute("""
            SELECT COUNT(DISTINCT d.listing_id) AS cnt
            FROM drops d JOIN listings l ON d.listing_id = l.listing_id
            WHERE l.area_name = ? AND d.drop_type = 'multi_cut' AND DATE(d.detected_at) = ?
        """, (area, today)).fetchone()["cnt"]

        capit = conn.execute("""
            SELECT COUNT(*) AS cnt
            FROM drops d JOIN listings l ON d.listing_id = l.listing_id
            WHERE l.area_name = ? AND d.drop_type = 'capitulation' AND DATE(d.detected_at) = ?
        """, (area, today)).fetchone()["cnt"]

        conn.execute("""
            INSERT OR REPLACE INTO area_drop_stats
                (area_name, stat_date, active_listings, drops_today, avg_drop_pct,
                 total_drop_aed, multi_cut_count, capitulation_count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (area, today, active, stats["drops_today"], stats["avg_drop_pct"],
              stats["total_drop_aed"], multi, capit))


# ─────────────────────────────────────────────────────────────────
# DISTRESS MATCHER / FEED
# ─────────────────────────────────────────────────────────────────

def normalize_area(area_name):
    """Map a listing's area name to the DLD benchmark area name."""
    if not area_name:
        return None
    key = area_name.strip().lower()
    if key in AREA_ALIASES:
        return AREA_ALIASES[key]
    upper = area_name.strip().upper()
    return upper


def load_benchmarks(conn):
    """Load the latest area benchmarks from DLD data."""
    rows = conn.execute("""
        SELECT area_name, median_price_sqm, q25_price_sqm, q10_price_sqm,
               median_txn_price, txn_count
        FROM area_benchmarks
        ORDER BY computed_at DESC
    """).fetchall()

    benchmarks = {}
    for r in rows:
        area = r["area_name"]
        if area not in benchmarks:
            benchmarks[area] = dict(r)
    return benchmarks


def score_listing(listing, benchmark, drops):
    """Score a listing for distress signals."""
    signals = []
    score = 0
    discount_pct = None

    price = listing["current_price"]
    sqft = listing["area_sqft"]
    sqm = listing["area_sqm"]

    if benchmark and sqm and sqm > 10 and price > 0:
        listing_psqm = price / sqm
        median_psqm = benchmark["median_price_sqm"]

        if median_psqm > 0:
            discount_pct = round((median_psqm - listing_psqm) / median_psqm * 100, 1)

            if discount_pct >= 30:
                score += 4
                signals.append(f"Price/sqm {discount_pct:.0f}% below area median sold price")
            elif discount_pct >= 20:
                score += 3
                signals.append(f"Price/sqm {discount_pct:.0f}% below area median sold price")
            elif discount_pct >= 10:
                score += 2
                signals.append(f"Price/sqm {discount_pct:.0f}% below area median sold price")
            elif discount_pct >= 5:
                score += 1
                signals.append(f"Price/sqm {discount_pct:.0f}% below area median")

    if benchmark and benchmark.get("median_txn_price"):
        median_txn = benchmark["median_txn_price"]
        if price < median_txn * 0.8:
            score += 2
            signals.append(f"Listed at {price:,.0f} vs area median sold {median_txn:,.0f}")

    total_drops = len(drops)
    if total_drops >= 3:
        score += 3
        signals.append(f"{total_drops} price drops — maximum urgency")
    elif total_drops >= 2:
        score += 2
        signals.append(f"{total_drops} price drops — multi-cut pattern")
    elif total_drops >= 1:
        score += 1
        total_drop_pct = sum(d["drop_pct"] for d in drops)
        signals.append(f"Price dropped {total_drop_pct:.1f}% from original")

    if listing.get("days_on_market") and listing["days_on_market"] > 90:
        score += 1
        signals.append(f"Listed for {listing['days_on_market']} days")

    if score >= 6:
        level = "High Distress"
    elif score >= 4:
        level = "Likely Distress"
    elif score >= 2:
        level = "Watch"
    else:
        level = "Normal"

    return {
        "distress_score": min(score, 10),
        "distress_level": level,
        "signals": signals,
        "discount_pct": discount_pct,
    }


def run_matching():
    """Match all active listings against DLD benchmarks."""
    init_db()
    conn = get_db()
    benchmarks = load_benchmarks(conn)

    if not benchmarks:
        print("No benchmarks loaded. Run: python radar.py --import-dld <csv> --benchmarks")
        return []

    print(f"Loaded benchmarks for {len(benchmarks)} areas")

    listings = conn.execute("""
        SELECT l.*,
            (SELECT price FROM price_history WHERE listing_id = l.listing_id
             ORDER BY recorded_at DESC LIMIT 1) AS current_price,
            CAST(julianday('now') - julianday(l.first_seen) AS INTEGER) AS days_on_market
        FROM listings l
        WHERE l.is_active = 1 AND l.purpose = 'for-sale'
    """).fetchall()

    print(f"Analyzing {len(listings)} active listings")

    feed = []
    for row in listings:
        listing = dict(row)
        area_name = normalize_area(listing.get("area_name", ""))
        benchmark = benchmarks.get(area_name)
        price = listing.get("current_price")

        if not price or price <= 0:
            continue

        drops = [dict(d) for d in conn.execute("""
            SELECT drop_pct, drop_amount, drop_type, detected_at
            FROM drops WHERE listing_id = ? ORDER BY detected_at ASC
        """, (listing["listing_id"],)).fetchall()]

        result = score_listing(listing, benchmark, drops)

        if result["distress_score"] >= 2:
            feed.append({
                "listing_id": listing["listing_id"],
                "title": listing.get("title", ""),
                "area_name": listing.get("area_name", ""),
                "dld_area": area_name,
                "property_type": listing.get("property_type", ""),
                "bedrooms": listing.get("bedrooms"),
                "bathrooms": listing.get("bathrooms"),
                "area_sqft": listing.get("area_sqft"),
                "area_sqm": listing.get("area_sqm"),
                "price": price,
                "url": listing.get("url", ""),
                "days_on_market": listing.get("days_on_market"),
                "first_seen": listing.get("first_seen"),
                "drop_count": len(drops),
                "cumulative_drop_pct": round(sum(d["drop_pct"] for d in drops), 1) if drops else 0,
                "benchmark_median_psqm": benchmark["median_price_sqm"] if benchmark else None,
                "benchmark_median_sold": benchmark.get("median_txn_price") if benchmark else None,
                "benchmark_txn_count": benchmark.get("txn_count") if benchmark else None,
                **result,
            })

    feed.sort(key=lambda x: (x["distress_score"], x.get("discount_pct") or 0), reverse=True)

    conn.close()
    return feed


def print_feed(feed, limit=50):
    """Print the distress feed to terminal."""
    icons = {"High Distress": "🔴", "Likely Distress": "🟠", "Watch": "🟡", "Normal": "⚪"}

    print("\n" + "=" * 70)
    print("  LIVE DISTRESS FEED — Dubai Real Estate")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)

    counts = defaultdict(int)
    for item in feed:
        counts[item["distress_level"]] += 1
    print(f"\n  {icons['High Distress']} High Distress: {counts['High Distress']}")
    print(f"  {icons['Likely Distress']} Likely Distress: {counts['Likely Distress']}")
    print(f"  {icons['Watch']} Watch: {counts['Watch']}")
    print(f"  Total flagged: {len(feed)}")

    print(f"\n{'─'*70}")

    for i, item in enumerate(feed[:limit]):
        icon = icons.get(item["distress_level"], "⚪")
        discount = f"-{item['discount_pct']:.0f}% vs sold" if item.get("discount_pct") and item["discount_pct"] > 0 else ""
        drops = f"x{item['drop_count']}" if item["drop_count"] > 0 else ""
        beds = f"{item['bedrooms']}BR" if item['bedrooms'] else "Studio"
        sqft = f"{item['area_sqft']:,.0f}sqft" if item.get("area_sqft") else ""
        dom = f"{item['days_on_market']}d" if item.get("days_on_market") else ""

        print(f"\n  {icon} #{i+1} | Score: {item['distress_score']} | {item['distress_level']}")
        print(f"     AED {item['price']:>12,.0f}  {discount}")
        print(f"     {item['area_name']} | {beds} | {sqft} | {dom} {drops}")
        if item["signals"]:
            for s in item["signals"]:
                print(f"     -> {s}")
        if item.get("url"):
            print(f"     {item['url']}")

    print(f"\n{'='*70}")


def print_report():
    """Print a summary report of all detected drops."""
    conn = get_db()

    total_drops = conn.execute("SELECT COUNT(*) AS c FROM drops").fetchone()["c"]
    total_listings = conn.execute("SELECT COUNT(*) AS c FROM listings WHERE is_active=1").fetchone()["c"]
    total_txns = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]

    print("=" * 60)
    print("  DUBAI DISTRESS RADAR — REPORT")
    print("=" * 60)
    print(f"  Active listings tracked:  {total_listings:,}")
    print(f"  DLD transactions loaded:  {total_txns:,}")
    print(f"  Total drops detected:     {total_drops:,}")

    print("\n  DROP CLASSIFICATION:")
    for row in conn.execute("""
        SELECT drop_type, COUNT(*) AS cnt, AVG(drop_pct) AS avg_pct,
               SUM(drop_amount) AS total_aed
        FROM drops GROUP BY drop_type ORDER BY cnt DESC
    """).fetchall():
        emoji = {"capitulation": "🔴", "multi_cut": "🟠", "mania_normal": "🟡"}.get(row["drop_type"], "⚪")
        print(f"    {emoji} {row['drop_type']:20} {row['cnt']:5} drops  "
              f"avg -{row['avg_pct']:.1f}%  total AED {row['total_aed']:,.0f}")

    print("\n  TOP 20 BIGGEST DROPS:")
    print(f"  {'Area':<25} {'Type':<15} {'Old Price':>12} {'New Price':>12} {'Drop':>8} {'#':>3}")
    print("  " + "-" * 78)
    for d in conn.execute("""
        SELECT l.area_name, l.property_type, l.bedrooms, l.url,
               d.previous_price, d.new_price, d.drop_pct, d.drop_number, d.drop_type
        FROM drops d JOIN listings l ON d.listing_id = l.listing_id
        ORDER BY d.drop_pct DESC LIMIT 20
    """).fetchall():
        print(f"  {d['area_name']:<25} {d['drop_type']:<15} "
              f"{d['previous_price']:>12,.0f} {d['new_price']:>12,.0f} "
              f"{d['drop_pct']:>6.1f}% {d['drop_number']:>3}")

    multi = conn.execute("SELECT * FROM v_multi_cutters LIMIT 10").fetchall()
    if multi:
        print(f"\n  TOP MULTI-CUT LISTINGS ({len(multi)} found):")
        print(f"  {'Area':<25} {'Drops':>5} {'Original':>12} {'Current':>12} {'Total %':>8}")
        print("  " + "-" * 65)
        for m in multi:
            print(f"  {m['area_name']:<25} {m['total_drops']:>5} "
                  f"{m['original_price']:>12,.0f} {m['current_price']:>12,.0f} "
                  f"{m['total_drop_pct']:>7.1f}%")

    print("\n  AREA HOTSPOTS (most drops):")
    for row in conn.execute("""
        SELECT l.area_name, COUNT(*) AS cnt, AVG(d.drop_pct) AS avg,
               SUM(d.drop_amount) AS total
        FROM drops d JOIN listings l ON d.listing_id = l.listing_id
        GROUP BY l.area_name ORDER BY cnt DESC LIMIT 15
    """).fetchall():
        print(f"    {row['area_name']:<30} {row['cnt']:>4} drops  "
              f"avg -{row['avg']:.1f}%  AED {row['total']:>12,.0f}")

    gaps = conn.execute("""
        SELECT area_name, asking_vs_sold_gap_pct, median_listing_price, median_txn_price
        FROM area_benchmarks
        WHERE asking_vs_sold_gap_pct IS NOT NULL
        ORDER BY asking_vs_sold_gap_pct DESC LIMIT 15
    """).fetchall()
    if gaps:
        print("\n  ASKING vs SOLD PRICE GAP:")
        for g in gaps:
            print(f"    {g['area_name']:<30} gap: +{g['asking_vs_sold_gap_pct']:.1f}%  "
                  f"listing: {g['median_listing_price']:>12,.0f}  "
                  f"sold: {g['median_txn_price']:>12,.0f}")

    conn.close()
    print("\n" + "=" * 60)


def export_csv(output_path="drops_export.csv"):
    """Export all active drops to CSV."""
    conn = get_db()
    drops = conn.execute("SELECT * FROM v_active_drops").fetchall()
    if not drops:
        print("No drops to export")
        return

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(drops[0].keys())
        for d in drops:
            writer.writerow(tuple(d))

    conn.close()
    print(f"Exported {len(drops)} drops to {output_path}")


def export_json_feed(feed, output_path="live_feed.json"):
    """Export feed as JSON."""
    with open(output_path, "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "total_flagged": len(feed),
            "deals": feed
        }, f, indent=2, default=str)
    print(f"Exported {len(feed)} deals to {output_path}")


def export_html(feed, path="live_feed.html"):
    """Generate a self-contained HTML page for the distress feed."""
    icons = {"High Distress": "🔴", "Likely Distress": "🟠", "Watch": "🟡"}
    colors = {"High Distress": "#ef4444", "Likely Distress": "#f97316", "Watch": "#eab308"}

    rows_html = ""
    for i, item in enumerate(feed[:200]):
        color = colors.get(item["distress_level"], "#666")
        discount = f"-{item['discount_pct']:.0f}%" if item.get("discount_pct") and item["discount_pct"] > 0 else "—"
        beds = f"{item['bedrooms']}BR" if item.get("bedrooms") else "Studio"
        sqft = f"{item['area_sqft']:,.0f}" if item.get("area_sqft") else "—"
        signals = "<br>".join(f"-> {s}" for s in item.get("signals", []))
        link = f'<a href="{item["url"]}" target="_blank" style="color:#3b82f6">View</a>' if item.get("url") else ""

        rows_html += f"""<tr>
            <td style="color:{color};font-weight:700">{icons.get(item['distress_level'],'⚪')} {item['distress_score']}</td>
            <td><strong>{item['area_name']}</strong><br><small style="color:#888">{item.get('title','')[:60]}</small></td>
            <td class="mono">AED {item['price']:,.0f}</td>
            <td style="color:{color};font-weight:700">{discount}</td>
            <td>{beds}</td>
            <td class="mono">{sqft}</td>
            <td>{item.get('drop_count',0)}</td>
            <td>{item.get('days_on_market','—')}</td>
            <td><small>{signals}</small></td>
            <td>{link}</td>
        </tr>"""

    counts = defaultdict(int)
    for item in feed:
        counts[item["distress_level"]] += 1

    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dubai Distress Feed — Live</title>
<style>
  *{{margin:0;padding:0;box-sizing:border-box}}
  body{{font-family:system-ui,sans-serif;background:#0a0e17;color:#d8dee9;padding:20px}}
  h1{{font-size:22px;color:#d4a843;margin-bottom:4px}}
  .sub{{color:#5a6578;font-size:13px;margin-bottom:20px}}
  .kpis{{display:flex;gap:16px;margin-bottom:24px;flex-wrap:wrap}}
  .kpi{{background:#0d1117;border:1px solid #1b2536;border-radius:8px;padding:14px 20px}}
  .kpi .v{{font-size:24px;font-weight:700}} .kpi .l{{font-size:11px;color:#5a6578;text-transform:uppercase;letter-spacing:1px}}
  table{{width:100%;border-collapse:collapse;font-size:13px}}
  th{{text-align:left;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:#5a6578;padding:10px 8px;border-bottom:2px solid #1b2536;position:sticky;top:0;background:#0a0e17}}
  td{{padding:10px 8px;border-bottom:1px solid #141c28;vertical-align:top}}
  tr:hover td{{background:rgba(212,168,67,0.03)}}
  .mono{{font-family:'Courier New',monospace}}
  a{{text-decoration:none}}
</style></head><body>
<h1>DUBAI DISTRESS FEED — LIVE</h1>
<div class="sub">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')} · {len(feed)} flagged deals</div>
<div class="kpis">
  <div class="kpi"><div class="v" style="color:#ef4444">{counts['High Distress']}</div><div class="l">High Distress</div></div>
  <div class="kpi"><div class="v" style="color:#f97316">{counts['Likely Distress']}</div><div class="l">Likely Distress</div></div>
  <div class="kpi"><div class="v" style="color:#eab308">{counts['Watch']}</div><div class="l">Watch</div></div>
  <div class="kpi"><div class="v">{len(feed)}</div><div class="l">Total Flagged</div></div>
</div>
<div style="overflow-x:auto">
<table>
<thead><tr><th>Score</th><th>Property</th><th>Price</th><th>vs Sold</th><th>Beds</th><th>Sqft</th><th>Drops</th><th>DOM</th><th>Signals</th><th>Link</th></tr></thead>
<tbody>{rows_html}</tbody>
</table></div></body></html>"""

    with open(path, "w") as f:
        f.write(html)
    print(f"Generated HTML feed: {path} ({len(feed)} deals)")


# ─────────────────────────────────────────────────────────────────
# FULL PIPELINE
# ─────────────────────────────────────────────────────────────────

def run_full_pipeline():
    """Run the complete daily pipeline: fetch + detect + feed."""
    print("=" * 60)
    print(f"  DUBAI DISTRESS RADAR — FULL RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    init_db()

    # Step 1: Fetch listings
    print(f"\n{'─'*60}")
    print("Step 1: Fetching listings from Bayut API")
    print(f"{'─'*60}")
    run_fetch()

    # Step 2: Detect drops
    print(f"\n{'─'*60}")
    print("Step 2: Detecting price drops")
    print(f"{'─'*60}")
    compute_area_benchmarks()
    detect_drops()

    # Step 3: Generate feed
    print(f"\n{'─'*60}")
    print("Step 3: Generating distress feed")
    print(f"{'─'*60}")
    feed = run_matching()
    if feed:
        export_json_feed(feed)
        export_html(feed)
        print_feed(feed)

    print(f"\n{'='*60}")
    print(f"  FULL RUN COMPLETE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    return feed


def test_api():
    """Test the Bayut API connection."""
    print("Testing Bayut14 API connection...")
    data = api_call("health")
    if data and data.get("success"):
        print("API connection successful!")
        print("\nFetching sample data from Downtown Dubai...")
        properties, total = fetch_properties("6901", "for-sale", "apartments", 1)
        print(f"Got {len(properties)} listings (total available: {total})")
        if properties:
            p = parse_property(properties[0])
            print(f"\n   Sample listing:")
            print(f"   Title:    {p['title']}")
            print(f"   Price:    AED {p['price']:,.0f}")
            print(f"   Area:     {p['area_name']} | {p['building_name']}")
            print(f"   Size:     {p['area_sqm']} sqm / {p['area_sqft']} sqft")
            print(f"   Beds:     {p['bedrooms']} | Baths: {p['bathrooms']}")
            print(f"   Type:     {p['property_type']}")
            print(f"   URL:      {p['url']}")
    else:
        print("API connection failed — check your key")


# ─────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dubai Distress Radar — All-in-one")
    parser.add_argument("--fetch", action="store_true", help="Fetch listings from Bayut")
    parser.add_argument("--detect", action="store_true", help="Detect price drops")
    parser.add_argument("--feed", action="store_true", help="Regenerate feed from existing data")
    parser.add_argument("--report", action="store_true", help="Print summary report")
    parser.add_argument("--export", choices=["csv", "json"], help="Export drops")
    parser.add_argument("--html", action="store_true", help="Generate HTML feed")
    parser.add_argument("--import-dld", metavar="CSV", help="Import DLD transaction CSV")
    parser.add_argument("--benchmarks", action="store_true", help="Recompute area benchmarks")
    parser.add_argument("--init", action="store_true", help="Initialize the database")
    parser.add_argument("--test", action="store_true", help="Test API connection")
    parser.add_argument("--crawl", action="store_true", help="Crawl via Firecrawl")
    parser.add_argument("--area", help="Focus on a specific area")
    args = parser.parse_args()

    if args.test:
        test_api()
    elif args.init:
        init_db()
        print("Database initialized.")
    elif args.import_dld:
        import_dld_csv(args.import_dld)
    elif args.benchmarks:
        compute_area_benchmarks()
    elif args.fetch:
        areas = [args.area] if args.area else None
        run_fetch(areas)
    elif args.crawl:
        crawl_all()
    elif args.detect:
        compute_area_benchmarks()
        detect_drops()
    elif args.report:
        print_report()
    elif args.export:
        if args.export == "csv":
            export_csv()
        else:
            init_db()
            feed = run_matching()
            export_json_feed(feed)
    elif args.html:
        init_db()
        feed = run_matching()
        export_html(feed)
    elif args.feed:
        init_db()
        feed = run_matching()
        export_json_feed(feed)
        export_html(feed)
        print_feed(feed)
    else:
        run_full_pipeline()
