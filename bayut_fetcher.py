"""
bayut_fetcher.py — Fetch listings from Bayut14 RapidAPI and store in SQLite.
Designed for daily cron runs. Tracks prices over time for drop detection.

Usage:
    python3 bayut_fetcher.py                        # Fetch all configured areas
    python3 bayut_fetcher.py --area business-bay     # Fetch single area
    python3 bayut_fetcher.py --test                  # Test API connection
"""

import sqlite3
import requests
import time
import argparse
from datetime import datetime, date

try:
    from config_local import *
except ImportError:
    from config import *


# ── Bayut14 API Configuration ────────────────────────────────

API_BASE = f"https://{RAPIDAPI_HOST}"
API_HEADERS = {
    "Content-Type": "application/json",
    "x-rapidapi-key": RAPIDAPI_KEY,
    "x-rapidapi-host": RAPIDAPI_HOST,
}

# Location IDs for Bayut14 API
# Use: python3 bayut_fetcher.py --test to verify connection
# Location ID 6901 = Downtown Dubai, 5002 = Dubai (city), etc.
AREA_IDS = {
    "downtown-dubai":       "6901",
    "business-bay":         "6573",
    "dubai-marina":         "5003",
    "palm-jumeirah":        "5548",
    "jumeirah-village-circle": "8143",
    "dubai-hills-estate":   "12695",
    "jumeirah-lake-towers": "5006",
    "arjan":                "8472",
    "dubai-creek-harbour":  "12700",
    "motor-city":           "5236",
    "sobha-hartland":       "12096",
    "al-furjan":            "8329",
    "dubai-sports-city":    "5200",
    "meydan-city":          "7918",
    "dubai-production-city":"5305",
    "dubai-silicon-oasis":  "5178",
    "jumeirah-village-triangle": "8144",
    "town-square":          "14611",
    "arabian-ranches":      "5095",
    "damac-hills":          "10822",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def api_call(endpoint, params=None, call_counter=None):
    """Make a rate-limited call to the Bayut14 API."""
    if call_counter and call_counter[0] >= MAX_API_CALLS_PER_RUN:
        print(f"  ⚠ API call limit ({MAX_API_CALLS_PER_RUN}) reached")
        return None

    url = f"{API_BASE}/{endpoint}"

    try:
        resp = requests.get(url, headers=API_HEADERS, params=params, timeout=30)
        if call_counter:
            call_counter[0] += 1

        if resp.status_code == 429:
            print("  ⚠ Rate limited — waiting 60s")
            time.sleep(60)
            return api_call(endpoint, params, call_counter)

        if resp.status_code == 403:
            print("  ✗ 403 Forbidden — check your API key and subscription")
            return None

        if resp.status_code != 200:
            print(f"  ✗ API returned {resp.status_code}: {resp.text[:200]}")
            return None

        time.sleep(API_DELAY_SECONDS)
        return resp.json()

    except requests.exceptions.Timeout:
        print("  ✗ Request timed out")
        return None
    except Exception as e:
        print(f"  ✗ API error: {e}")
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
    # Location parsing
    locations = raw.get("location", [])
    area_name = ""
    building_name = ""
    location_parts = []
    for loc in locations:
        name = loc.get("name", "")
        location_parts.append(name)
        level = loc.get("level", -1)
        if level == 2:  # neighbourhood
            area_name = name
        elif level == 3:
            if not area_name:
                area_name = name
        if loc.get("type") == "condo-building":
            building_name = name

    # Category parsing
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

    # Area is in sqm from the API
    area_sqm = raw.get("area") or 0
    area_sqft = round(area_sqm * 10.7639, 2) if area_sqm else None

    # Build URL
    slug = raw.get("slug", {})
    slug_en = slug.get("en", "") if isinstance(slug, dict) else str(slug)
    url = f"https://www.bayut.com/property/details-{raw.get('externalID', '')}.html"

    # Cover photo
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

    # Record price (one per day)
    existing_price = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) = ?
    """, (lid, today)).fetchone()

    if not existing_price:
        conn.execute("INSERT INTO price_history (listing_id, price, recorded_at) VALUES (?,?,?)",
                     (lid, price, now))

    # Get previous price
    prev = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) < ?
        ORDER BY recorded_at DESC LIMIT 1
    """, (lid, today)).fetchone()

    return is_new, prev["price"] if prev else None, price


# ── Main Execution ────────────────────────────────────────────

def run_fetch(areas=None):
    """Main fetch routine."""
    conn = get_db()
    start = time.time()
    call_counter = [0]
    stats = {"fetched": 0, "new": 0, "price_changes": 0}

    target_areas = areas if areas else list(AREA_IDS.keys())

    print(f"🔍 Dubai Distress Radar — Fetching {len(target_areas)} areas")
    print(f"   API: bayut14.p.rapidapi.com")
    print(f"   Budget: {MAX_API_CALLS_PER_RUN} calls | Delay: {API_DELAY_SECONDS}s\n")

    for area_slug in target_areas:
        loc_id = AREA_IDS.get(area_slug)
        if not loc_id:
            print(f"  ⚠ Unknown area: {area_slug} — skipping")
            continue

        for prop_type in ["apartments", "villas"]:
            for page in range(1, PAGES_PER_AREA + 1):
                print(f"  📦 {area_slug} | {prop_type} | page {page}", end="")

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
            print(f"\n⚠ API call budget exhausted ({call_counter[0]} calls)")
            break

    # Mark stale listings inactive
    conn.execute(f"""
        UPDATE listings SET is_active = 0
        WHERE is_active = 1 AND julianday('now') - julianday(last_seen) > {STALE_LISTING_DAYS}
    """)

    # Log the run
    duration = round(time.time() - start, 1)
    conn.execute("""
        INSERT INTO run_log (listings_fetched, new_listings, price_changes, api_calls_used, duration_sec)
        VALUES (?, ?, ?, ?, ?)
    """, (stats["fetched"], stats["new"], stats["price_changes"], call_counter[0], duration))

    conn.commit()
    conn.close()

    print(f"\n{'='*50}")
    print(f"✅ Done in {duration}s | {call_counter[0]} API calls")
    print(f"   {stats['fetched']} listings | {stats['new']} new | {stats['price_changes']} price changes")
    print(f"\n   Next: python3 drop_detector.py")
    print(f"   Then: python3 distress_matcher.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bayut14 API Fetcher")
    parser.add_argument("--area", help="Fetch single area (slug)")
    parser.add_argument("--test", action="store_true", help="Test API connection")
    args = parser.parse_args()

    if args.test:
        print("🔌 Testing Bayut14 API connection...")
        data = api_call("health")
        if data and data.get("success"):
            print("✅ API connection successful!")
            print("\nFetching sample data from Downtown Dubai...")
            properties, total = fetch_properties("6901", "for-sale", "apartments", 1)
            print(f"✅ Got {len(properties)} listings (total available: {total})")
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
            print("✗ API connection failed — check your key")
    elif args.area:
        run_fetch([args.area])
    else:
        run_fetch()
