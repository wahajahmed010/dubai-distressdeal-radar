"""
listing_crawler.py — Crawl listing pages via self-hosted Firecrawl.
Extracts structured property data and stores it in the radar database.

Firecrawl renders JavaScript, handles pagination, and returns clean data.
This script parses the output and feeds it into the existing radar DB.

Usage:
    python listing_crawler.py                          # Crawl all configured URLs
    python listing_crawler.py --url "https://..."      # Crawl a specific URL
    python listing_crawler.py --test                   # Test Firecrawl connection
"""

import sqlite3
import requests
import json
import re
import time
import hashlib
import argparse
from datetime import datetime, date

try:
    from config_local import *
except ImportError:
    from config import *


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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
            print(f"  ✗ Firecrawl returned {r.status_code}: {r.text[:200]}")
            return None
    except requests.exceptions.Timeout:
        print(f"  ✗ Firecrawl timeout for {url}")
        return None
    except Exception as e:
        print(f"  ✗ Firecrawl error: {e}")
        return None


# ── Parsing Engine ────────────────────────────────────────────
# Multiple strategies to extract listings from page content

def parse_listings_from_markdown(markdown_text, source_url=""):
    """Parse property listings from Firecrawl markdown output."""
    listings = []
    if not markdown_text:
        return listings

    # Strategy 1: Look for structured listing blocks
    # Bayut typically renders listings with price, location, beds/baths, area, link
    # We'll use regex patterns to extract these from the markdown

    # Split into potential listing blocks (double newline separated)
    blocks = re.split(r'\n{2,}', markdown_text)

    current = {}
    for block in blocks:
        block = block.strip()
        if not block:
            continue

        # Detect price patterns (AED with number)
        price_match = re.search(r'AED\s*([\d,]+(?:\.\d+)?)', block, re.IGNORECASE)
        if not price_match:
            price_match = re.search(r'([\d,]+)\s*AED', block, re.IGNORECASE)

        # Detect bedroom patterns
        bed_match = re.search(r'(\d+)\s*(?:Bed(?:room)?s?|BR|B/R)', block, re.IGNORECASE)
        studio_match = re.search(r'\bStudio\b', block, re.IGNORECASE)

        # Detect bathroom patterns
        bath_match = re.search(r'(\d+)\s*(?:Bath(?:room)?s?)', block, re.IGNORECASE)

        # Detect area (sqft)
        sqft_match = re.search(r'([\d,]+(?:\.\d+)?)\s*(?:sq\.?\s*ft|sqft|sq ft)', block, re.IGNORECASE)

        # Detect links to listing detail pages
        link_match = re.findall(r'\[([^\]]*)\]\((https?://[^\)]+)\)', block)
        bayut_link = None
        for text, url in link_match:
            if 'bayut.com' in url and ('/property/' in url or '/for-sale/' in url or '/for-rent/' in url):
                if re.search(r'\d{5,}', url):  # listing URLs have numeric IDs
                    bayut_link = url
                    break

        # Detect property type
        prop_type = "apartment"
        if re.search(r'\bvilla\b', block, re.IGNORECASE):
            prop_type = "villa"
        elif re.search(r'\bpenthouse\b', block, re.IGNORECASE):
            prop_type = "penthouse"
        elif re.search(r'\btownhouse\b', block, re.IGNORECASE):
            prop_type = "townhouse"
        elif re.search(r'\bstudio\b', block, re.IGNORECASE):
            prop_type = "apartment"

        # If we have enough to constitute a listing
        if price_match and (bed_match or studio_match or sqft_match):
            price_str = price_match.group(1).replace(',', '')
            price = float(price_str) if price_str else 0

            bedrooms = 0
            if studio_match and not bed_match:
                bedrooms = 0  # studio
            elif bed_match:
                bedrooms = int(bed_match.group(1))

            bathrooms = int(bath_match.group(1)) if bath_match else None
            sqft = float(sqft_match.group(1).replace(',', '')) if sqft_match else None

            # Extract area name from the source URL or the block text
            area_name = extract_area_from_url(source_url)

            # Generate a unique listing ID from content
            listing_id = generate_listing_id(bayut_link, block, price, bedrooms, sqft)

            # Extract title (first line of the block, or link text)
            title = block.split('\n')[0][:200].strip('#*[] ')
            if link_match:
                title = link_match[0][0][:200] if link_match[0][0] else title

            if price > 50000:  # filter out junk
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

    # Look for JSON-LD structured data (many listing sites embed this)
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
    # https://www.bayut.com/for-sale/apartments/dubai/business-bay/ -> Business Bay
    match = re.search(r'/dubai/([^/?#]+)', url)
    if match:
        slug = match.group(1).rstrip('/')
        return slug.replace('-', ' ').title().replace('Jvc', 'JVC').replace('Jlt', 'JLT')
    return ""


def generate_listing_id(url, text, price, beds, sqft):
    """Generate a stable ID for a listing."""
    if url:
        # Extract numeric ID from URL if present
        num_match = re.search(r'(\d{6,})', url)
        if num_match:
            return f"bayut_{num_match.group(1)}"
    # Fallback: hash the content
    content = f"{price}_{beds}_{sqft}_{text[:100]}"
    return f"crawl_{hashlib.md5(content.encode()).hexdigest()[:12]}"


# ── Database Storage ─────────────────────────────────────────

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

    # Record price snapshot
    existing_today = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) = ?
    """, (lid, today)).fetchone()

    if not existing_today:
        conn.execute("INSERT INTO price_history (listing_id, price, recorded_at) VALUES (?,?,?)",
                     (lid, price, now))
    elif existing_today["price"] != price:
        # Price changed same day — update
        conn.execute("""
            UPDATE price_history SET price = ?, recorded_at = ?
            WHERE listing_id = ? AND DATE(recorded_at) = ?
        """, (price, now, lid, today))

    # Get previous price
    prev = conn.execute("""
        SELECT price FROM price_history WHERE listing_id = ? AND DATE(recorded_at) < ?
        ORDER BY recorded_at DESC LIMIT 1
    """, (lid, today)).fetchone()

    return is_new, prev["price"] if prev else None, price


# ── Main Crawl Orchestrator ──────────────────────────────────

def crawl_url(url, conn, stats):
    """Crawl a single search page URL and store listings."""
    print(f"\n  🌐 {url}")

    # Scrape with Firecrawl
    data = firecrawl_scrape(url, formats=["markdown"])
    if not data:
        print("     ✗ No data returned")
        return

    markdown = data.get("markdown", "")
    html = data.get("html", "")

    # Try markdown parsing first
    listings = parse_listings_from_markdown(markdown, url)

    # Fallback to HTML if markdown parsing found nothing
    if not listings and html:
        listings = parse_listings_from_html(html, url)

    if not listings:
        print(f"     ⚠ No listings parsed (got {len(markdown)} chars of markdown)")
        # Save raw markdown for debugging
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

    print(f"     ✓ {len(listings)} listings ({new_count} new, {change_count} price changes)")


def crawl_all(urls=None):
    """Crawl all configured URLs or a provided list."""
    if not firecrawl_health():
        print("✗ Firecrawl is not running at", FIRECRAWL_URL)
        print("  Start it with: cd firecrawl && docker compose up -d")
        return None

    conn = get_db()
    target_urls = urls or BAYUT_SEARCH_URLS
    stats = {"pages": 0, "listings": 0, "new": 0, "price_changes": 0}

    print(f"🔍 Crawling {len(target_urls)} search pages via Firecrawl")
    print(f"   Firecrawl: {FIRECRAWL_URL}")

    for url in target_urls:
        crawl_url(url, conn, stats)

        # Paginate: try page 2 if configured
        for page in range(2, MAX_PAGES_PER_URL + 1):
            sep = "&" if "?" in url else "?"
            page_url = f"{url}{sep}page={page}"
            crawl_url(page_url, conn, stats)

    conn.commit()
    conn.close()
    return stats


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Listing Crawler (via Firecrawl)")
    parser.add_argument("--url", help="Crawl a specific URL")
    parser.add_argument("--test", action="store_true", help="Test Firecrawl connection")
    args = parser.parse_args()

    if args.test:
        if firecrawl_health():
            print("✅ Firecrawl is running at", FIRECRAWL_URL)
            print("   Testing a scrape...")
            data = firecrawl_scrape("https://www.bayut.com/for-sale/apartments/dubai/business-bay/?sort=date_desc")
            if data:
                md = data.get("markdown", "")
                print(f"   Got {len(md)} chars of markdown")
                listings = parse_listings_from_markdown(md, "https://www.bayut.com/for-sale/apartments/dubai/business-bay/")
                print(f"   Parsed {len(listings)} listings")
                for l in listings[:3]:
                    print(f"     AED {l['price']:,.0f} | {l['bedrooms']}BR | {l['area_sqft'] or '?'}sqft | {l['area_name']}")
        else:
            print("✗ Firecrawl not reachable at", FIRECRAWL_URL)
    elif args.url:
        crawl_all([args.url])
    else:
        stats = crawl_all()
        if stats:
            print(f"\n{'='*50}")
            print(f"✅ Crawl complete: {stats['pages']} pages, {stats['listings']} listings")
            print(f"   {stats['new']} new | {stats['price_changes']} price changes")
