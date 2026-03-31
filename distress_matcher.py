"""
distress_matcher.py — The core intelligence engine.

Compares every active listing against DLD transaction benchmarks
to flag live distress deals in real time.

Matching logic:
  1. For each active listing, find its area in DLD benchmarks
  2. Calculate listing price per sqm
  3. Compare against area's median SOLD price per sqm
  4. If listing is significantly below what properties actually sell for → DISTRESS
  5. Also flag: price drops, multi-cuts, days on market, Delayed Sell patterns

Usage:
    python distress_matcher.py                # Run matching + print feed
    python distress_matcher.py --json         # Output JSON feed
    python distress_matcher.py --html         # Generate HTML feed page
    python distress_matcher.py --stats        # Print area-level stats
"""

import sqlite3
import json
import re
from datetime import datetime, date
from collections import defaultdict

try:
    from config_local import *
except ImportError:
    from config import *


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Area Name Normalization ──────────────────────────────────
# DLD uses "BUSINESS BAY", Bayut uses "Business Bay"

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


def normalize_area(area_name):
    """Map a listing's area name to the DLD benchmark area name."""
    if not area_name:
        return None
    key = area_name.strip().lower()
    # Direct alias match
    if key in AREA_ALIASES:
        return AREA_ALIASES[key]
    # Try uppercase match
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
        if area not in benchmarks:  # keep most recent
            benchmarks[area] = dict(r)
    return benchmarks


# ── Distress Scoring ─────────────────────────────────────────

def score_listing(listing, benchmark, drops):
    """
    Score a listing for distress signals. Returns a dict with:
      - distress_score (0-10)
      - distress_level (Normal / Watch / Likely Distress / High Distress)
      - signals: list of reasons
      - discount_pct: % below area median sold price
    """
    signals = []
    score = 0
    discount_pct = None

    price = listing["current_price"]
    sqft = listing["area_sqft"]
    sqm = listing["area_sqm"]

    # ── Signal 1: Price per sqm below DLD median ──
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

    # ── Signal 2: Below absolute median transaction price ──
    if benchmark and benchmark.get("median_txn_price"):
        median_txn = benchmark["median_txn_price"]
        if price < median_txn * 0.8:
            score += 2
            signals.append(f"Listed at {price:,.0f} vs area median sold {median_txn:,.0f}")

    # ── Signal 3: Price drops detected ──
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

    # ── Signal 4: Days on market ──
    if listing.get("days_on_market") and listing["days_on_market"] > 90:
        score += 1
        signals.append(f"Listed for {listing['days_on_market']} days")

    # ── Classify ──
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


# ── Main Matching Engine ─────────────────────────────────────

def run_matching():
    """Match all active listings against DLD benchmarks."""
    conn = get_db()
    benchmarks = load_benchmarks(conn)

    if not benchmarks:
        print("⚠ No benchmarks loaded. Run: python drop_detector.py --import-dld <csv> && --benchmarks")
        return []

    print(f"📊 Loaded benchmarks for {len(benchmarks)} areas")

    # Get all active listings with their current price
    listings = conn.execute("""
        SELECT l.*,
            (SELECT price FROM price_history WHERE listing_id = l.listing_id
             ORDER BY recorded_at DESC LIMIT 1) AS current_price,
            CAST(julianday('now') - julianday(l.first_seen) AS INTEGER) AS days_on_market
        FROM listings l
        WHERE l.is_active = 1 AND l.purpose = 'for-sale'
    """).fetchall()

    print(f"🏠 Analyzing {len(listings)} active listings")

    feed = []
    for row in listings:
        listing = dict(row)
        area_name = normalize_area(listing.get("area_name", ""))
        benchmark = benchmarks.get(area_name)
        price = listing.get("current_price")

        if not price or price <= 0:
            continue

        # Get drop history for this listing
        drops = [dict(d) for d in conn.execute("""
            SELECT drop_pct, drop_amount, drop_type, detected_at
            FROM drops WHERE listing_id = ? ORDER BY detected_at ASC
        """, (listing["listing_id"],)).fetchall()]

        result = score_listing(listing, benchmark, drops)

        if result["distress_score"] >= 2:  # Include Watch and above
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

    # Sort by distress score (highest first), then by discount
    feed.sort(key=lambda x: (x["distress_score"], x.get("discount_pct") or 0), reverse=True)

    conn.close()
    return feed


# ── Output Formats ───────────────────────────────────────────

def print_feed(feed, limit=50):
    """Print the distress feed to terminal."""
    icons = {"High Distress": "🔴", "Likely Distress": "🟠", "Watch": "🟡", "Normal": "⚪"}

    print("\n" + "=" * 70)
    print("  🏠 LIVE DISTRESS FEED — Dubai Real Estate")
    print("  " + datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("=" * 70)

    # Summary
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
        drops = f"📉×{item['drop_count']}" if item["drop_count"] > 0 else ""
        beds = f"{item['bedrooms']}BR" if item["bedrooms"] else "Studio"
        sqft = f"{item['area_sqft']:,.0f}sqft" if item.get("area_sqft") else ""
        dom = f"{item['days_on_market']}d" if item.get("days_on_market") else ""

        print(f"\n  {icon} #{i+1} | Score: {item['distress_score']} | {item['distress_level']}")
        print(f"     AED {item['price']:>12,.0f}  {discount}")
        print(f"     {item['area_name']} | {beds} | {sqft} | {dom} {drops}")
        if item["signals"]:
            for s in item["signals"]:
                print(f"     → {s}")
        if item.get("url"):
            print(f"     🔗 {item['url']}")

    print(f"\n{'='*70}")


def export_json(feed, path="live_feed.json"):
    """Export feed as JSON."""
    with open(path, "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "total_flagged": len(feed),
            "deals": feed
        }, f, indent=2, default=str)
    print(f"✅ Exported {len(feed)} deals to {path}")


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
        signals = "<br>".join(f"→ {s}" for s in item.get("signals", []))
        link = f'<a href="{item["url"]}" target="_blank" style="color:#3b82f6">View →</a>' if item.get("url") else ""

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
<h1>🏠 DUBAI DISTRESS FEED — LIVE</h1>
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
    print(f"✅ Generated HTML feed: {path} ({len(feed)} deals)")


def print_area_stats(feed):
    """Print area-level distress statistics."""
    areas = defaultdict(lambda: {"count": 0, "high": 0, "likely": 0, "avg_discount": []})
    for item in feed:
        a = areas[item["area_name"]]
        a["count"] += 1
        if item["distress_level"] == "High Distress":
            a["high"] += 1
        elif item["distress_level"] == "Likely Distress":
            a["likely"] += 1
        if item.get("discount_pct") and item["discount_pct"] > 0:
            a["avg_discount"].append(item["discount_pct"])

    print(f"\n{'Area':<30} {'Total':>6} {'High':>6} {'Likely':>6} {'Avg Discount':>12}")
    print("─" * 65)
    for area, stats in sorted(areas.items(), key=lambda x: x[1]["count"], reverse=True):
        avg_d = sum(stats["avg_discount"]) / len(stats["avg_discount"]) if stats["avg_discount"] else 0
        print(f"{area:<30} {stats['count']:>6} {stats['high']:>6} {stats['likely']:>6} {avg_d:>10.1f}%")


# ── CLI ──────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Live Distress Matcher")
    parser.add_argument("--json", action="store_true", help="Export JSON feed")
    parser.add_argument("--html", action="store_true", help="Generate HTML feed")
    parser.add_argument("--stats", action="store_true", help="Print area stats")
    parser.add_argument("--limit", type=int, default=50, help="Max items to show")
    args = parser.parse_args()

    feed = run_matching()

    if args.json:
        export_json(feed)
    elif args.html:
        export_html(feed)
    elif args.stats:
        print_area_stats(feed)
    else:
        print_feed(feed, limit=args.limit)
