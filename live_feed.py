"""
live_feed.py — Full pipeline orchestrator.
Crawls listings → detects drops → matches against DLD → generates feed.

Usage:
    python live_feed.py                    # Full run: crawl + detect + feed
    python live_feed.py --feed             # Just regenerate feed from existing data
    python live_feed.py --html             # Generate HTML feed
    python live_feed.py --monitor          # Continuous mode (every 6 hours)
    python live_feed.py --area "palm jumeirah"  # Focus on one area
"""

import sys
import os
import time
import argparse
import sqlite3
from datetime import datetime

try:
    from config_local import *
except ImportError:
    from config import *


def init_db():
    """Ensure database exists and is initialized."""
    if not os.path.exists(DB_PATH):
        print("📦 Initializing database...")
        conn = sqlite3.connect(DB_PATH)
        with open("schema.sql", "r") as f:
            conn.executescript(f.read())
        conn.close()
        print("   Created:", DB_PATH)
        return True
    return False


def run_pipeline(crawl=True, area_filter=None):
    """Execute the full pipeline."""
    print("=" * 60)
    print(f"  🏠 DUBAI DISTRESS RADAR — LIVE FEED")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    init_db()

    # Step 1: Crawl listings
    if crawl:
        print(f"\n{'─'*60}")
        print("▶ Step 1: Crawling live listings via Firecrawl")
        print(f"{'─'*60}")

        from listing_crawler import crawl_all, firecrawl_health

        if not firecrawl_health():
            print("⚠ Firecrawl is not running — skipping crawl")
            print("  Start with: cd firecrawl && docker compose up -d")
            print("  Continuing with existing data...\n")
        else:
            urls = None
            if area_filter:
                # Build URL for the specific area
                slug = area_filter.lower().replace(" ", "-")
                urls = [
                    f"https://www.bayut.com/for-sale/apartments/dubai/{slug}/?sort=date_desc",
                    f"https://www.bayut.com/for-sale/villas/dubai/{slug}/?sort=date_desc",
                ]
                print(f"  Focusing on: {area_filter}")

            stats = crawl_all(urls)
            if stats:
                print(f"  ✓ Crawled {stats['pages']} pages, {stats['listings']} listings")
                print(f"    {stats['new']} new, {stats['price_changes']} price changes")

    # Step 2: Detect price drops
    print(f"\n{'─'*60}")
    print("▶ Step 2: Detecting price drops")
    print(f"{'─'*60}")

    from drop_detector import detect_drops, compute_area_benchmarks

    # Recompute benchmarks if we have transaction data
    conn = sqlite3.connect(DB_PATH)
    txn_count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    conn.close()

    if txn_count > 0:
        compute_area_benchmarks()

    new_drops = detect_drops()
    print(f"  ✓ {new_drops} new drops detected")

    # Step 3: Match listings against benchmarks
    print(f"\n{'─'*60}")
    print("▶ Step 3: Matching against DLD benchmarks")
    print(f"{'─'*60}")

    from distress_matcher import run_matching, print_feed, export_json, export_html

    feed = run_matching()

    # Step 4: Output
    print_feed(feed)

    # Always save JSON
    export_json(feed)

    return feed


def monitor_mode(interval_hours=6):
    """Run continuously, re-crawling every N hours."""
    print(f"🔁 Monitor mode — running every {interval_hours} hours")
    print("   Press Ctrl+C to stop\n")

    while True:
        try:
            feed = run_pipeline(crawl=True)

            # Also generate HTML each time
            from distress_matcher import export_html
            export_html(feed)

            next_run = datetime.now().timestamp() + (interval_hours * 3600)
            next_str = datetime.fromtimestamp(next_run).strftime('%H:%M')
            print(f"\n⏰ Next run at {next_str} — sleeping {interval_hours}h...")
            time.sleep(interval_hours * 3600)

        except KeyboardInterrupt:
            print("\n\n👋 Monitor stopped")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dubai Distress Radar — Live Feed")
    parser.add_argument("--feed", action="store_true", help="Regenerate feed without crawling")
    parser.add_argument("--html", action="store_true", help="Generate HTML feed")
    parser.add_argument("--monitor", action="store_true", help="Continuous mode")
    parser.add_argument("--area", help="Focus on a specific area")
    parser.add_argument("--interval", type=float, default=6, help="Monitor interval in hours")
    args = parser.parse_args()

    if args.monitor:
        monitor_mode(args.interval)
    elif args.feed:
        init_db()
        from distress_matcher import run_matching, print_feed, export_json, export_html
        feed = run_matching()
        print_feed(feed)
        export_json(feed)
        if args.html:
            export_html(feed)
    elif args.html:
        init_db()
        from distress_matcher import run_matching, export_html
        feed = run_matching()
        export_html(feed)
    else:
        feed = run_pipeline(crawl=True, area_filter=args.area)
        if args.html or True:  # always generate HTML
            from distress_matcher import export_html
            export_html(feed)
