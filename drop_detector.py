"""
drop_detector.py — Detect and classify listing price drops.
Run after bayut_fetcher.py to analyze price changes.

Drop Classification (inspired by PanicSelling):
  1. MANIA→NORMAL:  First-time cut, 5-15%, after 30+ days on market
  2. MULTI-CUT:     2+ drops within 90 days — seller increasingly desperate
  3. CAPITULATION:   Drop pushes price below area median (from DLD data)

Usage:
    python drop_detector.py                # Detect new drops
    python drop_detector.py --report       # Print summary report
    python drop_detector.py --export csv   # Export drops to CSV
    python drop_detector.py --import-dld sales_ready.csv  # Import DLD data
"""

import sqlite3
import csv
import argparse
import json
from datetime import datetime, date, timedelta
from collections import defaultdict

try:
    from config_local import *
except ImportError:
    from config import *


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── DLD Data Import ──────────────────────────────────────────

def import_dld_csv(csv_path):
    """Import DLD transaction CSV (supports both old and new column names)."""
    conn = get_db()
    count = 0
    skipped = 0

    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize column names (handle both DLD formats)
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
    print(f"✅ Imported {count} transactions from {csv_path} ({skipped} skipped)")


# ── Area Benchmark Computation ───────────────────────────────

def compute_area_benchmarks():
    """Compute area-level price benchmarks from DLD transactions and listings."""
    conn = get_db()
    today = date.today().isoformat()

    # From DLD transactions
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

    # For each area, compute full stats
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

        # Active listing median price for this area
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
    print(f"📊 Computed benchmarks for {count} areas")
    return count


# ── Drop Detection Engine ────────────────────────────────────

def detect_drops():
    """Scan all listings for price drops and classify them."""
    conn = get_db()
    today = date.today().isoformat()
    new_drops = 0

    # Find all listings with price history (at least 2 snapshots)
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

        # Get price history ordered by time
        prices = conn.execute("""
            SELECT price, recorded_at FROM price_history
            WHERE listing_id = ? ORDER BY recorded_at ASC
        """, (lid,)).fetchall()

        if len(prices) < 2:
            continue

        latest = prices[-1]
        previous = prices[-2]

        # Skip if price went up or stayed same
        if latest["price"] >= previous["price"]:
            continue

        drop_amount = previous["price"] - latest["price"]
        drop_pct = (drop_amount / previous["price"]) * 100

        # Apply minimum thresholds
        if drop_pct < MIN_DROP_PCT or drop_amount < MIN_DROP_AED:
            continue

        # Check if we already recorded this exact drop
        existing = conn.execute("""
            SELECT id FROM drops
            WHERE listing_id = ? AND new_price = ? AND previous_price = ?
        """, (lid, latest["price"], previous["price"])).fetchone()

        if existing:
            continue

        # Count previous drops for this listing
        prev_drops = conn.execute(
            "SELECT COUNT(*) AS cnt FROM drops WHERE listing_id = ?", (lid,)
        ).fetchone()["cnt"]

        drop_number = prev_drops + 1

        # Get first_seen for days calculation
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

        # ── Classify the drop ──
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

    # Update area drop stats
    update_area_stats(conn)
    conn.commit()
    conn.close()

    print(f"📉 Detected {new_drops} new price drops")
    return new_drops


def classify_drop(conn, listing_id, drop_pct, drop_number, days_listed,
                  days_since_last, new_price, listing_info):
    """
    Classify a drop into one of three types:
      - mania_normal:  First cut after sitting on market, typically 5-15%
      - multi_cut:     2+ drops within the multi-cut window
      - capitulation:  Price pushed below area median transaction value
    """
    area_name = listing_info["area_name"] if listing_info else ""

    # Check for capitulation: is new price below area median?
    if area_name:
        bench = conn.execute("""
            SELECT median_txn_price FROM area_benchmarks
            WHERE area_name = ? ORDER BY computed_at DESC LIMIT 1
        """, (area_name,)).fetchone()

        if bench and bench["median_txn_price"] and new_price < bench["median_txn_price"]:
            return "capitulation"

    # Check for multi-cut
    if drop_number >= 2:
        if days_since_last is not None and days_since_last <= MULTI_CUT_WINDOW_DAYS:
            return "multi_cut"
        return "multi_cut"  # Any 2nd+ drop is multi-cut

    # Check for deep single cut (>15% = capitulation even on first cut)
    if drop_pct >= CAPITULATION_THRESHOLD:
        return "capitulation"

    # Default: mania → normal adjustment
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


# ── Reporting ────────────────────────────────────────────────

def print_report():
    """Print a summary report of all detected drops."""
    conn = get_db()

    # Overview
    total_drops = conn.execute("SELECT COUNT(*) AS c FROM drops").fetchone()["c"]
    total_listings = conn.execute("SELECT COUNT(*) AS c FROM listings WHERE is_active=1").fetchone()["c"]
    total_txns = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]

    print("=" * 60)
    print("  DUBAI DISTRESS RADAR — REPORT")
    print("=" * 60)
    print(f"  Active listings tracked:  {total_listings:,}")
    print(f"  DLD transactions loaded:  {total_txns:,}")
    print(f"  Total drops detected:     {total_drops:,}")

    # By type
    print("\n  DROP CLASSIFICATION:")
    for row in conn.execute("""
        SELECT drop_type, COUNT(*) AS cnt, AVG(drop_pct) AS avg_pct,
               SUM(drop_amount) AS total_aed
        FROM drops GROUP BY drop_type ORDER BY cnt DESC
    """).fetchall():
        emoji = {"capitulation": "🔴", "multi_cut": "🟠", "mania_normal": "🟡"}.get(row["drop_type"], "⚪")
        print(f"    {emoji} {row['drop_type']:20} {row['cnt']:5} drops  "
              f"avg -{row['avg_pct']:.1f}%  total AED {row['total_aed']:,.0f}")

    # Top drops
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

    # Multi-cutters
    multi = conn.execute("SELECT * FROM v_multi_cutters LIMIT 10").fetchall()
    if multi:
        print(f"\n  TOP MULTI-CUT LISTINGS ({len(multi)} found):")
        print(f"  {'Area':<25} {'Drops':>5} {'Original':>12} {'Current':>12} {'Total %':>8}")
        print("  " + "-" * 65)
        for m in multi:
            print(f"  {m['area_name']:<25} {m['total_drops']:>5} "
                  f"{m['original_price']:>12,.0f} {m['current_price']:>12,.0f} "
                  f"{m['total_drop_pct']:>7.1f}%")

    # Area hotspots
    print("\n  AREA HOTSPOTS (most drops):")
    for row in conn.execute("""
        SELECT l.area_name, COUNT(*) AS cnt, AVG(d.drop_pct) AS avg,
               SUM(d.drop_amount) AS total
        FROM drops d JOIN listings l ON d.listing_id = l.listing_id
        GROUP BY l.area_name ORDER BY cnt DESC LIMIT 15
    """).fetchall():
        print(f"    {row['area_name']:<30} {row['cnt']:>4} drops  "
              f"avg -{row['avg']:.1f}%  AED {row['total']:>12,.0f}")

    # Asking vs Sold gap
    gaps = conn.execute("""
        SELECT area_name, asking_vs_sold_gap_pct, median_listing_price, median_txn_price
        FROM area_benchmarks
        WHERE asking_vs_sold_gap_pct IS NOT NULL
        ORDER BY asking_vs_sold_gap_pct DESC LIMIT 15
    """).fetchall()
    if gaps:
        print("\n  ASKING vs SOLD PRICE GAP (listing price vs DLD transaction):")
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
    print(f"✅ Exported {len(drops)} drops to {output_path}")


def export_json(output_path="drops_export.json"):
    """Export drops as JSON (for web frontend)."""
    conn = get_db()
    drops = conn.execute("SELECT * FROM v_active_drops LIMIT 500").fetchall()
    data = [dict(d) for d in drops]

    with open(output_path, "w") as f:
        json.dump(data, f, indent=2, default=str)

    conn.close()
    print(f"✅ Exported {len(data)} drops to {output_path}")


# ── Main ─────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Price Drop Detection Engine")
    parser.add_argument("--report", action="store_true", help="Print summary report")
    parser.add_argument("--export", choices=["csv", "json"], help="Export drops")
    parser.add_argument("--import-dld", metavar="CSV", help="Import DLD transaction CSV")
    parser.add_argument("--benchmarks", action="store_true", help="Recompute area benchmarks")
    args = parser.parse_args()

    if args.import_dld:
        import_dld_csv(args.import_dld)
    elif args.benchmarks:
        compute_area_benchmarks()
    elif args.report:
        print_report()
    elif args.export:
        if args.export == "csv":
            export_csv()
        else:
            export_json()
    else:
        # Default: detect drops, compute benchmarks, print report
        print("🔍 Running drop detection...\n")
        compute_area_benchmarks()
        new_drops = detect_drops()
        print()
        print_report()
