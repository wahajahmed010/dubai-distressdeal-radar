"""
run_daily.py — Orchestrate the full daily pipeline.
Schedule this via cron or Windows Task Scheduler.

Cron example (runs daily at 8am):
  0 8 * * * cd /path/to/distress-radar && python run_daily.py >> logs/daily.log 2>&1
"""

import subprocess
import sys
import os
import sqlite3
from datetime import datetime

try:
    from config_local import *
except ImportError:
    from config import *


def init_db():
    """Initialize database from schema if it doesn't exist."""
    if not os.path.exists(DB_PATH):
        print("📦 Initializing database...")
        conn = sqlite3.connect(DB_PATH)
        with open("schema.sql", "r") as f:
            conn.executescript(f.read())
        conn.close()
        print("   Database created: " + DB_PATH)
    else:
        print("📦 Database exists: " + DB_PATH)


def run_step(description, cmd):
    """Run a pipeline step and report status."""
    print(f"\n{'─'*50}")
    print(f"▶ {description}")
    print(f"{'─'*50}")
    result = subprocess.run([sys.executable] + cmd, capture_output=False)
    if result.returncode != 0:
        print(f"  ⚠ Step failed with exit code {result.returncode}")
        return False
    return True


def main():
    print("=" * 50)
    print(f"  DUBAI DISTRESS RADAR — DAILY RUN")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)

    # Step 0: Ensure DB exists
    init_db()

    # Step 1: Fetch new listings from Bayut
    run_step("Fetching listings from Bayut API", ["bayut_fetcher.py"])

    # Step 2: Detect drops and classify
    run_step("Detecting price drops", ["drop_detector.py"])

    # Step 3: Export fresh data
    run_step("Exporting drops to JSON", ["drop_detector.py", "--export", "json"])

    print(f"\n{'='*50}")
    print(f"  ✅ DAILY RUN COMPLETE")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
