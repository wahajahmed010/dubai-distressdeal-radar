# Dubai Distress Radar

> *"Markets don't move in a vacuum—they move on fear, uncertainty, and the quiet recalculations of millions of investors."*

---

## The Story Behind This Project

It started with a simple question that kept nagging at me: **How do geopolitical tensions actually ripple through real estate markets?**

I was watching the news one evening—another headline about rising tensions in the Middle East—and found myself wondering: *What happens to the people who own property in these regions? Do they panic-sell? Hold on and hope? And what does that do to prices?*

Dubai has always fascinated me. It's this glittering hub—an oasis of glass and steel rising from the desert, a place where capital from around the world flows in and out. It's a market that thrives on confidence and stability. So what happens when that confidence shakes?

I couldn't find good data. Sure, there were headlines about "property prices falling" or "investor sentiment shifting," but nothing that tracked the *mechanics* of it. The actual price drops, the patterns of distressed selling, the capitulation events where someone just wants out—NOW.

So I built this tool. Not as a commercial product. Not even really as a trading strategy. Just pure curiosity made tangible. A personal research project to watch how stress moves through one of the world's most interesting property markets.

This radar tracks daily price changes across Dubai's key areas—Business Bay, Marina, Downtown, Palm Jumeirah—and classifies the drops. A small adjustment? Just a seller testing the market. Multiple cuts in 90 days? That's urgency. A drop below the area's median transaction price? That's capitulation—someone who needs liquidity more than they need their investment.

By cross-referencing listing data with official DLD (Dubai Land Department) transaction records, this tool can distinguish between a seller correcting an overpriced listing and genuine panic selling.

The goal isn't to exploit anyone's distress. It's to understand. To see patterns invisible to the casual observer. To know—when the next headline hits—exactly how the numbers move.

---

## What This Tool Does

A personal research tool that tracks Dubai real estate listing price drops and identifies distressed properties by combining **live listing data** (Bayut API) with **official transaction records** (DLD/Dubai Pulse).

Think of it as your own version of [PanicSelling.xyz](https://panicselling.xyz) — but with the added power of DLD transaction data to validate whether a "drop" is a real deal or just an overpriced listing correcting.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     DAILY PIPELINE (run_daily.py)                │
│                                                                  │
│  ┌─────────────────┐    ┌──────────────────┐    ┌────────────┐  │
│  │ bayut_fetcher.py │───▶│  SQLite (radar.db) │◀──│ DLD CSVs   │  │
│  │                 │    │                  │    │ (manual     │  │
│  │ • Search areas  │    │  • listings      │    │  upload)    │  │
│  │ • Fetch listings│    │  • price_history  │    └────────────┘  │
│  │ • Record prices │    │  • transactions   │                    │
│  └─────────────────┘    │  • drops          │    ┌────────────┐  │
│                         │  • area_benchmarks│◀──│ Dubai Pulse │  │
│  ┌─────────────────┐    │  • area_drop_stats│    │ API (future)│  │
│  │ drop_detector.py │───▶│  • run_log       │    └────────────┘  │
│  │                 │    └──────────────────┘                    │
│  │ • Detect drops  │              │                              │
│  │ • Classify type │              ▼                              │
│  │ • Compute stats │    ┌──────────────────┐                    │
│  │ • Export data   │    │  drops_export.json │ ← for frontend   │
│  └─────────────────┘    │  drops_export.csv  │ ← for Excel      │
│                         └──────────────────┘                    │
└──────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
Day 1: Fetch listings → Store prices → No drops (baseline)
Day 2: Fetch listings → Compare prices → Detect drops → Classify
Day 3: Fetch listings → Compare prices → Detect multi-cuts → Update stats
...
Day N: Rich price history → Trend analysis → Capitulation detection
```

---

## Quick Start

### 1. Install dependencies

```bash
pip install requests
```

That's it. The project uses only `requests` + Python standard library (sqlite3, csv, json).

### 2. Get your Bayut API key (free)

1. Go to [rapidapi.com/BayutAPI/api/bayut-api1](https://rapidapi.com/BayutAPI/api/bayut-api1)
2. Click **Subscribe** → select the **Free** plan (750 calls/month, no credit card)
3. Copy your API key from the dashboard

### 3. Configure

```bash
cp config_local.py.example config_local.py
# Edit config_local.py and paste your API key:
# RAPIDAPI_KEY = "your_actual_key_here"
```

### 4. Initialize the database

```bash
sqlite3 radar.db < schema.sql
```

### 5. Import your DLD data (optional but recommended)

If you have DLD CSV exports from dubailand.gov.ae:

```bash
python drop_detector.py --import-dld sales_ready.csv
python drop_detector.py --import-dld sales_offplan.csv
python drop_detector.py --import-dld mortgages.csv
python drop_detector.py --benchmarks
```

This creates area-level price benchmarks that enable **capitulation detection** (identifying when listing prices drop below what properties actually sell for).

### 6. Run the fetcher

```bash
# Fetch all configured Dubai areas
python bayut_fetcher.py

# Fetch a single area
python bayut_fetcher.py --area business-bay

# Find a location ID
python bayut_fetcher.py --search "Palm Jumeirah"
```

### 7. Detect drops

```bash
# Run detection + print report
python drop_detector.py

# Export to CSV or JSON
python drop_detector.py --export csv
python drop_detector.py --export json
```

### 8. Automate (daily cron)

```bash
# Linux/Mac crontab
crontab -e
# Add:
0 8 * * * cd /path/to/distress-radar && python run_daily.py >> logs/daily.log 2>&1
```

---

## Drop Classification System

Each detected drop is classified into one of three types:

| Type | Signal | Criteria |
|------|--------|----------|
| 🟡 `mania_normal` | Seller adjusting from peak optimism | First drop, typically 5-15% |
| 🟠 `multi_cut` | Growing urgency — property not moving | 2+ drops within 90 days |
| 🔴 `capitulation` | Selling below market — wants out NOW | Price below area median (from DLD data) OR single drop > 15% |

### Your Edge Over PanicSelling

PanicSelling only tracks listing prices. You also have DLD transaction data, which means:

- **Capitulation detection** is based on _actual sold prices_, not just listing comparisons
- **Asking vs. Sold gap** shows you which areas have the most inflated listings
- **Transaction-level distress** (Delayed Sells, below-median sales) adds signals that listing data alone can't provide

---

## Database Schema

### Core Tables

| Table | Purpose |
|-------|---------|
| `listings` | Every property listing ever seen (Bayut ID, details, location) |
| `price_history` | Daily price snapshots per listing (builds the timeline) |
| `drops` | Each detected price drop with classification |
| `transactions` | DLD transaction records (from CSV imports or API) |
| `area_benchmarks` | Computed area-level price stats (updated daily) |
| `area_drop_stats` | Daily area-level drop aggregates |
| `run_log` | Pipeline execution history |

### Key Views

| View | What it shows |
|------|---------------|
| `v_active_drops` | All drops on currently active listings (joined with listing details) |
| `v_multi_cutters` | Listings with 2+ price drops — maximum urgency signals |

---

## API Budget Management

The free Bayut API tier gives you **750 calls/month** (~25/day).

The default config scans 16 areas × 3 pages = **48 calls per run**, leaving room for ~15 daily runs. For personal research, running once daily is sufficient.

To stretch your budget:
- Reduce `PAGES_PER_AREA` to 1-2
- Focus on fewer areas (edit `DUBAI_LOCATION_IDS` in config)
- Increase `API_DELAY_SECONDS` to be a good API citizen

For heavier usage, Bayut API paid plans start at ~$10/month for 10,000 calls.

---

## Useful Queries

```sql
-- Top 20 biggest drops ever
SELECT * FROM v_active_drops ORDER BY drop_pct DESC LIMIT 20;

-- All multi-cut listings (maximum urgency)
SELECT * FROM v_multi_cutters ORDER BY total_drop_pct DESC;

-- Areas with most drops this week
SELECT l.area_name, COUNT(*) as drops, AVG(d.drop_pct) as avg_pct
FROM drops d JOIN listings l ON d.listing_id = l.listing_id
WHERE d.detected_at > date('now', '-7 days')
GROUP BY l.area_name ORDER BY drops DESC;

-- Asking vs Sold gap (where are listings most overpriced?)
SELECT * FROM area_benchmarks
WHERE asking_vs_sold_gap_pct IS NOT NULL
ORDER BY asking_vs_sold_gap_pct DESC;

-- Properties with 3+ price cuts (truly desperate sellers)
SELECT l.area_name, l.title, l.url, COUNT(d.id) as cuts,
       MAX(d.previous_price) as original, MIN(d.new_price) as current
FROM drops d JOIN listings l ON d.listing_id = l.listing_id
GROUP BY l.listing_id HAVING cuts >= 3
ORDER BY cuts DESC;

-- Capitulation events this month
SELECT l.area_name, l.title, d.previous_price, d.new_price,
       d.drop_pct, l.url
FROM drops d JOIN listings l ON d.listing_id = l.listing_id
WHERE d.drop_type = 'capitulation'
  AND d.detected_at > date('now', '-30 days')
ORDER BY d.drop_pct DESC;

-- Price history for a specific listing
SELECT price, recorded_at FROM price_history
WHERE listing_id = '12345' ORDER BY recorded_at;
```

---

## File Structure

```
distress-radar/
├── config.py              # Default configuration (don't edit)
├── config_local.py.example # Example config file (copy to config_local.py)
├── config_local.py        # Your local config with API key (gitignored)
├── schema.sql             # Database schema
├── bayut_fetcher.py       # Bayut API integration
├── drop_detector.py       # Drop detection & classification engine
├── run_daily.py           # Daily pipeline orchestrator
├── radar.db               # SQLite database (gitignored, created on first run)
├── drops_export.csv       # Exported drops (generated)
├── drops_export.json      # Exported drops for frontend (generated)
└── README.md              # This file
```

---

## Future Enhancements

- [ ] Dubai Pulse API integration (historical transactions without CSV upload)
- [ ] Telegram/email alerts for capitulation events
- [ ] Web dashboard frontend (React)
- [ ] Rental market tracking
- [ ] Abu Dhabi coverage
- [ ] Price prediction model using drop patterns + DLD data
- [ ] Correlation analysis with geopolitical event timelines

---

## Disclaimer

This is a personal research project created for educational purposes. It is not financial advice. Real estate markets are complex and influenced by many factors beyond what's captured here. Always do your own due diligence.

The name "Distress Radar" refers to market signals, not individuals. Property sellers have many valid reasons for adjusting prices. This tool observes patterns—it doesn't judge the people behind them.
