# Live Distress Feed — Setup Guide

## Step 1: Install Docker

### Mac
```bash
# Download Docker Desktop from: https://www.docker.com/products/docker-desktop/
# Or via Homebrew:
brew install --cask docker
```

### Windows
```bash
# Download Docker Desktop from: https://www.docker.com/products/docker-desktop/
# Enable WSL2 when prompted during installation
```

### Linux (Ubuntu/Debian)
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
# Log out and back in, then:
docker --version
```

---

## Step 2: Self-Host Firecrawl

```bash
# Clone Firecrawl
git clone https://github.com/firecrawl/firecrawl.git
cd firecrawl

# Create environment file
cat > .env << 'EOF'
NUM_WORKERS_PER_QUEUE=8
PORT=3002
HOST=0.0.0.0
REDIS_URL=redis://redis:6379
REDIS_RATE_LIMIT_URL=redis://redis:6379
BULL_AUTH_KEY=myradarkey123
EOF

# Start Firecrawl (first run downloads images — takes 2-5 min)
docker compose up -d

# Verify it's running
curl http://localhost:3002/v1/health
# Should return: {"status":"ok"}
```

**That's it.** Firecrawl is now running locally at `http://localhost:3002`.
No API key needed for self-hosted. No rate limits. No costs.

### Quick test
```bash
curl -X POST http://localhost:3002/v1/scrape \
  -H 'Content-Type: application/json' \
  -d '{"url": "https://example.com", "formats": ["markdown"]}'
```

### Useful commands
```bash
docker compose up -d       # Start in background
docker compose down        # Stop
docker compose logs -f     # View logs
docker compose restart     # Restart
```

---

## Step 3: Set Up the Distress Radar

```bash
# Go to your distress-radar project folder
cd /path/to/distress-radar

# Create local config
cp config.py config_local.py

# Edit config_local.py:
#   FIRECRAWL_URL = "http://localhost:3002"
#   (Bayut API key is optional now — Firecrawl is your primary source)

# Initialize the database (if not done already)
sqlite3 radar.db < schema.sql

# Import your DLD benchmark data
python drop_detector.py --import-dld sales_ready.csv
python drop_detector.py --import-dld sales_offplan.csv
python drop_detector.py --benchmarks
```

---

## Step 4: Run the Live Feed

```bash
# Crawl listings and flag distress deals (single run)
python live_feed.py

# Run with a specific area focus
python live_feed.py --area "business bay"

# Run in continuous monitor mode (re-scans every 6 hours)
python live_feed.py --monitor

# Just view the current distress feed
python live_feed.py --feed

# Export feed as HTML (open in browser)
python live_feed.py --html
```

---

## Step 5: Daily Automation

```bash
# Add to crontab (runs at 8am and 8pm daily)
crontab -e

# Add these lines:
0 8 * * * cd /path/to/distress-radar && python live_feed.py >> logs/feed.log 2>&1
0 20 * * * cd /path/to/distress-radar && python live_feed.py >> logs/feed.log 2>&1
```

---

## Architecture

```
YOU (daily)
  │
  ├── DLD website ──► CSV upload ──► drop_detector.py --import-dld
  │                                        │
  │                                  area_benchmarks
  │                                  (108 areas with median
  │                                   sold prices per sqm)
  │                                        │
  └── live_feed.py ◄───────────────────────┘
        │                                  │
        ├── Firecrawl (Docker)             │
        │   Scrapes listing pages          │
        │   Returns structured data        │
        │                                  ▼
        ├── listing_crawler.py      ┌─────────────┐
        │   Parses property data    │  radar.db    │
        │   Stores in database      │  (SQLite)    │
        │                           └──────┬──────┘
        └── distress_matcher.py            │
            Compares listing price    ▼
            vs DLD benchmark     LIVE FEED
            Flags deals below    (JSON / HTML / Terminal)
            market value
```

## Resource Requirements

- **RAM**: ~2GB for Docker (Firecrawl + Redis + Playwright)
- **Disk**: ~1GB for Docker images + your database
- **CPU**: Minimal — crawling is I/O bound, not CPU bound
- **Network**: Normal bandwidth, one page at a time
