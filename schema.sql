-- ============================================================
-- DUBAI DISTRESS RADAR — Database Schema (SQLite)
-- ============================================================
-- Run once: sqlite3 radar.db < schema.sql
-- ============================================================

-- Tracks every listing we've ever seen
CREATE TABLE IF NOT EXISTS listings (
    listing_id      TEXT PRIMARY KEY,           -- Bayut external ID
    source          TEXT NOT NULL DEFAULT 'bayut', -- bayut / propertyfinder / manual
    title           TEXT,
    purpose         TEXT,                        -- for-sale / for-rent
    category        TEXT,                        -- residential / commercial
    property_type   TEXT,                        -- apartment / villa / penthouse / townhouse
    bedrooms        INTEGER,
    bathrooms       INTEGER,
    area_sqft       REAL,
    area_sqm        REAL,
    location_name   TEXT,                        -- full breadcrumb: Dubai > Business Bay > ...
    area_name       TEXT,                        -- neighborhood: Business Bay
    building_name   TEXT,
    latitude        REAL,
    longitude       REAL,
    furnishing      TEXT,
    completion      TEXT,                        -- ready / off-plan
    agent_name      TEXT,
    agency_name     TEXT,
    permit_number   TEXT,
    url             TEXT,
    photo_url       TEXT,
    first_seen      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active       BOOLEAN DEFAULT 1
);

-- Every price snapshot we record (one row per listing per day)
CREATE TABLE IF NOT EXISTS price_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL REFERENCES listings(listing_id),
    price           REAL NOT NULL,
    currency        TEXT DEFAULT 'AED',
    recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(listing_id, recorded_at)
);

-- Detected price drops (one row per drop event)
CREATE TABLE IF NOT EXISTS drops (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id      TEXT NOT NULL REFERENCES listings(listing_id),
    previous_price  REAL NOT NULL,
    new_price       REAL NOT NULL,
    drop_amount     REAL NOT NULL,               -- absolute AED difference
    drop_pct        REAL NOT NULL,               -- percentage drop
    drop_number     INTEGER DEFAULT 1,           -- 1st drop, 2nd drop, etc.
    drop_type       TEXT,                         -- mania_normal / multi_cut / capitulation
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    days_since_listed INTEGER,                   -- days between first_seen and this drop
    days_since_last_drop INTEGER                 -- days since previous drop (NULL if first)
);

-- DLD transaction data (from CSV uploads or Dubai Pulse API)
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          TEXT PRIMARY KEY,
    txn_date        TIMESTAMP,
    txn_type        TEXT,                        -- Sales / Mortgage / Gifts
    txn_subtype     TEXT,                        -- Sale / Delayed Sell / etc.
    registration    TEXT,                        -- Ready / Off-Plan
    freehold        TEXT,
    usage           TEXT,                        -- Residential / Commercial
    area_name       TEXT,
    property_type   TEXT,
    property_subtype TEXT,
    amount          REAL,
    txn_area_sqm    REAL,
    actual_area_sqm REAL,
    rooms           TEXT,
    parking         TEXT,
    project         TEXT,
    price_per_sqm   REAL,                        -- calculated: amount / actual_area_sqm
    imported_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Area-level benchmarks (recalculated daily from transactions)
CREATE TABLE IF NOT EXISTS area_benchmarks (
    area_name       TEXT NOT NULL,
    computed_at     DATE NOT NULL,
    txn_count       INTEGER,
    median_price_sqm REAL,
    mean_price_sqm  REAL,
    q25_price_sqm   REAL,
    q10_price_sqm   REAL,
    median_listing_price REAL,                   -- from active Bayut listings
    median_txn_price REAL,                       -- from DLD transactions
    asking_vs_sold_gap_pct REAL,                 -- (listing - txn) / txn * 100
    PRIMARY KEY (area_name, computed_at)
);

-- Daily area-level drop stats for trend tracking
CREATE TABLE IF NOT EXISTS area_drop_stats (
    area_name       TEXT NOT NULL,
    stat_date       DATE NOT NULL,
    active_listings INTEGER DEFAULT 0,
    drops_today     INTEGER DEFAULT 0,
    avg_drop_pct    REAL,
    total_drop_aed  REAL,
    multi_cut_count INTEGER DEFAULT 0,           -- listings with 2+ drops
    capitulation_count INTEGER DEFAULT 0,        -- drops below market value
    PRIMARY KEY (area_name, stat_date)
);

-- Run log for tracking daily execution
CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    listings_fetched INTEGER DEFAULT 0,
    new_listings    INTEGER DEFAULT 0,
    price_changes   INTEGER DEFAULT 0,
    drops_detected  INTEGER DEFAULT 0,
    api_calls_used  INTEGER DEFAULT 0,
    duration_sec    REAL,
    status          TEXT DEFAULT 'success',
    notes           TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_price_history_listing ON price_history(listing_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_drops_listing ON drops(listing_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_drops_type ON drops(drop_type, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_drops_area ON drops(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_listings_area ON listings(area_name, is_active);
CREATE INDEX IF NOT EXISTS idx_listings_active ON listings(is_active, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_area ON transactions(area_name, txn_date DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON transactions(txn_date DESC);

-- Views for quick analysis
CREATE VIEW IF NOT EXISTS v_active_drops AS
SELECT
    d.id AS drop_id,
    l.listing_id,
    l.title,
    l.property_type,
    l.bedrooms,
    l.area_sqft,
    l.area_name,
    l.building_name,
    l.url,
    l.photo_url,
    d.previous_price,
    d.new_price,
    d.drop_amount,
    d.drop_pct,
    d.drop_number,
    d.drop_type,
    d.detected_at,
    d.days_since_listed,
    l.first_seen,
    l.agent_name,
    l.agency_name
FROM drops d
JOIN listings l ON d.listing_id = l.listing_id
WHERE l.is_active = 1
ORDER BY d.detected_at DESC;

CREATE VIEW IF NOT EXISTS v_multi_cutters AS
SELECT
    l.listing_id,
    l.title,
    l.area_name,
    l.property_type,
    l.bedrooms,
    l.url,
    COUNT(d.id) AS total_drops,
    MIN(d.detected_at) AS first_drop,
    MAX(d.detected_at) AS last_drop,
    SUM(d.drop_amount) AS cumulative_drop_aed,
    ROUND((1.0 - (MIN(d.new_price) / MAX(d.previous_price))) * 100, 1) AS total_drop_pct,
    MAX(d.previous_price) AS original_price,
    MIN(d.new_price) AS current_price
FROM drops d
JOIN listings l ON d.listing_id = l.listing_id
WHERE l.is_active = 1
GROUP BY l.listing_id
HAVING COUNT(d.id) >= 2
ORDER BY total_drop_pct DESC;
