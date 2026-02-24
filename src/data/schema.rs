/// DDL for PhantomFill's own SQLite tables.

pub const CREATE_MARKETS: &str = "
CREATE TABLE IF NOT EXISTS pf_markets (
    id            TEXT PRIMARY KEY,
    platform      TEXT NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    category      TEXT NOT NULL DEFAULT '',
    open_ts       INTEGER NOT NULL,
    close_ts      INTEGER NOT NULL,
    duration_secs INTEGER NOT NULL,
    outcome       TEXT
);
";

pub const CREATE_TICKS: &str = "
CREATE TABLE IF NOT EXISTS pf_ticks (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id       TEXT NOT NULL,
    side            TEXT NOT NULL,
    timestamp_ms    INTEGER NOT NULL,
    offset_ms       INTEGER NOT NULL,
    best_bid        REAL,
    best_bid_size   REAL,
    best_ask        REAL,
    best_ask_size   REAL,
    total_bid_depth REAL NOT NULL DEFAULT 0.0,
    total_ask_depth REAL NOT NULL DEFAULT 0.0,
    reference_price REAL,
    oracle_price    REAL
);
";

pub const CREATE_DEPTH_LEVELS: &str = "
CREATE TABLE IF NOT EXISTS pf_depth_levels (
    tick_id         INTEGER NOT NULL,
    price           REAL NOT NULL,
    cumulative_size REAL NOT NULL,
    FOREIGN KEY (tick_id) REFERENCES pf_ticks(id)
);
";

pub const CREATE_INDEXES: &str = "
CREATE INDEX IF NOT EXISTS idx_pf_ticks_market ON pf_ticks(market_id);
CREATE INDEX IF NOT EXISTS idx_pf_ticks_offset ON pf_ticks(offset_ms);
CREATE INDEX IF NOT EXISTS idx_pf_ticks_market_side_offset ON pf_ticks(market_id, side, offset_ms);
CREATE INDEX IF NOT EXISTS idx_pf_depth_tick ON pf_depth_levels(tick_id);
";

// ---------------------------------------------------------------------------
// Queries for reading the external pm-spread-arb book_ticks table.
// ---------------------------------------------------------------------------

/// List distinct slugs with their metadata from the source book_ticks table.
pub const PM_LIST_SLUGS: &str = "
SELECT
    slug,
    asset,
    timeframe,
    window_ts,
    MIN(tick_ms) AS first_tick_ms,
    MAX(tick_ms) AS last_tick_ms
FROM book_ticks
GROUP BY slug
ORDER BY window_ts
";

/// Load all ticks for a given slug, ordered by offset_ms then side.
pub const PM_LOAD_TICKS: &str = "
SELECT
    slug,
    side,
    tick_ms,
    offset_ms,
    best_bid,
    best_bid_size,
    best_ask,
    best_ask_size,
    depth_at_049,
    depth_at_050,
    depth_at_051,
    total_bid_depth,
    total_ask_depth,
    btc_price,
    chainlink_price
FROM book_ticks
WHERE slug = ?1
ORDER BY offset_ms, side
";

/// Load the first and last reference/oracle prices for outcome determination.
pub const PM_OUTCOME_PRICES: &str = "
SELECT
    side,
    offset_ms,
    btc_price,
    chainlink_price
FROM book_ticks
WHERE slug = ?1 AND side = 'UP'
ORDER BY offset_ms
";
