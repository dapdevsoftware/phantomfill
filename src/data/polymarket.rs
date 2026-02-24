use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};
use tracing::debug;

use crate::types::{
    BookSnapshot, BookTick, Market, Outcome, Platform, PriceLevel, Side, SideState,
};

use super::schema;
use super::store::DataStore;

// ---------------------------------------------------------------------------
// PolymarketStore — direct read-only access to pm-spread-arb book_ticks
// ---------------------------------------------------------------------------

/// Read-only adapter that queries the pm-spread-arb `book_ticks` table
/// directly and produces PhantomFill's platform-agnostic types.
pub struct PolymarketStore {
    conn: Connection,
}

impl PolymarketStore {
    /// Open the source database in read-only mode.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        conn.execute_batch("PRAGMA query_only = ON;")?;
        Ok(Self { conn })
    }

    /// Open from the default pm-spread-arb database path.
    pub fn open_default() -> Result<Self> {
        let home = std::env::var("HOME").context("HOME not set")?;
        let path = Path::new(&home).join(".local/share/pm_trader/spread_arb.db");
        Self::open(&path)
    }

    /// List all available markets derived from distinct slugs in `book_ticks`.
    pub fn list_markets(&self) -> Result<Vec<Market>> {
        let mut stmt = self.conn.prepare(schema::PM_LIST_SLUGS)?;
        let rows = stmt.query_map([], |row| {
            let slug: String = row.get(0)?;
            let asset: String = row.get(1)?;
            let timeframe: String = row.get(2)?;
            let window_ts: i64 = row.get(3)?;

            let duration_secs = timeframe_to_secs(&timeframe);

            Ok(Market {
                id: slug,
                platform: Platform::Polymarket,
                description: format!("BTC up/down {} window at {}", timeframe, window_ts),
                category: asset,
                open_ts: window_ts,
                close_ts: window_ts + duration_secs,
                duration_secs,
                outcome: None,
            })
        })?;

        let mut markets = Vec::new();
        for r in rows {
            markets.push(r?);
        }

        debug!("listed {} markets from pm-spread-arb", markets.len());
        Ok(markets)
    }

    /// List markets with outcomes determined from price data.
    ///
    /// More expensive than [`list_markets`] because it queries first/last
    /// prices for every slug.
    pub fn list_markets_with_outcomes(&self) -> Result<Vec<Market>> {
        let mut markets = self.list_markets()?;
        for market in &mut markets {
            market.outcome = self.determine_outcome(&market.id)?;
        }
        Ok(markets)
    }

    /// Load all [`BookTick`]s for a slug, ordered by offset_ms then side.
    pub fn load_ticks(&self, slug: &str) -> Result<Vec<BookTick>> {
        let mut stmt = self.conn.prepare(schema::PM_LOAD_TICKS)?;

        let rows = stmt.query_map([slug], |row| {
            let slug: String = row.get(0)?;
            let side_str: String = row.get(1)?;
            let tick_ms: i64 = row.get(2)?;
            let offset_ms: i64 = row.get(3)?;
            let best_bid: Option<f64> = row.get(4)?;
            let best_bid_size: Option<f64> = row.get(5)?;
            let best_ask: Option<f64> = row.get(6)?;
            let best_ask_size: Option<f64> = row.get(7)?;
            let depth_049: Option<f64> = row.get(8)?;
            let depth_050: Option<f64> = row.get(9)?;
            let depth_051: Option<f64> = row.get(10)?;
            let total_bid_depth: Option<f64> = row.get(11)?;
            let total_ask_depth: Option<f64> = row.get(12)?;
            let btc_price: Option<f64> = row.get(13)?;
            let chainlink_price: Option<f64> = row.get(14)?;

            Ok(BookTick {
                market_id: slug,
                side: map_side(&side_str),
                timestamp_ms: tick_ms,
                offset_ms,
                best_bid,
                best_bid_size,
                best_ask,
                best_ask_size,
                depth: build_depth_levels(depth_049, depth_050, depth_051),
                total_bid_depth: total_bid_depth.unwrap_or(0.0),
                total_ask_depth: total_ask_depth.unwrap_or(0.0),
                reference_price: btc_price,
                oracle_price: chainlink_price,
            })
        })?;

        let mut ticks = Vec::new();
        for r in rows {
            ticks.push(r?);
        }

        debug!("loaded {} ticks for slug {}", ticks.len(), slug);
        Ok(ticks)
    }

    /// Load time-ordered [`BookSnapshot`]s for a market.
    ///
    /// Groups ticks by `offset_ms` and combines UP/DOWN sides into a single
    /// snapshot. When only one side has data at an offset, the previous
    /// state is carried forward for the missing side.
    pub fn load_snapshots(&self, slug: &str) -> Result<Vec<BookSnapshot>> {
        let ticks = self.load_ticks(slug)?;
        Ok(ticks_to_snapshots(slug, &ticks))
    }

    /// Determine the outcome of a market by comparing first vs last prices.
    ///
    /// Prefers `chainlink_price` when available; falls back to `btc_price`.
    fn determine_outcome(&self, slug: &str) -> Result<Option<Outcome>> {
        let mut stmt = self.conn.prepare(schema::PM_OUTCOME_PRICES)?;

        let mut first_btc: Option<f64> = None;
        let mut last_btc: Option<f64> = None;
        let mut first_chainlink: Option<f64> = None;
        let mut last_chainlink: Option<f64> = None;

        let rows = stmt.query_map([slug], |row| {
            let btc: Option<f64> = row.get(2)?;
            let chainlink: Option<f64> = row.get(3)?;
            Ok((btc, chainlink))
        })?;

        for r in rows {
            let (btc, chainlink) = r?;
            if first_btc.is_none() {
                first_btc = btc;
            }
            if first_chainlink.is_none() && chainlink.is_some() {
                first_chainlink = chainlink;
            }
            if btc.is_some() {
                last_btc = btc;
            }
            if chainlink.is_some() {
                last_chainlink = chainlink;
            }
        }

        // Prefer chainlink if both endpoints are available.
        let outcome = match (first_chainlink, last_chainlink) {
            (Some(first), Some(last)) => Some(if last > first {
                Outcome::Yes
            } else {
                Outcome::No
            }),
            _ => match (first_btc, last_btc) {
                (Some(first), Some(last)) => Some(if last > first {
                    Outcome::Yes
                } else {
                    Outcome::No
                }),
                _ => None,
            },
        };

        Ok(outcome)
    }
}

// ---------------------------------------------------------------------------
// Shared mapping helpers
// ---------------------------------------------------------------------------

/// Convert a Polymarket side string ("UP"/"DOWN") to platform-agnostic `Side`.
fn map_side(s: &str) -> Side {
    match s {
        "UP" => Side::Yes,
        _ => Side::No,
    }
}

/// Convert a timeframe string (e.g. "5m", "15m") to seconds.
fn timeframe_to_secs(tf: &str) -> i64 {
    match tf {
        "5m" => 300,
        "15m" => 900,
        "1h" => 3600,
        _ => {
            if let Some(rest) = tf.strip_suffix('m') {
                rest.parse::<i64>().unwrap_or(900) * 60
            } else if let Some(rest) = tf.strip_suffix('h') {
                rest.parse::<i64>().unwrap_or(1) * 3600
            } else {
                900
            }
        }
    }
}

/// Build a `Vec<PriceLevel>` from the three depth columns.
/// Only includes levels where the depth value is present and positive.
fn build_depth_levels(
    depth_049: Option<f64>,
    depth_050: Option<f64>,
    depth_051: Option<f64>,
) -> Vec<PriceLevel> {
    let mut levels = Vec::with_capacity(3);
    if let Some(d) = depth_049 {
        if d > 0.0 {
            levels.push(PriceLevel { price: 0.49, cumulative_size: d });
        }
    }
    if let Some(d) = depth_050 {
        if d > 0.0 {
            levels.push(PriceLevel { price: 0.50, cumulative_size: d });
        }
    }
    if let Some(d) = depth_051 {
        if d > 0.0 {
            levels.push(PriceLevel { price: 0.51, cumulative_size: d });
        }
    }
    levels
}

/// Convert a `BookTick` into a `SideState`.
fn tick_to_side_state(tick: &BookTick) -> SideState {
    SideState {
        best_bid: tick.best_bid,
        best_bid_size: tick.best_bid_size,
        best_ask: tick.best_ask,
        best_ask_size: tick.best_ask_size,
        depth: tick.depth.clone(),
        total_bid_depth: tick.total_bid_depth,
        total_ask_depth: tick.total_ask_depth,
    }
}

/// Group ticks into [`BookSnapshot`]s by offset_ms.
///
/// At each offset, UP (Yes) and/or DOWN (No) ticks are combined into one
/// snapshot. If a side is missing at a given offset, the previous snapshot's
/// state for that side is carried forward.
fn ticks_to_snapshots(market_id: &str, ticks: &[BookTick]) -> Vec<BookSnapshot> {
    if ticks.is_empty() {
        return Vec::new();
    }

    let mut snapshots = Vec::new();
    let mut prev_yes = SideState::default();
    let mut prev_no = SideState::default();

    let mut i = 0;
    while i < ticks.len() {
        let offset = ticks[i].offset_ms;
        let timestamp = ticks[i].timestamp_ms;
        let mut yes_state: Option<SideState> = None;
        let mut no_state: Option<SideState> = None;
        let mut ref_price: Option<f64> = None;
        let mut oracle_price: Option<f64> = None;

        // Consume all ticks at this offset_ms.
        while i < ticks.len() && ticks[i].offset_ms == offset {
            let tick = &ticks[i];
            match tick.side {
                Side::Yes => yes_state = Some(tick_to_side_state(tick)),
                Side::No => no_state = Some(tick_to_side_state(tick)),
            }
            if ref_price.is_none() {
                ref_price = tick.reference_price;
            }
            if oracle_price.is_none() {
                oracle_price = tick.oracle_price;
            }
            i += 1;
        }

        let yes = yes_state.unwrap_or_else(|| prev_yes.clone());
        let no = no_state.unwrap_or_else(|| prev_no.clone());

        prev_yes = yes.clone();
        prev_no = no.clone();

        snapshots.push(BookSnapshot {
            market_id: market_id.to_string(),
            offset_ms: offset,
            timestamp_ms: timestamp,
            yes,
            no,
            reference_price: ref_price,
            oracle_price,
        });
    }

    snapshots
}

// ---------------------------------------------------------------------------
// Import pipeline (existing code — reads source DB, writes to PhantomFill DB)
// ---------------------------------------------------------------------------

/// Statistics from an import run.
#[derive(Debug, Default)]
pub struct ImportStats {
    pub markets_imported: usize,
    pub ticks_imported: usize,
    pub markets_skipped: usize,
}

/// Minimum number of ticks a market must have to be imported.
const MIN_TICKS_PER_MARKET: usize = 10;

/// Import data from our capture database into a PhantomFill DataStore.
///
/// `source_path` — path to the spread_arb.db capture database.
/// `dest` — target DataStore (must already be init'd).
/// `filter` — optional asset name (e.g. "btc") or slug pattern (matched with LIKE).
pub fn import_from_capture_db(
    source_path: &Path,
    dest: &dyn DataStore,
    filter: Option<&str>,
) -> Result<ImportStats> {
    let src = Connection::open(source_path)
        .with_context(|| format!("Failed to open source DB: {}", source_path.display()))?;

    import_from_connection(&src, dest, filter)
}

/// Inner function that works on an already-opened connection (testable with in-memory DBs).
fn import_from_connection(
    src: &Connection,
    dest: &dyn DataStore,
    filter: Option<&str>,
) -> Result<ImportStats> {
    let mut stats = ImportStats::default();

    // Discover distinct markets (slug, asset, timeframe, window_ts)
    let mut market_sql = String::from(
        "SELECT DISTINCT slug, asset, timeframe, window_ts FROM book_ticks",
    );
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

    if let Some(f) = filter {
        if f.contains('%') {
            market_sql.push_str(" WHERE slug LIKE ?");
        } else {
            market_sql.push_str(" WHERE asset = ?");
        }
        params.push(Box::new(f.to_string()));
    }

    market_sql.push_str(" ORDER BY window_ts");

    let param_refs: Vec<&dyn rusqlite::types::ToSql> =
        params.iter().map(|p| p.as_ref()).collect();
    let mut stmt = src.prepare(&market_sql)?;
    let market_keys: Vec<(String, String, String, i64)> = stmt
        .query_map(param_refs.as_slice(), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    for (slug, asset, timeframe, window_ts) in &market_keys {
        // Load all ticks for this market window
        let mut tick_stmt = src.prepare_cached(
            "SELECT tick_ms, offset_ms, side, best_bid, best_bid_size, best_ask, best_ask_size,
                    depth_at_049, depth_at_050, depth_at_051,
                    total_bid_depth, total_ask_depth, btc_price, chainlink_price
             FROM book_ticks WHERE slug = ? ORDER BY offset_ms, side",
        )?;

        let raw_ticks: Vec<RawTick> = tick_stmt
            .query_map([slug], |row| {
                Ok(RawTick {
                    tick_ms: row.get(0)?,
                    offset_ms: row.get(1)?,
                    side: row.get::<_, String>(2)?,
                    best_bid: row.get(3)?,
                    best_bid_size: row.get(4)?,
                    best_ask: row.get(5)?,
                    best_ask_size: row.get(6)?,
                    depth_at_049: row.get(7)?,
                    depth_at_050: row.get(8)?,
                    depth_at_051: row.get(9)?,
                    total_bid_depth: row.get::<_, Option<f64>>(10)?.unwrap_or(0.0),
                    total_ask_depth: row.get::<_, Option<f64>>(11)?.unwrap_or(0.0),
                    btc_price: row.get(12)?,
                    chainlink_price: row.get(13)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Skip markets with too few ticks or no oracle data
        if raw_ticks.len() < MIN_TICKS_PER_MARKET {
            stats.markets_skipped += 1;
            continue;
        }

        let has_oracle = raw_ticks.iter().any(|t| t.chainlink_price.is_some());
        if !has_oracle {
            stats.markets_skipped += 1;
            continue;
        }

        // Determine outcome: compare first and last chainlink_price
        let outcome = determine_outcome(&raw_ticks);

        let duration_secs = parse_duration(timeframe);
        let close_ts = window_ts + duration_secs;

        let market = Market {
            id: slug.clone(),
            platform: Platform::Polymarket,
            description: format!("{} {} {}", asset.to_uppercase(), timeframe, slug),
            category: asset.clone(),
            open_ts: *window_ts,
            close_ts,
            duration_secs,
            outcome,
        };

        dest.insert_market(&market)?;

        // Convert ticks
        let book_ticks: Vec<BookTick> = raw_ticks
            .iter()
            .map(|rt| map_tick(slug, rt))
            .collect();

        dest.insert_ticks(&book_ticks)?;

        stats.markets_imported += 1;
        stats.ticks_imported += book_ticks.len();
    }

    Ok(stats)
}

/// Raw tick from the capture database.
struct RawTick {
    tick_ms: i64,
    offset_ms: i64,
    side: String,
    best_bid: Option<f64>,
    best_bid_size: Option<f64>,
    best_ask: Option<f64>,
    best_ask_size: Option<f64>,
    depth_at_049: Option<f64>,
    depth_at_050: Option<f64>,
    depth_at_051: Option<f64>,
    total_bid_depth: f64,
    total_ask_depth: f64,
    btc_price: Option<f64>,
    chainlink_price: Option<f64>,
}

fn map_tick(market_id: &str, rt: &RawTick) -> BookTick {
    BookTick {
        market_id: market_id.to_string(),
        side: map_side(&rt.side),
        timestamp_ms: rt.tick_ms,
        offset_ms: rt.offset_ms,
        best_bid: rt.best_bid,
        best_bid_size: rt.best_bid_size,
        best_ask: rt.best_ask,
        best_ask_size: rt.best_ask_size,
        depth: build_depth_levels(rt.depth_at_049, rt.depth_at_050, rt.depth_at_051),
        total_bid_depth: rt.total_bid_depth,
        total_ask_depth: rt.total_ask_depth,
        reference_price: rt.btc_price,
        oracle_price: rt.chainlink_price,
    }
}

fn determine_outcome(ticks: &[RawTick]) -> Option<Outcome> {
    let first_oracle = ticks.iter().find_map(|t| t.chainlink_price);
    let last_oracle = ticks.iter().rev().find_map(|t| t.chainlink_price);

    match (first_oracle, last_oracle) {
        (Some(open), Some(close)) => {
            if close > open {
                Some(Outcome::Yes) // price went up
            } else {
                Some(Outcome::No)
            }
        }
        _ => None,
    }
}

/// Alias for backwards compatibility with the import pipeline.
fn parse_duration(timeframe: &str) -> i64 {
    timeframe_to_secs(timeframe)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::store::SqliteStore;

    /// Create a minimal capture database in memory for testing.
    fn create_test_source_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE book_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT NOT NULL,
                asset TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                window_ts INTEGER NOT NULL,
                tick_ms INTEGER NOT NULL,
                offset_ms INTEGER NOT NULL,
                side TEXT NOT NULL,
                best_bid REAL,
                best_bid_size REAL,
                best_ask REAL,
                best_ask_size REAL,
                depth_at_049 REAL,
                depth_at_050 REAL,
                depth_at_051 REAL,
                total_bid_depth REAL,
                total_ask_depth REAL,
                num_bid_levels INTEGER,
                num_ask_levels INTEGER,
                btc_price REAL,
                chainlink_price REAL
            );",
        )
        .unwrap();
        conn
    }

    fn insert_test_ticks(conn: &Connection, slug: &str, count: usize, oracle_open: f64, oracle_close: f64) {
        let mut stmt = conn
            .prepare(
                "INSERT INTO book_ticks
                 (slug, asset, timeframe, window_ts, tick_ms, offset_ms, side,
                  best_bid, best_bid_size, best_ask, best_ask_size,
                  depth_at_049, depth_at_050, depth_at_051,
                  total_bid_depth, total_ask_depth, num_bid_levels, num_ask_levels,
                  btc_price, chainlink_price)
                 VALUES (?, 'btc', '5m', 1000, ?, ?, ?, 0.49, 100.0, 0.51, 200.0,
                         500.0, 120.0, 50.0, 600.0, 300.0, 5, 5, ?, ?)",
            )
            .unwrap();

        for i in 0..count {
            let offset = (i as i64) * 1000;
            let tick_ms = 1000_000 + offset;
            // Linearly interpolate oracle price
            let frac = if count > 1 { i as f64 / (count - 1) as f64 } else { 1.0 };
            let oracle = oracle_open + (oracle_close - oracle_open) * frac;
            let btc = oracle - 10.0; // slightly different reference
            for side in &["UP", "DOWN"] {
                stmt.execute(rusqlite::params![
                    slug, tick_ms, offset, side, btc, oracle
                ])
                .unwrap();
            }
        }
    }

    #[test]
    fn test_import_basic() {
        let src = create_test_source_db();
        // Market goes up: oracle 66000 -> 66100 => Outcome::Yes
        insert_test_ticks(&src, "btc-updown-5m-1000", 10, 66000.0, 66100.0);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let stats = import_from_connection(&src, &dest, None).unwrap();
        assert_eq!(stats.markets_imported, 1);
        assert_eq!(stats.ticks_imported, 20); // 10 offsets * 2 sides
        assert_eq!(stats.markets_skipped, 0);

        let markets = dest.list_markets(&Default::default()).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "btc-updown-5m-1000");
        assert_eq!(markets[0].outcome, Some(Outcome::Yes));
        assert_eq!(markets[0].duration_secs, 300);
        assert_eq!(markets[0].platform, Platform::Polymarket);
    }

    #[test]
    fn test_import_outcome_no() {
        let src = create_test_source_db();
        // Market goes down: oracle 66100 -> 66000 => Outcome::No
        insert_test_ticks(&src, "btc-updown-5m-2000", 10, 66100.0, 66000.0);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let stats = import_from_connection(&src, &dest, None).unwrap();
        assert_eq!(stats.markets_imported, 1);

        let markets = dest.list_markets(&Default::default()).unwrap();
        assert_eq!(markets[0].outcome, Some(Outcome::No));
    }

    #[test]
    fn test_import_skip_too_few_ticks() {
        let src = create_test_source_db();
        // Only 3 ticks (< MIN_TICKS_PER_MARKET of 10) => 6 rows but 3 unique offsets => skip
        insert_test_ticks(&src, "btc-updown-5m-3000", 3, 66000.0, 66100.0);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let stats = import_from_connection(&src, &dest, None).unwrap();
        assert_eq!(stats.markets_imported, 0);
        assert_eq!(stats.markets_skipped, 1);
    }

    #[test]
    fn test_import_filter_by_asset() {
        let src = create_test_source_db();
        insert_test_ticks(&src, "btc-updown-5m-4000", 10, 66000.0, 66100.0);

        // Add an ETH market
        src.execute_batch(
            "INSERT INTO book_ticks
             (slug, asset, timeframe, window_ts, tick_ms, offset_ms, side,
              best_bid, best_bid_size, best_ask, best_ask_size,
              depth_at_049, depth_at_050, depth_at_051,
              total_bid_depth, total_ask_depth, num_bid_levels, num_ask_levels,
              btc_price, chainlink_price)
             SELECT 'eth-updown-5m-4000', 'eth', timeframe, window_ts, tick_ms, offset_ms, side,
                    best_bid, best_bid_size, best_ask, best_ask_size,
                    depth_at_049, depth_at_050, depth_at_051,
                    total_bid_depth, total_ask_depth, num_bid_levels, num_ask_levels,
                    btc_price, chainlink_price
             FROM book_ticks WHERE slug = 'btc-updown-5m-4000';",
        )
        .unwrap();

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let stats = import_from_connection(&src, &dest, Some("btc")).unwrap();
        assert_eq!(stats.markets_imported, 1);

        let markets = dest.list_markets(&Default::default()).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].category, "btc");
    }

    #[test]
    fn test_import_depth_levels_preserved() {
        let src = create_test_source_db();
        insert_test_ticks(&src, "btc-updown-5m-5000", 10, 66000.0, 66100.0);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        import_from_connection(&src, &dest, None).unwrap();

        let ticks = dest.load_ticks("btc-updown-5m-5000").unwrap();
        assert!(!ticks.is_empty());

        // Every tick should have 3 depth levels (0.49, 0.50, 0.51)
        for tick in &ticks {
            assert_eq!(tick.depth.len(), 3, "tick at offset {} should have 3 depth levels", tick.offset_ms);
            assert!((tick.depth[0].price - 0.49).abs() < 1e-9);
            assert!((tick.depth[1].price - 0.50).abs() < 1e-9);
            assert!((tick.depth[2].price - 0.51).abs() < 1e-9);
            assert!(tick.depth[0].cumulative_size > 0.0);
        }
    }

    #[test]
    fn test_side_mapping() {
        let src = create_test_source_db();
        insert_test_ticks(&src, "btc-updown-5m-6000", 10, 66000.0, 66100.0);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        import_from_connection(&src, &dest, None).unwrap();

        let ticks = dest.load_ticks("btc-updown-5m-6000").unwrap();
        let yes_count = ticks.iter().filter(|t| t.side == Side::Yes).count();
        let no_count = ticks.iter().filter(|t| t.side == Side::No).count();
        assert_eq!(yes_count, no_count, "should have equal YES and NO ticks");
        assert!(yes_count > 0);
    }

    // -----------------------------------------------------------------------
    // Shared helper unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_map_side_values() {
        assert_eq!(map_side("UP"), Side::Yes);
        assert_eq!(map_side("DOWN"), Side::No);
        assert_eq!(map_side("anything"), Side::No);
    }

    #[test]
    fn test_timeframe_to_secs_values() {
        assert_eq!(timeframe_to_secs("5m"), 300);
        assert_eq!(timeframe_to_secs("15m"), 900);
        assert_eq!(timeframe_to_secs("1h"), 3600);
        assert_eq!(timeframe_to_secs("30m"), 1800);
        assert_eq!(timeframe_to_secs("2h"), 7200);
        // Unknown defaults to 900
        assert_eq!(timeframe_to_secs("garbage"), 900);
    }

    #[test]
    fn test_build_depth_levels_all_present() {
        let levels = build_depth_levels(Some(500.0), Some(120.0), Some(50.0));
        assert_eq!(levels.len(), 3);
        assert!((levels[0].price - 0.49).abs() < 1e-9);
        assert!((levels[0].cumulative_size - 500.0).abs() < 1e-9);
        assert!((levels[1].price - 0.50).abs() < 1e-9);
        assert!((levels[2].price - 0.51).abs() < 1e-9);
    }

    #[test]
    fn test_build_depth_levels_partial_null() {
        let levels = build_depth_levels(Some(500.0), None, Some(50.0));
        assert_eq!(levels.len(), 2);
        assert!((levels[0].price - 0.49).abs() < 1e-9);
        assert!((levels[1].price - 0.51).abs() < 1e-9);
    }

    #[test]
    fn test_build_depth_levels_zeros_excluded() {
        let levels = build_depth_levels(Some(500.0), Some(0.0), Some(0.0));
        assert_eq!(levels.len(), 1);
    }

    #[test]
    fn test_build_depth_levels_all_null() {
        let levels = build_depth_levels(None, None, None);
        assert!(levels.is_empty());
    }

    #[test]
    fn test_tick_to_side_state_conversion() {
        let tick = BookTick {
            market_id: "test".into(),
            side: Side::Yes,
            timestamp_ms: 1000,
            offset_ms: 0,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            depth: vec![PriceLevel { price: 0.49, cumulative_size: 500.0 }],
            total_bid_depth: 500.0,
            total_ask_depth: 200.0,
            reference_price: Some(66000.0),
            oracle_price: None,
        };

        let state = tick_to_side_state(&tick);
        assert_eq!(state.best_bid, Some(0.49));
        assert_eq!(state.best_ask, Some(0.51));
        assert_eq!(state.depth.len(), 1);
        assert!((state.total_bid_depth - 500.0).abs() < 1e-9);
    }

    #[test]
    fn test_ticks_to_snapshots_both_sides() {
        let ticks = vec![
            BookTick {
                market_id: "m1".into(),
                side: Side::No,
                timestamp_ms: 1000,
                offset_ms: 0,
                best_bid: Some(0.48),
                best_bid_size: Some(50.0),
                best_ask: Some(0.52),
                best_ask_size: Some(60.0),
                depth: vec![],
                total_bid_depth: 50.0,
                total_ask_depth: 60.0,
                reference_price: Some(66000.0),
                oracle_price: None,
            },
            BookTick {
                market_id: "m1".into(),
                side: Side::Yes,
                timestamp_ms: 1000,
                offset_ms: 0,
                best_bid: Some(0.49),
                best_bid_size: Some(100.0),
                best_ask: Some(0.51),
                best_ask_size: Some(200.0),
                depth: vec![],
                total_bid_depth: 500.0,
                total_ask_depth: 200.0,
                reference_price: Some(66000.0),
                oracle_price: Some(66010.0),
            },
        ];

        let snaps = ticks_to_snapshots("m1", &ticks);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].yes.best_bid, Some(0.49));
        assert_eq!(snaps[0].no.best_bid, Some(0.48));
        assert_eq!(snaps[0].reference_price, Some(66000.0));
        assert_eq!(snaps[0].oracle_price, Some(66010.0));
    }

    #[test]
    fn test_ticks_to_snapshots_carry_forward() {
        let ticks = vec![
            BookTick {
                market_id: "m1".into(),
                side: Side::No,
                timestamp_ms: 1000,
                offset_ms: 0,
                best_bid: Some(0.48),
                best_bid_size: Some(50.0),
                best_ask: Some(0.52),
                best_ask_size: Some(60.0),
                depth: vec![],
                total_bid_depth: 50.0,
                total_ask_depth: 60.0,
                reference_price: Some(66000.0),
                oracle_price: None,
            },
            BookTick {
                market_id: "m1".into(),
                side: Side::Yes,
                timestamp_ms: 1000,
                offset_ms: 0,
                best_bid: Some(0.49),
                best_bid_size: Some(100.0),
                best_ask: Some(0.51),
                best_ask_size: Some(200.0),
                depth: vec![],
                total_bid_depth: 500.0,
                total_ask_depth: 200.0,
                reference_price: Some(66000.0),
                oracle_price: None,
            },
            // Next offset: only YES side present.
            BookTick {
                market_id: "m1".into(),
                side: Side::Yes,
                timestamp_ms: 2000,
                offset_ms: 1000,
                best_bid: Some(0.50),
                best_bid_size: Some(110.0),
                best_ask: Some(0.51),
                best_ask_size: Some(210.0),
                depth: vec![],
                total_bid_depth: 510.0,
                total_ask_depth: 210.0,
                reference_price: Some(66100.0),
                oracle_price: None,
            },
        ];

        let snaps = ticks_to_snapshots("m1", &ticks);
        assert_eq!(snaps.len(), 2);
        // Second snapshot YES updated, NO carried forward.
        assert_eq!(snaps[1].yes.best_bid, Some(0.50));
        assert_eq!(snaps[1].no.best_bid, Some(0.48));
        assert_eq!(snaps[1].no.best_ask, Some(0.52));
    }

    #[test]
    fn test_ticks_to_snapshots_empty() {
        let snaps = ticks_to_snapshots("m1", &[]);
        assert!(snaps.is_empty());
    }

    #[test]
    fn test_ticks_to_snapshots_single_side_defaults() {
        let ticks = vec![BookTick {
            market_id: "m1".into(),
            side: Side::Yes,
            timestamp_ms: 1000,
            offset_ms: 0,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            depth: vec![],
            total_bid_depth: 500.0,
            total_ask_depth: 200.0,
            reference_price: Some(66000.0),
            oracle_price: None,
        }];

        let snaps = ticks_to_snapshots("m1", &ticks);
        assert_eq!(snaps.len(), 1);
        // NO side uses SideState::default().
        assert_eq!(snaps[0].no.best_bid, None);
        assert_eq!(snaps[0].no.best_ask, None);
        assert!((snaps[0].no.total_bid_depth).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // PolymarketStore integration tests (require real DB)
    // -----------------------------------------------------------------------

    #[test]
    fn test_polymarket_store_list_markets() {
        let store = match PolymarketStore::open_default() {
            Ok(s) => s,
            Err(_) => {
                eprintln!("skipping integration test: pm-spread-arb DB not found");
                return;
            }
        };

        let markets = store.list_markets().unwrap();
        assert!(!markets.is_empty(), "expected at least one market");

        let m = &markets[0];
        assert_eq!(m.platform, Platform::Polymarket);
        assert!(!m.id.is_empty());
        assert!(m.open_ts > 0);
        assert!(m.close_ts > m.open_ts);
        assert!(m.duration_secs == 300 || m.duration_secs == 900);
    }

    #[test]
    fn test_polymarket_store_load_snapshots() {
        let store = match PolymarketStore::open_default() {
            Ok(s) => s,
            Err(_) => {
                eprintln!("skipping integration test: pm-spread-arb DB not found");
                return;
            }
        };

        let markets = store.list_markets().unwrap();
        if markets.is_empty() {
            return;
        }

        let slug = &markets[0].id;
        let snaps = store.load_snapshots(slug).unwrap();
        assert!(!snaps.is_empty(), "expected snapshots for {}", slug);

        // Should be ordered by offset_ms.
        for pair in snaps.windows(2) {
            assert!(pair[0].offset_ms <= pair[1].offset_ms);
        }
        for s in &snaps {
            assert_eq!(s.market_id, *slug);
        }
    }

    #[test]
    fn test_polymarket_store_outcomes() {
        let store = match PolymarketStore::open_default() {
            Ok(s) => s,
            Err(_) => {
                eprintln!("skipping integration test: pm-spread-arb DB not found");
                return;
            }
        };

        let markets = store.list_markets_with_outcomes().unwrap();
        if markets.is_empty() {
            return;
        }

        let with_outcome = markets.iter().filter(|m| m.outcome.is_some()).count();
        assert!(with_outcome > 0, "expected some markets with outcomes");
    }
}
