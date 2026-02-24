use anyhow::Result;
use rusqlite::Connection;

use crate::types::{BookTick, Market, Outcome, Platform, PriceLevel, Side};

use super::schema;

/// Filter criteria for listing markets.
#[derive(Debug, Default)]
pub struct MarketFilter {
    pub platform: Option<Platform>,
    pub category: Option<String>,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
}

/// Abstraction over tick/market storage.
pub trait DataStore {
    fn init(&self) -> Result<()>;
    fn insert_market(&self, market: &Market) -> Result<()>;
    fn insert_ticks(&self, ticks: &[BookTick]) -> Result<()>;
    fn list_markets(&self, filter: &MarketFilter) -> Result<Vec<Market>>;
    fn load_ticks(&self, market_id: &str) -> Result<Vec<BookTick>>;
}

/// SQLite-backed implementation.
pub struct SqliteStore {
    conn: Connection,
}

impl SqliteStore {
    pub fn new(conn: Connection) -> Self {
        Self { conn }
    }

    /// Open a file-backed database.
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        Ok(Self { conn })
    }

    /// Open an in-memory database (useful for tests).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        Ok(Self { conn })
    }

    /// Borrow the underlying connection (for importers that need raw access).
    pub fn conn(&self) -> &Connection {
        &self.conn
    }
}

impl DataStore for SqliteStore {
    fn init(&self) -> Result<()> {
        self.conn.execute_batch(schema::CREATE_MARKETS)?;
        self.conn.execute_batch(schema::CREATE_TICKS)?;
        self.conn.execute_batch(schema::CREATE_DEPTH_LEVELS)?;
        self.conn.execute_batch(schema::CREATE_INDEXES)?;
        Ok(())
    }

    fn insert_market(&self, m: &Market) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO pf_markets
             (id, platform, description, category, open_ts, close_ts, duration_secs, outcome)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                m.id,
                m.platform.to_string(),
                m.description,
                m.category,
                m.open_ts,
                m.close_ts,
                m.duration_secs,
                m.outcome.as_ref().map(|o| o.label()),
            ],
        )?;
        Ok(())
    }

    fn insert_ticks(&self, ticks: &[BookTick]) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        {
            let mut tick_stmt = tx.prepare_cached(
                "INSERT INTO pf_ticks
                 (market_id, side, timestamp_ms, offset_ms,
                  best_bid, best_bid_size, best_ask, best_ask_size,
                  total_bid_depth, total_ask_depth, reference_price, oracle_price)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            )?;
            let mut depth_stmt = tx.prepare_cached(
                "INSERT INTO pf_depth_levels (tick_id, price, cumulative_size)
                 VALUES (?1, ?2, ?3)",
            )?;

            for t in ticks {
                tick_stmt.execute(rusqlite::params![
                    t.market_id,
                    t.side.label(),
                    t.timestamp_ms,
                    t.offset_ms,
                    t.best_bid,
                    t.best_bid_size,
                    t.best_ask,
                    t.best_ask_size,
                    t.total_bid_depth,
                    t.total_ask_depth,
                    t.reference_price,
                    t.oracle_price,
                ])?;

                if !t.depth.is_empty() {
                    let tick_id = tx.last_insert_rowid();
                    for lvl in &t.depth {
                        depth_stmt.execute(rusqlite::params![
                            tick_id,
                            lvl.price,
                            lvl.cumulative_size,
                        ])?;
                    }
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn list_markets(&self, filter: &MarketFilter) -> Result<Vec<Market>> {
        let mut sql = String::from("SELECT id, platform, description, category, open_ts, close_ts, duration_secs, outcome FROM pf_markets WHERE 1=1");
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(ref p) = filter.platform {
            sql.push_str(" AND platform = ?");
            params.push(Box::new(p.to_string()));
        }
        if let Some(ref c) = filter.category {
            sql.push_str(" AND category = ?");
            params.push(Box::new(c.clone()));
        }
        if let Some(ts) = filter.min_ts {
            sql.push_str(" AND open_ts >= ?");
            params.push(Box::new(ts));
        }
        if let Some(ts) = filter.max_ts {
            sql.push_str(" AND close_ts <= ?");
            params.push(Box::new(ts));
        }

        sql.push_str(" ORDER BY open_ts");

        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let platform_str: String = row.get(1)?;
            let outcome_str: Option<String> = row.get(7)?;
            Ok(Market {
                id: row.get(0)?,
                platform: match platform_str.as_str() {
                    "kalshi" => Platform::Kalshi,
                    _ => Platform::Polymarket,
                },
                description: row.get(2)?,
                category: row.get(3)?,
                open_ts: row.get(4)?,
                close_ts: row.get(5)?,
                duration_secs: row.get(6)?,
                outcome: outcome_str.map(|s| match s.as_str() {
                    "YES" => Outcome::Yes,
                    _ => Outcome::No,
                }),
            })
        })?;

        let mut markets = Vec::new();
        for r in rows {
            markets.push(r?);
        }
        Ok(markets)
    }

    fn load_ticks(&self, market_id: &str) -> Result<Vec<BookTick>> {
        // Load ticks
        let mut stmt = self.conn.prepare(
            "SELECT id, market_id, side, timestamp_ms, offset_ms,
                    best_bid, best_bid_size, best_ask, best_ask_size,
                    total_bid_depth, total_ask_depth, reference_price, oracle_price
             FROM pf_ticks WHERE market_id = ? ORDER BY offset_ms, side",
        )?;

        let tick_rows: Vec<(i64, BookTick)> = stmt
            .query_map([market_id], |row| {
                let side_str: String = row.get(2)?;
                Ok((
                    row.get::<_, i64>(0)?,
                    BookTick {
                        market_id: row.get(1)?,
                        side: if side_str == "YES" {
                            Side::Yes
                        } else {
                            Side::No
                        },
                        timestamp_ms: row.get(3)?,
                        offset_ms: row.get(4)?,
                        best_bid: row.get(5)?,
                        best_bid_size: row.get(6)?,
                        best_ask: row.get(7)?,
                        best_ask_size: row.get(8)?,
                        total_bid_depth: row.get(9)?,
                        total_ask_depth: row.get(10)?,
                        reference_price: row.get(11)?,
                        oracle_price: row.get(12)?,
                        depth: Vec::new(),
                    },
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        // Load depth levels for all tick IDs
        if tick_rows.is_empty() {
            return Ok(Vec::new());
        }

        let tick_ids: Vec<i64> = tick_rows.iter().map(|(id, _)| *id).collect();
        let placeholders: String = tick_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT tick_id, price, cumulative_size FROM pf_depth_levels WHERE tick_id IN ({}) ORDER BY tick_id, price",
            placeholders
        );
        let mut depth_stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> =
            tick_ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();

        let mut depth_map: std::collections::HashMap<i64, Vec<PriceLevel>> =
            std::collections::HashMap::new();
        let depth_rows = depth_stmt.query_map(param_refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                PriceLevel {
                    price: row.get(1)?,
                    cumulative_size: row.get(2)?,
                },
            ))
        })?;
        for r in depth_rows {
            let (tick_id, level) = r?;
            depth_map.entry(tick_id).or_default().push(level);
        }

        let ticks = tick_rows
            .into_iter()
            .map(|(id, mut tick)| {
                if let Some(levels) = depth_map.remove(&id) {
                    tick.depth = levels;
                }
                tick
            })
            .collect();

        Ok(ticks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BookTick, Market, Outcome, Platform, PriceLevel, Side};

    fn setup() -> SqliteStore {
        let store = SqliteStore::in_memory().unwrap();
        store.init().unwrap();
        store
    }

    fn sample_market(id: &str) -> Market {
        Market {
            id: id.to_string(),
            platform: Platform::Polymarket,
            description: format!("Test market {}", id),
            category: "btc".to_string(),
            open_ts: 1000,
            close_ts: 1300,
            duration_secs: 300,
            outcome: Some(Outcome::Yes),
        }
    }

    fn sample_tick(market_id: &str, side: Side, offset_ms: i64) -> BookTick {
        BookTick {
            market_id: market_id.to_string(),
            side,
            timestamp_ms: 1000_000 + offset_ms,
            offset_ms,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            depth: vec![
                PriceLevel { price: 0.49, cumulative_size: 500.0 },
                PriceLevel { price: 0.50, cumulative_size: 120.0 },
                PriceLevel { price: 0.51, cumulative_size: 50.0 },
            ],
            total_bid_depth: 500.0,
            total_ask_depth: 200.0,
            reference_price: Some(66000.0),
            oracle_price: Some(66010.0),
        }
    }

    #[test]
    fn test_insert_and_list_markets() {
        let store = setup();
        let m1 = sample_market("market-1");
        let m2 = Market {
            id: "market-2".to_string(),
            platform: Platform::Kalshi,
            category: "weather".to_string(),
            outcome: Some(Outcome::No),
            ..sample_market("market-2")
        };
        store.insert_market(&m1).unwrap();
        store.insert_market(&m2).unwrap();

        // List all
        let all = store.list_markets(&MarketFilter::default()).unwrap();
        assert_eq!(all.len(), 2);

        // Filter by platform
        let pm_only = store
            .list_markets(&MarketFilter {
                platform: Some(Platform::Polymarket),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(pm_only.len(), 1);
        assert_eq!(pm_only[0].id, "market-1");

        // Filter by category
        let weather = store
            .list_markets(&MarketFilter {
                category: Some("weather".to_string()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(weather.len(), 1);
        assert_eq!(weather[0].platform, Platform::Kalshi);
    }

    #[test]
    fn test_insert_and_load_ticks() {
        let store = setup();
        let m = sample_market("m1");
        store.insert_market(&m).unwrap();

        let ticks = vec![
            sample_tick("m1", Side::Yes, 0),
            sample_tick("m1", Side::No, 0),
            sample_tick("m1", Side::Yes, 1000),
        ];
        store.insert_ticks(&ticks).unwrap();

        let loaded = store.load_ticks("m1").unwrap();
        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded[0].offset_ms, 0);
        assert_eq!(loaded[2].offset_ms, 1000);
    }

    #[test]
    fn test_depth_levels_roundtrip() {
        let store = setup();
        let m = sample_market("d1");
        store.insert_market(&m).unwrap();

        let tick = sample_tick("d1", Side::Yes, 500);
        store.insert_ticks(&[tick]).unwrap();

        let loaded = store.load_ticks("d1").unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].depth.len(), 3);
        assert!((loaded[0].depth[0].price - 0.49).abs() < 1e-9);
        assert!((loaded[0].depth[0].cumulative_size - 500.0).abs() < 1e-9);
        assert!((loaded[0].depth[1].price - 0.50).abs() < 1e-9);
        assert!((loaded[0].depth[2].price - 0.51).abs() < 1e-9);
    }

    #[test]
    fn test_market_filter_by_timestamp() {
        let store = setup();
        store
            .insert_market(&Market {
                open_ts: 100,
                close_ts: 400,
                ..sample_market("early")
            })
            .unwrap();
        store
            .insert_market(&Market {
                open_ts: 500,
                close_ts: 800,
                ..sample_market("late")
            })
            .unwrap();

        let filtered = store
            .list_markets(&MarketFilter {
                min_ts: Some(400),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "late");

        let filtered = store
            .list_markets(&MarketFilter {
                max_ts: Some(500),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].id, "early");
    }

    #[test]
    fn test_empty_load() {
        let store = setup();
        let ticks = store.load_ticks("nonexistent").unwrap();
        assert!(ticks.is_empty());

        let markets = store.list_markets(&MarketFilter::default()).unwrap();
        assert!(markets.is_empty());
    }

    #[test]
    fn test_market_upsert() {
        let store = setup();
        let mut m = sample_market("u1");
        m.outcome = None;
        store.insert_market(&m).unwrap();

        // Update with outcome
        m.outcome = Some(Outcome::No);
        store.insert_market(&m).unwrap();

        let loaded = store.list_markets(&MarketFilter::default()).unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].outcome, Some(Outcome::No));
    }
}
