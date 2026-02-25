//! Import adapter for the HuggingFace `trentmkelly/polymarket_crypto_derivatives` dataset.
//!
//! Reads NDJSON files with tick-by-tick Polymarket orderbook data and writes
//! them into PhantomFill's native SQLite format.

use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::types::{BookTick, Market, Outcome, Platform, PriceLevel, Side};

use super::store::DataStore;

// ---------------------------------------------------------------------------
// NDJSON row schema
// ---------------------------------------------------------------------------

/// A single row from the HuggingFace NDJSON dataset.
#[derive(Debug, Deserialize)]
pub struct HfRow {
    /// Unix timestamp in milliseconds.
    pub ts: i64,
    /// Progress through the window, 0.0 → 1.0.
    pub progress: f64,
    /// Row type: 1 = orderbook snapshot, 2 = trade.
    #[serde(rename = "type")]
    pub row_type: i32,
    /// 1 if this row is for the UP (YES) outcome.
    pub outcome_up: Option<i32>,
    /// 1 if this row is for the DOWN (NO) outcome.
    pub outcome_down: Option<i32>,
    pub best_bid: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_size: Option<f64>,
    /// Total bid-side depth across all levels.
    pub bid_size_total: Option<f64>,
    /// Total ask-side depth across all levels.
    pub ask_size_total: Option<f64>,
}

// ---------------------------------------------------------------------------
// Filename parsing
// ---------------------------------------------------------------------------

/// Components extracted from an NDJSON filename.
#[derive(Debug, Clone)]
pub struct ParsedFilename {
    /// PhantomFill market id, e.g. `"hf-btc15m-42"`.
    pub market_id: String,
    /// Window open time (Unix seconds, UTC).
    pub open_ts: i64,
    /// Coin symbol, e.g. `"btc"`.
    pub coin: String,
    /// Timeframe label, e.g. `"15m"`.
    pub timeframe: String,
    /// Window duration in seconds.
    pub duration_secs: i64,
}

/// Parse a filename like `btc15m_market42_2026-01-15_10-30-00.ndjson`.
pub fn parse_filename(name: &str) -> Result<ParsedFilename> {
    let stem = name
        .strip_suffix(".ndjson")
        .or_else(|| name.strip_suffix(".jsonl"))
        .unwrap_or(name);

    // Split on "_market" to separate prefix from rest.
    let parts: Vec<&str> = stem.splitn(2, "_market").collect();
    if parts.len() != 2 {
        bail!(
            "filename doesn't match '{{coin}}{{tf}}_market{{id}}_{{date}}_{{time}}': {}",
            name
        );
    }

    let prefix = parts[0]; // e.g. "btc15m"
    let rest = parts[1]; // e.g. "42_2026-01-15_10-30-00"

    let (coin, timeframe, duration_secs) = if let Some(stripped) = prefix.strip_suffix("15m") {
        (stripped, "15m", 900i64)
    } else if let Some(stripped) = prefix.strip_suffix("5m") {
        (stripped, "5m", 300i64)
    } else if let Some(stripped) = prefix.strip_suffix("1h") {
        (stripped, "1h", 3600i64)
    } else {
        bail!("unknown timeframe in prefix: {}", prefix);
    };

    // rest = "42_2026-01-15_10-30-00"
    let underscore_parts: Vec<&str> = rest.splitn(3, '_').collect();
    if underscore_parts.len() != 3 {
        bail!(
            "cannot parse market id and datetime from: market{}",
            rest
        );
    }

    let market_num = underscore_parts[0]; // "42"
    let date_part = underscore_parts[1]; // "2026-01-15"
    let time_part = underscore_parts[2]; // "10-30-00"

    // Convert "2026-01-15" + "10-30-00" → NaiveDateTime → Unix seconds.
    let datetime_str = format!("{}T{}", date_part, time_part.replace('-', ":"));
    let dt = chrono::NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%dT%H:%M:%S")
        .with_context(|| {
            format!(
                "failed to parse datetime from '{}' + '{}'",
                date_part, time_part
            )
        })?;
    let open_ts = dt.and_utc().timestamp();

    let market_id = format!("hf-{}{}-{}", coin, timeframe, market_num);

    Ok(ParsedFilename {
        market_id,
        open_ts,
        coin: coin.to_string(),
        timeframe: timeframe.to_string(),
        duration_secs,
    })
}

// ---------------------------------------------------------------------------
// Row mapping
// ---------------------------------------------------------------------------

/// Convert one HF dataset row into a [`BookTick`].
///
/// Returns `None` for trade rows (type != 1) or rows without a clear side.
pub fn map_row(row: &HfRow, market_id: &str, duration_secs: i64) -> Option<BookTick> {
    if row.row_type != 1 {
        return None;
    }

    let side = if row.outcome_up == Some(1) {
        Side::Yes
    } else if row.outcome_down == Some(1) {
        Side::No
    } else {
        return None;
    };

    let duration_ms = duration_secs * 1000;
    let offset_ms = (row.progress * duration_ms as f64).round() as i64;

    let total_bid_depth = row.bid_size_total.unwrap_or(0.0);
    let total_ask_depth = row.ask_size_total.unwrap_or(0.0);

    // Conservative depth approximation: concentrate all bid depth at best_bid.
    // This makes the fill model harder to fill (safer than optimistic).
    let depth = match row.best_bid {
        Some(price) if total_bid_depth > 0.0 => {
            vec![PriceLevel {
                price,
                cumulative_size: total_bid_depth,
            }]
        }
        _ => vec![],
    };

    Some(BookTick {
        market_id: market_id.to_string(),
        side,
        timestamp_ms: row.ts,
        offset_ms,
        best_bid: row.best_bid,
        best_bid_size: row.best_bid_size,
        best_ask: row.best_ask,
        best_ask_size: row.best_ask_size,
        depth,
        total_bid_depth,
        total_ask_depth,
        reference_price: None,
        oracle_price: None,
    })
}

// ---------------------------------------------------------------------------
// Binance klines (oracle resolution)
// ---------------------------------------------------------------------------

/// Fetch Binance 15m klines for a time range.
///
/// Returns a map from kline open time (ms) → (open_price, close_price).
/// Paginates automatically if the range exceeds 1000 candles.
pub fn fetch_binance_klines(
    symbol: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<HashMap<i64, (f64, f64)>> {
    let mut klines = HashMap::new();
    let mut current_start = start_ms;

    loop {
        let url = format!(
            "https://api.binance.com/api/v3/klines?symbol={}&interval=15m&startTime={}&endTime={}&limit=1000",
            symbol, current_start, end_ms
        );

        let body: String = ureq::get(&url)
            .call()
            .with_context(|| format!("Binance API request failed for {}", symbol))?
            .into_string()
            .context("failed to read Binance response body")?;

        let candles: Vec<Vec<serde_json::Value>> =
            serde_json::from_str(&body).context("failed to parse Binance klines JSON")?;

        if candles.is_empty() {
            break;
        }

        for candle in &candles {
            if candle.len() < 5 {
                continue;
            }
            let open_time = candle[0].as_i64().unwrap_or(0);
            let open: f64 = candle[1]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            let close: f64 = candle[4]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0);
            klines.insert(open_time, (open, close));
        }

        let last_open = candles.last().and_then(|c| c[0].as_i64()).unwrap_or(end_ms);
        if last_open >= end_ms || candles.len() < 1000 {
            break;
        }
        current_start = last_open + 1;
    }

    info!("fetched {} Binance klines for {}", klines.len(), symbol);
    Ok(klines)
}

/// Determine the outcome of a window from Binance kline data.
///
/// Looks up the kline whose open time matches `open_ts_secs * 1000`.
/// Returns `Outcome::Yes` if close > open (price went up), else `Outcome::No`.
pub fn determine_outcome(
    klines: &HashMap<i64, (f64, f64)>,
    open_ts_secs: i64,
) -> Option<Outcome> {
    let open_ts_ms = open_ts_secs * 1000;
    klines.get(&open_ts_ms).map(|(open, close)| {
        if close > open {
            Outcome::Yes
        } else {
            Outcome::No
        }
    })
}

// ---------------------------------------------------------------------------
// Import pipeline
// ---------------------------------------------------------------------------

/// Statistics from an HF import run.
#[derive(Debug, Default)]
pub struct HfImportStats {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub markets_imported: usize,
    pub ticks_imported: usize,
    pub rows_filtered: usize,
}

/// Import a single NDJSON file into the destination store.
///
/// Streams line-by-line and flushes every 10K ticks to keep memory bounded.
pub fn import_single_file(
    path: &Path,
    parsed: &ParsedFilename,
    dest: &dyn DataStore,
    outcome: Option<Outcome>,
) -> Result<(usize, usize)> {
    let file =
        fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let reader = BufReader::new(file);

    let market = Market {
        id: parsed.market_id.clone(),
        platform: Platform::Polymarket,
        description: format!(
            "{} {} window at {}",
            parsed.coin.to_uppercase(),
            parsed.timeframe,
            parsed.open_ts
        ),
        category: parsed.coin.clone(),
        open_ts: parsed.open_ts,
        close_ts: parsed.open_ts + parsed.duration_secs,
        duration_secs: parsed.duration_secs,
        outcome,
    };
    dest.insert_market(&market)?;

    let mut ticks = Vec::with_capacity(10_000);
    let mut imported = 0usize;
    let mut filtered = 0usize;

    for (line_num, line) in reader.lines().enumerate() {
        let line = line.with_context(|| {
            format!("I/O error at line {} of {}", line_num + 1, path.display())
        })?;
        if line.is_empty() {
            continue;
        }

        let row: HfRow = serde_json::from_str(&line).with_context(|| {
            format!(
                "JSON parse error at line {} of {}",
                line_num + 1,
                path.display()
            )
        })?;

        match map_row(&row, &parsed.market_id, parsed.duration_secs) {
            Some(tick) => {
                ticks.push(tick);
                imported += 1;
            }
            None => {
                filtered += 1;
            }
        }

        if ticks.len() >= 10_000 {
            dest.insert_ticks(&ticks)?;
            ticks.clear();
        }
    }

    if !ticks.is_empty() {
        dest.insert_ticks(&ticks)?;
    }

    debug!(
        market_id = %parsed.market_id,
        imported,
        filtered,
        "imported file"
    );

    Ok((imported, filtered))
}

/// Recursively collect all `.ndjson` / `.jsonl` files under `dir`.
fn collect_ndjson_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_ndjson_recursive(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_ndjson_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        fs::read_dir(dir).with_context(|| format!("failed to read dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_ndjson_recursive(&path, files)?;
        } else if path
            .extension()
            .is_some_and(|ext| ext == "ndjson" || ext == "jsonl")
        {
            files.push(path);
        }
    }
    Ok(())
}

/// Import all NDJSON files from a directory into the destination store.
pub fn import_hf_directory(
    dir: &Path,
    dest: &dyn DataStore,
    klines: &HashMap<i64, (f64, f64)>,
    filter_coin: Option<&str>,
    limit: Option<usize>,
) -> Result<HfImportStats> {
    let mut stats = HfImportStats::default();

    let mut entries = collect_ndjson_files(dir)?;
    if let Some(max) = limit {
        entries.truncate(max);
    }

    info!("found {} NDJSON files in {}", entries.len(), dir.display());

    for (i, path) in entries.iter().enumerate() {
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        let parsed = match parse_filename(filename) {
            Ok(p) => p,
            Err(e) => {
                warn!("skipping {}: {}", filename, e);
                stats.files_skipped += 1;
                continue;
            }
        };

        if let Some(coin) = filter_coin {
            if parsed.coin != coin {
                stats.files_skipped += 1;
                continue;
            }
        }

        let outcome = determine_outcome(klines, parsed.open_ts);

        match import_single_file(path, &parsed, dest, outcome) {
            Ok((imported, filtered)) => {
                stats.ticks_imported += imported;
                stats.rows_filtered += filtered;
                stats.markets_imported += 1;
                stats.files_processed += 1;
            }
            Err(e) => {
                warn!("error importing {}: {}", filename, e);
                stats.files_skipped += 1;
            }
        }

        if (i + 1) % 100 == 0 || i + 1 == entries.len() {
            info!(
                "progress: {}/{} files, {} markets, {} ticks",
                i + 1,
                entries.len(),
                stats.markets_imported,
                stats.ticks_imported
            );
        }
    }

    Ok(stats)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::store::SqliteStore;
    use std::io::Write;
    use tempfile::TempDir;

    // -- parse_filename -------------------------------------------------------

    #[test]
    fn test_parse_filename_btc15m() {
        let p = parse_filename("btc15m_market42_2026-01-15_10-30-00.ndjson").unwrap();
        assert_eq!(p.market_id, "hf-btc15m-42");
        assert_eq!(p.coin, "btc");
        assert_eq!(p.timeframe, "15m");
        assert_eq!(p.duration_secs, 900);
        // 2026-01-15 10:30:00 UTC
        let expected = chrono::NaiveDateTime::parse_from_str("2026-01-15T10:30:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp();
        assert_eq!(p.open_ts, expected);
    }

    #[test]
    fn test_parse_filename_btc5m() {
        let p = parse_filename("btc5m_market7_2026-01-20_00-00-00.ndjson").unwrap();
        assert_eq!(p.market_id, "hf-btc5m-7");
        assert_eq!(p.coin, "btc");
        assert_eq!(p.timeframe, "5m");
        assert_eq!(p.duration_secs, 300);
    }

    #[test]
    fn test_parse_filename_eth1h() {
        let p = parse_filename("eth1h_market1_2026-01-20_12-00-00.ndjson").unwrap();
        assert_eq!(p.market_id, "hf-eth1h-1");
        assert_eq!(p.coin, "eth");
        assert_eq!(p.timeframe, "1h");
        assert_eq!(p.duration_secs, 3600);
    }

    #[test]
    fn test_parse_filename_jsonl_extension() {
        let p = parse_filename("btc15m_market1_2026-01-15_10-30-00.jsonl").unwrap();
        assert_eq!(p.market_id, "hf-btc15m-1");
    }

    #[test]
    fn test_parse_filename_invalid_no_market() {
        assert!(parse_filename("btc15m_2026-01-15.ndjson").is_err());
    }

    #[test]
    fn test_parse_filename_invalid_unknown_timeframe() {
        assert!(parse_filename("btc3h_market1_2026-01-15_10-30-00.ndjson").is_err());
    }

    #[test]
    fn test_parse_filename_invalid_bad_datetime() {
        assert!(parse_filename("btc15m_market1_not-a-date_10-30-00.ndjson").is_err());
    }

    // -- map_row --------------------------------------------------------------

    #[test]
    fn test_map_row_orderbook_yes() {
        let row = HfRow {
            ts: 1705315800000,
            progress: 0.5,
            row_type: 1,
            outcome_up: Some(1),
            outcome_down: None,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            bid_size_total: Some(500.0),
            ask_size_total: Some(300.0),
        };

        let tick = map_row(&row, "hf-btc15m-1", 900).unwrap();
        assert_eq!(tick.side, Side::Yes);
        assert_eq!(tick.timestamp_ms, 1705315800000);
        assert_eq!(tick.offset_ms, 450_000); // 0.5 * 900_000
        assert_eq!(tick.best_bid, Some(0.49));
        assert_eq!(tick.best_ask, Some(0.51));
        assert!((tick.total_bid_depth - 500.0).abs() < 1e-9);
        assert_eq!(tick.depth.len(), 1);
        assert!((tick.depth[0].price - 0.49).abs() < 1e-9);
        assert!((tick.depth[0].cumulative_size - 500.0).abs() < 1e-9);
    }

    #[test]
    fn test_map_row_orderbook_no() {
        let row = HfRow {
            ts: 1705315800000,
            progress: 0.0,
            row_type: 1,
            outcome_up: None,
            outcome_down: Some(1),
            best_bid: Some(0.48),
            best_bid_size: Some(50.0),
            best_ask: Some(0.52),
            best_ask_size: Some(60.0),
            bid_size_total: Some(200.0),
            ask_size_total: Some(100.0),
        };

        let tick = map_row(&row, "hf-btc15m-1", 900).unwrap();
        assert_eq!(tick.side, Side::No);
        assert_eq!(tick.offset_ms, 0);
    }

    #[test]
    fn test_map_row_trade_filtered() {
        let row = HfRow {
            ts: 1705315800000,
            progress: 0.5,
            row_type: 2, // trade
            outcome_up: Some(1),
            outcome_down: None,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            bid_size_total: Some(500.0),
            ask_size_total: Some(300.0),
        };

        assert!(map_row(&row, "hf-btc15m-1", 900).is_none());
    }

    #[test]
    fn test_map_row_no_side() {
        let row = HfRow {
            ts: 1705315800000,
            progress: 0.5,
            row_type: 1,
            outcome_up: None,
            outcome_down: None,
            best_bid: Some(0.49),
            best_bid_size: None,
            best_ask: Some(0.51),
            best_ask_size: None,
            bid_size_total: None,
            ask_size_total: None,
        };

        assert!(map_row(&row, "hf-btc15m-1", 900).is_none());
    }

    #[test]
    fn test_map_row_no_bid_no_depth() {
        let row = HfRow {
            ts: 1705315800000,
            progress: 0.1,
            row_type: 1,
            outcome_up: Some(1),
            outcome_down: None,
            best_bid: None,
            best_bid_size: None,
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            bid_size_total: Some(0.0),
            ask_size_total: Some(300.0),
        };

        let tick = map_row(&row, "hf-btc15m-1", 900).unwrap();
        assert!(tick.depth.is_empty());
    }

    #[test]
    fn test_map_row_5m_offset() {
        let row = HfRow {
            ts: 1000,
            progress: 1.0,
            row_type: 1,
            outcome_up: Some(1),
            outcome_down: None,
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(200.0),
            bid_size_total: Some(500.0),
            ask_size_total: Some(300.0),
        };

        let tick = map_row(&row, "hf-btc5m-1", 300).unwrap();
        assert_eq!(tick.offset_ms, 300_000); // 1.0 * 300_000
    }

    // -- determine_outcome ----------------------------------------------------

    #[test]
    fn test_determine_outcome_up() {
        let mut klines = HashMap::new();
        klines.insert(1705315800000i64, (100000.0, 100100.0));
        let outcome = determine_outcome(&klines, 1705315800);
        assert_eq!(outcome, Some(Outcome::Yes));
    }

    #[test]
    fn test_determine_outcome_down() {
        let mut klines = HashMap::new();
        klines.insert(1705315800000i64, (100100.0, 100000.0));
        let outcome = determine_outcome(&klines, 1705315800);
        assert_eq!(outcome, Some(Outcome::No));
    }

    #[test]
    fn test_determine_outcome_flat() {
        let mut klines = HashMap::new();
        klines.insert(1705315800000i64, (100000.0, 100000.0));
        // close == open → No (not strictly up)
        let outcome = determine_outcome(&klines, 1705315800);
        assert_eq!(outcome, Some(Outcome::No));
    }

    #[test]
    fn test_determine_outcome_missing() {
        let klines = HashMap::new();
        assert_eq!(determine_outcome(&klines, 1705315800), None);
    }

    // -- import pipeline (end-to-end with temp files) -------------------------

    fn make_ndjson_line(progress: f64, outcome_up: bool, best_bid: f64) -> String {
        let ts = 1705315800000i64 + (progress * 900_000.0) as i64;
        format!(
            r#"{{"ts":{},"progress":{},"type":1,"outcome_up":{},"outcome_down":{},"best_bid":{},"best_bid_size":100.0,"best_ask":{},"best_ask_size":200.0,"bid_size_total":500.0,"ask_size_total":300.0}}"#,
            ts,
            progress,
            if outcome_up { 1 } else { 0 },
            if outcome_up { 0 } else { 1 },
            best_bid,
            best_bid + 0.02,
        )
    }

    fn make_trade_line() -> String {
        r#"{"ts":1705315800000,"progress":0.5,"type":2,"outcome_up":1,"outcome_down":0,"best_bid":0.49,"best_bid_size":100.0,"best_ask":0.51,"best_ask_size":200.0,"bid_size_total":500.0,"ask_size_total":300.0}"#.to_string()
    }

    fn write_ndjson_file(dir: &Path, filename: &str, lines: &[String]) {
        let path = dir.join(filename);
        let mut f = fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
    }

    #[test]
    fn test_import_single_file_basic() {
        let tmp = TempDir::new().unwrap();
        let lines: Vec<String> = (0..10)
            .flat_map(|i| {
                let p = i as f64 / 9.0;
                vec![
                    make_ndjson_line(p, true, 0.49),  // YES
                    make_ndjson_line(p, false, 0.48),  // NO
                ]
            })
            .collect();
        // Add 2 trade lines that should be filtered.
        let mut all_lines = lines;
        all_lines.push(make_trade_line());
        all_lines.push(make_trade_line());

        write_ndjson_file(tmp.path(), "btc15m_market1_2026-01-15_10-30-00.ndjson", &all_lines);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let parsed = parse_filename("btc15m_market1_2026-01-15_10-30-00.ndjson").unwrap();
        let (imported, filtered) = import_single_file(
            &tmp.path().join("btc15m_market1_2026-01-15_10-30-00.ndjson"),
            &parsed,
            &dest,
            Some(Outcome::Yes),
        )
        .unwrap();

        assert_eq!(imported, 20); // 10 offsets * 2 sides
        assert_eq!(filtered, 2); // 2 trade rows

        let markets = dest.list_markets(&Default::default()).unwrap();
        assert_eq!(markets.len(), 1);
        assert_eq!(markets[0].id, "hf-btc15m-1");
        assert_eq!(markets[0].outcome, Some(Outcome::Yes));
        assert_eq!(markets[0].duration_secs, 900);

        let ticks = dest.load_ticks("hf-btc15m-1").unwrap();
        assert_eq!(ticks.len(), 20);
    }

    #[test]
    fn test_import_directory_multiple_files() {
        let tmp = TempDir::new().unwrap();

        for i in 1..=3 {
            let lines: Vec<String> = (0..5)
                .flat_map(|j| {
                    let p = j as f64 / 4.0;
                    vec![
                        make_ndjson_line(p, true, 0.49),
                        make_ndjson_line(p, false, 0.48),
                    ]
                })
                .collect();
            let filename = format!("btc15m_market{}_2026-01-15_10-{:02}-00.ndjson", i, i * 15);
            write_ndjson_file(tmp.path(), &filename, &lines);
        }

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let klines = HashMap::new(); // No oracle → outcomes will be None
        let stats =
            import_hf_directory(tmp.path(), &dest, &klines, None, None).unwrap();

        assert_eq!(stats.files_processed, 3);
        assert_eq!(stats.markets_imported, 3);
        assert_eq!(stats.ticks_imported, 30); // 3 files * 5 offsets * 2 sides
    }

    #[test]
    fn test_import_directory_coin_filter() {
        let tmp = TempDir::new().unwrap();

        let lines: Vec<String> = vec![make_ndjson_line(0.0, true, 0.49)];
        write_ndjson_file(tmp.path(), "btc15m_market1_2026-01-15_10-30-00.ndjson", &lines);
        write_ndjson_file(tmp.path(), "eth15m_market2_2026-01-15_10-30-00.ndjson", &lines);

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let klines = HashMap::new();
        let stats =
            import_hf_directory(tmp.path(), &dest, &klines, Some("btc"), None).unwrap();

        assert_eq!(stats.markets_imported, 1);
        assert_eq!(stats.files_skipped, 1); // eth file skipped
    }

    #[test]
    fn test_import_directory_limit() {
        let tmp = TempDir::new().unwrap();

        let lines: Vec<String> = vec![make_ndjson_line(0.0, true, 0.49)];
        for i in 1..=5 {
            let filename = format!("btc15m_market{}_2026-01-15_10-{:02}-00.ndjson", i, i * 15);
            write_ndjson_file(tmp.path(), &filename, &lines);
        }

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let klines = HashMap::new();
        let stats =
            import_hf_directory(tmp.path(), &dest, &klines, None, Some(2)).unwrap();

        assert_eq!(stats.markets_imported, 2);
    }

    #[test]
    fn test_import_skips_bad_filenames() {
        let tmp = TempDir::new().unwrap();

        let lines: Vec<String> = vec![make_ndjson_line(0.0, true, 0.49)];
        write_ndjson_file(tmp.path(), "btc15m_market1_2026-01-15_10-30-00.ndjson", &lines);
        write_ndjson_file(tmp.path(), "README.ndjson", &lines); // bad filename

        let dest = SqliteStore::in_memory().unwrap();
        dest.init().unwrap();

        let klines = HashMap::new();
        let stats =
            import_hf_directory(tmp.path(), &dest, &klines, None, None).unwrap();

        assert_eq!(stats.markets_imported, 1);
        assert_eq!(stats.files_skipped, 1);
    }
}
