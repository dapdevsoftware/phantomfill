use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

use phantomfill::data::huggingface::{fetch_binance_klines, import_hf_directory, parse_filename};
use phantomfill::data::{DataStore, SqliteStore};

#[derive(Parser)]
#[command(
    name = "pf-hf-import",
    about = "Import HuggingFace polymarket_crypto_derivatives dataset into PhantomFill"
)]
struct Cli {
    /// Directory containing NDJSON files
    #[arg(long)]
    dir: String,

    /// Destination database path
    #[arg(long)]
    dest: String,

    /// Filter by coin (e.g. "btc")
    #[arg(long)]
    coin: Option<String>,

    /// Binance symbol for oracle prices (default: BTCUSDT)
    #[arg(long, default_value = "BTCUSDT")]
    symbol: String,

    /// Skip fetching Binance klines (outcomes will be None)
    #[arg(long)]
    no_oracle: bool,

    /// Limit number of files to import
    #[arg(long)]
    limit: Option<usize>,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    let dir = PathBuf::from(&cli.dir);
    let dest_path = PathBuf::from(&cli.dest);

    println!("HuggingFace Dataset Import");
    println!("  Source:      {}", dir.display());
    println!("  Destination: {}", dest_path.display());
    if let Some(ref coin) = cli.coin {
        println!("  Coin filter: {}", coin);
    }
    println!();

    // Fetch Binance klines for outcome resolution.
    let klines = if cli.no_oracle {
        println!("  Skipping Binance oracle fetch (--no-oracle)");
        std::collections::HashMap::new()
    } else {
        println!("  Scanning files for date range...");
        let (start_ms, end_ms) = scan_date_range(&dir, cli.coin.as_deref())?;
        println!(
            "  Fetching Binance {} klines ({} to {})...",
            cli.symbol, start_ms, end_ms
        );
        let klines = fetch_binance_klines(&cli.symbol, start_ms, end_ms)
            .context("failed to fetch Binance klines")?;
        println!("  Got {} klines", klines.len());
        klines
    };
    println!();

    // Open destination store and initialize schema.
    let store = SqliteStore::open(&dest_path)
        .with_context(|| format!("failed to open destination at {}", cli.dest))?;
    store.init().context("failed to initialize schema")?;

    // Run import.
    let stats = import_hf_directory(&dir, &store, &klines, cli.coin.as_deref(), cli.limit)
        .context("import failed")?;

    println!();
    println!("Import complete:");
    println!("  Files processed:  {}", stats.files_processed);
    println!("  Files skipped:    {}", stats.files_skipped);
    println!("  Markets imported: {}", stats.markets_imported);
    println!("  Ticks imported:   {}", stats.ticks_imported);
    println!("  Rows filtered:    {}", stats.rows_filtered);
    println!();

    Ok(())
}

/// Scan the directory for NDJSON files and determine the min/max timestamps
/// for the Binance kline fetch.
fn scan_date_range(dir: &PathBuf, coin_filter: Option<&str>) -> Result<(i64, i64)> {
    let mut min_ts: Option<i64> = None;
    let mut max_ts: Option<i64> = None;

    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read dir {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

        if let Ok(parsed) = parse_filename(filename) {
            if let Some(coin) = coin_filter {
                if parsed.coin != coin {
                    continue;
                }
            }
            let ts = parsed.open_ts;
            min_ts = Some(min_ts.map_or(ts, |m: i64| m.min(ts)));
            max_ts = Some(max_ts.map_or(
                ts + parsed.duration_secs,
                |m: i64| m.max(ts + parsed.duration_secs),
            ));
        }
    }

    match (min_ts, max_ts) {
        (Some(min), Some(max)) => Ok((min * 1000, max * 1000)),
        _ => anyhow::bail!("no valid NDJSON files found in {}", dir.display()),
    }
}
