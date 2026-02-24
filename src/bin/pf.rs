use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use phantomfill::data::polymarket::{import_from_capture_db, PolymarketStore};
use phantomfill::data::{DataStore, SqliteStore};
use phantomfill::fill::{DeLiseConfig, DeLiseFillModel};
use phantomfill::report::Report;
use phantomfill::replay::{ReplayConfig, ReplayEngine};
use phantomfill::strategies::{create_strategy, list_strategies};

#[derive(Parser)]
#[command(name = "pf", about = "PhantomFill -- the honest prediction market backtester")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a backtest simulation
    Run {
        /// Strategy to simulate
        #[arg(short, long, default_value = "momentum")]
        strategy: String,

        /// Bid price
        #[arg(long, default_value = "0.49")]
        bid_price: f64,

        /// Shares per order
        #[arg(long, default_value = "10")]
        shares: f64,

        /// Minimum momentum (bps) for signal-based strategies
        #[arg(long, default_value = "5")]
        min_bps: f64,

        /// Path to source database (default: ~/.local/share/pm_trader/spread_arb.db)
        #[arg(long)]
        db: Option<String>,

        /// Export results to CSV
        #[arg(long)]
        csv: Option<String>,
    },

    /// List available strategies
    Strategies,

    /// Import data from capture database into PhantomFill format
    Import {
        /// Source database path
        #[arg(long)]
        source: Option<String>,

        /// Destination database path
        #[arg(long)]
        dest: String,

        /// Filter by asset (e.g. "btc")
        #[arg(long)]
        asset: Option<String>,
    },
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run {
            strategy,
            bid_price,
            shares,
            min_bps,
            db,
            csv,
        } => cmd_run(strategy, bid_price, shares, min_bps, db, csv),
        Commands::Strategies => cmd_strategies(),
        Commands::Import {
            source,
            dest,
            asset,
        } => cmd_import(source, dest, asset),
    }
}

fn cmd_run(
    strategy_name: String,
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    db_path: Option<String>,
    csv_path: Option<String>,
) -> Result<()> {
    // Validate strategy exists before loading data.
    if create_strategy(&strategy_name, bid_price, shares, min_bps).is_none() {
        let names: Vec<&str> = list_strategies().iter().map(|(n, _)| *n).collect();
        bail!(
            "unknown strategy '{}'. available: {}",
            strategy_name,
            names.join(", ")
        );
    }

    // Open data store.
    let store = match db_path {
        Some(ref p) => {
            let path = PathBuf::from(p);
            PolymarketStore::open(&path)
                .with_context(|| format!("failed to open database at {}", p))?
        }
        None => PolymarketStore::open_default().context("failed to open default database")?,
    };

    // Load markets with outcomes.
    let markets = store
        .list_markets_with_outcomes()
        .context("failed to list markets")?;

    if markets.is_empty() {
        bail!("no markets found in database");
    }

    println!(
        "Loaded {} markets. Running strategy '{}' (bid={}, shares={}, min_bps={})...",
        markets.len(),
        strategy_name,
        bid_price,
        shares,
        min_bps
    );

    // Create fill model and replay engine.
    let fill_model = Box::new(DeLiseFillModel::new(DeLiseConfig::default()));
    let fill_model_name = "delise-3rule".to_string();

    let engine = ReplayEngine::new(
        fill_model,
        ReplayConfig {
            bid_price,
            shares,
        },
    );

    // Run backtest.
    let results = engine.run_all(
        &markets,
        &|slug| store.load_snapshots(slug),
        &|| {
            create_strategy(&strategy_name, bid_price, shares, min_bps)
                .expect("strategy already validated")
        },
    );

    // Build and print report.
    let report = Report::from_results(&results, &strategy_name, &fill_model_name);
    report.print();

    // Export CSV if requested.
    if let Some(ref path) = csv_path {
        let csv_path = PathBuf::from(path);
        Report::export_csv(&results, &csv_path)
            .with_context(|| format!("failed to export CSV to {}", path))?;
        println!("Results exported to {}", path);
    }

    Ok(())
}

fn cmd_strategies() -> Result<()> {
    println!();
    println!("Available strategies:");
    println!();
    for (name, description) in list_strategies() {
        println!("  {:<16} {}", name, description);
    }
    println!();
    Ok(())
}

fn cmd_import(source: Option<String>, dest: String, asset: Option<String>) -> Result<()> {
    // Resolve source path.
    let source_path = match source {
        Some(ref p) => PathBuf::from(p),
        None => {
            let home = std::env::var("HOME").context("HOME not set")?;
            PathBuf::from(home).join(".local/share/pm_trader/spread_arb.db")
        }
    };

    println!("Importing from: {}", source_path.display());
    println!("Destination:    {}", dest);
    if let Some(ref a) = asset {
        println!("Asset filter:   {}", a);
    }

    // Open destination store and initialize schema.
    let dest_path = PathBuf::from(&dest);
    let store = SqliteStore::open(&dest_path)
        .with_context(|| format!("failed to open destination at {}", dest))?;
    store.init().context("failed to initialize destination schema")?;

    // Run import.
    let stats = import_from_capture_db(&source_path, &store, asset.as_deref())
        .context("import failed")?;

    println!();
    println!("Import complete:");
    println!("  Markets imported: {}", stats.markets_imported);
    println!("  Ticks imported:   {}", stats.ticks_imported);
    println!("  Markets skipped:  {}", stats.markets_skipped);
    println!();

    Ok(())
}
