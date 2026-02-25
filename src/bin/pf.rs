use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};

use phantomfill::data::polymarket::{import_from_capture_db, ticks_to_snapshots, PolymarketStore};
use phantomfill::data::{DataStore, MarketFilter, SqliteStore};
use phantomfill::fill::{DeLiseConfig, DeLiseFillModel};
use phantomfill::report::{MonteCarloSummary, Report};
use phantomfill::replay::{ReplayConfig, ReplayEngine};
use phantomfill::strategies::fade::{compute_fade_signals, FadeMomentum};
use phantomfill::strategies::scripted::RhaiStrategy;
use phantomfill::strategies::{create_strategy, is_known_strategy, list_strategies};

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

        /// Path to a custom .rhai strategy script (overrides --strategy)
        #[arg(long)]
        script: Option<PathBuf>,

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

        /// Random seed for reproducible results
        #[arg(long)]
        seed: Option<u64>,

        /// Number of Monte Carlo runs (default: 1 = single run)
        #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..))]
        runs: u32,

        /// Minimum streak length for fade strategy
        #[arg(long, default_value = "3")]
        min_streak: usize,

        /// Maximum streak length for fade strategy
        #[arg(long, default_value = "6")]
        max_streak: usize,

        /// Use PhantomFill native SQLite format (requires --db)
        #[arg(long)]
        native: bool,
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
            script,
            bid_price,
            shares,
            min_bps,
            min_streak,
            max_streak,
            db,
            csv,
            seed,
            runs,
            native,
        } => cmd_run(
            strategy, script, bid_price, shares, min_bps, min_streak, max_streak, db, csv, seed,
            runs as usize, native,
        ),
        Commands::Strategies => cmd_strategies(),
        Commands::Import {
            source,
            dest,
            asset,
        } => cmd_import(source, dest, asset),
    }
}

#[allow(clippy::too_many_arguments)]
fn cmd_run(
    strategy_name: String,
    script: Option<PathBuf>,
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    min_streak: usize,
    max_streak: usize,
    db_path: Option<String>,
    csv_path: Option<String>,
    seed: Option<u64>,
    runs: usize,
    native: bool,
) -> Result<()> {
    // If a script is provided, validate it can load; otherwise validate built-in strategy.
    let using_script = script.is_some();
    if let Some(ref path) = script {
        // Validate the script loads successfully (compile check).
        RhaiStrategy::from_file(path, shares, bid_price)
            .with_context(|| format!("failed to load script {}", path.display()))?;
    } else if !is_known_strategy(&strategy_name) {
        let names: Vec<&str> = list_strategies().iter().map(|(n, _)| *n).collect();
        bail!(
            "unknown strategy '{}'. available: {}",
            strategy_name,
            names.join(", ")
        );
    }

    if native {
        return cmd_run_native(
            strategy_name,
            script,
            bid_price,
            shares,
            min_bps,
            min_streak,
            max_streak,
            db_path,
            csv_path,
            seed,
            runs,
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

    let display_name = if let Some(ref path) = script {
        format!("script:{}", path.display())
    } else {
        strategy_name.clone()
    };

    println!(
        "Loaded {} markets. Running strategy '{}' (bid={}, shares={}, min_bps={})...",
        markets.len(),
        display_name,
        bid_price,
        shares,
        min_bps
    );

    let fill_model_name = "delise-3rule";

    // Build strategy factory (fade needs pre-computed signals).
    let fade_signals = if !using_script && strategy_name == "fade" {
        let signals = std::sync::Arc::new(compute_fade_signals(&markets, min_streak, max_streak));
        println!(
            "  Fade signals: {} of {} windows (streak {}..={})",
            signals.len(),
            markets.len(),
            min_streak,
            max_streak
        );
        Some(signals)
    } else {
        None
    };

    let make_strategy = |_sn: &str| -> Box<dyn phantomfill::strategies::Strategy> {
        if let Some(ref path) = script {
            Box::new(
                RhaiStrategy::from_file(path, shares, bid_price)
                    .expect("script already validated"),
            )
        } else if let Some(ref signals) = fade_signals {
            Box::new(FadeMomentum::new(bid_price, shares, signals.clone()))
        } else {
            create_strategy(_sn, bid_price, shares, min_bps).expect("strategy already validated")
        }
    };

    if runs <= 1 {
        let fill_model = Box::new(DeLiseFillModel::new(DeLiseConfig {
            seed,
            ..DeLiseConfig::default()
        }));

        let engine = ReplayEngine::new(
            fill_model,
            ReplayConfig {
                bid_price,
                shares,
            },
        );

        let results = engine.run_all(
            &markets,
            &|slug| store.load_snapshots(slug),
            &|| make_strategy(&strategy_name),
        );

        let report = Report::from_results(&results, &display_name, fill_model_name);
        report.print();

        if let Some(ref path) = csv_path {
            let csv_path_buf = PathBuf::from(path);
            Report::export_csv(&results, &csv_path_buf)
                .with_context(|| format!("failed to export CSV to {}", path))?;
            println!("Results exported to {}", path);
        }
    } else {
        let mut reports = Vec::new();
        for i in 0..runs {
            let run_seed = seed.map(|s| s + i as u64).unwrap_or_else(|| {
                use rand::Rng;
                rand::thread_rng().gen()
            });
            let fill_model = Box::new(DeLiseFillModel::new(DeLiseConfig {
                seed: Some(run_seed),
                ..DeLiseConfig::default()
            }));
            let engine = ReplayEngine::new(
                fill_model,
                ReplayConfig {
                    bid_price,
                    shares,
                },
            );
            let results = engine.run_all(
                &markets,
                &|slug| store.load_snapshots(slug),
                &|| make_strategy(&strategy_name),
            );

            if i == 0 {
                if let Some(ref path) = csv_path {
                    let csv_path_buf = PathBuf::from(path);
                    Report::export_csv(&results, &csv_path_buf)
                        .with_context(|| format!("failed to export CSV to {}", path))?;
                    println!("Results exported to {}", path);
                }
            }

            let report = Report::from_results(&results, &display_name, fill_model_name);
            reports.push(report);

            if (i + 1) % 10 == 0 || i + 1 == runs {
                println!("Monte Carlo run {}/{} complete", i + 1, runs);
            }
        }
        let summary = MonteCarloSummary::from_reports(reports, seed);
        summary.print();
    }

    Ok(())
}

/// Run backtest against PhantomFill native SQLite format (e.g. imported HF data).
#[allow(clippy::too_many_arguments)]
fn cmd_run_native(
    strategy_name: String,
    script: Option<PathBuf>,
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    min_streak: usize,
    max_streak: usize,
    db_path: Option<String>,
    csv_path: Option<String>,
    seed: Option<u64>,
    runs: usize,
) -> Result<()> {
    let db = db_path.as_deref().ok_or_else(|| {
        anyhow::anyhow!("--native mode requires --db path to a PhantomFill SQLite database")
    })?;

    let store = SqliteStore::open(&PathBuf::from(db))
        .with_context(|| format!("failed to open native database at {}", db))?;

    let markets = store
        .list_markets(&MarketFilter::default())
        .context("failed to list markets")?;

    if markets.is_empty() {
        bail!("no markets found in native database");
    }

    let display_name = if let Some(ref path) = script {
        format!("script:{}", path.display())
    } else {
        strategy_name.clone()
    };

    println!(
        "Loaded {} markets (native). Running strategy '{}' (bid={}, shares={}, min_bps={})...",
        markets.len(),
        display_name,
        bid_price,
        shares,
        min_bps
    );

    let fill_model_name = "delise-3rule";

    // Closure to load snapshots from the native store.
    let load_snapshots = |market_id: &str| -> anyhow::Result<Vec<_>> {
        let ticks = store.load_ticks(market_id)?;
        Ok(ticks_to_snapshots(market_id, &ticks))
    };

    // Build strategy factory (fade needs pre-computed signals).
    let using_script = script.is_some();
    let fade_signals = if !using_script && strategy_name == "fade" {
        let signals = std::sync::Arc::new(compute_fade_signals(&markets, min_streak, max_streak));
        println!(
            "  Fade signals: {} of {} windows (streak {}..={})",
            signals.len(),
            markets.len(),
            min_streak,
            max_streak
        );
        Some(signals)
    } else {
        None
    };

    let make_strategy = |_sn: &str| -> Box<dyn phantomfill::strategies::Strategy> {
        if let Some(ref path) = script {
            Box::new(
                RhaiStrategy::from_file(path, shares, bid_price)
                    .expect("script already validated"),
            )
        } else if let Some(ref signals) = fade_signals {
            Box::new(FadeMomentum::new(bid_price, shares, signals.clone()))
        } else {
            create_strategy(_sn, bid_price, shares, min_bps).expect("strategy already validated")
        }
    };

    if runs <= 1 {
        let fill_model = Box::new(DeLiseFillModel::new(DeLiseConfig {
            seed,
            ..DeLiseConfig::default()
        }));
        let engine = ReplayEngine::new(fill_model, ReplayConfig { bid_price, shares });

        let results = engine.run_all(&markets, &load_snapshots, &|| {
            make_strategy(&strategy_name)
        });

        let report = Report::from_results(&results, &display_name, fill_model_name);
        report.print();

        if let Some(ref path) = csv_path {
            let csv_path_buf = PathBuf::from(path);
            Report::export_csv(&results, &csv_path_buf)
                .with_context(|| format!("failed to export CSV to {}", path))?;
            println!("Results exported to {}", path);
        }
    } else {
        let mut reports = Vec::new();
        for i in 0..runs {
            let run_seed = seed.map(|s| s + i as u64).unwrap_or_else(|| {
                use rand::Rng;
                rand::thread_rng().gen()
            });
            let fill_model = Box::new(DeLiseFillModel::new(DeLiseConfig {
                seed: Some(run_seed),
                ..DeLiseConfig::default()
            }));
            let engine = ReplayEngine::new(fill_model, ReplayConfig { bid_price, shares });
            let results = engine.run_all(&markets, &load_snapshots, &|| {
                make_strategy(&strategy_name)
            });

            if i == 0 {
                if let Some(ref path) = csv_path {
                    let csv_path_buf = PathBuf::from(path);
                    Report::export_csv(&results, &csv_path_buf)
                        .with_context(|| format!("failed to export CSV to {}", path))?;
                    println!("Results exported to {}", path);
                }
            }

            let report = Report::from_results(&results, &display_name, fill_model_name);
            reports.push(report);

            if (i + 1) % 10 == 0 || i + 1 == runs {
                println!("Monte Carlo run {}/{} complete", i + 1, runs);
            }
        }
        let summary = MonteCarloSummary::from_reports(reports, seed);
        summary.print();
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
