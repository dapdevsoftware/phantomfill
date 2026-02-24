use std::path::Path;

use anyhow::{Context, Result};

use crate::types::WindowResult;

/// Summary report computed from a backtest run.
#[derive(Debug, Clone)]
pub struct Report {
    pub strategy_name: String,
    pub fill_model_name: String,

    // Counts
    pub total_windows: usize,
    pub trades_taken: usize,
    pub fills: usize,
    pub correct: usize,
    pub skipped: usize,

    // Rates
    pub fill_rate: f64,
    pub naive_win_rate: f64,
    pub realistic_win_rate: f64,

    // PnL
    pub naive_total_pnl: f64,
    pub realistic_total_pnl: f64,
    pub phantom_fill_gap: f64,
    pub avg_naive_pnl: f64,
    pub avg_realistic_pnl: f64,

    // Queue stats
    pub avg_queue_ahead: f64,
    pub avg_fill_time_ms: f64,
}

impl Report {
    /// Build a report from backtest results.
    pub fn from_results(
        results: &[WindowResult],
        strategy_name: &str,
        fill_model_name: &str,
    ) -> Self {
        let total_windows = results.len();

        // A trade was taken if bid_side is set (strategy placed an order).
        let traded: Vec<&WindowResult> = results.iter().filter(|r| r.bid_side.is_some()).collect();
        let trades_taken = traded.len();
        let skipped = total_windows - trades_taken;

        let fills = traded.iter().filter(|r| r.filled).count();
        // "correct" in naive sense: predicted the winner regardless of fill.
        let naive_correct = traded.iter().filter(|r| r.correct).count();
        // "correct" in realistic sense: filled AND correct.
        let realistic_correct = traded.iter().filter(|r| r.filled && r.correct).count();

        let fill_rate = if trades_taken > 0 {
            fills as f64 / trades_taken as f64
        } else {
            0.0
        };
        let naive_win_rate = if trades_taken > 0 {
            naive_correct as f64 / trades_taken as f64
        } else {
            0.0
        };
        let realistic_win_rate = if fills > 0 {
            realistic_correct as f64 / fills as f64
        } else {
            0.0
        };

        // PnL sums over traded windows only.
        let naive_total_pnl: f64 = traded.iter().map(|r| r.naive_pnl).sum();
        let realistic_total_pnl: f64 = traded.iter().map(|r| r.realistic_pnl).sum();
        let phantom_fill_gap = naive_total_pnl - realistic_total_pnl;
        let avg_naive_pnl = if trades_taken > 0 {
            naive_total_pnl / trades_taken as f64
        } else {
            0.0
        };
        let avg_realistic_pnl = if trades_taken > 0 {
            realistic_total_pnl / trades_taken as f64
        } else {
            0.0
        };

        // Queue stats over traded windows.
        let avg_queue_ahead = if trades_taken > 0 {
            traded.iter().map(|r| r.queue_ahead_at_place).sum::<f64>() / trades_taken as f64
        } else {
            0.0
        };

        let fill_times: Vec<f64> = traded
            .iter()
            .filter_map(|r| r.fill_time_ms.map(|ms| ms as f64))
            .collect();
        let avg_fill_time_ms = if !fill_times.is_empty() {
            fill_times.iter().sum::<f64>() / fill_times.len() as f64
        } else {
            0.0
        };

        Self {
            strategy_name: strategy_name.to_string(),
            fill_model_name: fill_model_name.to_string(),
            total_windows,
            trades_taken,
            fills,
            correct: realistic_correct,
            skipped,
            fill_rate,
            naive_win_rate,
            realistic_win_rate,
            naive_total_pnl,
            realistic_total_pnl,
            phantom_fill_gap,
            avg_naive_pnl,
            avg_realistic_pnl,
            avg_queue_ahead,
            avg_fill_time_ms,
        }
    }

    /// Print a formatted text report to stdout.
    pub fn print(&self) {
        let pct = |n: usize, d: usize| -> f64 {
            if d > 0 {
                n as f64 / d as f64 * 100.0
            } else {
                0.0
            }
        };

        println!();
        println!(
            "{}",
            "=".repeat(55)
        );
        println!(
            "  PhantomFill Report: {} + {}",
            self.strategy_name, self.fill_model_name
        );
        println!(
            "{}",
            "=".repeat(55)
        );
        println!();
        println!("  Windows:      {}", self.total_windows);
        println!(
            "  Trades taken: {}    ({:.1}%)",
            self.trades_taken,
            pct(self.trades_taken, self.total_windows)
        );
        println!(
            "  Fills:        {}    ({:.1}% fill rate)",
            self.fills,
            self.fill_rate * 100.0
        );
        println!(
            "  Correct:      {}    ({:.1}% WR)",
            self.correct,
            self.realistic_win_rate * 100.0
        );
        println!(
            "  Skipped:      {}    ({:.1}%)",
            self.skipped,
            pct(self.skipped, self.total_windows)
        );

        println!();
        println!("  --- PnL {}",  "-".repeat(45));
        println!(
            "  Naive paper:     {:+.2}",
            self.naive_total_pnl
        );
        println!(
            "  Realistic:       {:+.2}",
            self.realistic_total_pnl
        );
        println!(
            "  Phantom gap:      {:.2}  <- \"what you THOUGHT you'd make\"",
            self.phantom_fill_gap
        );
        println!();
        println!(
            "  Avg naive/trade:    {:+.2}",
            self.avg_naive_pnl
        );
        println!(
            "  Avg real/trade:     {:+.2}",
            self.avg_realistic_pnl
        );

        println!();
        println!("  --- Queue Stats {}", "-".repeat(37));
        println!(
            "  Avg queue ahead:   {:.1} shares",
            self.avg_queue_ahead
        );
        println!(
            "  Avg fill time:    {:.0} ms",
            self.avg_fill_time_ms
        );

        println!();
        println!(
            "{}",
            "=".repeat(55)
        );
        println!();
    }

    /// Export all WindowResult rows to a CSV file.
    pub fn export_csv(results: &[WindowResult], path: &Path) -> Result<()> {
        let mut wtr = csv::Writer::from_path(path)
            .with_context(|| format!("failed to create CSV at {}", path.display()))?;

        for r in results {
            wtr.serialize(r)
                .with_context(|| format!("failed to write CSV row for {}", r.market_id))?;
        }

        wtr.flush().context("failed to flush CSV")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_result(
        bid_side: Option<&str>,
        filled: bool,
        correct: bool,
        naive_pnl: f64,
        realistic_pnl: f64,
        queue_ahead: f64,
        fill_time_ms: Option<i64>,
    ) -> WindowResult {
        WindowResult {
            market_id: "test-market".to_string(),
            platform: "polymarket".to_string(),
            category: "btc".to_string(),
            open_ts: 1000,
            close_ts: 1300,
            outcome: "YES".to_string(),
            predicted: bid_side.map(|_| "YES".to_string()),
            signal_offset_ms: Some(90_000),
            bid_side: bid_side.map(|s| s.to_string()),
            bid_price: 0.49,
            shares: 10.0,
            filled,
            queue_ahead_at_place: queue_ahead,
            fill_time_ms,
            correct,
            realistic_pnl,
            naive_pnl,
            ref_price_open: Some(66000.0),
            ref_price_close: Some(66100.0),
        }
    }

    #[test]
    fn test_empty_results() {
        let report = Report::from_results(&[], "test", "delise");
        assert_eq!(report.total_windows, 0);
        assert_eq!(report.trades_taken, 0);
        assert_eq!(report.fills, 0);
        assert_eq!(report.correct, 0);
        assert_eq!(report.fill_rate, 0.0);
        assert_eq!(report.naive_total_pnl, 0.0);
        assert_eq!(report.realistic_total_pnl, 0.0);
    }

    #[test]
    fn test_all_skipped() {
        let results = vec![
            make_result(None, false, false, 0.0, 0.0, 0.0, None),
            make_result(None, false, false, 0.0, 0.0, 0.0, None),
        ];
        let report = Report::from_results(&results, "test", "delise");
        assert_eq!(report.total_windows, 2);
        assert_eq!(report.trades_taken, 0);
        assert_eq!(report.skipped, 2);
        assert_eq!(report.fill_rate, 0.0);
    }

    #[test]
    fn test_basic_counts() {
        let results = vec![
            // Traded, filled, correct
            make_result(Some("YES"), true, true, 0.51, 0.51, 200.0, Some(45000)),
            // Traded, filled, incorrect
            make_result(Some("YES"), true, false, -0.49, -0.49, 300.0, Some(60000)),
            // Traded, not filled
            make_result(Some("YES"), false, true, 0.51, 0.0, 400.0, None),
            // Skipped
            make_result(None, false, false, 0.0, 0.0, 0.0, None),
        ];
        let report = Report::from_results(&results, "momentum", "delise-3rule");

        assert_eq!(report.total_windows, 4);
        assert_eq!(report.trades_taken, 3);
        assert_eq!(report.fills, 2);
        assert_eq!(report.correct, 1); // filled AND correct
        assert_eq!(report.skipped, 1);

        // fill_rate = 2/3
        assert!((report.fill_rate - 2.0 / 3.0).abs() < 1e-9);
        // naive_win_rate = 2/3 (two have correct=true among traded)
        assert!((report.naive_win_rate - 2.0 / 3.0).abs() < 1e-9);
        // realistic_win_rate = 1/2 (1 correct fill / 2 fills)
        assert!((report.realistic_win_rate - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_pnl_computation() {
        let results = vec![
            make_result(Some("YES"), true, true, 0.51, 0.51, 100.0, Some(30000)),
            make_result(Some("NO"), true, false, -0.49, -0.49, 200.0, Some(50000)),
            make_result(Some("YES"), false, true, 0.51, 0.0, 300.0, None),
        ];
        let report = Report::from_results(&results, "test", "delise");

        assert!((report.naive_total_pnl - 0.53).abs() < 1e-9);
        assert!((report.realistic_total_pnl - 0.02).abs() < 1e-9);
        assert!((report.phantom_fill_gap - 0.51).abs() < 1e-9);
        assert!((report.avg_naive_pnl - 0.53 / 3.0).abs() < 1e-9);
        assert!((report.avg_realistic_pnl - 0.02 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_queue_stats() {
        let results = vec![
            make_result(Some("YES"), true, true, 0.51, 0.51, 200.0, Some(30000)),
            make_result(Some("YES"), true, false, -0.49, -0.49, 400.0, Some(60000)),
            make_result(Some("YES"), false, true, 0.51, 0.0, 300.0, None),
        ];
        let report = Report::from_results(&results, "test", "delise");

        // avg_queue_ahead = (200 + 400 + 300) / 3 = 300.0
        assert!((report.avg_queue_ahead - 300.0).abs() < 1e-9);
        // avg_fill_time_ms = (30000 + 60000) / 2 = 45000.0
        assert!((report.avg_fill_time_ms - 45000.0).abs() < 1e-9);
    }

    #[test]
    fn test_export_csv_roundtrip() {
        let results = vec![
            make_result(Some("YES"), true, true, 0.51, 0.51, 200.0, Some(30000)),
            make_result(Some("NO"), false, false, -0.49, 0.0, 300.0, None),
        ];

        let dir = std::env::temp_dir().join("phantomfill_test_csv");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("test_export.csv");

        Report::export_csv(&results, &path).unwrap();

        // Read back and verify
        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        // Header + 2 data rows
        assert_eq!(lines.len(), 3);
        assert!(lines[0].contains("market_id"));
        assert!(lines[0].contains("naive_pnl"));
        assert!(lines[0].contains("realistic_pnl"));

        // Cleanup
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_print_does_not_panic() {
        let results = vec![
            make_result(Some("YES"), true, true, 0.51, 0.51, 200.0, Some(30000)),
        ];
        let report = Report::from_results(&results, "momentum", "delise-3rule");
        // Just verify it doesn't panic.
        report.print();
    }

    #[test]
    fn test_report_names() {
        let report = Report::from_results(&[], "my_strat", "my_model");
        assert_eq!(report.strategy_name, "my_strat");
        assert_eq!(report.fill_model_name, "my_model");
    }
}
