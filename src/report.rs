use std::path::Path;

use anyhow::{Context, Result};

use crate::types::WindowResult;

/// Summary of multiple Monte Carlo runs with confidence intervals.
#[derive(Debug, Clone)]
pub struct MonteCarloSummary {
    pub runs: usize,
    pub seed: Option<u64>,

    // Naive PnL is deterministic (same across runs)
    pub naive_total_pnl: f64,

    // Realistic PnL distribution
    pub realistic_pnl_mean: f64,
    pub realistic_pnl_median: f64,
    pub realistic_pnl_p5: f64,
    pub realistic_pnl_p95: f64,
    pub realistic_pnl_std: f64,

    // Fill rate distribution
    pub fill_rate_mean: f64,
    pub win_rate_mean: f64,

    // Phantom gap
    pub phantom_gap_median: f64,

    // Per-run reports for detailed analysis
    pub reports: Vec<Report>,
}

impl MonteCarloSummary {
    /// Build a summary from multiple backtest reports.
    ///
    /// Naive PnL is taken from the first report (it is deterministic and
    /// identical across runs). Realistic PnL varies per run due to fill
    /// model randomness.
    pub fn from_reports(reports: Vec<Report>, seed: Option<u64>) -> Self {
        assert!(!reports.is_empty(), "need at least one report");

        let runs = reports.len();
        let naive_total_pnl = reports[0].naive_total_pnl;

        let mut pnls: Vec<f64> = reports.iter().map(|r| r.realistic_total_pnl).collect();
        pnls.sort_by(|a, b| a.total_cmp(b));

        let realistic_pnl_mean = pnls.iter().sum::<f64>() / runs as f64;
        let realistic_pnl_median = percentile(&pnls, 50.0);
        let realistic_pnl_p5 = percentile(&pnls, 5.0);
        let realistic_pnl_p95 = percentile(&pnls, 95.0);

        let variance =
            pnls.iter().map(|v| (v - realistic_pnl_mean).powi(2)).sum::<f64>() / runs as f64;
        let realistic_pnl_std = variance.sqrt();

        let fill_rate_mean =
            reports.iter().map(|r| r.fill_rate).sum::<f64>() / runs as f64;
        let win_rate_mean =
            reports.iter().map(|r| r.realistic_win_rate).sum::<f64>() / runs as f64;

        let mut gaps: Vec<f64> = reports.iter().map(|r| r.phantom_fill_gap).collect();
        gaps.sort_by(|a, b| a.total_cmp(b));
        let phantom_gap_median = percentile(&gaps, 50.0);

        Self {
            runs,
            seed,
            naive_total_pnl,
            realistic_pnl_mean,
            realistic_pnl_median,
            realistic_pnl_p5,
            realistic_pnl_p95,
            realistic_pnl_std,
            fill_rate_mean,
            win_rate_mean,
            phantom_gap_median,
            reports,
        }
    }

    /// Print a formatted Monte Carlo summary to stdout.
    pub fn print(&self) {
        let r = &self.reports[0];
        let strategy = &r.strategy_name;
        let fill_model = &r.fill_model_name;

        let seed_str = match self.seed {
            Some(s) => format!("{}", s),
            None => "random".to_string(),
        };

        let total_windows = r.total_windows;
        let trades_taken = r.trades_taken;
        let trade_pct = if total_windows > 0 {
            trades_taken as f64 / total_windows as f64 * 100.0
        } else {
            0.0
        };

        println!();
        println!("{}", "=".repeat(55));
        println!(
            "  PhantomFill Monte Carlo: {} + {}",
            strategy, fill_model
        );
        println!(
            "  {} runs, seed: {}",
            self.runs, seed_str
        );
        println!("{}", "=".repeat(55));
        println!();
        println!("  Windows:      {}", total_windows);
        println!(
            "  Trades taken: {}    ({:.1}%)",
            trades_taken, trade_pct
        );

        println!();
        println!("  --- PnL (95% confidence interval) {}", "-".repeat(19));
        println!(
            "  Naive paper:     {:+.2}   (deterministic)",
            self.naive_total_pnl
        );
        println!(
            "  Realistic:       {:+.2}   median [{:+.2}, {:+.2}]",
            self.realistic_pnl_median, self.realistic_pnl_p5, self.realistic_pnl_p95
        );
        println!(
            "  Phantom gap:      {:.2}    median",
            self.phantom_gap_median
        );

        println!();
        println!(
            "  Fill rate:       {:.1}%     mean across runs",
            self.fill_rate_mean * 100.0
        );
        println!(
            "  Win rate:        {:.1}%     mean across runs",
            self.win_rate_mean * 100.0
        );

        println!();
        println!(
            "  Std dev:          {:.2}    (realistic PnL)",
            self.realistic_pnl_std
        );

        println!();
        println!("{}", "=".repeat(55));
        println!();
    }
}

/// Compute a percentile from a sorted slice using nearest-rank.
fn percentile(sorted: &[f64], pct: f64) -> f64 {
    assert!(!sorted.is_empty());
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = pct / 100.0 * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    let frac = rank - lo as f64;
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

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

    // -----------------------------------------------------------------------
    // MonteCarloSummary tests
    // -----------------------------------------------------------------------

    fn make_report_with_pnl(naive: f64, realistic: f64, fill_rate: f64, win_rate: f64) -> Report {
        Report {
            strategy_name: "test-strat".to_string(),
            fill_model_name: "delise-3rule".to_string(),
            total_windows: 100,
            trades_taken: 95,
            fills: 80,
            correct: 70,
            skipped: 5,
            fill_rate,
            naive_win_rate: 0.9,
            realistic_win_rate: win_rate,
            naive_total_pnl: naive,
            realistic_total_pnl: realistic,
            phantom_fill_gap: naive - realistic,
            avg_naive_pnl: naive / 95.0,
            avg_realistic_pnl: realistic / 95.0,
            avg_queue_ahead: 200.0,
            avg_fill_time_ms: 45000.0,
        }
    }

    #[test]
    fn test_monte_carlo_from_three_reports() {
        let reports = vec![
            make_report_with_pnl(100.0, 60.0, 0.80, 0.85),
            make_report_with_pnl(100.0, 80.0, 0.90, 0.88),
            make_report_with_pnl(100.0, 100.0, 0.95, 0.92),
        ];

        let summary = MonteCarloSummary::from_reports(reports, Some(42));

        assert_eq!(summary.runs, 3);
        assert_eq!(summary.seed, Some(42));
        assert!((summary.naive_total_pnl - 100.0).abs() < 1e-9);
        assert!((summary.realistic_pnl_mean - 80.0).abs() < 1e-9);
        assert!((summary.realistic_pnl_median - 80.0).abs() < 1e-9);
        // fill_rate_mean = (0.80 + 0.90 + 0.95) / 3 = 0.8833...
        assert!((summary.fill_rate_mean - 0.8833333333).abs() < 1e-4);
        // win_rate_mean = (0.85 + 0.88 + 0.92) / 3 = 0.8833...
        assert!((summary.win_rate_mean - 0.8833333333).abs() < 1e-4);
    }

    #[test]
    fn test_monte_carlo_percentiles() {
        // 10 reports with realistic PnL from 10 to 100.
        let reports: Vec<Report> = (1..=10)
            .map(|i| make_report_with_pnl(200.0, i as f64 * 10.0, 0.85, 0.90))
            .collect();

        let summary = MonteCarloSummary::from_reports(reports, None);

        // sorted PnLs: [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        // p5 = index 0.45 => 10 * 0.55 + 20 * 0.45 = 14.5
        assert!((summary.realistic_pnl_p5 - 14.5).abs() < 1e-9);
        // p95 = index 8.55 => 90 * 0.45 + 100 * 0.55 = 95.5
        assert!((summary.realistic_pnl_p95 - 95.5).abs() < 1e-9);
        // median = index 4.5 => 50 * 0.5 + 60 * 0.5 = 55
        assert!((summary.realistic_pnl_median - 55.0).abs() < 1e-9);
    }

    #[test]
    fn test_monte_carlo_print_does_not_panic() {
        let reports = vec![
            make_report_with_pnl(100.0, 60.0, 0.80, 0.85),
            make_report_with_pnl(100.0, 80.0, 0.90, 0.88),
        ];
        let summary = MonteCarloSummary::from_reports(reports, Some(99));
        // Just verify it doesn't panic.
        summary.print();
    }

    #[test]
    fn test_monte_carlo_single_report() {
        let reports = vec![make_report_with_pnl(50.0, 30.0, 0.75, 0.80)];
        let summary = MonteCarloSummary::from_reports(reports, None);
        assert_eq!(summary.runs, 1);
        assert!((summary.realistic_pnl_median - 30.0).abs() < 1e-9);
        assert!((summary.realistic_pnl_p5 - 30.0).abs() < 1e-9);
        assert!((summary.realistic_pnl_p95 - 30.0).abs() < 1e-9);
        assert!((summary.realistic_pnl_std).abs() < 1e-9);
    }

    #[test]
    fn test_monte_carlo_no_seed() {
        let reports = vec![make_report_with_pnl(50.0, 30.0, 0.75, 0.80)];
        let summary = MonteCarloSummary::from_reports(reports, None);
        assert_eq!(summary.seed, None);
    }

    // -----------------------------------------------------------------------
    // Regression test: Bug 3 — NaN values in PnL must not cause panic during
    // Report::from_results or MonteCarloSummary::from_reports (sorting).
    //
    // Before the fix, pnls.sort_by(|a, b| a.partial_cmp(b).unwrap()) panicked
    // when PnL was NaN. The fix uses total_cmp which handles NaN without panic.
    // -----------------------------------------------------------------------

    #[test]
    fn test_report_from_results_with_nan_pnl_does_not_panic() {
        // A WindowResult where both naive and realistic PnL are NaN.
        // Report::from_results sums them up; NaN propagates but must not panic.
        let results = vec![
            make_result(Some("YES"), true, true, f64::NAN, f64::NAN, 100.0, Some(30000)),
            make_result(Some("YES"), true, false, f64::NAN, f64::NAN, 200.0, Some(50000)),
        ];

        // Must not panic.
        let report = Report::from_results(&results, "test", "delise");

        // NaN arithmetic propagates but we just care there's no panic.
        // The totals will be NaN — that's acceptable as long as no panic occurs.
        assert_eq!(report.total_windows, 2);
        assert_eq!(report.trades_taken, 2);
    }

    #[test]
    fn test_report_from_results_with_nan_in_some_windows_does_not_panic() {
        // Mix of normal and NaN PnL windows.
        let results = vec![
            make_result(Some("YES"), true, true, 1.0, 0.51, 100.0, Some(30000)),
            make_result(Some("YES"), true, false, f64::NAN, f64::NAN, 200.0, Some(50000)),
            make_result(Some("NO"), false, false, 0.5, 0.0, 300.0, None),
        ];

        // Must not panic.
        let report = Report::from_results(&results, "test", "delise");
        assert_eq!(report.total_windows, 3);
    }

    #[test]
    fn test_monte_carlo_from_reports_with_nan_pnl_does_not_panic() {
        // Reports with NaN realistic PnL — sort_by(total_cmp) must not panic.
        let mut report_nan = make_report_with_pnl(100.0, 60.0, 0.80, 0.85);
        report_nan.realistic_total_pnl = f64::NAN;
        report_nan.phantom_fill_gap = f64::NAN;

        let reports = vec![
            make_report_with_pnl(100.0, 60.0, 0.80, 0.85),
            report_nan,
            make_report_with_pnl(100.0, 80.0, 0.90, 0.88),
        ];

        // Must not panic — total_cmp handles NaN without panicking.
        let summary = MonteCarloSummary::from_reports(reports, Some(42));
        assert_eq!(summary.runs, 3);
    }

    #[test]
    fn test_monte_carlo_from_reports_all_nan_pnl_does_not_panic() {
        // All reports have NaN realistic PnL.
        let make_nan_report = || {
            let mut r = make_report_with_pnl(100.0, 60.0, 0.80, 0.85);
            r.realistic_total_pnl = f64::NAN;
            r
        };
        let reports = vec![make_nan_report(), make_nan_report()];

        // Must not panic.
        let summary = MonteCarloSummary::from_reports(reports, None);
        assert_eq!(summary.runs, 2);
    }
}
