use crate::fill::FillModel;
use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Market, SimOrder, WindowResult};
use tracing::{debug, info};

/// Configuration for the replay engine.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub bid_price: f64,
    pub shares: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            bid_price: 0.49,
            shares: 10.0,
        }
    }
}

/// The core replay engine. Runs strategies against historical data using
/// a fill model to simulate realistic order execution.
pub struct ReplayEngine {
    fill_model: Box<dyn FillModel>,
    config: ReplayConfig,
}

impl ReplayEngine {
    pub fn new(fill_model: Box<dyn FillModel>, config: ReplayConfig) -> Self {
        Self { fill_model, config }
    }

    /// Run a single market window: feed snapshots through the strategy,
    /// simulate fills, compute PnL.
    pub fn run_window(
        &self,
        market: &Market,
        snapshots: &[BookSnapshot],
        strategy: &mut dyn Strategy,
    ) -> Option<WindowResult> {
        if snapshots.is_empty() {
            return None;
        }

        let outcome = market.outcome?;

        // Reset strategy and notify market open.
        strategy.reset();
        strategy.on_market_open(&snapshots[0]);

        // Track orders and which have been cancelled.
        let mut orders: Vec<SimOrder> = Vec::new();
        let mut cancelled: Vec<bool> = Vec::new();

        let mut prev_offset_ms = snapshots[0].offset_ms;
        let mut signal_offset_ms: Option<i64> = None;

        for snap in snapshots {
            // Process fill model BEFORE strategy actions so adverse fills
            // can happen on the same tick as a cancel (prevents cancel/fill race bias).
            self.fill_model
                .process_tick(snap, &mut orders, prev_offset_ms);
            prev_offset_ms = snap.offset_ms;

            // Get strategy actions for this tick.
            let actions = strategy.on_tick(snap);

            for action in &actions {
                match action {
                    Action::PlaceBid {
                        side,
                        price,
                        shares,
                    } => {
                        // Only allow one order per side (active or already placed).
                        let already_has = orders
                            .iter()
                            .zip(cancelled.iter())
                            .any(|(o, &c)| o.side == *side && !c);
                        if already_has {
                            continue;
                        }
                        // Also skip if this side was previously cancelled.
                        let side_cancelled = orders
                            .iter()
                            .zip(cancelled.iter())
                            .any(|(o, &c)| o.side == *side && c);
                        if side_cancelled {
                            continue;
                        }

                        let order = self.fill_model.create_order(
                            *side,
                            *price,
                            *shares,
                            snap,
                            snap.offset_ms,
                        );

                        if signal_offset_ms.is_none() {
                            signal_offset_ms = Some(snap.offset_ms);
                        }

                        orders.push(order);
                        cancelled.push(false);
                    }
                    Action::Cancel { side } => {
                        // Find unfilled, non-cancelled order on this side and cancel it.
                        for (idx, order) in orders.iter_mut().enumerate() {
                            if order.side == *side && !order.filled && !cancelled[idx] {
                                // Mark as filled so fill_model.process_tick skips it,
                                // but do NOT set filled_at_ms (distinguishes cancel from real fill).
                                order.filled = true;
                                cancelled[idx] = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        // Compute naive PnL: assumes every non-cancelled PlaceBid fills.
        let mut naive_pnl = 0.0;
        for (idx, order) in orders.iter().enumerate() {
            if cancelled[idx] {
                continue;
            }
            if outcome.matches_side(order.side) {
                naive_pnl += order.shares * (1.0 - order.price);
            } else {
                naive_pnl -= order.shares * order.price;
            }
        }

        // Compute realistic PnL: only orders that actually filled and pass
        // the adverse selection filter.
        let mut realistic_pnl = 0.0;
        for (idx, order) in orders.iter().enumerate() {
            if cancelled[idx] {
                continue;
            }
            if !order.filled || order.filled_at_ms.is_none() {
                continue;
            }
            let is_winner = outcome.matches_side(order.side);
            if !self.fill_model.adverse_selection_filter(order, is_winner) {
                continue;
            }
            if is_winner {
                realistic_pnl += order.shares * (1.0 - order.price);
            } else {
                realistic_pnl -= order.shares * order.price;
            }
        }

        // Determine predicted side: first non-cancelled order's side.
        let predicted = orders
            .iter()
            .zip(cancelled.iter())
            .find(|(_, &c)| !c)
            .map(|(o, _)| o.side);

        // Correct = any non-cancelled order predicted the winning side.
        let correct = orders
            .iter()
            .zip(cancelled.iter())
            .any(|(o, &c)| !c && outcome.matches_side(o.side));

        // Find the first non-cancelled, actually-filled order for fill metadata.
        let primary_fill = orders
            .iter()
            .zip(cancelled.iter())
            .find(|(o, &c)| !c && o.filled && o.filled_at_ms.is_some());

        let (filled, queue_ahead_at_place, fill_time_ms) = match primary_fill {
            Some((o, _)) => (true, o.queue_ahead, o.filled_at_ms),
            None => {
                // Use queue_ahead from first non-cancelled order if available.
                let qa = orders
                    .iter()
                    .zip(cancelled.iter())
                    .find(|(_, &c)| !c)
                    .map(|(o, _)| o.queue_ahead)
                    .unwrap_or(0.0);
                (false, qa, None)
            }
        };

        let ref_price_open = snapshots.first().and_then(|s| s.reference_price);
        let ref_price_close = snapshots.last().and_then(|s| s.reference_price);

        let result = WindowResult {
            market_id: market.id.clone(),
            platform: market.platform.to_string(),
            category: market.category.clone(),
            open_ts: market.open_ts,
            close_ts: market.close_ts,
            outcome: outcome.label().to_string(),
            predicted: predicted.map(|s| s.label().to_string()),
            signal_offset_ms,
            bid_side: predicted.map(|s| s.label().to_string()),
            bid_price: self.config.bid_price,
            shares: self.config.shares,
            filled,
            queue_ahead_at_place,
            fill_time_ms,
            correct,
            realistic_pnl,
            naive_pnl,
            ref_price_open,
            ref_price_close,
        };

        debug!(
            market_id = %market.id,
            outcome = %outcome,
            predicted = ?predicted,
            correct,
            naive_pnl,
            realistic_pnl,
            filled,
            "window complete"
        );

        Some(result)
    }

    /// Run all markets through the replay engine, creating a fresh strategy
    /// per window.
    pub fn run_all(
        &self,
        markets: &[Market],
        snapshots_fn: &dyn Fn(&str) -> anyhow::Result<Vec<BookSnapshot>>,
        strategy_fn: &dyn Fn() -> Box<dyn Strategy>,
    ) -> Vec<WindowResult> {
        let mut results = Vec::new();
        let total = markets.len();

        for (i, market) in markets.iter().enumerate() {
            if (i + 1) % 100 == 0 || i + 1 == total {
                info!("processing market {}/{} ({})", i + 1, total, market.id);
            }

            let snapshots = match snapshots_fn(&market.id) {
                Ok(s) => s,
                Err(e) => {
                    debug!(market_id = %market.id, error = %e, "failed to load snapshots, skipping");
                    continue;
                }
            };

            let mut strategy = strategy_fn();
            if let Some(result) = self.run_window(market, &snapshots, strategy.as_mut()) {
                results.push(result);
            }
        }

        info!(
            "replay complete: {} results from {} markets",
            results.len(),
            total
        );

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fill::model::FillModel;
    use crate::strategies::make_test_snap;
    use crate::types::{Outcome, Platform, Side};

    /// A deterministic fill model for testing: fills every order on the second
    /// tick it sees (simulating immediate queue consumption).
    struct AlwaysFillModel;

    impl FillModel for AlwaysFillModel {
        fn name(&self) -> &str {
            "always-fill"
        }

        fn create_order(
            &self,
            side: Side,
            price: f64,
            shares: f64,
            snap: &BookSnapshot,
            offset_ms: i64,
        ) -> SimOrder {
            let _ = snap;
            SimOrder {
                side,
                price,
                shares,
                placed_at_ms: offset_ms,
                queue_ahead: 100.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            }
        }

        fn process_tick(
            &self,
            snap: &BookSnapshot,
            orders: &mut [SimOrder],
            _prev_offset_ms: i64,
        ) -> Vec<usize> {
            let mut filled = Vec::new();
            for (i, order) in orders.iter_mut().enumerate() {
                if order.filled {
                    continue;
                }
                // Fill if order was placed before this tick.
                if snap.offset_ms > order.placed_at_ms {
                    order.filled = true;
                    order.filled_at_ms = Some(snap.offset_ms);
                    filled.push(i);
                }
            }
            filled
        }

        fn adverse_selection_filter(&self, _order: &SimOrder, _is_winner: bool) -> bool {
            true
        }
    }

    /// A fill model that fills orders only after a minimum delay.
    struct SlowFillModel {
        min_delay_ms: i64,
    }

    impl FillModel for SlowFillModel {
        fn name(&self) -> &str {
            "slow-fill"
        }

        fn create_order(
            &self,
            side: Side,
            price: f64,
            shares: f64,
            _snap: &BookSnapshot,
            offset_ms: i64,
        ) -> SimOrder {
            SimOrder {
                side,
                price,
                shares,
                placed_at_ms: offset_ms,
                queue_ahead: 100.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            }
        }

        fn process_tick(
            &self,
            snap: &BookSnapshot,
            orders: &mut [SimOrder],
            _prev_offset_ms: i64,
        ) -> Vec<usize> {
            let mut filled = Vec::new();
            for (i, order) in orders.iter_mut().enumerate() {
                if order.filled {
                    continue;
                }
                if snap.offset_ms >= order.placed_at_ms + self.min_delay_ms {
                    order.filled = true;
                    order.filled_at_ms = Some(snap.offset_ms);
                    filled.push(i);
                }
            }
            filled
        }

        fn adverse_selection_filter(&self, _order: &SimOrder, _is_winner: bool) -> bool {
            true
        }
    }

    /// A fill model that never fills any orders.
    struct NeverFillModel;

    impl FillModel for NeverFillModel {
        fn name(&self) -> &str {
            "never-fill"
        }

        fn create_order(
            &self,
            side: Side,
            price: f64,
            shares: f64,
            _snap: &BookSnapshot,
            offset_ms: i64,
        ) -> SimOrder {
            SimOrder {
                side,
                price,
                shares,
                placed_at_ms: offset_ms,
                queue_ahead: 500.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            }
        }

        fn process_tick(
            &self,
            _snap: &BookSnapshot,
            _orders: &mut [SimOrder],
            _prev_offset_ms: i64,
        ) -> Vec<usize> {
            Vec::new()
        }

        fn adverse_selection_filter(&self, _order: &SimOrder, _is_winner: bool) -> bool {
            true
        }
    }

    fn make_market(outcome: Option<Outcome>) -> Market {
        Market {
            id: "test-market".to_string(),
            platform: Platform::Polymarket,
            description: "test".to_string(),
            category: "btc".to_string(),
            open_ts: 1_700_000_000,
            close_ts: 1_700_000_300,
            duration_secs: 300,
            outcome,
        }
    }

    fn make_snaps_with_ref(count: usize, oracle_start: f64, oracle_end: f64) -> Vec<BookSnapshot> {
        (0..count)
            .map(|i| {
                let frac = if count > 1 {
                    i as f64 / (count - 1) as f64
                } else {
                    1.0
                };
                let oracle = oracle_start + (oracle_end - oracle_start) * frac;
                let mut snap = make_test_snap(i as i64 * 1000, Some(oracle), 500.0, 500.0);
                snap.reference_price = Some(oracle - 10.0);
                snap
            })
            .collect()
    }

    // -----------------------------------------------------------------------
    // Test: spread_arb (both sides bid, one wins one loses)
    // -----------------------------------------------------------------------
    #[test]
    fn test_spread_arb_yes_wins() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));
        let snaps = make_snaps_with_ref(10, 50000.0, 50100.0);

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        assert_eq!(result.market_id, "test-market");
        assert_eq!(result.outcome, "YES");
        // Both sides placed: YES wins (+5.10), NO loses (-4.90), net = +0.20
        let expected_naive = 10.0 * (1.0 - 0.49) - 10.0 * 0.49;
        assert!(
            (result.naive_pnl - expected_naive).abs() < 1e-9,
            "naive_pnl={}, expected={}",
            result.naive_pnl,
            expected_naive
        );
        // Both should fill with AlwaysFillModel, realistic should match naive.
        assert!(
            (result.realistic_pnl - expected_naive).abs() < 1e-9,
            "realistic_pnl={}, expected={}",
            result.realistic_pnl,
            expected_naive
        );
        assert!(result.correct);
        assert!(result.filled);
    }

    #[test]
    fn test_spread_arb_no_wins() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::No));
        let snaps = make_snaps_with_ref(10, 50000.0, 49900.0);

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        assert_eq!(result.outcome, "NO");
        // YES loses (-4.90), NO wins (+5.10), net = +0.20
        let expected = 10.0 * (1.0 - 0.49) - 10.0 * 0.49;
        assert!((result.naive_pnl - expected).abs() < 1e-9);
        assert!(result.correct);
    }

    // -----------------------------------------------------------------------
    // Test: momentum strategy (single directional bet)
    // -----------------------------------------------------------------------
    #[test]
    fn test_momentum_single_bet_correct() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        // Build snapshots with strong positive momentum after 90s.
        let mut snaps: Vec<BookSnapshot> = Vec::new();
        for i in 0..20 {
            let offset = i * 5000; // 0, 5000, ..., 95000
            let oracle = 50000.0 + (i as f64) * 20.0; // gradually rising
            let mut snap = make_test_snap(offset, Some(oracle), 500.0, 500.0);
            snap.reference_price = Some(oracle);
            snaps.push(snap);
        }

        let mut strategy =
            crate::strategies::momentum::MomentumSignal::new(0.49, 10.0, 20.0, 90_000);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        assert_eq!(result.predicted.as_deref(), Some("YES"));
        assert!(result.correct);
        // Single YES bet on correct outcome: +shares*(1-price) = +5.10
        let expected = 10.0 * (1.0 - 0.49);
        assert!(
            (result.naive_pnl - expected).abs() < 1e-9,
            "naive_pnl={}, expected={}",
            result.naive_pnl,
            expected
        );
    }

    #[test]
    fn test_momentum_no_signal_no_result() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        // Only 5 ticks, all before 90s => momentum never triggers.
        let snaps: Vec<BookSnapshot> = (0..5)
            .map(|i| {
                let offset = i * 1000;
                let mut snap = make_test_snap(offset, Some(50000.0), 500.0, 500.0);
                snap.reference_price = Some(50000.0);
                snap
            })
            .collect();

        let mut strategy =
            crate::strategies::momentum::MomentumSignal::new(0.49, 10.0, 20.0, 90_000);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // No orders placed at all.
        assert_eq!(result.predicted, None);
        assert!(!result.correct);
        assert!((result.naive_pnl).abs() < 1e-9);
        assert!((result.realistic_pnl).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // Test: post_cancel strategy (cancel behavior)
    // -----------------------------------------------------------------------
    #[test]
    fn test_post_cancel_cancels_loser() {
        let engine = ReplayEngine::new(Box::new(NeverFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        // Snapshots: open at 0ms, ticks up to 80s, signal at 90s with positive momentum.
        let mut snaps: Vec<BookSnapshot> = Vec::new();
        // Open tick
        let mut s0 = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        s0.reference_price = Some(50000.0);
        snaps.push(s0);
        // Ticks before signal (offsets 10_000 through 80_000)
        for i in 1..9 {
            let offset = i * 10_000;
            let mut snap = make_test_snap(offset, Some(50000.0 + i as f64 * 10.0), 500.0, 500.0);
            snap.reference_price = Some(50000.0 + i as f64 * 10.0);
            snaps.push(snap);
        }
        // Signal tick (90s): +40 bps => cancel No side
        let mut signal_snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        signal_snap.reference_price = Some(50200.0);
        snaps.push(signal_snap);

        let mut strategy =
            crate::strategies::post_cancel::PostBothCancelLoser::new(0.49, 10.0, 20.0, 90_000);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // With NeverFillModel, nothing fills. Naive PnL counts only non-cancelled:
        // YES order survives (No is cancelled). YES wins => +5.10 naive.
        let expected_naive = 10.0 * (1.0 - 0.49);
        assert!(
            (result.naive_pnl - expected_naive).abs() < 1e-9,
            "naive_pnl={}, expected={}",
            result.naive_pnl,
            expected_naive
        );
        // Realistic PnL = 0 because NeverFillModel never fills.
        assert!(
            (result.realistic_pnl).abs() < 1e-9,
            "realistic_pnl should be 0 with NeverFillModel"
        );
        assert!(result.correct);
    }

    // -----------------------------------------------------------------------
    // Test: cancelled orders don't contribute to PnL
    // -----------------------------------------------------------------------
    #[test]
    fn test_cancelled_orders_excluded_from_pnl() {
        // Use SlowFillModel: orders only fill after 95s delay.
        // This means both orders are still unfilled when cancel arrives at 90s.
        // After cancel, the Yes order fills at 100s but the cancelled No order does not.
        let engine = ReplayEngine::new(
            Box::new(SlowFillModel { min_delay_ms: 95_000 }),
            ReplayConfig::default(),
        );
        let market = make_market(Some(Outcome::Yes));

        // Build snaps: open at 0, ticks to 80s, signal at 90s, fill tick at 100s.
        let mut snaps: Vec<BookSnapshot> = Vec::new();
        let mut s0 = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        s0.reference_price = Some(50000.0);
        snaps.push(s0);
        // Ticks before signal (offsets 10_000 through 80_000)
        for i in 1..9 {
            let offset = i * 10_000;
            let mut snap = make_test_snap(offset, Some(50000.0 + i as f64 * 10.0), 500.0, 500.0);
            snap.reference_price = Some(50000.0 + i as f64 * 10.0);
            snaps.push(snap);
        }
        // Signal tick (90s): +40 bps => cancel No side
        let mut signal_snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        signal_snap.reference_price = Some(50200.0);
        snaps.push(signal_snap);
        // Fill tick (100s): SlowFillModel fills surviving orders (placed at 0 + 95s delay)
        let mut fill_snap = make_test_snap(100_000, Some(50200.0), 500.0, 500.0);
        fill_snap.reference_price = Some(50200.0);
        snaps.push(fill_snap);

        let mut strategy =
            crate::strategies::post_cancel::PostBothCancelLoser::new(0.49, 10.0, 20.0, 90_000);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // post_cancel: places both at T+0, cancels No at 90s.
        // Naive: only YES (non-cancelled), YES wins => +5.10
        let expected_naive = 10.0 * (1.0 - 0.49);
        assert!(
            (result.naive_pnl - expected_naive).abs() < 1e-9,
            "naive_pnl={}, expected={}",
            result.naive_pnl,
            expected_naive
        );

        // Realistic: YES fills at 100s, No was cancelled at 90s.
        // YES wins => +5.10
        assert!(
            (result.realistic_pnl - expected_naive).abs() < 1e-9,
            "realistic_pnl={}, expected={}",
            result.realistic_pnl,
            expected_naive
        );
    }

    // -----------------------------------------------------------------------
    // Test: empty snapshots returns None
    // -----------------------------------------------------------------------
    #[test]
    fn test_empty_snapshots_returns_none() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &[], &mut strategy);
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Test: no outcome returns None
    // -----------------------------------------------------------------------
    #[test]
    fn test_no_outcome_returns_none() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(None);
        let snaps = make_snaps_with_ref(5, 50000.0, 50100.0);

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &snaps, &mut strategy);
        assert!(result.is_none());
    }

    // -----------------------------------------------------------------------
    // Test: reference prices captured correctly
    // -----------------------------------------------------------------------
    #[test]
    fn test_reference_prices() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));
        let snaps = make_snaps_with_ref(10, 50000.0, 50100.0);

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        assert!(result.ref_price_open.is_some());
        assert!(result.ref_price_close.is_some());
        // open ref = 50000 - 10 = 49990, close ref = 50100 - 10 = 50090
        assert!((result.ref_price_open.unwrap() - 49990.0).abs() < 1e-9);
        assert!((result.ref_price_close.unwrap() - 50090.0).abs() < 1e-9);
    }

    // -----------------------------------------------------------------------
    // Test: run_all batch method
    // -----------------------------------------------------------------------
    #[test]
    fn test_run_all_basic() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());

        let markets = vec![
            make_market(Some(Outcome::Yes)),
            {
                let mut m = make_market(Some(Outcome::No));
                m.id = "test-market-2".to_string();
                m
            },
            make_market(None), // no outcome, should be skipped
        ];

        let results = engine.run_all(
            &markets,
            &|id| Ok(make_snaps_with_ref(10, 50000.0, if id.contains("2") { 49900.0 } else { 50100.0 })),
            &|| Box::new(crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0)),
        );

        // Third market has no outcome, so only 2 results.
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].market_id, "test-market");
        assert_eq!(results[1].market_id, "test-market-2");
    }

    // -----------------------------------------------------------------------
    // Test: run_all handles snapshot load errors gracefully
    // -----------------------------------------------------------------------
    #[test]
    fn test_run_all_skips_load_errors() {
        let engine = ReplayEngine::new(Box::new(AlwaysFillModel), ReplayConfig::default());

        let markets = vec![make_market(Some(Outcome::Yes))];

        let results = engine.run_all(
            &markets,
            &|_id| Err(anyhow::anyhow!("database error")),
            &|| Box::new(crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0)),
        );

        assert!(results.is_empty());
    }

    // -----------------------------------------------------------------------
    // Test: NeverFillModel produces zero realistic PnL
    // -----------------------------------------------------------------------
    #[test]
    fn test_never_fill_zero_realistic_pnl() {
        let engine = ReplayEngine::new(Box::new(NeverFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));
        let snaps = make_snaps_with_ref(10, 50000.0, 50100.0);

        let mut strategy =
            crate::strategies::spread_arb::NaiveSpreadArb::new(0.49, 10.0);

        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // Naive PnL still counts both sides.
        let expected_naive = 10.0 * (1.0 - 0.49) - 10.0 * 0.49;
        assert!((result.naive_pnl - expected_naive).abs() < 1e-9);
        // Realistic = 0 since nothing filled.
        assert!((result.realistic_pnl).abs() < 1e-9);
        assert!(!result.filled);
    }

    // -----------------------------------------------------------------------
    // Regression test: Bug 2 — orders placed on tick N must NOT be
    // fill-processed on tick N. They should only be processable starting
    // from tick N+1.
    //
    // The fix moved process_tick to run BEFORE strategy actions in the loop,
    // so orders are placed after process_tick runs for that tick.
    // -----------------------------------------------------------------------

    /// A fill model that fills any order immediately (on the very first
    /// process_tick call after it is pushed to the orders slice), using
    /// `snap.offset_ms >= order.placed_at_ms` (non-strict comparison).
    /// If Bug 2 were present (order placed before process_tick on same tick),
    /// this would fill the order on tick N. With the fix, tick N's process_tick
    /// runs before the order is placed, so the first chance to fill is tick N+1.
    struct ImmediateFillModel;

    impl FillModel for ImmediateFillModel {
        fn name(&self) -> &str {
            "immediate-fill"
        }

        fn create_order(
            &self,
            side: Side,
            price: f64,
            shares: f64,
            _snap: &BookSnapshot,
            offset_ms: i64,
        ) -> SimOrder {
            SimOrder {
                side,
                price,
                shares,
                placed_at_ms: offset_ms,
                queue_ahead: 0.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            }
        }

        fn process_tick(
            &self,
            snap: &BookSnapshot,
            orders: &mut [SimOrder],
            _prev_offset_ms: i64,
        ) -> Vec<usize> {
            let mut filled = Vec::new();
            for (i, order) in orders.iter_mut().enumerate() {
                if order.filled {
                    continue;
                }
                // Non-strict: fills if snap.offset_ms >= placed_at_ms.
                // With the fix, process_tick runs before PlaceBid on the same tick,
                // so the order does not exist yet when this runs at tick N.
                // At tick N+1, snap.offset_ms > placed_at_ms => fills.
                if snap.offset_ms >= order.placed_at_ms {
                    order.filled = true;
                    order.filled_at_ms = Some(snap.offset_ms);
                    filled.push(i);
                }
            }
            filled
        }

        fn adverse_selection_filter(&self, _order: &SimOrder, _is_winner: bool) -> bool {
            true
        }
    }

    /// Strategy that places a YES bid on the very first tick (offset=0).
    struct PlaceOnFirstTick {
        placed: bool,
    }

    impl PlaceOnFirstTick {
        fn new() -> Self {
            Self { placed: false }
        }
    }

    impl crate::strategies::Strategy for PlaceOnFirstTick {
        fn name(&self) -> &str {
            "place-on-first-tick"
        }
        fn description(&self) -> &str {
            "places YES bid on first tick"
        }
        fn on_tick(&mut self, _snap: &BookSnapshot) -> Vec<crate::types::Action> {
            if !self.placed {
                self.placed = true;
                vec![crate::types::Action::PlaceBid {
                    side: Side::Yes,
                    price: 0.49,
                    shares: 10.0,
                }]
            } else {
                vec![]
            }
        }
        fn reset(&mut self) {
            self.placed = false;
        }
    }

    #[test]
    fn test_order_placed_on_tick_n_not_filled_on_tick_n() {
        // Two ticks: offset 0 and offset 1000.
        // Order is placed at tick 0 (offset=0).
        // ImmediateFillModel uses >=, so WITHOUT the fix, it would fill at tick 0.
        // WITH the fix (process_tick runs before PlaceBid), the order does not
        // exist during tick 0's process_tick call, so it only fills at tick 1.
        let engine = ReplayEngine::new(Box::new(ImmediateFillModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        let snaps = vec![
            make_test_snap(0, Some(50000.0), 500.0, 500.0),
            make_test_snap(1000, Some(50000.0), 500.0, 500.0),
        ];

        let mut strategy = PlaceOnFirstTick::new();
        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // Order must fill (at tick N+1), not be unfilled.
        assert!(result.filled, "order should fill at tick N+1");

        // The fill must happen at offset 1000 (tick N+1), NOT at offset 0 (tick N).
        assert_eq!(
            result.fill_time_ms,
            Some(1000),
            "order placed at tick 0 must fill at tick 1 (offset 1000), not tick 0"
        );
    }

    // -----------------------------------------------------------------------
    // Regression test: Bug 5 — adverse fill CAN happen on the same tick as
    // a cancel. Since process_tick now runs BEFORE strategy actions (cancels),
    // an adverse fill at tick N is applied before the cancel at tick N.
    // This prevents the cancel/fill race from unfairly blocking real fills.
    // -----------------------------------------------------------------------

    /// A fill model that fills the first unfilled order it finds on every tick,
    /// regardless of placed_at_ms. Used to simulate an adverse fill.
    struct AdverseFillAlwaysModel;

    impl FillModel for AdverseFillAlwaysModel {
        fn name(&self) -> &str {
            "adverse-fill-always"
        }

        fn create_order(
            &self,
            side: Side,
            price: f64,
            shares: f64,
            _snap: &BookSnapshot,
            offset_ms: i64,
        ) -> SimOrder {
            SimOrder {
                side,
                price,
                shares,
                placed_at_ms: offset_ms,
                queue_ahead: 0.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            }
        }

        fn process_tick(
            &self,
            snap: &BookSnapshot,
            orders: &mut [SimOrder],
            _prev_offset_ms: i64,
        ) -> Vec<usize> {
            let mut filled = Vec::new();
            for (i, order) in orders.iter_mut().enumerate() {
                if !order.filled {
                    order.filled = true;
                    order.filled_at_ms = Some(snap.offset_ms);
                    filled.push(i);
                }
            }
            filled
        }

        fn adverse_selection_filter(&self, _order: &SimOrder, _is_winner: bool) -> bool {
            true
        }
    }

    /// Strategy that: places YES bid at tick 0, then cancels YES at tick 1.
    /// If fills run before cancels (the fix), the YES order will be filled
    /// at tick 1 before the cancel is applied, leaving filled=true with
    /// a real filled_at_ms.
    struct PlaceThenCancelStrategy {
        placed: bool,
        cancelled: bool,
    }

    impl PlaceThenCancelStrategy {
        fn new() -> Self {
            Self {
                placed: false,
                cancelled: false,
            }
        }
    }

    impl crate::strategies::Strategy for PlaceThenCancelStrategy {
        fn name(&self) -> &str {
            "place-then-cancel"
        }
        fn description(&self) -> &str {
            "places YES at tick 0, cancels YES at tick 1"
        }
        fn on_tick(&mut self, _snap: &BookSnapshot) -> Vec<crate::types::Action> {
            if !self.placed {
                self.placed = true;
                vec![crate::types::Action::PlaceBid {
                    side: Side::Yes,
                    price: 0.49,
                    shares: 10.0,
                }]
            } else if !self.cancelled {
                self.cancelled = true;
                vec![crate::types::Action::Cancel { side: Side::Yes }]
            } else {
                vec![]
            }
        }
        fn reset(&mut self) {
            self.placed = false;
            self.cancelled = false;
        }
    }

    #[test]
    fn test_adverse_fill_happens_before_cancel_on_same_tick() {
        // Scenario:
        //   Tick 0: Strategy places YES bid. process_tick runs first (no orders yet), then PlaceBid.
        //   Tick 1: process_tick runs first => fills YES order. Then strategy emits Cancel{YES}.
        //           The cancel finds the YES order already filled, so it cannot cancel it.
        //
        // With the fix, the fill survives: filled=true, filled_at_ms=Some(1000).
        // Without the fix (cancel before process_tick), the cancel would mark the order
        // as cancelled before process_tick sees it, resulting in no real fill.
        let engine = ReplayEngine::new(Box::new(AdverseFillAlwaysModel), ReplayConfig::default());
        let market = make_market(Some(Outcome::Yes));

        let snaps = vec![
            make_test_snap(0, Some(50000.0), 500.0, 500.0),
            make_test_snap(1000, Some(50000.0), 500.0, 500.0),
        ];

        let mut strategy = PlaceThenCancelStrategy::new();
        let result = engine.run_window(&market, &snaps, &mut strategy).unwrap();

        // The YES order must be filled (not cancelled), because the fill model
        // runs BEFORE the cancel in the fixed code.
        assert!(
            result.filled,
            "adverse fill must survive: process_tick runs before cancel on tick 1"
        );
        assert_eq!(
            result.fill_time_ms,
            Some(1000),
            "fill should be recorded at tick 1 offset (1000 ms)"
        );
        // Since filled=true and filled_at_ms is set, the cancel attempt on the
        // already-filled order has no effect. The result should show it as filled.
        assert!(
            result.realistic_pnl > 0.0,
            "filled YES order in YES-outcome market should yield positive realistic PnL"
        );
    }
}
