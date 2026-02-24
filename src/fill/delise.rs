//! DeLise 3-rule fill model adapted for prediction markets.
//!
//! Based on DeLise (2024) "The Negative Drift of a Limit Order Fill":
//! - Rule 1: Adverse tick (best_ask <= our bid) => fill with high probability
//! - Rule 2: Non-adverse tick => small Rf probability of fill per second
//! - Rule 3: Price moves are discrete ($0.01 on Polymarket)
//!
//! Adaptations for prediction markets:
//! - Queue position estimated from bid depth at order price
//! - Taker volume estimated from depth changes between snapshots
//! - Adverse selection filter based on pre/post-signal timing

use crate::fill::model::FillModel;
use crate::fill::queue;
use crate::types::{BookSnapshot, Side, SimOrder};

/// Configuration for the DeLise fill model.
#[derive(Debug, Clone)]
pub struct DeLiseConfig {
    /// Non-adverse fill probability per second (default 0.02).
    pub rf: f64,
    /// Fill probability on an adverse tick (default 0.99).
    pub adverse_fill_prob: f64,
    /// Max queue_ahead for winner fills post-signal (default 50.0 shares).
    pub winner_queue_threshold: f64,
    /// Offset (ms from market open) when signal becomes public info (default 90_000).
    pub signal_offset_ms: i64,
    /// Taker rate multiplier after signal becomes public (default 1.8).
    pub post_signal_taker_mult: f64,
}

impl Default for DeLiseConfig {
    fn default() -> Self {
        Self {
            rf: 0.02,
            adverse_fill_prob: 0.99,
            winner_queue_threshold: 50.0,
            signal_offset_ms: 90_000,
            post_signal_taker_mult: 1.8,
        }
    }
}

/// DeLise 3-rule fill model for prediction markets.
pub struct DeLiseFillModel {
    config: DeLiseConfig,
    /// Deterministic mode for testing — when Some, this value is used
    /// instead of random sampling for the Rf check.
    deterministic_rand: Option<f64>,
}

impl DeLiseFillModel {
    pub fn new(config: DeLiseConfig) -> Self {
        Self {
            config,
            deterministic_rand: None,
        }
    }

    /// Create with deterministic random value for testing.
    /// The value is used in place of rand::random::<f64>() for Rf checks.
    #[cfg(test)]
    pub fn new_deterministic(config: DeLiseConfig, rand_val: f64) -> Self {
        Self {
            config,
            deterministic_rand: Some(rand_val),
        }
    }

    /// Sample a uniform [0, 1) value, or use the deterministic override.
    fn sample_uniform(&self) -> f64 {
        match self.deterministic_rand {
            Some(v) => v,
            None => rand::random::<f64>(),
        }
    }

    /// Compute fill probability for the non-adverse (Rf) path.
    ///
    /// Probability scales with elapsed time in seconds: P = 1 - (1 - rf)^dt_secs.
    /// After signal, taker rate increases by post_signal_taker_mult.
    fn rf_fill_probability(&self, dt_ms: i64, is_post_signal: bool) -> f64 {
        let dt_secs = (dt_ms as f64) / 1000.0;
        if dt_secs <= 0.0 {
            return 0.0;
        }
        let rf = if is_post_signal {
            self.config.rf * self.config.post_signal_taker_mult
        } else {
            self.config.rf
        };
        // P(fill in dt) = 1 - (1 - rf)^dt
        1.0 - (1.0 - rf).powf(dt_secs)
    }
}

impl FillModel for DeLiseFillModel {
    fn name(&self) -> &str {
        "delise-3rule"
    }

    fn create_order(
        &self,
        side: Side,
        price: f64,
        shares: f64,
        snap: &BookSnapshot,
        offset_ms: i64,
    ) -> SimOrder {
        let queue_ahead = queue::queue_position(snap, side, price);
        SimOrder {
            side,
            price,
            shares,
            placed_at_ms: offset_ms,
            queue_ahead,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }
    }

    fn process_tick(
        &self,
        snap: &BookSnapshot,
        orders: &mut [SimOrder],
        prev_offset_ms: i64,
    ) -> Vec<usize> {
        let dt_ms = snap.offset_ms - prev_offset_ms;
        let mut filled_indices = Vec::new();

        for (i, order) in orders.iter_mut().enumerate() {
            if order.filled {
                continue;
            }

            let is_post_signal = snap.offset_ms >= self.config.signal_offset_ms;

            // Rule 1: Adverse tick — best_ask <= our bid price
            if queue::is_adverse_tick(snap, order.side, order.price) {
                // Estimate sweep volume from the ask size at our price
                let state = queue::side_state(snap, order.side);
                let sweep_volume = state.best_ask_size.unwrap_or(0.0);

                // Advance queue consumed by sweep volume
                order.queue_consumed += sweep_volume;

                // If sweep clears through our position, fill with adverse_fill_prob
                if order.queue_consumed >= order.queue_ahead {
                    if self.sample_uniform() < self.config.adverse_fill_prob {
                        order.filled = true;
                        order.filled_at_ms = Some(snap.offset_ms);
                        filled_indices.push(i);
                    }
                }
                continue;
            }

            // Rule 2: Non-adverse tick — small probability of fill from retail flow
            let fill_prob = self.rf_fill_probability(dt_ms, is_post_signal);
            if self.sample_uniform() < fill_prob {
                order.filled = true;
                order.filled_at_ms = Some(snap.offset_ms);
                filled_indices.push(i);
            }
        }

        filled_indices
    }

    fn adverse_selection_filter(&self, order: &SimOrder, is_winner: bool) -> bool {
        let fill_offset = match order.filled_at_ms {
            Some(ms) => ms,
            None => return false, // unfilled orders don't survive
        };

        if fill_offset < self.config.signal_offset_ms {
            // Pre-signal: both winner and loser fills are equally realistic
            return true;
        }

        // Post-signal fills
        if is_winner {
            // Winner fill post-signal is only realistic if we were early in queue.
            // Remaining queue when filled = queue_ahead - queue_consumed (clamped to 0).
            let remaining = (order.queue_ahead - order.queue_consumed).max(0.0);
            remaining < self.config.winner_queue_threshold
        } else {
            // Loser fill post-signal: always realistic
            // (informed traders are happy to sell you losers)
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PriceLevel, SideState};

    fn make_side(
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        best_ask_size: Option<f64>,
        depth: Vec<(f64, f64)>,
    ) -> SideState {
        SideState {
            best_bid,
            best_bid_size: best_bid.map(|_| 100.0),
            best_ask,
            best_ask_size,
            depth: depth
                .into_iter()
                .map(|(price, cumulative_size)| PriceLevel {
                    price,
                    cumulative_size,
                })
                .collect(),
            total_bid_depth: 0.0,
            total_ask_depth: 0.0,
        }
    }

    fn make_snap_with(
        offset_ms: i64,
        yes: SideState,
        no: SideState,
    ) -> BookSnapshot {
        BookSnapshot {
            market_id: "test".to_string(),
            offset_ms,
            timestamp_ms: offset_ms,
            yes,
            no,
            reference_price: None,
            oracle_price: None,
        }
    }

    fn default_snap(offset_ms: i64) -> BookSnapshot {
        make_snap_with(
            offset_ms,
            make_side(Some(0.49), Some(0.51), Some(100.0), vec![(0.49, 200.0)]),
            make_side(Some(0.49), Some(0.51), Some(100.0), vec![(0.49, 200.0)]),
        )
    }

    #[test]
    fn test_create_order_captures_queue_position() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let snap = default_snap(5000);
        let order = model.create_order(Side::Yes, 0.49, 10.0, &snap, 5000);

        assert_eq!(order.side, Side::Yes);
        assert_eq!(order.price, 0.49);
        assert_eq!(order.shares, 10.0);
        assert_eq!(order.placed_at_ms, 5000);
        assert_eq!(order.queue_ahead, 200.0);
        assert!(!order.filled);
    }

    #[test]
    fn test_create_order_empty_book() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let snap = make_snap_with(
            1000,
            make_side(None, None, None, vec![]),
            SideState::default(),
        );
        let order = model.create_order(Side::Yes, 0.49, 10.0, &snap, 1000);

        // No depth => queue_ahead = 0
        assert_eq!(order.queue_ahead, 0.0);
    }

    #[test]
    fn test_adverse_tick_fill() {
        // Deterministic rand=0.0 means always < adverse_fill_prob (0.99)
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);

        let snap = make_snap_with(
            2000,
            // best_ask at 0.49 == our bid => adverse tick
            // best_ask_size 300 > queue_ahead 200 => sweep fills us
            make_side(Some(0.49), Some(0.49), Some(300.0), vec![(0.49, 200.0)]),
            SideState::default(),
        );

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        let filled = model.process_tick(&snap, &mut orders, 1000);
        assert_eq!(filled, vec![0]);
        assert!(orders[0].filled);
        assert_eq!(orders[0].filled_at_ms, Some(2000));
    }

    #[test]
    fn test_adverse_tick_insufficient_sweep() {
        // Sweep volume (50) < queue_ahead (200) => no fill
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);

        let snap = make_snap_with(
            2000,
            make_side(Some(0.49), Some(0.49), Some(50.0), vec![(0.49, 200.0)]),
            SideState::default(),
        );

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        let filled = model.process_tick(&snap, &mut orders, 1000);
        assert!(filled.is_empty());
        assert!(!orders[0].filled);
        // But queue_consumed should have advanced
        assert!((orders[0].queue_consumed - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rf_fill_non_adverse() {
        // Rand=0.0 means always < rf probability => fills via Rf path
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);
        let snap = default_snap(2000);

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        let filled = model.process_tick(&snap, &mut orders, 1000);
        assert_eq!(filled, vec![0]);
        assert!(orders[0].filled);
    }

    #[test]
    fn test_rf_no_fill_high_rand() {
        // Rand=0.999 => exceeds Rf probability for 1 second => no fill
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.999);
        let snap = default_snap(2000);

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        let filled = model.process_tick(&snap, &mut orders, 1000);
        assert!(filled.is_empty());
        assert!(!orders[0].filled);
    }

    #[test]
    fn test_already_filled_order_skipped() {
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);
        let snap = default_snap(3000);

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(2000),
        }];

        let filled = model.process_tick(&snap, &mut orders, 2000);
        assert!(filled.is_empty());
    }

    #[test]
    fn test_adverse_selection_pre_signal_winner() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(80_000), // before signal_offset_ms (90_000)
        };
        // Pre-signal winner fills always survive
        assert!(model.adverse_selection_filter(&order, true));
    }

    #[test]
    fn test_adverse_selection_pre_signal_loser() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(80_000),
        };
        assert!(model.adverse_selection_filter(&order, false));
    }

    #[test]
    fn test_adverse_selection_post_signal_winner_early_queue() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 30.0, // < winner_queue_threshold (50.0)
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(100_000),
        };
        // Early queue => survives
        assert!(model.adverse_selection_filter(&order, true));
    }

    #[test]
    fn test_adverse_selection_post_signal_winner_late_queue() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 200.0, // >> winner_queue_threshold (50.0)
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(100_000),
        };
        // Late queue + winner + post-signal => blocked
        assert!(!model.adverse_selection_filter(&order, true));
    }

    #[test]
    fn test_adverse_selection_post_signal_loser_always_passes() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 500.0, // large queue, doesn't matter for losers
            queue_consumed: 0.0,
            filled: true,
            filled_at_ms: Some(100_000),
        };
        // Loser fills always survive, even post-signal
        assert!(model.adverse_selection_filter(&order, false));
    }

    #[test]
    fn test_adverse_selection_unfilled_order() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let order = SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 5000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        };
        // Unfilled orders don't survive the filter
        assert!(!model.adverse_selection_filter(&order, true));
    }

    #[test]
    fn test_multiple_orders_mixed_fills() {
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);

        // Adverse tick snap: ask <= bid
        let snap = make_snap_with(
            2000,
            make_side(Some(0.49), Some(0.49), Some(300.0), vec![(0.49, 200.0)]),
            SideState::default(),
        );

        let mut orders = vec![
            // This one fills (queue_ahead=200, sweep=300)
            SimOrder {
                side: Side::Yes,
                price: 0.49,
                shares: 10.0,
                placed_at_ms: 1000,
                queue_ahead: 200.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            },
            // This one already filled — should be skipped
            SimOrder {
                side: Side::Yes,
                price: 0.49,
                shares: 10.0,
                placed_at_ms: 500,
                queue_ahead: 100.0,
                queue_consumed: 100.0,
                filled: true,
                filled_at_ms: Some(1500),
            },
            // This one on No side — no adverse tick on No side => Rf path
            // With rand=0.0 and dt=1000ms, Rf will trigger
            SimOrder {
                side: Side::No,
                price: 0.49,
                shares: 10.0,
                placed_at_ms: 1000,
                queue_ahead: 200.0,
                queue_consumed: 0.0,
                filled: false,
                filled_at_ms: None,
            },
        ];

        let filled = model.process_tick(&snap, &mut orders, 1000);
        assert_eq!(filled, vec![0, 2]);
        assert!(orders[0].filled);
        assert!(orders[2].filled);
    }

    #[test]
    fn test_rf_probability_increases_post_signal() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        let pre = model.rf_fill_probability(1000, false);
        let post = model.rf_fill_probability(1000, true);
        // Post-signal uses rf * post_signal_taker_mult => higher probability
        assert!(post > pre);
    }

    #[test]
    fn test_rf_probability_zero_dt() {
        let model = DeLiseFillModel::new(DeLiseConfig::default());
        assert_eq!(model.rf_fill_probability(0, false), 0.0);
        assert_eq!(model.rf_fill_probability(-100, false), 0.0);
    }

    #[test]
    fn test_cumulative_sweep_across_ticks() {
        // Two adverse ticks, each with 120 sweep volume. Queue ahead = 200.
        // First tick: consumed 120, remaining 80 => no fill
        // Second tick: consumed 240, remaining -40 => fill
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);

        let snap1 = make_snap_with(
            2000,
            make_side(Some(0.49), Some(0.49), Some(120.0), vec![(0.49, 200.0)]),
            SideState::default(),
        );

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 1000,
            queue_ahead: 200.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        // First tick: no fill yet
        let filled = model.process_tick(&snap1, &mut orders, 1000);
        assert!(filled.is_empty());
        assert!((orders[0].queue_consumed - 120.0).abs() < f64::EPSILON);

        let snap2 = make_snap_with(
            3000,
            make_side(Some(0.49), Some(0.49), Some(120.0), vec![(0.49, 80.0)]),
            SideState::default(),
        );

        // Second tick: fill
        let filled = model.process_tick(&snap2, &mut orders, 2000);
        assert_eq!(filled, vec![0]);
        assert!(orders[0].filled);
        assert_eq!(orders[0].filled_at_ms, Some(3000));
    }

    #[test]
    fn test_zero_depth_immediate_fill_on_adverse() {
        // queue_ahead=0, adverse tick => fills immediately
        let model = DeLiseFillModel::new_deterministic(DeLiseConfig::default(), 0.0);

        let snap = make_snap_with(
            1000,
            make_side(Some(0.49), Some(0.49), Some(10.0), vec![]),
            SideState::default(),
        );

        let mut orders = vec![SimOrder {
            side: Side::Yes,
            price: 0.49,
            shares: 10.0,
            placed_at_ms: 500,
            queue_ahead: 0.0,
            queue_consumed: 0.0,
            filled: false,
            filled_at_ms: None,
        }];

        let filled = model.process_tick(&snap, &mut orders, 500);
        assert_eq!(filled, vec![0]);
        assert!(orders[0].filled);
    }
}
