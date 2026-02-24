use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// Post both + cancel loser strategy.
///
/// At T+0: place YES + NO bids (like spread_arb).
/// At signal_offset_ms: read momentum signal.
///   - If strong enough: cancel predicted LOSER side (keep winner bid).
///   - If too weak: cancel BOTH sides (avoid blind exposure).
///
/// This is the consensus "best viable" strategy from expert analysis.
pub struct PostBothCancelLoser {
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    signal_offset_ms: i64,
    open_oracle: Option<f64>,
    placed: bool,
    signal_acted: bool,
}

impl PostBothCancelLoser {
    pub fn new(bid_price: f64, shares: f64, min_bps: f64, signal_offset_ms: i64) -> Self {
        Self {
            bid_price,
            shares,
            min_bps,
            signal_offset_ms,
            open_oracle: None,
            placed: false,
            signal_acted: false,
        }
    }
}

impl Strategy for PostBothCancelLoser {
    fn name(&self) -> &str {
        "post_cancel"
    }

    fn description(&self) -> &str {
        "Post both + cancel loser: bid both at T+0, cancel predicted loser at signal time"
    }

    fn on_market_open(&mut self, snap: &BookSnapshot) {
        self.open_oracle = snap.oracle_price;
    }

    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action> {
        let mut actions = vec![];

        // Phase 1: place both bids on first tick
        if !self.placed {
            self.placed = true;
            actions.push(Action::PlaceBid {
                side: Side::Yes,
                price: self.bid_price,
                shares: self.shares,
            });
            actions.push(Action::PlaceBid {
                side: Side::No,
                price: self.bid_price,
                shares: self.shares,
            });
            return actions;
        }

        // Phase 2: cancel at signal time
        if self.signal_acted || snap.offset_ms < self.signal_offset_ms {
            return actions;
        }
        self.signal_acted = true;

        let (open, current) = match (self.open_oracle, snap.oracle_price) {
            (Some(o), Some(c)) if o != 0.0 => (o, c),
            // No oracle data => cancel both to be safe
            _ => {
                actions.push(Action::Cancel { side: Side::Yes });
                actions.push(Action::Cancel { side: Side::No });
                return actions;
            }
        };

        let momentum_bps = (current - open) / open * 10_000.0;

        if momentum_bps.abs() < self.min_bps {
            // Weak signal => cancel both
            actions.push(Action::Cancel { side: Side::Yes });
            actions.push(Action::Cancel { side: Side::No });
        } else {
            // Cancel the predicted loser
            let loser = if momentum_bps > 0.0 {
                Side::No
            } else {
                Side::Yes
            };
            actions.push(Action::Cancel { side: loser });
        }

        actions
    }

    fn reset(&mut self) {
        self.open_oracle = None;
        self.placed = false;
        self.signal_acted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::make_test_snap;

    #[test]
    fn places_both_sides_on_first_tick() {
        let mut strat = PostBothCancelLoser::new(0.49, 100.0, 20.0, 90_000);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&snap);

        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (
                Action::PlaceBid { side: s1, .. },
                Action::PlaceBid { side: s2, .. },
            ) => {
                assert_eq!(*s1, Side::Yes);
                assert_eq!(*s2, Side::No);
            }
            _ => panic!("expected two PlaceBid actions"),
        }
    }

    #[test]
    fn cancels_loser_on_strong_positive_signal() {
        let mut strat = PostBothCancelLoser::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);
        strat.on_tick(&open_snap); // place bids

        // +40 bps => Yes predicted winner, cancel No
        let snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Cancel { side } => assert_eq!(*side, Side::No),
            _ => panic!("expected Cancel"),
        }
    }

    #[test]
    fn cancels_loser_on_strong_negative_signal() {
        let mut strat = PostBothCancelLoser::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);
        strat.on_tick(&open_snap);

        // -40 bps => No predicted winner, cancel Yes
        let snap = make_test_snap(90_000, Some(49800.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::Cancel { side } => assert_eq!(*side, Side::Yes),
            _ => panic!("expected Cancel"),
        }
    }

    #[test]
    fn cancels_both_on_weak_signal() {
        let mut strat = PostBothCancelLoser::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);
        strat.on_tick(&open_snap);

        // +5 bps < 20 min_bps => cancel both
        let snap = make_test_snap(90_000, Some(50025.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Cancel { side: s1 }, Action::Cancel { side: s2 }) => {
                assert_eq!(*s1, Side::Yes);
                assert_eq!(*s2, Side::No);
            }
            _ => panic!("expected two Cancel actions"),
        }
    }

    #[test]
    fn cancels_both_on_no_oracle_data() {
        let mut strat = PostBothCancelLoser::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, None, 500.0, 500.0);
        strat.on_market_open(&open_snap);
        strat.on_tick(&open_snap);

        let snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 2);
        match (&actions[0], &actions[1]) {
            (Action::Cancel { side: s1 }, Action::Cancel { side: s2 }) => {
                assert_eq!(*s1, Side::Yes);
                assert_eq!(*s2, Side::No);
            }
            _ => panic!("expected two Cancel actions"),
        }
    }
}
