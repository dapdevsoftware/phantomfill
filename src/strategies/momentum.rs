use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// Momentum signal strategy: wait for oracle price movement, then bet on
/// the predicted winner.
///
/// Records oracle_price at market open. At signal_offset_ms, computes
/// momentum_bps = (current - open) / open * 10000. If strong enough,
/// places a single bid on the predicted winning side.
pub struct MomentumSignal {
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    signal_offset_ms: i64,
    open_oracle: Option<f64>,
    acted: bool,
}

impl MomentumSignal {
    pub fn new(bid_price: f64, shares: f64, min_bps: f64, signal_offset_ms: i64) -> Self {
        Self {
            bid_price,
            shares,
            min_bps,
            signal_offset_ms,
            open_oracle: None,
            acted: false,
        }
    }
}

impl Strategy for MomentumSignal {
    fn name(&self) -> &str {
        "momentum"
    }

    fn description(&self) -> &str {
        "Momentum signal: wait for oracle price movement, bet on predicted winner"
    }

    fn on_market_open(&mut self, snap: &BookSnapshot) {
        self.open_oracle = snap.oracle_price;
    }

    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action> {
        if self.acted || snap.offset_ms < self.signal_offset_ms {
            return vec![];
        }
        self.acted = true;

        let (open, current) = match (self.open_oracle, snap.oracle_price) {
            (Some(o), Some(c)) => (o, c),
            _ => return vec![],
        };

        if open == 0.0 {
            return vec![];
        }

        let momentum_bps = (current - open) / open * 10_000.0;

        if momentum_bps.abs() < self.min_bps {
            return vec![];
        }

        let side = if momentum_bps > 0.0 {
            Side::Yes
        } else {
            Side::No
        };

        vec![Action::PlaceBid {
            side,
            price: self.bid_price,
            shares: self.shares,
        }]
    }

    fn reset(&mut self) {
        self.open_oracle = None;
        self.acted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::make_test_snap;

    #[test]
    fn no_action_before_signal_offset() {
        let mut strat = MomentumSignal::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        let snap = make_test_snap(30_000, Some(50100.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn bets_yes_on_positive_momentum() {
        let mut strat = MomentumSignal::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +200 bps = (50100 - 50000) / 50000 * 10000 = 20.0 bps (exactly at min)
        // Need > min_bps, so go higher
        let snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::Yes),
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn bets_no_on_negative_momentum() {
        let mut strat = MomentumSignal::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // -40 bps
        let snap = make_test_snap(90_000, Some(49800.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::No),
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn skips_when_signal_too_weak() {
        let mut strat = MomentumSignal::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +5 bps < 20 min_bps => skip
        let snap = make_test_snap(90_000, Some(50025.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn handles_no_oracle_price() {
        let mut strat = MomentumSignal::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, None, 500.0, 500.0);
        strat.on_market_open(&open_snap);

        let snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }
}
