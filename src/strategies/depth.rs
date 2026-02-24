use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// Depth + momentum strategy.
///
/// Like MomentumSignal but also checks orderbook depth agreement.
/// At signal_offset_ms: compute momentum AND check which side has more
/// depth at bid_price. Only place order if momentum direction matches
/// depth direction.
///
/// Higher selectivity = fewer trades but (theoretically) higher accuracy.
pub struct DepthMomentum {
    bid_price: f64,
    shares: f64,
    min_bps: f64,
    signal_offset_ms: i64,
    open_oracle: Option<f64>,
    acted: bool,
}

impl DepthMomentum {
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

impl Strategy for DepthMomentum {
    fn name(&self) -> &str {
        "depth"
    }

    fn description(&self) -> &str {
        "Depth + momentum: like momentum but also requires orderbook depth agreement"
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
            (Some(o), Some(c)) if o != 0.0 => (o, c),
            _ => return vec![],
        };

        let momentum_bps = (current - open) / open * 10_000.0;

        if momentum_bps.abs() < self.min_bps {
            return vec![];
        }

        let momentum_side = if momentum_bps > 0.0 {
            Side::Yes
        } else {
            Side::No
        };

        // Check depth agreement: the predicted winner side should have
        // more bid depth (more people betting on it)
        let yes_depth = snap.yes.bid_depth_at(self.bid_price);
        let no_depth = snap.no.bid_depth_at(self.bid_price);

        let depth_side = if yes_depth > no_depth {
            Side::Yes
        } else if no_depth > yes_depth {
            Side::No
        } else {
            // Equal depth => no agreement signal, skip
            return vec![];
        };

        if momentum_side != depth_side {
            return vec![];
        }

        vec![Action::PlaceBid {
            side: momentum_side,
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
    fn places_when_momentum_and_depth_agree() {
        let mut strat = DepthMomentum::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +40 bps positive momentum => Yes predicted
        // yes_depth=800 > no_depth=400 => depth agrees with Yes
        let snap = make_test_snap(90_000, Some(50200.0), 800.0, 400.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::Yes),
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn skips_when_momentum_and_depth_disagree() {
        let mut strat = DepthMomentum::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +40 bps => Yes predicted, but no_depth > yes_depth => disagree
        let snap = make_test_snap(90_000, Some(50200.0), 400.0, 800.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn skips_on_weak_momentum_even_with_depth_signal() {
        let mut strat = DepthMomentum::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +5 bps < 20 min_bps
        let snap = make_test_snap(90_000, Some(50025.0), 800.0, 400.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn skips_on_equal_depth() {
        let mut strat = DepthMomentum::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // +40 bps but equal depth => no agreement
        let snap = make_test_snap(90_000, Some(50200.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn negative_momentum_with_no_depth_agreement() {
        let mut strat = DepthMomentum::new(0.49, 100.0, 20.0, 90_000);
        let open_snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&open_snap);

        // -40 bps => No predicted, no_depth=800 > yes_depth=400 => agrees
        let snap = make_test_snap(90_000, Some(49800.0), 400.0, 800.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::No),
            _ => panic!("expected PlaceBid"),
        }
    }
}
