use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// Naive spread arb: place YES + NO bids at T+0, never cancel.
///
/// This is the baseline "dumb" strategy. It always bids both sides at the
/// configured price and hopes both fill for a guaranteed profit.
pub struct NaiveSpreadArb {
    bid_price: f64,
    shares: f64,
    placed: bool,
}

impl NaiveSpreadArb {
    pub fn new(bid_price: f64, shares: f64) -> Self {
        Self {
            bid_price,
            shares,
            placed: false,
        }
    }
}

impl Strategy for NaiveSpreadArb {
    fn name(&self) -> &str {
        "spread_arb"
    }

    fn description(&self) -> &str {
        "Naive spread arb: bid both sides at T+0, never cancel"
    }

    fn on_tick(&mut self, _snap: &BookSnapshot) -> Vec<Action> {
        if self.placed {
            return vec![];
        }
        self.placed = true;
        vec![
            Action::PlaceBid {
                side: Side::Yes,
                price: self.bid_price,
                shares: self.shares,
            },
            Action::PlaceBid {
                side: Side::No,
                price: self.bid_price,
                shares: self.shares,
            },
        ]
    }

    fn reset(&mut self) {
        self.placed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::make_test_snap;

    #[test]
    fn places_both_sides_on_first_tick() {
        let mut strat = NaiveSpreadArb::new(0.49, 100.0);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 2);
        match &actions[0] {
            Action::PlaceBid { side, price, shares } => {
                assert_eq!(*side, Side::Yes);
                assert!((price - 0.49).abs() < f64::EPSILON);
                assert!((shares - 100.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid"),
        }
        match &actions[1] {
            Action::PlaceBid { side, price, shares } => {
                assert_eq!(*side, Side::No);
                assert!((price - 0.49).abs() < f64::EPSILON);
                assert!((shares - 100.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn does_not_place_twice() {
        let mut strat = NaiveSpreadArb::new(0.49, 100.0);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_tick(&snap);

        let snap2 = make_test_snap(1000, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap2);
        assert!(actions.is_empty());
    }

    #[test]
    fn reset_allows_replaying() {
        let mut strat = NaiveSpreadArb::new(0.49, 100.0);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_tick(&snap);
        strat.reset();

        let snap2 = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap2);
        assert_eq!(actions.len(), 2);
    }
}
