use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// "Gabagool" combined-price arb: buy YES and NO at different times when
/// their combined best_bid < $1.00.
///
/// Named after the famous Polymarket bot gabagool22 (86% WR, $246K profit).
/// The strategy watches both sides' best_bid prices. When yes_bid + no_bid < 1.0,
/// it buys whichever side is cheaper first, then waits for the other side to
/// become cheap enough and buys that too. If both legs fill, you profit the
/// difference (1.0 - yes_bid - no_bid) per share regardless of outcome.
///
/// Unlike spread_arb (which bids both at the same price simultaneously),
/// gabagool exploits temporal price dislocations — moments when the two sides
/// are briefly mispriced relative to each other.
pub struct Gabagool {
    shares: f64,
    /// Maximum combined price to trigger (e.g., 0.995 = need at least $0.005 edge).
    max_combined: f64,
    /// Track which sides we've already bid on.
    yes_placed: bool,
    no_placed: bool,
}

impl Gabagool {
    pub fn new(shares: f64, max_combined: f64) -> Self {
        Self {
            shares,
            max_combined,
            yes_placed: false,
            no_placed: false,
        }
    }
}

impl Strategy for Gabagool {
    fn name(&self) -> &str {
        "gabagool"
    }

    fn description(&self) -> &str {
        "Gabagool combined-price arb: buy YES+NO at different times when combined bid < $1.00"
    }

    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action> {
        if self.yes_placed && self.no_placed {
            return vec![];
        }

        let yes_bid = snap.yes.best_bid.unwrap_or(0.0);
        let no_bid = snap.no.best_bid.unwrap_or(0.0);
        let combined = yes_bid + no_bid;

        // Only act when there's a combined discount.
        if combined >= self.max_combined {
            return vec![];
        }

        let mut actions = vec![];

        // Buy the cheaper side first (or both if both are available).
        if !self.yes_placed && !self.no_placed {
            // First entry: buy the cheaper side.
            if yes_bid <= no_bid && yes_bid > 0.0 {
                self.yes_placed = true;
                actions.push(Action::PlaceBid {
                    side: Side::Yes,
                    price: yes_bid,
                    shares: self.shares,
                });
            } else if no_bid > 0.0 {
                self.no_placed = true;
                actions.push(Action::PlaceBid {
                    side: Side::No,
                    price: no_bid,
                    shares: self.shares,
                });
            }
        }

        // Second entry: buy the other side if still cheap enough.
        if self.yes_placed && !self.no_placed && no_bid > 0.0 {
            // We already bought YES at some price. Now buy NO if combined is still good.
            self.no_placed = true;
            actions.push(Action::PlaceBid {
                side: Side::No,
                price: no_bid,
                shares: self.shares,
            });
        } else if self.no_placed && !self.yes_placed && yes_bid > 0.0 {
            self.yes_placed = true;
            actions.push(Action::PlaceBid {
                side: Side::Yes,
                price: yes_bid,
                shares: self.shares,
            });
        }

        actions
    }

    fn reset(&mut self) {
        self.yes_placed = false;
        self.no_placed = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PriceLevel, SideState};

    fn make_snap(offset_ms: i64, yes_bid: f64, no_bid: f64) -> BookSnapshot {
        BookSnapshot {
            market_id: "test".to_string(),
            offset_ms,
            timestamp_ms: 1_700_000_000_000 + offset_ms,
            yes: SideState {
                best_bid: Some(yes_bid),
                best_bid_size: Some(500.0),
                best_ask: Some(yes_bid + 0.01),
                best_ask_size: Some(100.0),
                depth: vec![PriceLevel {
                    price: yes_bid,
                    cumulative_size: 500.0,
                }],
                total_bid_depth: 500.0,
                total_ask_depth: 100.0,
            },
            no: SideState {
                best_bid: Some(no_bid),
                best_bid_size: Some(500.0),
                best_ask: Some(no_bid + 0.01),
                best_ask_size: Some(100.0),
                depth: vec![PriceLevel {
                    price: no_bid,
                    cumulative_size: 500.0,
                }],
                total_bid_depth: 500.0,
                total_ask_depth: 100.0,
            },
            reference_price: None,
            oracle_price: None,
        }
    }

    #[test]
    fn buys_cheaper_side_first() {
        let mut strat = Gabagool::new(10.0, 0.99);
        let snap = make_snap(0, 0.48, 0.50); // combined 0.98 < 0.99
        let actions = strat.on_tick(&snap);
        // Should buy YES (cheaper) first, then NO on same tick.
        assert_eq!(actions.len(), 2);
        match &actions[0] {
            Action::PlaceBid { side, price, .. } => {
                assert_eq!(*side, Side::Yes);
                assert!((price - 0.48).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid YES"),
        }
        match &actions[1] {
            Action::PlaceBid { side, price, .. } => {
                assert_eq!(*side, Side::No);
                assert!((price - 0.50).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid NO"),
        }
    }

    #[test]
    fn skips_when_combined_too_high() {
        let mut strat = Gabagool::new(10.0, 0.99);
        let snap = make_snap(0, 0.50, 0.50); // combined 1.00 >= 0.99
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn no_double_placement() {
        let mut strat = Gabagool::new(10.0, 0.99);
        let snap1 = make_snap(0, 0.48, 0.50);
        strat.on_tick(&snap1);
        let snap2 = make_snap(1000, 0.47, 0.49);
        let actions = strat.on_tick(&snap2);
        assert!(actions.is_empty()); // already placed both
    }

    #[test]
    fn reset_allows_replay() {
        let mut strat = Gabagool::new(10.0, 0.99);
        let snap = make_snap(0, 0.48, 0.50);
        strat.on_tick(&snap);
        strat.reset();
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 2);
    }
}
