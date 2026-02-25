use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// "Last 15 Seconds" strategy: wait until the final 15 seconds of a market
/// window, then buy whichever side has a best_bid >= the threshold (default 0.98).
///
/// This replicates the popular Twitter strategy claiming 100% win rates by
/// buying near-certain outcomes just before resolution. The claim is that if
/// one side is bid at $0.98+ with 15 seconds left, it's almost certainly the
/// winner, so you buy it for a quick $0.01-0.02 profit per share.
///
/// In reality, the order book at $0.98 is either empty or has massive queue
/// depth ahead of you. This strategy exists to demonstrate phantom fills.
pub struct Last15Seconds {
    /// Price to bid at (uses the observed best_bid, not a fixed price).
    shares: f64,
    /// Minimum best_bid to trigger entry.
    min_bid: f64,
    /// How many ms before market close to start looking (default 15_000).
    trigger_before_close_ms: i64,
    /// Market duration in ms (set on market open from close_ts - open_ts context,
    /// or defaults to 900_000 for 15m markets).
    window_duration_ms: i64,
    acted: bool,
}

impl Last15Seconds {
    pub fn new(shares: f64, min_bid: f64, window_duration_ms: i64) -> Self {
        Self {
            shares,
            min_bid,
            trigger_before_close_ms: 15_000,
            window_duration_ms,
            acted: false,
        }
    }
}

impl Strategy for Last15Seconds {
    fn name(&self) -> &str {
        "last_15s"
    }

    fn description(&self) -> &str {
        "Last 15 Seconds: buy the side bid at 98c+ in the final 15 seconds"
    }

    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action> {
        if self.acted {
            return vec![];
        }

        let trigger_offset = self.window_duration_ms - self.trigger_before_close_ms;
        if snap.offset_ms < trigger_offset {
            return vec![];
        }

        // Check which side (if any) has a best_bid >= threshold.
        let yes_bid = snap.yes.best_bid.unwrap_or(0.0);
        let no_bid = snap.no.best_bid.unwrap_or(0.0);

        // Pick the side with the higher bid, if it meets the threshold.
        let (side, price) = if yes_bid >= self.min_bid && yes_bid >= no_bid {
            (Side::Yes, yes_bid)
        } else if no_bid >= self.min_bid {
            (Side::No, no_bid)
        } else {
            return vec![];
        };

        self.acted = true;

        vec![Action::PlaceBid {
            side,
            price,
            shares: self.shares,
        }]
    }

    fn reset(&mut self) {
        self.acted = false;
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
    fn no_action_before_trigger_window() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap = make_snap(800_000, 0.99, 0.01); // 100s before close
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn buys_yes_when_bid_high_in_last_15s() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap = make_snap(886_000, 0.99, 0.01); // 14s before close
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, price, shares } => {
                assert_eq!(*side, Side::Yes);
                assert!((price - 0.99).abs() < f64::EPSILON);
                assert!((shares - 10.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn buys_no_when_no_side_bid_high() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap = make_snap(886_000, 0.01, 0.99);
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::No),
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn skips_when_no_side_meets_threshold() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap = make_snap(886_000, 0.50, 0.50);
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn acts_only_once() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap1 = make_snap(886_000, 0.99, 0.01);
        strat.on_tick(&snap1);
        let snap2 = make_snap(890_000, 0.99, 0.01);
        let actions = strat.on_tick(&snap2);
        assert!(actions.is_empty());
    }

    #[test]
    fn reset_allows_replay() {
        let mut strat = Last15Seconds::new(10.0, 0.98, 900_000);
        let snap = make_snap(886_000, 0.99, 0.01);
        strat.on_tick(&snap);
        strat.reset();
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
    }
}
