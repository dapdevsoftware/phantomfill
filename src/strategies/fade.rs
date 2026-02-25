use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Market, Outcome, Side};

/// Direction of a candle / market outcome (local to fade logic).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandleDir {
    Up,
    Down,
}

/// Pre-compute fade signals from a list of markets.
///
/// Groups markets by (category, duration) and walks through each group chronologically.
/// When a streak of `min_streak..=max_streak` consecutive same-direction outcomes is
/// detected, the NEXT window gets a fade signal (bet opposite direction).
///
/// Gap detection: if the gap between consecutive windows exceeds duration + 60s,
/// the streak resets (prevents phantom streaks across time gaps).
pub fn compute_fade_signals(
    markets: &[Market],
    min_streak: usize,
    max_streak: usize,
) -> HashMap<String, Side> {
    let mut signals = HashMap::new();

    // Group by (category, duration_secs) for independent streak tracking.
    let mut groups: HashMap<(&str, i64), Vec<&Market>> = HashMap::new();
    for market in markets {
        if market.outcome.is_some() {
            groups
                .entry((&market.category, market.duration_secs))
                .or_default()
                .push(market);
        }
    }

    for ((_cat, duration), mut group) in groups {
        group.sort_by_key(|m| m.open_ts);

        let mut history: VecDeque<(i64, CandleDir)> = VecDeque::new();
        let max_history = max_streak + 5;

        for i in 0..group.len() {
            let market = group[i];
            let dir = match market.outcome {
                Some(Outcome::Yes) => CandleDir::Up,
                Some(Outcome::No) => CandleDir::Down,
                None => continue,
            };

            history.push_back((market.open_ts, dir));
            while history.len() > max_history {
                history.pop_front();
            }

            // Count consecutive same-direction from the end.
            let mut streak = 0usize;
            let mut prev_ts: Option<i64> = None;

            for &(ts, d) in history.iter().rev() {
                if d != dir {
                    break;
                }
                if let Some(pt) = prev_ts {
                    let gap = pt - ts;
                    if gap > duration + 60 {
                        break;
                    }
                }
                prev_ts = Some(ts);
                streak += 1;
            }

            // If streak in range, signal the NEXT window.
            if streak >= min_streak && streak <= max_streak {
                if let Some(next_market) = group.get(i + 1) {
                    let fade_side = match dir {
                        CandleDir::Up => Side::No,   // streak UP -> fade DOWN -> bet NO
                        CandleDir::Down => Side::Yes, // streak DOWN -> fade UP -> bet YES
                    };
                    signals.insert(next_market.id.clone(), fade_side);
                }
            }
        }
    }

    signals
}

/// Fade momentum strategy: bet against detected streaks of consecutive same-direction candles.
///
/// Uses pre-computed signals from [`compute_fade_signals`]. On market open, looks up whether
/// this window has a fade signal. If yes, places a single bid on the fade side at T=0.
pub struct FadeMomentum {
    bid_price: f64,
    shares: f64,
    signals: Arc<HashMap<String, Side>>,
    current_signal: Option<Side>,
    acted: bool,
}

impl FadeMomentum {
    pub fn new(bid_price: f64, shares: f64, signals: Arc<HashMap<String, Side>>) -> Self {
        Self {
            bid_price,
            shares,
            signals,
            current_signal: None,
            acted: false,
        }
    }
}

impl Strategy for FadeMomentum {
    fn name(&self) -> &str {
        "fade"
    }

    fn description(&self) -> &str {
        "Fade momentum: bet against streaks of consecutive same-direction candles"
    }

    fn on_market_open(&mut self, snap: &BookSnapshot) {
        self.current_signal = self.signals.get(&snap.market_id).cloned();
    }

    fn on_tick(&mut self, _snap: &BookSnapshot) -> Vec<Action> {
        if self.acted {
            return vec![];
        }
        self.acted = true;

        match self.current_signal {
            Some(side) => vec![Action::PlaceBid {
                side,
                price: self.bid_price,
                shares: self.shares,
            }],
            None => vec![],
        }
    }

    fn reset(&mut self) {
        self.current_signal = None;
        self.acted = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Market, Outcome, Platform};

    fn make_market(
        id: &str,
        category: &str,
        open_ts: i64,
        duration: i64,
        outcome: Outcome,
    ) -> Market {
        Market {
            id: id.to_string(),
            platform: Platform::Polymarket,
            description: String::new(),
            category: category.to_string(),
            open_ts,
            close_ts: open_ts + duration,
            duration_secs: duration,
            outcome: Some(outcome),
        }
    }

    #[test]
    fn no_signal_below_min_streak() {
        let markets = vec![
            make_market("m1", "btc", 0, 900, Outcome::Yes),
            make_market("m2", "btc", 900, 900, Outcome::Yes),
            make_market("m3", "btc", 1800, 900, Outcome::No),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        assert!(signals.is_empty());
    }

    #[test]
    fn signal_at_min_streak() {
        let markets = vec![
            make_market("m1", "btc", 0, 900, Outcome::Yes),
            make_market("m2", "btc", 900, 900, Outcome::Yes),
            make_market("m3", "btc", 1800, 900, Outcome::Yes),
            make_market("m4", "btc", 2700, 900, Outcome::No),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        // 3 consecutive Yes (Up) -> m4 gets fade signal: bet No
        assert_eq!(signals.get("m4"), Some(&Side::No));
    }

    #[test]
    fn signal_continues_within_range() {
        let markets = vec![
            make_market("m1", "btc", 0, 900, Outcome::Yes),
            make_market("m2", "btc", 900, 900, Outcome::Yes),
            make_market("m3", "btc", 1800, 900, Outcome::Yes),
            make_market("m4", "btc", 2700, 900, Outcome::Yes),
            make_market("m5", "btc", 3600, 900, Outcome::No),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        // streak=3 at m3 -> signal on m4; streak=4 at m4 -> signal on m5
        assert_eq!(signals.get("m4"), Some(&Side::No));
        assert_eq!(signals.get("m5"), Some(&Side::No));
    }

    #[test]
    fn no_signal_above_max_streak() {
        let markets: Vec<Market> = (0..8)
            .map(|i| {
                make_market(
                    &format!("m{}", i),
                    "btc",
                    i as i64 * 900,
                    900,
                    Outcome::Yes,
                )
            })
            .collect();
        let signals = compute_fade_signals(&markets, 3, 4);
        // streak=3 at m2 -> signal on m3; streak=4 at m3 -> signal on m4
        // streak=5 at m4 -> above max=4, no signal on m5
        assert!(signals.contains_key("m3"));
        assert!(signals.contains_key("m4"));
        assert!(!signals.contains_key("m5"));
    }

    #[test]
    fn gap_resets_streak() {
        let markets = vec![
            make_market("m1", "btc", 0, 900, Outcome::Yes),
            make_market("m2", "btc", 900, 900, Outcome::Yes),
            // Gap: 900 -> 5000 = 4100s >> 960s tolerance
            make_market("m3", "btc", 5000, 900, Outcome::Yes),
            make_market("m4", "btc", 5900, 900, Outcome::No),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        // Gap resets streak: m3 is only 1 after gap
        assert!(signals.is_empty());
    }

    #[test]
    fn different_categories_independent() {
        let markets = vec![
            make_market("b1", "btc", 0, 900, Outcome::Yes),
            make_market("b2", "btc", 900, 900, Outcome::Yes),
            make_market("e1", "eth", 0, 900, Outcome::Yes),
            make_market("e2", "eth", 900, 900, Outcome::Yes),
            make_market("b3", "btc", 1800, 900, Outcome::Yes),
            make_market("e3", "eth", 1800, 900, Outcome::Yes),
            make_market("b4", "btc", 2700, 900, Outcome::No),
            make_market("e4", "eth", 2700, 900, Outcome::No),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        assert_eq!(signals.get("b4"), Some(&Side::No));
        assert_eq!(signals.get("e4"), Some(&Side::No));
    }

    #[test]
    fn fade_down_streak_bets_yes() {
        let markets = vec![
            make_market("m1", "btc", 0, 900, Outcome::No),
            make_market("m2", "btc", 900, 900, Outcome::No),
            make_market("m3", "btc", 1800, 900, Outcome::No),
            make_market("m4", "btc", 2700, 900, Outcome::Yes),
        ];
        let signals = compute_fade_signals(&markets, 3, 6);
        // 3 consecutive No (Down) -> fade Up -> bet Yes
        assert_eq!(signals.get("m4"), Some(&Side::Yes));
    }

    #[test]
    fn strategy_places_bid_when_signal_exists() {
        use crate::strategies::make_test_snap;

        let mut sigs = HashMap::new();
        sigs.insert("test-market".to_string(), Side::No);
        let signals = Arc::new(sigs);

        let mut strat = FadeMomentum::new(0.49, 25.0, signals);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&snap);

        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid { side, price, shares } => {
                assert_eq!(*side, Side::No);
                assert!((price - 0.49).abs() < 0.001);
                assert!((shares - 25.0).abs() < 0.001);
            }
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn strategy_no_action_without_signal() {
        use crate::strategies::make_test_snap;

        let signals = Arc::new(HashMap::new());
        let mut strat = FadeMomentum::new(0.49, 25.0, signals);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&snap);

        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }

    #[test]
    fn strategy_acts_only_once() {
        use crate::strategies::make_test_snap;

        let mut sigs = HashMap::new();
        sigs.insert("test-market".to_string(), Side::Yes);
        let signals = Arc::new(sigs);

        let mut strat = FadeMomentum::new(0.49, 25.0, signals);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&snap);

        let a1 = strat.on_tick(&snap);
        assert_eq!(a1.len(), 1);

        let a2 = strat.on_tick(&snap);
        assert!(a2.is_empty());
    }

    #[test]
    fn strategy_reset_clears_state() {
        use crate::strategies::make_test_snap;

        let mut sigs = HashMap::new();
        sigs.insert("test-market".to_string(), Side::Yes);
        let signals = Arc::new(sigs);

        let mut strat = FadeMomentum::new(0.49, 25.0, signals);
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        strat.on_market_open(&snap);
        strat.on_tick(&snap);
        strat.reset();

        // After reset, no signal (on_market_open not called again)
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());
    }
}
