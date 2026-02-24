pub mod depth;
pub mod momentum;
pub mod post_cancel;
pub mod spread_arb;

use crate::types::{Action, BookSnapshot};

/// Trait for trading strategies.
///
/// Strategies observe orderbook snapshots and emit actions (place bids, cancel orders).
/// They are stateful: `on_market_open` is called once per window, `on_tick` on every snapshot,
/// and `reset` between windows.
pub trait Strategy: Send {
    fn name(&self) -> &str;
    fn description(&self) -> &str;

    /// Called once on the first snapshot of a market window.
    fn on_market_open(&mut self, _snap: &BookSnapshot) {}

    /// Called on each tick. Returns a list of actions to execute.
    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action>;

    /// Reset internal state between market windows.
    fn reset(&mut self);
}

/// Create a strategy by name with the given parameters.
pub fn create_strategy(
    name: &str,
    bid_price: f64,
    shares: f64,
    min_bps: f64,
) -> Option<Box<dyn Strategy>> {
    match name {
        "spread_arb" => Some(Box::new(spread_arb::NaiveSpreadArb::new(bid_price, shares))),
        "momentum" => Some(Box::new(momentum::MomentumSignal::new(
            bid_price, shares, min_bps, 90_000,
        ))),
        "post_cancel" => Some(Box::new(post_cancel::PostBothCancelLoser::new(
            bid_price, shares, min_bps, 90_000,
        ))),
        "depth" => Some(Box::new(depth::DepthMomentum::new(
            bid_price, shares, min_bps, 90_000,
        ))),
        _ => None,
    }
}

/// List all available strategy names and descriptions.
pub fn list_strategies() -> Vec<(&'static str, &'static str)> {
    vec![
        ("spread_arb", "Naive spread arb: bid both sides at T+0, never cancel"),
        ("momentum", "Momentum signal: wait for oracle price movement, bet on predicted winner"),
        ("post_cancel", "Post both + cancel loser: bid both at T+0, cancel predicted loser at signal time"),
        ("depth", "Depth + momentum: like momentum but also requires orderbook depth agreement"),
    ]
}

#[cfg(test)]
pub(crate) fn make_test_snap(
    offset_ms: i64,
    oracle_price: Option<f64>,
    yes_depth: f64,
    no_depth: f64,
) -> BookSnapshot {
    use crate::types::{PriceLevel, SideState};

    BookSnapshot {
        market_id: "test-market".to_string(),
        offset_ms,
        timestamp_ms: 1_700_000_000_000 + offset_ms,
        yes: SideState {
            best_bid: Some(0.49),
            best_bid_size: Some(yes_depth),
            best_ask: Some(0.51),
            best_ask_size: Some(100.0),
            depth: vec![PriceLevel {
                price: 0.49,
                cumulative_size: yes_depth,
            }],
            total_bid_depth: yes_depth,
            total_ask_depth: 100.0,
        },
        no: SideState {
            best_bid: Some(0.49),
            best_bid_size: Some(no_depth),
            best_ask: Some(0.51),
            best_ask_size: Some(100.0),
            depth: vec![PriceLevel {
                price: 0.49,
                cumulative_size: no_depth,
            }],
            total_bid_depth: no_depth,
            total_ask_depth: 100.0,
        },
        reference_price: None,
        oracle_price,
    }
}
