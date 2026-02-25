//! Platform-agnostic types for prediction market simulation.

use serde::{Deserialize, Serialize};

/// Supported platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Platform {
    Polymarket,
    Kalshi,
}

impl std::fmt::Display for Platform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Platform::Polymarket => write!(f, "polymarket"),
            Platform::Kalshi => write!(f, "kalshi"),
        }
    }
}

/// Binary outcome side.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Side {
    Yes,
    No,
}

impl Side {
    pub fn opposite(&self) -> Side {
        match self {
            Side::Yes => Side::No,
            Side::No => Side::Yes,
        }
    }

    pub fn label(&self) -> &str {
        match self {
            Side::Yes => "YES",
            Side::No => "NO",
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// The actual outcome of a market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Outcome {
    Yes,
    No,
}

impl Outcome {
    pub fn matches_side(&self, side: Side) -> bool {
        matches!(
            (self, side),
            (Outcome::Yes, Side::Yes) | (Outcome::No, Side::No)
        )
    }

    pub fn label(&self) -> &str {
        match self {
            Outcome::Yes => "YES",
            Outcome::No => "NO",
        }
    }
}

impl std::fmt::Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Metadata about a market (one tradeable window / contract).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Market {
    /// Platform-specific identifier (e.g. Polymarket slug, Kalshi ticker).
    pub id: String,
    pub platform: Platform,
    /// Human-readable description.
    pub description: String,
    /// Asset/category tag (e.g. "btc", "weather", "politics").
    pub category: String,
    /// Market open timestamp (Unix seconds).
    pub open_ts: i64,
    /// Market close/expiry timestamp (Unix seconds).
    pub close_ts: i64,
    /// Duration in seconds.
    pub duration_secs: i64,
    /// Actual outcome (if resolved).
    pub outcome: Option<Outcome>,
}

/// A single orderbook snapshot for one side of a market.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookTick {
    /// Market identifier.
    pub market_id: String,
    /// Which side of the market this tick represents.
    pub side: Side,
    /// Absolute timestamp (Unix milliseconds).
    pub timestamp_ms: i64,
    /// Milliseconds from market open.
    pub offset_ms: i64,

    // Top of book
    pub best_bid: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_size: Option<f64>,

    // Depth at key price levels (cumulative shares at or better than price)
    pub depth: Vec<PriceLevel>,

    // Total book depth
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,

    // External reference prices
    pub reference_price: Option<f64>,
    pub oracle_price: Option<f64>,
}

/// Cumulative depth at a price level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: f64,
    pub cumulative_size: f64,
}

/// Combined snapshot of both sides at approximately the same time.
#[derive(Debug, Clone)]
pub struct BookSnapshot {
    pub market_id: String,
    pub offset_ms: i64,
    pub timestamp_ms: i64,
    pub yes: SideState,
    pub no: SideState,
    /// External reference (e.g. BTC/USD spot price).
    pub reference_price: Option<f64>,
    /// Oracle resolution price (e.g. Chainlink BTC/USD).
    pub oracle_price: Option<f64>,
}

/// State of one side of the book at a point in time.
#[derive(Debug, Clone, Default)]
pub struct SideState {
    pub best_bid: Option<f64>,
    pub best_bid_size: Option<f64>,
    pub best_ask: Option<f64>,
    pub best_ask_size: Option<f64>,
    pub depth: Vec<PriceLevel>,
    pub total_bid_depth: f64,
    pub total_ask_depth: f64,
}

impl SideState {
    /// Cumulative bid depth at a given price level.
    ///
    /// Finds the exact price level (within epsilon) first. If no exact match,
    /// falls back to the nearest level at or above the requested price.
    pub fn bid_depth_at(&self, price: f64) -> f64 {
        const EPSILON: f64 = 1e-9;

        // Exact match first.
        if let Some(level) = self.depth.iter().find(|l| (l.price - price).abs() < EPSILON) {
            return level.cumulative_size;
        }

        // Fallback: nearest level at or above the requested price.
        self.depth
            .iter()
            .filter(|l| l.price >= price)
            .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal))
            .map(|l| l.cumulative_size)
            .unwrap_or(0.0)
    }
}

/// An action a strategy can request.
#[derive(Debug, Clone)]
pub enum Action {
    /// Place a maker buy at `price` for `shares` on the given side.
    PlaceBid {
        side: Side,
        price: f64,
        shares: f64,
    },
    /// Cancel a previously placed order on the given side.
    Cancel { side: Side },
}

/// A simulated order tracked through its lifecycle.
#[derive(Debug, Clone)]
pub struct SimOrder {
    pub side: Side,
    pub price: f64,
    pub shares: f64,
    /// When the order was placed (offset_ms from market open).
    pub placed_at_ms: i64,
    /// Queue depth ahead of us when order was placed.
    pub queue_ahead: f64,
    /// How much queue has been consumed since placement.
    pub queue_consumed: f64,
    /// Whether this order has been filled.
    pub filled: bool,
    /// When filled (offset_ms).
    pub filled_at_ms: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_side_with_depth(levels: Vec<(f64, f64)>) -> SideState {
        SideState {
            best_bid: Some(0.49),
            best_bid_size: Some(100.0),
            best_ask: Some(0.51),
            best_ask_size: Some(100.0),
            depth: levels
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

    // -----------------------------------------------------------------------
    // Regression test: Bug 1 — bid_depth_at returns depth at exact price,
    // not at the last matching level via `.last()`.
    // Three levels: 0.49→500, 0.50→120, 0.51→50.
    // Querying 0.49 must return 500.0, not 50.0 (which `.last()` would give
    // after filtering levels >= 0.49, since 0.49 < 0.50 < 0.51).
    // -----------------------------------------------------------------------
    #[test]
    fn test_bid_depth_at_exact_match_first_level() {
        let side = make_side_with_depth(vec![(0.49, 500.0), (0.50, 120.0), (0.51, 50.0)]);

        // Exact match at 0.49 must return 500.0.
        assert_eq!(
            side.bid_depth_at(0.49),
            500.0,
            "bid_depth_at(0.49) should return 500.0, not the last filtered value"
        );
    }

    #[test]
    fn test_bid_depth_at_exact_match_middle_level() {
        let side = make_side_with_depth(vec![(0.49, 500.0), (0.50, 120.0), (0.51, 50.0)]);

        assert_eq!(
            side.bid_depth_at(0.50),
            120.0,
            "bid_depth_at(0.50) should return 120.0"
        );
    }

    #[test]
    fn test_bid_depth_at_exact_match_last_level() {
        let side = make_side_with_depth(vec![(0.49, 500.0), (0.50, 120.0), (0.51, 50.0)]);

        assert_eq!(
            side.bid_depth_at(0.51),
            50.0,
            "bid_depth_at(0.51) should return 50.0"
        );
    }

    #[test]
    fn test_bid_depth_at_no_exact_match_falls_back_to_nearest_above() {
        // No level at 0.495 — nearest above is 0.50 with 120.0.
        let side = make_side_with_depth(vec![(0.49, 500.0), (0.50, 120.0), (0.51, 50.0)]);

        assert_eq!(
            side.bid_depth_at(0.495),
            120.0,
            "bid_depth_at(0.495) should fall back to nearest level above (0.50 → 120.0)"
        );
    }

    #[test]
    fn test_bid_depth_at_above_all_levels_returns_zero() {
        let side = make_side_with_depth(vec![(0.49, 500.0), (0.50, 120.0)]);

        assert_eq!(
            side.bid_depth_at(0.55),
            0.0,
            "bid_depth_at above all levels should return 0.0"
        );
    }

    #[test]
    fn test_bid_depth_at_empty_depth_returns_zero() {
        let side = make_side_with_depth(vec![]);

        assert_eq!(
            side.bid_depth_at(0.49),
            0.0,
            "bid_depth_at with no depth levels should return 0.0"
        );
    }
}

/// Complete result for one simulated market window.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowResult {
    pub market_id: String,
    pub platform: String,
    pub category: String,
    pub open_ts: i64,
    pub close_ts: i64,
    pub outcome: String,

    // Signal
    pub predicted: Option<String>,
    pub signal_offset_ms: Option<i64>,

    // Order simulation
    pub bid_side: Option<String>,
    pub bid_price: f64,
    pub shares: f64,
    pub filled: bool,
    pub queue_ahead_at_place: f64,
    pub fill_time_ms: Option<i64>,

    // PnL
    pub correct: bool,
    pub realistic_pnl: f64,
    pub naive_pnl: f64,

    // Reference prices
    pub ref_price_open: Option<f64>,
    pub ref_price_close: Option<f64>,
}
