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
    /// Cumulative bid depth at or above a given price.
    pub fn bid_depth_at(&self, price: f64) -> f64 {
        self.depth
            .iter()
            .filter(|l| l.price >= price)
            .map(|l| l.cumulative_size)
            .last()
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
