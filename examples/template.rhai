// Template Strategy
// A blank starting point for writing your own PhantomFill strategy.
// Copy this file, rename it, and fill in your logic.
//
// Usage: pf run --script examples/template.rhai --db path/to/spread_arb.db

// ── State variables ──────────────────────────────────────────────
// Top-level `let` bindings are accessible inside all functions.
// Reset them in on_reset() so each market window starts clean.

// let my_flag = false;
// let my_counter = 0;

// ── Constants (injected from CLI flags) ──────────────────────────
// SHARES    — f64, from --shares flag
// BID_PRICE — f64, from --bid-price flag

// ── Snapshot properties (passed to on_tick / on_market_open) ─────
//
// Prices & sizes:
//   snap.yes_bid            f64   YES best bid (0.0 if absent)
//   snap.yes_ask            f64   YES best ask (0.0 if absent)
//   snap.yes_bid_size       f64   YES best bid size
//   snap.yes_ask_size       f64   YES best ask size
//   snap.yes_total_bid_depth f64  total YES bid depth
//
//   snap.no_bid             f64   NO best bid (0.0 if absent)
//   snap.no_ask             f64   NO best ask (0.0 if absent)
//   snap.no_bid_size        f64   NO best bid size
//   snap.no_ask_size        f64   NO best ask size
//   snap.no_total_bid_depth f64   total NO bid depth
//
// Oracle & time:
//   snap.oracle_price       f64   BTC oracle price (0.0 if absent)
//   snap.offset_ms          i64   milliseconds since market open
//   snap.timestamp_ms       i64   absolute Unix timestamp (ms)

// ── Action functions ─────────────────────────────────────────────
// bid(side, price, shares)  — place a limit order ("yes" or "no")
// cancel(side)              — cancel open order on side ("yes" or "no")
//
// Return an array of actions from on_tick. Examples:
//   [bid("yes", 0.49, 100.0)]
//   [cancel("no")]
//   [bid("yes", BID_PRICE, SHARES), bid("no", BID_PRICE, SHARES)]

// ── on_market_open (optional) ────────────────────────────────────
// Called once on the first tick of each market window.
//
// fn on_market_open(snap) {
//     // capture opening state
// }

// ── on_tick (required) ───────────────────────────────────────────
// Called on every tick. Must return an array of actions (can be empty).

fn on_tick(snap) {
    // Your strategy logic here.
    []
}

// ── on_reset (required) ──────────────────────────────────────────
// Called between market windows. Reset all state variables here.

fn on_reset() {
    // my_flag = false;
    // my_counter = 0;
}
