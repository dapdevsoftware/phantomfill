// Depth Imbalance Strategy
// Bets on the side with significantly more bid depth, interpreting
// lopsided depth as "smart money" conviction signal.
//
// Logic:
//   - Wait 60 seconds for the book to stabilize
//   - If one side has 2x+ more total bid depth than the other, bet on it
//   - Only act once per window
//
// Usage: pf run --script examples/depth_imbalance.rhai --db hf.db

let acted = false;
let min_wait_ms = 60000;
let depth_ratio = 2.0;

fn on_tick(snap) {
    if acted { return []; }
    if snap.offset_ms < min_wait_ms { return []; }

    let yes_depth = snap.yes_total_bid_depth;
    let no_depth = snap.no_total_bid_depth;

    // Need meaningful depth on both sides to compare
    if yes_depth < 10.0 || no_depth < 10.0 { return []; }

    let ratio = if yes_depth > no_depth {
        yes_depth / no_depth
    } else {
        no_depth / yes_depth
    };

    if ratio < depth_ratio { return []; }

    acted = true;

    if yes_depth > no_depth {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        [bid("no", BID_PRICE, SHARES)]
    }
}

fn on_reset() {
    acted = false;
}
