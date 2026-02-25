// Spread Arb
// The simplest strategy: bid both YES and NO at BID_PRICE on the first
// tick of each window. Never cancel. Profits when both sides fill and
// the combined cost is less than $1.00.
//
// Usage: pf run --script examples/spread_arb.rhai --db path/to/spread_arb.db

let placed = false;

fn on_tick(snap) {
    if placed {
        return [];
    }
    placed = true;
    [bid("yes", BID_PRICE, SHARES), bid("no", BID_PRICE, SHARES)]
}

fn on_reset() {
    placed = false;
}
