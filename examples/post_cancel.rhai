// Post & Cancel
// Two-phase strategy. Phase 1: immediately bid both YES and NO at
// BID_PRICE. Phase 2: after 90 seconds, use BTC oracle momentum to
// cancel the predicted losing side. If the signal is too weak or
// oracle data is missing, cancels both sides to avoid risk.
//
// This was the only strategy that survived PhantomFill's realistic
// fill simulation (+$282 real vs +$355 naive).
//
// Usage: pf run --script examples/post_cancel.rhai --db path/to/spread_arb.db

let open_oracle = 0.0;
let placed = false;
let signal_acted = false;

fn on_market_open(snap) {
    open_oracle = snap.oracle_price;
}

fn on_tick(snap) {
    // Phase 1: place both orders immediately
    if !placed {
        placed = true;
        return [bid("yes", BID_PRICE, SHARES), bid("no", BID_PRICE, SHARES)];
    }

    // Phase 2: wait for 90s signal window
    if signal_acted || snap.offset_ms < 90000 {
        return [];
    }

    signal_acted = true;

    // No oracle data -- cancel everything to be safe
    if open_oracle == 0.0 || snap.oracle_price == 0.0 {
        return [cancel("yes"), cancel("no")];
    }

    let momentum_bps = (snap.oracle_price - open_oracle) / open_oracle * 10000.0;

    // Weak signal -- cancel both
    if momentum_bps.abs() < 5.0 {
        return [cancel("yes"), cancel("no")];
    }

    // Strong signal -- cancel the predicted loser
    if momentum_bps > 0.0 {
        // BTC trending up -> YES likely wins -> cancel NO
        [cancel("no")]
    } else {
        // BTC trending down -> NO likely wins -> cancel YES
        [cancel("yes")]
    }
}

fn on_reset() {
    open_oracle = 0.0;
    placed = false;
    signal_acted = false;
}
