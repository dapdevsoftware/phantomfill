// Momentum
// Follows BTC price movement. Records the oracle price at market open,
// then waits 90 seconds for a signal. If BTC has moved more than 5 bps
// in either direction, bets on the predicted winner (YES if up, NO if
// down).
//
// Usage: pf run --script examples/momentum.rhai --db path/to/spread_arb.db

let open_oracle = 0.0;
let acted = false;

fn on_market_open(snap) {
    open_oracle = snap.oracle_price;
}

fn on_tick(snap) {
    if acted {
        return [];
    }

    // Wait 90 seconds for price signal to develop
    if snap.offset_ms < 90000 {
        return [];
    }

    acted = true;

    // Need valid oracle prices at both open and now
    if open_oracle == 0.0 || snap.oracle_price == 0.0 {
        return [];
    }

    let momentum_bps = (snap.oracle_price - open_oracle) / open_oracle * 10000.0;

    // Skip if momentum is too weak (< 5 bps)
    if momentum_bps.abs() < 5.0 {
        return [];
    }

    // BTC up -> YES, BTC down -> NO
    if momentum_bps > 0.0 {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        [bid("no", BID_PRICE, SHARES)]
    }
}

fn on_reset() {
    open_oracle = 0.0;
    acted = false;
}
