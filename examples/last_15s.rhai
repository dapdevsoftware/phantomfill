// Last 15 Seconds
// Time-gated conviction play. Waits until the final 15 seconds of a
// 15-minute window (offset >= 885,000 ms), then buys whichever side
// has a best bid at $0.98 or above -- betting the market has already
// decided the winner.
//
// Usage: pf run --script examples/last_15s.rhai --db path/to/spread_arb.db

let acted = false;

fn on_tick(snap) {
    if acted {
        return [];
    }

    // Wait until 15 seconds before window close (15min = 900,000ms)
    if snap.offset_ms < 885000 {
        return [];
    }

    let yes_bid = snap.yes_bid;
    let no_bid = snap.no_bid;

    // Buy YES if it looks like the winner
    if yes_bid >= 0.98 && yes_bid >= no_bid {
        acted = true;
        return [bid("yes", yes_bid, SHARES)];
    }

    // Buy NO if it looks like the winner
    if no_bid >= 0.98 {
        acted = true;
        return [bid("no", no_bid, SHARES)];
    }

    []
}

fn on_reset() {
    acted = false;
}
