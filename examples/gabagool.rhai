// Gabagool Combined-Price Arb
// Inspired by gabagool22's strategy. Monitors both sides and enters
// when the combined YES bid + NO bid drops below $0.99, buying the
// cheaper side first, then completing the pair on a later tick.
//
// Usage: pf run --script examples/gabagool.rhai --db path/to/spread_arb.db

let yes_placed = false;
let no_placed = false;

fn on_tick(snap) {
    if yes_placed && no_placed {
        return [];
    }

    let combined = snap.yes_bid + snap.no_bid;

    // Only enter when combined price is below $0.99
    if combined >= 0.99 {
        return [];
    }

    let actions = [];

    // First entry: pick the cheaper side
    if !yes_placed && !no_placed {
        if snap.yes_bid <= snap.no_bid && snap.yes_bid > 0.0 {
            yes_placed = true;
            actions.push(bid("yes", snap.yes_bid, SHARES));
        } else if snap.no_bid > 0.0 {
            no_placed = true;
            actions.push(bid("no", snap.no_bid, SHARES));
        }
    }

    // Second entry: complete the pair
    if yes_placed && !no_placed && snap.no_bid > 0.0 {
        no_placed = true;
        actions.push(bid("no", snap.no_bid, SHARES));
    } else if no_placed && !yes_placed && snap.yes_bid > 0.0 {
        yes_placed = true;
        actions.push(bid("yes", snap.yes_bid, SHARES));
    }

    actions
}

fn on_reset() {
    yes_placed = false;
    no_placed = false;
}
