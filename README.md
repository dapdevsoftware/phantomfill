<p align="center">
  <img src="https://phantomfill.com/favicon.svg" width="80" alt="PhantomFill">
</p>

<h1 align="center">PhantomFill</h1>

<p align="center">
  <strong>The honest prediction market backtester.</strong><br>
  Your backtest is lying to you. PhantomFill shows what would <em>actually</em> happen.
</p>

<p align="center">
  <a href="https://phantomfill.com">Website</a> &middot;
  <a href="#quick-start">Quick Start</a> &middot;
  <a href="#custom-strategies">Custom Strategies</a> &middot;
  <a href="#the-phantom-fill-problem">Why This Exists</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/rust-stable-orange?logo=rust" alt="Rust">
  <img src="https://img.shields.io/badge/tests-160_passing-brightgreen" alt="Tests">
  <img src="https://img.shields.io/badge/license-MIT-blue" alt="License">
</p>

---

## The Phantom Fill Problem

Every prediction market backtester makes the same mistake: **it assumes your limit orders get filled instantly at the price you want.**

In reality:
- Your order sits in a **queue** behind thousands of shares
- The market moves **against you** before you get filled
- Winners get filled last (adverse selection) — losers get filled first
- Your "profitable" strategy is actually **phantom fills** — trades that look good on paper but would never execute in production

PhantomFill uses the **DeLise 3-rule fill model** (academic: DeLise 2024, Lalor & Swishchuk 2024) to simulate realistic queue position, fill probability, and adverse selection. The result is the **phantom fill gap** — the difference between what your backtest says and what would actually happen.

```
=======================================================
  PhantomFill Report: spread_arb + delise-3rule
=======================================================

  Windows:      1770
  Trades taken: 1770    (100.0%)
  Fills:        1651    (93.3% fill rate)
  Correct:      1075    (65.1% WR)

  --- PnL ---------------------------------------------
  Naive paper:     +1157.10       <-- what your backtest says
  Realistic:        -422.80       <-- what actually happens
  Phantom gap:      1579.90       <-- the lie
```

**That gap is $1,579.** A strategy that looks like it makes $1,157 actually *loses* $423.

## Quick Start

### Install

```bash
# Clone the repo
git clone https://github.com/dapdevsoftware/phantomfill.git
cd phantomfill

# Build (requires Rust 1.70+)
cargo build --release

# Binary is at target/release/pf
```

### Get Data

PhantomFill works with Polymarket Up/Down market orderbook data. You can:

**Option A** — Import from a [HuggingFace dataset](https://huggingface.co/datasets/trentmkelly/polymarket_orderbook):
```bash
cargo run --release --bin pf-hf-import -- --input ./data/hf-ndjson/ --output hf.db
```

**Option B** — Import from a live capture database:
```bash
pf import --source ~/.local/share/pm_trader/spread_arb.db --dest my_data.db
```

### Run a Backtest

```bash
# Built-in strategy
pf run -s spread_arb --db ~/.local/share/pm_trader/spread_arb.db

# Custom strategy script
pf run --script examples/post_cancel.rhai --db ~/.local/share/pm_trader/spread_arb.db

# With native PhantomFill format (e.g. HF import)
pf run -s momentum --db hf.db --native

# Monte Carlo (100 runs with confidence intervals)
pf run -s post_cancel --db hf.db --native --runs 100
```

### List Strategies

```bash
$ pf strategies

Available strategies:

  spread_arb       Naive spread arb: bid both sides at T+0, never cancel
  momentum         Momentum signal: wait for oracle price movement, bet on predicted winner
  post_cancel      Post both + cancel loser: bid both at T+0, cancel predicted loser at signal time
  depth            Depth + momentum: like momentum but also requires orderbook depth agreement
  fade             Fade momentum: bet against streaks of consecutive same-direction candles
  last_15s         Last 15 Seconds: buy the side bid at 98c+ in the final 15 seconds
  gabagool         Gabagool combined-price arb: buy YES+NO at different times when combined bid < $1.00
```

## Custom Strategies

Write strategies in **Rhai** (a Rust-native, sandboxed scripting language with JS-like syntax). No Rust knowledge needed.

```javascript
// depth_imbalance.rhai — bet on the side with more orderbook depth
let acted = false;

fn on_tick(snap) {
    if acted { return []; }
    if snap.offset_ms < 60000 { return []; }  // wait 60s

    let yes_depth = snap.yes_total_bid_depth;
    let no_depth = snap.no_total_bid_depth;

    if yes_depth < 10.0 || no_depth < 10.0 { return []; }

    let ratio = if yes_depth > no_depth {
        yes_depth / no_depth
    } else {
        no_depth / yes_depth
    };

    if ratio < 2.0 { return []; }

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
```

Run it:
```bash
pf run --script depth_imbalance.rhai --db hf.db --native --shares 10 --bid-price 0.49
```

### Script API

Every tick, your `on_tick(snap)` function receives a snapshot of the orderbook:

| Property | Type | Description |
|---|---|---|
| `snap.yes_bid` | f64 | YES best bid price |
| `snap.yes_ask` | f64 | YES best ask price |
| `snap.yes_bid_size` | f64 | YES best bid size (shares) |
| `snap.yes_ask_size` | f64 | YES best ask size |
| `snap.yes_total_bid_depth` | f64 | Total YES bid depth |
| `snap.no_bid` | f64 | NO best bid price |
| `snap.no_ask` | f64 | NO best ask price |
| `snap.no_bid_size` | f64 | NO best bid size |
| `snap.no_ask_size` | f64 | NO best ask size |
| `snap.no_total_bid_depth` | f64 | Total NO bid depth |
| `snap.oracle_price` | f64 | BTC/USD oracle price (0.0 if absent) |
| `snap.offset_ms` | i64 | Milliseconds since market open |
| `snap.timestamp_ms` | i64 | Unix timestamp (ms) |

Actions you can return:

| Function | Description |
|---|---|
| `bid(side, price, shares)` | Place a limit bid ("yes" or "no") |
| `cancel(side)` | Cancel existing order on a side |

Built-in constants from CLI flags: `SHARES`, `BID_PRICE`

Required functions: `on_tick(snap)` and `on_reset()`
Optional: `on_market_open(snap)` — called once per window

### Example Scripts

| Script | Strategy | What it does |
|---|---|---|
| [`template.rhai`](examples/template.rhai) | Blank template | Starting point with full API docs |
| [`spread_arb.rhai`](examples/spread_arb.rhai) | Spread Arb | Bid both sides immediately |
| [`gabagool.rhai`](examples/gabagool.rhai) | Combined-Price Arb | Buy YES+NO when combined < $0.99 |
| [`post_cancel.rhai`](examples/post_cancel.rhai) | Post & Cancel | Bid both, cancel predicted loser at 90s |
| [`momentum.rhai`](examples/momentum.rhai) | Momentum | Follow BTC oracle price direction |
| [`last_15s.rhai`](examples/last_15s.rhai) | Last 15 Seconds | Buy the leading side in final 15s |
| [`depth_imbalance.rhai`](examples/depth_imbalance.rhai) | Depth Imbalance | Bet on side with 2x+ more depth |

## Architecture

```
phantomfill/
├── src/
│   ├── bin/
│   │   ├── pf.rs              # CLI entry point
│   │   └── hf_import.rs       # HuggingFace data importer
│   ├── data/
│   │   ├── mod.rs             # DataStore trait
│   │   ├── store.rs           # Native SQLite store
│   │   ├── polymarket.rs      # Polymarket capture DB adapter
│   │   ├── huggingface.rs     # HF NDJSON import adapter
│   │   └── schema.rs          # DB schema definitions
│   ├── fill/
│   │   ├── mod.rs             # Fill model trait
│   │   ├── delise.rs          # DeLise 3-rule fill model
│   │   ├── model.rs           # FillModel interface
│   │   └── queue.rs           # Queue position estimation
│   ├── strategies/
│   │   ├── mod.rs             # Strategy trait + factory
│   │   ├── scripted.rs        # Rhai scripting engine
│   │   ├── spread_arb.rs      # Naive spread arb
│   │   ├── momentum.rs        # Oracle momentum signal
│   │   ├── post_cancel.rs     # Post both + cancel loser
│   │   ├── depth.rs           # Depth + momentum
│   │   ├── gabagool.rs        # Combined-price arb
│   │   ├── last_15s.rs        # Last 15 seconds entry
│   │   └── fade.rs            # Fade momentum streaks
│   ├── replay.rs              # Replay engine (drives simulation)
│   ├── report.rs              # Report generation + Monte Carlo
│   ├── types.rs               # Core types (BookSnapshot, Action, etc.)
│   └── lib.rs                 # Library root
└── examples/                  # Rhai strategy scripts
```

## The DeLise Fill Model

PhantomFill doesn't just check "was price at my level?" — it simulates the full limit order lifecycle:

1. **Queue Position**: When you place an order, you join the back of the queue. Your position is estimated from the total bid depth at your price level.

2. **Adverse Tick Rule**: If the best ask drops to your bid price (adverse tick), you get filled with high probability — but this means the market moved against you.

3. **Non-Adverse Fill**: On normal ticks, there's a small probability (`Rf`) of fill per second from random flow. This correctly models the long waits real limit orders experience.

4. **Post-Signal Adjustment**: After the oracle signal becomes public knowledge (~90s into a 5-minute window), taker activity increases as informed traders act.

This model is calibrated from academic literature on limit order fill dynamics, not from curve-fitting to historical data.

## Monte Carlo Mode

Single backtests can be misleading due to fill randomness. Monte Carlo mode runs your strategy hundreds of times with different RNG seeds:

```bash
pf run -s post_cancel --db hf.db --native --runs 100

=======================================================
  Monte Carlo Summary: post_cancel + delise-3rule (100 runs)
=======================================================
  Naive paper PnL:      +355.20  (deterministic)
  Realistic PnL (mean): +278.40
  Realistic PnL (p5):   +198.60
  Realistic PnL (p95):  +342.10
  Std dev:               44.20
  Phantom gap (median):   73.80
```

The p5/p95 range gives you a confidence interval: "95% of the time, this strategy makes between $198 and $342."

## Contributing

PhantomFill is MIT licensed. Contributions welcome.

The most impactful things you can contribute:
- **New strategies** as `.rhai` scripts in `examples/`
- **Data adapters** for other prediction market platforms
- **Fill model improvements** backed by empirical data
- **Bug reports** with reproducible examples

```bash
# Run the test suite (160 tests)
cargo test

# Run with debug logging
RUST_LOG=debug pf run -s spread_arb --db hf.db --native
```

## License

MIT
