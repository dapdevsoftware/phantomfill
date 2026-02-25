use std::path::Path;

use anyhow::{bail, Context};
use rhai::{Dynamic, Engine, Map, Scope, AST};

use crate::strategies::Strategy;
use crate::types::{Action, BookSnapshot, Side};

/// A strategy loaded from a Rhai script file.
///
/// Scripts must define `on_tick(snap)` and `on_reset()` functions.
/// An optional `on_market_open(snap)` function is called once per window.
///
/// The script receives `SHARES` and `BID_PRICE` as global constants and
/// can use `bid(side, price, shares)` and `cancel(side)` helper functions.
pub struct RhaiStrategy {
    engine: Engine,
    ast: AST,
    scope: Scope<'static>,
    name: String,
    script_path: String,
    has_on_market_open: bool,
}

impl std::fmt::Debug for RhaiStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RhaiStrategy")
            .field("name", &self.name)
            .field("script_path", &self.script_path)
            .finish()
    }
}

impl RhaiStrategy {
    /// Load a strategy from a `.rhai` file.
    pub fn from_file(path: &Path, shares: f64, bid_price: f64) -> anyhow::Result<Self> {
        let source = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read script: {}", path.display()))?;

        let name = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_else(|| "unknown".to_string());

        let script_path = path.display().to_string();

        Self::from_source(&name, &source, shares, bid_price)
            .with_context(|| format!("failed to load script: {}", script_path))
    }

    /// Load a strategy from source code (useful for testing).
    pub fn from_source(
        name: &str,
        source: &str,
        shares: f64,
        bid_price: f64,
    ) -> anyhow::Result<Self> {
        let mut engine = Engine::new();
        engine.set_optimization_level(rhai::OptimizationLevel::Full);

        // Register helper: bid(side, price, shares) -> action map
        engine.register_fn("bid", |side: &str, price: f64, shares: f64| -> Dynamic {
            let mut map = Map::new();
            map.insert("type".into(), "bid".into());
            map.insert("side".into(), Dynamic::from(side.to_string()));
            map.insert("price".into(), Dynamic::from(price));
            map.insert("shares".into(), Dynamic::from(shares));
            Dynamic::from(map)
        });

        // Register helper: cancel(side) -> action map
        engine.register_fn("cancel", |side: &str| -> Dynamic {
            let mut map = Map::new();
            map.insert("type".into(), "cancel".into());
            map.insert("side".into(), Dynamic::from(side.to_string()));
            Dynamic::from(map)
        });

        // Register depth_at helpers
        engine.register_fn("yes_depth_at", yes_depth_at);
        engine.register_fn("no_depth_at", no_depth_at);

        // Compile the script
        let ast = engine
            .compile(source)
            .map_err(|e| anyhow::anyhow!("compile error: {}", e))?;

        // Verify required functions exist
        let fn_names: Vec<String> = ast.iter_functions().map(|f| f.name.to_string()).collect();

        if !fn_names.iter().any(|n| n == "on_tick") {
            bail!("script must define an `on_tick(snap)` function");
        }
        if !fn_names.iter().any(|n| n == "on_reset") {
            bail!("script must define an `on_reset()` function");
        }

        let has_on_market_open = fn_names.iter().any(|n| n == "on_market_open");

        // Set up scope with constants
        let mut scope = Scope::new();
        scope.push_constant("SHARES", shares);
        scope.push_constant("BID_PRICE", bid_price);

        // Run the top-level script once to initialize any global state
        engine
            .run_ast_with_scope(&mut scope, &ast)
            .map_err(|e| anyhow::anyhow!("initialization error: {}", e))?;

        Ok(Self {
            engine,
            ast,
            scope,
            name: name.to_string(),
            script_path: name.to_string(),
            has_on_market_open,
        })
    }
}

impl Strategy for RhaiStrategy {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> &str {
        &self.script_path
    }

    fn on_market_open(&mut self, snap: &BookSnapshot) {
        if !self.has_on_market_open {
            return;
        }
        let snap_map = snap_to_dynamic(snap);
        if let Err(e) = self.engine.call_fn::<Dynamic>(
            &mut self.scope,
            &self.ast,
            "on_market_open",
            (snap_map,),
        ) {
            tracing::warn!(script = %self.name, "on_market_open error: {}", e);
        }
    }

    fn on_tick(&mut self, snap: &BookSnapshot) -> Vec<Action> {
        let snap_map = snap_to_dynamic(snap);
        match self
            .engine
            .call_fn::<Dynamic>(&mut self.scope, &self.ast, "on_tick", (snap_map,))
        {
            Ok(result) => parse_actions(result),
            Err(e) => {
                tracing::warn!(script = %self.name, "on_tick error: {}", e);
                vec![]
            }
        }
    }

    fn reset(&mut self) {
        if let Err(e) =
            self.engine
                .call_fn::<Dynamic>(&mut self.scope, &self.ast, "on_reset", ())
        {
            tracing::warn!(script = %self.name, "on_reset error: {}", e);
        }
    }
}

/// Convert a BookSnapshot into a Rhai Dynamic map.
fn snap_to_dynamic(snap: &BookSnapshot) -> Dynamic {
    let mut map = Map::new();

    // Yes side
    map.insert(
        "yes_bid".into(),
        Dynamic::from(snap.yes.best_bid.unwrap_or(0.0)),
    );
    map.insert(
        "yes_ask".into(),
        Dynamic::from(snap.yes.best_ask.unwrap_or(0.0)),
    );
    map.insert(
        "yes_bid_size".into(),
        Dynamic::from(snap.yes.best_bid_size.unwrap_or(0.0)),
    );
    map.insert(
        "yes_ask_size".into(),
        Dynamic::from(snap.yes.best_ask_size.unwrap_or(0.0)),
    );
    map.insert(
        "yes_total_bid_depth".into(),
        Dynamic::from(snap.yes.total_bid_depth),
    );
    map.insert(
        "yes_total_ask_depth".into(),
        Dynamic::from(snap.yes.total_ask_depth),
    );

    // Store depth arrays for depth_at lookups
    let yes_depth: Vec<Dynamic> = snap
        .yes
        .depth
        .iter()
        .map(|l| {
            let mut lm = Map::new();
            lm.insert("price".into(), Dynamic::from(l.price));
            lm.insert("size".into(), Dynamic::from(l.cumulative_size));
            Dynamic::from(lm)
        })
        .collect();
    map.insert("yes_depth".into(), Dynamic::from(yes_depth));

    // No side
    map.insert(
        "no_bid".into(),
        Dynamic::from(snap.no.best_bid.unwrap_or(0.0)),
    );
    map.insert(
        "no_ask".into(),
        Dynamic::from(snap.no.best_ask.unwrap_or(0.0)),
    );
    map.insert(
        "no_bid_size".into(),
        Dynamic::from(snap.no.best_bid_size.unwrap_or(0.0)),
    );
    map.insert(
        "no_ask_size".into(),
        Dynamic::from(snap.no.best_ask_size.unwrap_or(0.0)),
    );
    map.insert(
        "no_total_bid_depth".into(),
        Dynamic::from(snap.no.total_bid_depth),
    );
    map.insert(
        "no_total_ask_depth".into(),
        Dynamic::from(snap.no.total_ask_depth),
    );

    let no_depth: Vec<Dynamic> = snap
        .no
        .depth
        .iter()
        .map(|l| {
            let mut lm = Map::new();
            lm.insert("price".into(), Dynamic::from(l.price));
            lm.insert("size".into(), Dynamic::from(l.cumulative_size));
            Dynamic::from(lm)
        })
        .collect();
    map.insert("no_depth".into(), Dynamic::from(no_depth));

    // Metadata
    map.insert("offset_ms".into(), Dynamic::from(snap.offset_ms));
    map.insert("timestamp_ms".into(), Dynamic::from(snap.timestamp_ms));
    map.insert(
        "oracle_price".into(),
        Dynamic::from(snap.oracle_price.unwrap_or(0.0)),
    );

    Dynamic::from(map)
}

/// Look up cumulative depth at a price from the yes_depth array in a snap map.
fn yes_depth_at(snap: Map, price: f64) -> f64 {
    depth_at_inner(&snap, "yes_depth", price)
}

/// Look up cumulative depth at a price from the no_depth array in a snap map.
fn no_depth_at(snap: Map, price: f64) -> f64 {
    depth_at_inner(&snap, "no_depth", price)
}

fn depth_at_inner(snap: &Map, key: &str, price: f64) -> f64 {
    const EPSILON: f64 = 1e-9;

    let depth_arr = match snap.get(key) {
        Some(d) => match d.clone().into_array() {
            Ok(arr) => arr,
            Err(_) => return 0.0,
        },
        None => return 0.0,
    };

    // Extract (price, size) from each level map
    let levels: Vec<(f64, f64)> = depth_arr
        .iter()
        .filter_map(|level| {
            let map: Map = level.clone().try_cast()?;
            let lp = map.get("price")?.as_float().ok()?;
            let ls = map.get("size")?.as_float().ok()?;
            Some((lp, ls))
        })
        .collect();

    // Exact match first
    for &(lp, ls) in &levels {
        if (lp - price).abs() < EPSILON {
            return ls;
        }
    }

    // Fallback: nearest level at or above
    let mut best: Option<(f64, f64)> = None;
    for &(lp, ls) in &levels {
        if lp >= price {
            match best {
                Some((bp, _)) if lp < bp => best = Some((lp, ls)),
                None => best = Some((lp, ls)),
                _ => {}
            }
        }
    }

    best.map(|(_, s)| s).unwrap_or(0.0)
}

/// Parse the return value of on_tick into a Vec<Action>.
fn parse_actions(result: Dynamic) -> Vec<Action> {
    let arr = match result.into_array() {
        Ok(a) => a,
        Err(_) => return vec![],
    };

    let mut actions = Vec::new();
    for item in arr {
        if let Some(map) = item.try_cast::<Map>() {
            if let Some(action) = parse_one_action(&map) {
                actions.push(action);
            }
        }
    }
    actions
}

fn parse_one_action(map: &Map) -> Option<Action> {
    let action_type = map.get("type")?.clone().into_string().ok()?;
    let side_str = map.get("side")?.clone().into_string().ok()?;

    let side = match side_str.as_str() {
        "yes" | "Yes" | "YES" => Side::Yes,
        "no" | "No" | "NO" => Side::No,
        _ => return None,
    };

    match action_type.as_str() {
        "bid" => {
            let price = map.get("price")?.as_float().ok()?;
            let shares = map.get("shares")?.as_float().ok()?;
            Some(Action::PlaceBid {
                side,
                price,
                shares,
            })
        }
        "cancel" => Some(Action::Cancel { side }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strategies::make_test_snap;

    #[test]
    fn test_load_valid_script() {
        let source = r#"
fn on_tick(snap) {
    []
}
fn on_reset() {}
"#;
        let strat = RhaiStrategy::from_source("test", source, 10.0, 0.49);
        assert!(strat.is_ok());
        let strat = strat.unwrap();
        assert_eq!(strat.name(), "test");
    }

    #[test]
    fn test_on_tick_returns_actions() {
        let source = r#"
fn on_tick(snap) {
    [bid("yes", BID_PRICE, SHARES)]
}
fn on_reset() {}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid {
                side,
                price,
                shares,
            } => {
                assert_eq!(*side, Side::Yes);
                assert!((price - 0.49).abs() < f64::EPSILON);
                assert!((shares - 10.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn test_on_reset_clears_state() {
        let source = r#"
let count = 0;

fn on_tick(snap) {
    count += 1;
    if count == 1 {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        []
    }
}

fn on_reset() {
    count = 0;
}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);

        // First tick should produce an action
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);

        // Second tick: count=2, empty
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());

        // Reset and try again
        strat.reset();
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_missing_on_tick_errors() {
        let source = r#"
fn on_reset() {}
"#;
        let result = RhaiStrategy::from_source("test", source, 10.0, 0.49);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("on_tick"), "error should mention on_tick: {}", err);
    }

    #[test]
    fn test_missing_on_reset_errors() {
        let source = r#"
fn on_tick(snap) { [] }
"#;
        let result = RhaiStrategy::from_source("test", source, 10.0, 0.49);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("on_reset"),
            "error should mention on_reset: {}",
            err
        );
    }

    #[test]
    fn test_compile_error_reports_line() {
        let source = r#"
fn on_tick(snap) {
    let x = ;
}
fn on_reset() {}
"#;
        let result = RhaiStrategy::from_source("test", source, 10.0, 0.49);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("compile error"),
            "should be a compile error: {}",
            err
        );
    }

    #[test]
    fn test_bid_and_cancel_actions() {
        let source = r#"
fn on_tick(snap) {
    [bid("yes", 0.49, 10.0), cancel("no")]
}
fn on_reset() {}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 2);
        match &actions[0] {
            Action::PlaceBid { side, .. } => assert_eq!(*side, Side::Yes),
            _ => panic!("expected PlaceBid"),
        }
        match &actions[1] {
            Action::Cancel { side } => assert_eq!(*side, Side::No),
            _ => panic!("expected Cancel"),
        }
    }

    #[test]
    fn test_constants_injected() {
        let source = r#"
fn on_tick(snap) {
    if SHARES == 25.0 && BID_PRICE == 0.48 {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        []
    }
}
fn on_reset() {}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 25.0, 0.48).unwrap();
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);

        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::PlaceBid {
                price, shares, ..
            } => {
                assert!((price - 0.48).abs() < f64::EPSILON);
                assert!((shares - 25.0).abs() < f64::EPSILON);
            }
            _ => panic!("expected PlaceBid"),
        }
    }

    #[test]
    fn test_snap_fields_accessible() {
        let source = r#"
fn on_tick(snap) {
    if snap.yes_bid > 0.0 && snap.no_bid > 0.0 && snap.offset_ms >= 0 {
        [bid("yes", snap.yes_bid, SHARES)]
    } else {
        []
    }
}
fn on_reset() {}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        let snap = make_test_snap(1000, Some(50000.0), 500.0, 500.0);
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_depth_at_functions() {
        let source = r#"
fn on_tick(snap) {
    let yd = yes_depth_at(snap, 0.49);
    if yd > 400.0 {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        []
    }
}
fn on_reset() {}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        // make_test_snap sets yes depth at 0.49 = 500.0
        let snap = make_test_snap(0, Some(50000.0), 500.0, 300.0);
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
    }

    #[test]
    fn test_on_market_open_called() {
        let source = r#"
let initial_oracle = 0.0;

fn on_market_open(snap) {
    initial_oracle = snap.oracle_price;
}

fn on_tick(snap) {
    if initial_oracle > 0.0 {
        [bid("yes", BID_PRICE, SHARES)]
    } else {
        []
    }
}

fn on_reset() {
    initial_oracle = 0.0;
}
"#;
        let mut strat = RhaiStrategy::from_source("test", source, 10.0, 0.49).unwrap();
        let snap = make_test_snap(0, Some(50000.0), 500.0, 500.0);

        // Without calling on_market_open, initial_oracle is 0
        let actions = strat.on_tick(&snap);
        assert!(actions.is_empty());

        // After on_market_open, initial_oracle is set
        strat.on_market_open(&snap);
        let actions = strat.on_tick(&snap);
        assert_eq!(actions.len(), 1);
    }
}
