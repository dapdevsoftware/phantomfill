use crate::types::{BookSnapshot, Side, SideState};

/// Get the SideState for a given Side from a BookSnapshot.
pub fn side_state<'a>(snap: &'a BookSnapshot, side: Side) -> &'a SideState {
    match side {
        Side::Yes => &snap.yes,
        Side::No => &snap.no,
    }
}

/// Estimate queue position (shares ahead) for a new order at `price` on `side`.
///
/// Uses the cumulative bid depth at the given price from the current snapshot.
/// If no depth data is available, returns 0.
pub fn queue_position(snap: &BookSnapshot, side: Side, price: f64) -> f64 {
    let state = side_state(snap, side);
    state.bid_depth_at(price)
}

/// Estimate taker volume consumed between two consecutive snapshots.
///
/// When bid depth at a price level decreases between ticks, the difference
/// represents shares that were taken (filled by incoming sell orders).
/// We only count decreases — increases represent new orders joining the queue.
pub fn estimate_taker_volume(
    prev: &BookSnapshot,
    curr: &BookSnapshot,
    side: Side,
    price: f64,
) -> f64 {
    let prev_depth = side_state(prev, side).bid_depth_at(price);
    let curr_depth = side_state(curr, side).bid_depth_at(price);

    // Depth decreased => shares were taken from the queue
    let decrease = prev_depth - curr_depth;
    if decrease > 0.0 {
        decrease
    } else {
        0.0
    }
}

/// Check if an adverse tick occurred: the best ask dropped to or below our bid price.
///
/// This means someone is aggressively selling into the bids at our price level,
/// sweeping through resting orders.
pub fn is_adverse_tick(snap: &BookSnapshot, side: Side, our_bid: f64) -> bool {
    let state = side_state(snap, side);
    match state.best_ask {
        Some(ask) => ask <= our_bid,
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{BookSnapshot, PriceLevel, SideState};

    fn make_snap(
        best_bid: Option<f64>,
        best_ask: Option<f64>,
        depth: Vec<(f64, f64)>,
    ) -> BookSnapshot {
        let side = SideState {
            best_bid,
            best_bid_size: best_bid.map(|_| 100.0),
            best_ask,
            best_ask_size: best_ask.map(|_| 100.0),
            depth: depth
                .into_iter()
                .map(|(price, cumulative_size)| PriceLevel {
                    price,
                    cumulative_size,
                })
                .collect(),
            total_bid_depth: 0.0,
            total_ask_depth: 0.0,
        };
        BookSnapshot {
            market_id: "test".to_string(),
            offset_ms: 0,
            timestamp_ms: 0,
            yes: side,
            no: SideState::default(),
            reference_price: None,
            oracle_price: None,
        }
    }

    #[test]
    fn test_queue_position_with_depth() {
        let snap = make_snap(Some(0.49), Some(0.51), vec![(0.49, 500.0), (0.48, 800.0)]);
        assert_eq!(queue_position(&snap, Side::Yes, 0.49), 500.0);
    }

    #[test]
    fn test_queue_position_empty_depth() {
        let snap = make_snap(Some(0.49), Some(0.51), vec![]);
        assert_eq!(queue_position(&snap, Side::Yes, 0.49), 0.0);
    }

    #[test]
    fn test_taker_volume_decrease() {
        let prev = make_snap(Some(0.49), Some(0.51), vec![(0.49, 500.0)]);
        let curr = make_snap(Some(0.49), Some(0.51), vec![(0.49, 350.0)]);
        let vol = estimate_taker_volume(&prev, &curr, Side::Yes, 0.49);
        assert!((vol - 150.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_taker_volume_increase_is_zero() {
        let prev = make_snap(Some(0.49), Some(0.51), vec![(0.49, 300.0)]);
        let curr = make_snap(Some(0.49), Some(0.51), vec![(0.49, 500.0)]);
        let vol = estimate_taker_volume(&prev, &curr, Side::Yes, 0.49);
        assert_eq!(vol, 0.0);
    }

    #[test]
    fn test_adverse_tick_detected() {
        let snap = make_snap(Some(0.49), Some(0.49), vec![(0.49, 100.0)]);
        assert!(is_adverse_tick(&snap, Side::Yes, 0.49));
    }

    #[test]
    fn test_no_adverse_tick() {
        let snap = make_snap(Some(0.49), Some(0.51), vec![(0.49, 100.0)]);
        assert!(!is_adverse_tick(&snap, Side::Yes, 0.49));
    }

    #[test]
    fn test_adverse_tick_no_ask() {
        let snap = make_snap(Some(0.49), None, vec![(0.49, 100.0)]);
        assert!(!is_adverse_tick(&snap, Side::Yes, 0.49));
    }
}
