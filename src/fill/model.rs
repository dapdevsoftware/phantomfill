use crate::types::{BookSnapshot, Side, SimOrder};

/// Trait for fill simulation models.
///
/// Implementors define how limit orders are placed, how queue position evolves,
/// and whether fills survive adverse selection filtering.
pub trait FillModel: Send {
    fn name(&self) -> &str;

    /// Create a new SimOrder based on current book state.
    fn create_order(
        &self,
        side: Side,
        price: f64,
        shares: f64,
        snap: &BookSnapshot,
        offset_ms: i64,
    ) -> SimOrder;

    /// Process a tick: advance queue position, check for fills.
    /// Returns indices of newly filled orders.
    fn process_tick(
        &self,
        snap: &BookSnapshot,
        orders: &mut [SimOrder],
        prev_offset_ms: i64,
    ) -> Vec<usize>;

    /// After outcome is known, apply adverse selection filter.
    /// Returns true if the fill "survives" (is realistic).
    fn adverse_selection_filter(&self, order: &SimOrder, is_winner: bool) -> bool;
}
