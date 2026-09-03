//! The snapshot rotation: which instrument is snapshotted next, and when.
//!
//! # Why a cycle and not an interval
//!
//! `[[feed]] snapshot_cycle` is *one full pass over the published set*, and the
//! per-instrument tick is derived from it and the instrument count. The
//! alternative — an interval each instrument is snapshotted at — has the whole
//! set falling due at the same moment, which is the burst the reference-data
//! specification forbids for definitions and which a snapshot, being a
//! multi-datagram group per instrument, would produce at a far worse scale. The
//! definition cycle in `dz-publisher-refdata` is paced for exactly this reason;
//! this is the same rule for the same hazard, and it is also the shape the one
//! shipped depth publisher runs.
//!
//! # One instrument per tick
//!
//! A snapshot is one book state a subscriber applies whole, so the unit of
//! progress is an instrument and never a level. One per tick keeps a tick's cost
//! O(1) in the size of the published set, and it means a stall degrades into a
//! slower lap rather than a spike.
//!
//! **O(1) is a claim about two calls, and both had to be made true for it.**
//! [`InstrumentTable::holds`] is a bounds check, and
//! [`InstrumentTable::len`] — which [`SnapshotRotation::due`] reads on every
//! tick to derive the per-instrument interval — is a cached count rather than a
//! walk of the slots. It was the walk, which made the pacing arithmetic the
//! most expensive thing in a tick that says here it is constant, and this is the
//! invariant a maintainer would size a large published set against.
//!
//! **What that does not survive**, stated because it is the ceiling and not a
//! detail: a set so large that `cycle / instruments` falls below the runtime's
//! own tick laps more slowly than configured, and a single instrument whose book
//! is enormous still goes out as one group. Both need a level-budget scheduler
//! with mid-group resumption, which is a different design; the seam for it is
//! [`Publisher::periodic_snapshot`](crate::publisher::Publisher::periodic_snapshot)
//! and replacing that changes nothing else in the loop.

use std::time::Duration;

use dz_adapter_core::InstrumentRef;
use dz_publisher_lowering::InstrumentTable;

/// The floor under a derived tick.
///
/// A cycle divided by a large published set rounds towards zero, and a tick of
/// zero is not a cycle at all — it is *every tick*, which would put the whole
/// set on the wire as fast as the loop runs and turn the anti-burst pacing into
/// the burst it exists to prevent. One millisecond is below the runtime's own
/// tick, so the clamp never slows a schedule that was achievable; it only stops
/// an unachievable one from collapsing.
const MIN_TICK: Duration = Duration::from_millis(1);

/// Where the rotation is, and when the next instrument falls due.
#[derive(Debug)]
pub struct SnapshotRotation {
    cycle_ns: u64,
    /// The next slot to consider. An index into the instrument table's slots,
    /// not a count of instruments: a handle is its slot for the lifetime of the
    /// table, so this walks holes and skips them.
    cursor: u32,
    /// Monotonic. `None` until the first pass is scheduled, which happens on the
    /// first call rather than at construction so that a publisher does not owe a
    /// snapshot for the instant it started.
    next_due_ns: Option<u64>,
}

/// The interval between per-instrument snapshots, from the cycle and the count.
///
/// Separate from the type so it can be asserted directly: the arithmetic is the
/// whole of the pacing.
#[must_use]
pub fn tick(cycle: Duration, instruments: usize) -> Duration {
    let instruments = u32::try_from(instruments.max(1)).unwrap_or(u32::MAX);
    (cycle / instruments).max(MIN_TICK)
}

impl SnapshotRotation {
    /// A rotation that completes one pass over the published set every `cycle`.
    #[must_use]
    pub const fn new(cycle: Duration) -> Self {
        Self {
            cycle_ns: cycle.as_nanos() as u64,
            cursor: 0,
            next_due_ns: None,
        }
    }

    /// The cycle this rotation was configured with.
    #[must_use]
    pub const fn cycle(&self) -> Duration {
        Duration::from_nanos(self.cycle_ns)
    }

    /// The next instrument due a snapshot, or `None` if none is yet.
    ///
    /// Advances the cursor past the instrument it returns, so an instrument the
    /// caller cannot snapshot — a book that has not bootstrapped — is skipped
    /// and comes back on the next lap rather than holding the rotation on one
    /// slot. That is the difference between one dormant instrument and a feed
    /// whose snapshots stop.
    ///
    /// An empty table is `None` and schedules nothing: there is no pass to make.
    pub fn due(&mut self, now_ns: u64, instruments: &InstrumentTable) -> Option<InstrumentRef> {
        let slots = u32::try_from(instruments.slots()).unwrap_or(u32::MAX);
        if slots == 0 {
            return None;
        }
        let tick = tick(self.cycle(), instruments.len());

        match self.next_due_ns {
            // The first call schedules the first snapshot rather than taking
            // one: a publisher that has just admitted its instruments has not
            // yet published a delta for any of them, and a snapshot anchored
            // before the first one is a datagram spent to say nothing.
            None => {
                self.next_due_ns = Some(now_ns.saturating_add(tick.as_nanos() as u64));
                return None;
            }
            Some(due) if now_ns < due => return None,
            Some(_) => {}
        }
        // Read off the clock as a debt, not counted in ticks: a tick the process
        // was too busy to serve leaves the next one due immediately rather than
        // pushing the whole lap out by a tick.
        self.next_due_ns = Some(now_ns.saturating_add(tick.as_nanos() as u64));

        // At most one pass over the slots. A table of nothing but holes cannot
        // be walked into progress, and an unbounded search for a held slot
        // would be a tick that never returns.
        for _ in 0..slots {
            let candidate = InstrumentRef::from_admission(self.cursor);
            self.cursor = (self.cursor + 1) % slots;
            if instruments.holds(candidate) {
                return Some(candidate);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dz_publisher_lowering::{Instrument, InstrumentTable};

    /// A table of `n` admitted instruments. Nothing here reads their fields;
    /// what a rotation walks is slots.
    fn table(n: usize) -> InstrumentTable {
        let mut table = InstrumentTable::new();
        for index in 0..n {
            table.admit(instrument(index as u32));
        }
        table
    }

    fn instrument(instrument_id: u32) -> Instrument {
        Instrument {
            instrument_id,
            price_exponent: -2,
            qty_exponent: -2,
            quoted_per_contract: None,
        }
    }

    #[test]
    fn the_tick_is_the_cycle_divided_by_the_published_set() {
        // The whole of the pacing, asserted as arithmetic: one pass over `n`
        // instruments takes `cycle`, so a tick is `cycle / n`.
        assert_eq!(tick(Duration::from_secs(5), 5), Duration::from_secs(1));
        assert_eq!(tick(Duration::from_secs(5), 50), Duration::from_millis(100));
    }

    #[test]
    fn an_empty_set_does_not_divide_by_zero() {
        assert_eq!(tick(Duration::from_secs(5), 0), Duration::from_secs(5));
    }

    #[test]
    fn a_set_too_large_for_the_cycle_is_clamped_rather_than_collapsed() {
        // `5s / 10_000_000` rounds to zero, and a tick of zero is not a slower
        // cycle - it is every tick, which is the burst the pacing exists to
        // prevent.
        assert_eq!(tick(Duration::from_secs(5), 10_000_000), MIN_TICK);
    }

    #[test]
    fn the_first_call_schedules_rather_than_snapshots() {
        // A publisher that has just admitted its instruments has published no
        // delta for any of them, and a snapshot anchored before the first one
        // is a datagram spent to say nothing.
        let mut rotation = SnapshotRotation::new(Duration::from_secs(1));
        assert_eq!(rotation.due(0, &table(1)), None);
    }

    #[test]
    fn one_instrument_per_tick_and_the_pass_wraps() {
        let mut rotation = SnapshotRotation::new(Duration::from_secs(3));
        let table = table(3);
        // One pass over three instruments in three seconds is one per second.
        assert_eq!(rotation.due(0, &table), None);
        let mut taken = Vec::new();
        for second in 1..=6u64 {
            let at = second * 1_000_000_000;
            // Exactly one instrument per due tick, never a batch: a snapshot is
            // several datagrams, so the unit of progress is an instrument.
            taken.push(rotation.due(at, &table).map(InstrumentRef::index));
        }
        assert_eq!(
            taken,
            [Some(0), Some(1), Some(2), Some(0), Some(1), Some(2)],
            "the rotation covers the set in order and laps"
        );
    }

    #[test]
    fn nothing_is_due_before_the_tick_elapses() {
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        let table = table(2);
        assert_eq!(rotation.due(0, &table), None);
        // The tick is one second; half of one is not due.
        assert_eq!(rotation.due(500_000_000, &table), None);
        assert!(rotation.due(1_000_000_000, &table).is_some());
    }

    #[test]
    fn a_withdrawn_instrument_is_skipped_and_its_handle_is_not_reused() {
        // A hole in the table is not the end of the pass: handles are slots for
        // the lifetime of the table, so a delisted instrument leaves one behind
        // and the rotation has to walk past it.
        let mut table = table(3);
        table.withdraw(InstrumentRef::from_admission(1));
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        assert_eq!(rotation.due(0, &table), None);
        let first = rotation
            .due(1_000_000_000, &table)
            .map(InstrumentRef::index);
        let second = rotation
            .due(2_000_000_000, &table)
            .map(InstrumentRef::index);
        assert_eq!((first, second), (Some(0), Some(2)));
    }

    #[test]
    fn a_table_of_nothing_but_holes_returns_and_does_not_spin() {
        // The bound on the search. An unbounded walk for a held slot would be a
        // tick that never returns, which is a publisher that stops publishing
        // rather than a rotation that finds nothing.
        let mut table = table(2);
        table.withdraw(InstrumentRef::from_admission(0));
        table.withdraw(InstrumentRef::from_admission(1));
        let mut rotation = SnapshotRotation::new(Duration::from_secs(1));
        assert_eq!(rotation.due(0, &table), None);
        assert_eq!(rotation.due(10_000_000_000, &table), None);
    }

    #[test]
    fn an_empty_table_schedules_nothing_at_all() {
        // Distinct from the case above: there are no slots, so there is no pass
        // to make and nothing to schedule for later either.
        let mut rotation = SnapshotRotation::new(Duration::from_secs(1));
        assert_eq!(rotation.due(0, &InstrumentTable::new()), None);
        assert_eq!(rotation.due(10_000_000_000, &InstrumentTable::new()), None);
    }

    #[test]
    fn a_late_tick_is_due_immediately_rather_than_pushing_the_lap_out() {
        // The debt is read off the clock. A tick the process was too busy to
        // serve leaves the next one due now, so a stall makes the lap denser
        // instead of shifting every later instrument by a tick.
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        let table = table(2);
        assert_eq!(rotation.due(0, &table), None);
        assert!(rotation.due(9_000_000_000, &table).is_some());
        assert!(
            rotation.due(10_000_000_000, &table).is_some(),
            "a second instrument is due one tick after the late one, not one tick after when it \
             should have been"
        );
    }
}
