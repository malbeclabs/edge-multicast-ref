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
//! [`InstrumentTable::holds`] is a bounds check, and the published count
//! [`SnapshotRotation::due`] divides the cycle by is a cached number rather
//! than a walk of the slots. It was the walk, which made the pacing arithmetic
//! the most expensive thing in a tick that says here it is constant, and this
//! is the invariant a maintainer would size a large published set against.
//!
//! # One rotation per shard, over slots every shard shares
//!
//! A rotation belongs to one channel instance, so the cycle it divides is
//! divided by *its shard's* published count and the slots it accepts are its
//! shard's. Both are the caller's to supply, because the instrument table is
//! one table for the process — the lowering's — and putting the shard on it
//! would drag a crate that publishes nothing into knowing where publication
//! goes. What that costs is the one place the tick is not constant: a rotation
//! walks past the slots of shards that are not its own, so the search for the
//! next instrument is linear in the number of shards rather than in one.
//!
//! # The ceiling is a sum over shards, not a comparison per shard
//!
//! Stated at length because the per-shard form of it was wrong the moment there
//! was more than one rotation, and wrong in the direction that reads as fine.
//!
//! Each rotation divides *its own* cycle by *its own* published count, which is
//! what makes each channel's `[[feed]] snapshot_cycle` mean what it says. The
//! **serving** rate is not per shard: the tick body calls
//! [`Publisher::periodic_snapshot`](crate::publisher::Publisher::periodic_snapshot)
//! once and it returns at most one instrument, because a snapshot is a group of
//! datagrams and the unit of progress is an instrument. So N shards draw on one
//! budget of one snapshot per runtime tick. The demand adds up and the supply
//! does not.
//!
//! The old statement of the ceiling was *a set so large that
//! `cycle / instruments` falls below the runtime's own tick*. That was the whole
//! of it with one rotation. With N it misses the case shards introduce:
//!
//! | | Derived tick | Reads as | Is |
//! |---|---|---|---|
//! | 1 shard, 1,000 instruments, 5 s cycle | 5 ms | breached, below the 10 ms tick | breached |
//! | 31 shards, 100 instruments each, 5 s cycle | 50 ms | comfortable, five times the tick | 620 snapshots a second wanted, 100 available: every channel laps in 31 s |
//!
//! Every shard in the second row is comfortable and the process is not. The
//! true condition is over the sum — `Σ (published_i / cycle_i) ≤ 1 / tick`, or
//! equivalently `Σ (tick / tick_i) ≤ 1` — and [`schedule_share`] is that term
//! for one shard, in integer arithmetic against [`WHOLE_SNAPSHOT_CAPACITY`], so
//! that the sum can be asserted directly the way [`tick`] is.
//!
//! It is **counted rather than refused**, and that is a decision rather than an
//! omission: the divisor is the published count, so at load there is nothing to
//! divide, and a refusal on the first tick that could compute it would darken a
//! publisher that is already sending — over a shortfall that degrades into a
//! slower lap and never into a wrong answer, and whose remedy is a
//! configuration edit an operator has to be told about rather than one the
//! process can make. So
//! [`Publisher::snapshot_schedule_overruns`](crate::publisher::Publisher::snapshot_schedule_overruns)
//! counts the ticks and the exit report names them.
//!
//! **What no arithmetic here survives**, stated because it is the other ceiling
//! and not a detail: a single instrument whose book is enormous still goes out
//! as one group. That, and the shortfall above, both want a level-budget
//! scheduler with mid-group resumption, which is a different design; the seam
//! for it is `periodic_snapshot` and replacing that changes nothing else in the
//! loop.

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

/// One process's whole snapshot-serving capacity, as a share.
///
/// A scale rather than a unit, chosen so that [`schedule_share`] is integer
/// arithmetic: a rational comparison of rates in floating point would have the
/// answer depend on rounding at exactly the boundary this is asked about.
pub const WHOLE_SNAPSHOT_CAPACITY: u64 = 1_000_000;

/// The share of one process's snapshot capacity one rotation's schedule asks
/// for.
///
/// `process_tick` is how often the runtime's loop serves one snapshot, and
/// serving one is all it does per tick — see this module's note. So a shard
/// wanting one snapshot every `tick_i` asks for `process_tick / tick_i` of the
/// whole, and a configuration is achievable exactly when the shares of every
/// shard sum to no more than [`WHOLE_SNAPSHOT_CAPACITY`].
///
/// Separate from the type for the same reason [`tick`] is: the sum is the whole
/// of the claim, and a claim that cannot be asserted on its own is one that
/// gets asserted through six other things or not at all.
///
/// **A shard with nothing published asks for nothing**, rather than for the
/// share of a set of one that [`tick`]'s own clamp would imply. There is no
/// pass to make over an empty published set and [`SnapshotRotation::due`] makes
/// none; charging it for one would have a document's worth of empty channels
/// add up to a shortfall nobody is experiencing.
#[must_use]
pub fn schedule_share(process_tick: Duration, cycle: Duration, instruments: usize) -> u64 {
    if instruments == 0 {
        return 0;
    }
    // `tick` clamps at `MIN_TICK`, so this divisor is never zero however the
    // cycle and the count are arranged.
    let per_instrument = tick(cycle, instruments).as_nanos();
    let wanted = process_tick
        .as_nanos()
        .saturating_mul(u128::from(WHOLE_SNAPSHOT_CAPACITY))
        / per_instrument;
    u64::try_from(wanted).unwrap_or(u64::MAX)
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
    ///
    /// `published` is **this rotation's own shard's** published count, and
    /// `on_shard` is what says whether a slot holds one of its instruments. The
    /// slots are shared across every shard, so both are the caller's to supply
    /// and neither can be read off the table: divided by the process's count a
    /// shard is paced as slowly as there are shards, and walking every shard's
    /// slots spends most of a rotation's ticks on instruments another channel
    /// serves. Both errors leave `[[feed]] snapshot_cycle` reading as honoured
    /// and lap the channel's own set many times too slowly.
    pub fn due(
        &mut self,
        now_ns: u64,
        instruments: &InstrumentTable,
        published: usize,
        on_shard: impl Fn(InstrumentRef) -> bool,
    ) -> Option<InstrumentRef> {
        let slots = u32::try_from(instruments.slots()).unwrap_or(u32::MAX);
        if slots == 0 {
            return None;
        }
        let tick = tick(self.cycle(), published);

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
            if instruments.holds(candidate) && on_shard(candidate) {
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

    /// One shard's rotation over a table that is all its own, which is what
    /// every test here but the two shard tests is about: the published count is
    /// the table's and every slot is a member.
    fn due(
        rotation: &mut SnapshotRotation,
        at_ns: u64,
        table: &InstrumentTable,
    ) -> Option<InstrumentRef> {
        rotation.due(at_ns, table, table.len(), |_| true)
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
    fn a_shards_rotation_is_paced_by_its_own_published_count() {
        // Five instruments in one table: four on one shard and one on the
        // other. Divided by the process's five, the shard holding one would
        // wait five ticks to snapshot it and its `[[feed]] snapshot_cycle`
        // would still read as honoured - which is the whole of the bug.
        let table = table(5);
        let shard_a = |instrument: InstrumentRef| instrument.index() < 4;
        let shard_b = |instrument: InstrumentRef| instrument.index() >= 4;
        let mut a = SnapshotRotation::new(Duration::from_secs(4));
        let mut b = SnapshotRotation::new(Duration::from_secs(4));
        assert_eq!(a.due(0, &table, 4, shard_a), None);
        assert_eq!(b.due(0, &table, 1, shard_b), None);

        // Four seconds over four instruments is one a second.
        assert!(
            a.due(1_000_000_000, &table, 4, shard_a).is_some(),
            "the larger shard laps its four instruments in its own cycle"
        );
        // Four seconds over one instrument is one every four seconds, and a
        // divisor of five would have made it one every 800ms.
        assert_eq!(
            b.due(1_000_000_000, &table, 1, shard_b),
            None,
            "the smaller shard is paced by the one instrument it publishes, not by the five the \
             process does"
        );
        assert!(b.due(4_000_000_000, &table, 1, shard_b).is_some());
    }

    /// The process tick every share below is measured against, and the one the
    /// runtime actually runs.
    const PROCESS_TICK: Duration = crate::run::TICK;

    #[test]
    fn a_share_is_the_process_tick_over_the_shards_own_derived_tick() {
        // The whole of the ceiling arithmetic, asserted as arithmetic. A shard
        // wanting one snapshot every 50ms out of a process serving one every
        // 10ms asks for a fifth of the process.
        assert_eq!(
            schedule_share(PROCESS_TICK, Duration::from_secs(5), 100),
            WHOLE_SNAPSHOT_CAPACITY / 5
        );
        // And one wanting one every 10ms asks for the whole of it: achievable,
        // and achievable only if it is the only shard.
        assert_eq!(
            schedule_share(PROCESS_TICK, Duration::from_secs(1), 100),
            WHOLE_SNAPSHOT_CAPACITY
        );
    }

    #[test]
    fn a_shard_with_nothing_published_asks_for_nothing() {
        // `tick` clamps an empty count to one, which would charge every empty
        // channel in a long document for a pass it never makes. There is no
        // pass to make over an empty published set and `due` makes none.
        assert_eq!(schedule_share(PROCESS_TICK, Duration::from_secs(5), 0), 0);
    }

    #[test]
    fn every_shards_derived_tick_can_be_comfortable_while_the_process_is_not() {
        // The case the per-shard statement of the ceiling misses, and the whole
        // reason this function exists. Each of these derives a 50ms tick, five
        // times the process tick, and every one of them reads as comfortable.
        let shards = 31;
        let per_shard = schedule_share(PROCESS_TICK, Duration::from_secs(5), 100);
        assert!(
            tick(Duration::from_secs(5), 100) > PROCESS_TICK,
            "the fixture must be one the per-shard reading calls comfortable"
        );
        let total = per_shard * shards;
        assert!(
            total > WHOLE_SNAPSHOT_CAPACITY,
            "31 shards each asking for a fifth of the process is not achievable: {total}"
        );
        // 6.2 processes' worth, which is the 31-second lap of a five-second
        // cycle stated as a share.
        assert_eq!(total, WHOLE_SNAPSHOT_CAPACITY * 62 / 10);
    }

    #[test]
    fn one_shard_below_the_process_tick_is_still_caught() {
        // The case the old per-shard statement did catch, which the sum must
        // not stop catching: a set so large that its derived tick falls below
        // the process tick is one shard already over the whole budget.
        assert!(tick(Duration::from_secs(5), 1_000) < PROCESS_TICK);
        assert!(
            schedule_share(PROCESS_TICK, Duration::from_secs(5), 1_000) > WHOLE_SNAPSHOT_CAPACITY
        );
    }

    #[test]
    fn a_rotation_takes_only_its_own_shards_instruments() {
        // The slots are one table for the process, so a rotation that took
        // whatever it found would snapshot another channel's instruments on
        // this channel's port and lap its own set half as often.
        let table = table(4);
        let odd_slots = |instrument: InstrumentRef| instrument.index() % 2 == 1;
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        assert_eq!(rotation.due(0, &table, 2, odd_slots), None);

        let mut taken = Vec::new();
        for second in 1..=4u64 {
            taken.push(
                rotation
                    .due(second * 1_000_000_000, &table, 2, odd_slots)
                    .map(InstrumentRef::index),
            );
        }
        assert_eq!(
            taken,
            [Some(1), Some(3), Some(1), Some(3)],
            "the rotation walks past the other shard's slots and laps its own two"
        );
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
        assert_eq!(due(&mut rotation, 0, &table(1)), None);
    }

    #[test]
    fn one_instrument_per_tick_and_the_pass_wraps() {
        let mut rotation = SnapshotRotation::new(Duration::from_secs(3));
        let table = table(3);
        // One pass over three instruments in three seconds is one per second.
        assert_eq!(due(&mut rotation, 0, &table), None);
        let mut taken = Vec::new();
        for second in 1..=6u64 {
            let at = second * 1_000_000_000;
            // Exactly one instrument per due tick, never a batch: a snapshot is
            // several datagrams, so the unit of progress is an instrument.
            taken.push(due(&mut rotation, at, &table).map(InstrumentRef::index));
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
        assert_eq!(due(&mut rotation, 0, &table), None);
        // The tick is one second; half of one is not due.
        assert_eq!(due(&mut rotation, 500_000_000, &table), None);
        assert!(due(&mut rotation, 1_000_000_000, &table).is_some());
    }

    #[test]
    fn a_withdrawn_instrument_is_skipped_and_its_handle_is_not_reused() {
        // A hole in the table is not the end of the pass: handles are slots for
        // the lifetime of the table, so a delisted instrument leaves one behind
        // and the rotation has to walk past it.
        let mut table = table(3);
        table.withdraw(InstrumentRef::from_admission(1));
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        assert_eq!(due(&mut rotation, 0, &table), None);
        let first = due(&mut rotation, 1_000_000_000, &table).map(InstrumentRef::index);
        let second = due(&mut rotation, 2_000_000_000, &table).map(InstrumentRef::index);
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
        assert_eq!(due(&mut rotation, 0, &table), None);
        assert_eq!(due(&mut rotation, 10_000_000_000, &table), None);
    }

    #[test]
    fn an_empty_table_schedules_nothing_at_all() {
        // Distinct from the case above: there are no slots, so there is no pass
        // to make and nothing to schedule for later either.
        let mut rotation = SnapshotRotation::new(Duration::from_secs(1));
        assert_eq!(due(&mut rotation, 0, &InstrumentTable::new()), None);
        assert_eq!(
            due(&mut rotation, 10_000_000_000, &InstrumentTable::new()),
            None
        );
    }

    #[test]
    fn a_late_tick_is_due_immediately_rather_than_pushing_the_lap_out() {
        // The debt is read off the clock. A tick the process was too busy to
        // serve leaves the next one due now, so a stall makes the lap denser
        // instead of shifting every later instrument by a tick.
        let mut rotation = SnapshotRotation::new(Duration::from_secs(2));
        let table = table(2);
        assert_eq!(due(&mut rotation, 0, &table), None);
        assert!(due(&mut rotation, 9_000_000_000, &table).is_some());
        assert!(
            due(&mut rotation, 10_000_000_000, &table).is_some(),
            "a second instrument is due one tick after the late one, not one tick after when it \
             should have been"
        );
    }
}
