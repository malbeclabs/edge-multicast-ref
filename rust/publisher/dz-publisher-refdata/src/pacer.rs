//! The definition cycle, spread across the cycle instead of emitted at the
//! start of it.

use std::time::Duration;

use dz_edge_core::{AppMessage, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE};
use dz_edge_refdata::InstrumentDefinition;

/// The share of the cycle period one lap of the published set is spread over.
///
/// Eighty percent, because the period is a **maximum on the interval between
/// retransmissions of any single definition** and not a lap target. A lap that
/// used the whole period would leave an instrument emitted early in one lap and
/// late in the next separated by very nearly two periods; the twenty percent
/// that is not used is what absorbs that, plus every tick the process was busy
/// elsewhere.
///
/// One existing publisher emits the whole set at once instead — its own
/// description is that the emission is a synchronized burst — which the
/// reference-data specification's second rule forbids in as many words:
/// publishers MUST NOT emit the entire published set as a single burst. This
/// crate exists to make that unreachable rather than discouraged, which is why
/// the pacing is a property of the type that owns the cycle and not of a
/// caller's loop.
pub const LAP_PERCENT: u64 = 80;

/// How many definitions fit one datagram, from the sizes rather than from a
/// number somebody wrote down.
///
/// `mtu` is clamped exactly as [`DatagramBuilder::new`](dz_edge_core::DatagramBuilder::new)
/// clamps it, so the pacer cannot pace against a datagram larger than the one
/// the builder will actually let through. Deriving this is what keeps it right
/// when the datagram header or the message grows: one existing publisher holds
/// its datagram limit in three places and two of them were fixed.
#[must_use]
pub fn definitions_per_datagram(mtu: u16) -> usize {
    let capacity = (mtu as usize).clamp(DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE);
    // At least one: a datagram that cannot carry a single definition would make
    // the cycle emit nothing forever, which is a silent feed rather than a
    // small one.
    ((capacity - DATAGRAM_HEADER_SIZE) / InstrumentDefinition::SIZE).max(1)
}

/// What one lap of the definition cycle is allowed to cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CycleSchedule {
    cycle_ns: u64,
    definitions_per_datagram: usize,
    max_datagrams_per_tick: usize,
}

impl CycleSchedule {
    /// The schedule for a feed's `definition_cycle`, at a datagram size, with a
    /// ceiling on how much of one tick the cycle may take.
    ///
    /// `max_datagrams_per_tick` is the anti-burst limit and the recovery
    /// behaviour at once. A tick that fell behind — because the process was
    /// busy, or because the set grew — cannot answer by emitting everything it
    /// owes; it emits its ceiling and the rest is owed against the time that is
    /// left in the lap, so the lap gets denser rather than spiky. A stall
    /// therefore degrades into a busier lap, and never into the burst the rule
    /// forbids.
    #[must_use]
    pub fn new(definition_cycle: Duration, mtu: u16, max_datagrams_per_tick: usize) -> Self {
        Self {
            cycle_ns: u64::try_from(definition_cycle.as_nanos()).unwrap_or(u64::MAX),
            definitions_per_datagram: definitions_per_datagram(mtu),
            // Zero would be a cycle that never emits; one datagram a tick is
            // the smallest schedule that makes progress.
            max_datagrams_per_tick: max_datagrams_per_tick.max(1),
        }
    }

    /// The lap length: [`LAP_PERCENT`] of the cycle period.
    #[must_use]
    pub const fn lap_ns(&self) -> u64 {
        // Never zero, so the pacing arithmetic never divides by it: a cycle
        // configured shorter than a nanosecond is a configuration error the
        // runtime reports, not a panic here.
        let lap = self.cycle_ns / 100 * LAP_PERCENT + self.cycle_ns % 100 * LAP_PERCENT / 100;
        if lap == 0 {
            1
        } else {
            lap
        }
    }

    /// The most definitions one tick may emit.
    #[must_use]
    pub const fn max_definitions_per_tick(&self) -> usize {
        self.definitions_per_datagram
            .saturating_mul(self.max_datagrams_per_tick)
    }

    #[must_use]
    pub const fn definitions_per_datagram(&self) -> usize {
        self.definitions_per_datagram
    }

    #[must_use]
    pub const fn cycle_ns(&self) -> u64 {
        self.cycle_ns
    }
}

/// How many definitions this tick owes.
///
/// The schedule is stated as a debt rather than as a rate: by the time `t` into
/// a lap, at least `ceil(published * t / lap)` of the published set should have
/// been emitted. Reading the debt off the clock rather than counting ticks is
/// what makes the pacing independent of how often the runtime happens to call —
/// a runtime ticking every 10ms and one ticking every 250ms lap the set in the
/// same time, and neither can be made to burst by ticking slowly.
///
/// The debt is then capped by [`CycleSchedule::max_definitions_per_tick`], and
/// the two together are the whole behaviour: the cap is what a burst would have
/// to get past, and the debt is what makes a capped tick catch up over the rest
/// of the lap instead of being forgotten.
#[derive(Debug, Clone, Copy)]
pub struct DefinitionPacer {
    schedule: CycleSchedule,
    lap_started_ns: Option<u64>,
    emitted_this_lap: usize,
    laps: u64,
}

impl DefinitionPacer {
    #[must_use]
    pub const fn new(schedule: CycleSchedule) -> Self {
        Self {
            schedule,
            lap_started_ns: None,
            emitted_this_lap: 0,
            laps: 0,
        }
    }

    /// How many definitions to emit now, given the size of the published set.
    ///
    /// The first call with a published set starts the lap and owes nothing,
    /// because a lap cannot be owed before there is one. That is also what
    /// makes a long idle period safe: a publisher whose venue listed nothing
    /// for an hour does not owe the whole set the moment something appears.
    ///
    /// Called once per tick with a monotonic reading. Never returns more than
    /// [`CycleSchedule::max_definitions_per_tick`], and never returns the whole
    /// published set in one call for a set larger than that — which is the rule
    /// this type exists to keep, and the thing to assert about it.
    pub fn due(&mut self, now_ns: u64, published: usize) -> usize {
        if published == 0 {
            // Not a stalled lap: there is nothing to lap. The next admission
            // starts a lap from that moment, rather than inheriting a partly
            // elapsed one and owing most of the set immediately.
            self.lap_started_ns = None;
            self.emitted_this_lap = 0;
            return 0;
        }
        let lap_started_ns = *self.lap_started_ns.get_or_insert(now_ns);
        let lap_ns = self.schedule.lap_ns();
        let elapsed_ns = now_ns.saturating_sub(lap_started_ns).min(lap_ns);

        // u128 because `published * elapsed_ns` overflows a u64 for a large set
        // and a long cycle, and the product is an intermediate nobody sees.
        let owed = u128::from(elapsed_ns)
            .saturating_mul(published as u128)
            .div_ceil(u128::from(lap_ns));
        let owed = usize::try_from(owed).unwrap_or(published).min(published);

        let due = owed
            .saturating_sub(self.emitted_this_lap)
            .min(self.schedule.max_definitions_per_tick());
        self.emitted_this_lap += due;

        if self.emitted_this_lap >= published {
            // The next lap starts where this one was due to end, so laps do not
            // drift later by whatever fraction of a tick each one overran by.
            // A lap that finished late enough that its successor is already
            // over starts now instead, rather than owing a full set at once.
            let scheduled = lap_started_ns.saturating_add(lap_ns);
            self.lap_started_ns = Some(if scheduled > now_ns {
                scheduled
            } else {
                now_ns
            });
            self.emitted_this_lap = 0;
            self.laps += 1;
        }
        due
    }

    /// Completed laps of the published set, for a caller reporting that the
    /// cycle is turning.
    #[must_use]
    pub const fn laps(&self) -> u64 {
        self.laps
    }

    #[must_use]
    pub const fn schedule(&self) -> CycleSchedule {
        self.schedule
    }
}
