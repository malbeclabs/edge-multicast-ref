//! The per-instrument delta sequence, stamped here and nowhere else.

use dz_adapter_core::InstrumentRef;

/// `Per-Instrument Seq` for every admitted instrument.
///
/// **The runtime's counter, not the adapter's**, and there is no field on any
/// event to pass one through. Two reasons, and the second is the one that
/// decides it.
///
/// The first is what the field is for: it narrows the blast radius of a channel
/// gap. A subscriber that lost a datagram knows a frame is missing but not
/// which instruments were in it; on the next delta for each instrument it
/// compares this number to what it last applied, and continuity means that
/// instrument is clean. That only works if the series is **dense** — the
/// specification requires no skips — which is a property of the publisher's
/// emit path and not of anything a venue knows.
///
/// The second is the recorder. Re-running an adapter offline over an archive of
/// what its upstream sent, lowering the result, and diffing against the
/// messages decoded from the capture is a *join* rather than a heuristic
/// alignment only if both sides carry the same number for the same upstream
/// event. A counter the adapter kept would have to be reconstructed identically
/// offline; a counter the runtime keeps is reconstructed by re-running the
/// runtime's own lowering, which is what the re-lowering does anyway.
///
/// # The era, and the two things that do not end one
///
/// The series restarts at `1` on a `Reset Count` change and at no other time.
/// It is explicitly **not** reset at a snapshot boundary: a subscriber that
/// missed a snapshot and then saw a delta numbered `1` could not tell a fresh
/// post-snapshot delta from a late duplicate of an old one. Keeping it
/// monotonic within the era is what makes "at or below what I applied" mean
/// *duplicate* and "more than one above" mean *gap*, unambiguously.
///
/// `LevelUpdate` and `BookClear` share one series, because both mutate the book
/// and their relative order is significant.
#[derive(Debug, Clone, Default)]
pub struct PerInstrumentSeq {
    /// The last value stamped for each instrument, indexed by handle. Zero
    /// means none yet, which is also what a snapshot declares for an instrument
    /// with no deltas in this era — the same sentinel, and not a coincidence.
    last: Vec<u32>,
}

impl PerInstrumentSeq {
    /// A counter with every instrument at the start of an era.
    #[must_use]
    pub const fn new() -> Self {
        Self { last: Vec::new() }
    }

    /// The next number for this instrument.
    ///
    /// The first call after an era change returns `1`, and each subsequent call
    /// returns exactly one more. Called once per message that reaches the wire
    /// and never speculatively: a number consumed by a message that then failed
    /// to encode is a gap every subscriber reads as packet loss, so the caller
    /// takes it only once the message is known to be constructible.
    pub fn stamp(&mut self, instrument: InstrumentRef) -> u32 {
        let slot = self.slot(instrument);
        // Saturating rather than wrapping: an instrument that has taken 2^32
        // deltas in one era is past what this field can express, and wrapping
        // would restate a number a subscriber already applied — read as a
        // duplicate, and the delta silently discarded. Repeating the last
        // number instead is read as a duplicate too, but it stops climbing,
        // which is at least visible.
        *slot = slot.saturating_add(1);
        *slot
    }

    /// The last number stamped for this instrument, or `0` for none in this
    /// era.
    ///
    /// This is what a snapshot declares as its `Last Instrument Seq`, which is
    /// the value a subscriber initialises its own tracker to after applying the
    /// snapshot.
    #[must_use]
    pub fn last(&self, instrument: InstrumentRef) -> u32 {
        self.last
            .get(instrument.index() as usize)
            .copied()
            .unwrap_or(0)
    }

    /// End the era: every instrument's series restarts at `1`.
    ///
    /// For a `Reset Count` change and nothing else — in particular not for a
    /// snapshot, and not for a reconnect that did not change the reset count.
    pub fn end_era(&mut self) {
        self.last.clear();
    }

    /// The slot for this instrument, grown to reach it.
    ///
    /// Handles are dense indices the instrument table minted, so growing to the
    /// largest one seen is bounded by the admitted set rather than by anything
    /// an adapter chooses.
    fn slot(&mut self, instrument: InstrumentRef) -> &mut u32 {
        let index = instrument.index() as usize;
        if index >= self.last.len() {
            self.last.resize(index + 1, 0);
        }
        &mut self.last[index]
    }
}
