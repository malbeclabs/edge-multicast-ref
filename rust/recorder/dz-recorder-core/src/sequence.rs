//! Sequence continuity for one channel instance: the reordering window, the
//! taxonomy of outcomes, and the monotonic era ordinal.
//!
//! This is one rule set with one implementation. The live health tier and the
//! offline analysis tier both classify a datagram by what its sequence number
//! did, and a dashboard whose live panel and historical panel disagree about the
//! same feed teaches nobody anything — so *when a datagram counts as reordered
//! rather than missing* is decided here, once, rather than agreed between two
//! copies.
//!
//! It sits in this crate because it is pure logic over two integers and a
//! bitmap, keyed on the [`ChannelInstance`](crate::ChannelInstance) this crate
//! already owns. Nothing here needs a metrics registry, a file or a socket,
//! which is what lets an offline loader link it without linking a recorder
//! process.

/// Sequence numbers the reordering window covers.
///
/// Inside the window, backward motion is a reordering or a duplicate and is
/// distinguishable; beyond it, the two are indistinguishable and both are
/// reported as backward motion instead. 1024 is chosen so the bitmap is 128
/// bytes per instance — small enough to hold thousands of instances resident,
/// wide enough that a reordering across a bonded path or an equal-cost pair is
/// still recognised as one rather than reported as a sequence restart.
pub const REORDER_WINDOW: u64 = 1024;

/// The largest forward jump that is credible as loss rather than as a forged
/// sequence number.
///
/// The reverse direction has always been bounded: past [`REORDER_WINDOW`] a
/// backward step is reported as backward motion rather than believed. The
/// forward direction had no such bound, and the asymmetry was exploitable,
/// because forward motion is the direction that *credits a counter*. One
/// datagram claiming `u64::MAX` is charged as that many missing datagrams —
/// enough to overflow the counter it lands in, which a rate query then reads as
/// a counter reset — and the tracker would adopt it as the highest seen, after
/// which every genuine datagram is behind the window and real loss is never
/// counted again. The sequence number is a field on the wire and an any-source
/// join accepts it from anybody, so this is a hostile input arriving on a path
/// that must not be able to poison a permanent counter.
///
/// 2^32 is far above any gap a real outage produces — at a million datagrams a
/// second it is an hour and a quarter of continuous loss on one channel
/// instance — and far below the range where the arithmetic stops being
/// meaningful. What matters more than the exact figure is what happens past it:
/// nothing is credited and nothing is adopted, so no number of forged datagrams
/// moves the loss accounting at all.
pub const MAX_FORWARD_JUMP: u64 = 1 << 32;

const WINDOW_WORDS: usize = (REORDER_WINDOW / 64) as usize;

/// A bitmap of the sequence numbers seen in the window ending at the highest
/// one seen, where bit `n` means `highest - n` has arrived.
///
/// Fixed size and inline: the drain thread must not allocate, and this is the
/// only per-datagram state that is not a scalar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SeenWindow {
    bits: [u64; WINDOW_WORDS],
}

impl SeenWindow {
    const fn new() -> Self {
        Self {
            bits: [0; WINDOW_WORDS],
        }
    }

    fn clear(&mut self) {
        self.bits = [0; WINDOW_WORDS];
    }

    fn contains(&self, behind: u64) -> bool {
        debug_assert!(behind < REORDER_WINDOW);
        let index = behind as usize;
        self.bits[index / 64] & (1 << (index % 64)) != 0
    }

    fn insert(&mut self, behind: u64) {
        debug_assert!(behind < REORDER_WINDOW);
        let index = behind as usize;
        self.bits[index / 64] |= 1 << (index % 64);
    }

    /// Slides the window forward by `delta`, so what was `highest - n` becomes
    /// `highest - n - delta`. A slide of a full window or more leaves nothing
    /// behind, which is the same as clearing it.
    fn slide(&mut self, delta: u64) {
        if delta >= REORDER_WINDOW {
            self.clear();
            return;
        }
        let words = (delta / 64) as usize;
        let bits = (delta % 64) as u32;
        for target in (0..WINDOW_WORDS).rev() {
            let mut value = 0;
            if let Some(source) = target.checked_sub(words) {
                value = self.bits[source] << bits;
                // A shift of 64 is not a shift, it is a panic in debug and a
                // no-op in release, so the carry is only taken when there is
                // one.
                if bits > 0 && source > 0 {
                    value |= self.bits[source - 1] >> (64 - bits);
                }
            }
            self.bits[target] = value;
        }
    }
}

/// What one datagram's sequence number did, relative to everything already seen
/// on its channel instance.
///
/// Exactly one of these per datagram: the outcomes are exclusive, so a fault
/// maps to one counter and a counter maps back to one fault.
///
/// A gap's size is the count of sequence numbers nobody delivered, and that is
/// the only measure of loss either tier reports. Duration is deliberately not a
/// second way of saying it: at fifty datagrams a second a three-second gap is a
/// hundred and fifty missing and on a channel that only heartbeats it is three,
/// so a figure in seconds is a statement about how busy the feed was as much as
/// about what was lost, and it compares neither between channels nor between two
/// hours of one. Timestamps are freshness and cadence, never loss.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceOutcome {
    /// The first datagram on this instance. Silent by construction: no gap, no
    /// loss, no alert. A tunnel address is a lease, it can be reassigned under
    /// a live host, and a reassignment must not page.
    Opened,
    /// Exactly the next sequence number.
    InOrder,
    /// `missing` sequence numbers were skipped over.
    Gap { missing: u64 },
    /// Already seen, inside the reordering window.
    Duplicate,
    /// Not seen, inside the reordering window, `behind` the highest — a late
    /// arrival filling a gap already counted.
    Reordered { behind: u64 },
    /// A `Reset Count` transition, carrying the new era's monotonic ordinal.
    Reset { era_ordinal: u64 },
    /// Backward motion beyond the reordering window with no reset — a publisher
    /// that restarted its sequence space without advancing `Reset Count`.
    Backward { behind: u64 },
    /// Forward motion beyond [`MAX_FORWARD_JUMP`] — a sequence number no
    /// outage explains. Nothing is credited as missing and the tracker does not
    /// adopt it, so the instance's accounting survives the datagram that
    /// carried it.
    ForwardJump { ahead: u64 },
}

/// Sequence continuity and era accounting for one channel instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceTracker {
    opened: bool,
    highest: u64,
    reset_count: u8,
    era_ordinal: u64,
    seen: SeenWindow,
}

impl Default for SequenceTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl SequenceTracker {
    /// A tracker whose first era will be numbered `era_ordinal + 1`.
    ///
    /// For an instance being reopened after its entry was evicted while its
    /// series survived: `era_ordinal` is documented as monotonic and as the
    /// value to group an era by, and a fresh tracker would restart it at 1 and
    /// merge the new era with the first. The ordinal belongs to the instance,
    /// which outlived the entry that tracked it.
    #[must_use]
    pub const fn resuming_from_era(era_ordinal: u64) -> Self {
        let mut tracker = Self::new();
        tracker.era_ordinal = era_ordinal;
        tracker
    }

    #[must_use]
    pub const fn new() -> Self {
        Self {
            opened: false,
            highest: 0,
            reset_count: 0,
            era_ordinal: 0,
            seen: SeenWindow::new(),
        }
    }

    /// The highest sequence number seen in the current era.
    #[must_use]
    pub const fn highest(&self) -> u64 {
        self.highest
    }

    /// The monotonic era ordinal, counting from 1 at the first datagram.
    ///
    /// This, and never the wire `Reset Count`, is what an era is identified by.
    #[must_use]
    pub const fn era_ordinal(&self) -> u64 {
        self.era_ordinal
    }

    /// Folds one datagram's header fields in, and says what they meant.
    ///
    /// The era rule is the load-bearing one. `Reset Count` is a `u8` and it
    /// wraps, so two eras 256 resets apart carry the same wire value. The
    /// transition is therefore recorded as it happens, in receive order, and the
    /// era is identified by an ordinal this tracker carries — because treating
    /// equal `Reset Count`s as one era merges them and *hides* the loss between
    /// them, which is worse than inventing a false gap: a tier that reports
    /// nothing is worse than one that reports wrongly.
    pub fn observe(&mut self, sequence_number: u64, reset_count: u8) -> SequenceOutcome {
        if !self.opened {
            self.open(sequence_number, reset_count);
            return SequenceOutcome::Opened;
        }
        if reset_count != self.reset_count {
            self.open(sequence_number, reset_count);
            return SequenceOutcome::Reset {
                era_ordinal: self.era_ordinal,
            };
        }
        if sequence_number > self.highest {
            let delta = sequence_number - self.highest;
            // Refused before anything is credited or adopted. Believing it
            // costs the instance its accounting for the rest of the era, and
            // the counter it would credit is one nothing can walk back.
            if delta > MAX_FORWARD_JUMP {
                return SequenceOutcome::ForwardJump { ahead: delta };
            }
            self.highest = sequence_number;
            self.seen.slide(delta);
            self.seen.insert(0);
            if delta == 1 {
                SequenceOutcome::InOrder
            } else {
                SequenceOutcome::Gap { missing: delta - 1 }
            }
        } else {
            let behind = self.highest - sequence_number;
            if behind >= REORDER_WINDOW {
                SequenceOutcome::Backward { behind }
            } else if self.seen.contains(behind) {
                SequenceOutcome::Duplicate
            } else {
                self.seen.insert(behind);
                SequenceOutcome::Reordered { behind }
            }
        }
    }

    /// Starts an era at `sequence_number`, advancing the ordinal.
    ///
    /// `era_ordinal` starts from 0, so opening the first era leaves it at 1 and
    /// the ordinal doubles as "how many eras this instance has been through".
    fn open(&mut self, sequence_number: u64, reset_count: u8) {
        self.opened = true;
        self.highest = sequence_number;
        self.reset_count = reset_count;
        self.era_ordinal += 1;
        self.seen.clear();
        self.seen.insert(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forward_jump_past_the_bound_is_neither_credited_nor_adopted() {
        // The bound the reverse direction always had. Forward is the direction
        // that credits a counter, so an unbounded one is a permanent number
        // written by whoever sends the datagram.
        let mut tracker = SequenceTracker::new();
        tracker.observe(10, 0);
        assert_eq!(
            tracker.observe(10 + MAX_FORWARD_JUMP + 1, 0),
            SequenceOutcome::ForwardJump {
                ahead: MAX_FORWARD_JUMP + 1
            }
        );
        assert_eq!(tracker.highest(), 10, "the tracker did not adopt it");
        assert_eq!(
            tracker.observe(11, 0),
            SequenceOutcome::InOrder,
            "the instance carries on from where it was"
        );
    }

    #[test]
    fn a_gap_at_the_bound_is_still_loss() {
        // The bound has to be above any gap an outage produces, or a real
        // outage stops being counted as one. At a million datagrams a second
        // this is an hour and a quarter of continuous loss.
        let mut tracker = SequenceTracker::new();
        tracker.observe(10, 0);
        assert_eq!(
            tracker.observe(10 + MAX_FORWARD_JUMP, 0),
            SequenceOutcome::Gap {
                missing: MAX_FORWARD_JUMP - 1
            }
        );
        assert_eq!(tracker.highest(), 10 + MAX_FORWARD_JUMP);
    }

    #[test]
    fn a_resumed_tracker_continues_the_era_numbering() {
        // An instance whose entry was evicted while its series survived. The
        // ordinal is documented as monotonic and as the value to group an era
        // by; restarting it at 1 merges the new era with the first.
        let mut tracker = SequenceTracker::resuming_from_era(3);
        assert_eq!(tracker.observe(1, 0), SequenceOutcome::Opened);
        assert_eq!(tracker.era_ordinal(), 4);
    }

    /// The window slide is the one piece of bit arithmetic here, and a lost
    /// carry between words turns a duplicate into a reordering silently.
    ///
    /// The carry is only exercised when a bit crosses a word boundary, so the
    /// slide that matters is the one-place slide off bit 63: a whole-word slide
    /// moves words and needs no carry at all.
    #[test]
    fn the_window_slides_across_word_boundaries() {
        let mut window = SeenWindow::new();
        window.insert(0);
        window.slide(63);
        assert!(window.contains(63), "the last bit of the first word");
        window.slide(1);
        assert!(
            window.contains(64),
            "a carry out of the first word was lost"
        );
        window.slide(64);
        assert!(window.contains(128), "a whole-word slide lost the bit");
        window.slide(REORDER_WINDOW - 128);
        assert!(
            (0..REORDER_WINDOW).all(|behind| !window.contains(behind)),
            "a bit slid past the window is still set"
        );
    }
}
