//! `Sequence Number`, per channel instance, in one era.

use std::collections::HashMap;

use dz_edge_core::{ChannelSequence, ResetCount};

use crate::instance::ChannelInstance;

/// The sequence series of every channel instance this publisher sends on.
///
/// # One era, many series
///
/// The era — the datagram header's `Reset Count` — is a property of the *feed*
/// and is handed to this type once, at startup, by [`EraStore`](crate::EraStore).
/// The series are per channel instance. That asymmetry is the design's: a
/// restart is a single event for the whole feed, and every series it carries
/// restarts together, but nothing about mktdata's numbering may be inferred
/// from refdata's.
///
/// # The sequence number is not persisted, and the era is
///
/// Only one of the two needs to survive a restart, and it is the cheaper one.
/// A subscriber decides *this publisher restarted* from a change in `Reset
/// Count`, not from the sequence number, and on seeing it drops the state it
/// had cached and re-syncs. So the sequence series is free to restart at 0 in
/// the new era.
///
/// Persisting the sequence number instead would be both more expensive — a
/// write per datagram, on the send path — and wrong: the last number written
/// before a crash is behind the last number sent, so a publisher resuming from
/// it re-uses numbers a subscriber has already applied. Those arrive as
/// duplicates and are discarded, and the messages inside them are silently
/// lost. See [`EraStore`](crate::EraStore) for the other half: an era that
/// *fails* to survive a restart re-uses the era too, and then the restart is
/// invisible.
pub struct Sequencer {
    era: ResetCount,
    instances: HashMap<ChannelInstance, ChannelSequence>,
}

impl Sequencer {
    /// A sequencer for one feed's era, holding no instances yet.
    #[must_use]
    pub fn new(era: ResetCount) -> Self {
        Self {
            era,
            instances: HashMap::new(),
        }
    }

    /// The era every series here is numbered in.
    #[must_use]
    pub const fn era(&self) -> ResetCount {
        self.era
    }

    /// Begin a series for a channel instance, at sequence 0 in this era.
    ///
    /// Idempotent, and that is load-bearing: re-registering an instance that
    /// already has a series must not restart it. A publisher that re-reads its
    /// configuration, or registers the same `Channel ID` from two code paths,
    /// would otherwise reset a live series to 0 without touching `Reset
    /// Count` — which is the one combination a subscriber cannot interpret,
    /// because the sequence has gone backwards inside an era it was told is
    /// still running.
    ///
    /// Returns whether a new series was created.
    pub fn register(&mut self, instance: ChannelInstance) -> bool {
        if self.instances.contains_key(&instance) {
            return false;
        }
        self.instances.insert(
            instance,
            ChannelSequence::new(instance.channel_id, self.era),
        );
        true
    }

    #[must_use]
    pub fn is_registered(&self, instance: &ChannelInstance) -> bool {
        self.instances.contains_key(instance)
    }

    /// The state the next datagram on this instance is stamped with, or `None`
    /// for an instance that was never registered.
    ///
    /// Reading does not consume: the number is stamped into a datagram that
    /// may take a while to fill, and until [`Self::advance`] it is still this
    /// instance's next number.
    #[must_use]
    pub fn current(&self, instance: &ChannelInstance) -> Option<ChannelSequence> {
        self.instances.get(instance).copied()
    }

    /// Spend this instance's number.
    ///
    /// Called once the datagram carrying it has been handed to the sink, and
    /// **whether or not the sink took it**. A refused datagram leaves a gap,
    /// and the gap is correct: that datagram existed, carried that number, and
    /// did not arrive — which is what a subscriber's loss counter is for.
    /// Handing the number to a replacement datagram instead would put two
    /// different payloads on the wire under one number in one era, and a
    /// conformant subscriber discards the second as a duplicate. The loss would
    /// then be invisible on both sides: no gap here, and no message there.
    ///
    /// No-op for an unregistered instance, which cannot have been numbered.
    pub fn advance(&mut self, instance: &ChannelInstance) {
        if let Some(sequence) = self.instances.get_mut(instance) {
            sequence.advance();
        }
    }

    /// How many series this sequencer holds. For a startup log line.
    #[must_use]
    pub fn len(&self) -> usize {
        self.instances.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }
}
