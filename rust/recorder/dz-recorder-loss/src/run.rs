//! The unit of loss: one contiguous run of sequence numbers nobody delivered.
//!
//! A run is what a `sequence_gap` row is, and the mapping is one to one: the
//! channel instance in full, the era it sits in, the first and last sequence
//! number absent, the count, and the timestamps of the datagrams either side.

use std::net::Ipv4Addr;

use dz_edge_core::PortRole;
use dz_recorder_core::ChannelInstance;

/// One contiguous run of missing sequence numbers, on one channel instance, in
/// one era.
///
/// **The measure is [`missing_count`](Self::missing_count), which is a count of
/// sequence values.** At fifty datagrams a second a three-second gap is a
/// hundred and fifty missing and on a channel that only heartbeats it is three,
/// so a figure in seconds compares neither between two channels nor between two
/// hours of one: it measures how busy the feed was as much as what was lost.
/// [`before_ts_ns`](Self::before_ts_ns) and [`after_ts_ns`](Self::after_ts_ns)
/// place the run against an incident and never quantify it.
///
/// A run never spans an era. A `Reset Count` transition opens a new sequence
/// space, so a comparison across one is an artefact rather than a gap, and
/// [`era_ordinal`](Self::era_ordinal) is the monotonic ordinal the deriver
/// assigned rather than the wire value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceRun {
    /// `(source address, Channel ID, destination port)` — the only key under
    /// which a sequence number means anything.
    pub instance: ChannelInstance,
    /// The multicast group the datagrams either side arrived on. Carried
    /// because the consuming report keys on it and a run without it cannot be
    /// placed.
    pub group: Ipv4Addr,
    pub role: PortRole,
    /// The era's monotonic ordinal, counting from 1 at the instance's first
    /// datagram.
    pub era_ordinal: u64,
    /// The wire `Reset Count` this era carried, kept as a fact and never used
    /// as a key: it is a `u8` and it wraps, so two eras 256 resets apart share
    /// a value, and treating them as one era merges two sequence spaces and
    /// hides the loss between them.
    pub reset_count: u8,
    /// First sequence number absent.
    pub missing_from: u64,
    /// Last sequence number absent.
    pub missing_to: u64,
    /// Receive stamp of the delivered datagram at `missing_from - 1`.
    pub before_ts_ns: u64,
    /// Receive stamp of the delivered datagram at `missing_to + 1`.
    pub after_ts_ns: u64,
}

impl SequenceRun {
    /// How many sequence numbers nobody delivered. This is the quantity.
    ///
    /// Derived from the bounds rather than stored beside them, so a row cannot
    /// carry a count that disagrees with the range it claims to describe.
    #[must_use]
    pub const fn missing_count(&self) -> u64 {
        // Saturating: the bounds are wire values, and a row that panics while
        // being counted is a worse answer than one that saturates.
        self.missing_to
            .saturating_sub(self.missing_from)
            .saturating_add(1)
    }

    /// How long the instance was silent across the run.
    ///
    /// Presentable beside [`missing_count`](Self::missing_count) and never
    /// instead of it: this is a statement about the feed's rate as much as
    /// about the loss, so it compares nothing between two channels or between
    /// two hours of one.
    #[must_use]
    pub const fn span_ns(&self) -> u64 {
        self.after_ts_ns.saturating_sub(self.before_ts_ns)
    }
}
