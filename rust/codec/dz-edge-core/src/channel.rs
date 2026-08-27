//! The sequencing state of one channel instance: the datagram-header fields the
//! builder stamps, carried as one value rather than as separate positional
//! arguments.

/// The sequencing state of one channel instance.
///
/// Holds the three datagram-header fields that belong together - `Channel ID`,
/// the sequence series and `Reset Count` - so they cannot be transposed at a
/// call site in a way that decodes cleanly and is wrong.
///
/// This is deliberately NOT named `ChannelInstance`. The glossary keys a channel
/// instance on `(source IP address, Channel ID, destination port)`, and this
/// crate knows nothing of sockets, addresses or ports, so it can carry the state
/// a channel instance owns but not its identity. The egress layer owns the full
/// channel instance and supplies this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChannelSequence {
    channel_id: u8,
    sequence_number: u64,
    reset_count: u8,
}

impl ChannelSequence {
    /// Start a channel's sequence at 0 in the given era.
    #[must_use]
    pub const fn new(channel_id: u8, reset_count: u8) -> Self {
        Self {
            channel_id,
            sequence_number: 0,
            reset_count,
        }
    }

    /// Resume at a known sequence number.
    #[must_use]
    pub const fn resume(channel_id: u8, reset_count: u8, sequence_number: u64) -> Self {
        Self {
            channel_id,
            sequence_number,
            reset_count,
        }
    }

    #[must_use]
    pub const fn channel_id(&self) -> u8 {
        self.channel_id
    }

    #[must_use]
    pub const fn sequence_number(&self) -> u64 {
        self.sequence_number
    }

    #[must_use]
    pub const fn reset_count(&self) -> u8 {
        self.reset_count
    }

    /// Advance the sequence to the next datagram.
    pub fn advance(&mut self) {
        self.sequence_number = self.sequence_number.wrapping_add(1);
    }

    /// Begin a new era: bump `Reset Count` and restart the sequence at 0, which
    /// is what the specification requires of a channel reset.
    pub fn begin_era(&mut self) {
        self.reset_count = self.reset_count.wrapping_add(1);
        self.sequence_number = 0;
    }
}
