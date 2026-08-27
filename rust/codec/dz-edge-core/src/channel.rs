//! The sequencing state of one channel instance: the datagram-header fields the
//! builder stamps, carried as one value rather than as separate positional
//! arguments.

/// A channel's reset era, the datagram header's `Reset Count`.
///
/// A newtype rather than a bare `u8` because it sits next to `Channel ID`, which
/// is also a `u8`: transposing the two at a call site would compile and would put
/// a wrong channel and a wrong era on the wire with nothing to catch it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ResetCount(pub u8);

impl ResetCount {
    /// The era a channel that has never reset advertises.
    pub const NEVER_RESET: Self = Self(0);

    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

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
    pub const fn new(channel_id: u8, reset_count: ResetCount) -> Self {
        Self {
            channel_id,
            sequence_number: 0,
            reset_count: reset_count.0,
        }
    }

    /// Resume at a known sequence number.
    #[must_use]
    pub const fn resume(channel_id: u8, reset_count: ResetCount, sequence_number: u64) -> Self {
        Self {
            channel_id,
            sequence_number,
            reset_count: reset_count.0,
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
    pub const fn reset_count(&self) -> ResetCount {
        ResetCount(self.reset_count)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `ChannelSequence::new(7, ResetCount(3))` cannot be silently transposed
    /// into `ChannelSequence::new(3, ResetCount(7))` any more: `ResetCount` is a
    /// distinct type from the bare `u8` channel id, so a swapped call site is a
    /// type error caught at compile time, not a bug caught (or missed) at
    /// review time.
    #[test]
    fn channel_id_and_reset_count_cannot_be_transposed() {
        let seq = ChannelSequence::new(7, ResetCount(3));
        assert_eq!(seq.channel_id(), 7);
        assert_eq!(seq.reset_count().get(), 3);
    }
}
