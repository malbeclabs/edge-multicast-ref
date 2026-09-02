use dz_edge_core::{AppMessage, DecodeError, PortRole};

/// No reason given.
pub const RESET_UNSPECIFIED: u8 = 0;
/// A publisher-side integrity check found its own book diverged.
pub const RESET_PUBLISHER_INCONSISTENCY: u8 = 1;
/// The upstream venue reset or resynchronised this instrument.
pub const RESET_VENUE_RESYNC: u8 = 2;
/// The publisher detected a gap in its upstream event stream.
pub const RESET_UPSTREAM_GAP: u8 = 3;
/// Publisher-specific, documented out of band.
pub const RESET_OTHER: u8 = 255;

/// `0x14 InstrumentReset` (28 bytes). One instrument's state is being discarded
/// and re-bootstrapped.
///
/// **This is the message a publisher owes when it has lost confidence in its own
/// book for one instrument** — an integrity check that found divergence, a gap
/// in the upstream event stream, a venue that resynchronised underneath it. The
/// specification is explicit that `BookClear` is not the answer: that one
/// asserts the named levels are gone and a subscriber applying it stays ready,
/// while this one says the state is untrustworthy and must be rebuilt.
///
/// Without it, a publisher that dropped one delta has only bad options. Every
/// later quantity at that price is wrong by the dropped amount **for the rest of
/// the era**, not for one update — because a level update states the absolute
/// resting quantity and a subscriber that missed one is not corrected by the
/// next. Publishing on is publishing a book that diverged silently; clearing is
/// telling subscribers levels are gone when they are not; saying nothing is the
/// same as publishing on.
///
/// # The obligation it carries
///
/// `new_anchor_seq` is a promise, not a diagnostic: the publisher **must** emit
/// a snapshot for this instrument on the snapshot port with `Anchor Seq` equal
/// to this value, before resuming delta emission. A subscriber discards any
/// snapshot for the instrument with an older anchor, so a reset whose promised
/// snapshot never arrives leaves that instrument waiting forever — worse than
/// the divergence it was announcing.
///
/// And it must equal the `Sequence Number` of the very datagram carrying this
/// message: the reset takes effect immediately, so the anchor is where the
/// stream is *now*. The specification's own conformance subscriber grades that
/// a violation, which is why [`Self::anchored_at`] exists rather than a bare
/// struct literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentReset {
    pub instrument_id: u32,
    /// One of the `RESET_*` values. Informational, and a subscriber must accept
    /// any `u8`.
    pub reason: u8,
    /// The `mktdata` sequence number from which the recovery snapshot will be
    /// valid, and the number of the datagram this message goes out in.
    pub new_anchor_seq: u64,
    pub timestamp_ns: u64,
}

impl InstrumentReset {
    /// A reset anchored at the sequence number of the datagram that will carry
    /// it.
    ///
    /// The constructor exists because the anchor is the one field a caller
    /// cannot get right by thinking about this instrument: it is a property of
    /// the send path, and the send path is the only thing that knows the number
    /// its next datagram will take. A struct literal invites reading it off the
    /// last delta instead, which is one behind and is exactly the off-by-one
    /// the conformance rule catches.
    #[must_use]
    pub const fn anchored_at(
        instrument_id: u32,
        reason: u8,
        sequence_number: u64,
        timestamp_ns: u64,
    ) -> Self {
        Self {
            instrument_id,
            reason,
            new_anchor_seq: sequence_number,
            timestamp_ns,
        }
    }
}

impl AppMessage for InstrumentReset {
    const TYPE_ID: u8 = 0x14;
    const SIZE: usize = 28;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8] = self.reason;
        dst[9..12].fill(0);
        dst[12..20].copy_from_slice(&self.new_anchor_seq.to_le_bytes());
        dst[20..28].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }

    // Byte-for-byte identical to the market-by-order feed's `0x14`, which
    // carries no redundant Channel ID either.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl InstrumentReset {
    /// # Errors
    ///
    /// The header errors [`LevelUpdate::decode`](crate::LevelUpdate::decode)
    /// returns.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer {
                need: Self::SIZE,
                got: buf.len(),
            });
        }
        if buf[0] != Self::TYPE_ID {
            return Err(DecodeError::BadTypeId(buf[0]));
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            instrument_id: u32::from_le_bytes(
                buf[4..8]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            reason: buf[8],
            new_anchor_seq: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            timestamp_ns: u64::from_le_bytes(
                buf[20..28]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
        })
    }
}
