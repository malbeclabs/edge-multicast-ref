use dz_edge_core::{AppMessage, DecodeError, PortRole};

/// Clear the bid side.
pub const CLEAR_BID: u8 = 0;
/// Clear the ask side.
pub const CLEAR_ASK: u8 = 1;
/// Clear both sides.
pub const CLEAR_BOTH: u8 = 2;

/// Clear the whole side, whatever `from_price_raw` holds.
pub const SCOPE_ENTIRE_SIDE: u8 = 0;
/// Clear from `from_price_raw` outward, inclusive.
pub const SCOPE_FROM_PRICE: u8 = 1;

/// `0x41 BookClear` (36 bytes). Bulk removal of levels.
///
/// **Not a resynchronisation signal.** A subscriber that applies one stays
/// ready: the publisher is saying these levels are gone, not that the book it
/// has is untrustworthy. Reading it as a reset is how a subscriber throws away
/// a book it could have kept and asks for a snapshot nobody needed to send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookClear {
    pub instrument_id: u32,
    pub source_id: u16,
    /// [`CLEAR_BID`], [`CLEAR_ASK`] or [`CLEAR_BOTH`].
    pub clear_side: u8,
    /// [`SCOPE_ENTIRE_SIDE`] or [`SCOPE_FROM_PRICE`].
    pub scope: u8,
    pub per_instrument_seq: u32,
    /// The inclusive bound, when `scope` is [`SCOPE_FROM_PRICE`].
    pub from_price_raw: i64,
    pub timestamp_ns: u64,
    pub clear_reason: u8,
}

impl AppMessage for BookClear {
    const TYPE_ID: u8 = 0x41;
    const SIZE: usize = 36;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.clear_side;
        dst[11] = self.scope;
        dst[12..16].copy_from_slice(&self.per_instrument_seq.to_le_bytes());
        dst[16..24].copy_from_slice(&self.from_price_raw.to_le_bytes());
        dst[24..32].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        dst[32] = self.clear_reason;
        dst[33..36].fill(0);
    }

    // BookClear carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl BookClear {
    /// # Errors
    ///
    /// The header errors [`LevelUpdate::decode`](crate::LevelUpdate::decode)
    /// returns, and [`DecodeError::MalformedBody`] for the one combination the
    /// feed's own rules forbid: a bounded clear of both sides.
    ///
    /// That combination is refused rather than interpreted because one price
    /// cannot bound two sides that run in opposite directions — *outward* from
    /// it means down on the bids and up on the asks, so a subscriber guessing
    /// would clear a different set of levels than the publisher meant, and
    /// silently. There is no reading of it that two implementations would agree
    /// on, which is exactly when a decoder must refuse.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::check_header::<Self>(buf)?;
        let clear = Self {
            instrument_id: crate::u32_at(buf, 4),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            clear_side: buf[10],
            scope: buf[11],
            per_instrument_seq: crate::u32_at(buf, 12),
            from_price_raw: crate::i64_at(buf, 16),
            timestamp_ns: crate::u64_at(buf, 24),
            clear_reason: buf[32],
        };
        if clear.scope == SCOPE_FROM_PRICE && clear.clear_side == CLEAR_BOTH {
            return Err(DecodeError::MalformedBody {
                type_id: Self::TYPE_ID,
                what: "a clear bounded by one price cannot apply to both sides",
            });
        }
        Ok(clear)
    }
}
