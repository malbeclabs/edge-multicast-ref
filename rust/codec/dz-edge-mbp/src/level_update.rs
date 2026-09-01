use dz_edge_core::{AppMessage, DecodeError, PortRole};

/// `0x40 LevelUpdate` (48 bytes). One price level's aggregate quantity, after
/// the change.
///
/// The core message of this feed, and the one whose contract is easiest to get
/// wrong: `qty_raw` is the **absolute** aggregate resting quantity at
/// `price_raw`, never a delta, and zero removes the level. A subscriber that
/// added it to what it held would drift; a subscriber that missed one is wrong
/// at that price and correct everywhere else, which is what makes the loss
/// bounded and detectable.
///
/// `action`, `level_index` and `update_reason` are informational. They must not
/// gate the apply — two subscribers receiving the same message must reach the
/// same book, and one branching on a field the other ignored would not.
///
/// Prices carry the instrument's Price Exponent and quantities its Qty
/// Exponent, both from `InstrumentDefinition`. This type does no scaling: the
/// caller supplies the raw fixed-point integers, which is what keeps the wire
/// exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelUpdate {
    pub instrument_id: u32,
    pub source_id: u16,
    /// [`SIDE_BID`](crate::SIDE_BID) or [`SIDE_ASK`](crate::SIDE_ASK).
    pub side: u8,
    /// Informational. Never a gate on the apply.
    pub action: u8,
    pub per_instrument_seq: u32,
    /// The level's key.
    pub price_raw: i64,
    /// Absolute aggregate quantity at the price. Zero removes the level.
    pub qty_raw: u64,
    pub timestamp_ns: u64,
    /// [`U16_UNAVAILABLE`](crate::U16_UNAVAILABLE) when absent.
    pub order_count: u16,
    /// Informational, and [`U16_UNAVAILABLE`](crate::U16_UNAVAILABLE) when
    /// absent. Never a gate on the apply.
    pub level_index: u16,
    /// Informational. Never a gate on the apply.
    pub update_reason: u8,
    pub level_flags: u8,
}

impl AppMessage for LevelUpdate {
    const TYPE_ID: u8 = 0x40;
    const SIZE: usize = 48;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Mktdata];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.side;
        dst[11] = self.action;
        dst[12..16].copy_from_slice(&self.per_instrument_seq.to_le_bytes());
        dst[16..24].copy_from_slice(&self.price_raw.to_le_bytes());
        dst[24..32].copy_from_slice(&self.qty_raw.to_le_bytes());
        dst[32..40].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        dst[40..42].copy_from_slice(&self.order_count.to_le_bytes());
        dst[42..44].copy_from_slice(&self.level_index.to_le_bytes());
        dst[44] = self.update_reason;
        dst[45] = self.level_flags;
        dst[46..48].fill(0);
    }

    // LevelUpdate carries no redundant Channel ID.
    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl LevelUpdate {
    /// # Errors
    ///
    /// [`DecodeError::ShortBuffer`], [`DecodeError::BadTypeId`] or
    /// [`DecodeError::LengthMismatch`], in that order: a buffer too short to
    /// hold the type id cannot be judged by it.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::check_header::<Self>(buf)?;
        Ok(Self {
            instrument_id: crate::u32_at(buf, 4),
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            side: buf[10],
            action: buf[11],
            per_instrument_seq: crate::u32_at(buf, 12),
            price_raw: crate::i64_at(buf, 16),
            qty_raw: crate::u64_at(buf, 24),
            timestamp_ns: crate::u64_at(buf, 32),
            order_count: u16::from_le_bytes([buf[40], buf[41]]),
            level_index: u16::from_le_bytes([buf[42], buf[43]]),
            update_reason: buf[44],
            level_flags: buf[45],
        })
    }
}
