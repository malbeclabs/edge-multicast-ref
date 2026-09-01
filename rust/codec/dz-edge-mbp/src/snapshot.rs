//! The three messages of a snapshot, which is one book state cut into a
//! sequence of datagrams.
//!
//! A snapshot is only useful if a subscriber can tell whether it received all
//! of it, and against which point in the live stream it applies. `anchor_seq`
//! answers the second question and `total_levels` the first: a subscriber that
//! counted fewer `SnapshotLevel`s than the `SnapshotBegin` promised has an
//! incomplete book state and must not apply it, and one that applied it against
//! the wrong point in the stream has a book that never existed.

use dz_edge_core::{AppMessage, DecodeError, PortRole};

/// `0x20 SnapshotBegin` (40 bytes). Opens one instrument's snapshot.
///
/// Everything a subscriber needs before the levels arrive: how many there will
/// be, which snapshot they belong to, and the sequence number the resulting
/// book is true as of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotBegin {
    pub instrument_id: u32,
    /// The channel sequence number this book state is true as of. The
    /// subscriber applies live messages after it and discards those before.
    pub anchor_seq: u64,
    /// How many [`SnapshotLevel`]s belong to this snapshot. A subscriber that
    /// counts fewer has an incomplete book and must not apply it.
    pub total_levels: u32,
    /// Ties the three messages together, so two overlapping snapshots for one
    /// instrument cannot be interleaved into one wrong book.
    pub snapshot_id: u32,
    pub last_instrument_seq: u32,
    pub timestamp_ns: u64,
    /// How deep the publisher's book goes, so absence past it is a bound rather
    /// than a missing level.
    pub depth_bound: u32,
}

impl AppMessage for SnapshotBegin {
    const TYPE_ID: u8 = 0x20;
    const SIZE: usize = 40;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Snapshot];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..16].copy_from_slice(&self.anchor_seq.to_le_bytes());
        dst[16..20].copy_from_slice(&self.total_levels.to_le_bytes());
        dst[20..24].copy_from_slice(&self.snapshot_id.to_le_bytes());
        dst[24..28].copy_from_slice(&self.last_instrument_seq.to_le_bytes());
        dst[28..36].copy_from_slice(&self.timestamp_ns.to_le_bytes());
        dst[36..40].copy_from_slice(&self.depth_bound.to_le_bytes());
    }

    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl SnapshotBegin {
    /// # Errors
    ///
    /// The header errors [`LevelUpdate::decode`](crate::LevelUpdate::decode)
    /// returns.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::check_header::<Self>(buf)?;
        Ok(Self {
            instrument_id: crate::u32_at(buf, 4),
            anchor_seq: crate::u64_at(buf, 8),
            total_levels: crate::u32_at(buf, 16),
            snapshot_id: crate::u32_at(buf, 20),
            last_instrument_seq: crate::u32_at(buf, 24),
            timestamp_ns: crate::u64_at(buf, 28),
            depth_bound: crate::u32_at(buf, 36),
        })
    }
}

/// `0x42 SnapshotLevel` (32 bytes). One price level of a snapshot.
///
/// The instrument is implied by the containing [`SnapshotBegin`] and is not
/// repeated — which is why `snapshot_id` is here instead: without it a level
/// could not be told from one belonging to an overlapping snapshot of another
/// instrument.
///
/// Quantity is non-zero by rule. An empty level is represented by its absence,
/// so a zero here is a publisher defect and not an instruction to delete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotLevel {
    pub snapshot_id: u32,
    pub price_raw: i64,
    pub qty_raw: u64,
    /// [`U16_UNAVAILABLE`](crate::U16_UNAVAILABLE) when absent.
    pub order_count: u16,
    /// [`SIDE_BID`](crate::SIDE_BID) or [`SIDE_ASK`](crate::SIDE_ASK).
    pub side: u8,
    pub level_flags: u8,
}

impl AppMessage for SnapshotLevel {
    const TYPE_ID: u8 = 0x42;
    const SIZE: usize = 32;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Snapshot];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.snapshot_id.to_le_bytes());
        dst[8..16].copy_from_slice(&self.price_raw.to_le_bytes());
        dst[16..24].copy_from_slice(&self.qty_raw.to_le_bytes());
        dst[24..26].copy_from_slice(&self.order_count.to_le_bytes());
        dst[26] = self.side;
        dst[27] = self.level_flags;
        dst[28..32].fill(0);
    }

    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl SnapshotLevel {
    /// # Errors
    ///
    /// The header errors [`LevelUpdate::decode`](crate::LevelUpdate::decode)
    /// returns.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::check_header::<Self>(buf)?;
        Ok(Self {
            snapshot_id: crate::u32_at(buf, 4),
            price_raw: crate::i64_at(buf, 8),
            qty_raw: crate::u64_at(buf, 16),
            order_count: u16::from_le_bytes([buf[24], buf[25]]),
            side: buf[26],
            level_flags: buf[27],
        })
    }
}

/// `0x22 SnapshotEnd` (20 bytes). Closes one instrument's snapshot.
///
/// It repeats `anchor_seq` and `snapshot_id` rather than leaving them to be
/// remembered: a subscriber that lost the [`SnapshotBegin`] would otherwise
/// have a run of levels it cannot place, and one that lost the end would apply
/// a snapshot it never saw completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotEnd {
    pub instrument_id: u32,
    pub anchor_seq: u64,
    pub snapshot_id: u32,
}

impl AppMessage for SnapshotEnd {
    const TYPE_ID: u8 = 0x22;
    const SIZE: usize = 20;
    const PORT_ROLES: &'static [PortRole] = &[PortRole::Snapshot];

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..16].copy_from_slice(&self.anchor_seq.to_le_bytes());
        dst[16..20].copy_from_slice(&self.snapshot_id.to_le_bytes());
    }

    fn stamp_channel_id(_dst: &mut [u8], _channel_id: u8) {}
}

impl SnapshotEnd {
    /// # Errors
    ///
    /// The header errors [`LevelUpdate::decode`](crate::LevelUpdate::decode)
    /// returns.
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        crate::check_header::<Self>(buf)?;
        Ok(Self {
            instrument_id: crate::u32_at(buf, 4),
            anchor_seq: crate::u64_at(buf, 8),
            snapshot_id: crate::u32_at(buf, 16),
        })
    }
}
