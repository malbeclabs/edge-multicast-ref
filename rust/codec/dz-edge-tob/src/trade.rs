use dz_edge_core::{AppMessage, DecodeError};

pub const AGGRESSOR_UNKNOWN: u8 = 0;
pub const AGGRESSOR_BUY: u8 = 1;
pub const AGGRESSOR_SELL: u8 = 2;

pub const TRADE_FLAG_BLOCK: u8 = 0x01;
/// Bit 1. Keeps the name `sweep`: it is the externally defined term for an
/// order sweeping several levels, and it is a wire field name.
pub const TRADE_FLAG_SWEEP: u8 = 0x02;
pub const TRADE_FLAG_CROSS: u8 = 0x04;

/// `0x04 Trade` (52 bytes). One execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub instrument_id: u32,
    pub source_id: u16,
    pub aggressor_side: u8,
    pub trade_flags: u8,
    pub source_timestamp_ns: u64,
    pub trade_price: i64,
    pub trade_qty: u64,
    /// Venue-assigned. 0 if the venue exposes none.
    pub trade_id: u64,
    /// Session cumulative volume. 0 if unavailable.
    pub cumulative_volume: u64,
}

impl AppMessage for Trade {
    const TYPE_ID: u8 = 0x04;
    const SIZE: usize = 52;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.aggressor_side;
        dst[11] = self.trade_flags;
        dst[12..20].copy_from_slice(&self.source_timestamp_ns.to_le_bytes());
        dst[20..28].copy_from_slice(&self.trade_price.to_le_bytes());
        dst[28..36].copy_from_slice(&self.trade_qty.to_le_bytes());
        dst[36..44].copy_from_slice(&self.trade_id.to_le_bytes());
        dst[44..52].copy_from_slice(&self.cumulative_volume.to_le_bytes());
    }
}

impl Trade {
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
            source_id: u16::from_le_bytes([buf[8], buf[9]]),
            aggressor_side: buf[10],
            trade_flags: buf[11],
            source_timestamp_ns: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            trade_price: i64::from_le_bytes(
                buf[20..28]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            trade_qty: u64::from_le_bytes(
                buf[28..36]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            trade_id: u64::from_le_bytes(
                buf[36..44]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            cumulative_volume: u64::from_le_bytes(
                buf[44..52]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
        })
    }
}
