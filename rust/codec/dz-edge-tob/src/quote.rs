use dz_edge_core::{AppMessage, DecodeError};

pub const QUOTE_BID_UPDATED: u8 = 0x01;
pub const QUOTE_ASK_UPDATED: u8 = 0x02;
pub const QUOTE_BID_GONE: u8 = 0x04;
pub const QUOTE_ASK_GONE: u8 = 0x08;

/// `0x03 Quote` (60 bytes). One two-sided BBO update.
///
/// Prices carry the instrument's Price Exponent and quantities its Qty
/// Exponent, both from `InstrumentDefinition`. This type does no scaling: the
/// caller supplies the raw fixed-point integers, which is what keeps the wire
/// exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Quote {
    pub instrument_id: u32,
    pub source_id: u16,
    pub update_flags: u8,
    pub source_timestamp_ns: u64,
    pub bid_price: i64,
    pub bid_qty: u64,
    pub ask_price: i64,
    pub ask_qty: u64,
    pub bid_source_count: u16,
    pub ask_source_count: u16,
}

impl AppMessage for Quote {
    const TYPE_ID: u8 = 0x03;
    const SIZE: usize = 60;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..8].copy_from_slice(&self.instrument_id.to_le_bytes());
        dst[8..10].copy_from_slice(&self.source_id.to_le_bytes());
        dst[10] = self.update_flags;
        dst[11] = 0;
        dst[12..20].copy_from_slice(&self.source_timestamp_ns.to_le_bytes());
        dst[20..28].copy_from_slice(&self.bid_price.to_le_bytes());
        dst[28..36].copy_from_slice(&self.bid_qty.to_le_bytes());
        dst[36..44].copy_from_slice(&self.ask_price.to_le_bytes());
        dst[44..52].copy_from_slice(&self.ask_qty.to_le_bytes());
        dst[52..54].copy_from_slice(&self.bid_source_count.to_le_bytes());
        dst[54..56].copy_from_slice(&self.ask_source_count.to_le_bytes());
        dst[56..60].fill(0);
    }
}

impl Quote {
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
            update_flags: buf[10],
            source_timestamp_ns: u64::from_le_bytes(
                buf[12..20]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            bid_price: i64::from_le_bytes(
                buf[20..28]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            bid_qty: u64::from_le_bytes(
                buf[28..36]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            ask_price: i64::from_le_bytes(
                buf[36..44]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            ask_qty: u64::from_le_bytes(
                buf[44..52]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
            bid_source_count: u16::from_le_bytes([buf[52], buf[53]]),
            ask_source_count: u16::from_le_bytes([buf[54], buf[55]]),
        })
    }
}
