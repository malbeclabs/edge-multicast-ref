use crate::constants::{SIZE_END_OF_SESSION, TYPE_END_OF_SESSION};
use crate::error::DecodeError;
use crate::message::AppMessage;

/// `0x06 EndOfSession` (12 bytes). No more data for this session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndOfSession {
    pub timestamp_ns: u64,
}

impl AppMessage for EndOfSession {
    const TYPE_ID: u8 = TYPE_END_OF_SESSION;
    const SIZE: usize = SIZE_END_OF_SESSION;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4..12].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }
}

impl EndOfSession {
    pub fn decode(buf: &[u8]) -> Result<Self, DecodeError> {
        if buf.len() < Self::SIZE {
            return Err(DecodeError::ShortBuffer {
                need: Self::SIZE,
                got: buf.len(),
            });
        }
        if buf[1] as usize != Self::SIZE {
            return Err(DecodeError::LengthMismatch {
                type_id: Self::TYPE_ID,
                declared: buf[1],
                expected: Self::SIZE as u8,
            });
        }
        Ok(Self {
            timestamp_ns: u64::from_le_bytes(buf[4..12].try_into().unwrap_or_default()),
        })
    }
}
