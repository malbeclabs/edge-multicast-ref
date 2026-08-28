use crate::constants::{SIZE_HEARTBEAT, TYPE_HEARTBEAT};
use crate::error::DecodeError;
use crate::message::AppMessage;

/// `0x01 Heartbeat` (16 bytes). Sent on `mktdata` when there is no other
/// traffic, so a subscriber can tell a quiet channel from a dead one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    /// Redundant with the datagram header's own `Channel ID`. Honoured by
    /// `encode_into`, which writes this value at its own offset. When the
    /// message is framed by a `DatagramBuilder`, `stamp_channel_id`
    /// overwrites it afterwards with the datagram's own `Channel ID`, so a
    /// builder-framed message can never disagree with its header. `decode`
    /// populates this field from whatever is actually on the wire.
    pub channel_id: u8,
    pub timestamp_ns: u64,
}

impl AppMessage for Heartbeat {
    const TYPE_ID: u8 = TYPE_HEARTBEAT;
    const SIZE: usize = SIZE_HEARTBEAT;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4] = self.channel_id;
        dst[5..8].fill(0);
        dst[8..16].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }

    // Channel ID at offset 4.
    fn stamp_channel_id(dst: &mut [u8], channel_id: u8) {
        dst[4] = channel_id;
    }
}

impl Heartbeat {
    /// Decode from a full message, including its 4-byte header.
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
            channel_id: buf[4],
            timestamp_ns: u64::from_le_bytes(
                buf[8..16]
                    .try_into()
                    .expect("range width matches the target array"),
            ),
        })
    }
}
