use dz_edge_core::{AppMessage, DecodeError};

/// `0x07 ManifestSummary` (24 bytes), on the `refdata` port role.
///
/// The manifest cadence MUST be shorter than the definition cycle period, so a
/// new subscriber sees a summary before it has finished collecting definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestSummary {
    /// Redundant with the datagram header; useful for standalone logging.
    pub channel_id: u8,
    /// 1 once the published set is established; 0 while uninitialized or
    /// shutting down.
    pub valid: u8,
    pub manifest_seq: u16,
    pub instrument_count: u32,
    pub timestamp_ns: u64,
}

impl AppMessage for ManifestSummary {
    const TYPE_ID: u8 = 0x07;
    const SIZE: usize = 24;

    fn encode_into(&self, dst: &mut [u8]) {
        debug_assert_eq!(dst.len(), Self::SIZE);
        dst[0] = Self::TYPE_ID;
        dst[1] = Self::SIZE as u8;
        dst[2..4].copy_from_slice(&0u16.to_le_bytes());
        dst[4] = self.channel_id;
        dst[5] = self.valid;
        dst[6..8].fill(0);
        dst[8..10].copy_from_slice(&self.manifest_seq.to_le_bytes());
        dst[10..12].fill(0);
        dst[12..16].copy_from_slice(&self.instrument_count.to_le_bytes());
        dst[16..24].copy_from_slice(&self.timestamp_ns.to_le_bytes());
    }
}

impl ManifestSummary {
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
            valid: buf[5],
            manifest_seq: u16::from_le_bytes([buf[8], buf[9]]),
            instrument_count: u32::from_le_bytes(buf[12..16].try_into().unwrap_or_default()),
            timestamp_ns: u64::from_le_bytes(buf[16..24].try_into().unwrap_or_default()),
        })
    }
}
