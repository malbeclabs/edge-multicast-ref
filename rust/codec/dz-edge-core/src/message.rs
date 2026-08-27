/// Implemented by every fixed-size application message in the feed family.
/// `DatagramBuilder` uses it to pack messages without knowing their types.
pub trait AppMessage {
    /// Message type ID byte.
    const TYPE_ID: u8;

    /// Fixed on-the-wire size in bytes, including the 4-byte message header.
    const SIZE: usize;

    /// Encode into `dst`, which MUST be exactly `SIZE` bytes; a longer slice
    /// has its first `SIZE` bytes written and the remainder left untouched,
    /// so a caller reusing a scratch buffer must slice to `SIZE` before
    /// transmitting.
    fn encode_into(&self, dst: &mut [u8]);
}
