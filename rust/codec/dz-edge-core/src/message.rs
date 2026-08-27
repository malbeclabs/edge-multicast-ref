/// Implemented by every fixed-size application message in the feed family.
/// `DatagramBuilder` uses it to pack messages without knowing their types.
pub trait AppMessage {
    /// Message type ID byte.
    const TYPE_ID: u8;

    /// Fixed on-the-wire size in bytes, including the 4-byte message header.
    const SIZE: usize;

    /// Encode into `dst`, which MUST be exactly `SIZE` bytes.
    fn encode_into(&self, dst: &mut [u8]);
}
