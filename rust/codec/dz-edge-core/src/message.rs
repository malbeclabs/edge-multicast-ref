/// Implemented by every fixed-size application message in the feed family.
/// `DatagramBuilder` uses it to pack messages without knowing their types.
pub trait AppMessage {
    /// Message type ID byte.
    const TYPE_ID: u8;

    /// Fixed on-the-wire size in bytes, including the 4-byte message header.
    const SIZE: usize;

    /// Encode into `dst`, which MUST be exactly `SIZE` bytes.
    ///
    /// Implementations guard this with a `debug_assert!`, so an over-long slice
    /// trips in debug builds and, in release, has its first `SIZE` bytes written
    /// with the remainder left untouched. Either way a caller reusing a scratch
    /// buffer must slice to `SIZE` before transmitting, or stale bytes ride along
    /// behind the message.
    fn encode_into(&self, dst: &mut [u8]);
}
