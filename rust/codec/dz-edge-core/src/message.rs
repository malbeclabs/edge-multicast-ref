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

    /// Stamp the datagram's `Channel ID` into a message that carries one
    /// redundantly with the datagram header.
    ///
    /// Every message type must implement this. A message carrying a `Channel
    /// ID` redundant with the datagram header writes it at its own offset, so
    /// the offset stays private to the message that owns it and this trait
    /// still names a behaviour rather than a byte position. A message with no
    /// such field writes an empty body - an explicit statement that it has
    /// nothing to stamp, rather than an inherited default silently doing
    /// nothing. The builder calls this after `encode_into`, so a
    /// caller-supplied value cannot disagree with the header that frames it.
    fn stamp_channel_id(dst: &mut [u8], channel_id: u8);
}
