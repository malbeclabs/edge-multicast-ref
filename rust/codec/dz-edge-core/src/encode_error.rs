/// Errors produced when building a datagram.
///
/// Separate from `DecodeError` so a publisher's send path does not name a decode
/// type, and so neither enum carries variants the other direction cannot reach.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum EncodeError {
    #[error("datagram builder full: {attempted} bytes would exceed capacity {capacity}")]
    DatagramFull { attempted: usize, capacity: usize },

    /// The datagram already holds 255 messages, the most the u8 Message Count
    /// field can express. Distinct from `DatagramFull`, which is a byte-capacity
    /// limit - conflating them sends a reader chasing an MTU problem.
    #[error("datagram already holds the maximum {max} messages")]
    MessageCountExhausted { max: u8 },
}
