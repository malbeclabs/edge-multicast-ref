/// Errors produced by the decoders in this crate family.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DecodeError {
    #[error("short buffer: need {need} bytes, got {got}")]
    ShortBuffer { need: usize, got: usize },

    #[error("unsupported schema version {0}")]
    UnsupportedSchema(u8),

    #[error("message length {declared} mismatches fixed size {expected} for type {type_id:#04x}")]
    LengthMismatch {
        type_id: u8,
        declared: u8,
        expected: u8,
    },

    #[error("unknown message type id {0:#04x}")]
    BadTypeId(u8),

    #[error("datagram builder full: {attempted} bytes would exceed max {max}")]
    DatagramFull { attempted: usize, max: usize },

    /// The datagram already holds 255 messages, the most the u8 Message Count
    /// field can express. Distinct from `DatagramFull`, which is a byte-capacity
    /// limit - conflating them sends a reader chasing an MTU problem.
    #[error("datagram already holds the maximum {max} messages")]
    MessageCountExhausted { max: u8 },

    /// Which type ids are reserved is per feed, not fleet-wide. This variant
    /// exists for a caller enforcing a reservation that applies to its own
    /// feed; it must refuse the reserved id rather than silently invent a
    /// meaning for it.
    #[error("type id {0:#04x} is reserved and carries no message")]
    ReservedTypeId(u8),

    /// The header declares zero messages. The Message Count range is 1-255, so
    /// a zero-message datagram is malformed rather than merely empty.
    #[error("datagram header declares 0 messages; the Message Count range is 1-255")]
    EmptyDatagram,
}
