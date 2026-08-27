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

    /// The header's declared datagram length is outside the representable
    /// range. Distinct from `ShortBuffer`, whose `got` is the buffer length -
    /// here the buffer may be intact and the header is what is wrong.
    #[error("datagram header declares length {declared}, outside the valid range {min}..={max}")]
    DeclaredLengthOutOfRange {
        declared: u16,
        min: usize,
        max: usize,
    },

    /// Magic is what rejects a datagram misrouted from another feed, so a
    /// mismatch is refused rather than parsed at the wrong layout.
    #[error("magic {found:#06x} does not match the expected {expected:#06x}")]
    MagicMismatch { expected: u16, found: u16 },

    /// A declared message length below the 4-byte message header is impossible.
    #[error(
        "message at offset {offset} declares length {declared}, below the 4-byte message header"
    )]
    MessageTooShort { offset: usize, declared: u8 },

    /// Fewer than the 4-byte message header remain, so no Length field was
    /// ever read. Distinct from `MessageTooShort`, whose `declared` value was
    /// actually read off the wire and found too small - reusing that variant
    /// here would mean inventing a declared length that was never seen.
    #[error(
        "message at offset {offset} is truncated: only {remaining} bytes remain, below the 4-byte message header"
    )]
    MessageHeaderTruncated { offset: usize, remaining: usize },

    #[error("message at offset {offset} declares length {declared} but only {remaining} bytes remain in the datagram")]
    MessageOverrunsDatagram {
        offset: usize,
        declared: u8,
        remaining: usize,
    },

    #[error("datagram header declares {declared} messages but {found} were found")]
    MessageCountMismatch { declared: u8, found: usize },
}
