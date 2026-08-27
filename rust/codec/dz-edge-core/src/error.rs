/// Errors produced by the decoders in this crate family.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DecodeError {
    #[error("short buffer: need {need} bytes, got {got}")]
    ShortBuffer { need: usize, got: usize },

    #[error("unsupported schema version {0}")]
    UnsupportedSchema(u8),

    #[error("message length {declared} mismatches fixed size {expected} for type {type_id:#04x}")]
    LengthMismatch { type_id: u8, declared: u8, expected: u8 },

    #[error("unknown message type id {0:#04x}")]
    BadTypeId(u8),

    #[error("datagram builder full: {attempted} bytes would exceed max {max}")]
    DatagramFull { attempted: usize, max: usize },

    /// `0x05` is marked reserved in every current feed spec. Two publishers
    /// transmit a private message there; a decoder must not silently invent a
    /// meaning for it.
    #[error("type id {0:#04x} is reserved and carries no message")]
    ReservedTypeId(u8),
}
