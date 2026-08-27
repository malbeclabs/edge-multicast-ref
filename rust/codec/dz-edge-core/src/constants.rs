//! Wire constants, transcribed from the edge-feed-spec field tables.

/// The schema generation this build emits. A publisher speaks one generation:
/// there is no reader asking it to downgrade, and a mixture would make the
/// version byte meaningless.
pub const SCHEMA_VERSION: u8 = 3;

/// The generation before the 3.0.0 cut: `Symbol` was `char[16]` and there was
/// no `Source ID`. Decode-only; nothing here emits it.
pub const SCHEMA_VERSION_V1: u8 = 1;

/// Generations a *reader* accepts, newest first.
///
/// There is no `2`. A 128-byte `InstrumentDefinition` carrying the widened
/// `Symbol` without `Source ID` was specified and superseded before any
/// publisher emitted it, so accepting it would invent a generation that never
/// reached the wire.
pub const SUPPORTED_SCHEMA_VERSIONS: [u8; 2] = [SCHEMA_VERSION, SCHEMA_VERSION_V1];

/// Datagram header size in bytes.
pub const DATAGRAM_HEADER_SIZE: usize = 24;

/// Application message header size in bytes.
pub const MSG_HEADER_SIZE: usize = 4;

/// Maximum datagram size in bytes.
///
/// **Mandated, not derived.** Every feed spec states 1,232 bytes "to leave room
/// for GRE encapsulation headers used by the DoubleZero network's last-mile
/// delivery". Do not recompute it from a path MTU; that is how it drifted, and
/// a configuration key that can express a larger value is why this constant is
/// not a default.
pub const MAX_DATAGRAM_SIZE: usize = 1232;

// Shared message type IDs. `0x05` is reserved in every current spec and is
// deliberately absent.
pub const TYPE_HEARTBEAT: u8 = 0x01;
pub const TYPE_END_OF_SESSION: u8 = 0x06;

// Shared payload sizes, including the 4-byte message header.
pub const SIZE_HEARTBEAT: usize = 16;
pub const SIZE_END_OF_SESSION: usize = 12;

/// Message header flag bit 0: set on the snapshot port, cleared elsewhere.
pub const FLAG_SNAPSHOT: u16 = 0x0001;
