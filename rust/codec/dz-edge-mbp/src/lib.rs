//! Market-by-Price feed wire format.
//!
//! The first depth feed in these crates, and the first to carry the snapshot
//! port role. What distinguishes it from top-of-book is not the depth alone: a
//! subscriber to this feed holds a book that only exists because it applied
//! every message in order, so the messages here are the ones whose *sequence*
//! carries meaning, and the analysis tier reads them to say whether a book the
//! publisher described could have been built from what it sent.
//!
//! Only the depth-grain messages live here. `Heartbeat`, `ManifestSummary` and
//! the rest are the family's and live in `dz-edge-core`; `Trade` and
//! `InstrumentDefinition` are byte-identical to their siblings' and live in
//! `dz-edge-tob` and `dz-edge-refdata`. This crate holds what only this feed
//! has.
//!
//! Two rules govern the bodies below, and both are the spec's rather than this
//! crate's. **Quantity is absolute, never a delta** — a `LevelUpdate` carries
//! the aggregate resting quantity at the price *after* the change, and zero
//! removes the level, so a subscriber that missed a message has a wrong book
//! and not a drifting one. And **`Action`, `Level Index` and `Update Reason`
//! are informational**: they must not gate the apply, because a subscriber that
//! branched on them would disagree with one that did not about a book they both
//! received identically. Informational to the *subscriber*, that is — a
//! publisher still owes `Action = Delete` on every zero quantity and no other,
//! which is a rule the specification names and a bug that has shipped.

#![forbid(unsafe_code)]

pub mod book_clear;
pub mod level_update;
pub mod snapshot;

pub use book_clear::{
    BookClear, CLEAR_ASK, CLEAR_BID, CLEAR_BOTH, SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE,
};
pub use level_update::{LevelUpdate, ACTION_CHANGE, ACTION_DELETE, ACTION_NEW, ACTION_UNKNOWN};
pub use snapshot::{SnapshotBegin, SnapshotEnd, SnapshotLevel};

/// Datagram delimiter for the market-by-price feed: "BD", little-endian on the
/// wire.
///
/// Distinct from the top-of-book feed's, which is what makes a datagram
/// misrouted between two sibling feeds refusable rather than parseable at the
/// wrong layout.
pub const MAGIC_MBP: u16 = 0x4442;

/// The bid side of a book.
pub const SIDE_BID: u8 = 0;
/// The ask side of a book.
pub const SIDE_ASK: u8 = 1;

/// The shared sentinel for `Order Count` and `Level Index`.
///
/// It means *not provided, or beyond what this field can express*, and it
/// saturates rather than wrapping. It must never be read as a magnitude: it is
/// neither a count of 65535 orders nor a rank of 65535, and a consumer that
/// averaged it would report a number no publisher sent.
pub const U16_UNAVAILABLE: u16 = 0xFFFF;

/// The three checks every `decode` in this crate makes before reading a field,
/// in the order that keeps each one answerable.
///
/// Length first, because a buffer too short to hold the type id cannot be
/// judged by it; then the type id, because a body decoded at the wrong
/// layout is worse than a refusal; then the declared length, which is the
/// publisher's own claim about the message and is checked against the size
/// this build knows rather than trusted.
fn check_header<M: dz_edge_core::AppMessage>(buf: &[u8]) -> Result<(), dz_edge_core::DecodeError> {
    if buf.len() < M::SIZE {
        return Err(dz_edge_core::DecodeError::ShortBuffer {
            need: M::SIZE,
            got: buf.len(),
        });
    }
    if buf[0] != M::TYPE_ID {
        return Err(dz_edge_core::DecodeError::BadTypeId(buf[0]));
    }
    if buf[1] as usize != M::SIZE {
        return Err(dz_edge_core::DecodeError::LengthMismatch {
            type_id: M::TYPE_ID,
            declared: buf[1],
            expected: M::SIZE as u8,
        });
    }
    Ok(())
}

/// The three little-endian reads the bodies share. `at` is always a literal in
/// the caller, so the ranges are the wire layout written down once per field.
fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(
        buf[at..at + 4]
            .try_into()
            .expect("range width matches the target array"),
    )
}

fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(
        buf[at..at + 8]
            .try_into()
            .expect("range width matches the target array"),
    )
}

fn i64_at(buf: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(
        buf[at..at + 8]
            .try_into()
            .expect("range width matches the target array"),
    )
}

/// The Market-by-Price feed.
pub struct MarketByPrice;

impl dz_edge_core::Feed for MarketByPrice {
    const MAGIC: u16 = MAGIC_MBP;
    const NAME: &'static str = "market-by-price";

    /// This feed's message table, transcribed from the specification.
    ///
    /// **`0x03` is absent deliberately, and the specification says why**: it is
    /// `Quote` in the top-of-book feed and `Midpoint` in the midpoint feed, and
    /// it is *"intentionally unused here to prevent accidental cross-decoding
    /// if a datagram is misrouted"*. Until this table existed nothing enforced
    /// that on the emitting side — a builder is generic over its feed, so the
    /// magic was always right and a `Quote` went into a market-by-price
    /// datagram unrefused.
    ///
    /// `0x05` is reserved and absent for the same reason.
    ///
    /// Four Type IDs are shared with the market-by-order feed at its own
    /// numbers rather than renumbered into this feed's range, because they are
    /// the same payload and reassignment is what the policy forbids.
    const CARRIES: &'static [u8] = &[
        0x01, // Heartbeat
        0x02, // InstrumentDefinition
        0x04, // Trade
        0x06, // EndOfSession
        0x07, // ManifestSummary
        0x08, // Liquidation
        0x13, // BatchBoundary
        0x14, // InstrumentReset
        0x20, // SnapshotBegin
        0x22, // SnapshotEnd
        0x40, // LevelUpdate
        0x41, // BookClear
        0x42, // SnapshotLevel
    ];
}
