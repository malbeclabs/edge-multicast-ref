//! Midpoint feed wire format.
//!
//! One derived price per instrument: a mid from an order book, or a VWAP from
//! trades. `Heartbeat` and `EndOfSession` live in `dz-edge-core`;
//! `InstrumentDefinition` and `ManifestSummary` live in `dz-edge-refdata`. The
//! midpoint feed uses the same 130-byte `InstrumentDefinition` as the other
//! feeds from its `3.0.0`. This crate holds only the `Midpoint` message.

#![forbid(unsafe_code)]

pub mod midpoint;

pub use midpoint::{
    is_book_method, is_trade_method, Midpoint, MidpointRuleError, METHOD_BBO_MID, METHOD_CUSTOM,
    METHOD_LAST_TRADE_FALLBACK, METHOD_MICRO_PRICE, METHOD_UNKNOWN, METHOD_VWAP,
    METHOD_WEIGHTED_MID, QUALITY_CROSSED_OR_LOCKED, QUALITY_ONE_SIDED, QUALITY_STALE,
    QUALITY_SYNTHETIC,
};

/// Datagram delimiter for the midpoint feed: "DM", little-endian on the wire.
pub const MAGIC_MIDPOINT: u16 = 0x4D44;

/// The Midpoint feed.
pub struct MidpointFeed;

impl dz_edge_core::Feed for MidpointFeed {
    const MAGIC: u16 = MAGIC_MIDPOINT;
    const NAME: &'static str = "midpoint";

    /// This feed's message table, transcribed from the specification.
    ///
    /// `0x04` is reserved: it is `Trade` in the top-of-book feed.
    const CARRIES: &'static [u8] = &[
        0x01, // Heartbeat
        0x02, // InstrumentDefinition
        0x03, // Midpoint
        0x06, // EndOfSession
        0x07, // ManifestSummary
    ];
}
