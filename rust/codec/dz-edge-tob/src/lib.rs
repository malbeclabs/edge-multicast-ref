//! Top-of-Book & Trades feed wire format.

#![forbid(unsafe_code)]

pub mod quote;
pub mod trade;

pub use quote::{Quote, QUOTE_ASK_GONE, QUOTE_ASK_UPDATED, QUOTE_BID_GONE, QUOTE_BID_UPDATED};
pub use trade::{
    Trade, AGGRESSOR_BUY, AGGRESSOR_SELL, AGGRESSOR_UNKNOWN, TRADE_FLAG_BLOCK, TRADE_FLAG_CROSS,
    TRADE_FLAG_SWEEP,
};

/// Datagram delimiter for the top-of-book feed: "DZ", little-endian on the wire.
pub const MAGIC_TOB: u16 = 0x445A;

/// The Top-of-Book & Trades feed.
pub struct TopOfBook;

impl dz_edge_core::Feed for TopOfBook {
    const MAGIC: u16 = MAGIC_TOB;
    const NAME: &'static str = "top-of-book";

    /// This feed's message table, transcribed from the specification.
    ///
    /// `0x05` is absent: the table steps from `0x04` to `0x06`. See this
    /// crate's README for what the reference Go parser does with it and why
    /// nothing here emits or rejects it.
    ///
    /// `0x08 Liquidation` is listed because the specification lists it. No type
    /// in this crate encodes one yet, so nothing can push it — a table that
    /// described what is implemented rather than what the feed carries would
    /// have to change every time a message lands, which is the opposite of
    /// what makes it a control.
    const CARRIES: &'static [u8] = &[
        0x01, // Heartbeat
        0x02, // InstrumentDefinition
        0x03, // Quote
        0x04, // Trade
        0x06, // EndOfSession
        0x07, // ManifestSummary
        0x08, // Liquidation
    ];
}
