//! The key the two streams are joined on.
//!
//! **This is a join and not an alignment**, and the difference is the whole
//! reason the interface was constrained the way it was. Nothing here compares
//! positions, orders, proximities or timestamps-within-a-window: a message on
//! the wire and a message in the re-lowered stream are the same message when
//! their keys are equal, and are not otherwise.
//!
//! Two key spaces, because the two feeds carry different identity.

use dz_edge_mbp::{BookClear, LevelUpdate};
use dz_edge_tob::{Quote, Trade};

/// What identifies one message independently of how it was framed.
///
/// Every component is a field of the message body. Nothing from the datagram
/// header is here — not the `Channel ID`, not the `Sequence Number`, not the
/// send timestamp — because those are exactly the things a batching or pacing
/// difference moves, and the fourth finding class exists to say that moving them
/// is not a defect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JoinKey {
    /// Depth: `(Instrument ID, Per-Instrument Seq)`.
    ///
    /// The publisher's own counter, keyed on the instrument and dense within an
    /// era, stamped by the lowering and by nothing else. `LevelUpdate` and
    /// `BookClear` share one series because both mutate the book and their
    /// relative order is significant — so they share one key space here too,
    /// and a `LevelUpdate` the publisher sent where the re-lowering produced a
    /// `BookClear` is a field difference at one key rather than two
    /// unexplained absences.
    Depth {
        instrument_id: u32,
        per_instrument_seq: u32,
    },

    /// Top-of-book: `(Instrument ID, source timestamp, tie)`.
    ///
    /// This feed has no per-instrument sequence, so the venue's own timestamp
    /// does the work — which is sound because it is the venue's stamp for the
    /// upstream event and not anybody's clock reading. See [`TopOfBookTie`] for
    /// the third component and why a quote's and a trade's differ.
    TopOfBook {
        instrument_id: u32,
        source_timestamp_ns: u64,
        tie: TopOfBookTie,
    },
}

/// The third component of a top-of-book key.
///
/// A venue can stamp two events at one nanosecond, so `(Instrument ID, source
/// timestamp)` alone is not unique and a join on it would pair a quote with a
/// trade. What breaks the tie differs by message type, and neither choice is
/// free:
///
/// - For a quote, `Update Flags` — which is the design's own key and the right
///   one: it is derived from the pair of sides, so two quotes stamped at the
///   same instant that differ at all in *which* sides they carry get different
///   keys.
/// - For a trade, `Trade ID`, because it is the only other deterministic
///   identity on the message: the venue assigned it, both copies carry it
///   unchanged, and nothing the publisher does to it is time-dependent. It is
///   `0` for a venue that publishes none, and then two trades on one instrument
///   at one nanosecond collide — which is reported as an ambiguous key rather
///   than resolved by guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TopOfBookTie {
    /// The quote's `Update Flags` byte.
    UpdateFlags(u8),
    /// The trade's venue-assigned `Trade ID`, or `0` where the venue assigns
    /// none.
    TradeId(u64),
}

impl JoinKey {
    /// The key a `Quote` joins on.
    #[must_use]
    pub const fn of_quote(quote: &Quote) -> Self {
        Self::TopOfBook {
            instrument_id: quote.instrument_id,
            source_timestamp_ns: quote.source_timestamp_ns,
            tie: TopOfBookTie::UpdateFlags(quote.update_flags),
        }
    }

    /// The key a `Trade` joins on.
    #[must_use]
    pub const fn of_trade(trade: &Trade) -> Self {
        Self::TopOfBook {
            instrument_id: trade.instrument_id,
            source_timestamp_ns: trade.source_timestamp_ns,
            tie: TopOfBookTie::TradeId(trade.trade_id),
        }
    }

    /// The key a `LevelUpdate` joins on.
    #[must_use]
    pub const fn of_level(level: &LevelUpdate) -> Self {
        Self::Depth {
            instrument_id: level.instrument_id,
            per_instrument_seq: level.per_instrument_seq,
        }
    }

    /// The key a `BookClear` joins on.
    #[must_use]
    pub const fn of_clear(clear: &BookClear) -> Self {
        Self::Depth {
            instrument_id: clear.instrument_id,
            per_instrument_seq: clear.per_instrument_seq,
        }
    }

    /// The instrument this key belongs to, for naming a finding.
    #[must_use]
    pub const fn instrument_id(&self) -> u32 {
        match self {
            Self::Depth { instrument_id, .. } | Self::TopOfBook { instrument_id, .. } => {
                *instrument_id
            }
        }
    }
}

impl core::fmt::Display for JoinKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Depth {
                instrument_id,
                per_instrument_seq,
            } => write!(
                f,
                "instrument {instrument_id}, per-instrument seq {per_instrument_seq}"
            ),
            Self::TopOfBook {
                instrument_id,
                source_timestamp_ns,
                tie,
            } => match tie {
                TopOfBookTie::UpdateFlags(flags) => write!(
                    f,
                    "instrument {instrument_id}, source timestamp {source_timestamp_ns}, update flags {flags:#04x}"
                ),
                TopOfBookTie::TradeId(trade_id) => write!(
                    f,
                    "instrument {instrument_id}, source timestamp {source_timestamp_ns}, trade id {trade_id}"
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_quote_and_a_trade_stamped_at_the_same_instant_do_not_share_a_key() {
        // The reason the tie exists. Both are one instrument at one nanosecond,
        // and a join on `(Instrument ID, source timestamp)` alone would pair
        // them and report the difference between a quote and a trade as a
        // lowering defect.
        let quote = Quote {
            instrument_id: 7,
            source_id: 1000,
            update_flags: 0x03,
            source_timestamp_ns: 1_700_000_000_000_000_000,
            bid_price: 1,
            bid_qty: 1,
            ask_price: 2,
            ask_qty: 1,
            bid_source_count: 0,
            ask_source_count: 0,
        };
        let trade = Trade {
            instrument_id: 7,
            source_id: 1000,
            aggressor_side: 0,
            trade_flags: 0,
            source_timestamp_ns: 1_700_000_000_000_000_000,
            trade_price: 1,
            trade_qty: 1,
            trade_id: 0,
            cumulative_volume: 0,
        };
        assert_ne!(JoinKey::of_quote(&quote), JoinKey::of_trade(&trade));
    }

    #[test]
    fn a_level_update_and_a_book_clear_share_one_key_space() {
        // One `Per-Instrument Seq` series covers both, so one key space covers
        // both: the publisher that sent a clear where the venue's events said a
        // level is a difference at one key, not two absences.
        let level = LevelUpdate {
            instrument_id: 3,
            source_id: 1000,
            side: 0,
            action: 3,
            per_instrument_seq: 9,
            price_raw: 0,
            qty_raw: 0,
            timestamp_ns: 1,
            order_count: 0xFFFF,
            level_index: 0xFFFF,
            update_reason: 0,
            level_flags: 0,
        };
        let clear = BookClear {
            instrument_id: 3,
            source_id: 1000,
            clear_side: 0,
            scope: 0,
            per_instrument_seq: 9,
            from_price_raw: 0,
            timestamp_ns: 1,
            clear_reason: 0,
        };
        assert_eq!(JoinKey::of_level(&level), JoinKey::of_clear(&clear));
    }
}
