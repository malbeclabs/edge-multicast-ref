//! Top-of-book: a normalized quote or trade, lowered onto `0x03` and `0x04`.

use dz_adapter_core::{Aggressor, InstrumentRef, Scalar, SideUpdate, TradeFlags};
use dz_edge_tob::{
    Quote, Trade, QUOTE_ASK_GONE, QUOTE_ASK_UPDATED, QUOTE_BID_GONE, QUOTE_BID_UPDATED,
};

use crate::error::LoweringError;
use crate::instrument::{Instrument, InstrumentTable};
use crate::scale::{price_for, qty_for};
use crate::source::SourceId;

/// Everything the lowering needs that is not in the event.
///
/// # Why the table is an argument and not a field
///
/// It was a field first, and that was wrong: holding `&InstrumentTable` for
/// this type's lifetime borrows the table immutably for as long as a publisher
/// is lowering anything, and the reference-data owner needs it mutably to admit
/// and withdraw instruments. A publisher would have had to stop lowering to
/// admit an instrument — and for [`DepthLowering`](crate::DepthLowering), which
/// carries the per-instrument sequence, rebuilding it to release the borrow
/// would restart that sequence, which no subscriber can be told apart from a
/// channel reset.
#[derive(Debug, Clone, Copy)]
pub struct Lowering {
    source_id: SourceId,
}

impl Lowering {
    /// Bind the publisher's own `Source ID`.
    ///
    /// The `Source ID` is the publisher's registered identity and is the same
    /// for every message a process sends, which is why it is here and not in
    /// any event: there is no per-event decision to make about it, so there is
    /// no per-event parameter for one. It arrives as a [`SourceId`] rather than
    /// a `u16` because the registry reserves most of that space, and a value it
    /// does not admit is a startup error rather than something to discover one
    /// message at a time.
    #[must_use]
    pub const fn new(source_id: SourceId) -> Self {
        Self { source_id }
    }

    /// `Event::Quote` to `0x03 Quote`.
    ///
    /// # `Update Flags` is derived here and nowhere else
    ///
    /// One bit pair per side, and the two bits of a pair are mutually
    /// exclusive: a side that is [`SideUpdate::Present`] sets its *updated*
    /// bit, a side that is [`SideUpdate::Gone`] sets its *gone* bit, and
    /// neither side can set both. That is what both existing publishers derive
    /// independently on live feeds, and it is the whole reason the byte is not
    /// reachable from the adapter boundary.
    ///
    /// A gone side is written as a zero price and a zero quantity, which the
    /// specification requires. Zero is an in-range price on the wire, so those
    /// zeros mean nothing on their own and the flag is what says so — which is
    /// exactly why an adapter must not be able to write the flag.
    ///
    /// # Errors
    ///
    /// [`LoweringError::UnknownInstrument`] for a handle the table does not
    /// hold; [`LoweringError::Scale`] naming the field for a price or quantity
    /// that cannot be stated exactly at this instrument's exponent.
    pub fn lower_quote(
        &self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        bid: SideUpdate<'_>,
        ask: SideUpdate<'_>,
    ) -> Result<Quote, LoweringError> {
        let inst = instruments.get(instrument)?;

        let bid = lower_side(inst, bid, Sides::BID)?;
        let ask = lower_side(inst, ask, Sides::ASK)?;

        Ok(Quote {
            instrument_id: inst.instrument_id,
            source_id: self.source_id.get(),
            update_flags: bid.flag | ask.flag,
            source_timestamp_ns: source_ts_ns,
            bid_price: bid.price,
            bid_qty: bid.qty,
            ask_price: ask.price,
            ask_qty: ask.qty,
            bid_source_count: bid.source_count,
            ask_source_count: ask.source_count,
        })
    }

    /// `Event::Trade` to `0x04 Trade`.
    ///
    /// **One implementation, for every feed a venue publishes**, and this
    /// method is a call to it rather than a copy of it — see [`crate::trade`]
    /// for the obligation that makes the distinction matter, and
    /// [`DepthLowering::lower_trade`](crate::DepthLowering::lower_trade) for
    /// the other caller.
    ///
    /// # Errors
    ///
    /// As [`lower_quote`](Self::lower_quote).
    #[allow(clippy::too_many_arguments)]
    pub fn lower_trade(
        &self,
        instruments: &InstrumentTable,
        instrument: InstrumentRef,
        source_ts_ns: u64,
        px: Scalar<'_>,
        qty: Scalar<'_>,
        aggressor: Aggressor,
        trade_id: Option<u64>,
        cumulative_volume: Option<Scalar<'_>>,
        flags: TradeFlags,
    ) -> Result<Trade, LoweringError> {
        crate::trade::lower(
            self.source_id,
            instruments,
            instrument,
            source_ts_ns,
            px,
            qty,
            aggressor,
            trade_id,
            cumulative_volume,
            flags,
        )
    }
}

/// One side of a quote, at the wire's exponents, with its flag bit.
struct LoweredSide {
    price: i64,
    qty: u64,
    source_count: u16,
    flag: u8,
}

/// The flag bits and field names belonging to one side.
///
/// Passed as a unit so that the bid's bits can never be paired with the ask's
/// field names — the mistake that a call taking four loose arguments invites,
/// and the one whose result no round-trip test can see.
struct Sides {
    updated: u8,
    gone: u8,
    price_field: &'static str,
    qty_field: &'static str,
}

impl Sides {
    const BID: Self = Self {
        updated: QUOTE_BID_UPDATED,
        gone: QUOTE_BID_GONE,
        price_field: "bid_price",
        qty_field: "bid_qty",
    };
    const ASK: Self = Self {
        updated: QUOTE_ASK_UPDATED,
        gone: QUOTE_ASK_GONE,
        price_field: "ask_price",
        qty_field: "ask_qty",
    };
}

/// The derivation itself: two cases in, one bit out, and no way to reach both
/// bits of a pair.
fn lower_side(
    inst: &Instrument,
    side: SideUpdate<'_>,
    wire: Sides,
) -> Result<LoweredSide, LoweringError> {
    match side {
        SideUpdate::Gone => Ok(LoweredSide {
            price: 0,
            qty: 0,
            // Zero is the specification's "unavailable" for this field, and a
            // side with nothing resting has nothing to count.
            source_count: 0,
            flag: wire.gone,
        }),
        SideUpdate::Present {
            px,
            qty,
            source_count,
        } => Ok(LoweredSide {
            price: price_for(inst, px, wire.price_field)?,
            qty: qty_for(inst, qty, wire.qty_field)?,
            // The wire's sentinel for "the venue does not expose this" is zero
            // itself, and a present side has at least one resting order, so a
            // true zero cannot coexist with a quoted side.
            source_count: source_count.unwrap_or(0),
            flag: wire.updated,
        }),
    }
}
