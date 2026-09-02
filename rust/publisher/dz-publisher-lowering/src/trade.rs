//! `0x04 Trade`: one implementation, and the file exists to say so.
//!
//! The wire's cross-specification policy is explicit about this message: a
//! Type ID appearing in more than one sibling feed must carry the same meaning
//! in each, and `Trade` is **byte-for-byte identical** between the top-of-book
//! feed, the market-by-price feed and the market-by-order feed. So a venue
//! publishing two of those feeds owes the same bytes on both, for the same
//! execution.
//!
//! In one existing publisher that obligation is held by a doc comment across
//! two separate encoder implementations, checked by hand. Here it is held by
//! there being one function: [`Lowering::lower_trade`](crate::Lowering) and
//! [`DepthLowering::lower_trade`](crate::DepthLowering) both call
//! [`lower`], and neither has a body of its own to drift.
//!
//! `tests/trade_is_one_implementation.rs` is what proves it stays that way.

use dz_adapter_core::{Aggressor, InstrumentRef, Scalar, TradeFlags};
use dz_edge_tob::{
    Trade, AGGRESSOR_BUY, AGGRESSOR_SELL, AGGRESSOR_UNKNOWN, TRADE_FLAG_BLOCK, TRADE_FLAG_CROSS,
    TRADE_FLAG_SWEEP,
};

use crate::error::LoweringError;
use crate::instrument::InstrumentTable;
use crate::scale::{price_for, qty_for};
use crate::source::SourceId;

/// `Event::Trade` to `0x04 Trade`.
///
/// The three sentinels are the specification's own, and each is what the venue
/// not publishing something looks like on the wire: no trade identifier is `0`,
/// no running total is `0`, and an unstated aggressor is the `Unknown` value
/// rather than a guess. Neither existing publisher exposes a running total on
/// its trade events, and neither sets a qualifier bit.
///
/// # Errors
///
/// [`LoweringError::UnknownInstrument`] for a handle the table does not hold,
/// and the conversion refusals named per field.
// The parameters are exactly the fields of `Event::Trade`. Grouping them into a
// struct would be a second definition of that variant, in another crate, free
// to drift from it — which is the failure this whole boundary exists to
// prevent, traded for one lint.
#[allow(clippy::too_many_arguments)]
pub(crate) fn lower(
    source_id: SourceId,
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
    let inst = instruments.get(instrument)?;

    let cumulative_volume = match cumulative_volume {
        Some(volume) => qty_for(inst, volume, "cumulative_volume")?,
        None => 0,
    };

    Ok(Trade {
        instrument_id: inst.instrument_id,
        source_id: source_id.get(),
        aggressor_side: aggressor_byte(aggressor),
        trade_flags: trade_flags_byte(flags),
        source_timestamp_ns: source_ts_ns,
        trade_price: price_for(inst, px, "trade_price")?,
        trade_qty: qty_for(inst, qty, "trade_qty")?,
        trade_id: trade_id.unwrap_or(0),
        cumulative_volume,
    })
}

/// The aggressor byte. Exhaustive, so a fourth case fails to compile here.
const fn aggressor_byte(aggressor: Aggressor) -> u8 {
    match aggressor {
        Aggressor::Unknown => AGGRESSOR_UNKNOWN,
        Aggressor::Buy => AGGRESSOR_BUY,
        Aggressor::Sell => AGGRESSOR_SELL,
    }
}

/// The trade qualifier byte, composed from the three booleans the boundary
/// carries. A bit nobody defined is unreachable because there is no fourth
/// boolean.
const fn trade_flags_byte(flags: TradeFlags) -> u8 {
    let mut byte = 0;
    if flags.block {
        byte |= TRADE_FLAG_BLOCK;
    }
    if flags.sweep {
        byte |= TRADE_FLAG_SWEEP;
    }
    if flags.cross {
        byte |= TRADE_FLAG_CROSS;
    }
    byte
}
