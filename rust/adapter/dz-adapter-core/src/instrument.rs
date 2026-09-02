//! The handle an adapter carries for an instrument, and what it declares to get
//! one.

use crate::scalar::Scalar;

/// An instrument the runtime has admitted, as the adapter refers to it.
///
/// **This is not an `Instrument ID`.** It is a dense index into the runtime's
/// own table, handed back by [`ListingSink::list`](crate::ListingSink::list) at
/// admission and carried in the adapter's per-symbol state so that the hot path
/// costs an array index rather than a hash of a venue ticker. The wire
/// `Instrument ID` is minted, persisted and published by the reference-data
/// owner, and never appears in this crate — an adapter that could name one
/// could name one that was never published, and a subscriber resolving it would
/// find nothing.
///
/// What it guarantees, precisely: an adapter cannot express a wire identifier,
/// and an index the runtime does not hold is refused where the refusal can be
/// counted. It is a handle, not a capability — it carries no proof of its own
/// origin, and the constructor is public because the runtime that admits
/// instruments lives in another crate. The name says who may call it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct InstrumentRef(u32);

impl InstrumentRef {
    /// Mint a handle for an instrument that has just been admitted.
    ///
    /// **For the runtime that owns the admitted set.** An adapter obtains its
    /// handles from [`ListingSink::list`](crate::ListingSink::list) and calls
    /// this never; a call site inside an adapter is the thing to reject in
    /// review, and the name is what makes it visible there.
    #[must_use]
    pub const fn from_admission(index: u32) -> Self {
        Self(index)
    }

    /// The index this handle holds.
    #[must_use]
    pub const fn index(self) -> u32 {
        self.0
    }
}

/// What a venue knows about one instrument, and the runtime does not.
///
/// This is `InstrumentDefinition` minus the three fields the runtime owns: the
/// `Instrument ID` it mints, the `Source ID` from configuration, and the
/// `Manifest Seq` its reference-data cycle maintains. Everything left is a
/// property of the venue's own listing.
///
/// The two exponents are the exception to this crate's rule that a venue states
/// no scale: they are not a scaling decision, they are the declaration a
/// subscriber reads out of `InstrumentDefinition` to interpret every price and
/// quantity for this instrument. Stating them is the venue's job. *Applying*
/// them is not, which is why [`Scalar`] carries unconverted values and these are
/// the numbers it will be converted against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstrumentSpec<'a> {
    /// The venue's own ticker. Padded and truncated to the wire's width above
    /// this boundary, so an adapter states the symbol it has and does not
    /// count bytes.
    pub symbol: &'a str,
    /// The first leg of a multi-leg instrument, if it has one.
    pub leg1: Option<&'a str>,
    /// The second leg of a multi-leg instrument, if it has one.
    pub leg2: Option<&'a str>,
    pub asset_class: AssetClass,
    /// The decimal exponent every price for this instrument is carried at.
    pub price_exponent: i8,
    /// The decimal exponent every quantity for this instrument is carried at.
    pub qty_exponent: i8,
    pub market_model: MarketModel,
    /// The minimum price increment.
    pub tick_size: Scalar<'a>,
    /// The minimum quantity increment.
    pub lot_size: Scalar<'a>,
    /// The value of one contract, where the venue defines one.
    pub contract_value: Option<Scalar<'a>>,
    /// Expiry as a nanosecond timestamp, for an instrument that has one.
    pub expiry_ns: Option<u64>,
    pub settle_type: SettleType,
    pub price_bound: PriceBound,
}

/// What kind of thing is being traded.
///
/// Mirrors the `Asset Class` table in the reference-data specification. A Rust
/// enumeration rather than the wire's `u8` so that a venue cannot state a value
/// outside the table, which is the failure the `Action` table already produced
/// on another feed: an encoder numbering a table from the wrong variant is
/// self-consistent, and therefore invisible to every round-trip test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AssetClass {
    Unknown,
    CryptoSpot,
    PredictionBinary,
    PredictionScalar,
    PredictionCategorical,
    PerpetualFuture,
}

/// How the venue matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketModel {
    Unknown,
    /// Central limit order book.
    Clob,
    /// Automated market maker.
    Amm,
}

/// How the instrument settles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SettleType {
    /// The instrument does not settle.
    NotApplicable,
    Cash,
    Physical,
}

/// The range prices for this instrument may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PriceBound {
    Unbounded,
    /// Bounded to `[0, 1]`.
    UnitInterval,
    NonNegative,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_handle_returns_the_index_it_was_minted_from() {
        assert_eq!(InstrumentRef::from_admission(7).index(), 7);
    }

    #[test]
    fn handles_order_by_index() {
        // Ordered so a runtime can hold admitted instruments in a sorted
        // structure; the order is the admission order and means nothing else.
        assert!(InstrumentRef::from_admission(1) < InstrumentRef::from_admission(2));
    }
}
