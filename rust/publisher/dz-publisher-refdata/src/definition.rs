//! An `InstrumentDefinition` from what the venue stated plus the three fields
//! it does not own.

use dz_adapter_core::{AssetClass, InstrumentSpec, MarketModel, PriceBound, SettleType};
use dz_edge_core::Fit;
use dz_edge_refdata::{
    InstrumentDefinition, ASSET_CLASS_CRYPTO_SPOT, ASSET_CLASS_PERPETUAL_FUTURE,
    ASSET_CLASS_PREDICTION_BINARY, ASSET_CLASS_PREDICTION_CATEGORICAL,
    ASSET_CLASS_PREDICTION_SCALAR, ASSET_CLASS_UNKNOWN, LEG_LEN, MARKET_MODEL_AMM,
    MARKET_MODEL_CLOB, MARKET_MODEL_UNKNOWN, PRICE_BOUND_NON_NEGATIVE, PRICE_BOUND_UNBOUNDED,
    PRICE_BOUND_UNIT_INTERVAL, SETTLE_TYPE_CASH, SETTLE_TYPE_NA, SETTLE_TYPE_PHYSICAL, SYMBOL_LEN,
};
use dz_publisher_lowering::{
    price_for, qty_at, qty_for, ContractSize, Instrument, LoweringError, SourceId,
};

use crate::refusal::Refusal;

/// A `Manifest Seq` that is not one.
///
/// A composed definition carries this until it is emitted, where the current
/// `Manifest Seq` is stamped into it. The alternative — writing the value that
/// was current at composition — would publish a definition claiming to belong
/// to a manifest that has since been superseded, and a subscriber reconciling
/// the two would see a definition it cannot place.
const UNSTAMPED: u16 = 0;

/// One admitted instrument, in the two shapes the layers above need.
///
/// Both are produced by one function and from one set of conversions, because
/// they have to agree: the exponents and the contract factor in
/// [`instrument`](Self::instrument) are what every price and quantity for this
/// instrument is converted against on the hot path, and the `Tick Size` and
/// `Lot Size` in [`definition`](Self::definition) are what a subscriber reads
/// as the grid those values sit on. Composing them separately is how the two
/// come to disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Composition {
    /// What the lowering's table holds for this instrument.
    pub instrument: Instrument,
    /// What the reference-data feed publishes for it, `Manifest Seq` excepted.
    pub definition: InstrumentDefinition,
    /// How the venue's own strings fitted the wire's fixed-width fields.
    pub fits: Fits,
}

/// Whether each of the three text fields could be stated honestly.
///
/// Carried out rather than logged here, and reported by the caller once per
/// reference-data load, which is what the codec's own
/// [`Fit`] documentation asks for: per message it would be
/// noise, and silently it is a symbol nobody can resolve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fits {
    pub symbol: Fit,
    /// `None` when the venue stated no leg, which is not a fit of any kind.
    pub leg1: Option<Fit>,
    pub leg2: Option<Fit>,
}

impl Fits {
    /// Whether anything here is worth an operator's attention.
    #[must_use]
    pub fn all_fitted(self) -> bool {
        let leg_fitted = |leg: Option<Fit>| leg.is_none_or(|fit| fit == Fit::Fitted);
        self.symbol == Fit::Fitted && leg_fitted(self.leg1) && leg_fitted(self.leg2)
    }
}

/// The `Symbol` field for a venue's ticker, which is the identity this crate
/// keys on.
///
/// Separate from [`compose`] because the identity is needed before the rest of
/// the definition is: a re-offer of an already-admitted instrument is resolved
/// by this alone.
#[must_use]
pub fn symbol_field(ticker: &str) -> ([u8; SYMBOL_LEN], Fit) {
    dz_edge_core::pad_ascii::<SYMBOL_LEN>(ticker)
}

/// Compose a definition, or refuse the instrument.
///
/// # Every scalar is converted here, once
///
/// `Tick Size`, `Lot Size` and `Contract Value` are the venue's own decimals,
/// and they go through the same [`price_for`]/[`qty_for`] the hot path uses —
/// not a second conversion written for reference data. That matters most for a
/// venue quoting per contract: `price_for` divides a price by the contract
/// factor and `qty_for` multiplies a quantity by it, so a tick size composed
/// any other way would describe a grid none of the published prices sit on.
///
/// `Contract Value` is the exception to the factor and not to the conversion.
/// It is carried at `Price Exponent` — it is a value, not a size — but the
/// contract factor is deliberately *not* applied to it: the field states what
/// one contract is worth, so restating it per unit of the underlying would
/// answer a question nobody asked. It goes through [`qty_at`] rather than
/// `price_at` because the wire field is unsigned, and a venue stating a
/// negative contract value is refused rather than published as an enormous
/// positive one.
///
/// # Errors
///
/// [`Refusal`], and every one of them means the instrument is not admitted.
/// That order is the whole point: an `Instrument ID` is minted only after a
/// definition has been composed, so a published ID always resolves to a
/// definition. The alternative — admit now, refuse per message later — is an
/// instrument that appears in the manifest, is counted in every dashboard, and
/// carries no data.
pub fn compose(
    spec: &InstrumentSpec<'_>,
    instrument_id: u32,
    source_id: SourceId,
) -> Result<Composition, Refusal> {
    let quoted_per_contract = match spec.quoted_per_contract {
        None => None,
        Some(stated) => Some(ContractSize::from_scalar(stated).ok_or(Refusal::ContractSize)?),
    };
    let instrument = Instrument {
        instrument_id,
        price_exponent: spec.price_exponent,
        qty_exponent: spec.qty_exponent,
        quoted_per_contract,
    };

    let tick_size = price_for(&instrument, spec.tick_size, "tick_size")?;
    let lot_size = qty_for(&instrument, spec.lot_size, "lot_size")?;
    let contract_value = match spec.contract_value {
        // Zero for a venue that defines no contract. The codec names no
        // constant for it because there is one sentinel and it is the field
        // left at zero; the same is true of `Expiry NS` below.
        None => 0,
        Some(stated) => qty_at(stated, spec.price_exponent).map_err(|source| {
            Refusal::Field(LoweringError::Scale {
                field: "contract_value",
                source,
            })
        })?,
    };

    let mut definition = InstrumentDefinition {
        instrument_id,
        source_id: source_id.get(),
        symbol: [0u8; SYMBOL_LEN],
        leg1: [0u8; LEG_LEN],
        leg2: [0u8; LEG_LEN],
        asset_class: asset_class_byte(spec.asset_class),
        price_exponent: spec.price_exponent,
        qty_exponent: spec.qty_exponent,
        market_model: market_model_byte(spec.market_model),
        tick_size,
        lot_size,
        contract_value,
        expiry_ns: spec.expiry_ns.unwrap_or(0),
        settle_type: settle_type_byte(spec.settle_type),
        price_bound: price_bound_byte(spec.price_bound),
        manifest_seq: UNSTAMPED,
    };
    let fits = Fits {
        symbol: definition.set_symbol(spec.symbol),
        leg1: spec.leg1.map(|leg| definition.set_leg1(leg)),
        leg2: spec.leg2.map(|leg| definition.set_leg2(leg)),
    };

    Ok(Composition {
        instrument,
        definition,
        fits,
    })
}

/// Whether two definitions describe the same published instrument.
///
/// `Manifest Seq` is excluded because it is stamped at emission and is not part
/// of what the venue stated. Everything else is compared, so a venue restating
/// a tick size is a change to the published set and advances the manifest,
/// while a venue re-offering exactly what it offered before is not.
#[must_use]
pub fn same_definition(left: &InstrumentDefinition, right: &InstrumentDefinition) -> bool {
    InstrumentDefinition {
        manifest_seq: UNSTAMPED,
        ..*left
    } == InstrumentDefinition {
        manifest_seq: UNSTAMPED,
        ..*right
    }
}

/// A definition as it goes on the wire, carrying the manifest it belongs to.
#[must_use]
pub fn stamped(definition: &InstrumentDefinition, manifest_seq: u16) -> InstrumentDefinition {
    InstrumentDefinition {
        manifest_seq,
        ..*definition
    }
}

// The four wire tables. Each is a total match on the boundary's enumeration, so
// a variant added there is a compile error here rather than a byte defaulted to
// zero - which is the shipped failure these tables are shaped against: an
// encoder numbering a table from the wrong variant is self-consistent, and
// therefore invisible to every round-trip test.

const fn asset_class_byte(asset_class: AssetClass) -> u8 {
    match asset_class {
        AssetClass::Unknown => ASSET_CLASS_UNKNOWN,
        AssetClass::CryptoSpot => ASSET_CLASS_CRYPTO_SPOT,
        AssetClass::PredictionBinary => ASSET_CLASS_PREDICTION_BINARY,
        AssetClass::PredictionScalar => ASSET_CLASS_PREDICTION_SCALAR,
        AssetClass::PredictionCategorical => ASSET_CLASS_PREDICTION_CATEGORICAL,
        AssetClass::PerpetualFuture => ASSET_CLASS_PERPETUAL_FUTURE,
    }
}

const fn market_model_byte(market_model: MarketModel) -> u8 {
    match market_model {
        MarketModel::Unknown => MARKET_MODEL_UNKNOWN,
        MarketModel::Clob => MARKET_MODEL_CLOB,
        MarketModel::Amm => MARKET_MODEL_AMM,
    }
}

const fn settle_type_byte(settle_type: SettleType) -> u8 {
    match settle_type {
        SettleType::NotApplicable => SETTLE_TYPE_NA,
        SettleType::Cash => SETTLE_TYPE_CASH,
        SettleType::Physical => SETTLE_TYPE_PHYSICAL,
    }
}

const fn price_bound_byte(price_bound: PriceBound) -> u8 {
    match price_bound {
        PriceBound::Unbounded => PRICE_BOUND_UNBOUNDED,
        PriceBound::UnitInterval => PRICE_BOUND_UNIT_INTERVAL,
        PriceBound::NonNegative => PRICE_BOUND_NON_NEGATIVE,
    }
}
