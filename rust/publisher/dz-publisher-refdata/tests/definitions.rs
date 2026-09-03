//! The composed `InstrumentDefinition`: every field the venue stated, the three
//! it did not, and the instruments that are never admitted at all.
//!
//! Every expected value below is derived by hand from the venue's statement and
//! the codec's own constants, and none is read off the code under test. The
//! failure that discipline is aimed at has shipped on another feed: a wire
//! table transcribed wrongly and then checked only against itself is
//! self-consistent, and therefore invisible to any test that encodes and then
//! decodes.

use dz_adapter_core::{
    AssetClass, InstrumentSpec, ListingSink, MarketModel, PriceBound, Scalar, SettleType,
};
use dz_edge_core::Fit;
use dz_edge_refdata::{
    ASSET_CLASS_PERPETUAL_FUTURE, ASSET_CLASS_UNKNOWN, LEG_LEN, MARKET_MODEL_AMM,
    MARKET_MODEL_UNKNOWN, PRICE_BOUND_UNBOUNDED, PRICE_BOUND_UNIT_INTERVAL, SETTLE_TYPE_CASH,
    SETTLE_TYPE_NA, SYMBOL_LEN,
};
use dz_publisher_lowering::{LoweringError, SourceId};
use dz_publisher_refdata::{
    compose, CycleSchedule, ManualClock, MemoryStore, Refusal, Registry, RegistryConfig,
    SelectionPolicy,
};

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

/// A venue that states everything: two legs, an expiry, a contract value, and a
/// bounded price.
fn everything() -> InstrumentSpec<'static> {
    InstrumentSpec {
        symbol: "BTC-USD-PERP",
        leg1: Some("BTC"),
        leg2: Some("USD"),
        asset_class: AssetClass::PerpetualFuture,
        price_exponent: -8,
        qty_exponent: -6,
        market_model: MarketModel::Amm,
        tick_size: Scalar::text("0.05"),
        lot_size: Scalar::text("0.001"),
        contract_value: Some(Scalar::text("2.5")),
        quoted_per_contract: None,
        expiry_ns: Some(1_700_000_000_000_000_000),
        settle_type: SettleType::Cash,
        price_bound: PriceBound::UnitInterval,
    }
}

/// A venue that states as little as the boundary allows.
fn nothing_optional() -> InstrumentSpec<'static> {
    InstrumentSpec {
        symbol: "AAA",
        leg1: None,
        leg2: None,
        asset_class: AssetClass::Unknown,
        price_exponent: 0,
        qty_exponent: 0,
        market_model: MarketModel::Unknown,
        tick_size: Scalar::fixed(1, 0),
        lot_size: Scalar::fixed(1, 0),
        contract_value: None,
        quoted_per_contract: None,
        expiry_ns: None,
        settle_type: SettleType::NotApplicable,
        price_bound: PriceBound::Unbounded,
    }
}

fn config(selection: SelectionPolicy) -> RegistryConfig {
    RegistryConfig {
        source_id: source_id(),
        channel_id: 3,
        selection,
        schedule: CycleSchedule::new(std::time::Duration::from_secs(30), 1232, 8),
    }
}

fn registry() -> Registry<MemoryStore, ManualClock> {
    let mut registry = Registry::open(
        config(SelectionPolicy::from_seed(8).expect("8 is a seed")),
        MemoryStore::new(),
        ManualClock::new(),
    )
    .expect("an empty directory is a cold start");
    registry.seeding_complete();
    registry
}

#[test]
fn a_composed_definition_carries_every_field_the_venue_stated() {
    let composed = compose(&everything(), 41, source_id()).expect("every scalar is exact");
    let definition = composed.definition;

    // The three fields the venue does not own.
    assert_eq!(definition.instrument_id, 41);
    assert_eq!(definition.source_id, 7);
    // `Manifest Seq` is stamped at emission, not at composition: a definition
    // held between two changes to the published set would otherwise claim a
    // manifest that no longer exists.
    assert_eq!(definition.manifest_seq, 0);

    // `Symbol`, `Leg 1` and `Leg 2` are left-justified and NUL-padded.
    assert_eq!(&definition.symbol[..12], b"BTC-USD-PERP");
    assert!(definition.symbol[12..].iter().all(|&byte| byte == 0));
    assert_eq!(&definition.leg1[..3], b"BTC");
    assert!(definition.leg1[3..].iter().all(|&byte| byte == 0));
    assert_eq!(&definition.leg2[..3], b"USD");
    assert_eq!(definition.symbol.len(), SYMBOL_LEN);
    assert_eq!(definition.leg1.len(), LEG_LEN);

    // The exponents are the venue's declaration and are carried through
    // unchanged: applying them is this layer's job, stating them is the
    // venue's.
    assert_eq!(definition.price_exponent, -8);
    assert_eq!(definition.qty_exponent, -6);

    // The tables, against the codec's own constants and against the numbers
    // transcribed from the specification's tables by hand.
    assert_eq!(definition.asset_class, ASSET_CLASS_PERPETUAL_FUTURE);
    assert_eq!(definition.asset_class, 5);
    assert_eq!(definition.market_model, MARKET_MODEL_AMM);
    assert_eq!(definition.market_model, 2);
    assert_eq!(definition.settle_type, SETTLE_TYPE_CASH);
    assert_eq!(definition.settle_type, 1);
    assert_eq!(definition.price_bound, PRICE_BOUND_UNIT_INTERVAL);
    assert_eq!(definition.price_bound, 1);

    // `0.05` at a price exponent of -8 is 5_000_000. `0.001` at a quantity
    // exponent of -6 is 1_000. `2.5` is a value rather than a size, so it is
    // carried at the price exponent: 250_000_000.
    assert_eq!(definition.tick_size, 5_000_000);
    assert_eq!(definition.lot_size, 1_000);
    assert_eq!(definition.contract_value, 250_000_000);
    assert_eq!(definition.expiry_ns, 1_700_000_000_000_000_000);

    // Nothing was truncated and nothing was unrepresentable, so there is
    // nothing for the caller to report once per load.
    assert!(composed.fits.all_fitted());
    assert_eq!(composed.fits.symbol, Fit::Fitted);
    assert_eq!(composed.fits.leg1, Some(Fit::Fitted));

    // And what the lowering will convert every price and quantity against is
    // the same statement, not a second reading of it.
    assert_eq!(composed.instrument.instrument_id, 41);
    assert_eq!(composed.instrument.price_exponent, -8);
    assert_eq!(composed.instrument.qty_exponent, -6);
    assert_eq!(composed.instrument.quoted_per_contract, None);
}

#[test]
fn every_absent_value_is_the_sentinel_the_codec_names() {
    let composed = compose(&nothing_optional(), 1, source_id()).expect("exact");
    let definition = composed.definition;

    assert_eq!(definition.asset_class, ASSET_CLASS_UNKNOWN);
    assert_eq!(definition.market_model, MARKET_MODEL_UNKNOWN);
    assert_eq!(definition.settle_type, SETTLE_TYPE_NA);
    assert_eq!(definition.price_bound, PRICE_BOUND_UNBOUNDED);
    // All four of those are 0 on the wire, and each is asserted against its own
    // constant above rather than against that shared zero: they are four
    // separate tables that happen to agree about their first row, and a table
    // renumbered upstream must fail here rather than pass because zero is
    // still zero.

    // An absent leg is the NUL-padded field, and no fit is reported for a value
    // the venue never stated.
    assert_eq!(definition.leg1, [0u8; LEG_LEN]);
    assert_eq!(definition.leg2, [0u8; LEG_LEN]);
    assert_eq!(composed.fits.leg1, None);
    assert_eq!(composed.fits.leg2, None);

    // `Contract Value` and `Expiry NS` have no named constant, because each has
    // one sentinel and it is the field left at zero.
    assert_eq!(definition.contract_value, 0);
    assert_eq!(definition.expiry_ns, 0);
}

#[test]
fn a_symbol_too_long_for_the_field_is_published_and_reported() {
    // The specification permits truncation, and the codec asks that a publisher
    // report it once per reference-data load rather than silently or per
    // message. So it is admitted, and the count is what an operator sees.
    let long = "A".repeat(70);
    let mut spec = nothing_optional();
    spec.symbol = &long;

    let composed = compose(&spec, 1, source_id()).expect("exact");
    assert_eq!(composed.fits.symbol, Fit::Truncated);
    assert!(!composed.fits.all_fitted());
    assert_eq!(composed.definition.symbol, [b'A'; SYMBOL_LEN]);

    let mut registry = registry();
    assert!(registry.list(&spec).is_some());
    assert_eq!(registry.counts().imperfect_symbols, 1);
}

#[test]
fn a_contract_size_that_cannot_be_resolved_exactly_is_never_admitted() {
    // The refusal happens at admission and not per message, which is the whole
    // argument for the factor living above the venue boundary. An instrument
    // admitted with a contract size we cannot represent would appear in the
    // manifest, be counted in every dashboard, and refuse every message.
    let mut registry = registry();

    for stated in [
        // Not strictly positive: a zero makes every price division undefined
        // and every quantity zero.
        Scalar::text("0"),
        Scalar::text("-1"),
        Scalar::fixed(0, 0),
        // Past nine decimal places, which is where a contract size is parsed.
        Scalar::text("0.0000000001"),
        // Not a decimal at all.
        Scalar::text("one"),
    ] {
        let mut spec = nothing_optional();
        spec.quoted_per_contract = Some(stated);

        assert_eq!(
            compose(&spec, 1, source_id()).expect_err("not resolvable"),
            Refusal::ContractSize
        );
        assert!(registry.list(&spec).is_none());
    }

    assert_eq!(registry.published(), 0);
    assert_eq!(registry.instruments().len(), 0);
    assert_eq!(registry.counts().declined_unrepresentable, 5);
    assert!(!registry.last_refusal().expect("declined").is_ordinary());

    // No `Instrument ID` was consumed by any of them, so the next instrument
    // that does compose takes the first one.
    let admitted = registry.list(&nothing_optional()).expect("admitted");
    assert_eq!(
        registry
            .definition(admitted)
            .expect("published")
            .instrument_id,
        1
    );
}

#[test]
fn a_tick_size_the_contract_size_does_not_divide_is_never_admitted() {
    // A venue quoting per contract states its prices per contract, so the tick
    // size is divided by the contract factor exactly as every price is. A tick
    // of `1` on a contract of `3` of the underlying is not a grid we can state,
    // and publishing a definition whose tick size was rounded to one we can
    // would describe a grid none of the published prices sit on.
    let mut spec = nothing_optional();
    spec.quoted_per_contract = Some(Scalar::text("3"));
    spec.tick_size = Scalar::text("1");

    let refusal = compose(&spec, 1, source_id()).expect_err("3 does not divide 1");
    assert_eq!(
        refusal,
        Refusal::Field(LoweringError::InexactContract { field: "tick_size" })
    );
    // The three conversion failures stay apart, because each is a different
    // operator action: this one says the contract size does not divide what the
    // venue quoted, and counting it as malformed would send somebody to look at
    // the upstream's format instead.
    assert_eq!(
        match refusal {
            Refusal::Field(error) => error.reason(),
            other => panic!("{other:?}"),
        },
        "inexact_contract"
    );

    let mut registry = registry();
    assert!(registry.list(&spec).is_none());
    assert_eq!(registry.instruments().len(), 0);
}

#[test]
fn a_tick_size_too_precise_for_the_price_exponent_is_never_admitted() {
    // The exponent is wrong for this instrument, and an operator acts on that
    // differently from a malformed number. Refusing here rather than rounding
    // is the same rule the hot path follows: a value that cannot be stated
    // exactly is never nudged to the nearest one that can.
    let mut spec = nothing_optional();
    spec.price_exponent = -2;
    spec.tick_size = Scalar::text("0.001");

    let refusal = compose(&spec, 1, source_id()).expect_err("one digit too many");
    assert_eq!(
        match refusal {
            Refusal::Field(error) => error.reason(),
            other => panic!("{other:?}"),
        },
        "too_precise"
    );

    let mut registry = registry();
    assert!(registry.list(&spec).is_none());
    assert_eq!(registry.instruments().len(), 0);
    assert_eq!(registry.published(), 0);
}

#[test]
fn a_venue_quoting_per_contract_gets_a_tick_and_a_lot_on_the_wires_own_grid() {
    // The factor is applied in opposite directions - a price is per contract
    // and is divided, a quantity is in contracts and is multiplied - and the
    // published tick and lot go through the same conversion the hot path uses,
    // so the grid the definition declares is the grid the quotes arrive on.
    //
    // A contract of `0.0001` of the underlying, a tick of `0.05` per contract:
    // 0.05 / 0.0001 = 500 of the underlying, which at -8 is 50_000_000_000. A
    // lot of `1` contract is 1 x 0.0001 = 0.0001 of the underlying, which at
    // -8 is 10_000.
    let mut spec = nothing_optional();
    spec.price_exponent = -8;
    spec.qty_exponent = -8;
    spec.quoted_per_contract = Some(Scalar::text("0.0001"));
    spec.tick_size = Scalar::text("0.05");
    spec.lot_size = Scalar::text("1");

    let composed = compose(&spec, 1, source_id()).expect("both are exact");
    assert_eq!(composed.definition.tick_size, 50_000_000_000);
    assert_eq!(composed.definition.lot_size, 10_000);
    assert!(composed.instrument.quoted_per_contract.is_some());
}

#[test]
fn a_negative_contract_value_is_refused_rather_than_published_as_an_enormous_one() {
    // The wire field is unsigned. Taking the magnitude of a negative would
    // publish a number the venue never stated; letting it wrap would publish
    // one nobody could read.
    let mut spec = nothing_optional();
    spec.contract_value = Some(Scalar::text("-2.5"));

    let refusal = compose(&spec, 1, source_id()).expect_err("unsigned field");
    assert_eq!(
        match refusal {
            Refusal::Field(error) => (error.reason(), error.field()),
            other => panic!("{other:?}"),
        },
        ("malformed", Some("contract_value"))
    );
}

#[test]
fn a_restated_exponent_is_refused_and_the_published_definition_stands() {
    // The exponents and the contract factor are what the lowering converts
    // every price and quantity against, and they are also published in the
    // definition. Accepting a restatement without replacing both would leave
    // the definition declaring one scale while every quote went out at the
    // other - self-consistent on each side, and invisible to any test that
    // encodes and then decodes.
    let mut registry = registry();
    let handle = registry.list(&nothing_optional()).expect("admitted");
    let published = registry.definition(handle).expect("published");

    let mut restated = nothing_optional();
    restated.price_exponent = -4;
    restated.tick_size = Scalar::fixed(1, -4);

    assert_eq!(
        registry.list(&restated),
        Some(handle),
        "the instrument is still published, under the same handle"
    );
    assert_eq!(registry.definition(handle), Some(published));
    assert_eq!(registry.last_refusal(), Some(Refusal::ScaleRestated));
    assert_eq!(registry.counts().declined_unrepresentable, 1);
}

#[test]
fn a_restated_tick_size_is_published_and_advances_the_manifest() {
    // Everything the lowering does not hold can be restated: a venue that
    // changes a tick size, an expiry or a settlement type is describing the
    // same instrument, and the published set has changed.
    let mut registry = registry();
    let handle = registry.list(&nothing_optional()).expect("admitted");
    let before = registry.manifest_seq();

    let mut restated = nothing_optional();
    restated.tick_size = Scalar::fixed(5, 0);

    assert_eq!(registry.list(&restated), Some(handle));
    assert_eq!(registry.definition(handle).expect("published").tick_size, 5);
    assert_eq!(registry.manifest_seq(), before + 1);
    assert_eq!(registry.published(), 1);
}

#[test]
fn a_restated_contract_factor_is_refused_even_when_the_definition_does_not_change() {
    // The contract factor is the one of the three numbers the lowering holds
    // that the definition does not carry. A venue that halved its factor and
    // doubled the tick it quotes composes a byte-identical definition — the
    // published grid is the same — while every price and quantity for the
    // instrument would now be converted by a different number. There is
    // nowhere on the wire a subscriber could see that, which is why it is
    // caught before the definitions are compared rather than after.
    //
    // Both statements below are the same published grid: a tick of `0.02` per
    // contract on a contract of `2` of the underlying is 0.01, and so is `0.04`
    // on `4`. A lot of `1` contract of `2` is 2 of the underlying, and so is
    // `0.5` of `4`.
    let mut first = nothing_optional();
    first.price_exponent = -8;
    first.qty_exponent = -8;
    first.quoted_per_contract = Some(Scalar::text("2"));
    first.tick_size = Scalar::text("0.02");
    first.lot_size = Scalar::text("1");

    let mut second = first;
    second.quoted_per_contract = Some(Scalar::text("4"));
    second.tick_size = Scalar::text("0.04");
    second.lot_size = Scalar::text("0.5");

    let one = compose(&first, 1, source_id()).expect("exact");
    let other = compose(&second, 1, source_id()).expect("exact");
    assert_eq!(one.definition, other.definition, "the same published grid");
    assert_ne!(
        one.instrument, other.instrument,
        "and not the same conversion"
    );
    assert_eq!(one.definition.tick_size, 1_000_000);
    assert_eq!(one.definition.lot_size, 200_000_000);

    let mut registry = registry();
    let handle = registry.list(&first).expect("admitted");
    assert_eq!(registry.list(&second), Some(handle));
    assert_eq!(registry.last_refusal(), Some(Refusal::ScaleRestated));
    assert_eq!(
        registry.instruments().get(handle).expect("published"),
        &one.instrument,
        "the conversion the instrument was admitted with stands"
    );
}
