//! `0x04 Trade`: one lowering, and the specification's sentinels for what a
//! venue does not publish.

use dz_adapter_core::{Aggressor, InstrumentRef, Scalar, TradeFlags};
use dz_edge_tob::{
    AGGRESSOR_BUY, AGGRESSOR_SELL, AGGRESSOR_UNKNOWN, TRADE_FLAG_BLOCK, TRADE_FLAG_CROSS,
    TRADE_FLAG_SWEEP,
};
use dz_publisher_lowering::{Instrument, InstrumentTable, Lowering};

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });
    instruments
}

#[test]
fn a_trade_lowers_its_price_quantity_and_identity() {
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let trade = lowering
        .lower_trade(
            &instruments,
            InstrumentRef::from_admission(0),
            1_700_000_000_000_000_000,
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Buy,
            Some(0xDEAD_BEEF),
            Some(Scalar::text("120")),
            TradeFlags::NONE,
        )
        .expect("lowers");

    assert_eq!(trade.instrument_id, 41);
    assert_eq!(trade.source_id, 7);
    assert_eq!(trade.source_timestamp_ns, 1_700_000_000_000_000_000);
    assert_eq!(trade.trade_price, 4_100);
    assert_eq!(trade.trade_qty, 500);
    assert_eq!(trade.trade_id, 0xDEAD_BEEF);
    assert_eq!(trade.cumulative_volume, 12_000);
    assert_eq!(trade.aggressor_side, AGGRESSOR_BUY);
    assert_eq!(trade.trade_flags, 0);
}

#[test]
fn what_a_venue_does_not_publish_reaches_the_wire_as_the_specifications_sentinel() {
    // Both existing publishers are in exactly this state: neither exposes a
    // running total on its trade events and neither sets a qualifier bit, and
    // one of them has no venue trade identifier to pass through either. Each
    // absence has a defined value, and none of them is a guess.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let trade = lowering
        .lower_trade(
            &instruments,
            InstrumentRef::from_admission(0),
            1,
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Unknown,
            None,
            None,
            TradeFlags::NONE,
        )
        .expect("lowers");

    assert_eq!(trade.trade_id, 0, "no venue identifier");
    assert_eq!(trade.cumulative_volume, 0, "no running total");
    assert_eq!(
        trade.aggressor_side, AGGRESSOR_UNKNOWN,
        "an unstated aggressor is the defined unknown value, never a guess"
    );
    assert_eq!(trade.trade_flags, 0, "no qualifier");
}

#[test]
fn each_aggressor_reaches_its_own_byte() {
    let instruments = table();
    let lowering = Lowering::new(source_id());

    for (aggressor, expected) in [
        (Aggressor::Unknown, AGGRESSOR_UNKNOWN),
        (Aggressor::Buy, AGGRESSOR_BUY),
        (Aggressor::Sell, AGGRESSOR_SELL),
    ] {
        let trade = lowering
            .lower_trade(
                &instruments,
                InstrumentRef::from_admission(0),
                1,
                Scalar::text("0.41"),
                Scalar::text("5"),
                aggressor,
                None,
                None,
                TradeFlags::NONE,
            )
            .expect("lowers");
        assert_eq!(trade.aggressor_side, expected, "{aggressor:?}");
    }
}

#[test]
fn each_qualifier_sets_its_own_bit_and_nothing_else() {
    // Three booleans in, three defined bits out. A bit nobody defined has no
    // way to be set because there is no fourth boolean to set it with.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let cases = [
        (
            TradeFlags {
                block: true,
                ..TradeFlags::NONE
            },
            TRADE_FLAG_BLOCK,
        ),
        (
            TradeFlags {
                sweep: true,
                ..TradeFlags::NONE
            },
            TRADE_FLAG_SWEEP,
        ),
        (
            TradeFlags {
                cross: true,
                ..TradeFlags::NONE
            },
            TRADE_FLAG_CROSS,
        ),
        (
            TradeFlags {
                block: true,
                sweep: true,
                cross: true,
            },
            TRADE_FLAG_BLOCK | TRADE_FLAG_SWEEP | TRADE_FLAG_CROSS,
        ),
    ];

    for (flags, expected) in cases {
        let trade = lowering
            .lower_trade(
                &instruments,
                InstrumentRef::from_admission(0),
                1,
                Scalar::text("0.41"),
                Scalar::text("5"),
                Aggressor::Buy,
                None,
                None,
                flags,
            )
            .expect("lowers");
        assert_eq!(trade.trade_flags, expected, "{flags:?}");
    }
}

#[test]
fn a_cumulative_volume_too_precise_for_the_exponent_names_its_own_field() {
    // The field name is what sends an operator to the right place, and a
    // running total is scaled at the quantity exponent like any other quantity.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let error = lowering
        .lower_trade(
            &instruments,
            InstrumentRef::from_admission(0),
            1,
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Buy,
            None,
            Some(Scalar::text("120.001")),
            TradeFlags::NONE,
        )
        .expect_err("three decimal places at an exponent of -2");

    assert_eq!(error.field(), Some("cumulative_volume"));
    assert_eq!(error.reason(), "too_precise");
}

/// An assigned production id, which is what a publisher runs under. Zero would
/// be refused by the type - see `tests/source_id.rs`.
fn source_id() -> dz_publisher_lowering::SourceId {
    dz_publisher_lowering::SourceId::new(7).expect("7 is in the assigned range")
}
