//! `0x04` across two feeds, which is the whole of plan task 5.
//!
//! The wire's cross-specification policy: a Type ID appearing in more than one
//! sibling feed must carry the same meaning in each, and `Trade` is
//! **byte-for-byte identical** between the top-of-book feed, the
//! market-by-price feed and the market-by-order feed. A venue publishing two of
//! them owes the same bytes on both for the same execution.
//!
//! In one existing publisher that obligation is a doc comment across two
//! encoder implementations, held to each other by hand. Nothing checks it, and
//! nothing could: each encoder round-trips against itself.
//!
//! Here it is structural — both channels call one function — and this file is
//! what keeps it structural. A future change that gave either channel its own
//! trade encoder would fail here rather than drift.

use dz_adapter_core::{Aggressor, InstrumentRef, Scalar, TradeFlags};
use dz_edge_core::AppMessage;
use dz_edge_tob::Trade;
use dz_publisher_lowering::{
    ContractSize, DepthLowering, Instrument, InstrumentTable, Lowering, SourceId,
};

fn source_id() -> SourceId {
    SourceId::new(7).expect("7 is an assigned production id")
}

fn encoded(trade: &Trade) -> [u8; Trade::SIZE] {
    let mut bytes = [0u8; Trade::SIZE];
    trade.encode_into(&mut bytes);
    bytes
}

/// One trade a venue could state: exactly the fields of `Event::Trade` that
/// differ between the cases, named so the table below reads as a table.
struct Case {
    state: &'static str,
    px: Scalar<'static>,
    qty: Scalar<'static>,
    aggressor: Aggressor,
    trade_id: Option<u64>,
    volume: Option<Scalar<'static>>,
    flags: TradeFlags,
}

/// Every trade a venue could state, including the shapes that exercise each
/// sentinel and each qualifier bit.
fn cases() -> Vec<Case> {
    let case = |state, px, qty, aggressor, trade_id, volume, flags| Case {
        state,
        px,
        qty,
        aggressor,
        trade_id,
        volume,
        flags,
    };
    vec![
        case(
            "everything stated",
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Buy,
            Some(0xDEAD_BEEF),
            Some(Scalar::text("120")),
            TradeFlags {
                block: true,
                sweep: true,
                cross: true,
            },
        ),
        case(
            "every sentinel",
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Unknown,
            None,
            None,
            TradeFlags::NONE,
        ),
        case(
            "the venue's own integers",
            Scalar::fixed(4_100, -4),
            Scalar::fixed(500, -2),
            Aggressor::Sell,
            Some(1),
            Some(Scalar::fixed(12_000, -2)),
            TradeFlags {
                sweep: true,
                ..TradeFlags::NONE
            },
        ),
        case(
            "a negative price, which some venues quote",
            Scalar::text("-0.41"),
            Scalar::text("5"),
            Aggressor::Buy,
            None,
            None,
            TradeFlags::NONE,
        ),
    ]
}

#[test]
fn one_trade_lowers_to_the_same_bytes_on_a_top_of_book_and_a_depth_channel() {
    let mut instruments = InstrumentTable::new();
    let instrument = instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });

    let tob = Lowering::new(source_id());
    let depth = DepthLowering::new(source_id());

    for Case {
        state,
        px,
        qty,
        aggressor,
        trade_id,
        volume,
        flags,
    } in cases()
    {
        let from_tob = tob
            .lower_trade(
                &instruments,
                instrument,
                1_700,
                px,
                qty,
                aggressor,
                trade_id,
                volume,
                flags,
            )
            .expect("lowers");
        let from_depth = depth
            .lower_trade(
                &instruments,
                instrument,
                1_700,
                px,
                qty,
                aggressor,
                trade_id,
                volume,
                flags,
            )
            .expect("lowers");

        assert_eq!(
            encoded(&from_tob),
            encoded(&from_depth),
            "{state}: the two channels disagree on the wire for one execution"
        );
    }
}

#[test]
fn the_two_channels_agree_for_a_venue_that_quotes_a_contract_too() {
    // The conversion is the newest thing in the path and the most likely place
    // for two callers to diverge, so it gets its own pass rather than riding on
    // the case table above.
    let mut instruments = InstrumentTable::new();
    let instrument = instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -8,
        qty_exponent: -8,
        quoted_per_contract: Some(
            ContractSize::from_scalar(Scalar::text("0.000100")).expect("exact"),
        ),
    });

    let tob = Lowering::new(source_id());
    let depth = DepthLowering::new(source_id());

    let args = (
        Scalar::fixed(631_910_000, -8),
        Scalar::fixed(100, -2),
        Aggressor::Sell,
        None,
        None,
        TradeFlags::NONE,
    );
    let from_tob = tob
        .lower_trade(
            &instruments,
            instrument,
            1_700,
            args.0,
            args.1,
            args.2,
            args.3,
            args.4,
            args.5,
        )
        .expect("lowers");
    let from_depth = depth
        .lower_trade(
            &instruments,
            instrument,
            1_700,
            args.0,
            args.1,
            args.2,
            args.3,
            args.4,
            args.5,
        )
        .expect("lowers");

    assert_eq!(encoded(&from_tob), encoded(&from_depth));
    assert_eq!(from_tob.trade_price, 6_319_100_000_000);
    assert_eq!(from_tob.trade_qty, 10_000);
}

#[test]
fn the_two_channels_refuse_the_same_trade_for_the_same_reason() {
    // Agreement on what is publishable is half of "one implementation": two
    // encoders that produced the same bytes and disagreed about which trades
    // they would produce them for would still be two encoders.
    let mut instruments = InstrumentTable::new();
    let instrument = instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });

    let tob = Lowering::new(source_id());
    let depth = DepthLowering::new(source_id());
    let forged = InstrumentRef::from_admission(9_999);

    for (state, handle, px) in [
        ("an unheld handle", forged, Scalar::text("0.41")),
        (
            "a price too precise for the exponent",
            instrument,
            Scalar::text("0.41005"),
        ),
        (
            "a price that is not a decimal",
            instrument,
            Scalar::text("not a number"),
        ),
    ] {
        let a = tob
            .lower_trade(
                &instruments,
                handle,
                1_700,
                px,
                Scalar::text("5"),
                Aggressor::Buy,
                None,
                None,
                TradeFlags::NONE,
            )
            .expect_err(state);
        let b = depth
            .lower_trade(
                &instruments,
                handle,
                1_700,
                px,
                Scalar::text("5"),
                Aggressor::Buy,
                None,
                None,
                TradeFlags::NONE,
            )
            .expect_err(state);
        assert_eq!(a, b, "{state}: the two channels refuse differently");
    }
}

#[test]
fn a_trade_spends_no_per_instrument_sequence_number() {
    // The depth channel's other two messages take a number from a series a
    // subscriber reads for loss. A trade does not mutate the book, so it is not
    // in that series - and spending a number on one would put a gap in it.
    use dz_adapter_core::ClearScope;

    let mut instruments = InstrumentTable::new();
    let instrument = instruments.admit(Instrument {
        instrument_id: 41,
        price_exponent: -4,
        qty_exponent: -2,
        quoted_per_contract: None,
    });
    let mut depth = DepthLowering::new(source_id());

    let first = depth
        .lower_clear(&instruments, instrument, 1, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(first.per_instrument_seq, 1);

    depth
        .lower_trade(
            &instruments,
            instrument,
            2,
            Scalar::text("0.41"),
            Scalar::text("5"),
            Aggressor::Buy,
            None,
            None,
            TradeFlags::NONE,
        )
        .expect("lowers");

    let second = depth
        .lower_clear(&instruments, instrument, 3, ClearScope::BothSides)
        .expect("lowers");
    assert_eq!(
        second.per_instrument_seq, 2,
        "the trade must not have taken a number"
    );
}
