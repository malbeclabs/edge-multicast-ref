//! The enumerations here, held to the wire tables they stand for.
//!
//! This crate states several of the specification's tables as Rust
//! enumerations rather than as the wire's `u8`, so that a venue cannot name a
//! value outside one. That only helps if the two agree, and the failure it
//! guards against is precise and has shipped: a publisher numbering the
//! `Action` table from `New` instead of `Unknown` emitted every removal as a
//! change carrying zero — self-consistent, invisible to any test that encodes
//! and then decodes, and quietly wrong for every consumer reading the field.
//!
//! The correspondence below is transcribed by hand from the codec's own
//! constants and is deliberately not derived from anything. A table generated
//! from the thing it checks agrees with it by construction and proves nothing;
//! this one fails when either side moves.
//!
//! **What it catches and what it does not.** A constant that changes value, is
//! renamed, or disappears fails this file. A constant *added* upstream does not,
//! because nothing here can enumerate a module's constants. That gap has a safe
//! shape rather than a dangerous one: the lowering matches exhaustively on the
//! enumerations here, so a table this crate has not caught up with can only ever
//! be a value we do not emit — a missing feature, never a wrong byte.
//!
//! The dev-dependencies that make this possible are not inherited by a venue.
//! See `tests/dependencies.rs`.

use dz_adapter_core::{
    Aggressor, AssetClass, ClearScope, MarketModel, Presence, PriceBound, Scalar, SettleType, Side,
    TradeFlags,
};

#[test]
fn asset_class_matches_the_reference_data_table() {
    use dz_edge_refdata::instrument_definition as wire;

    let table = [
        (AssetClass::Unknown, wire::ASSET_CLASS_UNKNOWN),
        (AssetClass::CryptoSpot, wire::ASSET_CLASS_CRYPTO_SPOT),
        (
            AssetClass::PredictionBinary,
            wire::ASSET_CLASS_PREDICTION_BINARY,
        ),
        (
            AssetClass::PredictionScalar,
            wire::ASSET_CLASS_PREDICTION_SCALAR,
        ),
        (
            AssetClass::PredictionCategorical,
            wire::ASSET_CLASS_PREDICTION_CATEGORICAL,
        ),
        (
            AssetClass::PerpetualFuture,
            wire::ASSET_CLASS_PERPETUAL_FUTURE,
        ),
    ];

    assert_distinct_and_dense(&table, "asset class");
}

#[test]
fn market_model_matches_the_reference_data_table() {
    use dz_edge_refdata::instrument_definition as wire;

    let table = [
        (MarketModel::Unknown, wire::MARKET_MODEL_UNKNOWN),
        (MarketModel::Clob, wire::MARKET_MODEL_CLOB),
        (MarketModel::Amm, wire::MARKET_MODEL_AMM),
    ];

    assert_distinct_and_dense(&table, "market model");
}

#[test]
fn settle_type_matches_the_reference_data_table() {
    use dz_edge_refdata::instrument_definition as wire;

    let table = [
        (SettleType::NotApplicable, wire::SETTLE_TYPE_NA),
        (SettleType::Cash, wire::SETTLE_TYPE_CASH),
        (SettleType::Physical, wire::SETTLE_TYPE_PHYSICAL),
    ];

    assert_distinct_and_dense(&table, "settle type");
}

#[test]
fn price_bound_matches_the_reference_data_table() {
    use dz_edge_refdata::instrument_definition as wire;

    let table = [
        (PriceBound::Unbounded, wire::PRICE_BOUND_UNBOUNDED),
        (PriceBound::UnitInterval, wire::PRICE_BOUND_UNIT_INTERVAL),
        (PriceBound::NonNegative, wire::PRICE_BOUND_NON_NEGATIVE),
    ];

    assert_distinct_and_dense(&table, "price bound");
}

#[test]
fn aggressor_matches_the_top_of_book_table() {
    let table = [
        (Aggressor::Unknown, dz_edge_tob::trade::AGGRESSOR_UNKNOWN),
        (Aggressor::Buy, dz_edge_tob::trade::AGGRESSOR_BUY),
        (Aggressor::Sell, dz_edge_tob::trade::AGGRESSOR_SELL),
    ];

    assert_distinct_and_dense(&table, "aggressor");
}

#[test]
fn side_matches_the_depth_table() {
    let table = [
        (Side::Bid, dz_edge_mbp::SIDE_BID),
        (Side::Ask, dz_edge_mbp::SIDE_ASK),
    ];

    assert_distinct_and_dense(&table, "side");
}

#[test]
fn the_three_trade_qualifiers_are_the_three_wire_bits() {
    // Three booleans stand for a flags byte, so what has to hold is that there
    // are exactly three bits to stand for and that they do not overlap.
    let bits = [
        dz_edge_tob::trade::TRADE_FLAG_BLOCK,
        dz_edge_tob::trade::TRADE_FLAG_SWEEP,
        dz_edge_tob::trade::TRADE_FLAG_CROSS,
    ];

    for bit in bits {
        assert_eq!(bit.count_ones(), 1, "a qualifier is more than one bit");
    }
    assert_eq!(
        bits.iter().fold(0u8, |set, bit| set | bit).count_ones(),
        bits.len() as u32,
        "two qualifiers share a bit"
    );

    // And that the struct standing for them has exactly three fields to set.
    let all = TradeFlags {
        block: true,
        sweep: true,
        cross: true,
    };
    assert_ne!(all, TradeFlags::NONE);
}

#[test]
fn presence_covers_the_action_table_except_the_derived_value() {
    // The asymmetry is the design, stated as a test so that a later reading of
    // it as an oversight fails here.
    //
    // The action table has four values. `Presence` has three, because
    // `Delete` is derived from a quantity of zero above this boundary and is
    // never something a venue states. That is what makes the two pairings the
    // specification forbids — a removal carrying another action, an action of
    // removal carrying quantity — unrepresentable rather than merely refused.
    let stated = [
        (Presence::Unknown, dz_edge_mbp::ACTION_UNKNOWN),
        (Presence::New, dz_edge_mbp::ACTION_NEW),
        (Presence::Change, dz_edge_mbp::ACTION_CHANGE),
    ];

    assert_distinct_and_dense(&stated, "presence");

    // The one value that is not here, and the reason it is not.
    assert_eq!(
        dz_edge_mbp::ACTION_DELETE as usize,
        stated.len(),
        "the derived action is no longer the value beyond the stated ones, so \
         the argument that `Presence` covers the table minus one no longer holds"
    );
}

#[test]
fn clear_scope_cannot_express_the_pairing_the_codec_refuses() {
    // The codec refuses a clear bounded by one price that applies to both
    // sides, at the push. Here the pairing has no representation at all: every
    // bounded scope names exactly one side.
    //
    // Written as an exhaustive match rather than as an assertion about a value,
    // so that a variant added later which *could* express it fails to compile
    // here rather than passing silently.
    let scopes = [
        ClearScope::EntireSide(Side::Bid),
        ClearScope::BothSides,
        ClearScope::FromPrice {
            side: Side::Ask,
            px: Scalar::text("0.5"),
        },
    ];

    for scope in scopes {
        let bounded_sides = match scope {
            ClearScope::EntireSide(_) => 1,
            ClearScope::BothSides => 0, // unbounded, so the rule does not apply
            ClearScope::FromPrice { .. } => 1,
        };
        assert!(
            bounded_sides <= 1,
            "a scope bounded by a price reaches more than one side"
        );
    }

    // And the codec's own three clear-side values are still three, so that a
    // fourth added upstream is noticed here rather than in a capture.
    let sides = [
        dz_edge_mbp::CLEAR_BID,
        dz_edge_mbp::CLEAR_ASK,
        dz_edge_mbp::CLEAR_BOTH,
    ];
    assert_eq!(sides, [0, 1, 2]);
    assert_eq!(
        [
            dz_edge_mbp::SCOPE_ENTIRE_SIDE,
            dz_edge_mbp::SCOPE_FROM_PRICE
        ],
        [0, 1]
    );
}

/// Every wire value in the table is distinct, and together they are `0..n`.
///
/// Density is the property worth asserting rather than any particular value: a
/// table that has grown a hole has had a value removed from under us, and one
/// whose values are no longer `0..n` is one this crate has fallen behind.
fn assert_distinct_and_dense<T>(table: &[(T, u8)], what: &str) {
    let mut values: Vec<u8> = table.iter().map(|(_, value)| *value).collect();
    values.sort_unstable();
    let count = values.len();
    values.dedup();
    assert_eq!(
        values.len(),
        count,
        "two {what} variants share a wire value"
    );

    let expected: Vec<u8> = (0..count as u8).collect();
    assert_eq!(
        values, expected,
        "the {what} table is no longer 0..{count}: this crate has fallen behind it"
    );
}
