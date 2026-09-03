//! The lowering against the cross-language golden vectors.
//!
//! `testdata/golden/` is the specification's meaning made concrete: every
//! implementation in every language must reproduce those bytes. Until now they
//! bound the *codec* — a hand-written wire struct in, the canonical bytes out.
//! This file binds the **interface**: a normalized event as a venue states it,
//! plus the instrument's exponents, in — and the same canonical bytes out.
//!
//! That is a stronger statement than a vector of its own would be. A vector
//! this crate generated would say the lowering agrees with itself; reproducing
//! the vector another language already reproduces says the lowering agrees with
//! the wire.
//!
//! Both directions are asserted for each message: the lowering encodes to the
//! committed bytes, and the codec's own decoder reads those bytes back to the
//! same values. A change here is a wire change and must be justified against
//! the specification, never adjusted to match code that started failing.

use std::path::PathBuf;

use dz_adapter_core::{Aggressor, Scalar, SideUpdate, TradeFlags};
use dz_edge_core::AppMessage;
use dz_edge_tob::{Quote, Trade};
use dz_publisher_lowering::{Instrument, InstrumentTable, Lowering, SourceId};

/// The vectors' `Source ID`. `2` in every one of them, and an assigned
/// production id, so the checked type admits it.
const SOURCE_ID: u16 = 2;

/// The exponents that make the vectors' raw integers the decimals a venue would
/// have quoted. Transcribed by reading the vector, not by running the code:
/// `bid_price = 9_999_500` at `-4` is `999.95`, and `bid_qty = 12_500` at `-2`
/// is `125.00`.
const PRICE_EXPONENT: i8 = -4;
const QTY_EXPONENT: i8 = -2;

fn golden(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../testdata/golden")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn table() -> InstrumentTable {
    let mut instruments = InstrumentTable::new();
    instruments.admit(Instrument {
        // The vectors' `Instrument ID`.
        instrument_id: 1,
        price_exponent: PRICE_EXPONENT,
        qty_exponent: QTY_EXPONENT,
        quoted_per_contract: None,
    });
    instruments
}

fn source_id() -> SourceId {
    SourceId::new(SOURCE_ID).expect("2 is an assigned production id")
}

#[test]
fn a_normalized_quote_lowers_to_the_canonical_quote_vector() {
    // The vector's `update_flags` is `0x03` — both sides updated — so both
    // sides of the event are present. The two source counts are `3` and `4`,
    // which is a venue that states them; the asymmetry is deliberate in the
    // vector so a transposed pair cannot pass, and it survives the lowering.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let quote = lowering
        .lower_quote(
            &instruments,
            dz_adapter_core::InstrumentRef::from_admission(0),
            1_700_000_000_000_000_000,
            SideUpdate::Present {
                px: Scalar::text("999.95"),
                qty: Scalar::text("125.00"),
                source_count: Some(3),
            },
            SideUpdate::Present {
                px: Scalar::text("1000.05"),
                qty: Scalar::text("72.50"),
                source_count: Some(4),
            },
        )
        .expect("the vector's values are exact at these exponents");

    let mut bytes = [0u8; Quote::SIZE];
    quote.encode_into(&mut bytes);
    assert_eq!(
        bytes.as_slice(),
        golden("quote-v3.bin").as_slice(),
        "the lowering no longer reproduces the cross-language vector"
    );

    // And the other direction: the codec reads those bytes back to what the
    // lowering produced, so the vector binds both halves rather than one.
    assert_eq!(
        Quote::decode(&bytes).expect("the canonical bytes decode"),
        quote
    );
}

#[test]
fn a_normalized_trade_lowers_to_the_canonical_trade_vector() {
    // `aggressor_side = 1` is the buy value and `trade_flags = 2` is the sweep
    // bit alone, which is the one qualifier this vector sets.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let trade = lowering
        .lower_trade(
            &instruments,
            dz_adapter_core::InstrumentRef::from_admission(0),
            1_700_000_000_000_000_001,
            Scalar::text("1000.00"),
            Scalar::text("5.00"),
            Aggressor::Buy,
            Some(987_654_321),
            Some(Scalar::text("10000.00")),
            TradeFlags {
                sweep: true,
                ..TradeFlags::NONE
            },
        )
        .expect("the vector's values are exact at these exponents");

    let mut bytes = [0u8; Trade::SIZE];
    trade.encode_into(&mut bytes);
    assert_eq!(
        bytes.as_slice(),
        golden("trade-v3.bin").as_slice(),
        "the lowering no longer reproduces the cross-language vector"
    );
    assert_eq!(
        Trade::decode(&bytes).expect("the canonical bytes decode"),
        trade
    );
}

#[test]
fn the_venues_own_integers_reach_the_canonical_bytes_too() {
    // The same two vectors from the other `Scalar` shape, because a venue whose
    // book already holds integers must reach the wire the text path reaches -
    // that equality is what lets it keep its integers instead of rendering them
    // to decimal for this interface to re-parse.
    let instruments = table();
    let lowering = Lowering::new(source_id());

    let quote = lowering
        .lower_quote(
            &instruments,
            dz_adapter_core::InstrumentRef::from_admission(0),
            1_700_000_000_000_000_000,
            SideUpdate::Present {
                px: Scalar::fixed(9_999_500, PRICE_EXPONENT),
                qty: Scalar::fixed(12_500, QTY_EXPONENT),
                source_count: Some(3),
            },
            SideUpdate::Present {
                px: Scalar::fixed(10_000_500, PRICE_EXPONENT),
                qty: Scalar::fixed(7_250, QTY_EXPONENT),
                source_count: Some(4),
            },
        )
        .expect("integers at the instrument's own exponent need no rescale");

    let mut bytes = [0u8; Quote::SIZE];
    quote.encode_into(&mut bytes);
    assert_eq!(bytes.as_slice(), golden("quote-v3.bin").as_slice());
}

// ---------------------------------------------------------------------------
// The depth messages, which need vectors of their own
// ---------------------------------------------------------------------------
//
// The codec's `level-update-v3.bin`, `book-clear-v3.bin` and snapshot vectors
// deliberately set every field to a distinct value so a transposed pair cannot
// pass — including three the adapter boundary cannot state at all: a level's
// `Level Index` (a rank in the publisher's own book at emission, not a property
// of the venue's event), an `Update Reason`, and a `Clear Reason`. So the
// lowering cannot reproduce them, and reproducing them would mean inventing a
// way for a venue to state those fields.
//
// These vectors therefore start where the interface starts: a normalized event
// and the instrument's exponents. `testdata/golden/manifest.json` records the
// event beside the bytes, so another language can reproduce them the way it
// reproduces the codec's.

use dz_adapter_core::{ClearScope, InstrumentRef, Presence, Side, SnapshotSink};
use dz_edge_mbp::{BookClear, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel};
use dz_publisher_lowering::DepthLowering;

/// The depth vectors' anchor and framing values, which are the runtime's rather
/// than the venue's and so are stated here as the runtime would.
const ANCHOR_SEQ: u64 = 918_273_645;
const SNAPSHOT_TS: u64 = 1_700_000_000_000_000_005;
const DEPTH_BOUND: u32 = 50;

/// The one level update these vectors carry, from the event a venue states.
fn lowered_level(instruments: &InstrumentTable, depth: &mut DepthLowering) -> LevelUpdate {
    depth
        .lower_level(
            instruments,
            InstrumentRef::from_admission(0),
            1_700_000_000_000_000_003,
            Side::Ask,
            Scalar::text("1000.05"),
            Scalar::text("72.50"),
            Some(5),
            Presence::New,
        )
        .expect("exact at these exponents")
}

/// The one clear, bounded by a price on the ask — the shape that exercises both
/// the side byte and the bound.
fn lowered_clear(instruments: &InstrumentTable, depth: &mut DepthLowering) -> BookClear {
    depth
        .lower_clear(
            instruments,
            InstrumentRef::from_admission(0),
            1_700_000_000_000_000_004,
            ClearScope::FromPrice {
                side: Side::Ask,
                px: Scalar::text("1000.05"),
            },
        )
        .expect("exact at these exponents")
}

/// The snapshot these vectors carry: two levels, one a side.
fn lowered_snapshot(
    instruments: &InstrumentTable,
    depth: &mut DepthLowering,
) -> (SnapshotBegin, Vec<SnapshotLevel>, SnapshotEnd) {
    let mut framer = depth
        .open_snapshot(
            instruments,
            InstrumentRef::from_admission(0),
            ANCHOR_SEQ,
            SNAPSHOT_TS,
            DEPTH_BOUND,
        )
        .expect("held");
    framer.level(
        Side::Bid,
        Scalar::text("999.95"),
        Scalar::text("125.00"),
        Some(3),
    );
    framer.level(
        Side::Ask,
        Scalar::text("1000.05"),
        Scalar::text("72.50"),
        None,
    );
    let snapshot = framer.finish().expect("both levels are exact");
    (snapshot.begin, snapshot.levels, snapshot.end)
}

/// Write the depth vectors. Ignored, like the codec's own generator: a run of
/// this is a wire change and has to be a deliberate act, reviewed against the
/// specification rather than triggered by a test that started failing.
#[test]
#[ignore = "regenerating a golden vector is a wire change, not a test fixup"]
fn generate_depth_vectors() {
    let instruments = table();

    let write = |name: &str, bytes: &[u8]| {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../../testdata/golden")
            .join(name);
        std::fs::write(&path, bytes).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
    };

    // Each group gets its own lowering, driven exactly as the asserting test
    // drives it. A shared one would leave the snapshot's `Last Instrument Seq`
    // carrying the deltas generated before it, and the vector would then only
    // be reproducible by replaying this generator's whole order.
    let mut depth = DepthLowering::new(source_id());
    let level = lowered_level(&instruments, &mut depth);
    let mut buf = [0u8; LevelUpdate::SIZE];
    level.encode_into(&mut buf);
    write("level-update-from-event-v3.bin", &buf);

    let clear = lowered_clear(&instruments, &mut depth);
    let mut buf = [0u8; BookClear::SIZE];
    clear.encode_into(&mut buf);
    write("book-clear-from-event-v3.bin", &buf);

    let mut depth = DepthLowering::new(source_id());
    let (begin, levels, end) = lowered_snapshot(&instruments, &mut depth);
    let mut buf = [0u8; SnapshotBegin::SIZE];
    begin.encode_into(&mut buf);
    write("snapshot-begin-from-event-v3.bin", &buf);
    let mut buf = [0u8; SnapshotLevel::SIZE];
    levels[0].encode_into(&mut buf);
    write("snapshot-level-from-event-v3.bin", &buf);
    let mut buf = [0u8; SnapshotEnd::SIZE];
    end.encode_into(&mut buf);
    write("snapshot-end-from-event-v3.bin", &buf);
}

#[test]
fn a_normalized_level_and_clear_lower_to_their_committed_vectors() {
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());

    let level = lowered_level(&instruments, &mut depth);
    let mut buf = [0u8; LevelUpdate::SIZE];
    level.encode_into(&mut buf);
    assert_eq!(
        buf.as_slice(),
        golden("level-update-from-event-v3.bin").as_slice()
    );
    assert_eq!(LevelUpdate::decode(&buf).expect("decodes"), level);

    // The three fields the boundary cannot state, at the values the
    // specification defines for absent — asserted so a later change that
    // invented a way to state one shows up here.
    assert_eq!(level.level_index, dz_edge_mbp::U16_UNAVAILABLE);
    assert_eq!(level.update_reason, 0);
    assert_eq!(level.level_flags, 0);
    // And the ones it does state.
    assert_eq!(level.action, dz_edge_mbp::ACTION_NEW);
    assert_eq!(level.order_count, 5);
    assert_eq!(level.per_instrument_seq, 1, "the first delta of an era");

    let clear = lowered_clear(&instruments, &mut depth);
    let mut buf = [0u8; BookClear::SIZE];
    clear.encode_into(&mut buf);
    assert_eq!(
        buf.as_slice(),
        golden("book-clear-from-event-v3.bin").as_slice()
    );
    assert_eq!(BookClear::decode(&buf).expect("decodes"), clear);
    assert_eq!(
        clear.per_instrument_seq, 2,
        "a clear takes the next number in the same series"
    );
    assert_eq!(clear.clear_reason, 0, "the boundary states no reason");
}

#[test]
fn a_pulled_snapshot_lowers_to_its_committed_vectors() {
    let instruments = table();
    let mut depth = DepthLowering::new(source_id());
    let (begin, levels, end) = lowered_snapshot(&instruments, &mut depth);

    let mut buf = [0u8; SnapshotBegin::SIZE];
    begin.encode_into(&mut buf);
    assert_eq!(
        buf.as_slice(),
        golden("snapshot-begin-from-event-v3.bin").as_slice()
    );
    assert_eq!(SnapshotBegin::decode(&buf).expect("decodes"), begin);
    assert_eq!(begin.total_levels, 2, "what the framer was actually given");
    assert_eq!(
        begin.last_instrument_seq, 0,
        "no delta has been sent in this era"
    );

    let mut buf = [0u8; SnapshotLevel::SIZE];
    levels[0].encode_into(&mut buf);
    assert_eq!(
        buf.as_slice(),
        golden("snapshot-level-from-event-v3.bin").as_slice()
    );
    assert_eq!(SnapshotLevel::decode(&buf).expect("decodes"), levels[0]);
    assert_eq!(
        levels[1].order_count,
        dz_edge_mbp::U16_UNAVAILABLE,
        "the side the venue did not count"
    );

    let mut buf = [0u8; SnapshotEnd::SIZE];
    end.encode_into(&mut buf);
    assert_eq!(
        buf.as_slice(),
        golden("snapshot-end-from-event-v3.bin").as_slice()
    );
    assert_eq!(SnapshotEnd::decode(&buf).expect("decodes"), end);
}
