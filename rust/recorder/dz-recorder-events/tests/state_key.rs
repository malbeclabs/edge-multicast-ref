//! `state_key`, and the three ways a race key fails.
//!
//! The key is what says two observation points saw the same top of book, and it
//! is compared across the things that differ between them: a publisher that
//! upgraded its schema, a publisher that packed its messages differently, and
//! two paths that share no sequence space, no address and no clock. Every one of
//! those moves the bytes; none of them moves the state.
//!
//! **A key that is a function of anything but the state fails quietly.** The
//! query still runs, the rows are all there, and the pairing simply finds
//! nothing — which is indistinguishable from a feed nobody was racing. So the
//! three failures the specification names each get a test that decodes one state
//! two ways and asserts the key did not move.
//!
//! The fourth test is the tuple itself: every field in it moves the key, which
//! is the assertion that a field cannot be quietly dropped from the hash. A
//! dropped field is the same failure inverted — two *different* states wearing
//! one key, paired against each other and reported as a lead time.
#![forbid(unsafe_code)]

mod common;

use common::{
    definition, identity, pack, pack_batched, pack_from, DatagramLog, Msg, AAA, CHANNEL_ID,
    SOURCE_ID,
};
use dz_edge_core::{PortRole, SCHEMA_VERSION, SCHEMA_VERSION_V1};
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_events::{derive_events, state_key, EventInput, Side, Top};
use dz_recorder_replay::synthetic::SECOND_SOURCE;
use dz_recorder_replay::OwnedDatagram;

/// The prices the fixture states, in order, one of them stated twice.
///
/// A repeated state is the case the whole equivalence key exists for, so it is
/// in the fixture rather than in one test: a key that varied with the bytes
/// would give the two identical tops two keys, and nothing downstream could
/// tell that the book had returned to a state it had been in.
const PRICES: [(i64, i64); 4] = [
    (9_950, 10_050),
    (9_960, 10_050),
    (9_950, 10_050),
    (9_940, 10_070),
];

fn quote(bid: i64, ask: i64) -> Msg {
    Msg::Quote(Quote {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        update_flags: 0x03,
        source_timestamp_ns: 1_000_000_001,
        bid_price: bid,
        bid_qty: 12,
        ask_price: ask,
        ask_qty: 7,
        bid_source_count: 2,
        ask_source_count: 3,
    })
}

fn quotes() -> Vec<Msg> {
    PRICES.iter().map(|(bid, ask)| quote(*bid, *ask)).collect()
}

fn defs() -> Vec<Msg> {
    vec![Msg::Definition(definition(AAA, "AAA", -2))]
}

/// The keys of one archive's `book_top` rows, in the order the rows were
/// derived, under a stated observation point.
fn keys_at(datagrams: Vec<OwnedDatagram>, observation: &str) -> Vec<u64> {
    let mut log = DatagramLog::new(datagrams);
    let id = identity();
    let derived = derive_events(
        &mut log,
        &EventInput {
            identity: &id,
            feed: "feed",
            object_key: "object",
            object_sha256: "sha",
            segment_seq: 3,
            magic: MAGIC_TOB,
            observation,
            persist_snapshot_levels: false,
        },
    )
    .expect("the log does not fail");
    assert_eq!(
        derived.book_top.len(),
        PRICES.len(),
        "every quote states a complete two-sided top, so every one is a row"
    );
    derived.book_top.iter().map(|row| row.state_key).collect()
}

fn keys(datagrams: Vec<OwnedDatagram>) -> Vec<u64> {
    keys_at(datagrams, "observation")
}

/// One archive of the fixture, with the market data stamped at a stated schema
/// generation.
///
/// The reference data is left at the generation the encoder emits, and that is
/// not a shortcut: `InstrumentDefinition` is the only message in the family
/// whose layout changed between generations — `Symbol` widened and `Source ID`
/// was added at the 3.0.0 cut — and the crate is explicit that v1 is decode-only
/// and nothing emits it. Every price-level message has one layout across both
/// generations, so a publisher speaking the older one puts the same bytes on the
/// wire behind a different header, which is exactly the thing under test.
fn archive_at(schema_version: u8) -> Vec<OwnedDatagram> {
    let mut out = pack::<TopOfBook>(&defs(), PortRole::Refdata, 1);
    let mut market = pack::<TopOfBook>(&quotes(), PortRole::Mktdata, 100);
    for datagram in &mut market {
        datagram.payload[2] = schema_version;
    }
    out.extend(market);
    out
}

/// A publisher upgrade must not repartition the key space.
///
/// This is the failure a payload hash produces and the reason the key is a
/// function of the decoded state rather than of the bytes: a key over the wire
/// bytes is a key over the schema version, the batching and any padding, so the
/// day a publisher upgrades, every state hashes a new way and the race reports
/// nothing at all — with no error anywhere, because both sides are still
/// loading rows.
#[test]
fn a_schema_version_bump_does_not_move_the_key() {
    let current = archive_at(SCHEMA_VERSION);
    let older = archive_at(SCHEMA_VERSION_V1);

    // The bytes really did move, or the assertion below is about nothing.
    let payloads_at_v3: Vec<&Vec<u8>> = current.iter().map(|d| &d.payload).collect();
    let payloads_at_v1: Vec<&Vec<u8>> = older.iter().map(|d| &d.payload).collect();
    assert_ne!(
        payloads_at_v3, payloads_at_v1,
        "the fixture has to state two generations for this to be a test"
    );

    assert_eq!(
        keys(current),
        keys(older),
        "a key that moved with the schema version would silently stop finding \
         pairs the day a publisher upgraded"
    );
}

/// Batching is the publisher's decision, and no part of the state.
///
/// A feed that packs an update burst into one datagram states the same tops as
/// one that sends them singly — under one sequence number, one arrival stamp and
/// four message indices instead of four of each. A key that moved with any of
/// that would find pairs only between observation points whose publishers
/// happened to batch alike, which is a condition nobody would think to check.
#[test]
fn a_change_in_batching_does_not_move_the_key() {
    let mut singly = pack::<TopOfBook>(&defs(), PortRole::Refdata, 1);
    singly.extend(pack::<TopOfBook>(&quotes(), PortRole::Mktdata, 100));

    let mut batched = pack::<TopOfBook>(&defs(), PortRole::Refdata, 1);
    let burst = quotes();
    let burst_len = burst.len();
    batched.extend(pack_batched::<TopOfBook>(
        &burst,
        PortRole::Mktdata,
        100,
        burst_len,
    ));

    assert_eq!(
        batched.len(),
        2,
        "the batched fixture is one datagram of quotes behind one of reference data"
    );
    assert_eq!(
        singly.len(),
        PRICES.len() + 1,
        "and the other is one datagram per quote"
    );

    assert_eq!(
        keys(singly),
        keys(batched),
        "the batching moved the sequence numbers, the message indices and the \
         arrival stamps, and the market moved not at all"
    );
}

/// Two observation points decoding one state agree, and share nothing else.
///
/// The second point is what a race actually compares against: a different
/// source address, a sequence space of its own, its own arrival stamps and its
/// own recorder. Everything a convenient key reaches for — a sequence number, a
/// `Reset Count`, an address, a timestamp — differs here by construction, so a
/// key holding any of them produces two keys for one book and pairs nothing.
#[test]
fn two_observation_points_decoding_one_state_agree_on_the_key() {
    let mut here = pack::<TopOfBook>(&defs(), PortRole::Refdata, 1);
    here.extend(pack::<TopOfBook>(&quotes(), PortRole::Mktdata, 100));

    // Another path carrying the same instruments: another source, another
    // sequence space, another `Reset Count`.
    let mut there = pack_from::<TopOfBook>(&defs(), PortRole::Refdata, 7_000, 4, SECOND_SOURCE);
    there.extend(pack_from::<TopOfBook>(
        &quotes(),
        PortRole::Mktdata,
        9_000,
        4,
        SECOND_SOURCE,
    ));

    assert_eq!(
        keys_at(here, "here"),
        keys_at(there, "there"),
        "one book state seen twice is one key, or a race between two transports \
         is a query that returns nothing"
    );
}

/// Every field of the specified tuple moves the key.
///
/// The inverse failure, and the quieter one: a field left out of the hash makes
/// two different books share a key, and the pairing then reports a lead time
/// between two states that were never the same state. The tuple is
/// `(channel_id, instrument_id, bid_px_raw, bid_qty_raw, bid_source_count,
/// ask_px_raw, ask_qty_raw, ask_source_count)` and this holds the hash to
/// exactly it.
#[test]
fn every_field_of_the_tuple_moves_the_key() {
    let top = Top {
        bid: Side {
            price_raw: Some(9_950),
            qty_raw: Some(12),
            source_count: Some(2),
        },
        ask: Side {
            price_raw: Some(10_050),
            qty_raw: Some(7),
            source_count: Some(3),
        },
    };
    let base = state_key(CHANNEL_ID, AAA, &top);

    let mut moved = top;
    moved.bid.price_raw = Some(9_951);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "bid_px_raw");

    let mut moved = top;
    moved.bid.qty_raw = Some(13);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "bid_qty_raw");

    let mut moved = top;
    moved.bid.source_count = Some(3);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "bid_source_count");

    let mut moved = top;
    moved.ask.price_raw = Some(10_051);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "ask_px_raw");

    let mut moved = top;
    moved.ask.qty_raw = Some(8);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "ask_qty_raw");

    let mut moved = top;
    moved.ask.source_count = Some(2);
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &moved), "ask_source_count");

    assert_ne!(base, state_key(CHANNEL_ID + 1, AAA, &top), "channel_id");
    assert_ne!(base, state_key(CHANNEL_ID, AAA + 1, &top), "instrument_id");

    // And the two sides are not interchangeable: a hash that ate them into one
    // accumulator without their order would give a crossed book the same key as
    // the book it crossed.
    let crossed = Top {
        bid: top.ask,
        ask: top.bid,
    };
    assert_ne!(base, state_key(CHANNEL_ID, AAA, &crossed), "the sides");
}
