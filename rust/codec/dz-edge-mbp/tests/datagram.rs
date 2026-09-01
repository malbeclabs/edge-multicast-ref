//! The messages inside a real datagram, which is the only place they are ever
//! read.
//!
//! Everything in `wire_layout.rs` tests one message alone, and a message alone
//! cannot show the faults that only appear in sequence: a wrong `SIZE` puts the
//! *next* message's walk at the wrong offset, and a port role that is merely
//! declared correctly in a constant is not the same as one the builder
//! enforces. Both are asserted here through `dz-edge-core`'s own builder and
//! walk, unchanged.

use dz_edge_core::{
    AppMessage, ChannelSequence, Datagram, DatagramBuilder, EncodeError, PortRole, ResetCount,
};
use dz_edge_mbp::{
    BookClear, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel, CLEAR_BID,
    CLEAR_BOTH, MAGIC_MBP, SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE, SIDE_ASK, SIDE_BID,
};

const CHANNEL: u8 = 7;
const MTU: u16 = 1232;

fn builder(role: PortRole) -> DatagramBuilder<MarketByPrice> {
    DatagramBuilder::<MarketByPrice>::new(ChannelSequence::new(CHANNEL, ResetCount(0)), role, MTU)
}

fn level_update(price_raw: i64, qty_raw: u64, side: u8) -> LevelUpdate {
    LevelUpdate {
        instrument_id: 1,
        source_id: 2,
        side,
        action: 0,
        per_instrument_seq: 10,
        price_raw,
        qty_raw,
        timestamp_ns: 1_700_000_000_000_000_000,
        order_count: 3,
        level_index: 0,
        update_reason: 0,
        level_flags: 0,
    }
}

fn book_clear() -> BookClear {
    BookClear {
        instrument_id: 1,
        source_id: 2,
        clear_side: CLEAR_BID,
        scope: SCOPE_ENTIRE_SIDE,
        per_instrument_seq: 11,
        from_price_raw: 0,
        timestamp_ns: 1_700_000_000_000_000_001,
        clear_reason: 1,
    }
}

#[test]
fn a_mktdata_datagram_of_depth_messages_walks_back_message_for_message() {
    // Three bodies of two different sizes, so a wrong SIZE on either one puts
    // the walk at the wrong offset for everything after it — which is the
    // failure a per-message test cannot see.
    let mut b = builder(PortRole::Mktdata);
    let first = level_update(10_000_500, 7_250, SIDE_ASK);
    let cleared = book_clear();
    let last = level_update(9_999_500, 0, SIDE_BID);
    b.push(&first).expect("mktdata carries a level update");
    b.push(&cleared).expect("mktdata carries a book clear");
    b.push(&last).expect("mktdata carries a level update");
    let out = b.finish(1_700_000_000_000_000_009).expect("three messages");

    let dg = Datagram::decode(&out, MAGIC_MBP).expect("a datagram this crate's feed built");
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 3, "every message is found, and only those");

    assert_eq!(msgs[0].type_id, LevelUpdate::TYPE_ID);
    assert_eq!(msgs[0].bytes.len(), LevelUpdate::SIZE);
    assert_eq!(LevelUpdate::decode(msgs[0].bytes).expect("decodes"), first);

    assert_eq!(msgs[1].type_id, BookClear::TYPE_ID);
    assert_eq!(msgs[1].bytes.len(), BookClear::SIZE);
    assert_eq!(BookClear::decode(msgs[1].bytes).expect("decodes"), cleared);

    // The one that would break first if either size above were wrong.
    assert_eq!(msgs[2].type_id, LevelUpdate::TYPE_ID);
    assert_eq!(LevelUpdate::decode(msgs[2].bytes).expect("decodes"), last);
    assert_eq!(
        LevelUpdate::decode(msgs[2].bytes).expect("decodes").qty_raw,
        0,
        "and a deletion survives the walk as a deletion"
    );
}

#[test]
fn a_snapshot_datagram_walks_back_begin_levels_and_end() {
    // The shape a snapshot actually takes: one begin, its levels, one end, all
    // tied by the same snapshot id.
    let mut b = builder(PortRole::Snapshot);
    let begin = SnapshotBegin {
        instrument_id: 1,
        anchor_seq: 918_273_645,
        total_levels: 2,
        snapshot_id: 77,
        last_instrument_seq: 4241,
        timestamp_ns: 1_700_000_000_000_000_005,
        depth_bound: 50,
    };
    let bid = SnapshotLevel {
        snapshot_id: 77,
        price_raw: 9_999_500,
        qty_raw: 12_500,
        order_count: 3,
        side: SIDE_BID,
        level_flags: 0,
    };
    let ask = SnapshotLevel {
        snapshot_id: 77,
        price_raw: 10_000_500,
        qty_raw: 7_250,
        order_count: 1,
        side: SIDE_ASK,
        level_flags: 0,
    };
    let end = SnapshotEnd {
        instrument_id: 1,
        anchor_seq: 918_273_645,
        snapshot_id: 77,
    };
    b.push(&begin).expect("snapshot carries a begin");
    b.push(&bid).expect("snapshot carries a level");
    b.push(&ask).expect("snapshot carries a level");
    b.push(&end).expect("snapshot carries an end");
    let out = b.finish(1_700_000_000_000_000_009).expect("four messages");

    let dg = Datagram::decode(&out, MAGIC_MBP).expect("a snapshot datagram");
    let msgs: Vec<_> = dg.messages().collect();
    assert_eq!(msgs.len(), 4);
    assert_eq!(
        SnapshotBegin::decode(msgs[0].bytes).expect("decodes"),
        begin
    );
    assert_eq!(SnapshotLevel::decode(msgs[1].bytes).expect("decodes"), bid);
    assert_eq!(SnapshotLevel::decode(msgs[2].bytes).expect("decodes"), ask);
    assert_eq!(SnapshotEnd::decode(msgs[3].bytes).expect("decodes"), end);

    // What the begin promised is what arrived, which is the check a subscriber
    // makes before applying a snapshot at all.
    let levels = msgs
        .iter()
        .filter(|m| m.type_id == SnapshotLevel::TYPE_ID)
        .count();
    assert_eq!(levels as u32, begin.total_levels);
}

#[test]
fn a_snapshot_message_is_refused_by_a_mktdata_builder() {
    // The port roles asserted as constants elsewhere prove only that they were
    // written down. This proves the builder enforces them — a snapshot on the
    // live port fails at the push rather than on a capture after a deploy.
    let mut b = builder(PortRole::Mktdata);
    let err = b
        .push(&SnapshotEnd {
            instrument_id: 1,
            anchor_seq: 2,
            snapshot_id: 3,
        })
        .expect_err("a snapshot message has no business on mktdata");
    assert!(matches!(
        err,
        EncodeError::WrongPortRole {
            role: "mktdata",
            ..
        }
    ));
}

#[test]
fn a_level_update_is_refused_by_a_snapshot_builder() {
    // And the other direction, which is the one that would corrupt a book: a
    // live update applied as though it were snapshot state.
    let mut b = builder(PortRole::Snapshot);
    let err = b
        .push(&level_update(10_000_500, 7_250, SIDE_ASK))
        .expect_err("a live update has no business on the snapshot port");
    assert!(matches!(
        err,
        EncodeError::WrongPortRole {
            role: "snapshot",
            ..
        }
    ));
}

#[test]
fn a_sibling_feeds_magic_refuses_this_feeds_datagram() {
    // What a misrouted datagram meets. The walk never runs, so no body is ever
    // decoded at the wrong layout.
    let mut b = builder(PortRole::Mktdata);
    b.push(&level_update(10_000_500, 7_250, SIDE_ASK))
        .expect("one message");
    let out = b.finish(1).expect("a datagram");
    assert!(Datagram::decode(&out, dz_edge_tob::MAGIC_TOB).is_err());
}

#[test]
fn a_bounded_clear_of_both_sides_is_refused_at_the_push() {
    // `decode` refuses the same bytes, and the two are not redundant: that one
    // governs what somebody else sent, this one governs what this build emits.
    // Without it a publisher ships a clear every conformant subscriber
    // discards, and the levels it meant to remove stay in every book — a defect
    // whose only symptom is a book that quietly did not change.
    let mut b = builder(PortRole::Mktdata);
    let err = b
        .push(&BookClear {
            instrument_id: 1,
            source_id: 2,
            clear_side: CLEAR_BOTH,
            scope: SCOPE_FROM_PRICE,
            per_instrument_seq: 3,
            from_price_raw: 10_000_000,
            timestamp_ns: 1,
            clear_reason: 0,
        })
        .expect_err("the specification forbids this pairing");
    assert!(matches!(err, EncodeError::MalformedMessage { .. }), "{err}");
}

#[test]
fn a_clear_the_specification_allows_still_pushes() {
    // The neighbouring combination, so the refusal above is not a blanket one.
    let mut b = builder(PortRole::Mktdata);
    b.push(&book_clear()).expect("an ordinary clear");
}
