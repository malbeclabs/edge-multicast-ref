//! The byte layout, asserted field by field at its own offset.
//!
//! Deliberately not written as "encode then decode and compare": that passes
//! whenever the two halves agree, including when both are wrong in the same
//! way. Every field below is read out of the encoded bytes at the offset the
//! specification puts it at, so a transposed pair or a shifted field fails here
//! rather than in a capture after a deploy.

use dz_edge_core::{AppMessage, DecodeError, PortRole};
use dz_edge_mbp::{
    BookClear, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel, CLEAR_ASK, CLEAR_BOTH,
    MAGIC_MBP, SCOPE_ENTIRE_SIDE, SCOPE_FROM_PRICE, SIDE_ASK, SIDE_BID, U16_UNAVAILABLE,
};

/// Asymmetric on purpose: every field holds a different value, so a
/// transposition cannot pass.
fn level_update() -> LevelUpdate {
    LevelUpdate {
        instrument_id: 1,
        source_id: 2,
        side: SIDE_ASK,
        action: 3,
        per_instrument_seq: 4,
        price_raw: 10_000_500,
        qty_raw: 7_250,
        timestamp_ns: 1_700_000_000_000_000_000,
        order_count: 5,
        level_index: 6,
        update_reason: 7,
        level_flags: 8,
    }
}

fn encoded<M: AppMessage>(message: &M) -> Vec<u8> {
    let mut buf = vec![0u8; M::SIZE];
    message.encode_into(&mut buf);
    buf
}

fn u16_at(buf: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(buf[at..at + 2].try_into().expect("two bytes"))
}

fn u32_at(buf: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(buf[at..at + 4].try_into().expect("four bytes"))
}

fn u64_at(buf: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(buf[at..at + 8].try_into().expect("eight bytes"))
}

fn i64_at(buf: &[u8], at: usize) -> i64 {
    i64::from_le_bytes(buf[at..at + 8].try_into().expect("eight bytes"))
}

#[test]
fn the_feed_magic_is_this_feeds_own() {
    // A sibling feed's datagram misrouted onto this group is refused by this
    // value and by nothing else, so it is worth asserting against the literal
    // rather than against a constant that could drift with it.
    assert_eq!(MAGIC_MBP, 0x4442);
    assert_ne!(MAGIC_MBP, dz_edge_tob::MAGIC_TOB);
}

#[test]
fn a_level_update_puts_every_field_where_the_spec_says() {
    let buf = encoded(&level_update());
    assert_eq!(buf.len(), 48);
    assert_eq!(buf[0], 0x40, "type id");
    assert_eq!(buf[1], 48, "declared length");
    assert_eq!(u16_at(&buf, 2), 0, "flags");
    assert_eq!(u32_at(&buf, 4), 1, "instrument id");
    assert_eq!(u16_at(&buf, 8), 2, "source id");
    assert_eq!(buf[10], SIDE_ASK, "side");
    assert_eq!(buf[11], 3, "action");
    assert_eq!(u32_at(&buf, 12), 4, "per-instrument sequence");
    assert_eq!(i64_at(&buf, 16), 10_000_500, "price");
    assert_eq!(u64_at(&buf, 24), 7_250, "quantity");
    assert_eq!(u64_at(&buf, 32), 1_700_000_000_000_000_000, "timestamp");
    assert_eq!(u16_at(&buf, 40), 5, "order count");
    assert_eq!(u16_at(&buf, 42), 6, "level index");
    assert_eq!(buf[44], 7, "update reason");
    assert_eq!(buf[45], 8, "level flags");
    assert_eq!(&buf[46..48], &[0, 0], "reserved");
}

#[test]
fn a_book_clear_puts_every_field_where_the_spec_says() {
    let clear = BookClear {
        instrument_id: 1,
        source_id: 2,
        clear_side: CLEAR_ASK,
        scope: SCOPE_FROM_PRICE,
        per_instrument_seq: 3,
        from_price_raw: -10_000_500,
        timestamp_ns: 1_700_000_000_000_000_001,
        clear_reason: 4,
    };
    let buf = encoded(&clear);
    assert_eq!(buf.len(), 36);
    assert_eq!(buf[0], 0x41);
    assert_eq!(buf[1], 36);
    assert_eq!(u32_at(&buf, 4), 1, "instrument id");
    assert_eq!(u16_at(&buf, 8), 2, "source id");
    assert_eq!(buf[10], CLEAR_ASK, "clear side");
    assert_eq!(buf[11], SCOPE_FROM_PRICE, "scope");
    assert_eq!(u32_at(&buf, 12), 3, "per-instrument sequence");
    assert_eq!(
        i64_at(&buf, 16),
        -10_000_500,
        "from price, and it is signed"
    );
    assert_eq!(u64_at(&buf, 24), 1_700_000_000_000_000_001, "timestamp");
    assert_eq!(buf[32], 4, "clear reason");
    assert_eq!(&buf[33..36], &[0, 0, 0], "reserved");
    assert_eq!(BookClear::decode(&buf).expect("round trip"), clear);
}

#[test]
fn a_snapshot_begin_puts_every_field_where_the_spec_says() {
    let begin = SnapshotBegin {
        instrument_id: 1,
        anchor_seq: 2,
        total_levels: 3,
        snapshot_id: 4,
        last_instrument_seq: 5,
        timestamp_ns: 1_700_000_000_000_000_002,
        depth_bound: 6,
    };
    let buf = encoded(&begin);
    assert_eq!(buf.len(), 40);
    assert_eq!(buf[0], 0x20);
    assert_eq!(buf[1], 40);
    assert_eq!(u32_at(&buf, 4), 1, "instrument id");
    assert_eq!(u64_at(&buf, 8), 2, "anchor sequence");
    assert_eq!(u32_at(&buf, 16), 3, "total levels");
    assert_eq!(u32_at(&buf, 20), 4, "snapshot id");
    assert_eq!(u32_at(&buf, 24), 5, "last instrument sequence");
    assert_eq!(u64_at(&buf, 28), 1_700_000_000_000_000_002, "timestamp");
    assert_eq!(u32_at(&buf, 36), 6, "depth bound");
    assert_eq!(SnapshotBegin::decode(&buf).expect("round trip"), begin);
}

#[test]
fn a_snapshot_level_puts_every_field_where_the_spec_says() {
    let level = SnapshotLevel {
        snapshot_id: 1,
        price_raw: 9_999_500,
        qty_raw: 12_500,
        order_count: 2,
        side: SIDE_BID,
        level_flags: 3,
    };
    let buf = encoded(&level);
    assert_eq!(buf.len(), 32);
    assert_eq!(buf[0], 0x42);
    assert_eq!(buf[1], 32);
    assert_eq!(u32_at(&buf, 4), 1, "snapshot id");
    assert_eq!(i64_at(&buf, 8), 9_999_500, "price");
    assert_eq!(u64_at(&buf, 16), 12_500, "quantity");
    assert_eq!(u16_at(&buf, 24), 2, "order count");
    assert_eq!(buf[26], SIDE_BID, "side");
    assert_eq!(buf[27], 3, "level flags");
    assert_eq!(&buf[28..32], &[0, 0, 0, 0], "reserved");
    assert_eq!(SnapshotLevel::decode(&buf).expect("round trip"), level);
}

#[test]
fn a_snapshot_end_puts_every_field_where_the_spec_says() {
    let end = SnapshotEnd {
        instrument_id: 1,
        anchor_seq: 2,
        snapshot_id: 3,
    };
    let buf = encoded(&end);
    assert_eq!(buf.len(), 20);
    assert_eq!(buf[0], 0x22);
    assert_eq!(buf[1], 20);
    assert_eq!(u32_at(&buf, 4), 1, "instrument id");
    assert_eq!(u64_at(&buf, 8), 2, "anchor sequence");
    assert_eq!(u32_at(&buf, 16), 3, "snapshot id");
    assert_eq!(SnapshotEnd::decode(&buf).expect("round trip"), end);
}

#[test]
fn a_level_update_round_trips_through_its_own_bytes() {
    let buf = encoded(&level_update());
    assert_eq!(
        LevelUpdate::decode(&buf).expect("round trip"),
        level_update()
    );
}

#[test]
fn a_quantity_of_zero_is_a_deletion_and_not_an_absent_field() {
    // The one value in this feed whose meaning is not its magnitude. A decoder
    // that treated it as missing would leave the level in the book for ever.
    let mut update = level_update();
    update.qty_raw = 0;
    let buf = encoded(&update);
    assert_eq!(u64_at(&buf, 24), 0);
    assert_eq!(LevelUpdate::decode(&buf).expect("round trip").qty_raw, 0);
}

#[test]
fn an_unavailable_order_count_survives_the_round_trip_as_the_sentinel() {
    // It saturates rather than wrapping, so it must come back as itself and
    // never as a count. A consumer averaging it would report a number no
    // publisher sent.
    let mut update = level_update();
    update.order_count = U16_UNAVAILABLE;
    update.level_index = U16_UNAVAILABLE;
    let back = LevelUpdate::decode(&encoded(&update)).expect("round trip");
    assert_eq!(back.order_count, U16_UNAVAILABLE);
    assert_eq!(back.level_index, U16_UNAVAILABLE);
}

#[test]
fn a_bounded_clear_of_both_sides_is_refused_rather_than_guessed() {
    // One price cannot bound two sides running in opposite directions: outward
    // means down on the bids and up on the asks. There is no reading two
    // implementations would agree on, which is when a decoder must refuse
    // rather than pick one.
    let clear = BookClear {
        instrument_id: 1,
        source_id: 2,
        clear_side: CLEAR_BOTH,
        scope: SCOPE_FROM_PRICE,
        per_instrument_seq: 3,
        from_price_raw: 10_000_000,
        timestamp_ns: 1,
        clear_reason: 0,
    };
    assert!(matches!(
        BookClear::decode(&encoded(&clear)),
        Err(DecodeError::MalformedBody { type_id: 0x41, .. })
    ));
}

#[test]
fn clearing_both_sides_entirely_is_ordinary() {
    // The neighbouring combination, and a common one: no bound, so no
    // contradiction.
    let clear = BookClear {
        instrument_id: 1,
        source_id: 2,
        clear_side: CLEAR_BOTH,
        scope: SCOPE_ENTIRE_SIDE,
        per_instrument_seq: 3,
        from_price_raw: 0,
        timestamp_ns: 1,
        clear_reason: 0,
    };
    assert_eq!(BookClear::decode(&encoded(&clear)).expect("valid"), clear);
}

#[test]
fn a_short_buffer_is_refused_before_its_type_id_is_judged() {
    // The order matters: a buffer too short to hold the type id cannot be
    // judged by it, and reporting BadTypeId for a truncation sends a reader
    // looking for the wrong fault.
    let buf = encoded(&level_update());
    for len in 0..LevelUpdate::SIZE {
        assert!(matches!(
            LevelUpdate::decode(&buf[..len]),
            Err(DecodeError::ShortBuffer { need: 48, .. })
        ));
    }
}

#[test]
fn a_body_at_another_types_id_is_refused() {
    // What a misrouted sibling message looks like once framing has accepted it.
    let mut buf = encoded(&level_update());
    buf[0] = 0x03; // Quote, in the top-of-book feed.
    assert!(matches!(
        LevelUpdate::decode(&buf),
        Err(DecodeError::BadTypeId(0x03))
    ));
}

#[test]
fn a_declared_length_that_is_not_this_messages_size_is_refused() {
    let mut buf = encoded(&level_update());
    buf[1] = 44;
    assert!(matches!(
        LevelUpdate::decode(&buf),
        Err(DecodeError::LengthMismatch {
            type_id: 0x40,
            declared: 44,
            expected: 48
        })
    ));
}

#[test]
fn the_depth_messages_carry_the_port_roles_the_spec_gives_them() {
    // The builder refuses a message on a role it does not list, so this is what
    // makes a snapshot message on the mktdata port fail at the push rather than
    // on a capture after a deploy. This feed is also the first to claim the
    // snapshot role at all.
    assert_eq!(LevelUpdate::PORT_ROLES, &[PortRole::Mktdata]);
    assert_eq!(BookClear::PORT_ROLES, &[PortRole::Mktdata]);
    assert_eq!(SnapshotBegin::PORT_ROLES, &[PortRole::Snapshot]);
    assert_eq!(SnapshotLevel::PORT_ROLES, &[PortRole::Snapshot]);
    assert_eq!(SnapshotEnd::PORT_ROLES, &[PortRole::Snapshot]);
}

#[test]
fn no_depth_message_stamps_a_channel_id_over_its_own_body() {
    // None of them carries one, so the stamp must be a no-op rather than a
    // write at a plausible-looking offset.
    for mut buf in [
        encoded(&level_update()),
        encoded(&SnapshotEnd {
            instrument_id: 1,
            anchor_seq: 2,
            snapshot_id: 3,
        }),
    ] {
        let before = buf.clone();
        LevelUpdate::stamp_channel_id(&mut buf, 0xAB);
        SnapshotEnd::stamp_channel_id(&mut buf, 0xAB);
        assert_eq!(buf, before);
    }
}
