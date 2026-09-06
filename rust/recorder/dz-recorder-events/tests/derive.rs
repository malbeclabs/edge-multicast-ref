//! Every message in the mapping table becomes the row the design says it does.
//!
//! Two of them carry no `Source ID` and one carries neither an instrument nor a
//! timestamp, so those three are the ones worth reading: what they become is
//! resolved from era-qualified reference data and from the cycle they belong to,
//! never invented and never carried over from an adjacent message.

mod common;

use common::{
    definition, identity, pack, pack_from, DatagramLog, Msg, AAA, ABSENT_U16, ACTION_NEW,
    AGGRESSOR_BUY, BOTH_UPDATED, PRIMARY_SOURCE, RESET_UPSTREAM_GAP, SIDE_BID, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd,
    SnapshotLevel, MAGIC_MBP,
};
use dz_edge_refdata::ManifestSummary;
use dz_edge_tob::{Quote, TopOfBook, Trade, MAGIC_TOB};
use dz_recorder_events::{derive_events, DerivedEvents, EventInput};
use dz_recorder_rows::{Event, MessageTypeLabel};

const SNAPSHOT: u32 = 7;
const ANCHOR_SEQ: u64 = 4_242;

fn input<'a>(identity: &'a dz_recorder_core::RecorderIdentity, magic: u16) -> EventInput<'a> {
    EventInput {
        identity,
        feed: "feed",
        object_key: "object",
        object_sha256: "sha",
        segment_seq: 3,
        magic,
        persist_snapshot_levels: true,
    }
}

/// Reference data first, then the messages that need it — the order an archive
/// holds them in, and the order the fold depends on.
///
/// Both are packed for the same feed, because `Magic` is what stops a datagram
/// from another feed in the family being parsed at the wrong layout: reference
/// data carried under one feed's magic is foreign to the other's walk, and an
/// archive holding both is exactly the case that check exists for.
fn derive<F: dz_edge_core::Feed>(
    refdata: &[Msg],
    market: &[Msg],
    role: PortRole,
    magic: u16,
) -> DerivedEvents {
    let mut log = DatagramLog::new(pack::<F>(refdata, PortRole::Refdata, 10));
    log.extend(pack::<F>(market, role, 100));
    let id = identity();
    derive_events(&mut log, &input(&id, magic)).expect("the log does not fail")
}

fn defined() -> Vec<Msg> {
    vec![
        Msg::Definition(definition(AAA, "AAA", -2)),
        Msg::Manifest(ManifestSummary {
            channel_id: common::CHANNEL_ID,
            valid: 1,
            manifest_seq: 3,
            instrument_count: 1,
            timestamp_ns: 1,
        }),
    ]
}

fn only(rows: &[Event], kind: MessageTypeLabel) -> &Event {
    let matching: Vec<&Event> = rows.iter().filter(|r| r.message_type == kind).collect();
    assert_eq!(
        matching.len(),
        1,
        "expected one {kind}, got {}",
        matching.len()
    );
    matching[0]
}

#[test]
fn a_quote_becomes_a_row_carrying_both_sides() {
    let derived = derive::<TopOfBook>(
        &defined(),
        &[Msg::Quote(Quote {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            update_flags: BOTH_UPDATED,
            source_timestamp_ns: 1_000_000_001,
            bid_price: 9_950,
            bid_qty: 12,
            ask_price: 10_050,
            ask_qty: 7,
            bid_source_count: 0,
            ask_source_count: 0,
        })],
        PortRole::Mktdata,
        MAGIC_TOB,
    );

    let row = only(&derived.event, MessageTypeLabel::Quote);
    assert_eq!(row.bid_px_raw, Some(9_950));
    assert_eq!(row.ask_qty_raw, Some(7));
    assert_eq!(row.flags_raw, Some(BOTH_UPDATED));
    assert_eq!(row.upstream_ts.map(|ts| ts.0), Some(1_000_000_001));
    // Resolved from the definition, not from the message: a Quote carries a
    // Source ID, but nothing on the wire carries an exponent.
    assert_eq!(row.price_exp, -2);
    assert_eq!(row.symbol, "AAA");
    assert_eq!(row.segment_seq, 3);
}

#[test]
fn a_trade_keeps_the_venue_identifiers_a_quote_has_none_of() {
    let derived = derive::<TopOfBook>(
        &defined(),
        &[Msg::Trade(Trade {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            aggressor_side: AGGRESSOR_BUY,
            trade_flags: 0,
            source_timestamp_ns: 1_000_000_005,
            trade_price: 10_025,
            trade_qty: 2,
            trade_id: 7_788,
            cumulative_volume: 91,
        })],
        PortRole::Mktdata,
        MAGIC_TOB,
    );

    let row = only(&derived.event, MessageTypeLabel::Trade);
    assert_eq!(row.trade_id, Some(7_788));
    assert_eq!(row.cumulative_volume, Some(91));
    assert_eq!(row.side_raw, Some(AGGRESSOR_BUY));
    assert_eq!(row.price_raw, Some(10_025));
}

#[test]
fn a_level_update_keeps_the_two_fields_an_earlier_mapping_dropped() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[Msg::Level(LevelUpdate {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            side: SIDE_BID,
            action: ACTION_NEW,
            per_instrument_seq: 9,
            price_raw: 9_950,
            qty_raw: 12,
            timestamp_ns: 1_000_000_002,
            order_count: ABSENT_U16,
            level_index: 2,
            update_reason: 4,
            level_flags: 0x10,
        })],
        PortRole::Mktdata,
        MAGIC_MBP,
    );

    let row = only(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(row.reason_raw, Some(4), "update_reason was dropped");
    assert_eq!(row.flags_raw, Some(0x10), "level_flags was dropped");
    assert_eq!(row.per_instrument_seq, Some(9));
    // The sentinel is not a count. Written through it is an instrument with
    // sixty-five thousand orders at a level.
    assert_eq!(row.order_count, None);
    assert_eq!(row.level_index, Some(2));
}

#[test]
fn a_book_clear_keeps_its_scope_and_its_reason() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[Msg::Clear(BookClear {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            clear_side: SIDE_BID,
            scope: 1,
            per_instrument_seq: 10,
            from_price_raw: 9_900,
            timestamp_ns: 1_000_000_003,
            clear_reason: 2,
        })],
        PortRole::Mktdata,
        MAGIC_MBP,
    );

    let row = only(&derived.event, MessageTypeLabel::BookClear);
    assert_eq!(row.side_raw, Some(SIDE_BID));
    assert_eq!(row.action_raw, Some(1), "scope");
    assert_eq!(row.reason_raw, Some(2));
    assert_eq!(row.price_raw, Some(9_900));
}

#[test]
fn a_reset_carries_no_source_id_and_keeps_its_recovery_anchor() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[Msg::Reset(InstrumentReset {
            instrument_id: AAA,
            reason: RESET_UPSTREAM_GAP,
            new_anchor_seq: ANCHOR_SEQ,
            timestamp_ns: 1_000_000_004,
        })],
        PortRole::Mktdata,
        MAGIC_MBP,
    );

    let row = only(&derived.event, MessageTypeLabel::InstrumentReset);
    // Nothing on this message says who published it. The row's `source_id` came
    // from the definition in force, never from an adjacent message.
    assert_eq!(row.source_id, SOURCE_ID);
    assert_eq!(row.reason_raw, Some(RESET_UPSTREAM_GAP));
    // Dropping this is unsafe rather than lossy: without it, a snapshot already
    // in flight when the reset was published is accepted as an anchor.
    assert_eq!(row.anchor_seq, Some(ANCHOR_SEQ));
}

#[test]
fn a_snapshot_level_inherits_the_instrument_and_time_it_does_not_carry() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[
            Msg::SnapshotBegin(SnapshotBegin {
                instrument_id: AAA,
                anchor_seq: ANCHOR_SEQ,
                total_levels: 2,
                snapshot_id: SNAPSHOT,
                last_instrument_seq: 900,
                timestamp_ns: 1_000_000_010,
                depth_bound: 10,
            }),
            Msg::SnapshotLevel(SnapshotLevel {
                snapshot_id: SNAPSHOT,
                price_raw: 100,
                qty_raw: 5,
                order_count: ABSENT_U16,
                side: SIDE_BID,
                level_flags: 0,
            }),
            Msg::SnapshotLevel(SnapshotLevel {
                snapshot_id: SNAPSHOT,
                price_raw: 99,
                qty_raw: 4,
                order_count: 3,
                side: SIDE_BID,
                level_flags: 0,
            }),
            Msg::SnapshotEnd(SnapshotEnd {
                instrument_id: AAA,
                anchor_seq: ANCHOR_SEQ,
                snapshot_id: SNAPSHOT,
            }),
        ],
        PortRole::Snapshot,
        MAGIC_MBP,
    );

    let levels: Vec<&Event> = derived
        .event
        .iter()
        .filter(|r| r.message_type == MessageTypeLabel::SnapshotLevel)
        .collect();
    assert_eq!(levels.len(), 2);
    for level in &levels {
        // Neither is on the wire. Both come from the begin this level's
        // `snapshot_id` ties it to, which is why the id is on the level at all.
        assert_eq!(level.instrument_id, AAA);
        assert_eq!(level.upstream_ts.map(|ts| ts.0), Some(1_000_000_010));
    }
    // Assigned by the fold from arrival order, and marked as derived rather than
    // read — the message has no level index field.
    assert_eq!(levels[0].level_index, Some(1));
    assert_eq!(levels[1].level_index, Some(2));
    assert_eq!(levels[0].order_count, None);
    assert_eq!(levels[1].order_count, Some(3));
}

#[test]
fn a_complete_cycle_states_what_it_promised_and_what_it_carried() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[
            Msg::SnapshotBegin(SnapshotBegin {
                instrument_id: AAA,
                anchor_seq: ANCHOR_SEQ,
                total_levels: 3,
                snapshot_id: SNAPSHOT,
                last_instrument_seq: 900,
                timestamp_ns: 1_000_000_010,
                depth_bound: 10,
            }),
            Msg::SnapshotLevel(SnapshotLevel {
                snapshot_id: SNAPSHOT,
                price_raw: 100,
                qty_raw: 5,
                order_count: 1,
                side: SIDE_BID,
                level_flags: 0,
            }),
            Msg::SnapshotEnd(SnapshotEnd {
                instrument_id: AAA,
                anchor_seq: ANCHOR_SEQ,
                snapshot_id: SNAPSHOT,
            }),
        ],
        PortRole::Snapshot,
        MAGIC_MBP,
    );

    let begin = only(&derived.event, MessageTypeLabel::SnapshotBegin);
    let end = only(&derived.event, MessageTypeLabel::SnapshotEnd);
    // The pair that answers *was the snapshot complete* from rows alone, which
    // is what makes persisting every level optional.
    assert_eq!(begin.total_levels, Some(3));
    assert_eq!(end.levels_seen, Some(1));
    assert_eq!(begin.anchor_seq, Some(ANCHOR_SEQ));
}

#[test]
fn a_level_belonging_to_no_open_cycle_is_refused_rather_than_attributed() {
    let derived = derive::<MarketByPrice>(
        &defined(),
        &[Msg::SnapshotLevel(SnapshotLevel {
            snapshot_id: 999,
            price_raw: 100,
            qty_raw: 5,
            order_count: 1,
            side: SIDE_BID,
            level_flags: 0,
        })],
        PortRole::Snapshot,
        MAGIC_MBP,
    );

    // Guessing the most recent instrument would silently move levels between
    // books, and nothing downstream could tell.
    assert_eq!(derived.refused.orphan_snapshot_level, 1);
    assert!(derived.event.is_empty());
}

#[test]
fn a_message_for_an_undefined_instrument_is_refused_rather_than_filled_in() {
    let derived = derive::<TopOfBook>(
        &[],
        &[Msg::Quote(Quote {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            update_flags: BOTH_UPDATED,
            source_timestamp_ns: 1_000_000_001,
            bid_price: 9_950,
            bid_qty: 12,
            ask_price: 10_050,
            ask_qty: 7,
            bid_source_count: 0,
            ask_source_count: 0,
        })],
        PortRole::Mktdata,
        MAGIC_TOB,
    );

    // `price_exp` is not nullable, and the value that would have to be invented
    // is the one that decides what the price means.
    assert_eq!(derived.refused.unresolved_instrument, 1);
    assert!(derived.event.is_empty());
}

#[test]
fn a_restatement_applies_to_the_prices_that_came_after_it() {
    let mut log = DatagramLog::new(pack::<TopOfBook>(
        &[Msg::Definition(definition(AAA, "AAA", -2))],
        PortRole::Refdata,
        10,
    ));
    log.extend(pack::<TopOfBook>(&[quote(9_950)], PortRole::Mktdata, 100));
    log.extend(pack::<TopOfBook>(
        &[Msg::Definition(definition(AAA, "AAA", -4))],
        PortRole::Refdata,
        200,
    ));
    log.extend(pack::<TopOfBook>(&[quote(9_951)], PortRole::Mktdata, 300));

    let id = identity();
    let derived = derive_events(&mut log, &input(&id, MAGIC_TOB)).expect("the log does not fail");

    // The whole reason reference data is surfaced with its position: the three
    // outputs are merged in archive order, so a definition applies from where it
    // was carried rather than to everything in the object.
    let quotes: Vec<&Event> = derived
        .event
        .iter()
        .filter(|r| r.message_type == MessageTypeLabel::Quote)
        .collect();
    assert_eq!(quotes.len(), 2);
    assert_eq!(quotes[0].price_exp, -2);
    assert_eq!(quotes[1].price_exp, -4);
}

#[test]
fn one_instrument_row_per_statement_and_not_per_definition_observed() {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(
        &[
            Msg::Definition(definition(AAA, "AAA", -2)),
            Msg::Definition(definition(AAA, "AAA", -2)),
            Msg::Definition(definition(AAA, "AAA", -2)),
            Msg::Definition(definition(AAA, "AAA", -4)),
        ],
        PortRole::Refdata,
        10,
    ));

    let id = identity();
    let derived = derive_events(&mut log, &input(&id, MAGIC_MBP)).expect("the log does not fail");

    // The definition cycle repeats forever. Two statements were made; the other
    // two repetitions are the publisher's pacing, not the venue's changes.
    assert_eq!(derived.instrument.len(), 2);
    assert_eq!(derived.instrument[0].price_exp, -2);
    assert_eq!(derived.instrument[1].price_exp, -4);
    assert!(derived.instrument[0].from_sequence < derived.instrument[1].from_sequence);
    // Observed three times, so the last sighting is later than the first.
    assert!(derived.instrument[0].last_seen_ts.0 > derived.instrument[0].first_seen_ts.0);
}

#[test]
fn two_paths_serving_one_channel_do_not_share_reference_data() {
    let other = std::net::Ipv4Addr::new(198, 51, 100, 9);
    let mut log = DatagramLog::new(pack_from::<TopOfBook>(
        &[Msg::Definition(definition(AAA, "AAA", -2))],
        PortRole::Refdata,
        10,
        0,
        PRIMARY_SOURCE,
    ));
    log.extend(pack_from::<TopOfBook>(
        &[quote(9_950)],
        PortRole::Mktdata,
        100,
        0,
        other,
    ));

    let id = identity();
    let derived = derive_events(&mut log, &input(&id, MAGIC_TOB)).expect("the log does not fail");

    // A definition on one path says nothing about the other's sequence space.
    assert_eq!(derived.refused.unresolved_instrument, 1);
    assert!(derived
        .event
        .iter()
        .all(|r| r.message_type != MessageTypeLabel::Quote));
}

fn quote(bid_price: i64) -> Msg {
    Msg::Quote(Quote {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        update_flags: BOTH_UPDATED,
        source_timestamp_ns: 1_000_000_001,
        bid_price,
        bid_qty: 12,
        ask_price: 10_050,
        ask_qty: 7,
        bid_source_count: 0,
        ask_source_count: 0,
    })
}
