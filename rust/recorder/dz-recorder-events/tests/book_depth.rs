//! How deep the book went, and whether the message beside the number moved it.
//!
//! **Most of what is here is about one ordering.** The fold pushes an event row
//! and applies the message to the book in the same arm, and the columns are
//! stated *after*. Filled at the push site the depth is the previous message's
//! for every delta — right by coincidence on a `Trade`, which moves no book — so
//! the assertions sit on the rows where the two orderings differ.
//!
//! The rest pin what `status_after` is *not*: it states the book, not whether
//! the message applied.

mod common;

use common::{
    definition, identity, pack, DatagramLog, Msg, AAA, ABSENT_U16, ACTION_NEW, BOTH_UPDATED,
    RESET_UPSTREAM_GAP, SIDE_BID, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{
    InstrumentReset, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel,
    MAGIC_MBP, SIDE_ASK,
};
use dz_edge_refdata::ManifestSummary;
use dz_edge_tob::{Quote, TopOfBook, Trade, MAGIC_TOB};
use dz_recorder_events::{derive_events, DerivedEvents, EventInput};
use dz_recorder_rows::{BookStatus, Derivation, Event, MessageTypeLabel};

const SNAPSHOT: u32 = 7;
const ANCHOR_SEQ: u64 = 4_242;
const RECOVERY_SEQ: u64 = 9_999;

fn input<'a>(identity: &'a dz_recorder_core::RecorderIdentity, magic: u16) -> EventInput<'a> {
    EventInput {
        identity,
        feed: "feed",
        object_key: "object",
        object_sha256: "sha",
        segment_seq: 3,
        magic,
        persist_snapshot_levels: true,
        derivation: Derivation::Archive,
        observation: "observation",
    }
}

/// Reference data, then runs of messages each on its own role and sequence
/// origin.
///
/// A run rather than one list, because a snapshot cycle arrives on the
/// `snapshot` role and the deltas around it on `mktdata` — two channel
/// instances, one book — and because a hole between two runs on one role is how
/// a gap is stated.
fn derive(refdata: &[Msg], runs: &[(PortRole, u64, Vec<Msg>)], magic: u16) -> DerivedEvents {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(refdata, PortRole::Refdata, 10));
    for (role, first_sequence, messages) in runs {
        log.extend(pack::<MarketByPrice>(messages, *role, *first_sequence));
    }
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

/// Every row of one message type, in the order the fold produced them.
fn of(rows: &[Event], kind: MessageTypeLabel) -> Vec<&Event> {
    rows.iter().filter(|r| r.message_type == kind).collect()
}

/// The pair, as a row states it.
fn depth(row: &Event) -> (u32, BookStatus) {
    (row.book_levels_after, row.status_after)
}

fn level(price_raw: i64, qty_raw: u64) -> Msg {
    Msg::Level(LevelUpdate {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        per_instrument_seq: 1,
        timestamp_ns: 1_000_000_020,
        price_raw,
        qty_raw,
        order_count: ABSENT_U16,
        level_index: ABSENT_U16,
        side: SIDE_BID,
        action: ACTION_NEW,
        update_reason: 0,
        level_flags: 0,
    })
}

fn ask(price_raw: i64, qty_raw: u64) -> Msg {
    let Msg::Level(mut update) = level(price_raw, qty_raw) else {
        panic!("`level` builds a level update");
    };
    update.side = SIDE_ASK;
    Msg::Level(update)
}

/// A complete two-level cycle for `AAA`, which is the only thing that anchors a
/// delta book.
fn cycle() -> Vec<Msg> {
    vec![
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
            order_count: ABSENT_U16,
            side: SIDE_BID,
            level_flags: 0,
        }),
        Msg::SnapshotEnd(SnapshotEnd {
            instrument_id: AAA,
            anchor_seq: ANCHOR_SEQ,
            snapshot_id: SNAPSHOT,
        }),
    ]
}

/// **A delta states the depth it produced, not the depth it found.**
///
/// The assertion the whole change exists for. Two bids anchored, a third
/// added, an ask making a fourth, the third removed: 3, 4, 3. Filled at the
/// push site they read 2, 3 and 4 — the whole run shifted by one row.
#[test]
fn a_delta_states_the_depth_after_it_was_applied_and_not_before() {
    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, cycle()),
            (
                PortRole::Mktdata,
                200,
                vec![
                    // A price the cycle did not carry: a third level.
                    level(98, 3),
                    // The other side of the same book. `book_levels_after` is
                    // resting levels across BOTH sides, and a cycle of bids
                    // alone would not notice a sum that counted one of them.
                    ask(101, 6),
                    // Absolute quantity, and zero removes the level — back to
                    // three.
                    level(98, 0),
                ],
            ),
        ],
        MAGIC_MBP,
    );

    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(deltas.len(), 3);
    assert_eq!(depth(deltas[0]), (3, BookStatus::Ready));
    assert_eq!(depth(deltas[1]), (4, BookStatus::Ready));
    assert_eq!(depth(deltas[2]), (3, BookStatus::Ready));
}

/// **A cycle refused over a good book leaves `ready` on the row.**
///
/// Where reading `status_after` as *did this take effect* goes wrong: the row
/// is identical to one whose cycle anchored, because the book really is
/// ready. Not a hole — `total_levels` against `levels_seen` answers it, and
/// this pins the division of labour between the two pairs.
#[test]
fn a_cycle_refused_over_a_good_book_says_the_book_is_still_good() {
    let mut second = cycle();
    // Three promised, one carried, under an id of its own.
    second.remove(2);
    for message in &mut second {
        match message {
            Msg::SnapshotBegin(begin) => {
                begin.snapshot_id = SNAPSHOT + 1;
                begin.total_levels = 3;
            }
            Msg::SnapshotLevel(level) => level.snapshot_id = SNAPSHOT + 1,
            Msg::SnapshotEnd(end) => end.snapshot_id = SNAPSHOT + 1,
            _ => panic!("a cycle is a begin, its levels and its end"),
        }
    }

    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, cycle()),
            (PortRole::Mktdata, 200, vec![level(98, 3)]),
            (PortRole::Snapshot, 300, second),
        ],
        MAGIC_MBP,
    );

    assert_eq!(derived.book_refused.incomplete_cycle, 1);
    let begins = of(&derived.event, MessageTypeLabel::SnapshotBegin);
    let ends = of(&derived.event, MessageTypeLabel::SnapshotEnd);
    // The book the refusal left alone: three levels, and still believable.
    assert_eq!(depth(ends[1]), (3, BookStatus::Ready));
    // Which is exactly what the end of a cycle that *had* anchored would say, so
    // the difference is here and not in the status.
    assert_eq!(begins[1].total_levels, Some(3));
    assert_eq!(ends[1].levels_seen, Some(1));
    // And the one that did anchor states what it anchored, under the same label.
    assert_eq!(depth(ends[0]), (2, BookStatus::Ready));
    assert_eq!(begins[0].total_levels, Some(2));
    assert_eq!(ends[0].levels_seen, Some(2));
}

/// **A cycle whose end never arrives does not poison the instrument once a
/// later cycle anchors it.**
///
/// The stranded cycle stays in the book's map — nothing in the stream says its
/// end was lost — so the count stays up with it and an unanchored instrument
/// keeps reading `building_snapshot`. That is honest, and it is bounded: the
/// established check comes first, so the moment a later cycle anchors the book
/// the status is the book's again.
///
/// Bounded is the whole of the argument. Testing `open_cycles` before
/// `established` — the other precedence available — would leave one lost end
/// reading `building_snapshot` for the life of a live derivation, which calls
/// `close_object` once at shutdown and not per window.
#[test]
fn a_stranded_cycle_stops_mattering_once_a_later_one_anchors() {
    let mut stranded = cycle();
    // The begin and one level, and no end: cycle #1 is never closed.
    stranded.truncate(2);

    let mut anchoring = cycle();
    for message in &mut anchoring {
        match message {
            Msg::SnapshotBegin(begin) => begin.snapshot_id = SNAPSHOT + 1,
            Msg::SnapshotLevel(level) => level.snapshot_id = SNAPSHOT + 1,
            Msg::SnapshotEnd(end) => end.snapshot_id = SNAPSHOT + 1,
            _ => panic!("a cycle is a begin, its levels and its end"),
        }
    }

    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, stranded),
            (PortRole::Snapshot, 200, anchoring),
            (PortRole::Mktdata, 300, vec![level(98, 3)]),
        ],
        MAGIC_MBP,
    );

    // Cycle #1 is still open at the end of the derivation, and that is the
    // counter that says so.
    assert_eq!(derived.book_refused.unclosed_cycle, 1);
    // And the book is the second cycle's, stated as the book it is.
    let ends = of(&derived.event, MessageTypeLabel::SnapshotEnd);
    assert_eq!(depth(ends[0]), (2, BookStatus::Ready));
    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(depth(deltas[0]), (3, BookStatus::Ready));
}

/// **A cycle the book refused stops being one the instrument is building.**
///
/// A cycle counted up and never back down leaves every later row claiming a
/// snapshot is on its way — the shape of a real incident.
#[test]
fn a_refused_cycle_stops_being_a_cycle_the_instrument_is_building() {
    let mut incomplete = cycle();
    // Three promised, one carried, and the end that refuses it.
    incomplete.remove(2);
    let Msg::SnapshotBegin(begin) = &mut incomplete[0] else {
        panic!("the first message of a cycle is its begin");
    };
    begin.total_levels = 3;

    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, incomplete),
            (PortRole::Mktdata, 200, vec![level(98, 3)]),
        ],
        MAGIC_MBP,
    );

    assert_eq!(derived.book_refused.incomplete_cycle, 1);
    let end = of(&derived.event, MessageTypeLabel::SnapshotEnd);
    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    // The end anchored nothing, so it states the book it did not establish.
    assert_eq!(depth(end[0]), (0, BookStatus::Unstated));
    assert_eq!(depth(deltas[0]), (0, BookStatus::Unstated));
}

/// **A cycle says it is building, and its end says what it anchored.**
///
/// The state branch's half of the ordering. A row filled before the book
/// consumed the end says `0` and `building_snapshot`.
#[test]
fn a_cycle_builds_and_its_end_states_the_book_it_anchored() {
    let derived = derive(&defined(), &[(PortRole::Snapshot, 100, cycle())], MAGIC_MBP);

    let begin = of(&derived.event, MessageTypeLabel::SnapshotBegin);
    let levels = of(&derived.event, MessageTypeLabel::SnapshotLevel);
    let end = of(&derived.event, MessageTypeLabel::SnapshotEnd);

    // A cycle accumulates in its own maps and touches the book only at the end,
    // so nothing here has a depth to state — and `building_snapshot` is what
    // says the zero is a book under construction rather than an empty one.
    assert_eq!(depth(begin[0]), (0, BookStatus::BuildingSnapshot));
    assert_eq!(depth(levels[0]), (0, BookStatus::BuildingSnapshot));
    assert_eq!(depth(levels[1]), (0, BookStatus::BuildingSnapshot));
    // The end is the only message in the cycle that moved the book.
    assert_eq!(depth(end[0]), (2, BookStatus::Ready));
}

/// **A delta with no anchor is refused, and `''` stops the zero beside it
/// being read as an observation.**
#[test]
fn a_delta_before_any_anchor_states_nothing_rather_than_an_empty_book() {
    let derived = derive(
        &defined(),
        &[(PortRole::Mktdata, 100, vec![level(98, 3), level(97, 2)])],
        MAGIC_MBP,
    );

    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(deltas.len(), 2);
    for delta in &deltas {
        assert_eq!(depth(delta), (0, BookStatus::Unstated));
    }
    // The row exists either way. A refused message that produced no row would be
    // indistinguishable from a quiet feed.
    assert_eq!(deltas[0].price_raw, Some(98));
}

/// **A delta during a cold start says which cycle it waits on.**
///
/// `building_snapshot` rather than `''` is *a cycle will fix this* against
/// *nothing is coming*.
#[test]
fn a_delta_during_a_cold_start_says_the_cycle_is_the_one_it_waits_on() {
    let mut opening = cycle();
    // Only the begin and the first level, so the cycle is still open when the
    // delta is folded.
    opening.truncate(2);
    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, opening),
            (PortRole::Mktdata, 200, vec![level(98, 3)]),
        ],
        MAGIC_MBP,
    );

    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(depth(deltas[0]), (0, BookStatus::BuildingSnapshot));
}

/// **A reset empties the book, and the deltas after it await a fresh
/// anchor.**
///
/// Filled at the push site the reset row states the two levels the
/// publisher had just disowned, under `ready`.
#[test]
fn a_reset_empties_the_book_and_what_follows_awaits_a_snapshot() {
    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, cycle()),
            (
                PortRole::Mktdata,
                200,
                vec![
                    level(98, 3),
                    Msg::Reset(InstrumentReset {
                        instrument_id: AAA,
                        reason: RESET_UPSTREAM_GAP,
                        new_anchor_seq: RECOVERY_SEQ,
                        timestamp_ns: 1_000_000_030,
                    }),
                    level(98, 7),
                ],
            ),
        ],
        MAGIC_MBP,
    );

    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    let reset = of(&derived.event, MessageTypeLabel::InstrumentReset);
    assert_eq!(depth(deltas[0]), (3, BookStatus::Ready));
    // The publisher disowned this book, so the reset both cleared it and stated
    // the terms of its own recovery.
    assert_eq!(depth(reset[0]), (0, BookStatus::AwaitingSnapshot));
    // And the delta behind it is refused, under a status that says a cycle
    // behind `new_anchor_seq` will not do.
    assert_eq!(depth(deltas[1]), (0, BookStatus::AwaitingSnapshot));
}

/// **A gap leaves the book established, so the deltas keep applying.**
///
/// This deriver is not a live subscriber: it does not buffer through a hole.
/// So a depth under `gap` is this book's and not the publisher's.
#[test]
fn a_gap_keeps_applying_and_says_the_depth_is_this_derivers() {
    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, cycle()),
            (PortRole::Mktdata, 200, vec![level(98, 3)]),
            // A hole: 201 through 204 were never delivered.
            (PortRole::Mktdata, 205, vec![level(97, 2)]),
        ],
        MAGIC_MBP,
    );

    let deltas = of(&derived.event, MessageTypeLabel::LevelUpdate);
    assert_eq!(depth(deltas[0]), (3, BookStatus::Ready));
    // Applied, counted, and known to be short of the publisher's.
    assert_eq!(depth(deltas[1]), (4, BookStatus::Gap));
}

/// **A trade moves no book, and states the book it did not move.**
///
/// Right under either ordering, which is why it is worth saying: a suite of
/// trades alone would pass over the defect.
#[test]
fn a_trade_states_the_book_it_did_not_move() {
    let derived = derive(
        &defined(),
        &[
            (PortRole::Snapshot, 100, cycle()),
            (
                PortRole::Mktdata,
                200,
                vec![
                    level(98, 3),
                    Msg::Trade(Trade {
                        instrument_id: AAA,
                        source_id: SOURCE_ID,
                        trade_id: 5,
                        source_timestamp_ns: 1_000_000_040,
                        trade_price: 98,
                        trade_qty: 1,
                        aggressor_side: SIDE_BID,
                        trade_flags: 0,
                        cumulative_volume: 1,
                    }),
                ],
            ),
        ],
        MAGIC_MBP,
    );

    let trade = of(&derived.event, MessageTypeLabel::Trade);
    assert_eq!(depth(trade[0]), (3, BookStatus::Ready));
}

/// **A quote feed states no resting levels, and `ready` beside the zero says
/// so rather than an empty book.**
#[test]
fn a_quote_feed_has_no_resting_levels_and_is_ready_all_the_same() {
    let mut log = DatagramLog::new(pack::<TopOfBook>(&defined(), PortRole::Refdata, 10));
    log.extend(pack::<TopOfBook>(
        &[Msg::Quote(Quote {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            update_flags: BOTH_UPDATED,
            source_timestamp_ns: 1_000_000_001,
            bid_price: 9_900,
            bid_qty: 12,
            ask_price: 10_050,
            ask_qty: 7,
            bid_source_count: 0,
            ask_source_count: 0,
        })],
        PortRole::Mktdata,
        100,
    ));
    let id = identity();
    let derived = derive_events(&mut log, &input(&id, MAGIC_TOB)).expect("the log does not fail");

    let quotes = of(&derived.event, MessageTypeLabel::Quote);
    assert_eq!(depth(quotes[0]), (0, BookStatus::Ready));
}
