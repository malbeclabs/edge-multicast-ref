//! The book, and the column it exists for.
//!
//! A live book that missed datagrams applies the deltas that arrived and keeps
//! quoting a top that has diverged from the publisher's, and it cannot notice,
//! because noticing needs the datagram it did not receive. Every test here is
//! about the difference that makes.

mod common;

use common::{
    definition, identity, pack, DatagramLog, Msg, AAA, ACTION_NEW, BOTH_UPDATED, CHANNEL_ID,
    SIDE_BID, SOURCE_ID,
};
use dz_edge_core::PortRole;
use dz_edge_mbp::{
    BookClear, InstrumentReset, LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd,
    SnapshotLevel, CLEAR_BOTH, MAGIC_MBP, SCOPE_ENTIRE_SIDE, SIDE_ASK, U16_UNAVAILABLE,
};
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_events::{derive_events, state_key, DerivedEvents, EventInput, Side, Top};
use dz_recorder_rows::{BookTop, UncertainReason};

const SNAPSHOT: u32 = 7;
const ANCHOR_SEQ: u64 = 4_242;
const RESET_UPSTREAM_GAP: u8 = 3;

/// A group of messages packed from a stated sequence number, so a test can put a
/// gap between two groups by leaving a hole in the numbering.
struct Group<'a>(&'a [Msg], PortRole, u64);

fn derive<F: dz_edge_core::Feed>(groups: &[Group<'_>], magic: u16) -> DerivedEvents {
    let mut log = DatagramLog::default();
    for Group(messages, role, first) in groups {
        log.extend(pack::<F>(messages, *role, *first));
    }
    let id = identity();
    derive_events(
        &mut log,
        &EventInput {
            identity: &id,
            feed: "feed",
            object_key: "object",
            object_sha256: "sha",
            segment_seq: 3,
            magic,
            observation: "observation",
            persist_snapshot_levels: false,
        },
    )
    .expect("the log does not fail")
}

fn defs() -> Vec<Msg> {
    vec![Msg::Definition(definition(AAA, "AAA", -2))]
}

fn quote(bid: i64, ask: i64) -> Msg {
    Msg::Quote(Quote {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        update_flags: BOTH_UPDATED,
        source_timestamp_ns: 1_000_000_001,
        bid_price: bid,
        bid_qty: 12,
        ask_price: ask,
        ask_qty: 7,
        bid_source_count: 2,
        ask_source_count: 3,
    })
}

fn level(side: u8, price_raw: i64, qty_raw: u64, seq: u32) -> Msg {
    Msg::Level(LevelUpdate {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        side,
        action: ACTION_NEW,
        per_instrument_seq: seq,
        price_raw,
        qty_raw,
        timestamp_ns: 1_000_000_000 + u64::from(seq),
        order_count: U16_UNAVAILABLE,
        level_index: U16_UNAVAILABLE,
        update_reason: 0,
        level_flags: 0,
    })
}

fn cycle(anchor_seq: u64, levels: &[(u8, i64, u64)]) -> Vec<Msg> {
    let mut out = vec![Msg::SnapshotBegin(SnapshotBegin {
        instrument_id: AAA,
        anchor_seq,
        total_levels: u32::try_from(levels.len()).expect("a small fixture"),
        snapshot_id: SNAPSHOT,
        last_instrument_seq: 900,
        timestamp_ns: 1_000_000_010,
        depth_bound: 10,
    })];
    out.extend(levels.iter().map(|(side, price_raw, qty_raw)| {
        Msg::SnapshotLevel(SnapshotLevel {
            snapshot_id: SNAPSHOT,
            price_raw: *price_raw,
            qty_raw: *qty_raw,
            order_count: U16_UNAVAILABLE,
            side: *side,
            level_flags: 0,
        })
    }));
    out.push(Msg::SnapshotEnd(SnapshotEnd {
        instrument_id: AAA,
        anchor_seq,
        snapshot_id: SNAPSHOT,
    }));
    out
}

fn last(rows: &[BookTop]) -> &BookTop {
    rows.last().expect("at least one book_top row")
}

#[test]
fn a_quote_is_its_own_anchor() {
    let d = derive::<TopOfBook>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(&[quote(9_950, 10_050)], PortRole::Mktdata, 100),
        ],
        MAGIC_TOB,
    );

    // No snapshot, no prior state, and a certain top from the first message. A
    // rule requiring a cycle would have produced nothing at all for this feed.
    assert_eq!(d.book_top.len(), 1);
    let row = &d.book_top[0];
    assert_eq!(row.book_certain, 1);
    assert_eq!(row.uncertain_reason, UncertainReason::None);
    assert_eq!(row.bid_px_raw, Some(9_950));
    assert_eq!(row.ask_px_raw, Some(10_050));
    // The counts a Quote carries and a depth feed does not.
    assert_eq!(row.bid_source_count, Some(2));
    assert_eq!(row.from_anchor, 0);
}

#[test]
fn a_quote_restores_certainty_after_a_gap_by_itself() {
    let d = derive::<TopOfBook>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(&[quote(9_950, 10_050)], PortRole::Mktdata, 100),
            // A hole in the sequence space, then another quote.
            Group(&[quote(9_960, 10_060)], PortRole::Mktdata, 140),
        ],
        MAGIC_TOB,
    );

    let reasons: Vec<UncertainReason> = d.book_top.iter().map(|r| r.uncertain_reason).collect();
    // Certain, then the gap, then certain again on the very next Quote —
    // nothing about a missed one makes this one less true.
    assert_eq!(
        reasons,
        [
            UncertainReason::None,
            UncertainReason::Gap,
            UncertainReason::None
        ]
    );
    assert_eq!(last(&d.book_top).book_certain, 1);
}

#[test]
fn a_delta_book_says_no_anchor_once_and_then_says_nothing() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &[
                    level(SIDE_BID, 9_950, 12, 1),
                    level(SIDE_BID, 9_951, 13, 2),
                    level(SIDE_ASK, 10_050, 7, 3),
                ],
                PortRole::Mktdata,
                100,
            ),
        ],
        MAGIC_MBP,
    );

    // One row, with no prices, rather than absence: absence cannot be told from
    // a silent feed, and a lookup into an unanchored window would return
    // whatever preceded it.
    assert_eq!(d.book_top.len(), 1);
    let row = &d.book_top[0];
    assert_eq!(row.book_certain, 0);
    assert_eq!(row.uncertain_reason, UncertainReason::NoAnchor);
    assert_eq!(row.bid_px_raw, None);
    assert_eq!(row.ask_px_raw, None);
}

#[test]
fn a_complete_cycle_anchors_a_delta_book_and_the_row_says_it_came_from_one() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12), (SIDE_ASK, 10_050, 7)]),
                PortRole::Snapshot,
                100,
            ),
        ],
        MAGIC_MBP,
    );

    let row = last(&d.book_top);
    assert_eq!(row.book_certain, 1);
    assert_eq!(row.bid_px_raw, Some(9_950));
    assert_eq!(row.ask_px_raw, Some(10_050));
    // A depth feed carries orders at a level, not contributing sources. Mapping
    // one onto the other would put a number in a column that does not mean it.
    assert_eq!(row.bid_source_count, None);
    // The runtime pulled this on its own cadence, so it is a starting state and
    // never an observation in a race.
    assert_eq!(row.from_anchor, 1);
}

#[test]
fn an_incomplete_cycle_is_refused_rather_than_applied() {
    let mut messages = cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12), (SIDE_ASK, 10_050, 7)]);
    // The begin promised two levels; deliver one.
    messages.remove(2);

    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(&messages, PortRole::Snapshot, 100),
        ],
        MAGIC_MBP,
    );

    assert_eq!(d.book_refused.incomplete_cycle, 1);
    // Nothing anchored, so nothing certain.
    assert!(d.book_top.iter().all(|r| r.book_certain == 0));
}

#[test]
fn a_snapshot_in_flight_when_a_reset_was_published_is_refused() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &[Msg::Reset(InstrumentReset {
                    instrument_id: AAA,
                    reason: RESET_UPSTREAM_GAP,
                    new_anchor_seq: ANCHOR_SEQ,
                    timestamp_ns: 1_000_000_004,
                })],
                PortRole::Mktdata,
                100,
            ),
            // A cycle whose anchor is behind the reset's: it was already on the
            // wire when the publisher disowned the book it describes.
            Group(
                &cycle(ANCHOR_SEQ - 1, &[(SIDE_BID, 9_950, 12)]),
                PortRole::Snapshot,
                200,
            ),
        ],
        MAGIC_MBP,
    );

    assert_eq!(d.book_refused.stale_cycle, 1);
    // The one refusal that is a safety property: accepting it would rebuild from
    // a book the publisher had disowned and mark the result certain.
    assert_eq!(last(&d.book_top).book_certain, 0);
    assert_eq!(
        last(&d.book_top).uncertain_reason,
        UncertainReason::InstrumentReset
    );
}

#[test]
fn a_cycle_at_or_past_the_reset_anchor_does_recover_the_book() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &[Msg::Reset(InstrumentReset {
                    instrument_id: AAA,
                    reason: RESET_UPSTREAM_GAP,
                    new_anchor_seq: ANCHOR_SEQ,
                    timestamp_ns: 1_000_000_004,
                })],
                PortRole::Mktdata,
                100,
            ),
            Group(
                &cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12)]),
                PortRole::Snapshot,
                200,
            ),
        ],
        MAGIC_MBP,
    );

    assert_eq!(d.book_refused.stale_cycle, 0);
    assert_eq!(last(&d.book_top).book_certain, 1);
}

/// The case the whole design exists for.
#[test]
fn a_gap_with_no_price_movement_still_says_the_book_is_uncertain() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12), (SIDE_ASK, 10_050, 7)]),
                PortRole::Snapshot,
                100,
            ),
            // A hole in the mktdata sequence space, and the message that reveals
            // it changes nothing about the top: it is a level far from it.
            Group(&[level(SIDE_BID, 9_000, 1, 4)], PortRole::Mktdata, 200),
            Group(&[level(SIDE_BID, 8_999, 1, 5)], PortRole::Mktdata, 260),
        ],
        MAGIC_MBP,
    );

    let row = last(&d.book_top);
    // A live book would still be quoting 9,950 / 10,050 and calling it good. The
    // top here is the same, and the row says it cannot be believed.
    assert_eq!(row.bid_px_raw, Some(9_950));
    assert_eq!(row.ask_px_raw, Some(10_050));
    assert_eq!(row.book_certain, 0);
    assert_eq!(row.uncertain_reason, UncertainReason::Gap);
    assert!(row.uncertain_since.is_some(), "the gap is named");
}

#[test]
fn a_certainty_transition_emits_a_row_carrying_the_top_it_no_longer_vouches_for() {
    let d = derive::<TopOfBook>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(&[quote(9_950, 10_050)], PortRole::Mktdata, 100),
            Group(&[quote(9_950, 10_050)], PortRole::Mktdata, 160),
        ],
        MAGIC_TOB,
    );

    // The second quote states the same top, so nothing moved — and the gap
    // before it did. Without a row on that transition, every lookup afterwards
    // would keep returning the first row, which says the book is certain.
    let gap_row = d
        .book_top
        .iter()
        .find(|r| r.uncertain_reason == UncertainReason::Gap)
        .expect("the gap emits its own row");
    assert_eq!(gap_row.bid_px_raw, Some(9_950));
    assert_eq!(gap_row.book_certain, 0);
}

#[test]
fn a_zero_quantity_removes_a_level_and_the_top_moves_to_the_next_one() {
    let mut messages = cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12), (SIDE_BID, 9_940, 8)]);
    let anchor_len = messages.len();
    messages.push(level(SIDE_BID, 9_950, 0, 4));

    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(&messages[..anchor_len], PortRole::Snapshot, 100),
            Group(&messages[anchor_len..], PortRole::Mktdata, 200),
        ],
        MAGIC_MBP,
    );

    // Absolute aggregate quantity, and zero removes the level — the codec says
    // so on the field itself.
    assert_eq!(last(&d.book_top).bid_px_raw, Some(9_940));
    assert_eq!(last(&d.book_top).book_certain, 1);
}

#[test]
fn a_book_clear_empties_a_side_without_anchoring_anything() {
    let d = derive::<MarketByPrice>(
        &[
            Group(&defs(), PortRole::Refdata, 10),
            Group(
                &cycle(ANCHOR_SEQ, &[(SIDE_BID, 9_950, 12), (SIDE_ASK, 10_050, 7)]),
                PortRole::Snapshot,
                100,
            ),
            Group(
                &[Msg::Clear(BookClear {
                    instrument_id: AAA,
                    source_id: SOURCE_ID,
                    clear_side: CLEAR_BOTH,
                    scope: SCOPE_ENTIRE_SIDE,
                    per_instrument_seq: 10,
                    from_price_raw: 0,
                    timestamp_ns: 1_000_000_003,
                    clear_reason: 2,
                })],
                PortRole::Mktdata,
                200,
            ),
        ],
        MAGIC_MBP,
    );

    let row = last(&d.book_top);
    // It asserts the named levels are gone and a subscriber stays ready, so the
    // book is empty and still believed.
    assert_eq!(row.bid_px_raw, None);
    assert_eq!(row.ask_px_raw, None);
    assert_eq!(row.book_certain, 1);
}

#[test]
fn an_absent_side_and_a_side_priced_at_zero_are_different_books() {
    let absent = Top {
        bid: Side::default(),
        ask: Side::default(),
    };
    let at_zero = Top {
        bid: Side {
            price_raw: Some(0),
            qty_raw: Some(0),
            source_count: Some(0),
        },
        ask: Side::default(),
    };

    // Top of book states *unavailable* with a zero, so a key that encoded an
    // absent side as zeros would collapse the two readings into one.
    assert_ne!(
        state_key(CHANNEL_ID, AAA, &absent),
        state_key(CHANNEL_ID, AAA, &at_zero)
    );
}

#[test]
fn one_state_hashes_the_same_way_twice() {
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

    assert_eq!(
        state_key(CHANNEL_ID, AAA, &top),
        state_key(CHANNEL_ID, AAA, &top)
    );
    // And a different instrument is a different key, even at the same prices.
    assert_ne!(
        state_key(CHANNEL_ID, AAA, &top),
        state_key(CHANNEL_ID, AAA + 1, &top)
    );
}
