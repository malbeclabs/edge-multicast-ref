//! Splitting the input is a no-op, or the state did not cross the cut.
//!
//! A caller reading a socket has to cut arrivals into windows, because the fold
//! sorts a whole input before folding any of it. The property that makes such a
//! caller correct is this one: the same bytes, cut anywhere, derive the same
//! rows. Every test here derives one input three ways — whole, split into one
//! kept [`DerivationState`], and split into two independent `derive_events` calls —
//! and asserts the first two agree and that the third is the failure the kept
//! state removes.
//!
//! The third assertion is what stops these from documenting the tree: without
//! it a test that never crossed a boundary would pass under any implementation.

mod common;

use common::{
    definition, identity, pack, DatagramLog, Msg, OwnedDatagram, AAA, ACTION_NEW, BBB,
    BOTH_UPDATED, CHANNEL_ID, PRIMARY_SOURCE, SIDE_BID, SOURCE_ID,
};
use dz_edge_core::{Feed, PortRole};
use dz_edge_mbp::{
    LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel, MAGIC_MBP, SIDE_ASK,
    U16_UNAVAILABLE,
};
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_core::{RecordedDatagram, Source, SourceError};
use dz_recorder_events::{
    derive_events, derive_events_into, BookRefused, Channel, DerivationState, DerivedEvents,
    EventInput, Refused,
};
use dz_recorder_rows::{Derivation, Instrument, UncertainReason};

const SNAPSHOT: u32 = 7;
const ANCHOR_SEQ: u64 = 4_242;

fn input<'a>(id: &'a dz_recorder_core::RecorderIdentity, magic: u16) -> EventInput<'a> {
    EventInput {
        identity: id,
        feed: "feed",
        object_key: "object",
        object_sha256: "sha",
        segment_seq: 3,
        magic,
        observation: "observation",
        persist_snapshot_levels: true,
        // `Archive` on both sides of every comparison here, because the
        // property under test is that splitting changes nothing and this field
        // is the caller's statement rather than the fold's finding. A live
        // caller sets `Live`, which is what stops one of its rows passing for a
        // verified one.
        derivation: Derivation::Archive,
    }
}

/// A group of messages packed from a stated sequence number, so a test can put a
/// gap between two groups by leaving a hole in the numbering.
struct Group<'a>(&'a [Msg], PortRole, u64);

fn build<F: Feed>(groups: &[Group<'_>]) -> Vec<OwnedDatagram> {
    let mut out = Vec::new();
    for Group(messages, role, first) in groups {
        out.extend(pack::<F>(messages, *role, *first));
    }
    out
}

/// Two derivations reported in parts, as one.
///
/// Counts add because every one of them is a count of occurrences, and the rows
/// concatenate because the halves are in archive order by construction.
fn merge(mut a: DerivedEvents, b: DerivedEvents, closed: BookRefused) -> DerivedEvents {
    a.event.extend(b.event);
    a.book_top.extend(b.book_top);
    a.instrument.extend(b.instrument);
    a.refused = Refused {
        unresolved_instrument: a.refused.unresolved_instrument + b.refused.unresolved_instrument,
        orphan_snapshot_level: a.refused.orphan_snapshot_level + b.refused.orphan_snapshot_level,
        out_of_order_definition: a.refused.out_of_order_definition
            + b.refused.out_of_order_definition,
    };
    a.book_refused = BookRefused {
        incomplete_cycle: a.book_refused.incomplete_cycle
            + b.book_refused.incomplete_cycle
            + closed.incomplete_cycle,
        stale_cycle: a.book_refused.stale_cycle + b.book_refused.stale_cycle + closed.stale_cycle,
        unclosed_cycle: a.book_refused.unclosed_cycle
            + b.book_refused.unclosed_cycle
            + closed.unclosed_cycle,
    };
    a
}

/// The whole input, one call — the answer the split ones are measured against.
fn whole(all: &[OwnedDatagram], magic: u16) -> DerivedEvents {
    let id = identity();
    derive_events(&mut DatagramLog::new(all.to_vec()), &input(&id, magic))
        .expect("the log does not fail")
}

/// Split, into one `DerivationState` the caller keeps across the cut.
fn split_kept(all: &[OwnedDatagram], at: usize, magic: u16) -> DerivedEvents {
    let id = identity();
    let (first, second) = all.split_at(at);
    let mut state = DerivationState::new();
    let a = derive_events_into(
        &mut state,
        &mut DatagramLog::new(first.to_vec()),
        &input(&id, magic),
    )
    .expect("the log does not fail");
    let b = derive_events_into(
        &mut state,
        &mut DatagramLog::new(second.to_vec()),
        &input(&id, magic),
    )
    .expect("the log does not fail");
    // Once, at the end of the derivation — never per window.
    let closed = state.close_object();
    merge(a, b, closed)
}

/// Split, into two derivations that share nothing — what a caller gets today.
fn split_today(all: &[OwnedDatagram], at: usize, magic: u16) -> DerivedEvents {
    let (first, second) = all.split_at(at);
    let a = whole(first, magic);
    let b = whole(second, magic);
    merge(a, b, BookRefused::default())
}

/// `recorder.instrument` is `ReplacingMergeTree(last_seen_ts)` on
/// `(channel_id, instrument_id, from_sequence, source address, dst_port, site,
/// recorder)`.
///
/// `seen` is per call by design — persisting it would re-emit every instrument
/// row in every later window — so a definition restated in both halves yields
/// one row per call where the whole input yields one row. The store collapses
/// them on that version column, keeping the greatest `last_seen_ts`, which is
/// the row the whole-input run produced. Reducing here asserts equality of what
/// is stored rather than of what is handed to the sink.
fn reduce(rows: &[Instrument]) -> Vec<Instrument> {
    let mut out: Vec<Instrument> = Vec::new();
    for row in rows {
        let key = |r: &Instrument| {
            (
                r.channel_id,
                r.instrument_id,
                r.from_sequence,
                r.source_addr,
                r.dst_port,
                r.site.clone(),
                r.recorder.clone(),
            )
        };
        match out.iter_mut().find(|held| key(held) == key(row)) {
            Some(held) if row.last_seen_ts.0 > held.last_seen_ts.0 => *held = row.clone(),
            Some(_) => {}
            None => out.push(row.clone()),
        }
    }
    out
}

/// The criterion, in one place: for this input and this cut, splitting changes
/// nothing that is stored.
fn assert_split_is_a_no_op(all: &[OwnedDatagram], at: usize, magic: u16) {
    let whole = whole(all, magic);
    let kept = split_kept(all, at, magic);

    assert_eq!(
        kept.event, whole.event,
        "the same messages became different event rows, or landed in a different order"
    );
    assert_eq!(
        kept.book_top, whole.book_top,
        "the book answered differently across the cut"
    );
    assert_eq!(
        kept.refused, whole.refused,
        "the per-call refusals do not sum to the whole's"
    );
    assert_eq!(
        kept.book_refused, whole.book_refused,
        "the book's per-call refusals do not sum to the whole's"
    );
    assert_eq!(
        reduce(&kept.instrument),
        reduce(&whole.instrument),
        "the instrument rows differ after the store's own collapse"
    );
}

fn quote(bid: i64) -> Msg {
    Msg::Quote(Quote {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        update_flags: BOTH_UPDATED,
        source_timestamp_ns: 1_000_000_001,
        bid_price: bid,
        bid_qty: 12,
        ask_price: bid + 100,
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

fn snapshot_begin(total_levels: u32) -> Msg {
    Msg::SnapshotBegin(SnapshotBegin {
        instrument_id: AAA,
        anchor_seq: ANCHOR_SEQ,
        total_levels,
        snapshot_id: SNAPSHOT,
        last_instrument_seq: 900,
        timestamp_ns: 1_000_000_010,
        depth_bound: 10,
    })
}

fn snapshot_level(side: u8, price_raw: i64, qty_raw: u64) -> Msg {
    Msg::SnapshotLevel(SnapshotLevel {
        snapshot_id: SNAPSHOT,
        price_raw,
        qty_raw,
        order_count: U16_UNAVAILABLE,
        side,
        level_flags: 0,
    })
}

fn snapshot_end() -> Msg {
    Msg::SnapshotEnd(SnapshotEnd {
        instrument_id: AAA,
        anchor_seq: ANCHOR_SEQ,
        snapshot_id: SNAPSHOT,
    })
}

/// The definition is in the first half and every price that needs it is in the
/// second.
///
/// The first consequence, and the one with no window length that removes it:
/// `source_id`, `price_exp` and `qty_exp` are not nullable on the row and are
/// exactly the values that decide what a price means, so a message with no
/// definition in force is refused rather than filled in. Correct, and per
/// window it is a floor rather than a rate.
#[test]
fn a_definition_in_the_first_half_resolves_the_second_halfs_prices() {
    let all = build::<TopOfBook>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(&[quote(9_950), quote(9_951)], PortRole::Mktdata, 100),
    ]);

    assert_split_is_a_no_op(&all, 1, MAGIC_TOB);

    // What the same cut does with no state across it.
    let today = split_today(&all, 1, MAGIC_TOB);
    assert_eq!(
        today.refused.unresolved_instrument, 2,
        "today both quotes are refused"
    );
    assert!(
        today.event.is_empty(),
        "and neither becomes a row: the whole window's prices are lost"
    );
}

/// A snapshot cycle straddles the cut.
///
/// Two states cross here and both are needed: the book's own open cycle, which
/// accumulates the prices to anchor from, and the snapshot-id attribution map,
/// without which a level — carrying neither an instrument nor a timestamp — is
/// a level nothing can attribute.
#[test]
fn a_snapshot_cycle_straddling_the_cut_still_anchors_the_book() {
    let all = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(
            &[snapshot_begin(2), snapshot_level(SIDE_BID, 9_950, 12)],
            PortRole::Snapshot,
            100,
        ),
        Group(
            &[snapshot_level(SIDE_ASK, 10_050, 7), snapshot_end()],
            PortRole::Snapshot,
            102,
        ),
    ]);

    // Cut between the two snapshot groups: begin plus one level, then the
    // second level and the end.
    assert_split_is_a_no_op(&all, 3, MAGIC_MBP);

    let kept = split_kept(&all, 3, MAGIC_MBP);
    assert!(
        kept.book_top.iter().any(|r| r.book_certain == 1),
        "the cycle completed across the cut and anchored the book"
    );

    let today = split_today(&all, 3, MAGIC_MBP);
    assert_eq!(
        today.refused.orphan_snapshot_level, 1,
        "today the level after the cut can be attributed to nothing"
    );
    assert_eq!(
        today.book_refused.unclosed_cycle, 1,
        "and the cycle the first half opened anchored neither side"
    );
    assert!(
        !today.book_top.iter().any(|r| r.book_certain == 1),
        "so nothing anchors the book at all"
    );
}

/// A `mktdata` sequence gap straddles the cut.
///
/// `Book::observe_sequence` keeps a high-water mark, and correctly so — a
/// last-seen mark would let a reordered datagram move it backwards and invent a
/// gap. The consequence is that a fresh book's first datagram only
/// *establishes* the mark, so with no state across the cut the gap is counted
/// nowhere at all.
#[test]
fn a_sequence_gap_straddling_the_cut_is_still_a_gap() {
    let all = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(
            &[
                snapshot_begin(2),
                snapshot_level(SIDE_BID, 9_950, 12),
                snapshot_level(SIDE_ASK, 10_050, 7),
                snapshot_end(),
            ],
            PortRole::Snapshot,
            100,
        ),
        Group(&[level(SIDE_BID, 9_000, 1, 4)], PortRole::Mktdata, 200),
        // A hole in the mktdata sequence space, straddling the cut below.
        Group(&[level(SIDE_BID, 8_999, 1, 5)], PortRole::Mktdata, 260),
    ]);

    // Cut after the first mktdata level: the hole is between the halves.
    assert_split_is_a_no_op(&all, 6, MAGIC_MBP);

    let kept = split_kept(&all, 6, MAGIC_MBP);
    assert!(
        kept.book_top
            .iter()
            .any(|r| r.book_certain == 0 && r.uncertain_reason == UncertainReason::Gap),
        "the gap across the cut made the book uncertain"
    );

    let today = split_today(&all, 6, MAGIC_MBP);
    assert!(
        !today
            .book_top
            .iter()
            .any(|r| r.uncertain_reason == UncertainReason::Gap),
        "today the second half's first datagram only establishes the mark, so the gap \
         is reported nowhere and a diverged book still reads as certain"
    );
}

/// A restatement in the second half applies only to the prices after it.
///
/// `a_restatement_applies_to_the_prices_that_came_after_it` asserts this within
/// one call. It has to survive the cut too, or the exponent a price decodes at
/// becomes a function of where the caller happened to cut.
#[test]
fn a_restatement_in_the_second_half_applies_only_after_itself() {
    let all = build::<TopOfBook>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(&[quote(9_950)], PortRole::Mktdata, 100),
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -4))],
            PortRole::Refdata,
            200,
        ),
        Group(&[quote(9_951)], PortRole::Mktdata, 300),
    ]);

    // Cut so that the restatement is separated from the price it applies to.
    // Cutting between the two pairs instead would leave each half carrying its
    // own definition, and a case that passes with or without the state across
    // the cut has documented the tree rather than tested it.
    assert_split_is_a_no_op(&all, 3, MAGIC_TOB);

    let kept = split_kept(&all, 3, MAGIC_TOB);
    let exps: Vec<i8> = kept.event.iter().map(|r| r.price_exp).collect();
    assert_eq!(
        exps,
        vec![-2, -4],
        "the price before the restatement keeps the old scale and the one after takes the new"
    );

    let today = split_today(&all, 3, MAGIC_TOB);
    assert_eq!(
        today.refused.unresolved_instrument, 1,
        "today the second half's quote is refused: both definitions arrived in a call \
         that ended, and the restatement went with it"
    );
    let today_exps: Vec<i8> = today.event.iter().map(|r| r.price_exp).collect();
    assert_eq!(
        today_exps,
        vec![-2],
        "so the restatement applies to nothing at all, rather than to the price after it"
    );
}

/// One call into the new entry point over fresh state, ended, *is* the archive
/// path.
///
/// `derive_events` is written as exactly that, so this asserts the two are one
/// path rather than two that agree. The fixture strands an incomplete cycle so
/// that the book's counters are non-zero and the comparison covers them: with
/// the delta subtracted in one place and the close added in another, a figure
/// reported twice or not at all is what this would catch.
#[test]
fn the_archive_path_is_the_new_entry_point_over_fresh_state() {
    let all = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        // Two levels promised and one sent: the cycle is refused rather than
        // applied, which is a count on the book.
        Group(
            &[
                snapshot_begin(2),
                snapshot_level(SIDE_BID, 9_950, 12),
                snapshot_end(),
            ],
            PortRole::Snapshot,
            100,
        ),
        Group(&[level(SIDE_BID, 9_000, 1, 4)], PortRole::Mktdata, 200),
    ]);

    let archive = whole(&all, MAGIC_MBP);

    let id = identity();
    let mut state = DerivationState::new();
    let once = derive_events_into(
        &mut state,
        &mut DatagramLog::new(all.clone()),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    let ended = merge(once, DerivedEvents::default(), state.close_object());

    assert_eq!(ended.event, archive.event);
    assert_eq!(ended.book_top, archive.book_top);
    assert_eq!(ended.instrument, archive.instrument);
    assert_eq!(ended.refused, archive.refused);
    assert_eq!(ended.book_refused, archive.book_refused);
    assert_eq!(
        archive.book_refused.incomplete_cycle, 1,
        "the fixture is meant to strand a cycle, or this compares two zeros"
    );
}

/// `datagram_index` numbers a position in the derivation, not in the call.
///
/// The field is a position in what the `Source` yielded, and each call absorbs
/// into a fresh `WireCapture` whose counter starts at zero. Left there, the same
/// datagram would carry one number derived whole and another derived after a
/// cut, and nothing in the rows would explain the difference.
#[test]
fn the_datagram_index_continues_across_a_call() {
    let all = build::<TopOfBook>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(&[quote(9_950)], PortRole::Mktdata, 100),
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            200,
        ),
        Group(&[quote(9_951)], PortRole::Mktdata, 300),
    ]);

    let whole_at: Vec<u64> = whole(&all, MAGIC_TOB)
        .event
        .iter()
        .map(|r| r.datagram_index)
        .collect();
    assert_eq!(whole_at, vec![1, 3], "the two quotes sit at 1 and 3");

    let kept_at: Vec<u64> = split_kept(&all, 2, MAGIC_TOB)
        .event
        .iter()
        .map(|r| r.datagram_index)
        .collect();
    assert_eq!(
        kept_at, whole_at,
        "the second half's quote is the same datagram and carries the same number"
    );

    let today_at: Vec<u64> = split_today(&all, 2, MAGIC_TOB)
        .event
        .iter()
        .map(|r| r.datagram_index)
        .collect();
    assert_eq!(
        today_at,
        vec![1, 1],
        "today the second call numbers from zero again, so two datagrams share a number"
    );
}

/// `book_refused` is this call's own, and a caller may sum its windows.
///
/// `Book::refused` is a running total for the life of the book. Reported raw, a
/// caller that sums it per window double-counts every earlier window, and
/// nothing in the result would say so — every other counter on `DerivedEvents`
/// is per call.
#[test]
fn book_refused_does_not_re_report_an_earlier_windows_refusal() {
    let first = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(
            &[
                snapshot_begin(2),
                snapshot_level(SIDE_BID, 9_950, 12),
                snapshot_end(),
            ],
            PortRole::Snapshot,
            100,
        ),
    ]);
    let second = build::<MarketByPrice>(&[Group(
        &[level(SIDE_BID, 9_000, 1, 4)],
        PortRole::Mktdata,
        200,
    )]);

    let id = identity();
    let mut state = DerivationState::new();
    let a = derive_events_into(
        &mut state,
        &mut DatagramLog::new(first),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    let b = derive_events_into(
        &mut state,
        &mut DatagramLog::new(second),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");

    assert_eq!(
        a.book_refused.incomplete_cycle, 1,
        "the first window refused the cycle it was given"
    );
    assert_eq!(
        b.book_refused,
        BookRefused::default(),
        "and the second window refused nothing of its own, so it reports nothing"
    );
    assert_eq!(
        state.close_object(),
        BookRefused::default(),
        "nor did the derivation strand anything when it ended"
    );
}

/// A log that yields datagrams and then fails, so a tear can be asserted about.
struct TornLog {
    datagrams: Vec<OwnedDatagram>,
    at: usize,
    tear_after: usize,
}

impl Source for TornLog {
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError> {
        if self.at == self.tear_after {
            return Err(SourceError::MalformedArchive(
                "the fixture tears here".to_owned(),
            ));
        }
        let Some(datagram) = self.datagrams.get(self.at) else {
            return Ok(None);
        };
        self.at += 1;
        Ok(Some(datagram.as_recorded()))
    }
}

fn channel() -> Channel {
    Channel {
        source_addr: PRIMARY_SOURCE,
        channel_id: CHANNEL_ID,
    }
}

/// A window whose source tears folds nothing, and the state is still usable.
///
/// The absorb is the only fallible step and it completes before the fold
/// begins, so a call that fails cannot have folded half a window into state
/// that outlives it. That is worth an assertion rather than a comment, because
/// the opposite belief — that a failed call left the state half-advanced — is
/// the one that would have a caller discard it, pay the tear's datagrams *and*
/// re-pay the first-window refusals this entry point exists to remove.
#[test]
fn a_source_that_tears_folds_nothing_and_leaves_the_state_usable() {
    let all = build::<TopOfBook>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(&[quote(9_950)], PortRole::Mktdata, 100),
        Group(&[quote(9_951)], PortRole::Mktdata, 200),
    ]);
    let id = identity();
    let mut state = DerivationState::new();

    // One clean window first, so the state holds something worth not losing.
    let clean = derive_events_into(
        &mut state,
        &mut DatagramLog::new(all[..2].to_vec()),
        &input(&id, MAGIC_TOB),
    )
    .expect("the log does not fail");
    assert_eq!(clean.event.len(), 1, "the definition resolved the quote");
    assert_eq!(
        state.table().defined_count(channel()),
        1,
        "and the table a caller can now read says so"
    );

    // A window whose source tears after yielding one datagram.
    let mut torn = TornLog {
        datagrams: all[2..].to_vec(),
        at: 0,
        tear_after: 1,
    };
    derive_events_into(&mut state, &mut torn, &input(&id, MAGIC_TOB))
        .expect_err("the source tore before it was exhausted");

    assert_eq!(
        state.table().defined_count(channel()),
        1,
        "the torn call folded nothing into the state"
    );

    // The next window folds as though the torn call had not happened: its quote
    // still resolves against the first window's definition, and the datagram
    // base did not advance over datagrams no row was derived from.
    let after = derive_events_into(
        &mut state,
        &mut DatagramLog::new(all[2..].to_vec()),
        &input(&id, MAGIC_TOB),
    )
    .expect("the log does not fail");
    assert_eq!(
        after.event.len(),
        1,
        "the state survived the tear and still resolves"
    );
    assert_eq!(
        after.event[0].datagram_index, 2,
        "numbered from the two datagrams that were actually folded"
    );
}

/// The reference data is readable per window, which is what says the state is
/// doing its job.
///
/// A window that refused nothing and a window that carried no prices look the
/// same from the rows, so `defined_count` is the only thing that distinguishes
/// *the table is populated* from *there was nothing to resolve*. It is also the
/// measurement the phase hypothesis needs, and it has to be reachable before
/// anyone can take it.
#[test]
fn the_reference_data_is_readable_per_window() {
    let id = identity();
    let mut state = DerivationState::new();

    assert_eq!(
        state.table().defined_count(channel()),
        0,
        "nothing defined before anything is folded"
    );
    assert_eq!(state.table().era(channel()), None, "and no era either");

    let first = build::<TopOfBook>(&[Group(
        &[Msg::Definition(definition(AAA, "AAA", -2))],
        PortRole::Refdata,
        10,
    )]);
    derive_events_into(
        &mut state,
        &mut DatagramLog::new(first),
        &input(&id, MAGIC_TOB),
    )
    .expect("the log does not fail");
    assert_eq!(state.table().defined_count(channel()), 1);
    assert_eq!(state.table().era(channel()), Some(0));

    // A second instrument in a later window, and the count is the derivation's
    // rather than the window's — which is the whole point of it crossing.
    let second = build::<TopOfBook>(&[Group(
        &[Msg::Definition(definition(BBB, "BBB", -2))],
        PortRole::Refdata,
        20,
    )]);
    derive_events_into(
        &mut state,
        &mut DatagramLog::new(second),
        &input(&id, MAGIC_TOB),
    )
    .expect("the log does not fail");
    assert_eq!(
        state.table().defined_count(channel()),
        2,
        "the second window's definition joined the first window's"
    );
}

/// A level whose cycle ended in an *earlier* window is an orphan.
///
/// The attribution map outlives the call now, and `Book::snapshot_end` removes
/// from the book's own map on every end while nothing removed from this one. So
/// the two would disagree: the book would ignore such a level, having forgotten
/// the cycle, and the map would still attribute it and emit a row saying it
/// belongs to a cycle that is over. Pruning at the end of the call is also what
/// keeps the map to the cycles in flight rather than one entry per cycle for
/// the life of the derivation.
#[test]
fn a_level_after_its_cycle_ended_in_an_earlier_window_is_an_orphan() {
    let first = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(
            &[
                snapshot_begin(2),
                snapshot_level(SIDE_BID, 9_950, 12),
                snapshot_level(SIDE_ASK, 10_050, 7),
                snapshot_end(),
            ],
            PortRole::Snapshot,
            100,
        ),
    ]);
    // A stray level in the next window, carrying the finished cycle's id.
    let second = build::<MarketByPrice>(&[Group(
        &[snapshot_level(SIDE_BID, 9_949, 3)],
        PortRole::Snapshot,
        200,
    )]);

    let id = identity();
    let mut state = DerivationState::new();
    let a = derive_events_into(
        &mut state,
        &mut DatagramLog::new(first),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    assert_eq!(
        a.refused.orphan_snapshot_level, 0,
        "the first window's own levels belong to its open cycle"
    );

    let b = derive_events_into(
        &mut state,
        &mut DatagramLog::new(second),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    assert_eq!(
        b.refused.orphan_snapshot_level, 1,
        "the cycle ended a window ago, so nothing can attribute this level"
    );
    assert!(
        b.event.is_empty(),
        "and no row claims it belongs to a cycle that is over"
    );
}

/// A level after its cycle's end *within one call* is still attributed.
///
/// This is the archive path's behaviour and this change does not touch it,
/// which is why the map is pruned when the call ends rather than on the end
/// itself. Asserted against both entry points, because the whole claim about
/// the archive path is that they are one path.
#[test]
fn a_level_after_its_cycle_ended_in_the_same_call_is_still_attributed() {
    let all = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        Group(
            &[
                snapshot_begin(2),
                snapshot_level(SIDE_BID, 9_950, 12),
                snapshot_level(SIDE_ASK, 10_050, 7),
                snapshot_end(),
                // After its own end, in the same call.
                snapshot_level(SIDE_BID, 9_949, 3),
            ],
            PortRole::Snapshot,
            100,
        ),
    ]);

    let archive = whole(&all, MAGIC_MBP);
    assert_eq!(
        archive.refused.orphan_snapshot_level, 0,
        "the archive path attributes it, and this change must not alter that"
    );

    let id = identity();
    let mut state = DerivationState::new();
    let once = derive_events_into(
        &mut state,
        &mut DatagramLog::new(all),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    let ended = merge(once, DerivedEvents::default(), state.close_object());
    assert_eq!(ended.refused, archive.refused);
    assert_eq!(ended.event, archive.event);
}

/// Ending the derivation clears the cycles still in flight, in both maps.
///
/// `Book::close_object` counts them as having anchored nothing and clears its
/// own; the attribution map is the other half of that statement. Left behind, a
/// level arriving after the derivation ended would be attributed to a cycle the
/// book had already written off, and the row and the counter would disagree
/// about the same cycle.
#[test]
fn ending_the_derivation_clears_the_cycles_still_in_flight() {
    let first = build::<MarketByPrice>(&[
        Group(
            &[Msg::Definition(definition(AAA, "AAA", -2))],
            PortRole::Refdata,
            10,
        ),
        // A begin and one level, and no end: still in flight when the window
        // closes, so it is kept -- and written off when the derivation ends.
        Group(
            &[snapshot_begin(2), snapshot_level(SIDE_BID, 9_950, 12)],
            PortRole::Snapshot,
            100,
        ),
    ]);
    let after = build::<MarketByPrice>(&[Group(
        &[snapshot_level(SIDE_ASK, 10_050, 7)],
        PortRole::Snapshot,
        200,
    )]);

    let id = identity();
    let mut state = DerivationState::new();
    let a = derive_events_into(
        &mut state,
        &mut DatagramLog::new(first),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    assert_eq!(
        a.refused.orphan_snapshot_level, 0,
        "the cycle is in flight, so its level is attributed"
    );

    let closed = state.close_object();
    assert_eq!(
        closed.unclosed_cycle, 1,
        "the cycle was still open when the derivation ended, so it anchored nothing"
    );

    let b = derive_events_into(
        &mut state,
        &mut DatagramLog::new(after),
        &input(&id, MAGIC_MBP),
    )
    .expect("the log does not fail");
    assert_eq!(
        b.refused.orphan_snapshot_level, 1,
        "and the cycle the book wrote off cannot still attribute a level"
    );
}
