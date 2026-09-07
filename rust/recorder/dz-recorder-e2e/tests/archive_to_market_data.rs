//! The whole chain again, and this time to the rows about instruments.
//!
//! `archive_to_rows` carries a stream from the encoder to the four transport
//! tables. This one carries a depth stream from the encoder to `event`,
//! `instrument` and `book_top`: the real `DatagramBuilder` frames it, the real
//! `ArchiveWriter` publishes it, the real deriver reads it back, and the
//! `FileSink` the loader's `--dry-run` uses writes the rows.
//!
//! **The comparison is against what was encoded.** Every price, quantity and
//! identifier asserted below is a value the fixture handed the builder, and the
//! message counts are read out of the datagrams at the offset the
//! specification's field table states. A derivation that agreed with itself
//! about a wrong layout would still fail here — which is why the wrong `Magic`
//! is exercised too.
//!
//! No socket, no privileges and no server. The half of this suite that needs a
//! column store is in `market_data_container.rs`, behind
//! `--features clickhouse-tests`.
#![forbid(unsafe_code)]

mod common;
mod depth;

use common::record_feed;
use depth::{
    depth_stream, derive, ANCHOR_ASK_PRICE, ANCHOR_ASK_QTY, ANCHOR_BID_PRICE, ANCHOR_BID_QTY,
    ANCHOR_SEQ, BETTER_ASK_PRICE, BETTER_ASK_QTY, BETTER_BID_PRICE, BETTER_BID_QTY,
    CUMULATIVE_VOLUME, DEPTH_ROLES, INSTRUMENT, LEVELS, PRICE_EXPONENT, QTY_EXPONENT, SNAPSHOT_ID,
    SOURCE_ID, SYMBOL, TRADE_ID,
};
use dz_edge_core::{Feed, PortRole};
use dz_edge_mbp::{MarketByPrice, ACTION_DELETE, MAGIC_MBP, SIDE_ASK, SIDE_BID};
use dz_edge_tob::MAGIC_TOB;
use dz_recorder_events::{DerivedEvents, Refused};
use dz_recorder_replay::OwnedDatagram;
use dz_recorder_rows::{
    FileSink, Grain, MessageTypeLabel, PortRoleLabel, RowBatch, RowSink, UncertainReason,
};

/// One instant for every sink call in this file.
///
/// The sinks take the clock as a parameter, so a test states it rather than
/// sleeping: what is under test is what a sink writes, never when it decides to.
const NOW: u64 = 1_700_000_000_000_000_000;

/// The `Message Count` byte, at the offset the specification's field table
/// states.
///
/// Read here rather than through anything under test, so a row count cannot be
/// satisfied by a derivation that agrees with itself about how many messages it
/// walked.
fn message_count(datagram: &OwnedDatagram) -> usize {
    datagram.payload[20] as usize
}

/// Every message that carries an event became one row that recovers it.
#[test]
fn every_encoded_message_becomes_a_row_that_recovers_it() {
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    let derived = derive(&recorded, MAGIC_MBP, false);

    // What was encoded, out of the datagrams: every message on the two roles
    // that carry events, less the levels this run does not persist. Reference
    // data is excluded because a definition is an instrument and not an event —
    // which is the mapping table's own split and not this test's.
    let encoded: usize = sent
        .iter()
        .filter(|dg| dg.role != PortRole::Refdata)
        .map(message_count)
        .sum();
    assert_eq!(derived.event.len(), encoded - LEVELS.len());
    assert_eq!(
        derived.refused,
        Refused::default(),
        "nothing was refused, so nothing below is asserted over a gap"
    );

    let types: Vec<MessageTypeLabel> = derived.event.iter().map(|e| e.message_type).collect();
    assert_eq!(
        types,
        vec![
            MessageTypeLabel::SnapshotBegin,
            MessageTypeLabel::SnapshotEnd,
            MessageTypeLabel::LevelUpdate,
            MessageTypeLabel::LevelUpdate,
            MessageTypeLabel::LevelUpdate,
            MessageTypeLabel::BookClear,
            MessageTypeLabel::Trade,
        ],
        "in archive order, across three port roles and two sequence spaces"
    );

    // The scale every price on these rows is read at came from a definition
    // that arrived on `refdata` — another port, another sequence space, and the
    // join the reference-data key exists to make possible.
    for row in &derived.event {
        assert_eq!(row.instrument_id, INSTRUMENT);
        assert_eq!(row.source_id, SOURCE_ID);
        assert_eq!(row.symbol, SYMBOL);
        assert_eq!(row.price_exp, PRICE_EXPONENT);
        assert_eq!(row.qty_exp, QTY_EXPONENT);
        assert_eq!(row.feed, MarketByPrice::NAME);
        assert_eq!(row.site, recorded.manifest.site);
        assert_eq!(row.object_sha256, recorded.manifest.sha256);
        assert_eq!(row.segment_seq, recorded.manifest.segment_seq);
    }

    let deltas: Vec<_> = derived
        .event
        .iter()
        .filter(|e| e.message_type == MessageTypeLabel::LevelUpdate)
        .collect();
    assert_eq!(deltas[0].price_raw, Some(BETTER_BID_PRICE));
    assert_eq!(deltas[0].qty_raw, Some(BETTER_BID_QTY));
    assert_eq!(deltas[0].side_raw, Some(SIDE_BID));
    assert_eq!(deltas[0].order_count, Some(3));
    assert_eq!(deltas[1].price_raw, Some(BETTER_ASK_PRICE));
    assert_eq!(deltas[1].side_raw, Some(SIDE_ASK));
    // Zero is a removal. It has to survive as a zero rather than becoming an
    // absent value on the way, or a level nobody removed stays in the book.
    assert_eq!(deltas[2].qty_raw, Some(0));
    assert_eq!(deltas[2].action_raw, Some(ACTION_DELETE));

    // A trade moves no book and is an event all the same.
    let trade = derived
        .event
        .iter()
        .find(|e| e.message_type == MessageTypeLabel::Trade)
        .expect("a trade row");
    assert_eq!(trade.trade_id, Some(TRADE_ID));
    assert_eq!(trade.cumulative_volume, Some(CUMULATIVE_VOLUME));
    // It shared its datagram with the clear, so the two are one sequence number
    // and one receive stamp apart by nothing but their position in it.
    let clear = derived
        .event
        .iter()
        .find(|e| e.message_type == MessageTypeLabel::BookClear)
        .expect("a clear row");
    assert_eq!(trade.sequence_number, clear.sequence_number);
    assert_eq!(trade.recv_ts, clear.recv_ts);
    assert_ne!(trade.message_index, clear.message_index);

    // And one instrument row, from the definition that was never an event.
    assert_eq!(derived.instrument.len(), 1);
    let instrument = &derived.instrument[0];
    assert_eq!(instrument.instrument_id, INSTRUMENT);
    assert_eq!(instrument.symbol, SYMBOL);
    assert_eq!(instrument.price_exp, PRICE_EXPONENT);
    assert_eq!(instrument.qty_exp, QTY_EXPONENT);
    assert_eq!(
        instrument.port_role,
        PortRoleLabel::Refdata,
        "reference data arrives on refdata, and the row says so rather than \
         leaving a reader to discover it"
    );
    assert_eq!(instrument.declared_count, Some(1), "the summary was valid");
}

/// A cycle is a row whether or not its levels are.
///
/// The switch is about rows and never about state: the book consumes every
/// level either way, and `total_levels` against `levels_seen` is what keeps
/// *was the snapshot complete* answerable when the levels are not there to be
/// counted.
#[test]
fn a_cycle_is_visible_from_its_begin_and_end_with_the_levels_unpersisted() {
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);

    let consumed = derive(&recorded, MAGIC_MBP, false);
    let persisted = derive(&recorded, MAGIC_MBP, true);

    assert_eq!(levels(&consumed), 0);
    assert_eq!(levels(&persisted), LEVELS.len());
    assert_eq!(
        persisted.event.len() - consumed.event.len(),
        LEVELS.len(),
        "the levels are the whole of the difference"
    );

    for derived in [&consumed, &persisted] {
        let begin = derived
            .event
            .iter()
            .find(|e| e.message_type == MessageTypeLabel::SnapshotBegin)
            .expect("a begin row");
        let end = derived
            .event
            .iter()
            .find(|e| e.message_type == MessageTypeLabel::SnapshotEnd)
            .expect("an end row");
        assert_eq!(begin.snapshot_id, Some(SNAPSHOT_ID));
        assert_eq!(begin.anchor_seq, Some(ANCHOR_SEQ));
        assert_eq!(begin.total_levels, Some(LEVELS.len() as u32));
        assert_eq!(
            end.levels_seen,
            Some(LEVELS.len() as u32),
            "the count the deriver observed, against the count the begin promised"
        );
    }

    // The persisted levels carry the prices the fixture encoded, and inherit
    // the instrument from the begin their `snapshot_id` ties them to — a level
    // carries none of its own.
    for (row, (price, qty, side)) in persisted
        .event
        .iter()
        .filter(|e| e.message_type == MessageTypeLabel::SnapshotLevel)
        .zip(LEVELS)
    {
        assert_eq!(row.instrument_id, INSTRUMENT, "inherited from the begin");
        assert_eq!(row.price_raw, Some(price));
        assert_eq!(row.qty_raw, Some(qty));
        assert_eq!(row.side_raw, Some(side));
        assert_eq!(row.snapshot_id, Some(SNAPSHOT_ID));
    }

    // And the book is the same book either way, which is the one thing
    // consuming every level is for: a level skipped before the book saw it
    // leaves a cycle that never completes, so nothing ever anchors.
    assert_eq!(consumed.book_top, persisted.book_top);
    assert!(
        consumed.book_top.iter().any(|row| row.from_anchor == 1),
        "no top came from applying the snapshot"
    );
}

fn levels(derived: &DerivedEvents) -> usize {
    derived
        .event
        .iter()
        .filter(|e| e.message_type == MessageTypeLabel::SnapshotLevel)
        .count()
}

/// The book states are the prices that were encoded, and the anchor is marked.
#[test]
fn the_book_tops_are_the_prices_that_were_encoded() {
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    let derived = derive(&recorded, MAGIC_MBP, false);

    assert!(!derived.book_top.is_empty(), "a depth feed builds a book");
    for row in &derived.book_top {
        assert_eq!(row.instrument_id, INSTRUMENT);
        assert_eq!(row.symbol, SYMBOL);
        assert_eq!(row.price_exp, PRICE_EXPONENT);
        // The cycle anchored the book before the first delta, and nothing here
        // is lost or reset, so every state is one that can be believed.
        assert_eq!(row.book_certain, 1, "{row:?}");
        assert_eq!(row.uncertain_since, None);
        assert_eq!(row.uncertain_reason, UncertainReason::None);
        // `site` names a recorder; this names one observation of one book, and
        // a race is one `state_key` seen at more than one of them.
        assert_eq!(
            row.observation,
            format!("{}/{}", recorded.manifest.site, recorded.manifest.recorder)
        );
    }

    // Exactly one state came from applying a snapshot, and it is the book the
    // cycle's own levels described. Such a row is a starting state and never an
    // observation in a race, because the archive records when a snapshot was
    // published and not when it was asked for.
    let anchored: Vec<_> = derived
        .book_top
        .iter()
        .filter(|row| row.from_anchor == 1)
        .collect();
    assert_eq!(anchored.len(), 1, "{anchored:?}");
    assert_eq!(anchored[0].bid_px_raw, Some(ANCHOR_BID_PRICE));
    assert_eq!(anchored[0].bid_qty_raw, Some(ANCHOR_BID_QTY));
    assert_eq!(anchored[0].ask_px_raw, Some(ANCHOR_ASK_PRICE));
    assert_eq!(anchored[0].ask_qty_raw, Some(ANCHOR_ASK_QTY));

    // And the last state is what the deltas left: the clear took the whole bid
    // side, and an absent side is absent rather than zero — top of book states
    // *unavailable* with a zero, and the two must not collapse.
    let last = derived.book_top.last().expect("a last state");
    assert_eq!(last.from_anchor, 0);
    assert_eq!(last.bid_px_raw, None, "the clear took the side");
    assert_eq!(last.bid_qty_raw, None);
    assert_eq!(last.ask_px_raw, Some(BETTER_ASK_PRICE));
    assert_eq!(last.ask_qty_raw, Some(BETTER_ASK_QTY));
    assert_ne!(
        last.state_key, anchored[0].state_key,
        "two different books hash two ways"
    );
}

/// The rows go out through the sink a `--dry-run` writes them through, and come
/// back out of the files it wrote.
///
/// Asserted through the file rather than off the struct because a sink that
/// counted a grain it did not write would satisfy every assertion above. It did:
/// `event`, `instrument` and `book_top` reached `Grain` and `RowBatch` without
/// reaching either sink's send, so `Written` counted rows that no file and no
/// insert ever held.
#[test]
fn the_rows_survive_the_sink_the_loader_writes_them_through() {
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    let derived = derive(&recorded, MAGIC_MBP, true);
    let expected: Vec<(Grain, usize)> = vec![
        (Grain::Event, derived.event.len()),
        (Grain::Instrument, derived.instrument.len()),
        (Grain::BookTop, derived.book_top.len()),
    ];

    let dir = tempfile::tempdir().expect("a temporary directory");
    let mut sink = FileSink::create(dir.path()).expect("the directory is writable");
    let batch = RowBatch {
        object_key: recorded.manifest.object_key.clone(),
        object_sha256: recorded.manifest.sha256.clone(),
        event: derived.event,
        instrument: derived.instrument,
        book_top: derived.book_top,
        ..RowBatch::default()
    };
    let accepted = sink.write_batch(batch, NOW).expect("the sink takes them");
    sink.flush(NOW).expect("flush");

    for (grain, count) in expected {
        assert!(count > 0, "{grain} had nothing to write");
        assert_eq!(accepted.accepted.rows(grain), count as u64, "{grain}");
        let text = std::fs::read_to_string(FileSink::path_in(dir.path(), grain))
            .unwrap_or_else(|e| panic!("{grain} was not written at all: {e}"));
        assert_eq!(
            text.lines().filter(|l| !l.is_empty()).count(),
            count,
            "{grain}: the sink counted rows it did not write"
        );
        // Every line is a row the column store's `JSONEachRow` body would carry.
        for line in text.lines().filter(|l| !l.is_empty()) {
            let row: serde_json::Value =
                serde_json::from_str(line).expect("one JSON object per line");
            assert_eq!(row["object_key"], recorded.manifest.object_key);
        }
    }
}

/// A datagram at another feed's `Magic` derives nothing rather than a wrong
/// layout.
///
/// The failure this rules out is the quiet one. The two feeds in this family
/// share a datagram header and differ in two bytes, so a walk that decoded at
/// the wrong `Magic` anyway would produce rows whose prices are other fields —
/// and every one of them would look like a price.
#[test]
fn the_wrong_magic_derives_nothing_rather_than_the_wrong_prices() {
    let sent = depth_stream();
    let recorded = record_feed(&sent, DEPTH_ROLES, MarketByPrice::NAME);
    assert_ne!(MAGIC_TOB, MarketByPrice::MAGIC, "two feeds, two delimiters");
    let derived = derive(&recorded, MAGIC_TOB, false);

    assert!(derived.event.is_empty(), "{:?}", derived.event);
    assert!(derived.instrument.is_empty());
    assert!(derived.book_top.is_empty());
    // And refused before a decoder, not after one: nothing was resolved
    // wrongly, because nothing was parsed at all.
    assert_eq!(derived.refused, Refused::default());
}
