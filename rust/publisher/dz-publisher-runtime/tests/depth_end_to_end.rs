//! The depth path, end to end: events in, datagrams out.
//!
//! The same shape as `end_to_end.rs` and for the same reason — a fake adapter's
//! events reach a fake `DatagramSink`, and what is asserted is what a
//! subscriber would decode rather than what the runtime believes it wrote.
//!
//! **Every expected value here is transcribed by hand**, from the
//! market-by-price specification's own tables and from the arithmetic the
//! instrument's exponents imply. The `Action` byte most of all: it is the field
//! this whole boundary was shaped around, an encoder numbering its table from
//! `New` instead of `Unknown` reached live traffic and emitted every removal as
//! a change carrying zero, and it is *self-consistent* — invisible to any test
//! that encodes and then decodes against the same constants. The specification's
//! own conformance subscriber does not check this feed's `Action` either: it has
//! 32 rules for market-by-price against 68 for market-by-order, and the enum
//! ranges on `Side` and `Action` are registered market-by-order-only. So the
//! literals below are the only independent control there is.

mod harness;

use dz_adapter_core::{EventSink, Presence, Side};
use dz_edge_mbp::{BookClear, LevelUpdate};
use dz_edge_tob::Trade;
use dz_publisher_runtime::Exit;
use harness::{depth_feed, harness, harness_both, FakeAdapter, SOURCE_ID};

// The wire values, transcribed from the market-by-price specification's own
// tables. `dz-edge-mbp` exports each as a constant; the literals are written out
// here so that a failure means the encoder disagrees with the specification
// rather than that it agrees with itself.
const SIDE_BID: u8 = 0;
const SIDE_ASK: u8 = 1;
const ACTION_UNKNOWN: u8 = 0;
const ACTION_NEW: u8 = 1;
const ACTION_CHANGE: u8 = 2;
const ACTION_DELETE: u8 = 3;
/// `Order Count` and `Level Index` absent. **The opposite value from
/// top-of-book's `Source Count`**, where zero means unavailable — two
/// specifications answering one question with opposite values.
const U16_UNAVAILABLE: u16 = 0xFFFF;
const CLEAR_BID: u8 = 0;
const SCOPE_ENTIRE_SIDE: u8 = 0;

/// Decode every `0x40 LevelUpdate` the mktdata port carried, in order.
fn levels(recorder: &harness::Recorder) -> Vec<LevelUpdate> {
    recorder
        .messages()
        .iter()
        .filter(|(type_id, _)| *type_id == 0x40)
        .map(|(_, bytes)| LevelUpdate::decode(bytes).expect("this publisher composed it"))
        .collect()
}

#[test]
fn a_fake_adapters_depth_events_reach_a_fake_datagram_sink_as_datagrams() {
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["A-B", "C-D"]);

    h.publisher.poll_listings(&mut adapter);
    let first = adapter.handles()[0];
    let second = adapter.handles()[1];

    // Three levels on the first instrument and one on the second, so the
    // per-instrument series can be shown to be *per instrument*.
    h.publisher.upstream_message("level");
    h.publisher.event(harness::level(
        first,
        1_700_000_000_000_000_001,
        Side::Bid,
        "100.25",
        "2.500",
        Presence::New,
    ));
    h.publisher.event(harness::level(
        first,
        1_700_000_000_000_000_002,
        Side::Ask,
        "100.75",
        "1.250",
        Presence::Change,
    ));
    h.publisher.event(harness::level(
        second,
        1_700_000_000_000_000_003,
        Side::Bid,
        "99.00",
        "10.000",
        Presence::Unknown,
    ));
    h.publisher.event(harness::level(
        first,
        1_700_000_000_000_000_004,
        Side::Bid,
        "100.25",
        "0",
        // The venue's hint says the level existed. It is ignored, and that is
        // the derivation: zero quantity is a removal and nothing else can be.
        Presence::Change,
    ));

    let decoded = levels(h.mktdata());
    assert_eq!(decoded.len(), 4);

    // ---- the first level ----
    let new_bid = decoded[0];
    // `Instrument ID`s are minted from 1 in offer order, which is the
    // reference-data owner's own rule.
    assert_eq!(new_bid.instrument_id, 1);
    assert_eq!(new_bid.source_id, SOURCE_ID);
    assert_eq!(new_bid.side, SIDE_BID);
    assert_eq!(new_bid.action, ACTION_NEW);
    // Scaled at the instrument's exponents: price -2, quantity -3.
    assert_eq!(new_bid.price_raw, 10_025);
    assert_eq!(new_bid.qty_raw, 2_500);
    assert_eq!(new_bid.timestamp_ns, 1_700_000_000_000_000_001);
    // The runtime's counter, stamped here and nowhere else, dense from 1.
    assert_eq!(new_bid.per_instrument_seq, 1);
    // Absent, and this feed's sentinel for it.
    assert_eq!(new_bid.order_count, U16_UNAVAILABLE);
    // A level's rank at emission is a property of the publisher's own book as
    // it emits, not of the venue's event, so it is absent rather than guessed.
    assert_eq!(new_bid.level_index, U16_UNAVAILABLE);
    // Informational and not expressible at the boundary; zero is each one's
    // defined default.
    assert_eq!(new_bid.update_reason, 0);
    assert_eq!(new_bid.level_flags, 0);

    // ---- the second: the other side, and the other non-zero action ----
    let changed_ask = decoded[1];
    assert_eq!(changed_ask.instrument_id, 1);
    assert_eq!(changed_ask.side, SIDE_ASK);
    assert_eq!(changed_ask.action, ACTION_CHANGE);
    assert_eq!(changed_ask.price_raw, 10_075);
    assert_eq!(changed_ask.qty_raw, 1_250);
    // The same series as the bid: `Per-Instrument Seq` is per *instrument*, not
    // per side. Both sides of one book are one stream of mutations, and their
    // relative order is significant.
    assert_eq!(changed_ask.per_instrument_seq, 2);

    // ---- the third: a different instrument, its own series ----
    let other = decoded[2];
    assert_eq!(other.instrument_id, 2);
    // `Unknown` is conformant, and it is the correct answer for an upstream
    // that does not distinguish an insertion from a change. Not a value to
    // avoid.
    assert_eq!(other.action, ACTION_UNKNOWN);
    assert_eq!(other.price_raw, 9_900);
    assert_eq!(other.qty_raw, 10_000);
    assert_eq!(
        other.per_instrument_seq, 1,
        "the second instrument's series began at 1, not at 3"
    );

    // ---- the fourth: the removal, and the whole reason this file exists ----
    let removal = decoded[3];
    assert_eq!(removal.instrument_id, 1);
    assert_eq!(
        removal.action, ACTION_DELETE,
        "a zero quantity is a removal and nothing else can be"
    );
    assert_eq!(removal.qty_raw, 0);
    // The price of the level being removed, which is how a subscriber knows
    // *which* level.
    assert_eq!(removal.price_raw, 10_025);
    assert_eq!(removal.per_instrument_seq, 3);

    // ---- and the datagrams themselves ----
    for (index, (sequence, era)) in h.mktdata().headers().iter().enumerate() {
        assert_eq!(*sequence, index as u64);
        // The depth feed's own era, which is not the top-of-book feed's: the
        // era store is keyed per feed, so a newly enabled feed cannot inherit
        // one from a feed that has published for months.
        assert_eq!(*era, harness::MBP_ERA);
    }
    for datagram in h.mktdata().datagrams() {
        assert!(datagram.len() <= 1232, "{} bytes", datagram.len());
    }
    assert_eq!(h.publisher.refusals().total(), 0);
    assert_eq!(h.publisher.unroutable(), 0);
}

#[test]
fn every_presence_and_quantity_pairing_reaches_the_action_the_table_states() {
    // A table over the exhausted `Presence` values against zero and non-zero
    // quantity, not a few examples. This derivation has no independent control
    // — the specification's conformance subscriber does not grade this feed's
    // `Action` — so the table is it.
    //
    // The two rows that matter are the removals: the specification forbids a
    // removal carrying any other action and forbids a removal action carrying
    // quantity, and both pairings are unreachable at this boundary rather than
    // merely refused. There is no `Presence` that can produce a removal, and no
    // removal that can carry a quantity.
    let cases = [
        ("0", Presence::Unknown, ACTION_DELETE),
        ("0", Presence::New, ACTION_DELETE),
        ("0", Presence::Change, ACTION_DELETE),
        ("2.500", Presence::Unknown, ACTION_UNKNOWN),
        ("2.500", Presence::New, ACTION_NEW),
        ("2.500", Presence::Change, ACTION_CHANGE),
    ];

    for (qty, presence, expected) in cases {
        let mut h = harness(depth_feed());
        let mut adapter = FakeAdapter::new(&["A-B"]);
        h.publisher.poll_listings(&mut adapter);
        let instrument = adapter.handles()[0];

        h.publisher.event(harness::level(
            instrument,
            1,
            Side::Bid,
            "100.25",
            qty,
            presence,
        ));

        let decoded = levels(h.mktdata());
        assert_eq!(decoded.len(), 1);
        assert_eq!(
            decoded[0].action, expected,
            "quantity {qty:?} with {presence:?} produced action {}, not {expected}",
            decoded[0].action
        );
        // And the pairing the specification forbids in the other direction: a
        // removal action never carries quantity.
        if decoded[0].action == ACTION_DELETE {
            assert_eq!(decoded[0].qty_raw, 0);
        } else {
            assert_ne!(decoded[0].qty_raw, 0);
        }
    }
}

#[test]
fn a_clear_takes_the_next_number_in_the_same_series_as_a_level() {
    // `LevelUpdate` and `BookClear` share one series, because both mutate the
    // book and their relative order is significant. A subscriber applying them
    // out of order has a book that never existed.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    h.publisher.event(harness::clear(instrument, 2));
    h.publisher.event(harness::bid_level(instrument, 3));

    let messages = h.mktdata().messages();
    let ordered: Vec<u8> = messages
        .iter()
        .filter(|(type_id, _)| *type_id == 0x40 || *type_id == 0x41)
        .map(|(type_id, _)| *type_id)
        .collect();
    assert_eq!(ordered, [0x40, 0x41, 0x40]);

    let clear: BookClear = messages
        .iter()
        .find(|(type_id, _)| *type_id == 0x41)
        .map(|(_, bytes)| BookClear::decode(bytes).expect("composed"))
        .expect("a clear was sent");
    assert_eq!(clear.instrument_id, 1);
    assert_eq!(clear.source_id, SOURCE_ID);
    assert_eq!(clear.clear_side, CLEAR_BID);
    assert_eq!(clear.scope, SCOPE_ENTIRE_SIDE);
    // A clear of an entire side is bounded by no price, so the field carries
    // the value the specification defines for absent.
    assert_eq!(clear.from_price_raw, 0);
    assert_eq!(clear.clear_reason, 0);
    assert_eq!(clear.timestamp_ns, 2);

    // The series: 1, then 2 for the clear, then 3.
    assert_eq!(clear.per_instrument_seq, 2);
    let decoded = levels(h.mktdata());
    assert_eq!(decoded[0].per_instrument_seq, 1);
    assert_eq!(decoded[1].per_instrument_seq, 3);
}

#[test]
fn a_trade_on_a_depth_channel_spends_no_per_instrument_number() {
    // `Trade` is not a book mutation and the message has no such field, so a
    // venue that publishes trades and levels on one channel must not have its
    // level series interrupted by them. Structural rather than remembered: both
    // lowerings delegate to one `trade::lower`, and a `Trade` has nowhere to
    // put a sequence number.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::bid_level(instrument, 1));
    h.publisher.event(harness::trade(instrument, 2));
    h.publisher.event(harness::trade(instrument, 3));
    h.publisher.event(harness::bid_level(instrument, 4));

    let type_ids: Vec<u8> = h
        .mktdata()
        .type_ids()
        .into_iter()
        .filter(|id| *id == 0x40 || *id == 0x04)
        .collect();
    assert_eq!(type_ids, [0x40, 0x04, 0x04, 0x40]);

    let decoded = levels(h.mktdata());
    assert_eq!(decoded[0].per_instrument_seq, 1);
    assert_eq!(
        decoded[1].per_instrument_seq, 2,
        "two trades between the levels consumed a number"
    );
}

#[test]
fn one_trade_reaches_both_feeds_as_the_same_bytes() {
    // The wire's cross-specification policy for `0x04`: a Type ID appearing in
    // more than one sibling feed carries the same meaning in each, and `Trade`
    // is byte-for-byte identical between them. In one existing publisher that
    // obligation is held by a doc comment across two encoder implementations,
    // checked by hand. Here the trade is lowered **once** and the same value is
    // handed to both send paths, so the two feeds do not carry two things that
    // agree — they carry one thing.
    let mut h = harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::trade(instrument, 77));

    let tob = h.tob.as_ref().expect("this publisher emits top-of-book");
    let mbp = h.mbp.as_ref().expect("and market-by-price");

    let trade_bytes = |recorder: &harness::Recorder| -> Vec<Vec<u8>> {
        recorder
            .messages()
            .into_iter()
            .filter(|(type_id, _)| *type_id == 0x04)
            .map(|(_, bytes)| bytes)
            .collect()
    };
    let on_tob = trade_bytes(&tob.mktdata);
    let on_mbp = trade_bytes(&mbp.mktdata);
    assert_eq!(on_tob.len(), 1, "the trade did not reach top-of-book");
    assert_eq!(on_mbp.len(), 1, "the trade did not reach market-by-price");
    assert_eq!(
        on_tob[0], on_mbp[0],
        "the same execution produced different bytes on two feeds"
    );

    // And it decodes to what the venue said, on both.
    for bytes in [&on_tob[0], &on_mbp[0]] {
        let trade = Trade::decode(bytes).expect("composed");
        assert_eq!(trade.instrument_id, 1);
        assert_eq!(trade.source_id, SOURCE_ID);
        assert_eq!(trade.trade_price, 10_050);
        assert_eq!(trade.trade_qty, 750);
        assert_eq!(trade.trade_id, 987_654);
        assert_eq!(trade.source_timestamp_ns, 77);
    }

    // The two feeds are separate channel instances in separate eras, which is
    // the other half of *the same bytes on two feeds*: identical messages,
    // independently numbered datagrams.
    assert_eq!(tob.mktdata.headers()[0].1, harness::TOB_ERA);
    assert_eq!(mbp.mktdata.headers()[0].1, harness::MBP_ERA);
}

#[test]
fn a_publisher_emitting_both_feeds_routes_each_event_to_the_feed_that_carries_it() {
    let mut h = harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.event(harness::quote(instrument, 1));
    h.publisher.event(harness::bid_level(instrument, 2));
    h.publisher.event(harness::clear(instrument, 3));

    let tob = h.tob.as_ref().expect("top-of-book");
    let mbp = h.mbp.as_ref().expect("market-by-price");
    // `0x03` on top-of-book and nowhere else; `0x40` and `0x41` on
    // market-by-price and nowhere else.
    assert!(tob.mktdata.type_ids().contains(&0x03));
    assert!(!tob.mktdata.type_ids().contains(&0x40));
    assert!(!tob.mktdata.type_ids().contains(&0x41));
    assert!(mbp.mktdata.type_ids().contains(&0x40));
    assert!(mbp.mktdata.type_ids().contains(&0x41));
    assert!(!mbp.mktdata.type_ids().contains(&0x03));
    assert_eq!(h.publisher.unroutable(), 0);
}

#[test]
fn one_registry_serves_both_feeds_and_each_refdata_port_carries_the_same_manifest() {
    // `Instrument ID` identity is the one thing there can only be one of, and
    // `Manifest Seq` describes the published set rather than a channel. So one
    // registry serves both feeds — and the manifest's own redundant
    // `Channel ID` is stamped by the builder from the datagram that frames it,
    // which is what makes one composed manifest truthful on both ports.
    use dz_edge_refdata::ManifestSummary;

    let mut h = harness_both();
    let mut adapter = FakeAdapter::new(&["A-B", "C-D"]);
    h.publisher.poll_listings(&mut adapter);
    let _ = h.publisher.tick();
    h.clock.advance(std::time::Duration::from_secs(20));
    let _ = h.publisher.tick();

    let manifests = |recorder: &harness::Recorder| -> Vec<ManifestSummary> {
        recorder
            .messages()
            .iter()
            .filter(|(type_id, _)| *type_id == 0x07)
            .map(|(_, bytes)| ManifestSummary::decode(bytes).expect("composed"))
            .collect()
    };
    let tob = h.tob.as_ref().expect("top-of-book");
    let mbp = h.mbp.as_ref().expect("market-by-price");
    let on_tob = manifests(&tob.refdata);
    let on_mbp = manifests(&mbp.refdata);
    assert!(!on_tob.is_empty() && !on_mbp.is_empty());

    // The same published set and the same manifest sequence...
    assert_eq!(on_tob[0].instrument_count, 2);
    assert_eq!(on_mbp[0].instrument_count, 2);
    assert_eq!(on_tob[0].manifest_seq, on_mbp[0].manifest_seq);
    // ...and each carries its own channel, stamped by the datagram that framed
    // it rather than by the one value the registry was configured with.
    assert_eq!(on_tob[0].channel_id, harness::CHANNEL_ID);
    assert_eq!(on_mbp[0].channel_id, harness::DEPTH_CHANNEL_ID);

    // Both feeds' definition cycles ran from one drained tick, so neither owes
    // the other's debt: the pacer is asked once per tick however many feeds are
    // enabled.
    let definitions = |recorder: &harness::Recorder| {
        recorder
            .type_ids()
            .into_iter()
            .filter(|id| *id == 0x02)
            .count()
    };
    assert_eq!(definitions(&tob.refdata), definitions(&mbp.refdata));
    assert!(definitions(&tob.refdata) > 0);
}

#[test]
fn shutting_down_a_depth_publisher_ends_every_feeds_mktdata_channel() {
    let mut h = harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    h.publisher
        .event(harness::bid_level(adapter.handles()[0], 1));

    h.publisher.shut_down(Exit::Signal);

    for recorders in [h.tob.as_ref().unwrap(), h.mbp.as_ref().unwrap()] {
        assert_eq!(
            recorders.mktdata.type_ids().last(),
            Some(&0x06),
            "a feed's mktdata channel did not end with EndOfSession"
        );
        assert_eq!(recorders.refdata.type_ids().last(), Some(&0x07));
    }
    // And nothing was sent on the snapshot port on the way down: a snapshot
    // describes a book a subscriber is about to be told has ended.
    assert!(h
        .mbp
        .as_ref()
        .unwrap()
        .snapshot
        .as_ref()
        .unwrap()
        .datagrams()
        .is_empty());
}
