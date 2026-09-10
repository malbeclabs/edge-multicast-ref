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

use std::time::Duration;

use dz_adapter_core::{Desync, EventSink, Presence, Side};
use dz_edge_mbp::{BookClear, LevelUpdate};
use dz_edge_refdata::ManifestSummary;
use dz_edge_tob::Trade;
use dz_publisher_runtime::Exit;
use harness::{
    depth_feed, harness, harness_both, harness_two_shards, harness_two_shards_with_rotation,
    FakeAdapter, SHARD_A, SHARD_B, SOURCE_ID,
};

/// `0x14 InstrumentReset`, from the market-by-price specification's table.
const TYPE_INSTRUMENT_RESET: u8 = 0x14;

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
    // more than one feed in the family carries the same meaning in each, and
    // `Trade` is byte-for-byte identical between them. In one existing
    // publisher that obligation is held by a doc comment across two encoder
    // implementations, checked by hand. Here the trade is lowered **once** and
    // the same value is handed to both send paths, so the two feeds do not
    // carry two things that agree — they carry one thing.
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

#[test]
fn a_quote_reaches_its_own_shards_top_of_book_feed_and_no_other_sink() {
    // Written as an emptiness rather than as a presence. *Shard A's quote is
    // on shard A's mktdata port* passes against a publisher that packs every
    // message onto every channel, which is the failure this whole partition
    // exists to prevent; only *and on nothing else* can fail against it.
    //
    // The instrument is on the **second** shard, deliberately. On the first it
    // would be at index 0, and a publisher that had lost the routing
    // altogether and always reached for the first shard would pass.
    let mut h = harness_two_shards();
    let mut adapter = FakeAdapter::on_shards(&[("A-B", SHARD_A), ("C-D", SHARD_B)]);
    h.publisher.poll_listings(&mut adapter);
    let on_b = adapter.handles()[1];

    h.publisher.event(harness::quote(on_b, 1));

    let mut carried = Vec::new();
    for (index, shard) in h.shards.iter().enumerate() {
        for (spec, recorders) in [("top-of-book", &shard.tob), ("market-by-price", &shard.mbp)] {
            let recorders = recorders.as_ref().expect("both specifications are carried");
            if recorders.mktdata.type_ids().contains(&0x03) {
                carried.push((index, spec));
            }
        }
    }
    assert_eq!(
        carried,
        [(1, "top-of-book")],
        "a quote must reach the top-of-book feed of the shard its instrument was admitted to, and \
         no other channel instance"
    );
    assert_eq!(h.publisher.unroutable(), 0);
}

#[test]
fn a_reset_is_anchored_at_its_own_shards_sequence() {
    // The sharpest of the failures the partition prevents, and the only one
    // that is a wrong answer rather than a slow one: a subscriber records the
    // anchor as the minimum `Anchor Seq` it will accept, and compares it
    // against the numbers it has seen on its own channel. A number from
    // another channel instance's series is one it will wait behind forever.
    //
    // The two shards' sequences are deliberately driven apart first, so the
    // assertion cannot pass by both being the same number.
    let mut h = harness_two_shards();
    let mut adapter = FakeAdapter::on_shards(&[("A-B", SHARD_A), ("C-D", SHARD_B)]);
    h.publisher.poll_listings(&mut adapter);
    let on_a = adapter.handles()[0];
    let on_b = adapter.handles()[1];

    // Three levels on shard A and one on shard B, so the two channels are at
    // different points in their own series.
    for source_ts_ns in 1..=3 {
        h.publisher.event(harness::bid_level(on_a, source_ts_ns));
    }
    h.publisher.event(harness::bid_level(on_b, 4));

    let a = h.shards[0].mbp.as_ref().expect("shard A carries depth");
    let b = h.shards[1].mbp.as_ref().expect("shard B carries depth");
    assert_ne!(
        a.mktdata.headers().last().map(|(sequence, _)| *sequence),
        b.mktdata.headers().last().map(|(sequence, _)| *sequence),
        "the two channels must be at different sequence numbers for this test to mean anything"
    );

    h.publisher.desynchronised(on_b, Desync::UpstreamGap);

    let headers = b.mktdata.headers();
    let messages = b.mktdata.messages();
    let position = messages
        .iter()
        .position(|(type_id, _)| *type_id == TYPE_INSTRUMENT_RESET)
        .expect("the reset reached shard B's market-data port");
    let (sequence, _) = headers[position];
    let anchor = u64::from_le_bytes(
        messages[position].1[12..20]
            .try_into()
            .expect("eight bytes"),
    );
    assert_eq!(
        anchor, sequence,
        "the anchor must be the number of the datagram that carried it, on the reset instrument's \
         own channel"
    );
    assert!(
        !a.mktdata.type_ids().contains(&TYPE_INSTRUMENT_RESET),
        "shard A's channel was told about a reset on an instrument it does not carry"
    );
}

#[test]
fn every_channel_instances_final_manifest_carries_its_own_shards_published_set() {
    // The two shards hold different published sets, because equal ones would
    // let a process-wide `Instrument Count` and a process-wide `Manifest Seq`
    // pass this unnoticed.
    let mut h = harness_two_shards();
    let mut adapter = FakeAdapter::on_shards(&[
        ("A-B", SHARD_A),
        ("C-D", SHARD_B),
        ("E-F", SHARD_B),
        ("G-H", SHARD_B),
    ]);
    h.publisher.poll_listings(&mut adapter);

    h.publisher.shut_down(Exit::Signal);

    let mut counts = Vec::new();
    for shard in &h.shards {
        for recorders in [&shard.tob, &shard.mbp] {
            let recorders = recorders.as_ref().expect("both specifications are carried");
            // Every channel instance ends the same way: the final manifest on
            // refdata and `EndOfSession` last on mktdata. One shard's teardown
            // is not the process's.
            assert_eq!(
                recorders.mktdata.type_ids().last(),
                Some(&0x06),
                "a channel instance did not end with EndOfSession"
            );
            let last = recorders
                .refdata
                .messages()
                .iter()
                .rev()
                .find(|(type_id, _)| *type_id == 0x07)
                .map(|(_, bytes)| ManifestSummary::decode(bytes).expect("composed"))
                .expect("a channel instance sent no final manifest");
            assert_eq!(last.valid, 0, "the final manifest must carry `Valid = 0`");
            counts.push((last.instrument_count, last.manifest_seq));
        }
    }
    assert_eq!(
        counts,
        [(1, 1), (1, 1), (3, 3), (3, 3)],
        "each channel instance's final manifest must describe its own shard's published set: one \
         instrument and one change on the first shard, three of each on the second"
    );
}

#[test]
fn a_shards_snapshot_rotation_serves_its_own_instruments_at_its_own_cycle() {
    // One shard publishes one instrument and the other three, over the same
    // configured cycle. Paced by the process's four the smaller shard would
    // wait four ticks for its one book; served from the shared slots without
    // the membership check, either rotation would snapshot the other's
    // instruments onto its own channel.
    let mut h = harness_two_shards_with_rotation(Duration::from_secs(4));
    let mut adapter = FakeAdapter::on_shards(&[
        ("A-B", SHARD_A),
        ("C-D", SHARD_B),
        ("E-F", SHARD_B),
        ("G-H", SHARD_B),
    ])
    .with_book(&[(Side::Bid, "100.00", "5.000")]);
    h.publisher.poll_listings(&mut adapter);

    // The first call of each rotation schedules rather than snapshots, so the
    // first pass is asked for and returns nothing.
    assert!(h.publisher.periodic_snapshot(&adapter).is_none());
    // Shard A's tick is the cycle over its one instrument: four seconds. Shard
    // B's is the cycle over its three, which is shorter, so B falls due first
    // and A does not until its own tick has elapsed.
    h.clock.advance(Duration::from_secs(2));
    assert!(
        h.publisher.periodic_snapshot(&adapter).is_some(),
        "the larger shard's tick is its cycle over its own three instruments"
    );
    let a = h.shards[0].mbp.as_ref().expect("shard A carries depth");
    let b = h.shards[1].mbp.as_ref().expect("shard B carries depth");
    assert!(
        a.snapshot
            .as_ref()
            .expect("a snapshot port")
            .datagrams()
            .is_empty(),
        "the smaller shard is paced by the one instrument it publishes, not by the four the \
         process does"
    );
    assert!(!b
        .snapshot
        .as_ref()
        .expect("a snapshot port")
        .datagrams()
        .is_empty());

    // And past the smaller shard's own tick, its one instrument is served -
    // on its own channel.
    h.clock.advance(Duration::from_secs(4));
    for _ in 0..2 {
        let _ = h.publisher.periodic_snapshot(&adapter);
    }
    assert!(
        !a.snapshot
            .as_ref()
            .expect("a snapshot port")
            .datagrams()
            .is_empty(),
        "the smaller shard's rotation never reached its own instrument"
    );
}

/// Every shard's cycle achievable on its own, and not together.
///
/// The rotation divisor is per shard, which is right. What that stopped
/// detecting is that the *serving* rate is not: the tick body takes at most one
/// periodic snapshot, so N shards draw on one budget of one per runtime tick and
/// the demand adds up while the supply does not.
///
/// The fixture is the case the per-shard reading of the ceiling calls fine.
/// Thirty milliseconds over two instruments is one snapshot every fifteen on
/// each shard — comfortably above the runtime's own ten-millisecond tick, so
/// neither shard's arithmetic is breached. Together they want one every seven
/// and a half milliseconds out of a process that can serve one every ten, so
/// both channels lap at four fifths of the rate their key states, and until
/// this count nothing said so: each rotation is honouring its own arithmetic
/// and every datagram counter keeps moving.
#[test]
fn cycles_that_are_achievable_per_shard_and_not_together_are_counted() {
    let mut h = harness_two_shards_with_rotation(Duration::from_millis(30));
    let mut adapter = FakeAdapter::on_shards(&[
        ("A-B", SHARD_A),
        ("C-D", SHARD_A),
        ("E-F", SHARD_B),
        ("G-H", SHARD_B),
    ]);
    assert!(h.publisher.poll_listings(&mut adapter));
    assert_eq!(
        h.publisher.snapshot_schedule_overruns(),
        0,
        "no tick has run yet, so there is nothing to have been behind on"
    );

    let _ = h.publisher.tick();
    assert_eq!(
        h.publisher.snapshot_schedule_overruns(),
        1,
        "two shards each asking for two thirds of the process is not achievable, and the \
         per-shard arithmetic of both of them is comfortable"
    );
    let _ = h.publisher.tick();
    assert_eq!(
        h.publisher.snapshot_schedule_overruns(),
        2,
        "the count is per tick, because the shortfall is per tick"
    );
}

/// The control: the same two shards on a cycle the process can actually serve.
///
/// Without this the count above passes against a publisher that increments on
/// every tick regardless, which is a counter that says nothing.
#[test]
fn cycles_the_process_can_serve_are_not_counted() {
    let mut h = harness_two_shards_with_rotation(Duration::from_millis(200));
    let mut adapter = FakeAdapter::on_shards(&[
        ("A-B", SHARD_A),
        ("C-D", SHARD_A),
        ("E-F", SHARD_B),
        ("G-H", SHARD_B),
    ]);
    assert!(h.publisher.poll_listings(&mut adapter));

    for _ in 0..4 {
        let _ = h.publisher.tick();
    }
    assert_eq!(
        h.publisher.snapshot_schedule_overruns(),
        0,
        "one snapshot every hundred milliseconds on each of two shards is a fifth of a process \
         that serves one every ten"
    );
}
