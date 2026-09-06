//! The sizing measurement, over a window that holds a burst and a cycle.
//!
//! The number these assert is the one a deployment decision is made against, so
//! every test here is about the count being what it claims rather than about a
//! behaviour: a fixture of a known message mix produces a known ratio, and each
//! test names the way the count could be wrong and still look plausible.

mod common;

use common::{
    definition, pack, pack_batched, pack_from, DatagramLog, Msg, AAA, ACTION_NEW, BBB, CHANNEL_ID,
    PRIMARY_SOURCE, SIDE_BID, SOURCE_ID,
};
use dz_edge_core::{Heartbeat, PortRole};
use dz_edge_mbp::{
    LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel, MAGIC_MBP,
    U16_UNAVAILABLE,
};
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_events::{Channel, FeedSizing, Incomplete, Sizing};
use dz_recorder_replay::synthetic::SECOND_SOURCE;

const SNAPSHOT: u32 = 7;

fn primary() -> Channel {
    Channel {
        source_addr: PRIMARY_SOURCE,
        channel_id: CHANNEL_ID,
    }
}

fn measure(log: &mut DatagramLog) -> Sizing {
    Sizing::measure(log, MAGIC_MBP).expect("the log does not fail")
}

fn one(log: &mut DatagramLog) -> FeedSizing {
    *measure(log)
        .feed(primary())
        .expect("the primary channel was measured")
}

fn level(seq: u32) -> Msg {
    Msg::Level(LevelUpdate {
        instrument_id: AAA,
        source_id: SOURCE_ID,
        side: SIDE_BID,
        action: ACTION_NEW,
        per_instrument_seq: seq,
        price_raw: 100 + i64::from(seq),
        qty_raw: 5,
        timestamp_ns: 1_000_000_000 + u64::from(seq),
        order_count: U16_UNAVAILABLE,
        level_index: U16_UNAVAILABLE,
        update_reason: 0,
        level_flags: 0,
    })
}

fn levels(count: u32) -> Vec<Msg> {
    (0..count).map(level).collect()
}

fn heartbeats(count: usize) -> Vec<Msg> {
    (0..count)
        .map(|i| {
            Msg::Heartbeat(Heartbeat {
                channel_id: CHANNEL_ID,
                timestamp_ns: 2_000_000_000 + i as u64,
            })
        })
        .collect()
}

/// A complete cycle: a begin, its levels, and the end that closes it.
fn cycle(total_levels: u32) -> Vec<Msg> {
    let mut out = vec![Msg::SnapshotBegin(SnapshotBegin {
        instrument_id: AAA,
        anchor_seq: 4_242,
        total_levels,
        snapshot_id: SNAPSHOT,
        last_instrument_seq: 900,
        timestamp_ns: 1_000_000_010,
        depth_bound: 10,
    })];
    out.extend((0..total_levels).map(|i| {
        Msg::SnapshotLevel(SnapshotLevel {
            snapshot_id: SNAPSHOT,
            price_raw: 100 + i64::from(i),
            qty_raw: 3,
            order_count: U16_UNAVAILABLE,
            side: SIDE_BID,
            level_flags: 0,
        })
    }));
    out.push(Msg::SnapshotEnd(SnapshotEnd {
        instrument_id: AAA,
        anchor_seq: 4_242,
        snapshot_id: SNAPSHOT,
    }));
    out
}

/// The window the design asks for: reference data, a quiet stretch, a burst and
/// a full snapshot cycle, on one feed across its three port roles.
///
/// 2 refdata datagrams + 3 heartbeat datagrams + 3 burst datagrams of four
/// levels each + 7 cycle datagrams = 15, carrying 2 + 0 + 12 + 7 = 21 messages.
fn window() -> DatagramLog {
    let mut log = DatagramLog::default();
    log.extend(pack::<MarketByPrice>(
        &[
            Msg::Definition(definition(AAA, "AAA", -2)),
            Msg::Definition(definition(BBB, "BBB", -2)),
        ],
        PortRole::Refdata,
        0,
    ));
    log.extend(pack::<MarketByPrice>(&heartbeats(3), PortRole::Mktdata, 0));
    log.extend(pack_batched::<MarketByPrice>(
        &levels(12),
        PortRole::Mktdata,
        3,
        4,
    ));
    log.extend(pack::<MarketByPrice>(&cycle(5), PortRole::Snapshot, 0));
    log
}

#[test]
fn a_known_mix_measures_the_known_ratio() {
    let feed = one(&mut window());

    assert_eq!(feed.datagrams, 15);
    assert_eq!(feed.messages(), 21);
    assert_eq!(feed.market_messages, 12);
    assert_eq!(feed.state_messages, 7);
    assert_eq!(feed.reference_messages, 2);
    assert_eq!(feed.datagrams_with_messages, 12);
    assert_eq!(feed.peak_messages_per_datagram, 4);
    assert_eq!(feed.snapshot_cycles, 1);
    assert!((feed.messages_per_datagram().expect("datagrams were walked") - 1.4).abs() < 1e-9);
    assert!(
        feed.incomplete().is_empty(),
        "this window holds a burst and a cycle: {:?}",
        feed.incomplete()
    );
}

/// The report is the deliverable — the number has to reach a person's eyes.
#[test]
fn the_report_states_the_ratio_and_the_channel_it_is_for() {
    let report = measure(&mut window()).to_string();

    assert!(report.contains("192.0.2.10/1"), "{report}");
    assert!(report.contains("1.40"), "{report}");
    assert!(
        report.contains("3 datagram(s) carried no message"),
        "{report}"
    );
}

/// The mutant: a datagram carrying several messages counted as one message.
///
/// Packing is the *only* thing that differs between the two windows here. The
/// numerator has to be blind to it and the denominator has to see it, and a
/// count that read the datagram instead of walking into it would report four
/// times too few rows for exactly the feed that batches hardest.
#[test]
fn a_datagram_of_several_messages_is_not_counted_as_one() {
    let mut one_each = DatagramLog::new(pack::<MarketByPrice>(&levels(12), PortRole::Mktdata, 0));
    let mut batched = DatagramLog::new(pack_batched::<MarketByPrice>(
        &levels(12),
        PortRole::Mktdata,
        0,
        4,
    ));

    let spread = one(&mut one_each);
    let packed = one(&mut batched);

    assert_eq!(spread.messages(), 12);
    assert_eq!(packed.messages(), 12, "the same twelve messages either way");
    assert_eq!(spread.datagrams, 12);
    assert_eq!(packed.datagrams, 3);
    assert_eq!(spread.peak_messages_per_datagram, 1);
    assert_eq!(packed.peak_messages_per_datagram, 4);
    assert!((spread.messages_per_datagram().expect("walked") - 1.0).abs() < 1e-9);
    assert!((packed.messages_per_datagram().expect("walked") - 4.0).abs() < 1e-9);
}

/// The mutant: a denominator taken from the messages' own provenance.
///
/// A heartbeat datagram yields no message, so a denominator counted from what
/// the messages say they arrived in never sees it — and reports a channel that
/// is nine parts silence as though every datagram of it were a burst.
#[test]
fn a_datagram_that_carried_no_message_is_still_in_the_denominator() {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(&heartbeats(4), PortRole::Mktdata, 0));
    log.extend(pack_batched::<MarketByPrice>(
        &levels(4),
        PortRole::Mktdata,
        4,
        4,
    ));

    let feed = one(&mut log);

    assert_eq!(feed.datagrams, 5);
    assert_eq!(feed.datagrams_with_messages, 1);
    assert_eq!(feed.messages(), 4);
    assert!(
        (feed.messages_per_datagram().expect("walked") - 0.8).abs() < 1e-9,
        "four messages over five datagrams, not over one"
    );
}

/// A window that saw neither of the two things a ratio is taken for says so.
#[test]
fn a_quiet_window_refuses_to_be_decided_against() {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(&levels(3), PortRole::Mktdata, 0));

    let feed = one(&mut log);

    assert_eq!(feed.messages(), 3);
    assert_eq!(
        feed.incomplete(),
        vec![Incomplete::NoBurst, Incomplete::NoSnapshotCycle]
    );
    assert!(measure(&mut DatagramLog::new(pack::<MarketByPrice>(
        &levels(3),
        PortRole::Mktdata,
        0
    )))
    .to_string()
    .contains("not yet decidable"));
}

/// A cycle that began and did not end is not a cycle.
///
/// A window that started mid-cycle or ended inside one saw part of the peak, and
/// counting that as a cycle would let a window claim it had seen the largest run
/// the feed produces when it had seen the beginning of one.
#[test]
fn an_unclosed_cycle_does_not_count_as_a_cycle() {
    let mut opened = cycle(5);
    opened.pop();
    let mut log = DatagramLog::new(pack::<MarketByPrice>(&opened, PortRole::Snapshot, 0));

    let feed = one(&mut log);

    assert_eq!(feed.state_messages, 6);
    assert_eq!(feed.snapshot_cycles, 0);
    assert!(feed.incomplete().contains(&Incomplete::NoSnapshotCycle));
}

/// The mutant: keying the ratio on the channel instance.
///
/// A feed's messages are spread over three port roles, which are three channel
/// instances. Keyed on the instance, the definition cycle is divided by its own
/// datagrams and the prices are a separate feed entirely — three ratios, none of
/// which is the feed's.
#[test]
fn a_feed_is_one_channel_across_its_port_roles() {
    let sizing = measure(&mut window());

    assert_eq!(
        sizing.by_channel().len(),
        1,
        "three port roles, one feed: {:?}",
        sizing.by_channel().keys().collect::<Vec<_>>()
    );
}

/// Two publishers serving one `Channel ID` are two feeds, not one.
#[test]
fn a_second_source_address_is_a_second_feed() {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(&levels(4), PortRole::Mktdata, 0));
    log.extend(pack_from::<MarketByPrice>(
        &levels(2),
        PortRole::Mktdata,
        0,
        0,
        SECOND_SOURCE,
    ));

    let sizing = measure(&mut log);

    assert_eq!(sizing.by_channel().len(), 2);
    assert_eq!(
        sizing
            .feed(primary())
            .expect("the primary channel")
            .messages(),
        4
    );
    assert_eq!(
        sizing
            .feed(Channel {
                source_addr: SECOND_SOURCE,
                channel_id: CHANNEL_ID,
            })
            .expect("the second channel")
            .messages(),
        2
    );
}

/// Another feed's datagrams are in neither half of this feed's ratio.
///
/// An archive may hold several feeds. A foreign datagram in the denominator
/// makes a busy feed look idle, and one in the numerator makes it look busier
/// than the publisher under measurement ever was.
#[test]
fn a_foreign_magic_is_in_neither_half() {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(&levels(4), PortRole::Mktdata, 0));
    log.extend(pack::<TopOfBook>(
        &[Msg::Quote(Quote {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            update_flags: 0x03,
            source_timestamp_ns: 1_000_000_001,
            bid_price: 10,
            bid_qty: 1,
            ask_price: 11,
            ask_qty: 1,
            bid_source_count: 1,
            ask_source_count: 1,
        })],
        PortRole::Mktdata,
        0,
    ));

    let mbp = one(&mut log);
    assert_eq!(mbp.datagrams, 4);
    assert_eq!(mbp.messages(), 4);

    // The same archive, read as the other feed, is the other four-to-nothing.
    let mut again = DatagramLog::new(pack::<MarketByPrice>(&levels(4), PortRole::Mktdata, 0));
    again.extend(pack::<TopOfBook>(
        &[Msg::Quote(Quote {
            instrument_id: AAA,
            source_id: SOURCE_ID,
            update_flags: 0x03,
            source_timestamp_ns: 1_000_000_001,
            bid_price: 10,
            bid_qty: 1,
            ask_price: 11,
            ask_qty: 1,
            bid_source_count: 1,
            ask_source_count: 1,
        })],
        PortRole::Mktdata,
        0,
    ));
    let tob = Sizing::measure(&mut again, MAGIC_TOB).expect("the log does not fail");
    let tob = tob.feed(primary()).expect("the primary channel");
    assert_eq!(tob.datagrams, 1);
    assert_eq!(tob.messages(), 1);
}

/// A channel with no datagrams has no ratio, which is not a ratio of zero.
#[test]
fn a_silent_channel_has_no_ratio_rather_than_a_zero_one() {
    let silent = FeedSizing::default();

    assert_eq!(silent.messages_per_datagram(), None);
    assert_eq!(
        measure(&mut DatagramLog::default()).by_channel().len(),
        0,
        "nothing was walked, so nothing is reported"
    );
}
