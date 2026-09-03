//! `Sequence Number` per channel instance, and `Reset Count` on every
//! datagram.

mod common;

use std::sync::Arc;

use dz_edge_core::{PortRole, ResetCount};
use dz_publisher_egress::{ChannelEgress, ChannelInstance, EgressEndpoint, EgressError, Sequencer};
use dz_publisher_metrics::{EgressErrorReason, EgressMessageType};

use common::{
    doc_source, metrics, other_doc_source, sample, sequence_number, FakeSink, Small, TestFeed,
    Verdict, OFF_CHANNEL_ID, OFF_RESET_COUNT,
};

const MTU: u16 = 1232;
const MKTDATA_PORT: u16 = 13_000;

fn egress_on(
    channel_ids: &[u8],
    era: ResetCount,
) -> (
    ChannelEgress<TestFeed, FakeSink>,
    FakeSink,
    Arc<dz_publisher_metrics::PublisherMetrics>,
) {
    let sink = FakeSink::new("mktdata");
    let metrics = metrics(&[PortRole::Mktdata], channel_ids);
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Mktdata, doc_source(), MKTDATA_PORT),
        sink.clone(),
        Arc::clone(&metrics),
        era,
        MTU,
    );
    for channel_id in channel_ids {
        assert!(egress.register(*channel_id));
    }
    (egress, sink, metrics)
}

#[test]
fn a_channel_instances_sequence_is_dense_and_starts_at_zero() {
    // Dense is what the field is for: a subscriber that lost a datagram knows
    // one is missing because the next number is more than one above the last.
    // A publisher that skips a number manufactures that signal.
    let (mut egress, sink, metrics) = egress_on(&[7], ResetCount(1));
    for _ in 0..3 {
        egress
            .push(7, &Small, EgressMessageType::Quote, 0)
            .expect("push");
        egress.flush(7, 0).expect("flush");
    }

    let numbers: Vec<u64> = sink.accepted().iter().map(|d| sequence_number(d)).collect();
    assert_eq!(numbers, vec![0, 1, 2]);
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_sequence_current",
            &[("port_role", "mktdata"), ("channel_id", "7")],
        ),
        2,
        "the gauge follows the datagram that was actually sent",
    );
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_datagrams_total",
            &[("port_role", "mktdata")],
        ),
        3,
    );
}

#[test]
fn two_channel_ids_on_one_port_role_number_independently() {
    // Two shards of one feed. Interleaved, because that is how they arrive:
    // the series must not be one counter that both advance.
    let (mut egress, sink, _metrics) = egress_on(&[7, 9], ResetCount(1));
    for channel_id in [7, 9, 7, 9, 7] {
        egress
            .push(channel_id, &Small, EgressMessageType::Quote, 0)
            .expect("push");
        egress.flush(channel_id, 0).expect("flush");
    }

    let sent = sink.accepted();
    let seven: Vec<u64> = sent
        .iter()
        .filter(|d| d[OFF_CHANNEL_ID] == 7)
        .map(|d| sequence_number(d))
        .collect();
    let nine: Vec<u64> = sent
        .iter()
        .filter(|d| d[OFF_CHANNEL_ID] == 9)
        .map(|d| sequence_number(d))
        .collect();
    assert_eq!(seven, vec![0, 1, 2]);
    assert_eq!(nine, vec![0, 1]);
}

#[test]
fn one_channel_id_from_two_source_addresses_is_two_series() {
    // The case a channel-only key cannot express: an operator running two
    // publishers serving the same `Channel ID` to the same group and port.
    // Keyed any less finely, the two interleave into one counter that goes
    // backwards on every alternation.
    let mut sequencer = Sequencer::new(ResetCount(4));
    let mine = ChannelInstance::new(doc_source(), 7, MKTDATA_PORT);
    let theirs = ChannelInstance::new(other_doc_source(), 7, MKTDATA_PORT);
    assert!(sequencer.register(mine));
    assert!(sequencer.register(theirs));

    sequencer.advance(&mine);
    sequencer.advance(&mine);
    sequencer.advance(&theirs);

    assert_eq!(
        sequencer
            .current(&mine)
            .expect("registered")
            .sequence_number(),
        2
    );
    assert_eq!(
        sequencer
            .current(&theirs)
            .expect("registered")
            .sequence_number(),
        1
    );
    assert_eq!(sequencer.era(), ResetCount(4), "one era, several series");
}

#[test]
fn the_same_port_role_on_two_ports_is_two_series() {
    // The mktdata and refdata ports are separate channel instances, which is
    // why every egress metric family carries `port_role` and why nothing may
    // aggregate across it.
    let mut sequencer = Sequencer::new(ResetCount(1));
    let mktdata = ChannelInstance::new(doc_source(), 7, MKTDATA_PORT);
    let refdata = ChannelInstance::new(doc_source(), 7, MKTDATA_PORT + 1);
    assert!(sequencer.register(mktdata));
    assert!(sequencer.register(refdata));
    sequencer.advance(&mktdata);

    assert_eq!(
        sequencer
            .current(&mktdata)
            .expect("registered")
            .sequence_number(),
        1
    );
    assert_eq!(
        sequencer
            .current(&refdata)
            .expect("registered")
            .sequence_number(),
        0
    );
}

#[test]
fn re_registering_a_live_channel_instance_does_not_restart_its_series() {
    // A sequence that goes back to 0 *without* `Reset Count` changing is the
    // one combination a subscriber cannot interpret: it was told the era is
    // still running, so backward motion reads as reordering and the datagrams
    // are discarded as duplicates.
    let mut sequencer = Sequencer::new(ResetCount(2));
    let instance = ChannelInstance::new(doc_source(), 7, MKTDATA_PORT);
    assert!(sequencer.register(instance));
    sequencer.advance(&instance);
    sequencer.advance(&instance);

    assert!(!sequencer.register(instance), "already registered");
    assert_eq!(
        sequencer
            .current(&instance)
            .expect("registered")
            .sequence_number(),
        2
    );
}

#[test]
fn a_datagram_the_sink_refused_still_spends_its_number() {
    // The alternative is worse. Handing the refused number to the next
    // datagram puts two different payloads on the wire under one number in one
    // era; a conformant subscriber discards the second as a duplicate, so the
    // loss is invisible on both sides — no gap here, no message there. A gap is
    // what a loss *is*, and the subscriber's own tracker is what reports it.
    let (mut egress, sink, metrics) = egress_on(&[7], ResetCount(1));
    sink.script([Verdict::Accept, Verdict::WouldBlock, Verdict::Accept]);

    let mut refusals = 0;
    for _ in 0..3 {
        egress
            .push(7, &Small, EgressMessageType::Quote, 0)
            .expect("push");
        if egress.flush(7, 0).is_err() {
            refusals += 1;
        }
    }

    assert_eq!(refusals, 1);
    let numbers: Vec<u64> = sink.accepted().iter().map(|d| sequence_number(d)).collect();
    assert_eq!(
        numbers,
        vec![0, 2],
        "1 was spent on the datagram that was lost"
    );
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "send_would_block")],
        ),
        1,
    );
}

#[test]
fn an_unregistered_channel_instance_is_refused_and_counted() {
    // Not registered on demand: a `Channel ID` nobody registered would begin a
    // fresh series at 0 in whatever era this process is in, and every
    // subscriber that *is* listening reads that as a publisher restart.
    let (mut egress, sink, metrics) = egress_on(&[7], ResetCount(1));

    let error = egress
        .push(3, &Small, EgressMessageType::Quote, 0)
        .expect_err("channel 3 was never registered");

    assert!(matches!(error, EgressError::NotRegistered { .. }));
    assert_eq!(error.reason(), EgressErrorReason::NotRegistered);
    assert_eq!(sink.accepted_count(), 0);
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "not_registered")],
        ),
        1,
    );
}

#[test]
fn every_datagram_carries_the_era_the_store_handed_out() {
    // `Reset Count` is at offset 21, and it is how a subscriber is told to drop
    // what it cached. A publisher that restarts without changing it has told
    // its subscribers nothing.
    let (mut egress, sink, _metrics) = egress_on(&[7], ResetCount(9));
    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    egress.flush(7, 0).expect("flush");

    let sent = sink.accepted();
    assert_eq!(sent[0][OFF_RESET_COUNT], 9);
}

#[test]
fn a_tick_that_packed_nothing_sends_nothing_and_spends_no_number() {
    // `Message Count` has range 1-255, so an empty datagram is not
    // representable. A flush on a quiet channel must not spend a number on one:
    // a subscriber counting gaps would charge the silence to the network.
    let (mut egress, sink, _metrics) = egress_on(&[7], ResetCount(1));
    egress
        .flush(7, 0)
        .expect("nothing to flush is not an error");
    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    egress.flush(7, 0).expect("flush");

    let numbers: Vec<u64> = sink.accepted().iter().map(|d| sequence_number(d)).collect();
    assert_eq!(numbers, vec![0]);
}

#[test]
fn a_tick_flushes_every_channel_id_even_after_one_of_them_fails() {
    // The `Channel ID`s on one port role are separate channel instances. One
    // socket failure must not leave another shard's datagram sitting in memory
    // with its number already assigned: that number is spent either way, so an
    // unflushed datagram is a gap with nothing sent under it.
    let (mut egress, sink, metrics) = egress_on(&[7, 9], ResetCount(1));
    for channel_id in [7, 9] {
        egress
            .push(channel_id, &Small, EgressMessageType::Quote, 0)
            .expect("push");
    }
    // The first flush of the tick fails, whichever shard it belongs to.
    sink.script([Verdict::Broken]);

    let error = egress
        .flush_all(0)
        .expect_err("the first failure of the tick is returned");

    assert!(matches!(error, EgressError::Sink { .. }));
    assert_eq!(
        sink.accepted_count(),
        1,
        "the other shard was still flushed",
    );
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "socket_error")],
        ),
        1,
    );
}
