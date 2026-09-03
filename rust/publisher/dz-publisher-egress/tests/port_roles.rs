//! What the port role decides, and what it refuses.

mod common;

use std::sync::Arc;

use dz_edge_core::{EncodeError, PortRole, ResetCount};
use dz_publisher_egress::{ChannelEgress, EgressEndpoint, EgressError};
use dz_publisher_metrics::{EgressErrorReason, EgressMessageType};

use common::{
    doc_source, first_msg_flags, metrics, sample, Contradictory, FakeSink, Small, SnapshotOnly,
    TestFeed, Uncarried,
};

/// Message header flag bit 0, set on the snapshot port and cleared elsewhere.
/// Transcribed from the field table.
const FLAG_SNAPSHOT: u16 = 0x0001;

const MTU: u16 = 1232;

fn egress(
    port_role: PortRole,
) -> (
    ChannelEgress<TestFeed, FakeSink>,
    FakeSink,
    Arc<dz_publisher_metrics::PublisherMetrics>,
) {
    let sink = FakeSink::new("sink");
    let metrics = metrics(
        &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot],
        &[7],
    );
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(port_role, doc_source(), 13_000),
        sink.clone(),
        Arc::clone(&metrics),
        ResetCount(1),
        MTU,
    );
    assert!(egress.register(7));
    (egress, sink, metrics)
}

#[test]
fn a_message_pushed_on_the_wrong_port_role_is_a_countable_error_not_a_panic() {
    // `SnapshotOnly` lists only the snapshot role. Pushing it on the refdata
    // port is a specification violation and a recoverable one: the send path
    // counts it and drops the message, because a publisher that panics goes
    // dark on every instrument it serves — including the ones whose messages
    // were correct.
    let (mut egress, sink, metrics) = egress(PortRole::Refdata);

    let error = egress
        .push(7, &SnapshotOnly, EgressMessageType::ManifestSummary, 0)
        .expect_err("a snapshot-only message on the refdata port");

    assert!(
        matches!(
            error,
            EgressError::Refused {
                source: EncodeError::WrongPortRole {
                    role: "refdata",
                    ..
                }
            }
        ),
        "got {error:?}"
    );
    assert_eq!(error.reason(), EgressErrorReason::WrongPortRole);
    assert_eq!(sink.accepted_count(), 0);
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "refdata"), ("reason", "wrong_port_role")],
        ),
        1,
    );
}

#[test]
fn a_refused_message_leaves_the_publisher_sending() {
    // The point of counting rather than aborting: the next message goes out.
    let (mut egress, sink, _metrics) = egress(PortRole::Mktdata);

    assert!(egress
        .push(7, &SnapshotOnly, EgressMessageType::ManifestSummary, 0)
        .is_err());
    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("the next message is unaffected");
    egress.flush(7, 0).expect("flush");

    assert_eq!(sink.accepted_count(), 1);
}

#[test]
fn a_malformed_message_is_refused_and_counted_as_malformed_message() {
    // The gap this used to report rather than paper over: `EgressErrorReason`
    // had five values and none of them described a message whose *combination*
    // of fields its own specification forbids, so `reason()` returned `None`
    // and the refusal reached no series at all. It now has its own value,
    // proposed as an addition to the closed set rather than folded into
    // `wrong_port_role`, which an operator reads as "sent to the wrong port".
    let (mut egress, sink, metrics) = egress(PortRole::Mktdata);

    let error = egress
        .push(7, &Contradictory, EgressMessageType::Quote, 0)
        .expect_err("a message whose own validate refuses it");

    assert!(
        matches!(
            error,
            EgressError::Refused {
                source: EncodeError::MalformedMessage { .. }
            }
        ),
        "got {error:?}"
    );
    assert_eq!(error.reason(), EgressErrorReason::MalformedMessage);
    assert_eq!(sink.accepted_count(), 0);
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "malformed_message")],
        ),
        1,
    );
    for reason in [
        "mtu_exceeded",
        "send_would_block",
        "socket_error",
        "not_registered",
        "wrong_port_role",
        "not_carried_by_feed",
    ] {
        assert_eq!(
            sample(
                &metrics,
                "dz_publisher_egress_errors_total",
                &[("port_role", "mktdata"), ("reason", reason)],
            ),
            0,
            "a malformed message must not be counted as {reason}",
        );
    }
}

#[test]
fn a_message_the_feed_does_not_carry_is_counted_as_not_carried_by_feed() {
    // The second failure that used to have no reason. It is the nearest thing
    // to a wrong port role — both are the specification refusing a placement —
    // and it is a different mistake: a wrong role is a send path wired to the
    // wrong socket, and this is a publisher composing for a feed it is not
    // emitting. Folding the two together would make one value mean both.
    let (mut egress, sink, metrics) = egress(PortRole::Mktdata);

    let error = egress
        .push(7, &Uncarried, EgressMessageType::Quote, 0)
        .expect_err("a Type ID this feed's table does not list");

    assert!(
        matches!(
            error,
            EgressError::Refused {
                source: EncodeError::NotCarriedByFeed { type_id: 0x15, .. }
            }
        ),
        "got {error:?}"
    );
    assert_eq!(error.reason(), EgressErrorReason::NotCarriedByFeed);
    assert_eq!(sink.accepted_count(), 0);
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "not_carried_by_feed")],
        ),
        1,
    );
    for reason in [
        "mtu_exceeded",
        "send_would_block",
        "socket_error",
        "not_registered",
        "wrong_port_role",
        "malformed_message",
    ] {
        assert_eq!(
            sample(
                &metrics,
                "dz_publisher_egress_errors_total",
                &[("port_role", "mktdata"), ("reason", reason)],
            ),
            0,
            "a message the feed does not carry must not be counted as {reason}",
        );
    }
}

#[test]
fn the_two_refusals_that_had_no_reason_reach_two_distinct_values() {
    // Stated once, as literals, rather than left implied by the two tests
    // above: the point of proposing two values instead of one is that they do
    // not collapse. A single value would have been the cheaper change and it
    // would have made the panel mean two things.
    let (mut egress, _sink, _metrics) = egress(PortRole::Mktdata);

    let malformed = egress
        .push(7, &Contradictory, EgressMessageType::Quote, 0)
        .expect_err("malformed");
    let uncarried = egress
        .push(7, &Uncarried, EgressMessageType::Quote, 0)
        .expect_err("not carried");

    assert_ne!(malformed.reason(), uncarried.reason());
    assert_eq!(malformed.reason().as_str(), "malformed_message");
    assert_eq!(uncarried.reason().as_str(), "not_carried_by_feed");
}

#[test]
fn the_snapshot_flag_follows_the_port_role_rather_than_the_caller() {
    // The builder owns the flag, so a caller cannot set the snapshot bit on a
    // live update or clear it on a snapshot: there is no wrong method to call.
    let (mut snapshot, snapshot_sink, _m1) = egress(PortRole::Snapshot);
    snapshot
        .push(7, &SnapshotOnly, EgressMessageType::ManifestSummary, 0)
        .expect("push");
    snapshot.flush(7, 0).expect("flush");

    let (mut mktdata, mktdata_sink, _m2) = egress(PortRole::Mktdata);
    mktdata
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    mktdata.flush(7, 0).expect("flush");

    assert_eq!(first_msg_flags(&snapshot_sink.accepted()[0]), FLAG_SNAPSHOT);
    assert_eq!(first_msg_flags(&mktdata_sink.accepted()[0]), 0);
}

#[test]
fn messages_are_counted_by_type_only_once_they_have_been_sent() {
    // `dz_publisher_egress_messages_total` says "messages sent". Counting at
    // the push instead would report messages this publisher composed and lost,
    // which makes the ratio between messages and datagrams meaningless in
    // exactly the incident where it is being read.
    let (mut egress, _sink, metrics) = egress(PortRole::Mktdata);
    for _ in 0..3 {
        egress
            .push(7, &Small, EgressMessageType::Quote, 0)
            .expect("push");
    }
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_messages_total",
            &[("port_role", "mktdata"), ("message_type", "quote")],
        ),
        0,
        "nothing has been sent yet",
    );

    egress.flush(7, 0).expect("flush");

    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_messages_total",
            &[("port_role", "mktdata"), ("message_type", "quote")],
        ),
        3,
    );
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_bytes_total",
            &[("port_role", "mktdata")],
        ),
        // The 24-byte datagram header and three 16-byte messages, by hand.
        24 + 3 * 16,
    );
}

#[test]
fn a_heartbeat_that_reached_the_wire_sets_the_gauge_that_measures_its_age() {
    // The runtime decides when to heartbeat; only the send path knows one was
    // actually sent, and the second fact is the one worth alerting on. The
    // gauge is pre-created at 0, so if nothing set it, a staleness rule could
    // not tell a publisher that has stopped heartbeating from one whose
    // heartbeat path was never wired up.
    //
    // `Send Timestamp` is nanoseconds since the Unix epoch, so the gauge is
    // that value in seconds — not a second clock read, which would let the
    // gauge and the datagram disagree.
    const SENT_AT_NS: u64 = 1_700_000_000_000_000_000;
    let (mut egress, _sink, metrics) = egress(PortRole::Mktdata);

    egress
        .push(7, &Small, EgressMessageType::Heartbeat, SENT_AT_NS)
        .expect("push");
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
            &[("port_role", "mktdata"), ("channel_id", "7")],
        ),
        0,
        "composed is not sent",
    );

    egress.flush(7, SENT_AT_NS).expect("flush");

    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
            &[("port_role", "mktdata"), ("channel_id", "7")],
        ),
        1_700_000_000,
    );
}
