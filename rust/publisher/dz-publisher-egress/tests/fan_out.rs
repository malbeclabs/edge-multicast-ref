//! The fan-out: what a member's failure costs, and what it must never cost.

mod common;

use std::sync::Arc;

use dz_edge_core::{PortRole, ResetCount};
use dz_publisher_egress::{
    ChannelEgress, DatagramSink, EgressEndpoint, EgressError, FailureScope, ReferenceStream,
    SinkError, Tee,
};
use dz_publisher_metrics::{EgressErrorReason, EgressMessageType};

use common::{doc_source, metrics, sample, FakeSink, Small, TestFeed, Verdict};

const MTU: u16 = 1232;

#[test]
fn a_failing_member_is_counted_and_dropped_and_the_send_still_succeeds() {
    // Non-negotiable, and the reason is upstream of this call: above the sink
    // boundary sits the only code that advances `Sequence Number`. If one
    // member's refusal were the outcome of the send, then whatever the caller
    // did about it — retry, abort, exit — would be done on behalf of every
    // other member, all of which took the datagram. The retry re-sends a
    // number the live members already have and is discarded as a duplicate;
    // the abort leaves the number spent with nothing sent under it. One
    // auxiliary consumer's broken pipe would have become a defect in the series
    // every subscriber tracks.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let healthy = FakeSink::new("wire");
    let broken = FakeSink::new("reference-stream");
    broken.always(Verdict::Broken);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(healthy.boxed());
    tee.add(broken.boxed());

    tee.send(b"first").expect("a member's failure is absorbed");
    tee.send(b"second")
        .expect("and the next send is unaffected");

    assert_eq!(healthy.accepted().len(), 2, "the live member saw both");
    assert_eq!(broken.accepted_count(), 0);
    assert_eq!(tee.live(), 1);
    assert_eq!(tee.dropped().collect::<Vec<_>>(), vec!["reference-stream"]);
    assert_eq!(
        tee.absorbed_failures(),
        1,
        "offered once, then dropped rather than retried per datagram",
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

#[test]
fn a_transient_refusal_does_not_drop_the_member() {
    // A full send buffer drains. Dropping the mktdata transmitter over one
    // would turn a microsecond of backpressure into a permanent outage, and
    // nothing would say so: `datagrams_total` would simply stop climbing.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::new("wire");
    sink.script([Verdict::WouldBlock]);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(sink.boxed());

    tee.send(b"lost").expect("absorbed");
    tee.send(b"sent").expect("and the buffer has drained");

    assert_eq!(sink.accepted().len(), 1);
    assert_eq!(tee.live(), 1);
    assert_eq!(tee.dropped().count(), 0);
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
fn an_essential_members_failure_is_reported_between_ticks_rather_than_returned() {
    // The design distinguishes a transmitter whose failure ends the process
    // from one that darkens only its own channel. The distinction survives the
    // absorbing: the failure is not the send's outcome, and it is not lost
    // either — it waits where the runtime's guard reads it, between ticks,
    // rather than exiting halfway through a fan-out and abandoning the
    // datagrams the other members already took.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let wire = FakeSink::essential("wire");
    let copy = FakeSink::new("reference-stream");
    wire.always(Verdict::Broken);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(wire.boxed());
    tee.add(copy.boxed());
    assert_eq!(tee.failure_scope(), FailureScope::Process);

    tee.send(b"datagram").expect("absorbed");

    assert_eq!(tee.process_failure(), Some("wire"));
    assert_eq!(copy.accepted().len(), 1, "the other member still got it");
}

#[test]
fn a_channel_scoped_members_failure_is_not_reported_as_a_process_failure() {
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let copy = FakeSink::new("reference-stream");
    copy.always(Verdict::Broken);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(copy.boxed());
    assert_eq!(tee.failure_scope(), FailureScope::Channel);

    tee.send(b"datagram").expect("absorbed");

    assert_eq!(tee.process_failure(), None);
}

#[test]
fn a_fan_out_with_nothing_live_left_reports_that_it_has_nowhere_to_send() {
    // The one outcome that *is* returned, because it is not a member's failure
    // but the absence of any destination: there is no other member whose
    // delivery a caller's reaction could damage.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let only = FakeSink::new("wire");
    only.always(Verdict::Broken);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(only.boxed());

    tee.send(b"first").expect("the failure itself is absorbed");
    let error = tee
        .send(b"second")
        .expect_err("with nothing live, there is nowhere to send");

    assert!(matches!(error, SinkError::NotRegistered));
    assert_eq!(error.reason(), EgressErrorReason::NotRegistered);
    assert_eq!(tee.live(), 0);
}

#[test]
fn an_empty_fan_out_reports_that_it_has_nowhere_to_send() {
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let mut tee = Tee::new(PortRole::Mktdata, metrics);

    assert!(matches!(
        tee.send(b"datagram"),
        Err(SinkError::NotRegistered)
    ));
}

#[test]
fn every_live_member_receives_the_same_bytes() {
    // The seam a reference stream hangs off is only useful if the copy is the
    // datagram, byte for byte: a consumer of the copy decodes it with the same
    // decoder a subscriber uses.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let wire = FakeSink::new("wire");
    let copy = FakeSink::new("reference-stream");

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(wire.boxed());
    tee.add(copy.boxed());

    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Mktdata, doc_source(), 13_000),
        tee,
        Arc::clone(&metrics),
        ResetCount(1),
        MTU,
    );
    assert!(egress.register(7));
    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    egress.flush(7, 0).expect("flush");

    assert_eq!(wire.accepted(), copy.accepted());
    assert_eq!(wire.accepted().len(), 1);
}

#[test]
fn a_fan_out_collapse_reaches_the_composer_as_a_countable_error() {
    // The composer counts what it receives, and the tee counts what it
    // absorbs, so nothing is counted twice: one `socket_error` from the
    // member's failure, one `not_registered` from the send that then had
    // nowhere to go.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let only = FakeSink::new("wire");
    only.always(Verdict::Broken);
    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(only.boxed());

    let mut egress = ChannelEgress::<TestFeed, _>::new(
        EgressEndpoint::new(PortRole::Mktdata, doc_source(), 13_000),
        tee,
        Arc::clone(&metrics),
        ResetCount(1),
        MTU,
    );
    assert!(egress.register(7));

    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    egress.flush(7, 0).expect("the first failure is absorbed");
    egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect("push");
    let error = egress
        .flush(7, 0)
        .expect_err("the second has nowhere to go");

    assert!(matches!(
        error,
        EgressError::Sink {
            source: SinkError::NotRegistered
        }
    ));
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "socket_error")],
        ),
        1,
    );
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
fn a_reference_stream_whose_consumer_has_not_started_keeps_its_place_in_the_fan_out() {
    // **The startup order this stream is built for.** A recorder that is not
    // running yet answers every `sendto` with `ENOENT`, and one that restarted
    // leaving its socket file behind answers `ECONNREFUSED`. Both used to be
    // counted as non-transient, so the member was dropped on the publisher's
    // first datagram and nothing ever restored it: the reference stream was over
    // for the life of the process, silently, in exactly the case the module
    // documentation says costs only the datagrams that were missed.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let wire = FakeSink::new("wire");
    let absent = std::env::temp_dir().join(format!(
        "dz-fanout-absent-{}-{:?}.sock",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&absent);

    let mut tee = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    tee.add(wire.boxed());
    tee.add(Box::new(
        ReferenceStream::open("reference-stream", &absent).expect("an unbound socket"),
    ));

    for _ in 0..3 {
        tee.send(b"datagram")
            .expect("a member's failure is absorbed");
    }

    assert_eq!(
        tee.live(),
        2,
        "the recorder has not gone away; it is not up"
    );
    assert_eq!(tee.dropped().count(), 0);
    assert_eq!(
        tee.absorbed_failures(),
        3,
        "offered on every datagram, because the next one may arrive",
    );
    assert_eq!(wire.accepted().len(), 3, "the wire is untouched by it");
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "socket_error")],
        ),
        3,
        "counted every time, under the label a failed send to a socket has",
    );
}
