//! The mandated 1,232-byte cap, enforced on the way out.
//!
//! Every expected value here is transcribed from the specifications' own field
//! tables and from `dz_edge_core::constants`, never read off the encoder: a
//! test that asks the builder how big a datagram may be agrees with whatever
//! the builder currently thinks, which is the bug this crate exists to make
//! impossible.

mod common;

use std::sync::Arc;

use dz_edge_core::{AppMessage, EncodeError, PortRole, ResetCount};
use dz_publisher_egress::{
    ChannelEgress, DatagramSink, EgressEndpoint, FailureScope, MulticastTransmitter, SinkError,
};
use dz_publisher_metrics::{EgressErrorReason, EgressMessageType};

use common::{
    declared_len, doc_source, metrics, sample, Big, FakeSink, FakeSocket, Small, TestFeed,
    OFF_MSG_COUNT,
};

/// Mandated by every feed specification, "to leave room for GRE encapsulation
/// headers used by the DoubleZero network's last-mile delivery". Transcribed,
/// not imported: one publisher shipped 1448 to production from a configuration
/// key, and a test that reads the constant it is checking would have passed
/// there too.
const MANDATED_CAP: usize = 1232;
/// The datagram header, from the field table.
const HEADER: usize = 24;
/// The largest message the 4-byte message header's `u8` Length field can frame.
const LARGEST_MESSAGE: usize = 255;

fn endpoint(port_role: PortRole) -> EgressEndpoint {
    EgressEndpoint::new(port_role, doc_source(), 13_000)
}

#[test]
fn a_datagram_is_finished_at_the_cap_rather_than_grown_past_it() {
    // Four 255-byte messages behind a 24-byte header is 1,044 bytes; a fifth
    // would be 1,299, which is over the cap. So six messages must arrive as
    // two datagrams of four and two, with nothing lost and nothing truncated.
    const {
        assert!(HEADER + 4 * LARGEST_MESSAGE == 1044, "four fit");
        assert!(
            HEADER + 5 * LARGEST_MESSAGE > MANDATED_CAP,
            "a fifth is over"
        );
    }

    let sink = FakeSink::new("mktdata");
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    // An MTU far above the cap, as a misconfigured deployment would supply.
    // The builder clamps it; nothing here may raise the cap.
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        endpoint(PortRole::Mktdata),
        sink.clone(),
        Arc::clone(&metrics),
        ResetCount(1),
        u16::MAX,
    );
    assert!(egress.register(7));

    for _ in 0..6 {
        egress
            .push(7, &Big, EgressMessageType::Quote, 0)
            .expect("a 255-byte message fits in a fresh datagram");
    }
    egress.flush(7, 0).expect("the tail datagram");

    let sent = sink.accepted();
    assert_eq!(sent.len(), 2, "six messages, four to a datagram");
    assert_eq!(sent[0].len(), 1044);
    assert_eq!(sent[0][OFF_MSG_COUNT], 4);
    assert_eq!(declared_len(&sent[0]), 1044);
    assert_eq!(sent[1].len(), HEADER + 2 * LARGEST_MESSAGE);
    assert_eq!(sent[1][OFF_MSG_COUNT], 2);
    for datagram in &sent {
        assert!(
            datagram.len() <= MANDATED_CAP,
            "a datagram of {} bytes reached the sink",
            datagram.len()
        );
    }
}

#[test]
fn a_message_no_datagram_can_carry_is_refused_as_mtu_exceeded_not_truncated() {
    // A capacity of exactly the datagram header leaves no room for any
    // message, which is the clamp's lower bound doing what the upper bound
    // does at the other end. The message is refused; nothing is sent; and in
    // particular nothing arrives cut to fit, because a truncated message is
    // framed by a Length field that no longer describes it and every message
    // behind it in the datagram is mis-parsed too.
    let sink = FakeSink::new("mktdata");
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let mut egress = ChannelEgress::<TestFeed, _>::new(
        endpoint(PortRole::Mktdata),
        sink.clone(),
        Arc::clone(&metrics),
        ResetCount(1),
        HEADER as u16,
    );
    assert!(egress.register(7));

    let error = egress
        .push(7, &Small, EgressMessageType::Quote, 0)
        .expect_err("a 16-byte message cannot fit in 0 bytes of capacity");

    assert_eq!(error.reason(), EgressErrorReason::MtuExceeded);
    assert!(
        matches!(
            error,
            dz_publisher_egress::EgressError::Refused {
                source: EncodeError::DatagramFull {
                    attempted,
                    capacity: 24,
                }
            } if attempted == HEADER + Small::SIZE
        ),
        "expected a full-datagram refusal naming the clamped capacity, got {error:?}"
    );
    assert_eq!(sink.accepted_count(), 0, "nothing may reach the sink");
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "mtu_exceeded")],
        ),
        1,
    );
}

#[test]
fn an_over_cap_datagram_is_refused_where_the_bytes_meet_the_socket() {
    // The builder cannot compose one this long, and the builder is not the
    // only thing that can hand a transmitter bytes. The publisher that shipped
    // a 1448-byte default had the limit in configuration rather than in the
    // send path; this is the send path refusing it regardless.
    let socket = FakeSocket::new();
    let mut transmitter = MulticastTransmitter::new(
        "mktdata",
        socket.clone(),
        EgressEndpoint::new(PortRole::Mktdata, doc_source(), 13_000),
        FailureScope::Process,
    );

    transmitter
        .send(&vec![0u8; MANDATED_CAP])
        .expect("a datagram at the cap is sendable");
    let error = transmitter
        .send(&vec![0u8; MANDATED_CAP + 1])
        .expect_err("one byte over the cap is not");

    assert!(matches!(error, SinkError::TooLarge { len } if len == MANDATED_CAP + 1));
    assert_eq!(error.reason(), EgressErrorReason::MtuExceeded);
    assert!(
        error.to_string().contains("1232"),
        "the refusal should name the cap: {error}"
    );
    assert_eq!(
        socket.sent().len(),
        1,
        "the over-cap datagram must not reach the socket"
    );
}
