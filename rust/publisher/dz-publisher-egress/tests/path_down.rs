//! A route that goes and comes back: what the fan-out and the transmitter do
//! while the tunnel interface is gone.

mod common;

use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_publisher_egress::{
    DatagramSink, EgressEndpoint, FailureScope, MulticastTransmitter, SinkError, Tee,
};

use common::{doc_source, metrics, sample, FakeSink, FakeSocket, Verdict};

#[test]
fn a_member_whose_route_is_down_stays_in_the_fan_out_and_takes_the_next_datagram_when_it_returns() {
    // The whole point: a client upgrade restarts the tunnel daemon, the
    // interface is gone for a few seconds, and the socket works again once it
    // is back. Dropping the member over it would end the process.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::essential("mktdata");
    sink.script([Verdict::PathDown, Verdict::PathDown]);

    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(sink.boxed());

    fan_out.send(b"lost").expect("absorbed");
    assert_eq!(fan_out.path_down().collect::<Vec<_>>(), vec!["mktdata"]);
    fan_out.send(b"lost too").expect("absorbed");
    fan_out.send(b"sent").expect("and the route is back");

    assert_eq!(sink.accepted(), vec![b"sent".to_vec()]);
    assert_eq!(fan_out.live(), 1);
    assert_eq!(fan_out.dropped().count(), 0);
    assert_eq!(
        fan_out.path_down().count(),
        0,
        "cleared by the send it took"
    );
    assert_eq!(
        fan_out.process_failure(),
        None,
        "nothing for the guard to end"
    );
    assert_eq!(
        sample(
            &metrics,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", "socket_error")],
        ),
        2,
        "each refused datagram is counted",
    );
}

fn transmitter(socket: &FakeSocket) -> MulticastTransmitter<FakeSocket> {
    MulticastTransmitter::new(
        "mktdata",
        socket.clone(),
        EgressEndpoint::new(PortRole::Mktdata, doc_source(), 13_000),
        FailureScope::Process,
    )
}

#[test]
fn a_route_that_stays_down_past_the_window_is_no_longer_transient() {
    // The interface that comes back under a different address never serves
    // this socket again. Past the window the refusal is a socket failure, so
    // the fan-out drops the member and the guard ends the process, whose
    // restart re-derives the address. Given no window at all, that is the
    // first refusal.
    let socket = FakeSocket::new();
    socket.always(Verdict::PathDown);
    let mut transmitter = transmitter(&socket).with_max_path_down(Duration::ZERO);

    let error = transmitter.send(b"a").expect_err("down");
    assert!(
        matches!(error, SinkError::Socket(_)),
        "past the window it is a socket failure: {error}"
    );
    assert!(!error.is_transient());
}

#[test]
fn a_send_that_succeeds_closes_the_window() {
    // A second outage later in the day starts its own window rather than
    // inheriting the first one's elapsed time.
    let socket = FakeSocket::new();
    let mut transmitter = transmitter(&socket).with_max_path_down(Duration::from_secs(3600));

    socket.always(Verdict::PathDown);
    assert!(transmitter.send(b"a").expect_err("down").is_transient());
    socket.always(Verdict::Accept);
    transmitter.send(b"b").expect("back");
    socket.always(Verdict::PathDown);
    assert!(transmitter
        .send(b"c")
        .expect_err("down again")
        .is_transient());
    assert_eq!(socket.sent(), vec![b"b".to_vec()]);
}

#[test]
fn within_the_window_a_path_down_is_transient_on_every_send() {
    let socket = FakeSocket::new();
    socket.always(Verdict::PathDown);
    let mut transmitter = transmitter(&socket);

    for _ in 0..1000 {
        let error = transmitter.send(b"x").expect_err("down");
        assert!(matches!(error, SinkError::PathDown(_)), "{error}");
    }
}
