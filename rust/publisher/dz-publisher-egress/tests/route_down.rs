//! A route that goes and comes back: what the fan-out and the transmitter do
//! while the tunnel interface is gone.

mod common;

use std::sync::Arc;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_publisher_egress::{
    DatagramSink, EgressEndpoint, FailureScope, MulticastTransmitter, RouteState, SinkError, Tee,
};

use common::{doc_source, metrics, sample, FakeSink, FakeSocket, Verdict};

#[test]
fn a_member_whose_route_is_down_stays_in_the_fan_out_and_takes_the_next_datagram_when_it_returns() {
    // The whole point: a client upgrade restarts the tunnel daemon, the
    // interface is gone for a few seconds, and the socket works again once it
    // is back. Dropping the member over it would end the process.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::essential("mktdata");
    sink.script([Verdict::RouteDown, Verdict::RouteDown]);

    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(sink.boxed());

    fan_out.send(b"lost").expect("absorbed");
    assert_eq!(
        fan_out.routes().collect::<Vec<_>>(),
        vec![("mktdata", RouteState::Down)]
    );
    fan_out.send(b"lost too").expect("absorbed");
    fan_out.send(b"sent").expect("and the route is back");

    assert_eq!(sink.accepted(), vec![b"sent".to_vec()]);
    assert_eq!(fan_out.live(), 1);
    assert_eq!(fan_out.dropped().count(), 0);
    assert_eq!(
        fan_out.routes().collect::<Vec<_>>(),
        vec![("mktdata", RouteState::Up)],
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
    socket.always(Verdict::RouteDown);
    let mut transmitter = transmitter(&socket).with_max_route_down(Duration::ZERO);

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
    let mut transmitter = transmitter(&socket).with_max_route_down(Duration::from_secs(3600));

    socket.always(Verdict::RouteDown);
    assert!(transmitter.send(b"a").expect_err("down").is_transient());
    socket.always(Verdict::Accept);
    transmitter.send(b"b").expect("back");
    socket.always(Verdict::RouteDown);
    assert!(transmitter
        .send(b"c")
        .expect_err("down again")
        .is_transient());
    assert_eq!(socket.sent(), vec![b"b".to_vec()]);
}

#[test]
fn within_the_window_a_route_down_is_transient_on_every_send() {
    let socket = FakeSocket::new();
    socket.always(Verdict::RouteDown);
    let mut transmitter = transmitter(&socket);

    for _ in 0..1000 {
        let error = transmitter.send(b"x").expect_err("down");
        assert!(matches!(error, SinkError::RouteDown(_)), "{error}");
    }
}

#[test]
fn a_route_that_stays_down_past_a_real_window_gives_up() {
    // The window is timed, not counted: refused every few milliseconds, and
    // only the refusal once it has passed is a socket failure.
    let socket = FakeSocket::new();
    socket.always(Verdict::RouteDown);
    let mut transmitter = transmitter(&socket).with_max_route_down(Duration::from_millis(50));

    let started = std::time::Instant::now();
    let mut last = transmitter.send(b"a").expect_err("down");
    while started.elapsed() < Duration::from_millis(80) {
        assert!(last.is_transient(), "gave up inside the window: {last}");
        std::thread::sleep(Duration::from_millis(5));
        last = transmitter.send(b"x").expect_err("still down");
        if matches!(last, SinkError::Socket(_)) {
            break;
        }
    }
    assert!(matches!(last, SinkError::Socket(_)), "{last}");
}

#[test]
fn a_refusal_long_after_the_last_one_starts_a_new_window() {
    // A port that sends rarely: refused once, silent past the window, and
    // refused again in a later outage. That is a new outage, not the old one
    // run long, and it must not end the process on its first refusal.
    let socket = FakeSocket::new();
    socket.always(Verdict::RouteDown);
    let mut transmitter = transmitter(&socket).with_max_route_down(Duration::from_millis(20));

    assert!(transmitter.send(b"a").expect_err("down").is_transient());
    std::thread::sleep(Duration::from_millis(40));
    let error = transmitter.send(b"b").expect_err("down again");
    assert!(matches!(error, SinkError::RouteDown(_)), "{error}");
}

#[test]
fn a_member_the_transmitter_gave_up_on_is_reported_as_given_up_not_back() {
    // What keeps the runtime from logging "the route is back" for a member it
    // has just dropped.
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::essential("mktdata");
    sink.script([Verdict::RouteDown, Verdict::Broken]);
    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(sink.boxed());

    fan_out.send(b"a").expect("absorbed");
    assert!(fan_out.essential_route_down());
    fan_out.send(b"b").expect("absorbed");

    assert_eq!(
        fan_out.routes().collect::<Vec<_>>(),
        vec![("mktdata", RouteState::GaveUp)]
    );
    assert!(!fan_out.essential_route_down());
    assert_eq!(fan_out.process_failure(), Some("mktdata"));
}

#[test]
fn a_full_buffer_during_an_outage_says_nothing_about_the_route() {
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::essential("mktdata");
    sink.script([Verdict::RouteDown, Verdict::WouldBlock]);
    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(sink.boxed());

    fan_out.send(b"a").expect("absorbed");
    fan_out.send(b"b").expect("absorbed");

    assert_eq!(
        fan_out.routes().collect::<Vec<_>>(),
        vec![("mktdata", RouteState::Down)],
        "a full buffer is not a route that came back"
    );
}

#[test]
fn only_an_essential_member_without_a_route_is_an_essential_route_down() {
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let copy = FakeSink::new("mktdata");
    copy.always(Verdict::RouteDown);
    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(FakeSink::essential("mktdata").boxed());
    fan_out.add(copy.boxed());

    fan_out.send(b"a").expect("absorbed");
    assert!(
        !fan_out.essential_route_down(),
        "the datagram left through the transmitter"
    );
    assert_eq!(
        fan_out.routes().collect::<Vec<_>>(),
        vec![("mktdata", RouteState::Up), ("mktdata", RouteState::Down)],
        "two members of one name, told apart by position"
    );
}

#[test]
fn a_fan_out_whose_transmitter_has_no_route_did_not_reach_the_wire() {
    let metrics = metrics(&[PortRole::Mktdata], &[7]);
    let sink = FakeSink::essential("mktdata");
    sink.script([Verdict::RouteDown]);
    let mut fan_out = Tee::new(PortRole::Mktdata, Arc::clone(&metrics));
    fan_out.add(sink.boxed());

    fan_out.send(b"a").expect("absorbed");
    assert!(!fan_out.reached_wire());
    fan_out.send(b"b").expect("sent");
    assert!(fan_out.reached_wire());
}
