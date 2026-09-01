//! The one thing that cannot be proved without a socket: that a real datagram
//! comes back out of [`SocketSource`] with a kernel stamp on it.
//!
//! Behind the `loopback-tests` feature. It needs a host with a multicast route
//! and `IP_MULTICAST_LOOP` delivery to itself, which CI does not have, and a
//! capture test that can only run by hand must not be able to fail the build.
#![cfg(feature = "loopback-tests")]

use dz_edge_core::PortRole;
use dz_recorder_capture::socket::{PortBinding, SocketSource, SocketSourceConfig};
use dz_recorder_core::{RecvTsKind, Source};
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 1);
const PORT: u16 = 41777;

/// The address of whichever interface this host would send multicast out of.
/// `connect` on a datagram socket sends nothing; it only makes the kernel pick
/// the route, and with it the source address to join on.
fn multicast_interface() -> Ipv4Addr {
    let probe = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).expect("probe socket");
    probe.connect((GROUP, 9)).expect("multicast route");
    match probe.local_addr().expect("local address") {
        SocketAddr::V4(addr) => *addr.ip(),
        SocketAddr::V6(_) => unreachable!("bound to an IPv4 address"),
    }
}

#[test]
fn a_datagram_on_the_loopback_path_arrives_with_a_kernel_stamp() {
    let interface = multicast_interface();
    let mut config = SocketSourceConfig::new(
        interface,
        vec![PortBinding::new(PortRole::Mktdata, GROUP, PORT)],
    );
    config.read_timeout = Duration::from_millis(20);
    // The sender is this host, which is not a publisher: the counter must move
    // and the datagram must still be delivered.
    config.expected_sources = vec![Ipv4Addr::new(192, 0, 2, 1)];

    let mut source = SocketSource::bind(&config).expect("bind");

    let sender = UdpSocket::bind((interface, 0)).expect("sender socket");
    sender.set_multicast_loop_v4(true).expect("multicast loop");

    let payload = [9u8; 24];
    let deadline = Instant::now() + Duration::from_secs(5);
    let received = loop {
        assert!(Instant::now() < deadline, "no datagram looped back");
        sender.send_to(&payload, (GROUP, PORT)).expect("send");
        if let Some(dg) = source.next().expect("receive") {
            break (
                dg.payload.to_vec(),
                dg.dst,
                dg.recv_ts_kind,
                dg.drop_delta,
                dg.ttl,
                *dg.src.ip(),
            );
        }
    };

    let (bytes, dst, kind, drop_delta, ttl, src) = received;
    assert_eq!(bytes, payload);
    assert_eq!(*dst.ip(), GROUP);
    assert_eq!(dst.port(), PORT);
    assert_eq!(kind, RecvTsKind::KernelSoftware);
    assert_eq!(drop_delta, 0, "the first datagram is the baseline");
    assert!(ttl.is_some(), "IP_RECVTTL reported it");
    assert_eq!(src, interface);

    let stats = source.stats();
    assert!(stats.datagrams >= 1);
    assert_eq!(stats.queue_drops, 0);
    assert!(
        stats.unexpected_source_datagrams >= 1,
        "counted, and delivered anyway"
    );
}

#[test]
fn a_second_group_on_the_same_port_is_not_recorded() {
    // The bind is what keeps the two apart. With the wildcard address Linux
    // leaves IP_MULTICAST_ALL at its default and hands this handle every group
    // any socket on the host has joined, so a recorder on a shared port would
    // archive a feed nobody asked it to keep — and ChannelInstance carries no
    // group, so the two sequence spaces would merge into one coverage row.
    const OTHER: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 2);
    // Its own port, so that SO_REUSEPORT cannot hand this test a datagram the
    // other test in this binary sent.
    const SHARED_PORT: u16 = PORT + 1;
    const OURS: [u8; 24] = [1u8; 24];
    const THEIRS: [u8; 24] = [2u8; 24];

    let interface = multicast_interface();

    // A second membership on this host, so the kernel has a reason to deliver
    // the other group to a wildcard-bound socket. Without this subscriber the
    // test would pass on a broken bind too.
    let bystander = UdpSocket::bind((OTHER, SHARED_PORT)).expect("bystander socket");
    bystander
        .join_multicast_v4(&OTHER, &interface)
        .expect("bystander join");

    let stop = Arc::new(AtomicBool::new(false));
    let sending = Arc::clone(&stop);
    // Steadily, and the other group first each time: Source::next blocks on a
    // quiet feed, so what ends this test is a datagram arriving rather than a
    // deadline passing.
    let sender = thread::spawn(move || {
        let socket = UdpSocket::bind((interface, 0)).expect("sender socket");
        socket.set_multicast_loop_v4(true).expect("multicast loop");
        while !sending.load(Ordering::Relaxed) {
            socket
                .send_to(&THEIRS, (OTHER, SHARED_PORT))
                .expect("send other");
            socket
                .send_to(&OURS, (GROUP, SHARED_PORT))
                .expect("send ours");
            thread::sleep(Duration::from_millis(20));
        }
    });

    let (done_tx, done_rx) = mpsc::channel();
    let collecting = Arc::clone(&stop);
    let collector = thread::spawn(move || {
        let mut config = SocketSourceConfig::new(
            interface,
            vec![PortBinding::new(PortRole::Mktdata, GROUP, SHARED_PORT)],
        );
        config.read_timeout = Duration::from_millis(20);
        let mut source = SocketSource::bind(&config).expect("bind");
        let mut kept = Vec::new();
        while kept.len() < 8 {
            match source.next().expect("receive") {
                Some(dg) => kept.push(dg.payload.to_vec()),
                None => break,
            }
        }
        collecting.store(true, Ordering::Relaxed);
        let _ = done_tx.send((kept, source.stats()));
    });

    // The bound the blocking receive does not have: a host that loops nothing
    // back must fail this test rather than hang it.
    let (kept, stats) = match done_rx.recv_timeout(Duration::from_secs(20)) {
        Ok(result) => result,
        Err(_) => {
            stop.store(true, Ordering::Relaxed);
            panic!("no datagram looped back within 20s");
        }
    };
    sender.join().expect("sender thread");
    collector.join().expect("collector thread");

    assert!(
        kept.iter().all(|p| p.as_slice() == OURS),
        "a datagram addressed to another group reached the archive"
    );
    assert!(!kept.is_empty(), "our own group never arrived");
    assert_eq!(
        stats.foreign_group_datagrams, 0,
        "the bind refuses them, so nothing should reach the check behind it"
    );
}
