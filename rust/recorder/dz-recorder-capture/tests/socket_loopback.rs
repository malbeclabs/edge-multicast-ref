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
