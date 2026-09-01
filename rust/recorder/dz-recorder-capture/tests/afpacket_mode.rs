//! `AF_PACKET` mode's decisions, tested as the pure logic they are: no
//! privileges, no device, no network.
//!
//! The filter string, the frame parse, the stats-to-delta arithmetic and the
//! precision check's decision logic are all values and functions, so all of
//! them run in CI. The one thing that cannot be proved without a real device
//! and `CAP_NET_RAW` is behind the `afpacket-live-tests` feature at the bottom
//! of this file.
//!
//! Addresses are documentation-range (RFC 5737) and MCAST-TEST-NET (RFC 5771)
//! placeholders throughout, and the interface name is a placeholder too.
#![cfg(feature = "afpacket")]

use dz_edge_core::PortRole;
use dz_recorder_capture::afpacket::{
    bpf_filter_for, classify_frame, precision_from_savefile_magic, stamp_ns, AfPacketSource,
    AfPacketSourceConfig, FeedFilter, FrameSkip, PortMap, Precision, RingAccounting, Stat,
};
use dz_recorder_capture::{PendingLoss, PortBinding};
use std::net::Ipv4Addr;

const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const MKTDATA_PORT: u16 = 40000;
const REFDATA_PORT: u16 = 40001;
const SNAPSHOT_PORT: u16 = 40002;

/// The plan's fixture, with the group in MCAST-TEST-NET: this repository is
/// public and every address in it is a documentation-range placeholder.
fn feed_on(group: &str, ports: &[u16]) -> FeedFilter {
    FeedFilter::new(group.parse().expect("a placeholder group"), ports)
}

fn bindings() -> Vec<PortBinding> {
    vec![
        PortBinding::new(PortRole::Mktdata, GROUP, MKTDATA_PORT),
        PortBinding::new(PortRole::Refdata, GROUP, REFDATA_PORT),
        PortBinding::new(PortRole::Snapshot, GROUP, SNAPSHOT_PORT),
    ]
}

fn stat(received: u32, dropped: u32, if_dropped: u32) -> Stat {
    Stat {
        received,
        dropped,
        if_dropped,
    }
}

/// How a fixture frame departs from a clean datagram.
#[derive(Debug, Default, Clone, Copy)]
struct Wire {
    ethertype: Option<u16>,
    protocol: Option<u8>,
    /// The IPv4 flags-and-fragment-offset field.
    flags_and_offset: u16,
    /// Bytes to cut off the end, as a capture length would.
    cut: usize,
    /// Bytes of Ethernet padding after the IPv4 datagram, as a short frame
    /// carries.
    padding: usize,
    /// A UDP length field that contradicts the IPv4 total length.
    udp_len_override: Option<u16>,
}

fn frame(ttl: u8, dst_port: u16, payload: &[u8], wire: Wire) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(&wire.ethertype.unwrap_or(0x0800).to_be_bytes());

    let total_len = u16::try_from(20 + 8 + payload.len()).expect("a small fixture");
    out.push(0x45);
    out.push(0);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&wire.flags_and_offset.to_be_bytes());
    out.push(ttl);
    out.push(wire.protocol.unwrap_or(17));
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&SOURCE.octets());
    out.extend_from_slice(&GROUP.octets());

    out.extend_from_slice(&41000u16.to_be_bytes());
    out.extend_from_slice(&dst_port.to_be_bytes());
    let udp_len = wire
        .udp_len_override
        .unwrap_or_else(|| u16::try_from(8 + payload.len()).expect("a small fixture"));
    out.extend_from_slice(&udp_len.to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(payload);

    out.extend(std::iter::repeat_n(0xffu8, wire.padding));
    out.truncate(out.len() - wire.cut);
    out
}

fn clean_frame(payload: &[u8]) -> Vec<u8> {
    frame(31, MKTDATA_PORT, payload, Wire::default())
}

/// The length libpcap reports for a frame it captured: what arrived, which is
/// more than what was captured when the capture length cut the frame short.
fn on_wire_len(captured: &[u8], wire: Wire) -> usize {
    captured.len() + wire.cut
}

fn an_arp_frame() -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(&0x0806u16.to_be_bytes());
    out.extend_from_slice(&[0u8; 28]);
    out
}

#[test]
fn the_filter_is_derived_from_the_configured_groups_and_ports() {
    // Without a filter the recorder archives every datagram on the interface.
    let f = bpf_filter_for(&feed_on("233.252.0.10", &[40000, 40001, 40002]));
    assert_eq!(
        f,
        "udp and dst host 233.252.0.10 and (dst port 40000 or dst port 40001 or dst port 40002)"
    );
}

#[test]
fn the_filter_a_configuration_compiles_is_the_one_its_bindings_state() {
    let config = AfPacketSourceConfig::new("tun0", Ipv4Addr::new(192, 0, 2, 7), bindings());
    assert_eq!(
        config.filter(),
        "udp and dst host 233.252.0.10 and (dst port 40000 or dst port 40001 or dst port 40002)"
    );
}

#[test]
fn the_filter_is_canonical_whatever_order_the_roles_were_configured_in() {
    // The filter string is provenance as much as it is a filter: two hosts
    // recording the same feed must compile the same program.
    let mut reordered = bindings();
    reordered.reverse();
    assert_eq!(
        bpf_filter_for(&FeedFilter::from_bindings(&reordered)),
        bpf_filter_for(&FeedFilter::from_bindings(&bindings()))
    );
}

#[test]
fn a_single_port_needs_no_disjunction() {
    assert_eq!(
        bpf_filter_for(&feed_on("233.252.0.10", &[40000])),
        "udp and dst host 233.252.0.10 and dst port 40000"
    );
}

#[test]
fn a_feed_with_no_port_still_narrows_to_its_group() {
    // Narrower than the interface, and still not a configuration the source
    // will open: see below.
    assert_eq!(
        bpf_filter_for(&feed_on("233.252.0.10", &[])),
        "udp and dst host 233.252.0.10"
    );
}

#[test]
fn a_capture_with_no_port_roles_is_refused_rather_than_compiled() {
    // `udp` on an interface is every datagram on the wire. Refusing the
    // configuration costs nothing; discovering it from an archive costs a disk.
    let config = AfPacketSourceConfig::new("tun0", Ipv4Addr::new(192, 0, 2, 7), Vec::new());
    assert!(AfPacketSource::open(&config).is_err());
}

#[test]
fn nanosecond_precision_is_requested_and_verified_not_assumed() {
    // libpcap silently gives microseconds if the request is not honoured, and a
    // microsecond archive is indistinguishable from a nanosecond one that
    // happens to end in three zeros. The savefile magic is derived from the
    // handle's own precision, so it answers what the request cannot.
    assert_eq!(
        precision_from_savefile_magic(&0xa1b2_3c4du32.to_ne_bytes()),
        Some(Precision::Nano)
    );
    assert_eq!(
        precision_from_savefile_magic(&0xa1b2_c3d4u32.to_ne_bytes()),
        Some(Precision::Micro)
    );
}

#[test]
fn a_microsecond_handle_is_recognised_whichever_byte_order_wrote_it() {
    assert_eq!(
        precision_from_savefile_magic(&0xa1b2_c3d4u32.to_be_bytes()),
        Some(Precision::Micro)
    );
    assert_eq!(
        precision_from_savefile_magic(&0xa1b2_c3d4u32.to_le_bytes()),
        Some(Precision::Micro)
    );
}

#[test]
fn an_unrecognisable_savefile_header_is_not_read_as_nanoseconds() {
    // Unknown is not the same as honoured, and only one of the two may lead to
    // an archive.
    assert_eq!(precision_from_savefile_magic(&[0, 0, 0, 0]), None);
    assert_eq!(precision_from_savefile_magic(&[0xa1, 0xb2]), None);
}

#[test]
fn a_stamp_is_seconds_and_the_nanoseconds_the_verified_precision_reports() {
    assert_eq!(stamp_ns(1, 123_456_789), 1_123_456_789);
    // A negative timeval is not a time; it must not wrap into one.
    assert_eq!(stamp_ns(-1, 0), 0);
}

#[test]
fn the_first_poll_on_a_handle_establishes_the_ring_baseline() {
    // Both counters are running totals and both wrap. Reporting the whole
    // counter as a loss on the first datagram invents an outage.
    let mut ring = RingAccounting::new();
    let delta = ring.poll(&stat(1_000_000, 4_000, 7));
    assert_eq!(
        (delta.capture_drops, delta.interface_drops, delta.received),
        (0, 0, 0)
    );
}

#[test]
fn ring_drops_become_the_per_datagram_delta() {
    let mut ring = RingAccounting::new();
    // The poll the handle makes at open, before any datagram: the baseline.
    ring.poll(&stat(0, 0, 0));
    assert_eq!(ring.poll(&stat(10, 4, 1)).capture_drops, 4);
    assert_eq!(
        ring.poll(&stat(20, 4, 1)).capture_drops,
        0,
        "the delta, not the total"
    );
}

#[test]
fn a_datagram_that_never_reached_the_record_loop_is_owed_to_the_next_one() {
    // A datagram we dropped is a datagram lost between the one before it and
    // the one after it, which is exactly what drop_delta and epb_dropcount are
    // defined as. Unadmitted, the archive shows a gap with nothing behind it and
    // the analysis tier charges it to the publisher.
    let mut owed = PendingLoss::new();
    owed.owe(40);
    assert_eq!(owed.owed(), 40);
    owed.undelivered();
    assert_eq!(owed.owed(), 41, "the burst, and the datagram we dropped");
    owed.settled();
    assert_eq!(owed.owed(), 0, "it travelled with the datagram");
}

#[test]
fn the_ring_delta_arithmetic_wraps() {
    let mut ring = RingAccounting::new();
    ring.poll(&stat(0, u32::MAX - 1, 0));
    assert_eq!(ring.poll(&stat(0, 2, 0)).capture_drops, 4);
}

#[test]
fn interface_drops_are_a_separate_category_from_ring_drops() {
    // "gap, no capture drops, interface drops rising" is loss upstream of the
    // capture point, and folding it into publisher loss is how a switch problem
    // becomes a publisher finding.
    let mut ring = RingAccounting::new();
    ring.poll(&stat(0, 0, 0));
    let delta = ring.poll(&stat(10, 0, 6));
    assert_eq!((delta.capture_drops, delta.interface_drops), (0, 6));
}

#[test]
fn a_non_ipv4_or_non_udp_frame_that_slips_the_filter_is_skipped_not_archived() {
    let arp = an_arp_frame();
    assert!(AfPacketSource::parse_frame(&arp, arp.len()).is_none());
    let icmp = frame(
        31,
        MKTDATA_PORT,
        &[7u8; 24],
        Wire {
            protocol: Some(1),
            ..Wire::default()
        },
    );
    assert!(AfPacketSource::parse_frame(&icmp, icmp.len()).is_none());
    assert_eq!(classify_frame(&arp, arp.len()), Err(FrameSkip::NotIpv4));
    assert_eq!(classify_frame(&icmp, icmp.len()), Err(FrameSkip::NotUdp));
}

#[test]
fn the_addresses_the_ttl_and_the_payload_are_the_ones_that_were_captured() {
    // Nothing here is synthesised, which is the whole reason this mode is the
    // default.
    let payload: Vec<u8> = (0..24u8).collect();
    let wire = clean_frame(&payload);
    let parsed = AfPacketSource::parse_frame(&wire, wire.len()).expect("a clean datagram");
    assert_eq!(*parsed.src.ip(), SOURCE);
    assert_eq!(parsed.src.port(), 41000);
    assert_eq!(*parsed.dst.ip(), GROUP);
    assert_eq!(parsed.dst.port(), MKTDATA_PORT);
    assert_eq!(parsed.ttl, 31, "observed, not a plausible default");
    assert_eq!(parsed.payload, &payload[..]);
    assert_eq!(
        parsed.wire_payload_len as usize,
        payload.len(),
        "nothing was cut, and saying so is what asserts it"
    );
}

#[test]
fn the_ethernet_ipv4_and_udp_bytes_travel_as_they_arrived() {
    // Rebuilt headers are what a socket capture has to settle for. The
    // identification field, the fragmentation flags and the checksums are
    // evidence, and an archive that reconstructs them cannot tell a reader
    // whether a datagram was fragmented or delivered twice.
    let wire = clean_frame(&[7u8; 24]);
    let parsed = AfPacketSource::parse_frame(&wire, wire.len()).expect("a clean datagram");
    assert_eq!(parsed.link_headers, &wire[..14 + 20 + 8]);
}

#[test]
fn ethernet_padding_is_not_archived_as_payload() {
    // A datagram shorter than Ethernet's minimum frame arrives padded. The IPv4
    // total length decides where the datagram ends; the end of the frame does
    // not.
    let payload = [9u8; 4];
    let padded = frame(
        31,
        MKTDATA_PORT,
        &payload,
        Wire {
            padding: 18,
            ..Wire::default()
        },
    );
    let parsed =
        AfPacketSource::parse_frame(&padded, padded.len()).expect("a padded datagram is still one");
    assert_eq!(parsed.payload, &payload[..]);
    assert_eq!(parsed.wire_payload_len as usize, payload.len());
}

#[test]
fn a_fragmented_frame_is_not_mis_parsed_into_a_plausible_datagram() {
    // A first fragment has a UDP header over a partial payload and a later one
    // has no UDP header at all. Both parse into a datagram no publisher sent.
    let first = frame(
        31,
        MKTDATA_PORT,
        &[7u8; 24],
        Wire {
            flags_and_offset: 0x2000,
            ..Wire::default()
        },
    );
    let later = frame(
        31,
        MKTDATA_PORT,
        &[7u8; 24],
        Wire {
            flags_and_offset: 185,
            ..Wire::default()
        },
    );
    assert_eq!(
        classify_frame(&first, first.len()),
        Err(FrameSkip::Fragmented)
    );
    assert_eq!(
        classify_frame(&later, later.len()),
        Err(FrameSkip::Fragmented)
    );
}

#[test]
fn a_datagram_the_capture_length_cut_short_is_archived_declaring_what_arrived() {
    // Discarding it produces a sequence gap with nothing admitted behind it, so
    // a publisher violation becomes publisher loss. A truncated datagram in the
    // archive is evidence; a gap is not.
    let wire = Wire {
        cut: 8,
        ..Wire::default()
    };
    let cut = frame(31, MKTDATA_PORT, &[7u8; 24], wire);
    let parsed =
        classify_frame(&cut, on_wire_len(&cut, wire)).expect("what was captured is still evidence");
    assert_eq!(parsed.payload.len(), 16, "as much as was captured");
    assert_eq!(parsed.wire_payload_len, 24, "as much as was sent");
    assert_eq!(
        parsed.link_headers,
        &cut[..14 + 20 + 8],
        "every header was captured whole"
    );
    assert_eq!(parsed.dst.port(), MKTDATA_PORT, "so a role still owns it");
}

#[test]
fn a_frame_cut_inside_its_own_headers_carries_no_ports_and_is_skipped() {
    // No destination port means no port role owns it and no interface in the
    // archive holds it, so this one stays a skip and a counter.
    let wire = Wire {
        cut: 30,
        ..Wire::default()
    };
    let cut = frame(31, MKTDATA_PORT, &[7u8; 24], wire);
    assert_eq!(
        classify_frame(&cut, on_wire_len(&cut, wire)),
        Err(FrameSkip::HeadersCut)
    );
}

#[test]
fn headers_claiming_more_than_the_frame_that_arrived_are_malformed() {
    // The same short frame, with libpcap reporting that nothing was cut: the
    // IPv4 total length then contradicts the frame itself, which is not a
    // capture length and not a datagram.
    let cut = frame(
        31,
        MKTDATA_PORT,
        &[7u8; 24],
        Wire {
            cut: 8,
            ..Wire::default()
        },
    );
    assert_eq!(classify_frame(&cut, cut.len()), Err(FrameSkip::Malformed));
}

#[test]
fn a_frame_shorter_than_the_headers_it_claims_is_skipped() {
    assert_eq!(classify_frame(&[], 0), Err(FrameSkip::TooShort));
    assert_eq!(classify_frame(&[0u8; 20], 20), Err(FrameSkip::NotIpv4));
    let mut ethernet_only = vec![0u8; 12];
    ethernet_only.extend_from_slice(&0x0800u16.to_be_bytes());
    assert_eq!(
        classify_frame(&ethernet_only, ethernet_only.len()),
        Err(FrameSkip::TooShort)
    );
}

#[test]
fn a_udp_length_that_contradicts_the_ip_length_is_skipped() {
    let lying = frame(
        31,
        MKTDATA_PORT,
        &[7u8; 24],
        Wire {
            udp_len_override: Some(900),
            ..Wire::default()
        },
    );
    assert_eq!(
        classify_frame(&lying, lying.len()),
        Err(FrameSkip::Malformed)
    );
}

#[test]
fn the_port_role_is_read_off_the_destination_port() {
    // The capture is on an interface, so the role is not a property of the
    // handle the way it is in socket mode.
    let map = PortMap::from_bindings(&bindings());
    assert_eq!(map.role_for(MKTDATA_PORT), Some(PortRole::Mktdata));
    assert_eq!(map.role_for(REFDATA_PORT), Some(PortRole::Refdata));
    assert_eq!(map.role_for(SNAPSHOT_PORT), Some(PortRole::Snapshot));
    assert_eq!(
        map.role_for(9),
        None,
        "no role owns it, so no interface in the archive holds it"
    );
}

/// The one thing that cannot be proved without a real device: that a datagram
/// on the wire comes back out of [`AfPacketSource`] with the headers that were
/// captured, at the precision the handle verified.
///
/// Behind `afpacket-live-tests`. It needs `CAP_NET_RAW` and a host that
/// delivers multicast to itself on the loopback interface, and a capture test
/// that can only run by hand must not be able to fail the build.
#[test]
fn the_default_capture_length_holds_a_capped_datagram_behind_ipv4_options() {
    // 14 + 60 + 8, not 14 + 20 + 8. A datagram at the mandated cap whose IPv4
    // header carries the full 40 bytes of options is 1314 bytes on the wire; a
    // capture length of cap + 42 keeps 1274 of them and loses the payload tail.
    // What makes that worse than a lost tail is the accounting: the frame comes
    // back short, truncated_datagrams counts it, and a compliant publisher is
    // reported for a violation the recorder committed. This is also the snaplen
    // the archive declares in every interface description block.
    let config = AfPacketSourceConfig::new(
        "placeholder0",
        Ipv4Addr::new(192, 0, 2, 20),
        vec![PortBinding::new(PortRole::Mktdata, GROUP, MKTDATA_PORT)],
    );
    assert_eq!(
        config.snaplen,
        dz_edge_core::MAX_DATAGRAM_SIZE + 14 + 60 + 8
    );
}

#[cfg(feature = "afpacket-live-tests")]
mod live {
    use super::{bindings, GROUP, MKTDATA_PORT};
    use dz_edge_core::PortRole;
    use dz_recorder_capture::afpacket::{AfPacketSource, AfPacketSourceConfig, Precision};
    use dz_recorder_capture::PortBinding;
    use dz_recorder_core::{RecvTsKind, Source};
    use std::net::{Ipv4Addr, UdpSocket};
    use std::time::{Duration, Instant};

    const LOOPBACK: Ipv4Addr = Ipv4Addr::new(127, 0, 0, 1);
    const DEVICE: &str = "lo";

    fn config() -> AfPacketSourceConfig {
        let mut config = AfPacketSourceConfig::new(DEVICE, LOOPBACK, bindings());
        config.read_timeout = Duration::from_millis(20);
        config
    }

    #[test]
    fn a_live_handle_reports_the_precision_it_verified() {
        let source = AfPacketSource::open(&config()).expect(
            "open the capture device — this test needs CAP_NET_RAW: build with \
             --features afpacket-live-tests --no-run and run the test binary under sudo",
        );
        assert_eq!(source.precision(), Precision::Nano);
        assert!(source.filter().starts_with("udp and dst host "));
    }

    #[test]
    fn a_captured_datagram_carries_the_headers_that_were_on_the_wire() {
        let mut config = AfPacketSourceConfig::new(
            DEVICE,
            LOOPBACK,
            vec![PortBinding::new(PortRole::Mktdata, GROUP, MKTDATA_PORT)],
        );
        config.read_timeout = Duration::from_millis(20);
        let mut source = AfPacketSource::open(&config).expect(
            "open the capture device — this test needs CAP_NET_RAW: build with \
             --features afpacket-live-tests --no-run and run the test binary under sudo",
        );

        let sender = UdpSocket::bind((LOOPBACK, 0)).expect("sender socket");
        sender.set_multicast_loop_v4(true).expect("multicast loop");
        let payload: Vec<u8> = (0..24u8).collect();

        let deadline = Instant::now() + Duration::from_secs(5);
        let observed = loop {
            assert!(Instant::now() < deadline, "no datagram was captured");
            sender
                .send_to(&payload, (GROUP, MKTDATA_PORT))
                .expect("send");
            if let Some(dg) = source.next().expect("capture") {
                break (
                    dg.payload.to_vec(),
                    dg.dst,
                    dg.role,
                    dg.recv_ts_kind,
                    dg.drop_delta,
                    dg.ttl,
                    dg.link_headers.map(<[u8]>::to_vec),
                    dg.wire_payload_len,
                );
            }
        };

        let (bytes, dst, role, kind, drop_delta, ttl, link_headers, wire_payload_len) = observed;
        assert_eq!(bytes, payload);
        assert_eq!(
            link_headers.map(|h| h.len()),
            Some(14 + 20 + 8),
            "the headers were captured, not rebuilt"
        );
        assert_eq!(
            wire_payload_len as usize,
            payload.len(),
            "nothing was cut, and saying so is what asserts it"
        );
        assert_eq!(*dst.ip(), GROUP);
        assert_eq!(dst.port(), MKTDATA_PORT);
        assert_eq!(role, PortRole::Mktdata);
        assert_eq!(kind, RecvTsKind::KernelSoftware);
        assert_eq!(drop_delta, 0, "the open poll is the baseline");
        assert!(ttl.is_some(), "captured off the IPv4 header");

        let stats = source.stats();
        assert!(stats.capture.datagrams >= 1);
        assert_eq!(stats.capture.queue_drops, 0);
        assert_eq!(stats.skipped_unmapped_port, 0);
        assert_eq!(source.capture_drops(), 0);
        assert_eq!(source.interface_drops(), 0);
    }
}
