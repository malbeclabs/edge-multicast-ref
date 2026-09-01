//! Fixtures shared by the archive's test binaries.
//!
//! Every helper here builds its bytes by hand at the offsets the spec states,
//! so a test never agrees with the implementation merely by calling it.
#![allow(dead_code)]

use std::io::Cursor;
use std::net::SocketAddrV4;
use std::path::Path;
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{CaptureDropScope, LinkHeaders, RoleJoin, SegmentWriterConfig};
use dz_recorder_archive::Compression;
use dz_recorder_core::{RecordedDatagram, RecorderIdentity, RecvTsKind};
use pcap_file::pcapng::blocks::enhanced_packet::EnhancedPacketBlock;
use pcap_file::pcapng::blocks::interface_description::{
    InterfaceDescriptionBlock, InterfaceDescriptionOption,
};
use pcap_file::pcapng::blocks::interface_statistics::InterfaceStatisticsBlock;
use pcap_file::pcapng::blocks::section_header::SectionHeaderBlock;
use pcap_file::pcapng::{Block, PcapNgReader};

/// A stamp with all nine digits populated: a writer that rounds to
/// microseconds cannot pass a comparison against it.
pub const KNOWN_RECV_TS_NS: u64 = 1_700_000_000_123_456_789;

pub const DATAGRAM_HEADER_SIZE: usize = 24;

/// MCAST-TEST-NET (RFC 2365) and the RFC 5737 documentation ranges. A fixture
/// on `10.0.0.0/8` or `239.0.0.0/8` names a network an operator really runs, and
/// an address in a test is copied into a configuration sooner or later.
pub const GROUP: &str = "233.252.0.10";
pub const SOURCE: &str = "192.0.2.1";
pub const SECOND_SOURCE: &str = "192.0.2.2";
pub const OTHER_SOURCE: &str = "198.51.100.7";
pub const JOIN_INTERFACE: &str = "gre1";
pub const JOIN_SOURCE: &str = "192.0.2.10";

pub fn at_secs(secs: u64) -> u64 {
    secs * 1_000_000_000
}

pub fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

/// The port each role is joined on in every fixture, so a manifest row and a
/// datagram's destination port can be compared.
pub fn port_of(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => 40000,
        PortRole::Refdata => 40001,
        PortRole::Snapshot => 40002,
    }
}

/// The intent behind one join: the group, the port, the interface and the source
/// address, which is what a reader needs to tell "joined and silent" from
/// "joined on the wrong port and silent".
pub fn join(role: PortRole) -> RoleJoin {
    RoleJoin {
        role,
        group: GROUP.parse().expect("fixture group"),
        port: port_of(role),
        interface: Some(JOIN_INTERFACE.to_owned()),
        source: Some(JOIN_SOURCE.parse().expect("fixture source")),
    }
}

pub fn joins(roles: &[PortRole]) -> Vec<RoleJoin> {
    roles.iter().copied().map(join).collect()
}

pub fn writer_config(roles_joined: &[PortRole]) -> SegmentWriterConfig {
    SegmentWriterConfig {
        identity: identity(),
        roles_joined: joins(roles_joined),
        link_headers: LinkHeaders::Synthesised,
        // Socket mode's scope, because socket mode really does hold one
        // accumulator per role. A fixture that claimed the other scope would be
        // claiming a fact about a capture handle no fixture has.
        capture_drop_scope: CaptureDropScope::PortRole,
    }
}

pub fn archive_config(
    staging: &Path,
    completed: &Path,
    roles_joined: &[PortRole],
) -> ArchiveWriterConfig {
    ArchiveWriterConfig {
        staging_dir: staging.to_path_buf(),
        completed_dir: completed.to_path_buf(),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(60),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: identity(),
        feed: "top-of-book".to_owned(),
        roles_joined: joins(roles_joined),
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    }
}

/// The 24-byte datagram header, written at the offsets in the spec's table:
/// `Channel ID` at 3, `Sequence Number` at 4, `Reset Count` at 21.
pub fn header_bytes(channel_id: u8, seq: u64, reset_count: u8, schema_version: u8) -> Vec<u8> {
    let mut buf = vec![0u8; DATAGRAM_HEADER_SIZE];
    buf[0..2].copy_from_slice(&0xA1B2u16.to_le_bytes());
    buf[2] = schema_version;
    buf[3] = channel_id;
    buf[4..12].copy_from_slice(&seq.to_le_bytes());
    buf[12..20].copy_from_slice(&KNOWN_RECV_TS_NS.to_le_bytes());
    buf[20] = 1;
    buf[21] = reset_count;
    buf[22..24].copy_from_slice(&(DATAGRAM_HEADER_SIZE as u16).to_le_bytes());
    buf
}

pub fn src(addr: &str) -> SocketAddrV4 {
    addr.parse().expect("fixture address")
}

pub fn datagram<'a>(payload: &'a [u8], src_addr: &str, dst_addr: &str) -> RecordedDatagram<'a> {
    RecordedDatagram {
        payload,
        src: src(src_addr),
        dst: src(dst_addr),
        role: PortRole::Mktdata,
        recv_ts_ns: KNOWN_RECV_TS_NS,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta: 0,
        ttl: Some(4),
        link_headers: None,
        wire_payload_len: payload.len() as u32,
    }
}

/// A datagram whose payload is a well-formed header, so the coverage tracker
/// has something to read.
pub fn sequenced<'a>(payload: &'a [u8], src_addr: &str) -> RecordedDatagram<'a> {
    datagram(payload, src_addr, &format!("{GROUP}:40000"))
}

/// Ethernet, IPv4 and UDP as `AF_PACKET` mode reads them off the interface,
/// built by hand at the offsets the headers define.
///
/// Every field a synthesis has to leave at zero carries a distinctive value
/// here — the MAC addresses, the identification, the flags and both checksums —
/// so a writer that rebuilds these bytes cannot produce them by accident.
pub fn captured_link_headers(src: &str, dst: &str, ttl: u8, payload_len: usize) -> Vec<u8> {
    let src: SocketAddrV4 = src.parse().expect("fixture address");
    let dst: SocketAddrV4 = dst.parse().expect("fixture address");
    let total_len = u16::try_from(20 + 8 + payload_len).expect("fixture payload");
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
    out.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x02]);
    out.extend_from_slice(&0x0800u16.to_be_bytes());

    out.push(0x45);
    out.push(0);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&0x1234u16.to_be_bytes());
    out.extend_from_slice(&0x4000u16.to_be_bytes());
    out.push(ttl);
    out.push(17);
    out.extend_from_slice(&0xbeefu16.to_be_bytes());
    out.extend_from_slice(&src.ip().octets());
    out.extend_from_slice(&dst.ip().octets());

    out.extend_from_slice(&src.port().to_be_bytes());
    out.extend_from_slice(&dst.port().to_be_bytes());
    out.extend_from_slice(
        &(u16::try_from(8 + payload_len).expect("fixture payload")).to_be_bytes(),
    );
    out.extend_from_slice(&0xcafeu16.to_be_bytes());
    out
}

pub fn blocks(bytes: &[u8]) -> Vec<Block<'static>> {
    let mut reader = PcapNgReader::new(Cursor::new(bytes)).expect("pcapng section header");
    let mut out = Vec::new();
    while let Some(block) = reader.next_block() {
        out.push(block.expect("block parses").into_owned());
    }
    out
}

/// The reader consumes the section header on construction, which is itself the
/// assertion that there is exactly one and that it comes first.
pub fn first_section_header(bytes: &[u8]) -> SectionHeaderBlock<'static> {
    let reader = PcapNgReader::new(Cursor::new(bytes)).expect("one section header per segment");
    reader.section().clone()
}

pub fn interface_blocks(bytes: &[u8]) -> Vec<InterfaceDescriptionBlock<'static>> {
    blocks(bytes)
        .into_iter()
        .filter_map(Block::into_interface_description)
        .collect()
}

pub fn packet_blocks(bytes: &[u8]) -> Vec<EnhancedPacketBlock<'static>> {
    blocks(bytes)
        .into_iter()
        .filter_map(Block::into_enhanced_packet)
        .collect()
}

pub fn statistics_blocks(bytes: &[u8]) -> Vec<InterfaceStatisticsBlock<'static>> {
    blocks(bytes)
        .into_iter()
        .filter_map(Block::into_interface_statistics)
        .collect()
}

pub fn if_description(idb: &InterfaceDescriptionBlock<'_>) -> String {
    idb.options
        .iter()
        .find_map(|o| match o {
            InterfaceDescriptionOption::IfDescription(d) => Some(d.to_string()),
            _ => None,
        })
        .expect("every interface description block states what was joined")
}

pub fn if_name(idb: &InterfaceDescriptionBlock<'_>) -> String {
    idb.options
        .iter()
        .find_map(|o| match o {
            InterfaceDescriptionOption::IfName(n) => Some(n.to_string()),
            _ => None,
        })
        .expect("every interface description block names its port role")
}

/// Writes `payload_bytes` worth of datagrams into an archive writer.
pub fn write_bytes(w: &mut ArchiveWriter, payload_bytes: usize) {
    let payload = header_bytes(1, 0, 0, 3);
    let mut written = 0;
    while written < payload_bytes {
        let dg = sequenced(&payload, &format!("{SOURCE}:40000"));
        dz_recorder_core::Sink::write(w, &dg).expect("the write path never fails the caller");
        written += payload.len();
    }
}
