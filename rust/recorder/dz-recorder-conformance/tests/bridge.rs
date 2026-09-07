//! The three things the promoted bridge must not assert.
//!
//! The version of this conversion that lived in `dz-recorder-e2e` was correct
//! for the fixtures it served — one group, synthesised headers, nothing
//! truncated — and each of those was an assumption rather than a property. Here
//! each one is a datagram the archive really can hold, and what the file says
//! about it is read back out of the bytes rather than out of the writer's own
//! intent.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;

use dz_edge_core::PortRole;
use dz_recorder_conformance::pcap::{
    pcap_len, write_group_pcaps, write_pcap, FILE_HEADER_LEN, RECORD_HEADER_LEN,
    SYNTHESISED_LINK_HEADER_LEN,
};
use dz_recorder_core::RecvTsKind;
use dz_recorder_replay::OwnedDatagram;

const PUBLISHER_A: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const GROUP_A: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const GROUP_B: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 11);
const EGRESS_PORT: u16 = 41_000;
const RECV_TS_NS: u64 = 1_772_000_000_123_456_789;

/// One record as a reader gets it back: the two lengths and the frame.
#[derive(Debug, PartialEq, Eq)]
struct Record {
    secs: u32,
    micros: u32,
    incl_len: u32,
    orig_len: u32,
    frame: Vec<u8>,
}

/// Reads the file the way the tool would, and not the way the writer wrote it.
///
/// A test that asserted against the writer's own arithmetic would agree with
/// whatever the writer did, which is the failure the whole crate is arranged
/// against one layer up.
fn read_pcap(path: &Path) -> Vec<Record> {
    let bytes = std::fs::read(path).expect("the file the bridge wrote is readable");
    assert_eq!(
        &bytes[0..4],
        &0xa1b2_c3d4u32.to_le_bytes(),
        "classic pcap, little-endian, microsecond resolution"
    );
    assert_eq!(
        &bytes[20..24],
        &1u32.to_le_bytes(),
        "LINKTYPE_ETHERNET, so the 42 bytes in front of the payload are read as a frame"
    );

    let mut out = Vec::new();
    let mut at = FILE_HEADER_LEN;
    while at < bytes.len() {
        let word = |off: usize| {
            u32::from_le_bytes(
                bytes[at + off..at + off + 4]
                    .try_into()
                    .expect("a record header is sixteen bytes"),
            )
        };
        let incl_len = word(8);
        let frame_at = at + RECORD_HEADER_LEN;
        let frame_end = frame_at + incl_len as usize;
        out.push(Record {
            secs: word(0),
            micros: word(4),
            incl_len,
            orig_len: word(12),
            frame: bytes[frame_at..frame_end].to_vec(),
        });
        at = frame_end;
    }
    out
}

fn datagram(group: Ipv4Addr, role: PortRole, payload: Vec<u8>) -> OwnedDatagram {
    let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
    OwnedDatagram {
        payload,
        src: SocketAddrV4::new(PUBLISHER_A, EGRESS_PORT),
        dst: SocketAddrV4::new(group, port_of(role)),
        role,
        recv_ts_ns: RECV_TS_NS,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta: 0,
        ttl: Some(4),
        link_headers: None,
        wire_payload_len,
    }
}

fn port_of(role: PortRole) -> u16 {
    match role {
        PortRole::Mktdata => 40_000,
        PortRole::Refdata => 40_001,
        PortRole::Snapshot => 40_002,
    }
}

/// Forty-two bytes with a value in every field a synthesised header leaves at
/// zero, so that *reproduced* and *rebuilt* cannot be confused.
fn captured_link_headers(payload_len: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(SYNTHESISED_LINK_HEADER_LEN);
    out.extend_from_slice(&[0xaa; 6]); // a real destination MAC
    out.extend_from_slice(&[0xbb; 6]); // a real source MAC
    out.extend_from_slice(&0x0800u16.to_be_bytes());
    out.push(0x45);
    out.push(0xb8); // DSCP the sender set
    out.extend_from_slice(&(20 + 8 + payload_len).to_be_bytes());
    out.extend_from_slice(&0x1234u16.to_be_bytes()); // identification
    out.extend_from_slice(&0x4000u16.to_be_bytes()); // don't fragment
    out.push(64); // the TTL as it arrived
    out.push(17);
    out.extend_from_slice(&0xfeedu16.to_be_bytes()); // the header checksum as computed
    out.extend_from_slice(&PUBLISHER_A.octets());
    out.extend_from_slice(&GROUP_A.octets());
    out.extend_from_slice(&EGRESS_PORT.to_be_bytes());
    out.extend_from_slice(&port_of(PortRole::Mktdata).to_be_bytes());
    out.extend_from_slice(&(8 + payload_len).to_be_bytes());
    out.extend_from_slice(&0xcafeu16.to_be_bytes()); // a real UDP checksum
    assert_eq!(out.len(), SYNTHESISED_LINK_HEADER_LEN);
    out
}

#[test]
fn a_truncated_datagram_writes_an_included_length_below_its_original() {
    // `wire_payload_len` beside a shorter payload is what the archive means by
    // *the capture cut this short*. Writing one length into both fields would
    // declare it complete, which hands the rule set a body shorter than the
    // header in it declares — a structural violation with our snaplen behind
    // it, filed against the publisher.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcap");
    let mut dg = datagram(GROUP_A, PortRole::Mktdata, vec![7u8; 200]);
    dg.wire_payload_len = 1_500;

    write_pcap(&path, [&dg]).expect("the bridge writes");
    let records = read_pcap(&path);

    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0].incl_len,
        (SYNTHESISED_LINK_HEADER_LEN + 200) as u32,
        "the included length is what follows the record header"
    );
    assert_eq!(
        records[0].orig_len,
        (SYNTHESISED_LINK_HEADER_LEN + 1_500) as u32,
        "and the original length is what the publisher sent"
    );
    assert!(
        records[0].orig_len > records[0].incl_len,
        "so a reader can see that it is looking at less than the whole datagram"
    );
    assert_eq!(
        records[0].frame.len(),
        SYNTHESISED_LINK_HEADER_LEN + 200,
        "only the bytes the archive holds are written; nothing is padded out"
    );

    // And the header in front of the payload states the same thing, so the
    // truncation is not hidden a second time one layer down.
    let udp_len = u16::from_be_bytes(records[0].frame[38..40].try_into().expect("the UDP length"));
    assert_eq!(udp_len, 8 + 1_500, "the UDP length is what was sent");
}

#[test]
fn an_untruncated_datagram_writes_the_two_lengths_equal() {
    // The other half of the pair, and the one that makes the assertion above an
    // assertion rather than an unconditional inequality: a bridge that always
    // wrote a longer original length would report every datagram truncated.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcap");
    let dg = datagram(GROUP_A, PortRole::Mktdata, vec![7u8; 200]);

    write_pcap(&path, [&dg]).expect("the bridge writes");
    let records = read_pcap(&path);

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].incl_len, records[0].orig_len);
    assert_eq!(
        records[0].incl_len,
        (SYNTHESISED_LINK_HEADER_LEN + 200) as u32
    );
    let udp_len = u16::from_be_bytes(records[0].frame[38..40].try_into().expect("the UDP length"));
    assert_eq!(udp_len, 8 + 200);
}

#[test]
fn captured_link_headers_are_reproduced_rather_than_synthesised() {
    // `link_headers: Some(..)` means the capture mode read these bytes off the
    // interface, and the manifest says so for the whole object. Rebuilding over
    // them discards the identification field, the fragmentation flags and the
    // checksums the archive kept on purpose, and presents a fiction to any rule
    // that reads below UDP.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcap");
    let payload = vec![9u8; 120];
    let captured = captured_link_headers(120);
    let mut dg = datagram(GROUP_A, PortRole::Mktdata, payload.clone());
    dg.link_headers = Some(captured.clone());

    write_pcap(&path, [&dg]).expect("the bridge writes");
    let records = read_pcap(&path);

    assert_eq!(records.len(), 1);
    assert_eq!(
        &records[0].frame[..SYNTHESISED_LINK_HEADER_LEN],
        &captured[..],
        "byte for byte, including the fields a synthesised header has no value for"
    );
    assert_eq!(
        &records[0].frame[SYNTHESISED_LINK_HEADER_LEN..],
        &payload[..]
    );

    // The same datagram without the captured bytes, so that the assertion above
    // is not satisfied by a synthesised header that happens to agree.
    let mut synthesised = datagram(GROUP_A, PortRole::Mktdata, payload);
    synthesised.link_headers = None;
    let other = dir.path().join("synthesised.pcap");
    write_pcap(&other, [&synthesised]).expect("the bridge writes");
    assert_ne!(
        read_pcap(&other)[0].frame[..SYNTHESISED_LINK_HEADER_LEN],
        captured[..],
        "a synthesised header cannot carry the TTL, the identification or the checksums"
    );
}

#[test]
fn two_groups_in_one_archive_produce_two_files_each_holding_only_its_own() {
    // The tool takes one `-group`. A single file holding both would be read
    // under one group's flags, and the other group's datagrams would be
    // evaluated by nothing at all while the exit code still said zero.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let datagrams = vec![
        datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]),
        datagram(GROUP_B, PortRole::Mktdata, vec![2u8; 40]),
        datagram(GROUP_A, PortRole::Snapshot, vec![3u8; 50]),
    ];

    let files = write_group_pcaps(dir.path(), &datagrams).expect("the bridge writes");

    assert_eq!(
        files.iter().map(|f| f.group).collect::<Vec<_>>(),
        vec![GROUP_A, GROUP_B],
        "one file per group, ordered so that two runs over one object agree"
    );
    assert_eq!(files[0].datagram_count, 2);
    assert_eq!(files[1].datagram_count, 1);

    let a = read_pcap(&files[0].path);
    assert_eq!(
        a.iter()
            .map(|r| r.frame[SYNTHESISED_LINK_HEADER_LEN])
            .collect::<Vec<_>>(),
        vec![1, 3],
        "group A's file holds group A's datagrams, in arrival order"
    );
    let b = read_pcap(&files[1].path);
    assert_eq!(
        b.iter()
            .map(|r| r.frame[SYNTHESISED_LINK_HEADER_LEN])
            .collect::<Vec<_>>(),
        vec![2],
        "and group B's holds only its own"
    );

    // The destination address in each frame is the group the file is named for,
    // so a file pointed at the wrong `-group` cannot look right.
    for (file, records) in [(&files[0], &a), (&files[1], &b)] {
        for record in records {
            let dst: [u8; 4] = record.frame[30..34]
                .try_into()
                .expect("the IPv4 destination");
            assert_eq!(Ipv4Addr::from(dst), file.group);
        }
    }
}

#[test]
fn the_predicted_size_is_the_size_written() {
    // The manifest states the datagram count and the payload bytes before the
    // object is opened, and the runner refuses an object it has no room for on
    // that prediction. A prediction that disagreed with the writer would fill
    // the disk the archive is staged on.
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcap");
    let mut truncated = datagram(GROUP_A, PortRole::Mktdata, vec![4u8; 60]);
    truncated.wire_payload_len = 9_000;
    // A captured header that is not 42 bytes, which is what a VLAN tag makes it
    // and what a prediction assuming the synthesised length gets wrong. The
    // headers the capture mode kept are whatever preceded the payload, and the
    // prediction has to be over those rather than over the ones this crate
    // would have written.
    let mut captured = datagram(GROUP_A, PortRole::Refdata, vec![5u8; 70]);
    let mut tagged = captured_link_headers(70);
    tagged.splice(12..12, [0x81, 0x00, 0x00, 0x64]);
    assert_eq!(tagged.len(), SYNTHESISED_LINK_HEADER_LEN + 4);
    captured.link_headers = Some(tagged);
    let datagrams = vec![
        datagram(GROUP_A, PortRole::Mktdata, vec![3u8; 80]),
        truncated,
        captured,
    ];

    write_pcap(&path, &datagrams).expect("the bridge writes");

    assert_eq!(
        pcap_len(&datagrams),
        std::fs::metadata(&path).expect("the file exists").len(),
        "the bytes written are the bytes predicted, truncation and captured headers included"
    );
}
