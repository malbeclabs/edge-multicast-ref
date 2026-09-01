//! The blocks a segment is made of, and the metadata each one has to carry.

mod common;

use common::{
    captured_link_headers, first_section_header, header_bytes, if_description, if_name,
    interface_blocks, packet_blocks, sequenced, statistics_blocks, writer_config, GROUP,
    JOIN_INTERFACE, JOIN_SOURCE, KNOWN_RECV_TS_NS, OTHER_SOURCE, SOURCE,
};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_recorder_archive::writer::{
    CaptureDropScope, LinkHeaders, SegmentWriter, LINK_HEADER_LEN, MAX_LINK_HEADER_LEN,
};
use dz_recorder_core::RecvTsKind;
use pcap_file::pcapng::blocks::enhanced_packet::EnhancedPacketOption;
use pcap_file::pcapng::blocks::interface_description::InterfaceDescriptionOption;
use pcap_file::pcapng::blocks::interface_statistics::InterfaceStatisticsOption;
use pcap_file::pcapng::blocks::section_header::SectionHeaderOption;
use pcap_file::Endianness;

const ALL_THREE: [PortRole; 3] = [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot];

fn write_one_segment() -> Vec<u8> {
    write_payload(&header_bytes(1, 100, 0, 3))
}

fn write_payload(payload: &[u8]) -> Vec<u8> {
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata]))
        .expect("segment opens");
    w.write(&sequenced(payload, &format!("{SOURCE}:40000")))
        .unwrap();
    w.finish().unwrap().0
}

fn write_three_roles() -> Vec<u8> {
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&ALL_THREE)).expect("segment opens");
    let payload = header_bytes(1, 1, 0, 3);
    for role in ALL_THREE {
        let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
        dg.role = role;
        w.write(&dg).unwrap();
    }
    w.finish().unwrap().0
}

fn write_with_drop_deltas(deltas: &[u32]) -> Vec<u8> {
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata]))
        .expect("segment opens");
    let payload = header_bytes(1, 1, 0, 3);
    for delta in deltas {
        let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
        dg.drop_delta = *delta;
        w.write(&dg).unwrap();
    }
    w.finish().unwrap().0
}

#[test]
fn the_section_header_carries_the_recorder_identity() {
    // A self-describing archive cannot be separated from its provenance. An
    // archive copied, renamed or pulled out of a bucket by hand still knows
    // which recorder, which build and which configuration wrote it.
    let bytes = write_one_segment();
    let shb = first_section_header(&bytes);
    assert!(shb
        .options
        .iter()
        .any(|o| matches!(o, SectionHeaderOption::Hardware(h) if h == "site-1/recorder-1")));
    assert!(shb.options.iter().any(
        |o| matches!(o, SectionHeaderOption::UserApplication(u) if u.starts_with("dz-recorder/"))
    ));
    assert!(shb
        .options
        .iter()
        .any(|o| matches!(o, SectionHeaderOption::OS(_))));

    let comment = section_comment(&bytes);
    for expected in [
        "site=site-1",
        "recorder=recorder-1",
        "env=test",
        "build_version=0.1.0",
        "build_commit=0000000",
    ] {
        assert!(
            comment.contains(expected),
            "{expected} missing from {comment}"
        );
    }
    assert!(comment.contains(&format!("config_hash={}", "a".repeat(64))));
}

#[test]
fn one_interface_description_block_per_port_role() {
    // The manifest states the intent. A port that was never joined produces no
    // data, and no data looks exactly like a clean feed.
    let idbs = interface_blocks(&write_three_roles());
    let names: Vec<_> = idbs.iter().map(if_name).collect();
    assert_eq!(names, ["mktdata", "refdata", "snapshot"]);
}

#[test]
fn the_interface_order_is_fixed_whatever_the_recorder_joined() {
    // interface_id has to mean the same thing in every segment of every run, so
    // a reader maps it to a port role without reading options at all.
    let joined_one = interface_blocks(&write_one_segment());
    let joined_three = interface_blocks(&write_three_roles());
    let names = |v: &[_]| v.iter().map(if_name).collect::<Vec<_>>();
    assert_eq!(names(&joined_one), names(&joined_three));
}

#[test]
fn timestamps_are_nanoseconds_not_microseconds() {
    // pcapng's default resolution is 10^-6. A recorder taking kernel nanosecond
    // stamps and writing them at microsecond resolution silently discards the
    // three digits the whole latency argument rests on.
    let bytes = write_one_segment();
    for idb in interface_blocks(&bytes) {
        assert!(idb
            .options
            .iter()
            .any(|o| matches!(o, InterfaceDescriptionOption::IfTsResol(9))));
    }
    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(epb.timestamp.as_nanos() as u64, KNOWN_RECV_TS_NS);
}

#[test]
fn the_drop_delta_travels_inside_the_archive() {
    let bytes = write_with_drop_deltas(&[0, 0, 7]);
    let epbs = packet_blocks(&bytes);
    assert!(epbs[2]
        .options
        .iter()
        .any(|o| matches!(o, EnhancedPacketOption::DropCount(7))));
    assert!(
        !epbs[0]
            .options
            .iter()
            .any(|o| matches!(o, EnhancedPacketOption::DropCount(_))),
        "a zero delta writes no option; every datagram carrying one is noise"
    );
}

#[test]
fn a_datagram_is_written_whole_and_verbatim() {
    let payload: Vec<u8> = (0u8..=255).collect();
    let bytes = write_payload(&payload);
    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(
        &epb.data[LINK_HEADER_LEN..],
        &payload[..],
        "no truncation, no byte swap"
    );
    assert_eq!(epb.original_len as usize, LINK_HEADER_LEN + payload.len());
}

#[test]
fn the_synthesised_headers_say_where_the_datagram_came_from_and_went() {
    // Replay recovers src, dst and ttl from these bytes, so the fields have to
    // be the observed ones rather than a plausible-looking constant.
    let payload = header_bytes(1, 1, 0, 3);
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    w.write(&sequenced(&payload, &format!("{OTHER_SOURCE}:40100")))
        .unwrap();
    let bytes = w.finish().unwrap().0;

    let data = &packet_blocks(&bytes)[0].data;
    assert_eq!(&data[12..14], &[0x08, 0x00], "ethertype ipv4");
    assert_eq!(data[14], 0x45, "ipv4, no options");
    assert_eq!(
        u16::from_be_bytes([data[16], data[17]]) as usize,
        20 + 8 + payload.len()
    );
    assert_eq!(data[22], 4, "the observed ttl, not a guess");
    assert_eq!(data[23], 17, "udp");
    assert_eq!(&data[26..30], &[198, 51, 100, 7]);
    assert_eq!(&data[30..34], &[233, 252, 0, 10]);
    assert_eq!(u16::from_be_bytes([data[34], data[35]]), 40100);
    assert_eq!(u16::from_be_bytes([data[36], data[37]]), 40000);
    assert_eq!(
        u16::from_be_bytes([data[38], data[39]]) as usize,
        8 + payload.len()
    );
}

#[test]
fn a_synthesised_link_header_is_recorded_as_synthesised() {
    // A synthesised field must never be mistaken for a captured one, and the
    // only place that survives a copy out of the bucket is the archive itself.
    assert!(section_comment(&write_one_segment()).contains("link_headers=synthesised"));
}

#[test]
fn an_unobserved_ttl_is_not_written_as_an_observed_zero() {
    let payload = header_bytes(1, 1, 0, 3);
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.ttl = None;
    w.write(&dg).unwrap();
    let bytes = w.finish().unwrap().0;

    assert_eq!(packet_blocks(&bytes)[0].data[22], 0);
    assert!(section_comment(&bytes).contains("ttl_zero_means_unobserved"));
}

#[test]
fn an_application_fallback_stamp_is_marked_on_the_datagram_that_carries_one() {
    // The section states the kind the capture handle produces; a datagram that
    // fell back says so on itself, because a latency computed from an
    // application stamp measures our own scheduler.
    let payload = header_bytes(1, 1, 0, 3);
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    w.write(&sequenced(&payload, &format!("{SOURCE}:40000")))
        .unwrap();
    let mut fell_back = sequenced(&payload, &format!("{SOURCE}:40000"));
    fell_back.recv_ts_kind = RecvTsKind::ApplicationFallback;
    w.write(&fell_back).unwrap();
    let bytes = w.finish().unwrap().0;

    let epbs = packet_blocks(&bytes);
    let comment_of = |epb: &pcap_file::pcapng::blocks::enhanced_packet::EnhancedPacketBlock| {
        epb.options.iter().find_map(|o| match o {
            EnhancedPacketOption::Comment(c) => Some(c.to_string()),
            _ => None,
        })
    };
    assert!(
        comment_of(&epbs[0]).is_none(),
        "the section default writes no option"
    );
    assert_eq!(
        comment_of(&epbs[1]).as_deref(),
        Some("recv_ts_kind=application-fallback")
    );
    assert!(section_comment(&bytes).contains("recv_ts_kind=kernel-software"));
}

#[test]
fn the_interface_statistics_blocks_close_the_segment_with_its_counters() {
    let payload = header_bytes(1, 1, 0, 3);
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.drop_delta = 3;
    w.write(&dg).unwrap();
    w.record_interface_drops(PortRole::Mktdata, 6);
    let bytes = w.finish().unwrap().0;

    let isbs = statistics_blocks(&bytes);
    assert_eq!(isbs.len(), 1, "one per joined port role, and no others");
    assert_eq!(isbs[0].interface_id, 0);
    assert!(isbs[0]
        .options
        .iter()
        .any(|o| matches!(o, InterfaceStatisticsOption::IsbIfRecv(1))));
    assert!(
        isbs[0]
            .options
            .iter()
            .any(|o| matches!(o, InterfaceStatisticsOption::IsbOsDrop(3))),
        "our own overflow is what a gap is subtracted against"
    );
    assert!(
        isbs[0]
            .options
            .iter()
            .any(|o| matches!(o, InterfaceStatisticsOption::IsbIfDrop(6))),
        "loss upstream of the capture point is a separate category"
    );
}

#[test]
fn the_archive_is_little_endian_whatever_the_host_is() {
    // The byte-order magic states it, and the payload is never byte-swapped, so
    // a reader on any host sees the bytes that arrived.
    let bytes = write_one_segment();
    assert_eq!(first_section_header(&bytes).endianness, Endianness::Little);
    assert_eq!(&bytes[8..12], &[0x4d, 0x3c, 0x2b, 0x1a]);
}

#[test]
fn the_interface_description_block_carries_the_group_port_and_interface_joined() {
    // The design's pcapng table asks each interface block for the group, the
    // port, the port role, the interface joined and the source address at join
    // time. Without them the archive can say "the snapshot port was silent" but
    // not "the snapshot port was joined on the wrong port and silent", and a
    // reader cannot map a coverage row's port back to a stated intent.
    let idbs = interface_blocks(&write_three_roles());
    let mktdata = if_description(&idbs[0]);
    for expected in [
        "port_role=mktdata",
        "joined=true",
        &format!("group={GROUP}"),
        "port=40000",
        &format!("interface={JOIN_INTERFACE}"),
        &format!("source={JOIN_SOURCE}"),
    ] {
        assert!(
            mktdata.contains(expected),
            "{expected} missing from {mktdata}"
        );
    }
    assert!(
        if_description(&idbs[2]).contains("port=40002"),
        "one port per role"
    );

    // A role nobody joined states no group and no port: absent, never a zero
    // somebody reads as an address.
    let not_joined = if_description(&interface_blocks(&write_one_segment())[2]);
    assert_eq!(not_joined, "port_role=snapshot; joined=false");
}

#[test]
fn captured_link_headers_are_kept_and_not_rebuilt() {
    // AF_PACKET mode exists to keep the fields a socket discards: the
    // identification, the fragmentation flags and the checksums are how a reader
    // tells a fragmented or twice-delivered datagram from a clean one, and a
    // rebuild replaces every one of them with a zero.
    let payload = header_bytes(1, 1, 0, 3);
    let headers = captured_link_headers(
        &format!("{OTHER_SOURCE}:40100"),
        &format!("{GROUP}:40000"),
        7,
        payload.len(),
    );
    let mut cfg = writer_config(&[PortRole::Mktdata]);
    cfg.link_headers = LinkHeaders::Captured;
    let mut w = SegmentWriter::new(Vec::new(), &cfg).unwrap();
    let mut dg = sequenced(&payload, &format!("{OTHER_SOURCE}:40100"));
    dg.link_headers = Some(&headers);
    w.write(&dg).unwrap();
    let (bytes, stats) = w.finish().unwrap();

    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(
        &epb.data[..LINK_HEADER_LEN],
        &headers[..],
        "the bytes that arrived, verbatim"
    );
    assert_eq!(&epb.data[LINK_HEADER_LEN..], &payload[..]);
    assert!(section_comment(&bytes).contains("link_headers=captured"));
    assert_eq!(
        stats.link_header_exceptions, 0,
        "the datagrams agree with the claim, so there is nothing to except"
    );
    assert!(
        !epb.options
            .iter()
            .any(|o| matches!(o, EnhancedPacketOption::Comment(_))),
        "the section default writes no option"
    );
}

#[test]
fn a_synthesised_datagram_under_a_captured_claim_says_so_and_is_counted() {
    // The section states the mode before any datagram arrives, so the exception
    // is what needs saying — and a configured claim the datagrams contradict
    // must not be able to pass silently.
    let payload = header_bytes(1, 1, 0, 3);
    let mut cfg = writer_config(&[PortRole::Mktdata]);
    cfg.link_headers = LinkHeaders::Captured;
    let mut w = SegmentWriter::new(Vec::new(), &cfg).unwrap();
    w.write(&sequenced(&payload, &format!("{SOURCE}:40000")))
        .unwrap();
    let (bytes, stats) = w.finish().unwrap();

    let epb = &packet_blocks(&bytes)[0];
    assert!(
        epb.options.iter().any(
            |o| matches!(o, EnhancedPacketOption::Comment(c) if c == "link_headers=synthesised")
        ),
        "a rebuilt header under a captured claim is marked on the datagram"
    );
    assert_eq!(stats.link_header_exceptions, 1);
}

#[test]
fn an_over_cap_datagram_is_not_declared_whole() {
    // pcapng's original_len is the on-wire length, so setting it from the
    // captured length asserts *not truncated*: a publisher's over-cap datagram
    // becomes a clean one and the violation worth recording disappears.
    let payload = header_bytes(1, 1, 0, 3);
    let cut_by = 400u32;
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.wire_payload_len = payload.len() as u32 + cut_by;
    w.write(&dg).unwrap();
    let bytes = w.finish().unwrap().0;

    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(
        epb.data.len(),
        LINK_HEADER_LEN + payload.len(),
        "captured_len is what is actually held"
    );
    assert_eq!(
        epb.original_len as usize,
        LINK_HEADER_LEN + payload.len() + cut_by as usize,
        "original_len is what was on the wire, so a reader sees the truncation"
    );
}

fn section_comment(bytes: &[u8]) -> String {
    first_section_header(bytes)
        .options
        .iter()
        .find_map(|o| match o {
            SectionHeaderOption::Comment(c) => Some(c.to_string()),
            _ => None,
        })
        .expect("the section header carries a provenance comment")
}

/// Every block in the segment as it lies on the disk: type, offset and total
/// length, walked from the byte-order magic rather than through `pcap-file`'s
/// reader, which is the side under test in two of the assertions below.
fn raw_blocks(bytes: &[u8]) -> Vec<(u32, usize, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 12 <= bytes.len() {
        let type_ = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let len = u32::from_le_bytes(bytes[at + 4..at + 8].try_into().unwrap()) as usize;
        assert!(
            len >= 12 && len.is_multiple_of(4),
            "block length {len} at {at}"
        );
        out.push((type_, at, len));
        at += len;
    }
    assert_eq!(at, bytes.len(), "the blocks tile the segment exactly");
    out
}

const INTERFACE_STATISTICS_BLOCK: u32 = 0x0000_0005;

#[test]
fn the_interface_statistics_timestamp_is_written_high_word_first() {
    // pcapng defines this field as Timestamp (High) then Timestamp (Low), two
    // 32-bit words in the section's byte order. pcap-file 2.0.0 writes it with
    // one write_u64 — its enhanced_packet.rs splits correctly, this block does
    // not — so on a little-endian archive the low half lands first and a
    // conforming reader shows a nonsense capture-end time on every segment.
    // Asserted against the bytes: pcap-file's own reader reads the field back
    // the same wrong way and would pass either way.
    let bytes = write_one_segment();
    let (_, at, len) = *raw_blocks(&bytes)
        .iter()
        .find(|(type_, ..)| *type_ == INTERFACE_STATISTICS_BLOCK)
        .expect("the segment closes with its statistics block");
    assert!(len >= 20);

    let word = |offset: usize| u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap());
    // Type, block length, then interface_id: the timestamp words follow.
    let high = word(at + 12);
    let low = word(at + 16);
    assert_ne!(high, low, "the fixture stamp tells the halves apart");
    assert_eq!(
        high,
        (KNOWN_RECV_TS_NS >> 32) as u32,
        "the high word comes first, as the spec asks"
    );
    assert_eq!(low, (KNOWN_RECV_TS_NS & 0xffff_ffff) as u32);
}

#[test]
fn the_section_header_counts_towards_the_segment_the_rotation_bound_measures() {
    // rotate_bytes is a bound on the segment on disk, and pcap-file writes the
    // section header inside its own constructor without reporting a length. A
    // per-block tally therefore starts a fixed ~100 bytes short and every
    // segment rotates that much late.
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let payload = header_bytes(1, 1, 0, 3);
    w.write(&sequenced(&payload, &format!("{SOURCE}:40000")))
        .unwrap();
    let before_finish = w.bytes_written();
    let bytes = w.finish().unwrap().0;

    let statistics: usize = raw_blocks(&bytes)
        .iter()
        .filter(|(type_, ..)| *type_ == INTERFACE_STATISTICS_BLOCK)
        .map(|(_, _, len)| *len)
        .sum();
    assert!(statistics > 0);
    assert_eq!(
        before_finish as usize,
        bytes.len() - statistics,
        "every byte written before the statistics blocks is counted, the section header included"
    );
}

#[test]
fn a_captured_link_header_with_ipv4_options_does_not_grow_the_record_path_buffer() {
    // AF_PACKET mode slices the headers off the frame as they arrived, so an
    // IPv4 header carrying options makes them 82 bytes rather than the
    // synthesised 42. A buffer sized for the short case reallocates on the
    // record path for exactly those datagrams, which is an allocation per
    // datagram on the path that must not have one.
    let payload = vec![0xa5u8; MAX_DATAGRAM_SIZE];
    let mut headers = captured_link_headers(
        &format!("{OTHER_SOURCE}:40100"),
        &format!("{GROUP}:40000"),
        7,
        payload.len(),
    );
    // Every IPv4 option byte the header length field can express, so this is
    // the longest link header a capture can hand over.
    headers[14] = 0x4f;
    headers.splice(34..34, std::iter::repeat_n(0u8, 40));
    assert_eq!(headers.len(), MAX_LINK_HEADER_LEN);

    let mut cfg = writer_config(&[PortRole::Mktdata]);
    cfg.link_headers = LinkHeaders::Captured;
    let mut w = SegmentWriter::new(Vec::new(), &cfg).unwrap();
    let before = w.scratch_capacity();
    assert!(before >= MAX_DATAGRAM_SIZE + MAX_LINK_HEADER_LEN);

    let mut dg = sequenced(&payload, &format!("{OTHER_SOURCE}:40100"));
    dg.link_headers = Some(&headers);
    w.write(&dg).unwrap();

    assert_eq!(
        w.scratch_capacity(),
        before,
        "the record path's buffer did not have to grow"
    );
    let epb = &packet_blocks(&w.finish().unwrap().0)[0];
    assert_eq!(&epb.data[..headers.len()], &headers[..]);
    assert_eq!(&epb.data[headers.len()..], &payload[..]);
}

#[test]
fn a_datagram_past_the_ipv4_length_fields_clamps_rather_than_overflowing_them() {
    // wire_payload_len is a public field on a public struct and UDP over IPv4
    // sits exactly at this boundary: 65507 + 28 is 65535. One byte more used to
    // panic in debug and wrap to an IPv4 total length of 27 in release.
    let payload = header_bytes(1, 1, 0, 3);
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.wire_payload_len = 65_508;
    w.write(&dg).unwrap();
    let bytes = w.finish().unwrap().0;

    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(
        u16::from_be_bytes([epb.data[16], epb.data[17]]),
        u16::MAX,
        "an IPv4 header cannot express a longer datagram, so it says the most it can"
    );
    assert_eq!(
        u16::from_be_bytes([epb.data[38], epb.data[39]]),
        65_516,
        "the UDP length still fits at this size, and is not clamped for company"
    );
    assert_eq!(
        epb.original_len as usize,
        LINK_HEADER_LEN + 65_508,
        "and the length that arrived survives where pcapng can hold it"
    );

    // And past where the UDP length field runs out too.
    let mut w = SegmentWriter::new(Vec::new(), &writer_config(&[PortRole::Mktdata])).unwrap();
    let mut dg = sequenced(&payload, &format!("{SOURCE}:40000"));
    dg.wire_payload_len = u32::MAX;
    w.write(&dg).unwrap();
    let bytes = w.finish().unwrap().0;
    let epb = &packet_blocks(&bytes)[0];
    assert_eq!(u16::from_be_bytes([epb.data[16], epb.data[17]]), u16::MAX);
    assert_eq!(u16::from_be_bytes([epb.data[38], epb.data[39]]), u16::MAX);
}

#[test]
fn a_ring_s_drops_are_recorded_at_capture_handle_scope_and_not_charged_to_one_role() {
    // A ring counts frames dropped before demultiplexing, so if it drops forty
    // mktdata frames and the next datagram through the filter is refdata, a
    // per-role attribution charges forty to refdata. An analysis tier
    // subtracting per-role capture drops from per-role sequence gaps then reads a
    // forty-datagram mktdata gap with nothing admitted behind it, which is the
    // false publisher-loss finding this design exists to prevent.
    let payload = header_bytes(1, 1, 0, 3);
    let mut cfg = writer_config(&ALL_THREE);
    cfg.capture_drop_scope = CaptureDropScope::CaptureHandle;
    let mut w = SegmentWriter::new(Vec::new(), &cfg).unwrap();
    let mut refdata = sequenced(&payload, &format!("{SOURCE}:40000"));
    refdata.role = PortRole::Refdata;
    refdata.drop_delta = 40;
    w.write(&refdata).unwrap();
    let mut mktdata = sequenced(&payload, &format!("{SOURCE}:40000"));
    mktdata.role = PortRole::Mktdata;
    w.write(&mktdata).unwrap();
    let (bytes, stats) = w.finish().unwrap();

    assert!(
        section_comment(&bytes).contains("capture_drop_scope=capture-handle"),
        "the scope is stated in the archive: {}",
        section_comment(&bytes)
    );
    assert_eq!(stats.capture_drop_total, 40, "measured once, by the handle");

    for isb in statistics_blocks(&bytes) {
        assert!(
            isb.options
                .iter()
                .any(|o| matches!(o, InterfaceStatisticsOption::IsbOsDrop(40))),
            "the handle's total, the same on every interface, and never one role's share"
        );
        assert!(
            isb.options.iter().any(|o| matches!(
                o,
                InterfaceStatisticsOption::Comment(c)
                    if c.contains("capture_drop_scope=capture-handle")
                        && c.contains("must not be summed")
            )),
            "the block carrying it says at what scope it may be read"
        );
    }

    // The finest attribution a ring can offer stays exactly where the design
    // puts it: on the datagram behind which the loss was admitted.
    let epbs = packet_blocks(&bytes);
    assert!(epbs[0]
        .options
        .iter()
        .any(|o| matches!(o, EnhancedPacketOption::DropCount(40))));
    assert!(!epbs[1]
        .options
        .iter()
        .any(|o| matches!(o, EnhancedPacketOption::DropCount(_))));
}

#[test]
fn socket_mode_keeps_the_per_role_attribution_it_really_has() {
    // One socket per role means one SO_RXQ_OVFL accumulator per role, so here
    // the per-role number is measured and not guessed — and the scope is carried
    // as its own configured value rather than inferred from the link headers,
    // which say nothing about how many accumulators a capture holds.
    let payload = header_bytes(1, 1, 0, 3);
    let mut cfg = writer_config(&ALL_THREE);
    cfg.capture_drop_scope = CaptureDropScope::PortRole;
    let mut w = SegmentWriter::new(Vec::new(), &cfg).unwrap();
    let mut refdata = sequenced(&payload, &format!("{SOURCE}:40000"));
    refdata.role = PortRole::Refdata;
    refdata.drop_delta = 40;
    w.write(&refdata).unwrap();
    let mut mktdata = sequenced(&payload, &format!("{SOURCE}:40000"));
    mktdata.role = PortRole::Mktdata;
    w.write(&mktdata).unwrap();
    let (bytes, stats) = w.finish().unwrap();

    assert!(section_comment(&bytes).contains("capture_drop_scope=port-role"));
    assert_eq!(stats.capture_drop_total, 40);
    let isbs = statistics_blocks(&bytes);
    let os_drop =
        |isb: &pcap_file::pcapng::blocks::interface_statistics::InterfaceStatisticsBlock| {
            isb.options
                .iter()
                .find_map(|o| match o {
                    InterfaceStatisticsOption::IsbOsDrop(n) => Some(*n),
                    _ => None,
                })
                .expect("every statistics block states our own drops")
        };
    assert_eq!(os_drop(&isbs[0]), 0, "mktdata saw none of it");
    assert_eq!(
        os_drop(&isbs[1]),
        40,
        "refdata's own accumulator reported it"
    );
    assert!(
        !isbs[1]
            .options
            .iter()
            .any(|o| matches!(o, InterfaceStatisticsOption::Comment(_))),
        "nothing to except: the number is this interface's"
    );
}
