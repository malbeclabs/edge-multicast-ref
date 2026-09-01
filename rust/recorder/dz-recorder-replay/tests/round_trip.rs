//! The round trip: what the recorder archived is what replay yields.

mod common;

use std::borrow::Cow;
use std::io::Cursor;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use common::{
    identity, record, record_at_scope, replay, sequence_number, truncate_compressed,
    truncate_mid_block,
};
use dz_edge_core::PortRole;
use dz_recorder_archive::Compression;
use dz_recorder_core::{CaptureDropScope, RecvTsKind, Source};
use dz_recorder_replay::synthetic::{StarvationWindow, SyntheticPublisher, FIRST_RECV_TS_NS};
use dz_recorder_replay::{
    ArchiveSource, LinkHeaderProvenance, OwnedDatagram, PortRoles, Termination,
};
use pcap_file::pcapng::blocks::enhanced_packet::{EnhancedPacketBlock, EnhancedPacketOption};
use pcap_file::pcapng::blocks::interface_description::{
    InterfaceDescriptionBlock, InterfaceDescriptionOption,
};
use pcap_file::pcapng::blocks::section_header::{SectionHeaderBlock, SectionHeaderOption};
use pcap_file::pcapng::PcapNgWriter;
use pcap_file::{DataLink, Endianness};

const MKTDATA_ONLY: &[PortRole] = &[PortRole::Mktdata];
const ZSTD: Compression = Compression::Zstd { level: 1 };

#[test]
fn replay_yields_exactly_what_was_recorded() {
    // The Source symmetry is the load-bearing property of the design: a live
    // capture and a replayed archive present identically, so the analysis tier
    // runs unchanged over live traffic and the health tier runs unchanged over
    // an archive, and a recorder is testable end-to-end with no network.
    let (original, archive) = record(&SyntheticPublisher::clean(1000), ZSTD, MKTDATA_ONLY);
    let replayed = replay(&archive.object);

    assert_eq!(original.len(), 1000, "the stream reached the sink whole");
    assert_eq!(replayed.len(), original.len());
    for (a, b) in original.iter().zip(&replayed) {
        assert_eq!(a.payload, b.payload);
        assert_eq!(a.src, b.src);
        assert_eq!(a.dst, b.dst);
        assert_eq!(a.role, b.role);
        assert_eq!(a.recv_ts_ns, b.recv_ts_ns, "nanosecond, not microsecond");
        assert_eq!(a.recv_ts_kind, b.recv_ts_kind);
        assert_eq!(a.drop_delta, b.drop_delta);
        assert_eq!(a.ttl, b.ttl);
    }
    // The whole value, so a field added to the datagram later is compared
    // without anybody having to remember to extend the list above.
    assert_eq!(original, replayed);

    // A comparison of a field that only ever held one value proves nothing, so
    // the stream is asserted to have exercised both stamp kinds.
    assert!(
        replayed
            .iter()
            .any(|d| d.recv_ts_kind == RecvTsKind::KernelSoftware)
            && replayed
                .iter()
                .any(|d| d.recv_ts_kind == RecvTsKind::ApplicationFallback),
        "the stream carried only one stamp kind, so the kind assertion is vacuous"
    );
    assert_eq!(
        sequence_number(&replayed[0].payload),
        0,
        "the header came back at its own offsets"
    );
}

#[test]
fn a_compressed_and_an_uncompressed_archive_replay_identically() {
    // Compression is a property of the object, never of its contents. A reader
    // that reached a different conclusion from the same datagrams because of how
    // they were stored would make every finding depend on a storage setting.
    let publisher = SyntheticPublisher::clean(200);
    let (original, plain) = record(&publisher, Compression::None, MKTDATA_ONLY);
    let (_, compressed) = record(&publisher, ZSTD, MKTDATA_ONLY);

    assert_eq!(plain.object.extension().unwrap(), "pcapng");
    assert_eq!(compressed.object.extension().unwrap(), "zst");
    assert_eq!(replay(&plain.object), replay(&compressed.object));
    assert_eq!(replay(&plain.object), original);
}

#[test]
fn a_truncated_segment_replays_what_survived_and_says_so() {
    // A recorder killed mid-write leaves a partial block. Returning an error for
    // the whole file would discard every datagram before the tear.
    let (original, archive) = record(
        &SyntheticPublisher::clean(100),
        Compression::None,
        MKTDATA_ONLY,
    );
    // One section header and three interface descriptions come first, so this
    // cut lands inside the thirty-seventh datagram's block.
    let path = truncate_mid_block(&archive.object, 40);

    let mut source = ArchiveSource::open(&path).unwrap();
    let survived: Vec<OwnedDatagram> = (&mut source).collect();
    assert!(!survived.is_empty() && survived.len() < 100);
    assert!(matches!(source.terminated_by(), Termination::Truncated));
    assert!(
        source.last_error().is_some(),
        "the tear is reported, not merely counted"
    );
    assert_eq!(
        survived,
        &original[..survived.len()],
        "what survived is what was written, verbatim and in order"
    );
}

#[test]
fn a_tear_is_an_error_to_the_source_and_not_a_clean_end() {
    // Iteration stops at the tear either way. A caller reading through the trait
    // must be told, because a short replay read as complete becomes a sequence
    // gap, and a gap with no admitted loss behind it becomes a publisher
    // finding — the one mistake the whole design exists to prevent.
    let (_, archive) = record(
        &SyntheticPublisher::clean(100),
        Compression::None,
        MKTDATA_ONLY,
    );
    let path = truncate_mid_block(&archive.object, 40);

    let mut source = ArchiveSource::open(&path).unwrap();
    let mut count = 0;
    let outcome = loop {
        match Source::next(&mut source) {
            Ok(Some(_)) => count += 1,
            other => break other,
        }
    };
    assert!(count > 0);
    assert!(outcome.is_err(), "the tear reached the caller");
    // And the stream stays ended: a reader that re-parsed the torn block would
    // hand back the same error for ever.
    assert!(matches!(Source::next(&mut source), Ok(None)));
}

#[test]
fn a_truncated_zstd_object_replays_what_survived_and_says_so() {
    // Every object that ships is compressed, so this is the tear that actually
    // happens: an interrupted copy or upload out of the completed directory. It
    // must reach the same verdict as the plain case, because an operator reading
    // corruption where there was an interruption responds to the wrong thing.
    //
    // The stream is large enough to span several zstd blocks. Below one block
    // nothing survives at all — a partial block is not decodable — and that case
    // is the next test rather than this one.
    let (original, archive) = record(&SyntheticPublisher::clean(5000), ZSTD, MKTDATA_ONLY);
    let whole = std::fs::metadata(&archive.object).unwrap().len() as usize;
    let path = truncate_compressed(&archive.object, whole * 60 / 100);

    let mut source = ArchiveSource::open(&path).unwrap();
    let survived: Vec<OwnedDatagram> = (&mut source).collect();

    assert!(!survived.is_empty() && survived.len() < original.len());
    assert!(
        matches!(source.terminated_by(), Termination::Truncated),
        "a tear in the compressed stream read as {:?}",
        source.terminated_by()
    );
    assert_eq!(
        survived,
        &original[..survived.len()],
        "what survived is what was written, verbatim and in order"
    );
    // The verdict is only as good as the evidence recorded for it: zstd calls a
    // partial frame an incomplete frame, and every corruption something else.
    let evidence = source.last_error().expect("the tear is reported");
    assert!(
        evidence.contains("incomplete frame"),
        "the tear was classified on evidence that does not say so: {evidence}"
    );
}

#[test]
fn a_zstd_object_torn_inside_its_first_block_does_not_open_as_an_empty_archive() {
    // zstd cannot decode a partial block, so a tear this early leaves not one
    // datagram to yield. The truthful report is a refusal to open, naming the
    // tear — never an archive that opens, yields nothing and ends cleanly, which
    // would turn a partial object into a feed that was silent.
    let (_, archive) = record(&SyntheticPublisher::clean(5000), ZSTD, MKTDATA_ONLY);
    let whole = std::fs::metadata(&archive.object).unwrap().len() as usize;
    let path = truncate_compressed(&archive.object, whole / 20);

    let error = match ArchiveSource::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("a partial first block opened as an archive"),
    };
    assert!(
        error.to_string().contains("ends inside its first block"),
        "the refusal does not name the tear: {error}"
    );
}

#[test]
fn an_object_torn_before_its_first_datagram_yields_nothing_and_still_says_truncated() {
    // The same misattribution one layer down: nothing survived, and an empty
    // replay reported as a clean end is a non-empty object read as a quiet feed.
    let (_, archive) = record(
        &SyntheticPublisher::clean(100),
        Compression::None,
        MKTDATA_ONLY,
    );
    // A section header and three interface descriptions come first, so a cut
    // inside the fourth block is before any datagram.
    let path = truncate_mid_block(&archive.object, 3);

    let mut source = ArchiveSource::open(&path).unwrap();
    let survived: Vec<OwnedDatagram> = (&mut source).collect();
    assert!(survived.is_empty());
    assert!(matches!(source.terminated_by(), Termination::Truncated));
}

#[test]
fn corruption_inside_an_intact_frame_is_not_reported_as_a_tear() {
    // A tear and corruption are different findings with different responses, so
    // the classification must not collapse them. Here the compressed frame is
    // whole and decodes cleanly; what is damaged is the pcapng inside it.
    //
    // Note what this test does not claim: zstd detects only some corruption,
    // because the frame carries no content checksum, and most single-byte damage
    // decompresses to different bytes with no error at all. What answers that is
    // the manifest's sha256 of the object, checked before the object is
    // replayed. This asserts only that damage which is detected is called
    // corruption and never a tear.
    let (_, archive) = record(
        &SyntheticPublisher::clean(100),
        Compression::None,
        MKTDATA_ONLY,
    );
    let path = compress_with_a_damaged_block_length(&archive.object);

    let mut source = ArchiveSource::open(&path).unwrap();
    let _: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(
        source.terminated_by(),
        Termination::Failed,
        "corruption inside a whole frame read as {:?}: {:?}",
        source.terminated_by(),
        source.last_error()
    );
}

#[test]
fn a_drop_delta_survives_the_round_trip() {
    // epb_dropcount is the whole loss-attribution story, so it is asserted on a
    // stream that actually admits losses: with every delta zero the round trip's
    // comparison of the field would hold no matter what the writer did with it.
    let publisher = SyntheticPublisher::clean(300).starved(&[
        StarvationWindow {
            first: 100,
            count: 7,
        },
        StarvationWindow {
            first: 200,
            count: 1,
        },
    ]);
    let (original, archive) = record(&publisher, ZSTD, MKTDATA_ONLY);
    let replayed = replay(&archive.object);

    assert_eq!(original, replayed);
    let deltas: Vec<u32> = replayed
        .iter()
        .map(|d| d.drop_delta)
        .filter(|d| *d != 0)
        .collect();
    assert_eq!(deltas, vec![7, 1]);
    assert_eq!(
        replayed.iter().filter(|d| d.drop_delta == 0).count(),
        replayed.len() - 2,
        "an absent DropCount option means zero, not unknown"
    );
}

#[test]
fn every_port_role_recovers_from_the_block_it_references() {
    // interface_id maps to a port role, and the mapping travels in the archive.
    // A port role with two spellings, or one recovered by position from a
    // reader's own assumption, is a join that silently returns nothing.
    for role in [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot] {
        let publisher = SyntheticPublisher::clean(20).on_role(role);
        let (original, archive) = record(&publisher, ZSTD, &[role]);
        let replayed = replay(&archive.object);
        assert_eq!(original, replayed, "{} did not round trip", role.as_str());
        assert!(replayed.iter().all(|d| d.role == role));
    }
}

#[test]
fn a_synthesised_zero_ttl_is_not_observed_and_a_captured_one_is() {
    // An IPv4 header has no way to express absent, so socket mode writes an
    // unobserved TTL as zero. A reader that reported that zero as an
    // observation would put a value nobody measured into an average.
    let (original, archive) = record(&SyntheticPublisher::clean(1), ZSTD, MKTDATA_ONLY);
    let source = ArchiveSource::open(&archive.object).unwrap();
    assert_eq!(source.link_headers(), LinkHeaderProvenance::Synthesised);
    // The section states the stamp kind too: an archive that cannot say which
    // kind it holds cannot be trusted for latency at all.
    assert_eq!(source.section_recv_ts_kind(), RecvTsKind::KernelSoftware);
    // A TTL that was observed is a TTL either way, and it round trips.
    assert_eq!(replay(&archive.object)[0].ttl, original[0].ttl);
    assert!(original[0].ttl.is_some_and(|t| t != 0));

    // A synthesised section with a zero in the header byte: written by the
    // recorder for a TTL the socket never reported.
    let bytes = one_packet_archive(0, 9, "synthesised", 1);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();
    assert_eq!(replayed[0].ttl, None);

    // The same byte in a captured section is a TTL that was on the wire.
    let bytes = one_packet_archive(0, 9, "captured", 1);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();
    assert_eq!(replayed[0].ttl, Some(0));
}

/// The section states the scope its drops may be subtracted at and who wrote
/// it, and a loader must be able to read both out of the object.
///
/// Out of the object and not out of the sidecar manifest: an object gets copied,
/// renamed and pulled out of a bucket by hand, and a loader told its scope out
/// of band can be told it wrong. Subtracting per role under a capture-handle
/// scope credits one role with another's losses and leaves the first role's gap
/// looking unexplained, which is the false publisher finding the whole design
/// exists to prevent.
#[test]
fn a_replayed_archive_reports_the_scope_and_the_identity_its_writer_declared() {
    for scope in [CaptureDropScope::PortRole, CaptureDropScope::CaptureHandle] {
        let (_, archive) =
            record_at_scope(&SyntheticPublisher::clean(4), ZSTD, MKTDATA_ONLY, scope);
        let source = ArchiveSource::open(&archive.object).expect("the archive opens");
        assert_eq!(
            source.capture_drop_scope(),
            Some(scope),
            "the section states {}, and a reader that could not recover it would \
             have to be told out of band",
            scope.as_str()
        );
        // Every field, compared whole: a provenance that recovers the site and
        // loses the build commit cannot attribute a finding to a build.
        assert_eq!(source.identity(), Some(&identity()));
    }
}

#[test]
fn a_microsecond_archive_is_read_through_its_own_resolution() {
    // pcap-file 2.0.0 writes and parses the block's integer as raw nanoseconds
    // and ignores if_tsresol, so the resolution has to be applied by the reader.
    // A microsecond archive is otherwise indistinguishable from a nanosecond one
    // that happens to end in three zeros, which is the failure that makes every
    // latency number in the archive unfalsifiable.
    let micros = FIRST_RECV_TS_NS / 1_000;
    let bytes = one_packet_archive(8, 6, "captured", micros);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();
    assert_eq!(replayed[0].recv_ts_ns, micros * 1_000);

    // The same integer under this recorder's own resolution is nanoseconds.
    let bytes = one_packet_archive(8, 9, "captured", micros);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();
    assert_eq!(replayed[0].recv_ts_ns, micros);
}

#[test]
fn a_resolution_no_clock_has_still_yields_a_stamp() {
    // Every value of the low seven bits of if_tsresol is a legal byte, and one
    // arrives from a corrupt or hostile interface description as readily as from
    // a real clock. A byte that panicked would take down whatever replays the
    // archive; one that wrapped its own divisor would put a fabricated time into
    // a latency number, which is worse.
    let units = FIRST_RECV_TS_NS;

    // The finest decimal resolution the field can state, and one between it and
    // this recorder's own: both are under a nanosecond per unit, so nanoseconds
    // can only say zero about them.
    for resol in [127, 40] {
        let replayed: Vec<OwnedDatagram> =
            archive_from_bytes(&one_packet_archive(8, resol, "captured", units)).collect();
        assert_eq!(replayed.len(), 1, "if_tsresol={resol} cost the datagram");
        assert_eq!(replayed[0].recv_ts_ns, 0, "if_tsresol={resol}");
    }

    // Picoseconds divide to a stamp that is still a time, so the branch is not
    // merely returning zero for everything it cannot multiply.
    let replayed: Vec<OwnedDatagram> =
        archive_from_bytes(&one_packet_archive(8, 12, "captured", units)).collect();
    assert_eq!(replayed[0].recv_ts_ns, units / 1_000);

    // Coarse enough that the product does not fit: saturated, never wrapped. A
    // wrapped stamp is a plausible time that is wrong.
    let replayed: Vec<OwnedDatagram> =
        archive_from_bytes(&one_packet_archive(8, 0, "captured", 20_000_000_000)).collect();
    assert_eq!(replayed[0].recv_ts_ns, u64::MAX);

    // The top bit selects a power of two rather than a power of ten, at both
    // ends of the same seven bits: 2^0 is one second per unit and 2^-127 is far
    // below a nanosecond.
    let replayed: Vec<OwnedDatagram> =
        archive_from_bytes(&one_packet_archive(8, 0x80, "captured", 3)).collect();
    assert_eq!(replayed[0].recv_ts_ns, 3_000_000_000);
    let replayed: Vec<OwnedDatagram> =
        archive_from_bytes(&one_packet_archive(8, 0xFF, "captured", units)).collect();
    assert_eq!(replayed[0].recv_ts_ns, 0);
}

#[test]
fn no_resolution_byte_panics_and_a_coarser_one_never_reads_finer() {
    // All 256 bytes, because the guard that matters is the one nobody thought to
    // write a case for. A stamp that rose as the declared resolution coarsened
    // would be arithmetic that overflowed or a divisor that wrapped, which is
    // how a fabricated time gets in without an error anywhere.
    let units = FIRST_RECV_TS_NS;
    for family in [0x00u8, 0x80u8] {
        let mut previous = u64::MAX;
        for low in 0..=0x7fu8 {
            let resol = family | low;
            let mut source = archive_from_bytes(&one_packet_archive(8, resol, "captured", units));
            let replayed: Vec<OwnedDatagram> = (&mut source).collect();
            assert_eq!(replayed.len(), 1, "if_tsresol={resol} cost the datagram");
            assert_eq!(source.terminated_by(), Termination::Eof);
            let ns = replayed[0].recv_ts_ns;
            assert!(
                ns <= previous,
                "if_tsresol={resol} read finer than the byte before it: {ns} > {previous}"
            );
            previous = ns;
        }
    }
}

#[test]
fn a_frame_that_is_not_ours_mid_stream_ends_the_stream_and_says_why() {
    // mergecap output and any mixed capture hold frames that are not ours, and
    // an archive this recorder did not write is a supported input. Yielding the
    // datagrams before such a block and then reporting Running would be a short
    // replay that reads as a complete one: a sequence gap with no admitted loss
    // behind it becomes a publisher finding, which is the one mistake this whole
    // design exists to prevent.
    let bytes = archive_of(
        &["mktdata"],
        &[
            (0, datagram_frame(0x11)),
            (0, arp_frame()),
            (0, datagram_frame(0x33)),
        ],
    );

    let mut source = archive_from_bytes(&bytes);
    let yielded: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(yielded.len(), 1, "the datagrams before the block survived");
    assert_eq!(
        source.terminated_by(),
        Termination::Rejected,
        "a stream that ended on a refused block reported {:?}",
        source.terminated_by()
    );
    let evidence = source.last_error().expect("the refusal is reported");
    assert!(
        evidence.contains("not IPv4"),
        "the refusal does not name what was refused: {evidence}"
    );

    // Through the trait the error itself reaches the caller, and the stream then
    // stays ended rather than re-parsing the block for ever.
    let mut source = archive_from_bytes(&bytes);
    assert!(matches!(Source::next(&mut source), Ok(Some(_))));
    assert!(
        Source::next(&mut source).is_err(),
        "the refusal reached the caller"
    );
    assert!(matches!(Source::next(&mut source), Ok(None)));
    assert_eq!(source.terminated_by(), Termination::Rejected);
}

#[test]
fn a_block_naming_an_interface_that_is_no_port_role_ends_the_stream_and_says_why() {
    // The other refusal, and the one a merged archive reaches first: an
    // interface_id that resolves to something no port role is named for. A
    // datagram whose role was guessed instead is a join that silently returns
    // nothing, so the block is refused — and the refusal, like every other
    // ending, has to be on the record.
    let bytes = archive_of(
        &["mktdata", "eth0"],
        &[
            (0, datagram_frame(0x11)),
            (1, datagram_frame(0x22)),
            (0, datagram_frame(0x33)),
        ],
    );

    let mut source = archive_from_bytes(&bytes);
    let yielded: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(yielded.len(), 1);
    assert_eq!(yielded[0].role, PortRole::Mktdata);
    assert_eq!(source.terminated_by(), Termination::Rejected);
    let evidence = source.last_error().expect("the refusal is reported");
    assert!(
        evidence.contains("names no port role"),
        "the refusal does not name what was refused: {evidence}"
    );
}

fn archive_from_bytes(bytes: &[u8]) -> ArchiveSource {
    ArchiveSource::from_reader(Box::new(Cursor::new(bytes.to_vec()))).expect("the archive opens")
}

/// One pcapng segment, built here rather than by the writer, so a reader
/// assumption the writer happens to share is still caught.
fn one_packet_archive(ttl: u8, ts_resol: u8, link_headers: &str, timestamp: u64) -> Vec<u8> {
    let payload = [0xAAu8; 24];
    let src = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 50_000);
    let dst = SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 10), 40_000);

    let section = SectionHeaderBlock {
        endianness: Endianness::Little,
        options: vec![SectionHeaderOption::Comment(Cow::Owned(format!(
            "link_headers={link_headers}; recv_ts_kind=kernel-software"
        )))],
        ..Default::default()
    };
    let mut writer = PcapNgWriter::with_section_header(Cursor::new(Vec::new()), section)
        .expect("section header");
    writer
        .write_pcapng_block(InterfaceDescriptionBlock {
            linktype: DataLink::ETHERNET,
            snaplen: 2048,
            options: vec![
                InterfaceDescriptionOption::IfName(Cow::Borrowed("mktdata")),
                InterfaceDescriptionOption::IfTsResol(ts_resol),
            ],
        })
        .expect("interface description");

    let mut data = link_header_bytes(src, dst, ttl, payload.len());
    data.extend_from_slice(&payload);
    writer
        .write_pcapng_block(EnhancedPacketBlock {
            interface_id: 0,
            timestamp: Duration::from_nanos(timestamp),
            original_len: u32::try_from(data.len()).unwrap(),
            data: Cow::Owned(data),
            options: Vec::new(),
        })
        .expect("packet block");

    writer.into_inner().into_inner()
}

/// Ethernet, IPv4 and UDP, at the offsets the headers define.
fn link_header_bytes(src: SocketAddrV4, dst: SocketAddrV4, ttl: u8, payload_len: usize) -> Vec<u8> {
    let payload_len = u16::try_from(payload_len).unwrap();
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(&0x0800u16.to_be_bytes());
    out.push(0x45);
    out.push(0);
    out.extend_from_slice(&(20 + 8 + payload_len).to_be_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.push(ttl);
    out.push(17);
    out.extend_from_slice(&[0, 0]);
    out.extend_from_slice(&src.ip().octets());
    out.extend_from_slice(&dst.ip().octets());
    out.extend_from_slice(&src.port().to_be_bytes());
    out.extend_from_slice(&dst.port().to_be_bytes());
    out.extend_from_slice(&(8 + payload_len).to_be_bytes());
    out.extend_from_slice(&[0, 0]);
    out
}

/// Several blocks over the interfaces it names, so a block partway through a
/// stream can be one this reader refuses.
///
/// Built by hand because the writer cannot produce these archives at all — a
/// foreign frame, or an interface that is not a port role — and they are
/// precisely what a reader pointed at `mergecap` output has to survive.
fn archive_of(interface_names: &[&str], blocks: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let section = SectionHeaderBlock {
        endianness: Endianness::Little,
        options: vec![SectionHeaderOption::Comment(Cow::Borrowed(
            "link_headers=captured; recv_ts_kind=kernel-software",
        ))],
        ..Default::default()
    };
    let mut writer = PcapNgWriter::with_section_header(Cursor::new(Vec::new()), section)
        .expect("section header");
    for name in interface_names {
        writer
            .write_pcapng_block(InterfaceDescriptionBlock {
                linktype: DataLink::ETHERNET,
                snaplen: 2048,
                options: vec![
                    InterfaceDescriptionOption::IfName(Cow::Owned((*name).to_owned())),
                    InterfaceDescriptionOption::IfTsResol(9),
                ],
            })
            .expect("interface description");
    }
    for (interface_id, data) in blocks {
        writer
            .write_pcapng_block(EnhancedPacketBlock {
                interface_id: *interface_id,
                timestamp: Duration::from_nanos(FIRST_RECV_TS_NS),
                original_len: u32::try_from(data.len()).unwrap(),
                data: Cow::Borrowed(data.as_slice()),
                options: Vec::new(),
            })
            .expect("packet block");
    }
    writer.into_inner().into_inner()
}

/// A section claiming captured headers, with a per-datagram exception on the
/// blocks whose index is in `synthesised`.
fn archive_with_provenance_exceptions(frames: &[Vec<u8>], synthesised: &[usize]) -> Vec<u8> {
    let section = SectionHeaderBlock {
        endianness: Endianness::Little,
        options: vec![SectionHeaderOption::Comment(Cow::Borrowed(
            "link_headers=captured; recv_ts_kind=kernel-software",
        ))],
        ..Default::default()
    };
    let mut writer = PcapNgWriter::with_section_header(Cursor::new(Vec::new()), section)
        .expect("section header");
    writer
        .write_pcapng_block(InterfaceDescriptionBlock {
            linktype: DataLink::ETHERNET,
            snaplen: 2048,
            options: vec![
                InterfaceDescriptionOption::IfName(Cow::Borrowed("mktdata")),
                InterfaceDescriptionOption::IfTsResol(9),
            ],
        })
        .expect("interface description");
    for (i, data) in frames.iter().enumerate() {
        let options = if synthesised.contains(&i) {
            vec![EnhancedPacketOption::Comment(Cow::Borrowed(
                "link_headers=synthesised",
            ))]
        } else {
            Vec::new()
        };
        writer
            .write_pcapng_block(EnhancedPacketBlock {
                interface_id: 0,
                timestamp: Duration::from_nanos(FIRST_RECV_TS_NS),
                original_len: u32::try_from(data.len()).unwrap(),
                data: Cow::Borrowed(data.as_slice()),
                options,
            })
            .expect("packet block");
    }
    writer.into_inner().into_inner()
}

/// One of ours, with `tag` filling the payload so a yielded datagram is
/// identifiable.
fn datagram_frame(tag: u8) -> Vec<u8> {
    let src = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 50_000);
    let dst = SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 10), 40_000);
    let payload = [tag; 24];
    let mut data = link_header_bytes(src, dst, 8, payload.len());
    data.extend_from_slice(&payload);
    data
}

/// An ARP request: 42 bytes, so it clears the length check and is refused on its
/// ethertype rather than on its size, which is the refusal a mixed capture
/// actually produces.
fn arp_frame() -> Vec<u8> {
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(&[0xff; 6]);
    out.extend_from_slice(&[0x02, 0, 0, 0, 0, 0x01]);
    out.extend_from_slice(&0x0806u16.to_be_bytes());
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&0x0800u16.to_be_bytes());
    out.extend_from_slice(&[6, 4]);
    out.extend_from_slice(&1u16.to_be_bytes());
    out.extend_from_slice(&[0x02, 0, 0, 0, 0, 0x01]);
    out.extend_from_slice(&Ipv4Addr::new(192, 0, 2, 1).octets());
    out.extend_from_slice(&[0; 6]);
    out.extend_from_slice(&Ipv4Addr::new(192, 0, 2, 2).octets());
    out
}

/// Damages a block's declared length in the plain object, then compresses it
/// into a whole zstd frame: the decoder is satisfied and the pcapng is not,
/// which is corruption rather than a tear.
fn compress_with_a_damaged_block_length(path: &std::path::Path) -> std::path::PathBuf {
    let mut bytes = std::fs::read(path).expect("the object is readable");
    let mut offset = 0usize;
    // Past the section header and the three interface descriptions, so the
    // archive opens and some datagrams are yielded before the damage.
    for _ in 0..8 {
        let len = u32::from_le_bytes(bytes[offset + 4..offset + 8].try_into().unwrap()) as usize;
        offset += len;
    }
    // A length that is not a multiple of four is a length no writer produces.
    bytes[offset + 4..offset + 8].copy_from_slice(&7u32.to_le_bytes());

    let out = path.with_file_name("damaged.pcapng.zst");
    let file = std::fs::File::create(&out).expect("the damaged copy is writable");
    let mut encoder = zstd::stream::Encoder::new(file, 1).expect("an encoder");
    std::io::Write::write_all(&mut encoder, &bytes).expect("the write succeeds");
    encoder.finish().expect("a whole frame");
    out
}

#[test]
fn a_datagram_excepted_from_a_captured_claim_does_not_report_a_synthesised_zero_ttl() {
    // The section states the provenance and a datagram may except itself, the
    // same way the stamp kind works. A reader that honours only the section
    // turns a zero the writer synthesised into a TTL somebody measured — which
    // is exactly the reading `ttl: Option<u8>` exists to make impossible.
    let captured = datagram_frame(1); // ttl 8, genuinely on the wire
    let synthesised = {
        let src = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 50_000);
        let dst = SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 10), 40_000);
        let payload = [2u8; 24];
        let mut data = link_header_bytes(src, dst, 0, payload.len());
        data.extend_from_slice(&payload);
        data
    };

    let bytes = archive_with_provenance_exceptions(&[captured, synthesised], &[1]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();

    assert_eq!(replayed.len(), 2);
    assert_eq!(
        replayed[0].ttl,
        Some(8),
        "a captured TTL is observed and reported"
    );
    assert_eq!(
        replayed[1].ttl, None,
        "a zero under a synthesised exception is not observed"
    );
}

/// One captured-headers section whose blocks state an on-wire length of their
/// own, which is what a capture length shorter than the datagram produces.
fn archive_with_original_lens(frames: &[(Vec<u8>, u32)]) -> Vec<u8> {
    let section = SectionHeaderBlock {
        endianness: Endianness::Little,
        options: vec![SectionHeaderOption::Comment(Cow::Borrowed(
            "link_headers=captured; recv_ts_kind=kernel-software",
        ))],
        ..Default::default()
    };
    let mut writer = PcapNgWriter::with_section_header(Cursor::new(Vec::new()), section)
        .expect("section header");
    writer
        .write_pcapng_block(InterfaceDescriptionBlock {
            linktype: DataLink::ETHERNET,
            snaplen: 2048,
            options: vec![
                InterfaceDescriptionOption::IfName(Cow::Borrowed("mktdata")),
                InterfaceDescriptionOption::IfTsResol(9),
            ],
        })
        .expect("interface description");
    for (data, original_len) in frames {
        writer
            .write_pcapng_block(EnhancedPacketBlock {
                interface_id: 0,
                timestamp: Duration::from_nanos(FIRST_RECV_TS_NS),
                original_len: *original_len,
                data: Cow::Borrowed(data.as_slice()),
                options: Vec::new(),
            })
            .expect("packet block");
    }
    writer.into_inner().into_inner()
}

#[test]
fn a_truncated_datagram_replays_declaring_what_was_sent_not_what_was_kept() {
    // The block's original length is the only place the on-wire size was
    // recorded. A reader that reports the captured size instead turns a
    // publisher violation into a clean datagram — and re-recording that replay
    // writes an original length equal to the captured one, which pcapng defines
    // as *not truncated*.
    let src = SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, 10), 50_000);
    let dst = SocketAddrV4::new(Ipv4Addr::new(233, 252, 0, 10), 40_000);
    const SENT: usize = 1300;
    const KEPT: usize = 24;

    let mut frame = link_header_bytes(src, dst, 8, SENT);
    frame.extend_from_slice(&[7u8; KEPT]);
    let original_len = u32::try_from(42 + SENT).unwrap();

    let bytes = archive_with_original_lens(&[(frame, original_len)]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();

    assert_eq!(replayed.len(), 1);
    assert_eq!(replayed[0].payload.len(), KEPT, "what the archive holds");
    assert_eq!(
        replayed[0].wire_payload_len,
        u32::try_from(SENT).unwrap(),
        "what the publisher sent"
    );
}

#[test]
fn captured_headers_come_back_as_captured_and_synthesised_ones_do_not() {
    // Handing a synthesised header on as captured is how a reconstruction
    // becomes a claim about the wire; dropping a captured one is how the
    // fragmentation and duplicate-delivery evidence that justified capturing at
    // the interface is lost on the first replay.
    let captured = datagram_frame(1);
    let len = u32::try_from(captured.len()).unwrap();
    let bytes = archive_with_original_lens(&[(captured.clone(), len)]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes).collect();
    assert_eq!(
        replayed[0].link_headers.as_deref(),
        Some(&captured[..42]),
        "the bytes that were on the wire, verbatim"
    );

    let synthesised = archive_with_provenance_exceptions(&[datagram_frame(2)], &[0]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&synthesised).collect();
    assert_eq!(
        replayed[0].link_headers, None,
        "a synthesised header is not evidence and is not handed on as one"
    );
}

#[test]
fn a_capture_named_after_a_device_is_refused_and_the_error_says_what_would_fix_it() {
    // tcpdump names its interface after the device. Refusing such a capture is
    // defensible; refusing it without saying that a port role mapping resolves
    // it leaves an operator with a dead end.
    let bytes = archive_of(&["eth0"], &[(0, datagram_frame(1))]);
    let mut src = archive_from_bytes(&bytes);
    let err = Source::next(&mut src).expect_err("no role can be resolved");
    let message = err.to_string();
    assert!(message.contains("names no port role"), "{message}");
    assert!(
        message.contains("40000"),
        "and names the port a mapping would have to cover: {message}"
    );
}

#[test]
fn a_foreign_capture_reads_once_the_port_roles_are_stated() {
    // The design's acceptance step compares a recorder's archive against a
    // capture taken at the same point by independent tooling. Nothing could read
    // that capture, which made the step unexecutable with our own tools.
    let bytes = archive_of(&["eth0"], &[(0, datagram_frame(1)), (0, datagram_frame(2))]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes)
        .with_port_roles(PortRoles::new(&[(40_000, PortRole::Mktdata)]))
        .collect();

    assert_eq!(
        replayed.len(),
        2,
        "both datagrams, from a device-named capture"
    );
    assert!(replayed.iter().all(|d| d.role == PortRole::Mktdata));
    assert_eq!(replayed[0].payload, [1u8; 24]);
    assert_eq!(replayed[1].payload, [2u8; 24]);
}

#[test]
fn an_interface_that_names_its_role_beats_a_port_mapping() {
    // Our own archives state the role per interface, and a caller's mapping must
    // not be able to override what the archive itself vouches for.
    let bytes = archive_of(&["mktdata"], &[(0, datagram_frame(1))]);
    let replayed: Vec<OwnedDatagram> = archive_from_bytes(&bytes)
        .with_port_roles(PortRoles::new(&[(40_000, PortRole::Snapshot)]))
        .collect();
    assert_eq!(replayed[0].role, PortRole::Mktdata);
}

#[test]
#[should_panic(expected = "more than one port role")]
fn a_port_cannot_be_given_two_roles() {
    // First-wins would attribute datagrams to whichever role was listed first,
    // silently and for the life of the analysis.
    let _ = PortRoles::new(&[(40_000, PortRole::Mktdata), (40_000, PortRole::Refdata)]);
}
