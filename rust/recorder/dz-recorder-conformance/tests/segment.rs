//! What the re-write must carry, and what it must refuse to invent.
//!
//! The version of this conversion that lived in `dz-recorder-e2e` was correct
//! for the fixtures it served — one group, synthesised headers, nothing
//! truncated, nothing dropped — and each of those was an assumption rather than
//! a property. Here each one is a datagram the archive really can hold, and what
//! the file says about it is read back out of the bytes rather than out of the
//! writer's own intent.
//!
//! **The reader is `ArchiveSource`**, which is the reader the analysis tier uses
//! and not a parser written for this suite. A test that walked the blocks itself
//! would be a third implementation of the format, agreeing with whichever of the
//! other two it was written beside.

use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;

use dz_edge_core::PortRole;
use dz_recorder_archive::{LinkHeaders, LINK_HEADER_LEN};
use dz_recorder_conformance::segment::{
    write_group_segments, write_segment, BridgeError, SectionProvenance,
};
use dz_recorder_core::{CaptureDropScope, RecorderIdentity, RecvTsKind};
use dz_recorder_replay::{ArchiveSource, LinkHeaderProvenance, OwnedDatagram, Termination};

const PUBLISHER_A: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 1);
const GROUP_A: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const GROUP_B: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 11);
const EGRESS_PORT: u16 = 41_000;
const RECV_TS_NS: u64 = 1_772_000_000_123_456_789;

/// A section stating synthesised headers, which is what socket mode produces and
/// what every datagram below carries unless it says otherwise.
fn synthesised_section() -> SectionProvenance {
    SectionProvenance {
        identity: RecorderIdentity {
            site: "lab".into(),
            recorder: "rec-1".into(),
            env: "test".into(),
            build_version: "0.0.0".into(),
            build_commit: "0000000".into(),
            config_hash: "cafe".into(),
        },
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::CaptureHandle,
    }
}

/// Reads the segment back the way the analysis tier would.
fn read_back(path: &Path) -> Vec<OwnedDatagram> {
    let mut source = ArchiveSource::open(path).expect("the segment the bridge wrote opens");
    let out: Vec<OwnedDatagram> = (&mut source).collect();
    assert_eq!(
        source.terminated_by(),
        Termination::Eof,
        "the segment did not end cleanly: {:?}",
        source.last_error()
    );
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
    let mut out = Vec::with_capacity(LINK_HEADER_LEN);
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
    assert_eq!(out.len(), LINK_HEADER_LEN);
    out
}

/// The case that decided the format, and the one a classic `pcap` cannot hold.
///
/// `epb_dropcount` is the recorder saying *I did not write the datagrams before
/// this one*. It is the only thing in a segment that separates capture loss from
/// publisher loss, and a classic record has no field for it: converting to one
/// hands the rule set a sequence gap with nothing to say the recorder caused it,
/// and the gap is then graded against the publisher.
#[test]
fn the_recorders_own_admitted_loss_survives_the_re_write() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");

    let mut first = datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]);
    first.drop_delta = 0;
    let mut after_a_hole = datagram(GROUP_A, PortRole::Mktdata, vec![2u8; 30]);
    after_a_hole.drop_delta = 7;

    write_segment(&path, [&first, &after_a_hole], &synthesised_section())
        .expect("the bridge writes");

    let back = read_back(&path);
    assert_eq!(
        back.iter().map(|dg| dg.drop_delta).collect::<Vec<_>>(),
        vec![0, 7],
        "the drop the recorder admitted is on the datagram that followed it"
    );
}

/// The section's claim about the whole segment, which the drop scope qualifies.
///
/// A `capture-handle` scope says the ring counted frames before it could tell
/// the roles apart, so the total may not be subtracted from one role's gaps. A
/// re-write that lost the scope would leave the analysis tier free to subtract a
/// guess, which is how a false publisher-loss finding is made.
#[test]
fn the_section_states_the_scope_the_drops_may_be_subtracted_at() {
    let dir = tempfile::tempdir().expect("a temporary directory");

    for scope in [CaptureDropScope::CaptureHandle, CaptureDropScope::PortRole] {
        let path = dir.path().join(format!("{}.pcapng", scope.as_str()));
        let mut provenance = synthesised_section();
        provenance.capture_drop_scope = scope;
        let dg = datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]);
        write_segment(&path, [&dg], &provenance).expect("the bridge writes");

        let source = ArchiveSource::open(&path).expect("the segment opens");
        assert_eq!(
            source.capture_drop_scope(),
            Some(scope),
            "the scope the archive stated is the scope the re-write states"
        );
    }
}

/// The identity is carried, not minted.
#[test]
fn the_section_carries_the_recorder_the_archive_named() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");
    let provenance = synthesised_section();
    let dg = datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]);
    write_segment(&path, [&dg], &provenance).expect("the bridge writes");

    let source = ArchiveSource::open(&path).expect("the segment opens");
    let identity = source.identity().expect("the section names a recorder");
    assert_eq!(identity.site, provenance.identity.site);
    assert_eq!(identity.recorder, provenance.identity.recorder);
    assert_eq!(
        identity.build_commit, provenance.identity.build_commit,
        "the build that recorded it, and not the build that re-wrote it"
    );
}

/// A capture this recorder did not write states no section, and a re-write of it
/// would have to invent one. It is refused, and the refusal names what was
/// missing.
#[test]
fn a_foreign_capture_is_refused_rather_than_given_a_section_we_made_up() {
    let (foreign, source) = foreign_capture();
    let err = SectionProvenance::of(&source).expect_err(&format!(
        "{} states no section of ours, so it cannot be re-written as one",
        foreign.display()
    ));
    assert!(
        matches!(err, BridgeError::Unstated { .. }),
        "the refusal names what was missing: {err}"
    );
}

/// A capture in `pcaps/` that this repository did not write and this reader
/// accepts.
///
/// Whichever of them opens, rather than a named file: the two differ in link
/// type and only one is Ethernet, and a test naming the wrong one would fail for
/// a reason that has nothing to do with sections.
fn foreign_capture() -> (std::path::PathBuf, ArchiveSource) {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("..")
        .join("pcaps");
    let mut paths: Vec<_> = std::fs::read_dir(&dir)
        .expect("pcaps/ exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "pcap"))
        .collect();
    paths.sort();
    paths
        .into_iter()
        .find_map(|p| ArchiveSource::open(&p).ok().map(|s| (p, s)))
        .expect("pcaps/ holds a capture this reader accepts")
}

/// A datagram the capture cut short must read as cut short.
///
/// The block's captured length is what is held and its original length is what
/// was sent. Writing one value into both asserts *not truncated*, which turns a
/// recorder's snap length into a publisher declaring a length past its own body.
#[test]
fn a_truncated_datagram_keeps_the_length_it_was_sent_at() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");

    let mut dg = datagram(GROUP_A, PortRole::Mktdata, vec![7u8; 40]);
    dg.wire_payload_len = 900; // 860 bytes the capture never saw
    write_segment(&path, [&dg], &synthesised_section()).expect("the bridge writes");

    let back = read_back(&path);
    assert_eq!(back.len(), 1);
    assert_eq!(back[0].payload.len(), 40, "what the capture held");
    assert_eq!(
        back[0].wire_payload_len, 900,
        "what the publisher sent, which is what makes the shortfall visible"
    );
}

#[test]
fn an_untruncated_datagram_reads_back_at_the_length_it_holds() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");

    let dg = datagram(GROUP_A, PortRole::Mktdata, vec![7u8; 40]);
    write_segment(&path, [&dg], &synthesised_section()).expect("the bridge writes");

    let back = read_back(&path);
    assert_eq!(back[0].payload.len(), 40);
    assert_eq!(
        back[0].wire_payload_len, 40,
        "nothing was cut short, and the two lengths say so"
    );
}

/// Captured bytes are reproduced, never rebuilt.
///
/// The identification field, the fragmentation flags, the DSCP and both
/// checksums are observations. A rebuild produces a well-formed header carrying
/// none of them, and a rule reading below UDP would be reading a fiction.
#[test]
fn captured_link_headers_are_reproduced_rather_than_rebuilt() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");

    let captured = captured_link_headers(30);
    let mut dg = datagram(GROUP_A, PortRole::Mktdata, vec![9u8; 30]);
    dg.link_headers = Some(captured.clone());

    let mut provenance = synthesised_section();
    provenance.link_headers = LinkHeaders::Captured;
    write_segment(&path, [&dg], &provenance).expect("the bridge writes");

    let back = read_back(&path);
    assert_eq!(
        back[0].link_headers.as_deref(),
        Some(&captured[..]),
        "byte for byte, including the fields a rebuild has no value for"
    );
    assert_eq!(
        back[0].ttl,
        Some(64),
        "the TTL as it arrived, and not the synthesised default"
    );
}

/// Two groups on one set of ports, which is the case the split exists for.
///
/// The tool's port map is keyed on the destination port alone, so one file
/// holding both would be read as one series and the two sequence spaces
/// interleaved would be reported as loss in both. `-group` does not save it: the
/// tool ignores that flag in replay.
#[test]
fn two_groups_in_one_archive_produce_two_files_each_holding_only_its_own() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let datagrams = vec![
        datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]),
        datagram(GROUP_B, PortRole::Mktdata, vec![2u8; 40]),
        datagram(GROUP_A, PortRole::Snapshot, vec![3u8; 50]),
    ];
    assert_eq!(
        datagrams[0].dst.port(),
        datagrams[1].dst.port(),
        "both groups on one port, which is what makes the port map insufficient"
    );

    let files = write_group_segments(dir.path(), &datagrams, &synthesised_section())
        .expect("the bridge writes");

    assert_eq!(
        files.iter().map(|f| f.group).collect::<Vec<_>>(),
        vec![GROUP_A, GROUP_B],
        "one file per group, ordered so that two runs over one object agree"
    );
    assert_eq!(files[0].datagram_count, 2);
    assert_eq!(files[1].datagram_count, 1);

    let a = read_back(&files[0].path);
    assert_eq!(
        a.iter().map(|dg| dg.payload[0]).collect::<Vec<_>>(),
        vec![1, 3],
        "group A's file holds group A's datagrams, in arrival order"
    );
    let b = read_back(&files[1].path);
    assert_eq!(
        b.iter().map(|dg| dg.payload[0]).collect::<Vec<_>>(),
        vec![2],
        "and group B's holds only its own"
    );

    for (file, datagrams) in [(&files[0], &a), (&files[1], &b)] {
        for dg in datagrams {
            assert_eq!(
                *dg.dst.ip(),
                file.group,
                "a file pointed at the wrong group cannot look right"
            );
        }
    }
}

/// A group with no datagrams produces no file.
///
/// The tool's exit code cannot distinguish *clean* from *saw nothing*, so an
/// empty file is a trap rather than a convenience.
#[test]
fn an_archive_holding_nothing_produces_no_file_to_read_a_clean_exit_from() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let files = write_group_segments(dir.path(), &[], &synthesised_section())
        .expect("the bridge writes nothing");
    assert!(files.is_empty());
    assert_eq!(
        std::fs::read_dir(dir.path())
            .expect("the directory exists")
            .count(),
        0
    );
}

/// The recorder's admission belongs to no group, so a split must not decide one.
///
/// At capture-handle scope the ring lost frames before it could tell groups or
/// roles apart, and the count rides on whichever datagram was kept next — here
/// group B's. Left there, group A's file would hold A's gap with nothing beside
/// it, and the rule set would charge that gap to the publisher. Carried, both
/// files say the recorder lost something before their next datagram.
#[test]
fn a_split_carries_the_recorders_admission_into_every_groups_file() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mut after_the_hole = datagram(GROUP_B, PortRole::Mktdata, vec![2u8; 30]);
    after_the_hole.drop_delta = 3;
    let datagrams = vec![
        datagram(GROUP_A, PortRole::Mktdata, vec![1u8; 30]),
        after_the_hole,
        datagram(GROUP_A, PortRole::Refdata, vec![3u8; 30]),
        datagram(GROUP_B, PortRole::Mktdata, vec![4u8; 30]),
    ];

    let files = write_group_segments(dir.path(), &datagrams, &synthesised_section())
        .expect("the bridge writes");

    let a = read_back(&files[0].path);
    assert_eq!(
        a.iter().map(|dg| dg.drop_delta).collect::<Vec<_>>(),
        vec![0, 3],
        "group A's next datagram carries the admission, whatever its role: at \
         capture-handle scope the ring could not tell roles apart either"
    );
    let b = read_back(&files[1].path);
    assert_eq!(
        b.iter().map(|dg| dg.drop_delta).collect::<Vec<_>>(),
        vec![3, 0],
        "and group B keeps it on the datagram it arrived on, once"
    );
}

/// At port-role scope the admission is the role's, and still no group's.
///
/// Two groups on one set of ports share the role's handle, so the count may be
/// either group's — but it is not another role's, and carrying it onto a
/// refdata datagram would taint a series the handle never lost anything from.
#[test]
fn at_port_role_scope_an_admission_stays_on_its_own_role() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let mut provenance = synthesised_section();
    provenance.capture_drop_scope = CaptureDropScope::PortRole;

    let mut after_the_hole = datagram(GROUP_B, PortRole::Mktdata, vec![2u8; 30]);
    after_the_hole.drop_delta = 3;
    let datagrams = vec![
        after_the_hole,
        datagram(GROUP_A, PortRole::Refdata, vec![3u8; 30]),
        datagram(GROUP_A, PortRole::Mktdata, vec![4u8; 30]),
    ];

    let files =
        write_group_segments(dir.path(), &datagrams, &provenance).expect("the bridge writes");

    let a = read_back(&files[0].path);
    assert_eq!(
        a.iter()
            .map(|dg| (dg.role, dg.drop_delta))
            .collect::<Vec<_>>(),
        vec![(PortRole::Refdata, 0), (PortRole::Mktdata, 3)],
        "the refdata datagram is untouched and the next mktdata one carries it"
    );
}

/// What was sent is never less than what was kept.
///
/// `OwnedDatagram` is public, and a caller that leaves `wire_payload_len` unset
/// would otherwise produce a block whose original length undercuts its captured
/// length — one every reader rejects.
#[test]
fn an_unset_wire_length_is_floored_at_the_payload_held() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = dir.path().join("one.pcapng");
    let mut dg = datagram(GROUP_A, PortRole::Mktdata, vec![7u8; 40]);
    dg.wire_payload_len = 0;
    write_segment(&path, [&dg], &synthesised_section()).expect("the bridge writes");

    let back = read_back(&path);
    assert_eq!(back[0].payload.len(), 40);
    assert_eq!(
        back[0].wire_payload_len, 40,
        "the original length is at least the captured one"
    );
}

/// Each fact a section may fail to state is refused on its own, by name.
///
/// A foreign capture states none of the three and so only ever reaches the
/// first refusal. The two after it are the defaults that would do the damage — a
/// `captured` claim over synthesised bytes, an invented per-role drop scope — so
/// they are reached here directly.
#[test]
fn each_unstated_fact_is_refused_by_name_rather_than_defaulted() {
    let identity = synthesised_section().identity;
    let missing = |r: Result<SectionProvenance, BridgeError>| match r {
        Err(BridgeError::Unstated { missing }) => missing,
        other => panic!("expected a refusal, got {other:?}"),
    };

    assert_eq!(
        missing(SectionProvenance::from_section(
            None,
            LinkHeaderProvenance::Synthesised,
            Some(CaptureDropScope::CaptureHandle),
        )),
        "recorder identity"
    );
    assert_eq!(
        missing(SectionProvenance::from_section(
            Some(&identity),
            LinkHeaderProvenance::Unstated,
            Some(CaptureDropScope::CaptureHandle),
        )),
        "link-header provenance"
    );
    assert_eq!(
        missing(SectionProvenance::from_section(
            Some(&identity),
            LinkHeaderProvenance::Synthesised,
            None,
        )),
        "capture drop scope"
    );

    let stated = SectionProvenance::from_section(
        Some(&identity),
        LinkHeaderProvenance::Captured,
        Some(CaptureDropScope::PortRole),
    )
    .expect("a section stating all three is carried");
    assert_eq!(stated.link_headers, LinkHeaders::Captured);
    assert_eq!(stated.capture_drop_scope, CaptureDropScope::PortRole);
}
