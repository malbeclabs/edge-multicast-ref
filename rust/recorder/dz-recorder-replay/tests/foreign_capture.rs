//! The two captures in `pcaps/`, read by the reader that claims to accept an
//! archive this recorder did not write.
//!
//! Neither is a fixture of our own traffic, and that is what makes them worth
//! keeping here: both are real captures taken by other tooling, and each one is
//! a mistake an operator makes when taking the independent capture the design's
//! acceptance step calls for. One was taken on the physical interface, so its
//! datagrams are still inside their GRE tunnel. The other was taken with
//! `tcpdump -i any`, so its link layer is Linux's cooked header and not
//! Ethernet at all.
//!
//! Both are refused, and the point of these tests is that each refusal names its
//! own cause. A capture that cannot be read is an operator's afternoon either
//! way; a capture that cannot be read and does not say why is their week.
//!
//! Nothing here asserts on an address, so these tests say nothing about
//! anybody's network.

use std::path::PathBuf;

use dz_recorder_core::Source;
use dz_recorder_replay::{ArchiveSource, PortRoles, Termination};

/// The captures are found by what their file header says, not by their names.
///
/// A name can carry an address, and this file should not: a test that names
/// somebody's multicast group has put it in the source tree just as surely as a
/// fixture would.
fn capture_with_link_type(want: u32) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../pcaps");
    let mut found: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("the pcaps directory is in the repository")
        .filter_map(|e| {
            let path = e.ok()?.path();
            (link_type_of(&path)? == want).then_some(path)
        })
        .collect();
    found.sort();
    found
        .pop()
        .unwrap_or_else(|| panic!("no capture in pcaps/ with link type {want}"))
}

/// The link type out of a classic pcap file header, or `None` for anything that
/// is not one.
fn link_type_of(path: &std::path::Path) -> Option<u32> {
    let head = std::fs::read(path).ok()?;
    let header: [u8; 24] = head.get(..24)?.try_into().ok()?;
    let magic = u32::from_le_bytes(header[0..4].try_into().ok()?);
    match magic {
        0xa1b2_c3d4 | 0xa1b2_3c4d => Some(u32::from_le_bytes(header[20..24].try_into().ok()?)),
        0xd4c3_b2a1 | 0x4d3c_b2a1 => Some(u32::from_be_bytes(header[20..24].try_into().ok()?)),
        _ => None,
    }
}

const ETHERNET: u32 = 1;
const LINUX_COOKED: u32 = 113;

/// The tunnelled capture is classic pcap over Ethernet, so it opens; what it
/// cannot do is yield a datagram.
fn encapsulated() -> PathBuf {
    capture_with_link_type(ETHERNET)
}

/// The cooked capture cannot even open, because its link layer is decided in the
/// file header.
fn cooked() -> PathBuf {
    capture_with_link_type(LINUX_COOKED)
}

#[test]
fn a_capture_of_the_physical_interface_is_still_inside_its_tunnel() {
    // The recorder captures on the interface the feed arrives on, where the
    // frames are already de-encapsulated. A capture taken one hop earlier holds
    // the outer frames, and reading those as the feed would mean parsing a GRE
    // header as a datagram.
    let Ok(source) = ArchiveSource::open(&encapsulated()) else {
        panic!("classic pcap over Ethernet opens");
    };
    let mut source =
        source // Any mapping at all: the refusal happens while parsing the frame, before
            // a port is ever reached, so the value here is not this capture's.
            .with_port_roles(PortRoles::new(&[(40_000, dz_edge_core::PortRole::Mktdata)]));

    let err = Source::next(&mut source).expect_err("a tunnelled frame is not a datagram");
    let message = err.to_string();
    assert!(
        message.contains("47") || message.to_lowercase().contains("udp"),
        "the refusal names the protocol it found rather than failing vaguely: {message}"
    );
    assert_eq!(
        source.terminated_by(),
        Termination::Rejected,
        "and the stream ends saying it refused, never reporting a clean end"
    );
}

#[test]
fn a_capture_taken_on_any_is_refused_at_its_header_and_says_so() {
    // `tcpdump -i any` writes Linux's cooked header, sixteen bytes where an
    // Ethernet one is fourteen. Read as Ethernet it yields a plausible-looking
    // IPv4 header at the wrong offset, so the link type is checked once at the
    // file header rather than misparsed per packet.
    let Err(err) = ArchiveSource::open(&cooked()) else {
        panic!("a cooked capture is not Ethernet and must not open");
    };
    let message = err.to_string();
    assert!(
        message.to_lowercase().contains("link type"),
        "the refusal names the link layer: {message}"
    );
    assert!(
        message.contains("any"),
        "and says what to do instead of taking it on `any`: {message}"
    );
}

/// A classic capture states no scope and no identity, and says so rather than
/// answering with a default.
///
/// This is the half of the pcapng decision that only shows up on somebody else's
/// file. A reader that answered `port-role` here would license a per-role
/// subtraction on a capture that never claimed one was valid, and one that
/// answered with an identity would put a recorder's name on an object no
/// recorder of ours wrote. Both are measurements invented out of an absence.
#[test]
fn a_classic_capture_reports_that_it_knows_neither_scope_nor_identity() {
    let Ok(source) = ArchiveSource::open(&encapsulated()) else {
        panic!("classic pcap over Ethernet opens");
    };
    assert_eq!(
        source.capture_drop_scope(),
        None,
        "a capture with no section header declares no scope, and unknown is the \
         only honest answer"
    );
    assert_eq!(source.identity(), None);
}

#[test]
fn both_captures_are_refused_before_a_single_datagram_is_believed() {
    // The property that matters across both: a capture this reader cannot
    // interpret never yields a datagram it would have to guess about. Silently
    // skipping what it cannot parse is how a short replay reads as a complete
    // one, and a short replay read as complete becomes a publisher finding.
    let Ok(mut tunnelled) = ArchiveSource::open(&encapsulated()) else {
        panic!("classic pcap over Ethernet opens");
    };
    assert!(Source::next(&mut tunnelled).is_err());
    assert_ne!(tunnelled.terminated_by(), Termination::Eof);

    assert!(ArchiveSource::open(&cooked()).is_err());
}
