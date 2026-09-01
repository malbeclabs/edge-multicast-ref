//! The chain the whole design rests on, judged by the specification's own rule
//! set: **publisher's encoder → recorder's archive → replay → conformance**.
//!
//! Every other test in this crate checks the chain against itself — the bytes
//! that came back are the bytes that went out. That catches corruption and
//! cannot catch agreement on something the spec forbids: an encoder writing an
//! invalid stream and an archive faithfully keeping it pass every round trip in
//! this repository. `dz-conformance` is the third party. It is the
//! specification's own tool, in the specification's own repository, and it
//! knows 88 rules this repository has never encoded.
//!
//! It reads classic `pcap` and the archive is `pcapng`, so replay's output is
//! written into one here. That conversion is the only thing this test adds to
//! the chain, and it adds nothing to the payloads: the datagram bytes handed to
//! the writer are exactly the bytes replay produced.
//!
//! Behind the `conformance` feature, and it does not skip. If the feature is on
//! and the tool is absent the test fails, because a conformance gate that
//! quietly passes when it cannot run is worse than no gate: it reports a clean
//! feed for a stream nobody validated.
//! A submodule of `common` rather than a suite of its own: it holds no tests,
//! and every suite that validates a chain reaches it through the same helpers.

use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::process::Command;

use super::{port_of, replay, Recorded, GROUP};
use dz_edge_core::PortRole;
use dz_recorder_replay::OwnedDatagram;

/// Where the tool is. Set by whatever runs the suite, because it is built from
/// a sibling repository this one does not vendor.
const TOOL_ENV: &str = "DZ_CONFORMANCE_BIN";

fn tool() -> PathBuf {
    let path = std::env::var(TOOL_ENV).unwrap_or_else(|_| {
        panic!(
            "{TOOL_ENV} is unset. This suite validates the archive against \
             edge-feed-spec's dz-conformance, which lives in that repository: build it with \
             `go build -o <path> ./tools/conformance` and point {TOOL_ENV} at the result. \
             Skipping instead would report a clean feed for a stream nobody checked."
        )
    });
    let path = PathBuf::from(path);
    assert!(
        path.is_file(),
        "{TOOL_ENV} points at {}, which is not a file",
        path.display()
    );
    path
}

/// The 42 bytes of Ethernet, IPv4 and UDP the datagram travelled behind.
///
/// Synthesised rather than captured, exactly as socket mode's archive does it,
/// and for the same reason: what is being validated is the payload, and these
/// bytes only have to be well-formed enough for the tool to find it.
fn link_headers(src: SocketAddrV4, dst: SocketAddrV4, payload_len: usize) -> Vec<u8> {
    let payload_len = u16::try_from(payload_len).expect("a datagram is small");
    let mut out = Vec::with_capacity(42);
    out.extend_from_slice(&[0u8; 12]); // destination and source MAC
    out.extend_from_slice(&0x0800u16.to_be_bytes()); // IPv4
    out.push(0x45); // version 4, 5 words of header
    out.push(0);
    out.extend_from_slice(&(20 + 8 + payload_len).to_be_bytes());
    out.extend_from_slice(&[0, 0, 0, 0]);
    out.push(4); // TTL
    out.push(17); // UDP
    out.extend_from_slice(&[0, 0]); // header checksum, unchecked by the tool
    out.extend_from_slice(&src.ip().octets());
    out.extend_from_slice(&dst.ip().octets());
    out.extend_from_slice(&src.port().to_be_bytes());
    out.extend_from_slice(&dst.port().to_be_bytes());
    out.extend_from_slice(&(8 + payload_len).to_be_bytes());
    out.extend_from_slice(&[0, 0]); // UDP checksum, zero means unchecked
    out
}

/// Classic pcap, little-endian, microsecond resolution — what `pcapgo.Reader`
/// expects. Written by hand because it is twenty-four bytes of file header and
/// sixteen per record, and pulling a dependency in to produce that would be
/// more code than this.
fn write_pcap(path: &Path, datagrams: &[OwnedDatagram]) {
    let mut out = Vec::new();
    out.extend_from_slice(&0xa1b2_c3d4u32.to_le_bytes()); // magic, microseconds
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&4u16.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes()); // no timezone offset
    out.extend_from_slice(&0u32.to_le_bytes()); // no timestamp accuracy claim
    out.extend_from_slice(&65_535u32.to_le_bytes()); // snaplen
    out.extend_from_slice(&1u32.to_le_bytes()); // LINKTYPE_ETHERNET

    for dg in datagrams {
        let mut frame = link_headers(dg.src, dg.dst, dg.payload.len());
        frame.extend_from_slice(&dg.payload);
        let secs = u32::try_from(dg.recv_ts_ns / 1_000_000_000).expect("a recent timestamp");
        let micros = u32::try_from(dg.recv_ts_ns % 1_000_000_000 / 1_000).expect("under a second");
        let len = u32::try_from(frame.len()).expect("a frame is small");
        out.extend_from_slice(&secs.to_le_bytes());
        out.extend_from_slice(&micros.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes()); // captured
        out.extend_from_slice(&len.to_le_bytes()); // on the wire
        out.extend_from_slice(&frame);
    }
    std::fs::write(path, &out).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

/// What one conformance run concluded.
pub struct Verdict {
    pub code: i32,
    pub stderr: String,
}

impl Verdict {
    /// The tool's own contract: 0 passed, 1 found a violation, 2 could not run.
    pub fn assert_clean(&self) {
        assert_ne!(
            self.code, 2,
            "dz-conformance could not run at all:\n{}",
            self.stderr
        );
        assert_eq!(
            self.code, 0,
            "the specification's own rule set found violations in what this \
             publisher wrote and this recorder kept:\n{}",
            self.stderr
        );
    }
}

/// Replays the archive, writes it as a pcap and runs the tool over it.
pub fn conformance_of(archive: &Recorded, feed: &str) -> Verdict {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let pcap = dir.path().join("replayed.pcap");
    write_pcap(&pcap, &replay(&archive.object));

    let out = Command::new(tool())
        .arg("-feed")
        .arg(feed)
        .arg("-pcap")
        .arg(&pcap)
        .arg("-group")
        .arg(GROUP.to_string())
        .arg("-mktdata-port")
        .arg(port_of(PortRole::Mktdata).to_string())
        .arg("-refdata-port")
        .arg(port_of(PortRole::Refdata).to_string())
        .arg("-snapshot-port")
        .arg(port_of(PortRole::Snapshot).to_string())
        .output()
        .expect("the conformance tool runs");

    Verdict {
        code: out.status.code().expect("the tool was not signalled"),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}
