//! Classic pcap out of a replayed archive, because the tool reads classic pcap
//! and the archive is pcapng.
//!
//! Written by hand: twenty-four bytes of file header and sixteen per record,
//! and a dependency that produced that would be more code than this. What is
//! *not* incidental is the three things below, each of which is a place where a
//! careless conversion manufactures a finding the recorder itself caused.
//!
//! **The two record lengths are written separately.** A pcap record carries an
//! included length and an original length. Writing one value into both asserts
//! *this datagram was not truncated*, and a datagram whose
//! [`wire_payload_len`](OwnedDatagram::wire_payload_len) exceeds its payload is
//! exactly one the capture cut short. Declaring it complete hands the rule set
//! a body shorter than its own declared length — a structural violation with
//! our snaplen behind it. The synthesised IPv4 and UDP length fields state the
//! wire length for the same reason: a header that declared only the bytes that
//! survived would make the truncation invisible twice.
//!
//! **Captured link headers are reproduced, never rebuilt.**
//! [`OwnedDatagram::link_headers`] is `Some` when the capture mode read the
//! Ethernet, IPv4 and UDP bytes off the interface, and `None` when the archive
//! synthesised them and they are therefore not evidence about the wire.
//! Rebuilding over the captured case discards the identification field, the
//! fragmentation flags and the checksums the archive kept on purpose.
//!
//! **One file per multicast group.** The tool takes one `-group` and one port
//! per role, so an archive holding several groups needs one invocation each and
//! therefore one file each. A single file holding every group would be read
//! under one group's flags and the rest would be silently unevaluated, which is
//! the vacuity `na` exists to make visible.

use std::collections::BTreeMap;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::{Path, PathBuf};

use dz_recorder_replay::OwnedDatagram;

/// The synthesised Ethernet, IPv4 and UDP bytes, when the archive has none of
/// its own.
pub const SYNTHESISED_LINK_HEADER_LEN: usize = 42;

/// The record header a classic pcap writes in front of every frame.
pub const RECORD_HEADER_LEN: usize = 16;

/// The file header a classic pcap opens with.
pub const FILE_HEADER_LEN: usize = 24;

/// What stopped a datagram becoming a pcap record.
///
/// Every variant is a fact about the archive rather than about the caller, so
/// none of them is a panic: an object is not a caller's argument, and a loader
/// that aborted on one datagram would leave the rest of a retained archive
/// unjudged for ever.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    #[error("writing {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    /// An IPv4 datagram cannot carry this, so an archive that claims one is an
    /// archive whose length field is not to be believed.
    #[error(
        "a datagram claims {wire_payload_len} payload bytes, which does not fit an IPv4 \
         datagram behind {header_len} bytes of link header"
    )]
    DatagramTooLarge {
        wire_payload_len: u64,
        header_len: usize,
    },
    /// Classic pcap stamps seconds in 32 bits, which runs out in 2106.
    #[error("a receive stamp of {recv_ts_ns}ns does not fit a classic pcap's 32-bit seconds")]
    TimestampOutOfRange { recv_ts_ns: u64 },
}

/// One group's datagrams, as one file the tool can be pointed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPcap {
    /// The `-group` this file is to be read under.
    pub group: Ipv4Addr,
    pub path: PathBuf,
    /// How many datagrams went in, so a caller can refuse to invoke the tool
    /// over a file holding none rather than reading its clean exit as a pass.
    pub datagram_count: u64,
}

/// Writes one classic pcap per multicast group present, into `dir`.
///
/// The files come back ordered by group, and a group with no datagrams produces
/// no file: the tool's exit code cannot distinguish *clean* from *saw nothing*,
/// so an empty file is a trap rather than a convenience.
pub fn write_group_pcaps(
    dir: &Path,
    datagrams: &[OwnedDatagram],
) -> Result<Vec<GroupPcap>, BridgeError> {
    let mut by_group: BTreeMap<Ipv4Addr, Vec<&OwnedDatagram>> = BTreeMap::new();
    for dg in datagrams {
        by_group.entry(*dg.dst.ip()).or_default().push(dg);
    }

    let mut out = Vec::with_capacity(by_group.len());
    for (group, group_datagrams) in by_group {
        let path = dir.join(format!("group-{group}.pcap"));
        write_pcap(&path, group_datagrams.iter().copied())?;
        out.push(GroupPcap {
            group,
            path,
            datagram_count: group_datagrams.len() as u64,
        });
    }
    Ok(out)
}

/// Writes one classic pcap holding exactly the datagrams given.
///
/// Little-endian, microsecond resolution, `LINKTYPE_ETHERNET` — what
/// `pcapgo.Reader` expects.
pub fn write_pcap<'a, I>(path: &Path, datagrams: I) -> Result<(), BridgeError>
where
    I: IntoIterator<Item = &'a OwnedDatagram>,
{
    let mut out = Vec::new();
    out.extend_from_slice(&0xa1b2_c3d4u32.to_le_bytes()); // magic, microseconds
    out.extend_from_slice(&2u16.to_le_bytes());
    out.extend_from_slice(&4u16.to_le_bytes());
    out.extend_from_slice(&0i32.to_le_bytes()); // no timezone offset
    out.extend_from_slice(&0u32.to_le_bytes()); // no timestamp accuracy claim
    out.extend_from_slice(&65_535u32.to_le_bytes()); // snaplen
    out.extend_from_slice(&1u32.to_le_bytes()); // LINKTYPE_ETHERNET
    debug_assert_eq!(out.len(), FILE_HEADER_LEN);

    for dg in datagrams {
        append_record(&mut out, dg)?;
    }
    std::fs::write(path, &out).map_err(|e| BridgeError::Io {
        path: path.to_path_buf(),
        source: e,
    })
}

/// How large the file for these datagrams will be, before any of it is written.
///
/// The manifest states `datagram_count` and `payload_byte_count` before the
/// object is opened, so a caller can refuse an object it has no room for rather
/// than filling the disk the archive is staged on. This is the same arithmetic
/// against bytes already in hand.
#[must_use]
pub fn pcap_len(datagrams: &[OwnedDatagram]) -> u64 {
    datagrams.iter().fold(FILE_HEADER_LEN as u64, |acc, dg| {
        acc + RECORD_HEADER_LEN as u64 + link_headers_len(dg) as u64 + dg.payload.len() as u64
    })
}

fn link_headers_len(dg: &OwnedDatagram) -> usize {
    dg.link_headers
        .as_ref()
        .map_or(SYNTHESISED_LINK_HEADER_LEN, Vec::len)
}

fn append_record(out: &mut Vec<u8>, dg: &OwnedDatagram) -> Result<(), BridgeError> {
    // What was sent, which is what the original length has to state. Below the
    // payload we hold only when the archive's own field is unset or short, and
    // a record whose original length undercut its included length would be
    // rejected by every reader — so the payload is the floor.
    let wire_payload_len = u64::from(dg.wire_payload_len).max(dg.payload.len() as u64);

    let headers = match &dg.link_headers {
        // Captured. Reproduced byte for byte: the identification field, the
        // fragmentation flags and the checksums are observations, and a
        // rebuilt header would present a fiction to any rule reading below UDP.
        Some(captured) => captured.clone(),
        // Synthesised, exactly as socket mode's archive does it and for the
        // same reason: what is being validated is the payload, and these bytes
        // only have to be well-formed enough for the tool to find it.
        None => synthesised_link_headers(dg.src, dg.dst, wire_payload_len)?,
    };

    let incl_len = headers.len() as u64 + dg.payload.len() as u64;
    let orig_len = headers.len() as u64 + wire_payload_len;

    let secs = u32::try_from(dg.recv_ts_ns / 1_000_000_000).map_err(|_| {
        BridgeError::TimestampOutOfRange {
            recv_ts_ns: dg.recv_ts_ns,
        }
    })?;
    let micros = u32::try_from(dg.recv_ts_ns % 1_000_000_000 / 1_000).expect("under a second");

    out.extend_from_slice(&secs.to_le_bytes());
    out.extend_from_slice(&micros.to_le_bytes());
    // The two lengths, and they are two. `incl_len` is what follows this
    // header; `orig_len` is what the datagram was before the capture length cut
    // it short. Equal for an untruncated datagram, and the difference is the
    // evidence that the rule set is looking at less than the publisher sent.
    out.extend_from_slice(
        &u32::try_from(incl_len)
            .map_err(|_| BridgeError::DatagramTooLarge {
                wire_payload_len,
                header_len: headers.len(),
            })?
            .to_le_bytes(),
    );
    out.extend_from_slice(
        &u32::try_from(orig_len)
            .map_err(|_| BridgeError::DatagramTooLarge {
                wire_payload_len,
                header_len: headers.len(),
            })?
            .to_le_bytes(),
    );
    out.extend_from_slice(&headers);
    out.extend_from_slice(&dg.payload);
    Ok(())
}

/// The 42 bytes of Ethernet, IPv4 and UDP a datagram travelled behind, for an
/// archive that did not keep them.
///
/// `wire_payload_len` and not the payload's length: the length fields state
/// what the publisher sent, so that a truncated datagram reads as truncated
/// rather than as a short one somebody may then blame the publisher for.
fn synthesised_link_headers(
    src: SocketAddrV4,
    dst: SocketAddrV4,
    wire_payload_len: u64,
) -> Result<Vec<u8>, BridgeError> {
    let payload_len = u16::try_from(wire_payload_len)
        .ok()
        .filter(|len| usize::from(*len) <= usize::from(u16::MAX) - 28)
        .ok_or(BridgeError::DatagramTooLarge {
            wire_payload_len,
            header_len: SYNTHESISED_LINK_HEADER_LEN,
        })?;
    let mut out = Vec::with_capacity(SYNTHESISED_LINK_HEADER_LEN);
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
    debug_assert_eq!(out.len(), SYNTHESISED_LINK_HEADER_LEN);
    Ok(out)
}
