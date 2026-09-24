//! A replayed archive, written back out as the pcapng segment the tool reads.
//!
//! The rule set takes a capture file and decides which format it is from the
//! file's own magic, so this could be either. It is pcapng because **a classic
//! `pcap` record has nowhere to write `epb_dropcount`**, and that single field
//! is the only thing in a segment that separates capture loss from publisher
//! loss. Converting to classic `pcap` strips the recorder's own admission of
//! what it failed to record, which leaves every gap the recorder caused sitting
//! in front of the rule set with nothing to say it was not the publisher's.
//! `dz-conformance` grades an admitted gap `capture_loss` rather than
//! `violation`; it cannot do that over a field the conversion deleted.
//!
//! **The writer here is the recorder's own.** `dz-recorder-archive`'s
//! [`SegmentWriter`] writes the segment, not a hand-rolled encoder beside it,
//! so what the tool is shown is produced by the code that produced the archive.
//! That is also what makes the design's cross-language claim literal rather than
//! approximate: *a pcapng segment written by the Rust writer is read by the Go
//! conformance tool.* A second encoder here would be a second thing to keep in
//! agreement with the first, and the disagreement would be invisible — the gate
//! would go on passing over bytes no recorder would ever write.
//!
//! Three things the writer already gets right, each of which a careless
//! conversion gets wrong:
//!
//! **The two lengths are written separately.** A block carries a captured length
//! and an original length, and writing one value into both asserts *this
//! datagram was not truncated*. A datagram whose `wire_payload_len` exceeds its
//! payload is exactly one the capture cut short, and declaring it complete hands
//! the rule set a body shorter than its own declared length — a structural
//! violation with our snap length behind it.
//!
//! **Captured link headers are reproduced, never rebuilt.** Rebuilding discards
//! the identification field, the fragmentation flags and the checksums the
//! archive kept on purpose. The writer reproduces them when the archive has
//! them and synthesises only when it does not, and it marks any datagram whose
//! provenance contradicts the section's claim.
//!
//! **The timestamps stay nanoseconds.** The interfaces are written with
//! `if_tsresol = 9`. A classic `pcap` record's last header field is
//! microseconds, so a conversion drops three digits; here the question does not
//! arise.
//!
//! # One file per multicast group
//!
//! The split is by destination address, and the reason is **not** the tool's
//! `-group` flag. That flag is inert in replay: the tool consults it only when
//! opening a live socket, and nothing in its rule engine reads it. What decides
//! the split is the port map, which is keyed on the destination port **alone**.
//! Two groups carried on the same three port roles — the ordinary arrangement —
//! would be read out of one file as a single series, and the two sequence
//! spaces interleaved would be reported as loss in both.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use dz_recorder_archive::{
    LinkHeaders, RoleJoin, SegmentWriter, SegmentWriterConfig, ALL_ROLES, LINK_HEADER_LEN,
};
use dz_recorder_core::{CaptureDropScope, RecorderIdentity, SinkError};
use dz_recorder_replay::{ArchiveSource, LinkHeaderProvenance, OwnedDatagram};

/// What stopped a datagram becoming part of a segment.
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
    #[error("writing {path}: {detail}")]
    Encode { path: PathBuf, detail: String },
    /// The archive states no section of its own, so there is nothing to write
    /// one from. Named rather than defaulted: see [`SectionProvenance::of`].
    #[error(
        "the archive states no {missing}, so a segment written from it would claim one it \
         cannot support"
    )]
    Unstated { missing: &'static str },
}

/// What the archive being replayed said about itself, carried into the segment
/// written from it.
///
/// **Every field is read off the source and none of them has a default**, and
/// that is the whole point of the type. A re-write is a second copy of an
/// archive, and a second copy that states a recorder identity, a link-header
/// provenance or a drop scope the first one did not is a copy asserting
/// something no evidence supports. The two that would do real damage are the
/// last two: `link_headers=captured` over synthesised bytes invites a reader to
/// treat a zero TTL as an observation, and a `capture_drop_scope` invented as
/// `port-role` invites the analysis tier to subtract one role's drops from that
/// role's sequence gaps when the ring never knew whose frames it lost.
#[derive(Debug, Clone)]
pub struct SectionProvenance {
    pub identity: RecorderIdentity,
    pub link_headers: LinkHeaders,
    pub capture_drop_scope: CaptureDropScope,
}

impl SectionProvenance {
    /// What the source archive states, or a refusal naming what it does not.
    ///
    /// `ArchiveSource` recovers all three from the Section Header block, and a
    /// pcapng this recorder did not write states none of them. Such a capture is
    /// judgeable — the datagrams are all there is and the tool needs nothing
    /// else — but it is not re-writable *as one of ours*, and the refusal says
    /// which fact was missing rather than inventing it.
    pub fn of(source: &ArchiveSource) -> Result<Self, BridgeError> {
        let identity = source
            .identity()
            .ok_or(BridgeError::Unstated {
                missing: "recorder identity",
            })?
            .clone();
        let link_headers = match source.link_headers() {
            LinkHeaderProvenance::Captured => LinkHeaders::Captured,
            LinkHeaderProvenance::Synthesised => LinkHeaders::Synthesised,
            LinkHeaderProvenance::Unstated => {
                return Err(BridgeError::Unstated {
                    missing: "link-header provenance",
                })
            }
        };
        let capture_drop_scope = source.capture_drop_scope().ok_or(BridgeError::Unstated {
            missing: "capture drop scope",
        })?;
        Ok(Self {
            identity,
            link_headers,
            capture_drop_scope,
        })
    }
}

/// One group's datagrams, as one file the tool can be pointed at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSegment {
    /// The group this file holds, and the `-group` it is named for. The tool
    /// ignores that flag in replay; the field is kept because a caller
    /// dispatching one invocation per group needs to know which is which.
    pub group: Ipv4Addr,
    pub path: PathBuf,
    /// How many datagrams went in, so a caller can refuse to invoke the tool
    /// over a file holding none rather than reading its clean exit as a pass.
    pub datagram_count: u64,
}

/// Writes one pcapng segment per multicast group present, into `dir`.
///
/// The files come back ordered by group, and a group with no datagrams produces
/// no file: the tool's exit code cannot distinguish *clean* from *saw nothing*,
/// so an empty file is a trap rather than a convenience.
pub fn write_group_segments(
    dir: &Path,
    datagrams: &[OwnedDatagram],
    provenance: &SectionProvenance,
) -> Result<Vec<GroupSegment>, BridgeError> {
    let mut by_group: BTreeMap<Ipv4Addr, Vec<&OwnedDatagram>> = BTreeMap::new();
    for dg in datagrams {
        by_group.entry(*dg.dst.ip()).or_default().push(dg);
    }

    let mut out = Vec::with_capacity(by_group.len());
    for (group, group_datagrams) in by_group {
        let path = dir.join(format!("group-{group}.pcapng"));
        write_segment(&path, group_datagrams.iter().copied(), provenance)?;
        out.push(GroupSegment {
            group,
            path,
            datagram_count: group_datagrams.len() as u64,
        });
    }
    Ok(out)
}

/// Writes one pcapng segment holding exactly the datagrams given.
pub fn write_segment<'a, I>(
    path: &Path,
    datagrams: I,
    provenance: &SectionProvenance,
) -> Result<(), BridgeError>
where
    I: IntoIterator<Item = &'a OwnedDatagram>,
{
    let datagrams: Vec<&OwnedDatagram> = datagrams.into_iter().collect();
    let cfg = SegmentWriterConfig {
        identity: provenance.identity.clone(),
        roles_joined: roles_carrying(&datagrams),
        link_headers: provenance.link_headers,
        capture_drop_scope: provenance.capture_drop_scope,
    };

    let file = File::create(path).map_err(|source| BridgeError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut writer =
        SegmentWriter::new(BufWriter::new(file), &cfg).map_err(|e| sink_error(path, e))?;
    for dg in datagrams {
        writer
            .write(&dg.as_recorded())
            .map_err(|e| sink_error(path, e))?;
    }
    let (inner, _stats) = writer.finish().map_err(|e| sink_error(path, e))?;
    // `finish` flushes the `BufWriter` it was handed; the `File` underneath is
    // closed by the drop below. Nothing here needs the stats: the manifest is
    // the archive's record of what it held, and a second one computed over a
    // re-write would be a second number able to disagree with it.
    drop(inner);
    Ok(())
}

/// The roles that actually carry a datagram in this file, in the order that
/// fixes `interface_id`.
///
/// Derived from the datagrams and not from a configuration, because the
/// datagrams are all a re-write has. **This is a narrower claim than the
/// archive's `roles_joined` and deliberately so**: a role that was joined and
/// stayed silent is absent here, and describing it as joined would put a
/// statement in the section that these bytes cannot support. The distinction
/// costs the rule set nothing — it starves rules from the port flags it is
/// given, which the caller sets from the recorder's configuration — and it keeps
/// the file honest about itself.
fn roles_carrying(datagrams: &[&OwnedDatagram]) -> Vec<RoleJoin> {
    ALL_ROLES
        .into_iter()
        .filter_map(|role| {
            let dg = datagrams.iter().find(|dg| dg.role == role)?;
            Some(RoleJoin::on(role, *dg.dst.ip(), dg.dst.port()))
        })
        .collect()
}

/// The largest a segment holding these datagrams can be, before any of it is
/// written.
///
/// The manifest states `datagram_count` and `payload_byte_count` before the
/// object is opened, so a caller can refuse an object it has no room for rather
/// than filling the disk the archive is staged on.
///
/// An upper bound and not the exact size, which is the difference from the
/// arithmetic a classic `pcap` allowed. A pcapng block's length depends on
/// options the writer adds per datagram — a drop count, a stamp-kind mark, a
/// provenance mark — and on padding to a four-byte boundary, so an exact figure
/// would mean predicting the writer's decisions here and then keeping the
/// prediction in step with them. A bound cannot silently become wrong in the
/// direction that matters.
#[must_use]
pub fn segment_len_bound(datagrams: &[OwnedDatagram]) -> u64 {
    datagrams.iter().fold(SECTION_BOUND, |acc, dg| {
        acc + BLOCK_BOUND + link_headers_len(dg) as u64 + dg.payload.len() as u64
    })
}

/// Section Header and three Interface Description blocks, with room for the
/// options the writer puts in them: the identity, the build, the configuration
/// hash and the per-role descriptions are all bounded strings.
const SECTION_BOUND: u64 = 4096;

/// One Enhanced Packet Block's header, its options and its padding.
const BLOCK_BOUND: u64 = 256;

fn link_headers_len(dg: &OwnedDatagram) -> usize {
    dg.link_headers.as_ref().map_or(LINK_HEADER_LEN, Vec::len)
}

fn sink_error(path: &Path, e: SinkError) -> BridgeError {
    match e {
        SinkError::Io(source) => BridgeError::Io {
            path: path.to_path_buf(),
            source,
        },
        other => BridgeError::Encode {
            path: path.to_path_buf(),
            detail: other.to_string(),
        },
    }
}
