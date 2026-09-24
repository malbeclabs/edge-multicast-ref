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
//!
//! **Splitting has a cost the writer cannot pay for itself: the recorder's
//! admissions do not know which group they belong to.** A `drop_delta` is what
//! the capture handle lost between the previous datagram and this one, and it
//! rides on whichever datagram happened to be kept next — of any group. Left on
//! that datagram, a split puts group A's losses in group B's file, and A's file
//! then holds a sequence gap with no admission beside it: the exact
//! misattribution the pcapng format was chosen to prevent, back by another
//! route. So an admission is carried into **every** group's file, onto the next
//! datagram each one keeps. See [`write_group_segments`] for why that
//! overstatement is the right way to be wrong.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufWriter};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use dz_edge_core::PortRole;
use dz_recorder_archive::{
    role_index, LinkHeaders, RoleJoin, SegmentWriter, SegmentWriterConfig, ALL_ROLES,
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
    #[error("writing {path}: {io_error}")]
    Io {
        path: PathBuf,
        #[source]
        io_error: io::Error,
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
/// **Every field is read from the archive and none of them has a default**, and
/// that is the whole point of the type. A re-write is a second copy of an
/// archive, and a second copy that states a recorder identity, a link-header
/// provenance or a drop scope the first one did not is a copy asserting
/// something no evidence supports. The two that would do real damage are the
/// last two: `link_headers=captured` over synthesised bytes invites a reader to
/// treat a zero TTL as an observation, and a `capture_drop_scope` invented as
/// `port-role` invites the analysis tier to subtract one role's drops from that
/// role's sequence gaps when the ring never knew whose frames it lost.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionProvenance {
    pub identity: RecorderIdentity,
    pub link_headers: LinkHeaders,
    pub capture_drop_scope: CaptureDropScope,
}

impl SectionProvenance {
    /// What the archive being replayed states, or a refusal naming what it does
    /// not.
    ///
    /// `ArchiveSource` recovers all three from the Section Header block, and a
    /// pcapng this recorder did not write states none of them. Such a capture is
    /// judgeable — the datagrams are all there is and the tool needs nothing
    /// else — but it is not re-writable *as one of ours*, and the refusal says
    /// which fact was missing rather than inventing it.
    pub fn of(archive: &ArchiveSource) -> Result<Self, BridgeError> {
        Self::from_section(
            archive.identity(),
            archive.link_headers(),
            archive.capture_drop_scope(),
        )
    }

    /// The same, from the three facts a section states, each of which it may
    /// not.
    ///
    /// Separate from [`of`](Self::of) so that every refusal is reachable on its
    /// own: a capture this recorder did not write states none of the three, and
    /// a test through it could only ever reach the first.
    pub fn from_section(
        identity: Option<&RecorderIdentity>,
        link_headers: LinkHeaderProvenance,
        capture_drop_scope: Option<CaptureDropScope>,
    ) -> Result<Self, BridgeError> {
        let identity = identity
            .ok_or(BridgeError::Unstated {
                missing: "recorder identity",
            })?
            .clone();
        let link_headers = match link_headers {
            LinkHeaderProvenance::Captured => LinkHeaders::Captured,
            LinkHeaderProvenance::Synthesised => LinkHeaders::Synthesised,
            LinkHeaderProvenance::Unstated => {
                return Err(BridgeError::Unstated {
                    missing: "link-header provenance",
                })
            }
        };
        let capture_drop_scope = capture_drop_scope.ok_or(BridgeError::Unstated {
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
///
/// # Every admission goes into every group's file
///
/// A datagram's `drop_delta` is carried into each group's file, onto the next
/// datagram that file holds, and not left only on the datagram it arrived on.
/// How far it is carried is the drop scope's to say:
///
/// - **`capture-handle`**: the ring lost frames before it could tell anything
///   apart, so the admission may belong to any group and any role. It is
///   carried to the next datagram of every group, whatever its role.
/// - **`port-role`**: the handle was per role, so the admission belongs to the
///   role it arrived on — but two groups on one set of ports share that handle,
///   so it may still belong to either group. It is carried to the next
///   datagram of that role in every group.
///
/// **This overstates each file's loss, and that is the direction to be wrong
/// in.** The rule set grades a window with an admitted drop in it as
/// `capture_loss` rather than `violation`, so an admission carried into a file
/// that did not need it costs that file some coverage — a window reported as
/// unverifiable. An admission withheld from the file that did need it costs a
/// publisher a violation for a loss the recorder caused. The first is a
/// coverage figure an operator can read; the second is an accusation.
///
/// An admission after a group's last datagram has nowhere to ride in that file
/// and is dropped from it. That loses nothing the rule set could have used: a
/// gap is only visible between two sequence numbers, and there is no second
/// one.
pub fn write_group_segments(
    dir: &Path,
    datagrams: &[OwnedDatagram],
    provenance: &SectionProvenance,
) -> Result<Vec<GroupSegment>, BridgeError> {
    let groups: Vec<Ipv4Addr> = datagrams
        .iter()
        .map(|dg| *dg.dst.ip())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    // Keyed on the group and, at port-role scope, the role — by `role_index`, the
    // fixed mapping `interface_id` already uses: the unit a pending admission is
    // owed to.
    let owed_to = |group: Ipv4Addr, role: PortRole| match provenance.capture_drop_scope {
        CaptureDropScope::CaptureHandle => (group, None),
        CaptureDropScope::PortRole => (group, Some(role_index(role))),
    };
    let mut pending: BTreeMap<(Ipv4Addr, Option<u32>), u32> = BTreeMap::new();
    let mut by_group: BTreeMap<Ipv4Addr, Vec<OwnedDatagram>> = BTreeMap::new();
    for dg in datagrams {
        if dg.drop_delta != 0 {
            for group in &groups {
                let owed = pending.entry(owed_to(*group, dg.role)).or_default();
                *owed = owed.saturating_add(dg.drop_delta);
            }
        }
        let group = *dg.dst.ip();
        let mut kept = dg.clone();
        kept.drop_delta = pending.remove(&owed_to(group, dg.role)).unwrap_or(0);
        by_group.entry(group).or_default().push(kept);
    }

    let mut out = Vec::with_capacity(by_group.len());
    for (group, group_datagrams) in by_group {
        let path = dir.join(format!("group-{group}.pcapng"));
        write_segment(&path, group_datagrams.iter(), provenance)?;
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

    let file = File::create(path).map_err(|io_error| BridgeError::Io {
        path: path.to_path_buf(),
        io_error,
    })?;
    let mut writer =
        SegmentWriter::new(BufWriter::new(file), &cfg).map_err(|e| sink_error(path, e))?;
    for dg in datagrams {
        let mut recorded = dg.as_recorded();
        // What was sent is at least what was kept. A block whose original
        // length undercuts its captured length is one every reader rejects, and
        // `OwnedDatagram` is a public struct a caller can hand this with the
        // field unset. Replay already floors it; this is for everyone else.
        let held = u32::try_from(recorded.payload.len()).unwrap_or(u32::MAX);
        recorded.wire_payload_len = recorded.wire_payload_len.max(held);
        writer.write(&recorded).map_err(|e| sink_error(path, e))?;
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

fn sink_error(path: &Path, e: SinkError) -> BridgeError {
    match e {
        SinkError::Io(io_error) => BridgeError::Io {
            path: path.to_path_buf(),
            io_error,
        },
        other => BridgeError::Encode {
            path: path.to_path_buf(),
            detail: other.to_string(),
        },
    }
}
