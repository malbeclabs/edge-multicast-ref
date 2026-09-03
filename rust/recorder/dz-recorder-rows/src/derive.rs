//! One object into one [`RowBatch`], and nothing outside that object consulted.
//!
//! # Pure, and why that is the property everything rests on
//!
//! Every value a row carries is observable inside the object being loaded, or is
//! stated by the manifest beside it. There is no cross-object cursor and no
//! dependence on load order, so re-running an object replaces its own rows and
//! nothing else — which is what makes idempotence on `(object key, sha256)` a
//! property rather than a procedure. A loader that is not a function of the
//! object it is given is one whose output cannot be reproduced.
//!
//! The one exception is deliberate and bounded: [`DeriveInput::preceding`] is
//! the immediately preceding segment's trailer, and it decides exactly one bit —
//! [`Era::anchor_certain`](crate::Era::anchor_certain). Absent, the boundary era
//! is written uncertain and the load proceeds. **The loader never waits for it.**
//! A loader that must see segment *n−1* before it can anchor segment *n* is a
//! loader that stalls on the first eviction, and under a staging budget that
//! evicts, the predecessor is routinely gone.
//!
//! # It reads the 24-byte header, and nothing past it
//!
//! No message walk, no decode. `DatagramHeader::peek` judges nothing but the
//! buffer's length: `decode` refuses an unsupported schema version and an
//! out-of-range declared length, which is correct for a subscriber and wrong for
//! anything counting loss, because the datagram a decoder would refuse still
//! carries the sequence number whose absence is the finding.
//!
//! # What it refuses
//!
//! Three things, and each refusal is the alternative to a finding drawn from
//! something we did to the evidence ourselves:
//!
//! - **A digest that does not match the manifest.** A finding drawn from an
//!   object whose sha256 was never checked is a finding about a file, not about
//!   a feed. Verification is part of loading and not an operator's habit, and a
//!   mismatch names the object rather than loading part of it.
//! - **A replay that did not end at a block boundary.** A short window read as a
//!   complete one is a sequence gap with nothing admitted behind it — a
//!   publisher finding manufactured out of our own truncation.
//! - **A capture-drop scope the archive does not state, or states twice
//!   differently.** Every subtraction below is valid only at a declared scope,
//!   and a default here would license one the archive never claimed.

use std::collections::BTreeMap;
use std::io::{BufReader, Read};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use dz_edge_core::DatagramHeader;
use dz_recorder_archive::{InstanceCoverage, SegmentManifest};
use dz_recorder_core::{CaptureDropScope, ChannelInstance, RecordedDatagram, Source, SourceError};
use dz_recorder_loss::{DeriverLimits, EraCoverage, InstanceLoss, LossDeriver, Unexplained};
use dz_recorder_replay::{ArchiveSource, Termination};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::rows::{
    Datagram, DropScope, Era, Nanos, PortRoleLabel, RoleJoinRow, RowBatch, SegmentCoverage,
    SequenceGap, Verdict,
};

/// Bytes read from the object at a time while its digest is checked.
///
/// The whole object is not held in memory for the check: an object is a rotation
/// bound — hundreds of megabytes — and a loader that had to hold one to hash it
/// would be sized by the archive rather than by the rows.
const DIGEST_CHUNK: usize = 1 << 16;

/// Deriving rows from one object failed, and no partial row set was produced.
#[derive(Debug, Error)]
pub enum DeriveError {
    #[error(
        "{object_key}: the object hashes to {found} and its manifest states {stated}; no row is \
         derived from an object whose bytes are not the ones the manifest describes"
    )]
    DigestMismatch {
        object_key: String,
        stated: String,
        found: String,
    },
    #[error("reading {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "{object_key}: the source failed before it was exhausted, so no window in it is \
         complete: {source}"
    )]
    Source {
        object_key: String,
        #[source]
        source: SourceError,
    },
    #[error(
        "{object_key}: the replay ended as {termination} rather than on a block boundary, so \
         the window is short and any gap derived from it is one we truncated ourselves"
    )]
    Incomplete {
        object_key: String,
        termination: String,
    },
    #[error(
        "{object_key}: the object's own section declares its capture drops at `{section}` and \
         its manifest at `{manifest}`; a subtraction under the wrong scope is how a false \
         publisher-loss finding is made"
    )]
    ScopeDisagreement {
        object_key: String,
        section: String,
        manifest: String,
    },
    #[error(
        "{object_key}: neither the object's section nor its manifest states the scope its \
         capture drops are valid at, and a default here would license a subtraction the archive \
         never claimed"
    )]
    ScopeUnstated { object_key: String },
}

/// One channel instance's `Reset Count` on the last datagram of a segment.
///
/// In arrival order, which is the whole reason this exists: the manifest's
/// `reset_counts_seen` is a *set*, so a predecessor that itself spanned a reset
/// cannot say which member came last, and `255`→`0` is a reset like any other.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceReset {
    pub source_addr: Ipv4Addr,
    pub channel_id: u8,
    pub dst_port: u16,
    pub reset_count: u8,
}

/// What loading one segment leaves behind for the next one.
///
/// Small, serialisable and held by the loader: it is the evidence the adjacency
/// check needs, and it costs one row in a ledger rather than a second walk of
/// the predecessor's object.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentTrailer {
    pub segment_seq: u64,
    /// Cumulative, as the counter is. The delta against the next segment's
    /// total is the only quantity that says anything about a window.
    pub interface_drop_total: u64,
    pub instances: Vec<InstanceReset>,
}

impl SegmentTrailer {
    #[must_use]
    pub fn last_reset_count(&self, key: &ChannelInstance) -> Option<u8> {
        self.instances
            .iter()
            .find(|i| {
                i.source_addr == key.source
                    && i.channel_id == key.channel_id
                    && i.dst_port == key.dst_port
            })
            .map(|i| i.reset_count)
    }

    /// Whether this trailer describes the segment immediately before
    /// `segment_seq`.
    ///
    /// `segment_seq` restarts at 0 on every recorder run, so density is the only
    /// available evidence that two segments belong to one run — and a run
    /// boundary means the recorder was down, which is exactly a case where
    /// continuity is genuinely unknown rather than merely unrecorded.
    #[must_use]
    pub const fn precedes(&self, segment_seq: u64) -> bool {
        self.segment_seq.saturating_add(1) == segment_seq
    }
}

/// Everything one object's derivation needs that is not in the object.
#[derive(Debug, Clone, Copy)]
pub struct DeriveInput<'a> {
    pub manifest: &'a SegmentManifest,
    /// The scope the archive declares its capture drops at. Resolved by
    /// [`derive_object`] from the object's own section, cross-checked against
    /// the manifest.
    pub drop_scope: CaptureDropScope,
    /// The immediately preceding segment's trailer, when the loader has it.
    /// `None` is *unknown*, and never *there was none*.
    pub preceding: Option<&'a SegmentTrailer>,
}

/// The rows, and what the next segment's derivation will want.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derived {
    pub rows: RowBatch,
    pub trailer: SegmentTrailer,
    /// Datagrams too short to hold a 24-byte header. Archived, counted by the
    /// manifest, and attributable to no channel instance — so they carry no
    /// row, and a reader is told rather than left to notice the difference
    /// between the manifest's count and the rows'.
    pub short_datagrams: u64,
}

/// Verifies the object against its manifest, then derives every row it holds.
///
/// This is the entry point a loader uses. The digest is checked before the
/// object is opened for replay, so a row is never derived from bytes the
/// manifest does not describe.
///
/// # Errors
///
/// [`DeriveError`]: an unreadable object, a digest mismatch, a replay that did
/// not end cleanly, or a capture-drop scope the archive does not state.
pub fn derive_object(
    object: &Path,
    input_manifest: &SegmentManifest,
    preceding: Option<&SegmentTrailer>,
) -> Result<Derived, DeriveError> {
    verify_digest(object, input_manifest)?;

    let mut source = ArchiveSource::open(object).map_err(|source| match source {
        SourceError::Io(source) => DeriveError::Io {
            path: object.to_path_buf(),
            source,
        },
        other => DeriveError::Source {
            object_key: input_manifest.object_key.clone(),
            source: other,
        },
    })?;
    let drop_scope = resolve_scope(&source, input_manifest)?;

    let input = DeriveInput {
        manifest: input_manifest,
        drop_scope,
        preceding,
    };
    let derived = derive(&mut source, &input)?;

    // After the walk, because a tear is only knowable once the reader has
    // reached it. The digest above already rules out a truncated object; this
    // catches the block a whole, undamaged archive holds that this reader will
    // not read as a datagram — a foreign capture, a merged file — where the
    // stream ends early and every count taken from it is short.
    if source.terminated_by() != Termination::Eof {
        return Err(DeriveError::Incomplete {
            object_key: input_manifest.object_key.clone(),
            termination: format!(
                "{:?}{}",
                source.terminated_by(),
                source
                    .last_error()
                    .map(|e| format!(" ({e})"))
                    .unwrap_or_default()
            ),
        });
    }
    Ok(derived)
}

/// The pure half: rows out of a [`Source`], with the provenance stated.
///
/// Split from [`derive_object`] so that the derivation is exercisable over any
/// source — a stream assembled in a test, a live capture — and so that the
/// verification a loader must not skip is a step a caller cannot forget by
/// accident: the only path that opens a file is the one that hashes it first.
///
/// # Errors
///
/// [`DeriveError::Source`] if the source failed before it was exhausted. The
/// datagrams already folded in are discarded: a window that is not complete may
/// not be reported as one.
pub fn derive<S: Source + ?Sized>(
    source: &mut S,
    input: &DeriveInput<'_>,
) -> Result<Derived, DeriveError> {
    let manifest = input.manifest;
    let scope = DropScope::from(input.drop_scope);
    let limits = DeriverLimits::default();

    let mut loss = LossDeriver::new(input.drop_scope);
    let mut datagram = Vec::new();
    let mut last_reset: BTreeMap<ChannelInstance, u8> = BTreeMap::new();
    // Per `(instance, sequence number)`, the admitted loss the datagram at that
    // number carried. `drop_delta` is what the handle lost *before* the datagram
    // it rides on, so this is what makes an admission attributable to the run it
    // closes rather than to the instance as a whole — see `admitted_for_run`.
    let mut admitted_before: BTreeMap<(ChannelInstance, u64), u32> = BTreeMap::new();
    let mut short_datagrams = 0u64;

    while let Some(dg) = source.next().map_err(|source| DeriveError::Source {
        object_key: manifest.object_key.clone(),
        source,
    })? {
        // Before the header, and for every datagram: the handle's loss is the
        // handle's whatever we can read of the datagram that carried the delta.
        loss.observe(&dg);

        let Ok(header) = DatagramHeader::peek(dg.payload) else {
            short_datagrams += 1;
            continue;
        };
        let key = ChannelInstance::new(*dg.src.ip(), header.channel_id, dg.dst.port());
        // The same bound the loss deriver applies, so that the trailer this
        // load hands to the next one describes the same instances the rows do.
        if last_reset.contains_key(&key) || last_reset.len() < limits.max_instances {
            last_reset.insert(key, header.reset_count);
        }
        if dg.drop_delta != 0 {
            // First arrival wins, as the deriver's own delivered ranges do: a
            // duplicate is not a second admission of the same loss.
            admitted_before
                .entry((key, header.sequence_number))
                .or_insert(dg.drop_delta);
        }
        datagram.push(datagram_row(manifest, scope, &dg, &header));
    }

    let report = loss.finish();
    let mut era = Vec::new();
    let mut sequence_gap = Vec::new();

    for loss in report.instances() {
        let key = loss.instance;
        for coverage in &loss.eras {
            era.push(era_row(
                manifest,
                key,
                coverage,
                boundary(coverage, key, input),
            ));
        }
        // Whether a per-instance subtraction is valid at all, which is the
        // scope question and not the quantity one.
        let subtraction_is_valid = !matches!(
            report.unexplained(&key),
            None | Some(Unexplained::Unverifiable)
        );
        let interface_drops = interface_drop_delta(manifest, input.preceding);
        for run in &loss.runs {
            let anchor = loss
                .eras
                .iter()
                .find(|e| e.ordinal == run.era_ordinal)
                .map(|e| (Nanos(e.anchor_ts_ns), boundary(e, key, input).0));
            sequence_gap.push(gap_row(
                manifest,
                scope,
                loss,
                run,
                GapEvidence {
                    era_anchor_ts: anchor.map_or(Nanos(run.before_ts_ns), |(ts, _)| ts),
                    anchor_certain: anchor.map_or(0, |(_, certain)| certain),
                    admitted: admitted_for_run(&admitted_before, key, run),
                    subtraction_is_valid,
                    interface_drops,
                    on_redundant_path: on_redundant_path(&report, loss, run),
                },
            ));
        }
    }

    let segment_coverage = manifest
        .instances
        .iter()
        .map(|(key, coverage)| coverage_row(manifest, scope, *key, coverage))
        .collect();

    Ok(Derived {
        rows: RowBatch {
            object_key: manifest.object_key.clone(),
            object_sha256: manifest.sha256.clone(),
            datagram,
            era,
            segment_coverage,
            sequence_gap,
            // No runner ran. The table is written by the conformance runner over
            // replay, which is the other half of the design's plan 3, and an
            // empty vector here is the honest statement that nothing judged
            // this object — where a `pass` row would be a pass over a rule that
            // never ran.
            conformance_finding: Vec::new(),
        },
        trailer: SegmentTrailer {
            segment_seq: manifest.segment_seq,
            interface_drop_total: manifest.interface_drop_total,
            instances: last_reset
                .into_iter()
                .map(|(key, reset_count)| InstanceReset {
                    source_addr: key.source,
                    channel_id: key.channel_id,
                    dst_port: key.dst_port,
                    reset_count,
                })
                .collect(),
        },
        short_datagrams,
    })
}

/// Hashes the object and holds it against the manifest.
///
/// # Errors
///
/// [`DeriveError::Io`] if the object cannot be read, and
/// [`DeriveError::DigestMismatch`] if the bytes are not the ones described.
pub fn verify_digest(object: &Path, manifest: &SegmentManifest) -> Result<(), DeriveError> {
    let io = |source: std::io::Error| DeriveError::Io {
        path: object.to_path_buf(),
        source,
    };
    let mut reader = BufReader::new(std::fs::File::open(object).map_err(io)?);
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; DIGEST_CHUNK];
    loop {
        let read = reader.read(&mut buf).map_err(io)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    let found = hex(&hasher.finalize());
    // Case-insensitively, because the digest is carried as text: the manifest
    // writes lowercase, and an object described by something that wrote upper
    // is still described.
    if !found.eq_ignore_ascii_case(&manifest.sha256) {
        return Err(DeriveError::DigestMismatch {
            object_key: manifest.object_key.clone(),
            stated: manifest.sha256.clone(),
            found,
        });
    }
    Ok(())
}

/// The scope, from the object's own section, cross-checked against the manifest.
///
/// The section is preferred because it travels *inside* the bytes: an object
/// copied, renamed or pulled out of a bucket by hand still says what its drop
/// counts mean, and a loader that had to be told out of band is a loader that
/// can be told wrong. The manifest is nonetheless checked, because a
/// disagreement between the two means one of them describes different bytes.
fn resolve_scope(
    source: &ArchiveSource,
    manifest: &SegmentManifest,
) -> Result<CaptureDropScope, DeriveError> {
    let stated = manifest.capture_drop_scope.trim();
    match (source.capture_drop_scope(), stated) {
        (Some(section), "") => Ok(section),
        (Some(section), stated) if section.as_str() == stated => Ok(section),
        (Some(section), stated) => Err(DeriveError::ScopeDisagreement {
            object_key: manifest.object_key.clone(),
            section: section.as_str().to_owned(),
            manifest: stated.to_owned(),
        }),
        (None, "port-role") => Ok(CaptureDropScope::PortRole),
        (None, "capture-handle") => Ok(CaptureDropScope::CaptureHandle),
        (None, _) => Err(DeriveError::ScopeUnstated {
            object_key: manifest.object_key.clone(),
        }),
    }
}

fn datagram_row(
    manifest: &SegmentManifest,
    drop_scope: DropScope,
    dg: &RecordedDatagram<'_>,
    header: &DatagramHeader,
) -> Datagram {
    Datagram {
        recv_ts: Nanos(dg.recv_ts_ns),
        send_ts: Nanos(header.send_timestamp_ns),
        recv_ts_kind: dg.recv_ts_kind.into(),
        source_addr: *dg.src.ip(),
        channel_id: header.channel_id,
        dst_port: dg.dst.port(),
        feed: manifest.feed.clone(),
        port_role: dg.role.into(),
        group_addr: *dg.dst.ip(),
        sequence_number: header.sequence_number,
        reset_count: header.reset_count,
        segment_seq: manifest.segment_seq,
        payload_len: u16::try_from(dg.payload.len()).unwrap_or(u16::MAX),
        wire_payload_len: dg.wire_payload_len,
        drop_delta: dg.drop_delta,
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        env: manifest.env.clone(),
        drop_scope,
        object_key: manifest.object_key.clone(),
        object_sha256: manifest.sha256.clone(),
    }
}

/// `(anchor_certain, continuation)` for one era.
///
/// An era opened by a transition observed inside this object is certain and
/// opens an era. The instance's *first* era in the object is the one the
/// predecessor decides, and the three answers are the three states the design
/// enumerates: settled as new, settled as a continuation, or not settled at all.
fn boundary(coverage: &EraCoverage, key: ChannelInstance, input: &DeriveInput<'_>) -> (u8, u8) {
    if coverage.ordinal > 1 {
        return (1, 0);
    }
    let Some(preceding) = input
        .preceding
        .filter(|t| t.precedes(input.manifest.segment_seq))
    else {
        // Neither of the two guesses. Treating it as a continuation merges two
        // sequence spaces and hides every gap between them; treating it as new
        // invents a boundary that may not exist, which puts a false reset in
        // front of an operator.
        return (0, 0);
    };
    match preceding.last_reset_count(&key) {
        Some(last) if last == coverage.reset_count => (1, 1),
        // The predecessor was there and this instance was not in it, which is
        // as much a new era as a changed `Reset Count` is: the instance's
        // sequence space did not carry across a segment it is absent from.
        Some(_) | None => (1, 0),
    }
}

fn era_row(
    manifest: &SegmentManifest,
    key: ChannelInstance,
    coverage: &EraCoverage,
    (anchor_certain, continuation): (u8, u8),
) -> Era {
    Era {
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        feed: manifest.feed.clone(),
        source_addr: key.source,
        channel_id: key.channel_id,
        dst_port: key.dst_port,
        anchor_ts: Nanos(coverage.anchor_ts_ns),
        anchor_seq: coverage.anchor_seq,
        reset_count: coverage.reset_count,
        segment_seq: manifest.segment_seq,
        anchor_certain,
        continuation,
        object_key: manifest.object_key.clone(),
        object_sha256: manifest.sha256.clone(),
    }
}

fn coverage_row(
    manifest: &SegmentManifest,
    drop_scope: DropScope,
    key: ChannelInstance,
    coverage: &InstanceCoverage,
) -> SegmentCoverage {
    SegmentCoverage {
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        env: manifest.env.clone(),
        feed: manifest.feed.clone(),
        source_addr: key.source,
        channel_id: key.channel_id,
        dst_port: key.dst_port,
        segment_seq: manifest.segment_seq,
        start_ts: Nanos(manifest.start_ns),
        end_ts: Nanos(manifest.end_ns),
        first_seq: coverage.first_seq,
        last_seq: coverage.last_seq,
        datagram_count: coverage.count,
        reset_counts_seen: coverage.reset_counts_seen.clone(),
        capture_drop_total: manifest.capture_drop_total,
        interface_drop_total: manifest.interface_drop_total,
        drop_scope,
        roles_joined: manifest
            .roles_joined
            .iter()
            .map(|r| RoleJoinRow(r.role.clone(), r.group, r.port))
            .collect(),
        object_key: manifest.object_key.clone(),
        object_sha256: manifest.sha256.clone(),
        build_version: manifest.build_version.clone(),
        build_commit: manifest.build_commit.clone(),
        config_hash: manifest.config_hash.clone(),
    }
}

/// What one gap row needs that the run itself does not carry.
struct GapEvidence {
    era_anchor_ts: Nanos,
    anchor_certain: u8,
    /// The recorder's own admitted loss covering *this run*, not the instance's
    /// total over the window.
    admitted: u64,
    /// Whether a per-instance subtraction is valid at the archive's declared
    /// scope. When it is not there is no residue to report, whatever the number
    /// above says.
    subtraction_is_valid: bool,
    interface_drops: Option<u64>,
    on_redundant_path: Option<u8>,
}

/// The recorder's own admitted loss covering one run.
///
/// **Per run, and not per instance, because the consuming report sums this
/// column over a window.** `LossReport::unexplained` is the instance's residue
/// over the whole window, which is the right number for one row per instance and
/// the wrong one for one row per gap: carried on every gap row of an instance it
/// would be counted once per gap, so an instance with four gaps would report four
/// times its own loss.
///
/// The attribution is the archive's own. `drop_delta` is what the capture handle
/// lost *before* the datagram it rides on, so the datagram at `missing_to + 1`
/// is the one whose delta explains this run — that is the same reading the
/// recorder's own `epb_dropcount` has, and it is why the debt is carried forward
/// onto the next datagram that gets through rather than recorded where it
/// happened.
///
/// Whether that number may be *subtracted* is a separate question, decided by
/// the scope. See [`GapEvidence::subtraction_is_valid`].
fn admitted_for_run(
    admitted_before: &BTreeMap<(ChannelInstance, u64), u32>,
    key: ChannelInstance,
    run: &dz_recorder_loss::SequenceRun,
) -> u64 {
    admitted_before
        .get(&(key, run.missing_to.saturating_add(1)))
        .map_or(0, |delta| u64::from(*delta))
}

fn gap_row(
    manifest: &SegmentManifest,
    drop_scope: DropScope,
    loss: &InstanceLoss,
    run: &dz_recorder_loss::SequenceRun,
    evidence: GapEvidence,
) -> SequenceGap {
    // No per-instance subtraction is meaningful at every scope, and where it is
    // not there is no residue to report: zero would exonerate the publisher and
    // the missing count would accuse it, and the archive can support neither.
    let unexplained_count = evidence.subtraction_is_valid.then(|| {
        run.missing_count()
            .saturating_sub(evidence.admitted.min(run.missing_count()))
    });
    SequenceGap {
        site: manifest.site.clone(),
        recorder: manifest.recorder.clone(),
        env: manifest.env.clone(),
        feed: manifest.feed.clone(),
        port_role: PortRoleLabel::from(run.role),
        group_addr: run.group,
        source_addr: run.instance.source,
        channel_id: run.instance.channel_id,
        dst_port: run.instance.dst_port,
        reset_count: run.reset_count,
        era_index: u32::try_from(run.era_ordinal).unwrap_or(u32::MAX),
        era_anchor_ts: evidence.era_anchor_ts,
        anchor_certain: evidence.anchor_certain,
        missing_from: run.missing_from,
        missing_to: run.missing_to,
        missing_count: run.missing_count(),
        reference_seqs: loss.reference_seqs,
        before_ts: Nanos(run.before_ts_ns),
        after_ts: Nanos(run.after_ts_ns),
        // A site has no clock reading for a datagram it never received, so the
        // publisher's send stamps come from a site that did — which is the
        // cross-site read this loader does not perform.
        sent_from_ts: None,
        sent_to_ts: None,
        admitted_recorder: evidence.admitted,
        admitted_scope: drop_scope,
        unexplained_count,
        interface_drops: evidence.interface_drops,
        seen_elsewhere: None,
        on_redundant_path: evidence.on_redundant_path,
        verdict: verdict(
            unexplained_count,
            evidence.interface_drops,
            evidence.on_redundant_path,
        ),
        object_key: manifest.object_key.clone(),
    }
}

/// The five verdicts, in the order the design tests them.
///
/// The three exculpatory answers come first, and each is decided from evidence
/// one object holds. [`Verdict::Publisher`] is not among the outcomes here, and
/// its absence is the design: it needs a datagram absent from *every* site with
/// no recorder overflow anywhere, so a loader over one object answers
/// [`Verdict::Unverifiable`] and a later pass over the rows upgrades it. Writing
/// `publisher` from one vantage would be reporting the strongest finding this
/// system makes on the weakest evidence it has.
fn verdict(
    unexplained_count: Option<u64>,
    interface_drops: Option<u64>,
    on_redundant_path: Option<u8>,
) -> Verdict {
    match unexplained_count {
        // Fully covered by our own admitted drops: a counter and an alert on
        // us, and never a publisher finding.
        Some(0) => Verdict::Recorder,
        // Not covered by ours, and loss upstream of the capture point rose over
        // the window: a switch or link question.
        _ if interface_drops.is_some_and(|d| d > 0) => Verdict::Upstream,
        // Absent here and present in a redundant instance on the same channel
        // and port: the redundancy earned its cost, and this is not feed loss.
        _ if on_redundant_path == Some(1) => Verdict::Path,
        // Either the residue could not be computed, or it could and the only
        // remaining explanation is one no single site can establish.
        _ => Verdict::Unverifiable,
    }
}

/// Loss upstream of the capture point, as a delta over this window.
///
/// The counter is cumulative and never resets, so a host carries the sum of
/// every burst it ever had: a panel showing the total shows history, and only
/// the delta says anything about now. Absent when the preceding segment is not
/// available, because then there is nothing to subtract from — and an absent
/// delta costs a verdict nothing, since the only verdict it could reach is one
/// this loader does not write.
fn interface_drop_delta(
    manifest: &SegmentManifest,
    preceding: Option<&SegmentTrailer>,
) -> Option<u64> {
    let preceding = preceding.filter(|t| t.precedes(manifest.segment_seq))?;
    // Saturating: a counter that went backwards is a host that rebooted or an
    // interface that was replaced, and a wrapped subtraction there would report
    // eighteen quintillion drops upstream.
    Some(
        manifest
            .interface_drop_total
            .saturating_sub(preceding.interface_drop_total),
    )
}

/// Whether a redundant instance on the same channel and port delivered every
/// sequence value this run is missing.
///
/// Two source addresses on one channel and port are two instances of one
/// channel, each advancing its own space, and a sequence value absent from one
/// but present in the other is `path` loss rather than feed loss. The ratio of
/// those is the fill rate — the number that says whether the redundancy is
/// earning its cost.
///
/// `None` when this channel and port carried no second source in this object:
/// there is then nothing to have looked in, and a `0` would say we looked and
/// found nothing. The judgement is per object, which is why the column is
/// nullable rather than a claim about the feed.
fn on_redundant_path(
    report: &dz_recorder_loss::LossReport,
    loss: &InstanceLoss,
    run: &dz_recorder_loss::SequenceRun,
) -> Option<u8> {
    let mut any_peer = false;
    let mut covered = false;
    for peer in report.instances() {
        if peer.instance.channel_id != loss.instance.channel_id
            || peer.instance.dst_port != loss.instance.dst_port
            || peer.instance.source == loss.instance.source
        {
            continue;
        }
        any_peer = true;
        covered |= delivered_whole_run(peer, run);
    }
    any_peer.then_some(u8::from(covered))
}

/// Whether `peer` delivered every value in the run, inside an era carrying the
/// same wire `Reset Count`.
///
/// Derived from what the loss deriver already decided: an era states the span it
/// covered, and the runs state which values inside that span nobody delivered.
/// A value inside the span and outside every run is one the archive holds.
///
/// The `Reset Count` has to match because a sequence number means nothing across
/// a reset: a redundant publisher in a different era of its own is not carrying
/// the same datagram.
fn delivered_whole_run(peer: &InstanceLoss, run: &dz_recorder_loss::SequenceRun) -> bool {
    peer.eras.iter().any(|era| {
        era.reset_count == run.reset_count
            && era.first_seq <= run.missing_from
            && run.missing_to <= era.last_seq
            && !peer.runs.iter().any(|peer_run| {
                peer_run.era_ordinal == era.ordinal
                    && peer_run.missing_from <= run.missing_to
                    && run.missing_from <= peer_run.missing_to
            })
    })
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // Writing to a String cannot fail.
        let _ = write!(out, "{byte:02x}");
    }
    out
}
