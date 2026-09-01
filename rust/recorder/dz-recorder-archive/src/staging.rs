//! The staging layout, and the watermark over everything on the disk it fills.
//!
//! The staging directory is the buffer for an object-storage outage, and when it
//! fills **the oldest segment is deleted and counted, and the write path is
//! never blocked.** This is the opposite of what a naive implementation does. A
//! writer that blocks on a full disk stalls the drain thread, which overflows
//! the receive queue, which loses live data — so an outage, a credential expiry
//! or a slow disk is converted into a feed-loss incident and into false
//! publisher-loss findings in every archive written during it. Deleting the
//! oldest segment loses history, which is bounded, counted and alertable.
//!
//! Every name the two directories can hold is built here, because the budget can
//! only be enforced over files it can classify: a file this module cannot name
//! is a file eviction cannot reach, and an outage that produces one is an
//! unbounded disk.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

use dz_recorder_core::SinkError;

use crate::compress::InFlight;

const SEGMENT_PREFIX: &str = "segment-";
const PCAPNG_SUFFIX: &str = ".pcapng";
const ZSTD_SUFFIX: &str = ".pcapng.zst";
const MANIFEST_SUFFIX: &str = ".manifest.json";
/// What marks a segment whose run ended before it rotated.
const RECOVERED_MARKER: &str = ".recovered-";

/// The open segment: `segment-{seq}.pcapng` in the staging directory.
#[must_use]
pub(crate) fn open_segment_name(segment_seq: u64) -> String {
    format!("{SEGMENT_PREFIX}{segment_seq}{PCAPNG_SUFFIX}")
}

/// The sequence number in a working segment's name, if the name is one.
///
/// Startup reads it back to adopt what a dead run left under this name: the
/// number is the previous run's, and the file has to carry a name the budget
/// accounts for before this run's sequence reaches it.
#[must_use]
pub(crate) fn working_segment_seq(name: &str) -> Option<u64> {
    let seq = name
        .strip_prefix(SEGMENT_PREFIX)?
        .strip_suffix(PCAPNG_SUFFIX)?;
    // Digits and nothing else: a file this crate did not write is not ours to
    // adopt, and an honest budget must not become a licence to delete somebody
    // else's data.
    if seq.is_empty() || !seq.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    seq.parse().ok()
}

/// Where a partial segment is moved so that the next one does not truncate it.
///
/// Stamped with the wall clock rather than the sequence number alone, because
/// `segment_seq` restarts at 0 on every run and the file being kept is the
/// previous run's.
#[must_use]
pub(crate) fn recovered_segment_name(segment_seq: u64, at_ns: u64) -> String {
    format!("{SEGMENT_PREFIX}{segment_seq}{RECOVERED_MARKER}{at_ns}{PCAPNG_SUFFIX}")
}

/// A segment that could not be published, named as the object it was going to
/// be.
///
/// Named this way so the budget accounts for it and eviction can reach it: a
/// publication that cannot land is exactly the outage the buffer exists for, and
/// bytes nothing accounts for are bytes nothing bounds.
#[must_use]
pub(crate) fn unpublished_object_name(start_ns: u64, end_ns: u64, segment_seq: u64) -> String {
    format!("{start_ns}-{end_ns}-{segment_seq}{PCAPNG_SUFFIX}")
}

#[must_use]
pub(crate) fn manifest_name(start_ns: u64, end_ns: u64, segment_seq: u64) -> String {
    format!("{start_ns}-{end_ns}-{segment_seq}{MANIFEST_SUFFIX}")
}

/// The name an object is assembled under before it is published.
///
/// Hidden and suffixed, so a shipper that matches object keys never sees a
/// partial object, and so the compressor's own temporary files are identifiable
/// as such by anything sweeping the directory.
#[must_use]
pub(crate) fn temp_name(final_name: &str) -> String {
    format!(".{final_name}.part")
}

#[must_use]
pub(crate) fn is_compressor_temp(name: &str) -> bool {
    name.starts_with('.') && name.ends_with(".part")
}

/// One object and the manifest that describes it, published or retained.
#[derive(Debug, Clone)]
pub struct SegmentObject {
    pub path: PathBuf,
    pub manifest_path: PathBuf,
    /// From the object key, which is a wall-clock nanosecond stamp and
    /// therefore orders segments across recorder runs — `segment_seq` restarts
    /// at 0 on every run and cannot.
    pub start_ns: u64,
    pub segment_seq: u64,
    /// The object and its manifest together: they are deleted together, so they
    /// are accounted together.
    pub bytes: u64,
}

/// Everything else on the disk the budget has to answer for.
#[derive(Debug, Clone)]
struct Residue {
    path: PathBuf,
    bytes: u64,
    /// The only age a file whose name carries no receive window has.
    modified: SystemTime,
    /// A partial or still-working segment is history and eviction may take it;
    /// anything else is counted and left alone. A manifest is deleted only with
    /// its object, and a file this crate did not write is not ours to delete at
    /// all — a budget honest about the disk must not become a licence to remove
    /// somebody's data.
    evictable: bool,
}

pub struct StagingWatermark {
    staging_dir: PathBuf,
    completed_dir: PathBuf,
    staging_max: u64,
    /// Which `segment-{seq}.pcapng` is the one being written right now.
    ///
    /// Only that one file is genuinely transient. Every other name of that shape
    /// is a segment the compressor still holds or one a publication left behind,
    /// and excluding those by shape alone leaves bytes `bytes_on_disk` never
    /// counts and `enforce` can never reach — the unbounded disk this module's
    /// naming scheme exists to prevent.
    open_segment_seq: u64,
    /// What the compressor is holding, when there is a compressor behind this
    /// watermark. A segment mid-publication is the one other file eviction must
    /// not take: its source is the compressor's input, and taking it destroys an
    /// object that was about to land.
    in_flight: Option<Arc<InFlight>>,
    segments_evicted_total: u64,
    bytes_evicted_total: u64,
}

impl StagingWatermark {
    #[must_use]
    pub fn new(staging_dir: PathBuf, completed_dir: PathBuf, staging_max: u64) -> Self {
        Self {
            staging_dir,
            completed_dir,
            staging_max,
            open_segment_seq: 0,
            in_flight: None,
            segments_evicted_total: 0,
            bytes_evicted_total: 0,
        }
    }

    /// Follows the writer's sequence number, so the one transient file is the
    /// one the writer is actually holding open.
    pub fn track_open_segment(&mut self, segment_seq: u64) {
        self.open_segment_seq = segment_seq;
    }

    pub(crate) fn track_in_flight(&mut self, in_flight: Arc<InFlight>) {
        self.in_flight = Some(in_flight);
    }

    /// Objects, oldest first: published, and retained after a publication that
    /// could not land.
    #[must_use]
    pub fn objects(&self) -> Vec<SegmentObject> {
        self.scan().0
    }

    /// Every byte the budget governs.
    ///
    /// Two files are excluded and no others: the segment being appended to, and
    /// a segment the compressor is publishing right now. Both are bounded — by
    /// `rotate_bytes` and by a single compression — and counting an uncompressed
    /// transient would let it evict history eviction was not asked to touch. A
    /// segment still under a working name that is *neither* of those is an
    /// orphan, and it is counted and evictable, because bytes nothing accounts
    /// for are bytes nothing bounds.
    ///
    /// Everything else in either directory is counted, including what a failed
    /// publication left behind, because an outage that accumulates unaccounted
    /// bytes is an outage the budget cannot bound.
    #[must_use]
    pub fn bytes_on_disk(&self) -> u64 {
        let (objects, residue) = self.scan();
        objects.iter().map(|o| o.bytes).sum::<u64>() + residue.iter().map(|r| r.bytes).sum::<u64>()
    }

    #[must_use]
    pub fn segments_on_disk(&self) -> usize {
        self.objects().len()
    }

    #[must_use]
    pub fn oldest_segment_seq(&self) -> Option<u64> {
        self.objects().first().map(|o| o.segment_seq)
    }

    #[must_use]
    pub fn segments_evicted_total(&self) -> u64 {
        self.segments_evicted_total
    }

    #[must_use]
    pub fn bytes_evicted_total(&self) -> u64 {
        self.bytes_evicted_total
    }

    /// Deletes oldest-first until the archive is inside its budget.
    ///
    /// Called on rotation and on a periodic sweep, never on the write path.
    /// Whole objects go first and partial segments last: a partial is the only
    /// copy of the window it holds, so it is the last history worth giving up.
    pub fn enforce(&mut self) -> Result<(), SinkError> {
        let (objects, residue) = self.scan();
        let mut total = objects.iter().map(|o| o.bytes).sum::<u64>()
            + residue.iter().map(|r| r.bytes).sum::<u64>();
        if total <= self.staging_max {
            return Ok(());
        }

        // One undeletable file must not stop the buffer from being bounded, so
        // the sweep runs to the end and reports afterwards.
        let mut failure = None;
        for object in objects {
            if total <= self.staging_max {
                break;
            }
            match fs::remove_file(&object.path) {
                Ok(()) => {
                    // The manifest may already be gone if a shipper took it;
                    // that is not a failure of eviction.
                    let _ = fs::remove_file(&object.manifest_path);
                    total = total.saturating_sub(object.bytes);
                    self.segments_evicted_total += 1;
                    self.bytes_evicted_total += object.bytes;
                }
                Err(e) => failure = failure.or(Some(e)),
            }
        }
        for partial in residue.iter().filter(|r| r.evictable) {
            if total <= self.staging_max {
                break;
            }
            match fs::remove_file(&partial.path) {
                Ok(()) => {
                    total = total.saturating_sub(partial.bytes);
                    self.segments_evicted_total += 1;
                    self.bytes_evicted_total += partial.bytes;
                }
                Err(e) => failure = failure.or(Some(e)),
            }
        }

        match failure {
            Some(e) => Err(SinkError::Io(e)),
            None => Ok(()),
        }
    }

    fn is_in_flight(&self, path: &Path) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(|in_flight| in_flight.holds(path))
    }

    /// Both directories, classified. Objects come back oldest first, partial
    /// segments oldest first by modification time.
    fn scan(&self) -> (Vec<SegmentObject>, Vec<Residue>) {
        let open_segment = open_segment_name(self.open_segment_seq);
        let mut objects = Vec::new();
        let mut residue = Vec::new();
        for dir in [&self.completed_dir, &self.staging_dir] {
            let Ok(entries) = fs::read_dir(dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let Some(name) = path.file_name().and_then(|n| n.to_str()).map(str::to_owned)
                else {
                    continue;
                };
                // The open segment is one path and not one name shape: the same
                // name anywhere else is a file the budget has to answer for.
                let is_open = dir == &self.staging_dir && name == open_segment;
                if is_open || is_compressor_temp(&name) || self.is_in_flight(&path) {
                    continue;
                }
                if let Some((start_ns, _end_ns, segment_seq)) = parse_object_key(&path) {
                    let manifest_path = dir.join(manifest_name_of(&path).unwrap_or_default());
                    let bytes = file_len(&path) + file_len(&manifest_path);
                    objects.push(SegmentObject {
                        path,
                        manifest_path,
                        start_ns,
                        segment_seq,
                        bytes,
                    });
                    continue;
                }
                if name.ends_with(MANIFEST_SUFFIX) && object_of(dir, &name).is_some() {
                    // Accounted with the object it describes, and deleted with
                    // it.
                    continue;
                }
                let metadata = entry.metadata().ok();
                residue.push(Residue {
                    bytes: metadata.as_ref().map_or(0, std::fs::Metadata::len),
                    modified: metadata
                        .and_then(|m| m.modified().ok())
                        .unwrap_or(SystemTime::UNIX_EPOCH),
                    evictable: is_recovered_segment(&name) || is_working_segment(&name),
                    path,
                });
            }
        }
        objects.sort_by_key(|o| (o.start_ns, o.segment_seq));
        residue.sort_by_key(|r| r.modified);
        (objects, residue)
    }
}

fn file_len(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// `segment-{seq}.pcapng`: a segment under its working name.
///
/// The open one is excluded by name and one mid-publication by the compressor's
/// own account of what it holds. What is left is an orphan — a publication whose
/// source removal failed, a rename that could not land — and an orphan is
/// history: counted, and evictable, because what the budget accounts for
/// eviction has to be able to reach.
fn is_working_segment(name: &str) -> bool {
    working_segment_seq(name).is_some()
}

fn is_recovered_segment(name: &str) -> bool {
    name.starts_with(SEGMENT_PREFIX)
        && name.contains(RECOVERED_MARKER)
        && name.ends_with(PCAPNG_SUFFIX)
}

/// `<start_ns>-<end_ns>-<segment_seq>.pcapng[.zst]`.
fn parse_object_key(path: &Path) -> Option<(u64, u64, u64)> {
    let name = path.file_name()?.to_str()?;
    let stem = name
        .strip_suffix(ZSTD_SUFFIX)
        .or_else(|| name.strip_suffix(PCAPNG_SUFFIX))?;
    let mut parts = stem.split('-');
    let start = parts.next()?.parse().ok()?;
    let end = parts.next()?.parse().ok()?;
    let seq = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((start, end, seq))
}

fn manifest_name_of(path: &Path) -> Option<String> {
    let (start, end, seq) = parse_object_key(path)?;
    Some(manifest_name(start, end, seq))
}

/// The object a manifest describes, compressed or not, if it is still there.
fn object_of(dir: &Path, manifest: &str) -> Option<PathBuf> {
    let stem = manifest.strip_suffix(MANIFEST_SUFFIX)?;
    [ZSTD_SUFFIX, PCAPNG_SUFFIX]
        .into_iter()
        .map(|suffix| dir.join(format!("{stem}{suffix}")))
        .find(|path| path.exists())
}
