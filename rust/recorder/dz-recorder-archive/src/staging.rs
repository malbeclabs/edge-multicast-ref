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

use crate::compress::{Faults, InFlight};

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
    /// Submitted to the compressor and not yet picked up. Counted like any
    /// other history and evictable like it, but taken last: it is the only copy
    /// of the window it holds, and the object it is about to become is one
    /// nothing else can produce.
    queued: bool,
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
    /// What the compressor has been handed and has not started on. The budget
    /// counts it — an unbounded queue nothing counts is an unbounded disk — and
    /// evicts it only after everything else.
    queued: Option<Arc<InFlight>>,
    /// Where a budget that cannot be met is reported, when there is a writer
    /// behind this watermark to report it to.
    faults: Option<Arc<Faults>>,
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
            queued: None,
            faults: None,
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

    pub(crate) fn track_queued(&mut self, queued: Arc<InFlight>) {
        self.queued = Some(queued);
    }

    pub(crate) fn track_faults(&mut self, faults: Arc<Faults>) {
        self.faults = Some(faults);
    }

    /// Objects, oldest first: published, and retained after a publication that
    /// could not land.
    #[must_use]
    pub fn objects(&self) -> Vec<SegmentObject> {
        self.scan().0
    }

    /// Every byte the budget governs.
    ///
    /// Three files are excluded and no others: the segment being appended to,
    /// the segment the compressor is publishing right now, and the newest one
    /// waiting in its queue. Each is a single file bounded by `rotate_bytes`,
    /// and counting an uncompressed transient — one about to shrink by an order
    /// of magnitude — is how it evicts history eviction was not asked to touch.
    ///
    /// The *rest* of that queue is counted, and that is the difference that
    /// matters: the queue has no bound of its own. A publication that stalls —
    /// a hung completed_dir mount, compression slower than rotation — puts
    /// every later segment behind it, and excluding those is a staging
    /// directory that grows without bound while this method reports it
    /// comfortably under budget.
    ///
    /// A segment still under a working name that is none of the three is an
    /// orphan, and it is counted and evictable, because bytes nothing accounts
    /// for are bytes nothing bounds.
    ///
    /// Everything else in either directory is counted, including what a failed
    /// publication left behind, because an outage that accumulates unaccounted
    /// bytes is an outage the budget cannot bound.
    #[must_use]
    pub fn bytes_on_disk(&self) -> u64 {
        let (objects, residue) = self.scan();
        governed_bytes(&objects, &residue)
    }

    /// The bytes on the disk that eviction cannot reach.
    ///
    /// A file in either directory that this module cannot name is not ours to
    /// delete — `completed_dir` is a directory a shipper writes into, and a
    /// budget that doubles as a licence to delete somebody else's data is worse
    /// than an unbounded one. Counted, because bytes nothing bounds are the
    /// thing the watermark exists to report, and separated, because evicting
    /// our own history does not reclaim one byte of them.
    #[must_use]
    pub fn unreclaimable_bytes(&self) -> u64 {
        self.scan()
            .1
            .iter()
            .filter(|r| !r.evictable)
            .map(|r| r.bytes)
            .sum()
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
        let unreclaimable: u64 = residue
            .iter()
            .filter(|r| !r.evictable)
            .map(|r| r.bytes)
            .sum();
        // Only what eviction can actually take. Counting the rest in here is
        // how one stray file in completed_dir — a shipper's own, a mount point,
        // anything this module cannot name — at or over the budget makes every
        // sweep delete the entire archive and still report success: the total
        // never falls below a floor eviction cannot move, so the loop runs to
        // the end every time, and the disk is no emptier for it.
        let mut total = governed_bytes(&objects, &residue) - unreclaimable;

        // Said out loud, because the budget is not being met and no amount of
        // eviction will meet it. The disk is bounded by this plus staging_max,
        // and that is a fact an operator has to be told rather than one the
        // recorder should act on by deleting what it can reach.
        if unreclaimable > self.staging_max {
            if let Some(faults) = &self.faults {
                faults.record(format!(
                    "staging holds {unreclaimable} bytes eviction cannot reach, over a budget of {}",
                    self.staging_max
                ));
            }
        }

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
        // Orphans first and queued segments last, for the reason objects come
        // before either: a queued segment is the only copy of the window it
        // holds and an object nothing else can now produce, so it is the last
        // history worth giving up — but it is still history the budget can
        // reach, which is what keeps a stalled publication from filling the
        // disk.
        //
        // All but the newest of them. The queue is what has to be bounded, not
        // the segment that was just closed: taking that one costs the most
        // recent window — the one an operator is most likely to be asking about
        // — and hands the compressor a publication that can only fail. Keeping
        // it bounds the disk at the budget plus one segment, which `rotate_bytes`
        // already bounds, and leaves the queue itself as short as the budget
        // requires.
        let newest_queued = newest_queued(&residue).cloned();
        let by_priority = residue.iter().filter(|r| r.evictable && !r.queued).chain(
            residue
                .iter()
                .filter(|r| r.evictable && r.queued)
                .filter(|r| Some(&r.path) != newest_queued.as_ref()),
        );
        for partial in by_priority {
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

    fn is_queued(&self, path: &Path) -> bool {
        self.queued.as_ref().is_some_and(|q| q.holds(path))
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
                // symlink_metadata, for the reason file_len gives.
                let metadata = fs::symlink_metadata(&path).ok();
                let queued = self.is_queued(&path);
                residue.push(Residue {
                    bytes: metadata.as_ref().map_or(0, std::fs::Metadata::len),
                    modified: metadata
                        .and_then(|m| m.modified().ok())
                        .unwrap_or(SystemTime::UNIX_EPOCH),
                    evictable: queued || is_recovered_segment(&name) || is_working_segment(&name),
                    queued,
                    path,
                });
            }
        }
        objects.sort_by_key(|o| (o.start_ns, o.segment_seq));
        residue.sort_by_key(|r| r.modified);
        (objects, residue)
    }
}

/// What the budget counts, over both directories.
///
/// The newest queued segment is left out with the open one and the one being
/// published: three bounded transients, for the same reason in all three cases.
/// Everything queued behind it is in, because that backlog is the part with no
/// bound of its own.
fn governed_bytes(objects: &[SegmentObject], residue: &[Residue]) -> u64 {
    let newest = newest_queued(residue);
    objects.iter().map(|o| o.bytes).sum::<u64>()
        + residue
            .iter()
            .filter(|r| Some(&r.path) != newest)
            .map(|r| r.bytes)
            .sum::<u64>()
}

/// The most recent segment handed to the compressor and not yet started on.
fn newest_queued(residue: &[Residue]) -> Option<&PathBuf> {
    residue
        .iter()
        .filter(|r| r.queued)
        .max_by_key(|r| r.modified)
        .map(|r| &r.path)
}

/// The bytes of the file itself, never of what it points at.
///
/// `metadata` follows a symlink, so a link in either directory would charge the
/// budget for a file on another filesystem — and eviction, deleting the link,
/// would reclaim the length of the link and not the length that was counted.
fn file_len(path: &Path) -> u64 {
    fs::symlink_metadata(path).map(|m| m.len()).unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::thread::sleep;
    use std::time::Duration;

    /// A working-segment name, so the file is history the budget can classify.
    fn segment(dir: &Path, seq: u64, bytes: usize) -> PathBuf {
        let path = dir.join(open_segment_name(seq));
        let mut f = fs::File::create(&path).expect("a segment file");
        f.write_all(&vec![0u8; bytes]).expect("its bytes");
        // The queue's order is modification time, and a test that writes three
        // files inside one timestamp tick is a test that asserts nothing.
        sleep(Duration::from_millis(10));
        path
    }

    #[test]
    fn a_stalled_publication_queue_is_counted_after_its_first_segment() {
        // The unbounded half of the archive. A publication that stalls leaves
        // every later rotation sitting in an unbounded job queue: excluded from
        // the budget, those bytes grow the disk without limit while
        // bytes_on_disk reports it under. Counted from the second one, because
        // the first is a bounded transient like the open segment beside it.
        let dir = tempfile::tempdir().expect("a temporary directory");
        let staging = dir.path().join("staging");
        let completed = dir.path().join("completed");
        fs::create_dir_all(&staging).expect("staging");
        fs::create_dir_all(&completed).expect("completed");

        let queued = Arc::new(InFlight::default());
        let mut w = StagingWatermark::new(staging.clone(), completed, u64::MAX);
        // Not the open segment: 99 is what the writer is holding.
        w.track_open_segment(99);
        w.track_queued(Arc::clone(&queued));

        for seq in 1..=3 {
            queued.enter(segment(&staging, seq, 1000));
        }

        assert_eq!(
            w.bytes_on_disk(),
            2000,
            "a queue of three counts two: the newest is the bounded transient"
        );
    }

    #[test]
    fn eviction_bounds_the_queue_without_taking_the_segment_just_closed() {
        // The other half of the same rule. The queue has to be reachable or the
        // disk is unbounded, and the newest segment has to survive or every
        // rotation hands the compressor a publication that can only fail — and
        // costs the most recent window, which is the one an operator is most
        // likely to be asking about.
        let dir = tempfile::tempdir().expect("a temporary directory");
        let staging = dir.path().join("staging");
        let completed = dir.path().join("completed");
        fs::create_dir_all(&staging).expect("staging");
        fs::create_dir_all(&completed).expect("completed");

        let queued = Arc::new(InFlight::default());
        let mut w = StagingWatermark::new(staging.clone(), completed, 1500);
        w.track_open_segment(99);
        w.track_queued(Arc::clone(&queued));

        let paths: Vec<PathBuf> = (1..=3).map(|seq| segment(&staging, seq, 1000)).collect();
        for path in &paths {
            queued.enter(path.clone());
        }

        w.enforce().expect("eviction");

        assert!(
            !paths[0].exists(),
            "the oldest queued segment was not evicted"
        );
        assert!(
            paths[2].exists(),
            "the segment just closed was evicted out from under the compressor"
        );
    }
}
