//! Compression, hashing and publication, all of it off the write path.
//!
//! Rotation fsyncs the segment, closes it, hands the file here and returns. A
//! writer that compressed inline would stall the drain thread for the length of
//! a zstd over a whole segment, which is the failure the whole design is shaped
//! to avoid.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

use dz_recorder_core::{CompletedSegment, SinkError};
use sha2::{Digest, Sha256};

use crate::manifest::SegmentManifest;
use crate::staging::{manifest_name, temp_name, unpublished_object_name};

/// Where a fault is recorded, whichever thread saw it.
///
/// The compressor's failures reach the caller only through the completed
/// channel, and a caller that is still recording may never read it — the
/// reviewer's unwritable `completed_dir` produced five failed publications and
/// a `last_error` of `None`. An outage that is silent until the disk is full is
/// an outage nobody acts on, so every thread records here and the writer reads
/// it back.
#[derive(Debug, Default)]
pub(crate) struct Faults {
    last: Mutex<Option<String>>,
    publications_failed: AtomicU64,
    /// Recovery is not fault, and the two share nothing but a reader.
    ///
    /// Sweeping a dead run's temporary files and adopting the segments it left
    /// is routine work after an unclean restart, and it is worth stating — but
    /// recorded as a fault it puts a message in `last_error` after every such
    /// restart while `publications_failed_total` stays zero, so anything reading
    /// `last_error` as a health signal reports a fault for a recovery that
    /// worked.
    last_recovery: Mutex<Option<String>>,
    recoveries: AtomicU64,
}

impl Faults {
    pub(crate) fn record(&self, message: String) {
        // A poisoned lock still holds the last fault, and panicking here would
        // lose exactly the thing being reported.
        *self.last.lock().unwrap_or_else(PoisonError::into_inner) = Some(message);
    }

    /// Work that succeeded, on a path only an unclean restart reaches.
    pub(crate) fn record_recovery(&self, message: String) {
        self.recoveries.fetch_add(1, Ordering::Relaxed);
        *self
            .last_recovery
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(message);
    }

    pub(crate) fn publication_failed(&self, message: String) {
        self.publications_failed.fetch_add(1, Ordering::Relaxed);
        self.record(message);
    }

    pub(crate) fn last(&self) -> Option<String> {
        self.last
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn publications_failed(&self) -> u64 {
        self.publications_failed.load(Ordering::Relaxed)
    }

    pub(crate) fn last_recovery(&self) -> Option<String> {
        self.last_recovery
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn recoveries(&self) -> u64 {
        self.recoveries.load(Ordering::Relaxed)
    }
}

/// A set of segment paths the compressor can answer for.
///
/// Two of these exist, and the difference between them is the whole of the
/// budget's relationship with the compressor. **In flight** is the one segment
/// being read right now: evicting it destroys an object that was about to land,
/// so the budget leaves it alone. **Queued** is everything submitted behind it:
/// counted and evictable like any other history, because the job queue is
/// unbounded and a publication that stalls would otherwise grow staging without
/// bound while the budget reported it under.
///
/// Everything else still under a working name is an orphan — one the compressor
/// has finished with, or one a dead run left — and only the compressor knows
/// which is which, so it says so here rather than leaving it to be guessed from
/// a file name.
#[derive(Debug, Default)]
pub(crate) struct InFlight {
    paths: Mutex<HashSet<PathBuf>>,
}

impl InFlight {
    pub(crate) fn enter(&self, path: PathBuf) {
        self.lock().insert(path);
    }

    fn leave(&self, path: &Path) {
        self.lock().remove(path);
    }

    pub(crate) fn holds(&self, path: &Path) -> bool {
        self.lock().contains(path)
    }

    /// A poisoned lock still holds the truth about what is in flight, and
    /// panicking here would take a recorder down over an accounting question.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<PathBuf>> {
        self.paths.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// How a segment is stored. `zstd` is the default: the payloads are dense
/// fixed-size binary structures with high inter-record redundancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd { level: i32 },
}

impl Compression {
    /// The extension of the object that lands, which is also how a reader knows
    /// whether to decode it.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::None => "pcapng",
            Self::Zstd { .. } => "pcapng.zst",
        }
    }
}

pub(crate) struct Job {
    pub(crate) source: PathBuf,
    pub(crate) staging_dir: PathBuf,
    pub(crate) completed_dir: PathBuf,
    pub(crate) manifest: SegmentManifest,
    pub(crate) compression: Compression,
}

/// The compressor thread and the queue into it.
///
/// The queue is unbounded so that handing a segment over cannot wait: a bounded
/// queue would put the write path behind a slow compressor, which is exactly
/// the coupling being avoided.
pub(crate) struct Compressor {
    jobs: Option<Sender<Job>>,
    completed: Receiver<Result<Published, SinkError>>,
    faults: Arc<Faults>,
    in_flight: Arc<InFlight>,
    queued: Arc<InFlight>,
    thread: Option<JoinHandle<()>>,
}

impl Compressor {
    pub(crate) fn spawn() -> Self {
        let (jobs_tx, jobs_rx) = channel::<Job>();
        let (done_tx, done_rx) = channel();
        let faults = Arc::new(Faults::default());
        let thread_faults = Arc::clone(&faults);
        let in_flight = Arc::new(InFlight::default());
        let thread_in_flight = Arc::clone(&in_flight);
        let queued = Arc::new(InFlight::default());
        let thread_queued = Arc::clone(&queued);
        let thread = std::thread::Builder::new()
            .name("dz-recorder-compress".to_owned())
            .spawn(move || {
                for job in jobs_rx {
                    let source = job.source.clone();
                    // In flight only once this thread is actually reading it,
                    // and queued until then. Entering at submission instead is
                    // what makes an unbounded job queue invisible to the
                    // budget: a stalled publication — a hung completed_dir
                    // mount, compression slower than rotation — would grow
                    // staging without bound while bytes_on_disk reported it
                    // comfortably under.
                    //
                    // In this order, so the path is never in neither set: a
                    // moment as both is an accounting the budget can read
                    // safely, and a moment as neither is a file it would treat
                    // as an orphan.
                    thread_in_flight.enter(source.clone());
                    thread_queued.leave(&source);
                    // A publication that failed must not end the thread: the
                    // next segment still has to land.
                    let done = publish(&job, &thread_faults);
                    // Before the result is handed over, so that a caller which
                    // sees the completion sees a budget that already accounts
                    // for whatever the publication left behind.
                    thread_in_flight.leave(&source);
                    if done_tx.send(done).is_err() {
                        break;
                    }
                }
            })
            .expect("a thread per recorder process");
        Self {
            jobs: Some(jobs_tx),
            completed: done_rx,
            faults,
            in_flight,
            queued,
            thread: Some(thread),
        }
    }

    /// The fault state the compressor thread writes into, so the writer can
    /// answer for a publication that never landed.
    pub(crate) fn faults(&self) -> Arc<Faults> {
        Arc::clone(&self.faults)
    }

    /// What the compressor is holding, so the budget leaves those segments alone
    /// and counts every other one.
    pub(crate) fn in_flight(&self) -> Arc<InFlight> {
        Arc::clone(&self.in_flight)
    }

    /// What has been submitted and not yet picked up, so the budget counts it,
    /// can reach it, and takes it only after everything else.
    pub(crate) fn queued(&self) -> Arc<InFlight> {
        Arc::clone(&self.queued)
    }

    /// A submitted segment is queued, not in flight: counted by the budget and
    /// evictable by it, but only after every other kind of history.
    pub(crate) fn submit(&self, job: Job) -> Result<(), SinkError> {
        let source = job.source.clone();
        self.queued.enter(source.clone());
        match self
            .jobs
            .as_ref()
            .expect("the queue outlives every submission")
            .send(job)
        {
            Ok(()) => Ok(()),
            Err(_) => {
                // Nothing will publish it, so it is history the budget accounts
                // for from here on — as an orphan rather than as a queue entry.
                self.queued.leave(&source);
                Err(SinkError::Compress(
                    "the compressor thread is gone".to_owned(),
                ))
            }
        }
    }

    pub(crate) fn try_completed(&self) -> Option<Result<Published, SinkError>> {
        self.completed.try_recv().ok()
    }

    pub(crate) fn wait_completed(&self) -> Option<Result<Published, SinkError>> {
        self.completed.recv().ok()
    }
}

impl Drop for Compressor {
    fn drop(&mut self) {
        drop(self.jobs.take());
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

/// Compress, hash what was written, write the manifest, then publish by moving.
///
/// The moves are last and the manifest moves first, so a shipper never sees a
/// partial object and never sees an object it cannot attribute.
fn publish(job: &Job, faults: &Faults) -> Result<Published, SinkError> {
    let names = Names::of(job);
    match assemble(job, &names) {
        Ok(published) => {
            // The object has landed, so a source that will not delete is a
            // stale copy and not a failed publication. Reporting it as one
            // would tell an operator a segment is missing when it is not.
            if let Err(e) = fs::remove_file(&job.source) {
                faults.record(format!(
                    "removing {} after publishing {}: {e}",
                    job.source.display(),
                    names.file_name
                ));
            }
            Ok(published)
        }
        Err(e) => {
            let kept = clean_up(job, &names);
            faults.publication_failed(format!("publishing {}: {e}; {kept}", names.file_name));
            Err(e)
        }
    }
}

/// Every name one publication touches, built once so the failure path can reach
/// the same files the success path made.
struct Names {
    /// The name the object carries inside `completed_dir`. The key it lands
    /// under in object storage is the manifest's, and it is not this.
    file_name: String,
    object_tmp: PathBuf,
    manifest_name: String,
    manifest_tmp: PathBuf,
    retained: PathBuf,
}

impl Names {
    fn of(job: &Job) -> Self {
        let m = &job.manifest;
        let file_name = format!(
            "{}-{}-{}.{}",
            m.start_ns,
            m.end_ns,
            m.segment_seq,
            job.compression.extension()
        );
        let manifest_name = manifest_name(m.start_ns, m.end_ns, m.segment_seq);
        Self {
            object_tmp: job.staging_dir.join(temp_name(&file_name)),
            manifest_tmp: job.staging_dir.join(temp_name(&manifest_name)),
            retained: job.staging_dir.join(unpublished_object_name(
                m.start_ns,
                m.end_ns,
                m.segment_seq,
            )),
            file_name,
            manifest_name,
        }
    }
}

/// A segment that landed, with the manifest that describes it.
///
/// The manifest is finalised at publication — it is where the object key and the
/// digest are filled in — so handing it back costs nothing and saves every
/// in-process consumer a round trip through the file beside the object. A test
/// asserting on coverage, and a shipper deciding where to put the object, were
/// both reading a file the process had just written.
#[derive(Debug, Clone)]
pub struct Published {
    pub segment: CompletedSegment,
    pub manifest: SegmentManifest,
}

fn assemble(job: &Job, names: &Names) -> Result<Published, SinkError> {
    let m = &job.manifest;
    let (byte_count, sha256) = encode(&job.source, &names.object_tmp, job.compression)?;

    let mut manifest = m.clone();
    // The partitioned key, from the manifest's own fields: a shipper reading it
    // needs to know nothing about the layout, and the analysis tier gets a key
    // that cannot collide between two recorders at two sites.
    manifest.object_key = crate::object_key::object_key(
        &manifest.feed,
        &manifest.env,
        &manifest.site,
        &manifest.recorder,
        manifest.start_ns,
        &names.file_name,
    );
    manifest.byte_count = byte_count;
    manifest.sha256 = hex(&sha256);
    write_and_sync(&names.manifest_tmp, manifest.to_json()?.as_bytes())?;

    fs::rename(
        &names.manifest_tmp,
        job.completed_dir.join(&names.manifest_name),
    )
    .map_err(SinkError::Io)?;
    let path = job.completed_dir.join(&names.file_name);
    fs::rename(&names.object_tmp, &path).map_err(SinkError::Io)?;

    Ok(Published {
        segment: CompletedSegment {
            path,
            segment_seq: m.segment_seq,
            start_ns: m.start_ns,
            end_ns: m.end_ns,
            datagram_count: m.datagram_count,
            byte_count,
            sha256,
        },
        manifest,
    })
}

/// Removes what a publication that could not land would otherwise leave, and
/// keeps the one file worth keeping.
///
/// Without this, a `completed_dir` that cannot be written to leaks two
/// temporary files and one segment per rotation, none of them accounted and
/// none of them evictable — an unbounded disk out of a storage outage, which is
/// the failure the watermark exists to prevent. The segment itself is retained
/// under the object key it was going to have, which is the name the budget
/// accounts for and eviction can reach.
fn clean_up(job: &Job, names: &Names) -> String {
    let _ = fs::remove_file(&names.object_tmp);
    let _ = fs::remove_file(&names.manifest_tmp);
    // A manifest lands before its object, so one that survives a failed object
    // move is a row pointing at nothing.
    let _ = fs::remove_file(job.completed_dir.join(&names.manifest_name));
    match fs::rename(&job.source, &names.retained) {
        Ok(()) => format!("the segment is retained as {}", names.retained.display()),
        Err(e) => format!(
            "the segment could not be retained as {}: {e}",
            names.retained.display()
        ),
    }
}

/// Streams `source` into `dest`, hashing the bytes that go into `dest`.
///
/// The hash is of the object that lands, because integrity and idempotent
/// reprocessing key on `(object key, sha256)`.
fn encode(
    source: &Path,
    dest: &Path,
    compression: Compression,
) -> Result<(u64, [u8; 32]), SinkError> {
    let mut reader = BufReader::new(File::open(source).map_err(SinkError::Io)?);
    let file = File::create(dest).map_err(SinkError::Io)?;
    let sink = HashingWriter {
        inner: file,
        hasher: Sha256::new(),
        bytes: 0,
    };

    let sink = match compression {
        Compression::None => {
            let mut sink = sink;
            io::copy(&mut reader, &mut sink).map_err(SinkError::Io)?;
            sink
        }
        Compression::Zstd { level } => {
            let mut encoder = zstd::stream::Encoder::new(sink, level)
                .map_err(|e| SinkError::Compress(e.to_string()))?;
            // Four bytes per frame, against the failure mode that defeats the
            // whole point of keeping the bytes. Without the frame checksum,
            // damage to a compressed object mostly decodes to a *different*
            // buffer with no error at all: the pcapng block lengths survive,
            // replay yields a full stream and a clean end, and the payloads are
            // quietly wrong. An archive that cannot tell it has been damaged is
            // not evidence.
            encoder
                .include_checksum(true)
                .map_err(|e| SinkError::Compress(e.to_string()))?;
            io::copy(&mut reader, &mut encoder).map_err(SinkError::Io)?;
            encoder
                .finish()
                .map_err(|e| SinkError::Compress(e.to_string()))?
        }
    };

    let HashingWriter {
        mut inner,
        hasher,
        bytes,
    } = sink;
    inner.flush().map_err(SinkError::Io)?;
    inner.sync_all().map_err(SinkError::Io)?;
    Ok((bytes, hasher.finalize().into()))
}

fn write_and_sync(path: &Path, bytes: &[u8]) -> Result<(), SinkError> {
    let mut f = File::create(path).map_err(SinkError::Io)?;
    f.write_all(bytes).map_err(SinkError::Io)?;
    f.sync_all().map_err(SinkError::Io)
}

fn hex(bytes: &[u8; 32]) -> String {
    let mut s = String::with_capacity(64);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

struct HashingWriter {
    inner: File,
    hasher: Sha256,
    bytes: u64,
}

impl Write for HashingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
