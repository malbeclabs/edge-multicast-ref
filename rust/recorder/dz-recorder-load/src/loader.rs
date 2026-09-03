//! One pass: walk the directory oldest object first, derive, write, record.
//!
//! # Oldest first, and why the order is not a preference
//!
//! Two reasons, and they point the same way. Objects are evicted under the
//! recorder's staging budget, so the oldest object is the one closest to being
//! gone for ever — a loader that took the newest first would lose exactly the
//! history nobody can re-derive. And the adjacency check that settles an era
//! boundary needs the *preceding* segment, so loading in order is what makes
//! every boundary certain: out of order, the first object of every run writes an
//! uncertain anchor and every gap inside it is reported `unverifiable`.
//!
//! # Nothing here blocks the recorder
//!
//! The directory is opened read-only and the ledger lives elsewhere. A
//! destination that is down, slow or full costs loading progress and nothing
//! else: every failure is counted, the object stays unloaded, and the pass moves
//! on to the next object rather than waiting.
//!
//! # A failed object is left unloaded on purpose
//!
//! The ledger entry is written *after* the rows are in. An entry written first
//! would make a failed load look complete for ever; a retry after a failure is a
//! replace, because the tables are `ReplacingMergeTree` and the rows are a pure
//! function of `(object key, sha256)`.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use dz_recorder_archive::SegmentManifest;
use dz_recorder_rows::{
    derive_object, DeriveError, ObjectId, RowSink, RowSinkError, SegmentTrailer, Written,
};

use crate::ledger::{Entry, Ledger, LedgerError};
use crate::metrics::{ErrorKind, LoaderMetrics, SkipReason};

/// The suffix the recorder writes beside every object.
const MANIFEST_SUFFIX: &str = ".manifest.json";

/// One object and the manifest that describes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidate {
    pub object: PathBuf,
    pub manifest_path: PathBuf,
    /// From the object's name, which is a wall-clock nanosecond stamp and
    /// therefore orders segments across recorder runs — `segment_seq` restarts
    /// at 0 on every run and cannot.
    pub start_ns: u64,
}

/// What one pass did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pass {
    /// Objects whose rows landed and were recorded in the ledger this pass.
    ///
    /// Not the same as [`derived`](Self::derived), and the gap is the
    /// coalescing: a pass may derive four objects and load none because the sink
    /// is still holding them, or derive none and load six because the age bound
    /// came due.
    pub loaded: u64,
    /// Objects derived and handed to the sink this pass.
    pub derived: u64,
    pub failed: u64,
    pub skipped: u64,
    /// Objects with no ledger entry when the pass ended, which is half of lag.
    /// Includes those the sink is still holding: rows in memory are not loaded.
    pub unloaded: u64,
    /// Of those, the ones the sink has taken and not yet posted. A backlog that
    /// is all held is a sink coalescing as designed; one that is all underived
    /// is a loader behind.
    pub held: u64,
    /// How old the oldest unloaded object is, from its own receive window.
    pub oldest_unloaded_age_seconds: i64,
    pub written: Written,
}

/// Everything one pass needs.
pub struct Loader<'a, S: RowSink> {
    pub objects_dir: &'a Path,
    pub site: &'a str,
    pub recorder: &'a str,
    pub max_objects: usize,
    pub ledger: &'a mut Ledger,
    pub sink: &'a mut S,
    pub metrics: &'a LoaderMetrics,
    /// Objects derived and accepted by the sink, waiting for the insert that
    /// carries their rows to be acknowledged.
    ///
    /// **The ledger entry is written when the rows land, never when they are
    /// accepted.** A sink that coalesces across objects holds rows without
    /// having sent them, so an entry written on acceptance would mark an object
    /// loaded whose rows are still in memory — and a crash would lose them with
    /// nothing recording that it did.
    ///
    /// Carried across passes because the sink is: a quiet lane's rows may be
    /// held for the whole `insert_max_delay`, which is several passes.
    pub pending: &'a mut Vec<Pending>,
}

/// One object whose rows the sink has taken and not yet posted.
#[derive(Debug, Clone)]
pub struct Pending {
    pub id: ObjectId,
    /// What the next object's adjacency check needs. Held here as well as in
    /// the ledger, because within one pass object *n* is still pending when
    /// object *n+1* is derived — and without it every boundary after the first
    /// in a pass would be written uncertain.
    pub trailer: SegmentTrailer,
    pub written: Written,
    pub bytes_read: u64,
}

impl<S: RowSink> Loader<'_, S> {
    /// Walks the directory once.
    ///
    /// Never returns an error for an object: every failure is counted, named on
    /// the returned list of messages, and left unloaded. A pass that gave up on
    /// the first bad object would let one damaged file stop an archive from being
    /// loaded.
    pub fn run_once(&mut self, stop: &dyn Fn() -> bool) -> (Pass, Vec<String>) {
        // Read once for the whole pass, so every age bound in it is measured
        // against one instant rather than against however long the pass took.
        let now_ns = now_unix_nanos();
        let mut pass = Pass::default();
        let mut errors = Vec::new();
        let candidates = self.candidates(&mut errors);
        let mut present: HashSet<(String, String)> = HashSet::new();
        // Every object the pass saw that had no ledger entry when it was
        // scanned, with the end of its receive window. Filtered against the
        // ledger *after* the pass rather than pruned during it, because an
        // object can land at any point: the sink coalesces, so the insert that
        // makes object one durable may be the one object four triggered.
        let mut seen: Vec<(ObjectId, u64)> = Vec::new();
        // Turns off at a stop signal or at the pass bound, while the scan below
        // carries on: see the comment in the loop.
        let mut loading = true;

        for candidate in &candidates {
            let manifest = match read_manifest(&candidate.manifest_path) {
                Ok(manifest) => manifest,
                Err(message) => {
                    self.fail(ErrorKind::Manifest, message, &mut pass, &mut errors);
                    continue;
                }
            };
            present.insert((manifest.object_key.clone(), manifest.sha256.clone()));

            if manifest.site != self.site || manifest.recorder != self.recorder {
                // Counted rather than loaded: a `dz_loader_*` series labelled
                // with this host's name for another host's archive is worse than
                // a gap in the numbers, and an object from elsewhere in this
                // directory is a deployment mistake worth seeing.
                self.metrics.object_skipped(SkipReason::ForeignHost);
                pass.skipped += 1;
                continue;
            }
            if self
                .ledger
                .is_loaded(&manifest.object_key, &manifest.sha256)
            {
                self.metrics.object_skipped(SkipReason::AlreadyLoaded);
                pass.skipped += 1;
                continue;
            }
            let id = ObjectId {
                key: manifest.object_key.clone(),
                sha256: manifest.sha256.clone(),
            };
            seen.push((id.clone(), manifest.end_ns));

            // Derived already, and with the sink: an object waiting for the
            // insert that carries its rows has no ledger entry by design, so
            // the check above does not see it and the walk would derive it
            // again on every pass until the insert went — thirty times over at
            // the deployed poll interval. The re-derivation is not free and it
            // is not harmless: the second copy is derived against a *pending*
            // trailer of its own object rather than its predecessor's, so it
            // writes as unverified the era boundaries the first copy settled,
            // and the second copy is the one that lands.
            if self.pending.iter().any(|p| p.id == id) {
                self.metrics.object_skipped(SkipReason::Held);
                pass.skipped += 1;
                continue;
            }

            // Checked before the object rather than after it: a signal that
            // arrived mid-pass finishes the object it is in and stops, so a
            // restart never leaves one half-loaded. A bound on the pass does the
            // same, so a loader eight hours behind still publishes its lag every
            // poll interval instead of after an unbounded catch-up.
            //
            // The scan carries on past either, and does not `break`. That is
            // what makes the two lag numbers whole: a pass that stopped
            // scanning where it stopped loading would report the backlog it
            // happened to have reached, and a loader four hours behind would
            // publish the same small number as one that was caught up. Reading
            // the remaining manifests costs a few kilobytes each and no object
            // is opened. It is also what keeps compaction honest — the ledger
            // is rewritten against every object present, not against the
            // prefix this pass got through.
            if stop() {
                loading = false;
            }
            if self.max_objects != 0 && pass.loaded >= self.max_objects as u64 {
                loading = false;
            }
            if !loading {
                continue;
            }

            match self.load(candidate, &manifest, now_ns) {
                Ok((landed, rows)) => {
                    pass.derived += 1;
                    pass.loaded += landed;
                    // Rows accepted, which is what this pass derived and handed
                    // over. Whether they are in the store yet is `loaded`.
                    pass.written.add(rows);
                }
                Err(failure) => {
                    let (kind, message) = failure;
                    // Every object the sink was holding failed with this one,
                    // so none of them is loaded and all of them are re-derived
                    // next pass.
                    self.pending.clear();
                    self.fail(kind, message, &mut pass, &mut errors);
                }
            }
        }

        // Once a pass, including a pass that found no new object: a lane quiet
        // enough to produce nothing would otherwise hold its last rows until
        // something else arrived, which is the opposite of what the age bound is
        // for.
        match self.sink.post_if_due(now_ns) {
            Ok(landed) => match self.record_landed(&landed) {
                Ok(recorded) => pass.loaded += recorded,
                Err((kind, message)) => self.fail(kind, message, &mut pass, &mut errors),
            },
            Err(e) => {
                let kind = kind_of_sink(&e);
                self.pending.clear();
                self.fail(kind, e.to_string(), &mut pass, &mut errors);
            }
        }

        // Every object with no ledger entry, which includes the ones the sink is
        // holding: rows in memory are not loaded, and a lag metric that counted
        // them as loaded would report a loader caught up while its last insert
        // sat unsent.
        pass.held = self.pending.len() as u64;
        // What is still unloaded now the pass is over: in the directory, and
        // with no ledger entry. An object the sink is holding is in here, and
        // deliberately — rows in memory are not loaded, and a lag metric that
        // counted them as loaded would report a loader caught up while its last
        // insert sat unsent.
        let unloaded: Vec<u64> = seen
            .iter()
            .filter(|(id, _)| !self.ledger.is_loaded(&id.key, &id.sha256))
            .map(|(_, end_ns)| *end_ns)
            .collect();
        pass.unloaded = unloaded.len() as u64;
        pass.oldest_unloaded_age_seconds = unloaded
            .iter()
            .min()
            .map_or(0, |end_ns| age_seconds(*end_ns, now_unix_seconds()));

        if let Err(e) = self.ledger.compact(&present) {
            // Not a failed load: the ledger is still correct, only longer than
            // it needs to be.
            self.metrics.error(ErrorKind::Ledger, now_unix_seconds());
            errors.push(e.to_string());
        }
        self.metrics.pass_finished(
            pass.unloaded as i64,
            pass.held as i64,
            pass.oldest_unloaded_age_seconds,
            self.ledger.entries() as i64,
            now_unix_seconds(),
        );
        (pass, errors)
    }

    /// Derives one object, hands it to the sink, and records whatever that
    /// caused to land.
    ///
    /// The order is the property: nothing is recorded until the insert carrying
    /// its rows has been acknowledged. A sink may take this object's rows and
    /// post nothing, in which case the object joins `pending` and is recorded on
    /// a later pass — or take them and post several objects' worth at once, in
    /// which case all of those are recorded here.
    fn load(
        &mut self,
        candidate: &Candidate,
        manifest: &SegmentManifest,
        now_ns: u64,
    ) -> Result<(u64, Written), (ErrorKind, String)> {
        let bytes_read = std::fs::metadata(&candidate.object)
            .map(|m| m.len())
            .unwrap_or(manifest.byte_count);

        // The latest trailer, pending or recorded. Pending first: within one
        // pass the previous object is still pending when this one is derived,
        // and consulting only the ledger would write an uncertain boundary for
        // every object after the first.
        let trailer = self.trailer();
        let derived = derive_object(&candidate.object, manifest, trailer.as_ref())
            .map_err(|e| (kind_of(&e), e.to_string()))?;

        let accepted = self
            .sink
            .write_batch(derived.rows, now_ns)
            .map_err(|e| (kind_of_sink(&e), e.to_string()))?;
        let rows = accepted.accepted;

        self.pending.push(Pending {
            id: ObjectId {
                key: manifest.object_key.clone(),
                sha256: manifest.sha256.clone(),
            },
            trailer: derived.trailer,
            written: rows,
            bytes_read,
        });
        let landed = self.record_landed(&accepted.landed)?;
        Ok((landed, rows))
    }

    /// The trailer the next object's adjacency check should consult.
    ///
    /// The highest `segment_seq` known, pending or recorded. Pending wins on a
    /// tie of nothing: a pending trailer is evidence about an object on disk,
    /// which is what the check is about — whether its rows have landed yet is a
    /// different question.
    fn trailer(&self) -> Option<SegmentTrailer> {
        let pending = self
            .pending
            .iter()
            .map(|p| &p.trailer)
            .max_by_key(|t| t.segment_seq);
        match (pending, self.ledger.trailer()) {
            (Some(p), Some(l)) if l.segment_seq > p.segment_seq => Some(l.clone()),
            (Some(p), _) => Some(p.clone()),
            (None, l) => l.cloned(),
        }
    }

    /// Writes a ledger entry for every object whose rows have landed.
    ///
    /// # Errors
    ///
    /// The ledger could not be written, which the caller treats as a failed
    /// load: the rows are in the store and the loader has forgotten it did that,
    /// so the re-load that follows is a replace.
    fn record_landed(&mut self, landed: &[ObjectId]) -> Result<u64, (ErrorKind, String)> {
        let mut recorded = 0u64;
        for id in landed {
            let Some(index) = self.pending.iter().position(|p| &p.id == id) else {
                // The sink named an object this loader is not holding. It cannot
                // happen — a sink only ever lands what it was given — and if it
                // did, recording an entry for an object with no trailer would
                // put a boundary check on evidence nobody derived.
                continue;
            };
            let done = self.pending.remove(index);
            self.ledger
                .record(Entry {
                    object_key: done.id.key.clone(),
                    object_sha256: done.id.sha256.clone(),
                    loaded_at_ns: now_unix_nanos(),
                    trailer: done.trailer,
                })
                .map_err(|e: LedgerError| (ErrorKind::Ledger, e.to_string()))?;
            self.metrics.object_loaded(&done.written, done.bytes_read);
            recorded += 1;
        }
        Ok(recorded)
    }

    fn fail(&self, kind: ErrorKind, message: String, pass: &mut Pass, errors: &mut Vec<String>) {
        if kind == ErrorKind::Sink {
            self.metrics.batch_failed();
        }
        self.metrics.error(kind, now_unix_seconds());
        pass.failed += 1;
        errors.push(message);
    }

    /// Every object in the directory with a manifest beside it, oldest first.
    fn candidates(&self, errors: &mut Vec<String>) -> Vec<Candidate> {
        let entries = match std::fs::read_dir(self.objects_dir) {
            Ok(entries) => entries,
            Err(e) => {
                self.metrics.error(ErrorKind::Io, now_unix_seconds());
                errors.push(format!("{}: {e}", self.objects_dir.display()));
                return Vec::new();
            }
        };

        let mut manifests: Vec<PathBuf> = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.ends_with(MANIFEST_SUFFIX) {
                manifests.push(entry.path());
            }
        }

        let mut out = Vec::with_capacity(manifests.len());
        for manifest_path in manifests {
            let stem = manifest_path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let stem = stem.trim_end_matches(MANIFEST_SUFFIX).to_owned();
            // The compressor names an object `{start}-{end}-{seq}.pcapng.zst`
            // and its manifest `{start}-{end}-{seq}.manifest.json`, so the
            // object is the same stem with either archive suffix.
            let object = ["pcapng.zst", "pcapng"]
                .into_iter()
                .map(|suffix| self.objects_dir.join(format!("{stem}.{suffix}")))
                .find(|p| p.is_file());
            let Some(object) = object else {
                // A manifest lands before its object, so a pass that ran during
                // a publication sees this and it resolves itself on the next
                // one. Counted, never an error.
                self.metrics.object_skipped(SkipReason::Unpaired);
                continue;
            };
            out.push(Candidate {
                object,
                manifest_path,
                start_ns: stem
                    .split('-')
                    .next()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
            });
        }
        // Oldest first: the oldest object is the one closest to eviction, and
        // in-order loading is what makes an era boundary certain.
        out.sort_by_key(|c| (c.start_ns, c.object.clone()));
        out
    }
}

fn read_manifest(path: &Path) -> Result<SegmentManifest, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// Which counter a derivation failure lands on.
///
/// Split by cause rather than counted together, because the responses differ:
/// a digest mismatch is a damaged or replaced object, a scope disagreement is a
/// configuration or a manifest that does not describe these bytes, and a short
/// replay is a torn file. An operator seeing one number cannot tell which.
const fn kind_of(error: &DeriveError) -> ErrorKind {
    match error {
        DeriveError::DigestMismatch { .. } => ErrorKind::Digest,
        DeriveError::Io { .. } => ErrorKind::Io,
        DeriveError::Source { .. } | DeriveError::Incomplete { .. } => ErrorKind::Replay,
        DeriveError::ScopeDisagreement { .. } | DeriveError::ScopeUnstated { .. } => {
            ErrorKind::Scope
        }
    }
}

const fn kind_of_sink(error: &RowSinkError) -> ErrorKind {
    match error {
        RowSinkError::Io { .. } => ErrorKind::Io,
        RowSinkError::Encode { .. } | RowSinkError::Rejected { .. } => ErrorKind::Sink,
    }
}

/// How far behind the wall clock an object's receive window ends.
///
/// From the object's own window rather than from a file's mtime, because an
/// mtime is a property of the copy and this number is compared against the
/// recorder's eviction window, which is about the traffic.
fn age_seconds(end_ns: u64, now_unix_seconds: i64) -> i64 {
    let end = (end_ns / 1_000_000_000) as i64;
    // Saturating at zero: an object whose window ends in the future is a clock
    // that disagrees, and a negative lag would read as a loader ahead of the
    // recorder.
    (now_unix_seconds - end).max(0)
}

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(crate) fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_object_whose_window_ends_in_the_future_is_not_negative_lag() {
        // A clock that disagrees, and a negative lag would read as a loader
        // running ahead of the recorder it reads from.
        assert_eq!(age_seconds(2_000_000_000_000_000_000, 1_000_000_000), 0);
        assert_eq!(age_seconds(1_000_000_000_000_000_000, 1_000_000_060), 60);
    }
}

#[cfg(test)]
mod pass_tests {
    use std::time::Duration;

    use dz_edge_core::PortRole;
    use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
    use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
    use dz_recorder_archive::Compression;
    use dz_recorder_core::{CaptureDropScope, RecorderIdentity};
    use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};
    use dz_recorder_rows::{FileSink, Grain};
    use tempfile::TempDir;

    use super::*;

    pub(super) const SITE: &str = "site-1";
    pub(super) const RECORDER: &str = "recorder-1";

    /// A recorder host's completed directory, with `segments` objects in it.
    pub(super) struct Archive {
        _dir: TempDir,
        pub(super) completed: PathBuf,
        pub(super) rows: PathBuf,
        pub(super) ledger: PathBuf,
    }

    pub(super) fn archive(segments: usize, per_segment: usize) -> Archive {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let completed = dir.path().join("completed");
        let cfg = ArchiveWriterConfig {
            staging_dir: dir.path().join("staging"),
            completed_dir: completed.clone(),
            rotate_bytes: 1 << 30,
            rotate_interval: Duration::from_secs(3600),
            staging_max: 1 << 40,
            compression: Compression::Zstd { level: 1 },
            identity: RecorderIdentity {
                site: SITE.to_owned(),
                recorder: RECORDER.to_owned(),
                env: "test".to_owned(),
                build_version: "0.1.0".to_owned(),
                build_commit: "0000000".to_owned(),
                config_hash: "a".repeat(64),
            },
            feed: "top-of-book".to_owned(),
            roles_joined: vec![RoleJoin::on(
                PortRole::Mktdata,
                GROUP,
                port_for(PortRole::Mktdata),
            )],
            link_headers: LinkHeaders::Synthesised,
            capture_drop_scope: CaptureDropScope::PortRole,
        };
        let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
        for segment in 0..segments {
            SyntheticPublisher::clean(per_segment)
                .publish_into(&mut writer)
                .expect("the write path never fails the caller");
            writer
                .rotate_at(1_000_000_000 * (segment as u64 + 1))
                .expect("rotation")
                .expect("a segment that held datagrams produces an object");
            writer
                .wait_completed()
                .expect("the compressor publishes exactly one object")
                .expect("publication");
        }
        Archive {
            completed,
            rows: dir.path().join("rows"),
            ledger: dir.path().join("ledger.jsonl"),
            _dir: dir,
        }
    }

    fn never() -> impl Fn() -> bool {
        || false
    }

    fn pass(
        archive: &Archive,
        ledger: &mut Ledger,
        metrics: &LoaderMetrics,
    ) -> (Pass, Vec<String>) {
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = never();
        let result = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger,
            sink: &mut sink,
            metrics,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);
        sink.flush(now_unix_nanos()).expect("flush");
        result
    }

    fn rows_in(archive: &Archive, grain: Grain) -> usize {
        let path = FileSink::path_in(&archive.rows, grain);
        std::fs::read_to_string(path)
            .map(|t| t.lines().filter(|l| !l.is_empty()).count())
            .unwrap_or(0)
    }

    #[test]
    fn a_pass_loads_every_object_and_records_each_one() {
        let archive = archive(3, 50);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");

        let (pass, errors) = pass(&archive, &mut ledger, &metrics);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.loaded, 3);
        assert_eq!(pass.failed, 0);
        assert_eq!(pass.unloaded, 0, "nothing is waiting");
        assert_eq!(pass.oldest_unloaded_age_seconds, 0);
        assert_eq!(rows_in(&archive, Grain::Datagram), 150);
        assert_eq!(ledger.entries(), 3);
    }

    /// The ledger is what makes a second pass a no-op, and a restart resume.
    #[test]
    fn a_second_pass_and_a_restart_both_load_nothing_again() {
        let archive = archive(3, 50);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        pass(&archive, &mut ledger, &metrics);

        let (again, _) = pass(&archive, &mut ledger, &metrics);
        assert_eq!(again.loaded, 0);
        assert_eq!(again.skipped, 3);
        assert_eq!(
            rows_in(&archive, Grain::Datagram),
            150,
            "nothing re-written"
        );

        // A restart: a fresh ledger over the same file, which is the case a
        // supervisor produces every time it restarts this process.
        let mut restarted = Ledger::open(&archive.ledger).expect("the ledger reads back");
        assert_eq!(restarted.entries(), 3);
        let (after_restart, _) = pass(&archive, &mut restarted, &metrics);
        assert_eq!(
            after_restart.loaded, 0,
            "a restart resumes rather than reloads"
        );
        assert_eq!(after_restart.skipped, 3);
    }

    /// In-order loading is what makes an era boundary certain.
    ///
    /// The first object has no predecessor to consult, so its boundary is
    /// written uncertain; every object after it is settled by the trailer the
    /// previous one left. That is the difference between a gap that can be
    /// escalated past `unverifiable` and one that cannot.
    #[test]
    fn loading_in_order_settles_every_era_boundary_after_the_first() {
        let archive = archive(3, 50);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        pass(&archive, &mut ledger, &metrics);

        let text = std::fs::read_to_string(FileSink::path_in(&archive.rows, Grain::Era))
            .expect("era rows were written");
        let eras: Vec<serde_json::Value> = text
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
            .collect();
        assert_eq!(eras.len(), 3, "one boundary per object");
        assert_eq!(eras[0]["anchor_certain"], 0, "nothing preceded the first");
        assert_eq!(eras[1]["anchor_certain"], 1);
        assert_eq!(eras[2]["anchor_certain"], 1);
        // The stream carries the same `Reset Count` throughout, so the settled
        // boundaries are continuations: recorded, and not ranked as openings.
        assert_eq!(eras[1]["continuation"], 1);
        assert_eq!(eras[2]["continuation"], 1);
        // And the trailer that settled them is in the ledger, so a restart
        // settles the next one too rather than starting uncertain again.
        assert_eq!(
            ledger.trailer().expect("a trailer").segment_seq,
            2,
            "the highest segment, not the last line written"
        );
    }

    /// An object from another host is counted and not loaded.
    ///
    /// A `dz_loader_*` series labelled with this host's name for another host's
    /// archive is worse than a gap in the numbers.
    #[test]
    fn an_object_from_another_host_is_skipped_and_counted() {
        let archive = archive(1, 20);
        let metrics = LoaderMetrics::new("some-other-site", RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = never();
        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: "some-other-site",
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);

        assert!(errors.is_empty(), "a foreign object is not a failure");
        assert_eq!(pass.loaded, 0);
        assert_eq!(pass.skipped, 1);
        assert!(metrics
            .render()
            .contains("reason=\"foreign_host\",recorder"));
    }

    /// A manifest with no object beside it resolves itself on the next pass.
    ///
    /// The recorder writes the manifest first, so this is what a pass that ran
    /// during a publication sees. It is counted and never an error.
    #[test]
    fn a_manifest_with_no_object_is_counted_and_not_an_error() {
        let archive = archive(1, 20);
        std::fs::write(
            archive.completed.join("999-1000-9.manifest.json"),
            "{\"not\": \"read, because there is no object\"}",
        )
        .expect("the directory is writable");

        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let (pass, errors) = pass(&archive, &mut ledger, &metrics);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.loaded, 1);
        assert!(metrics.render().contains("reason=\"unpaired\",recorder"));
    }

    /// One damaged object costs itself and nothing else.
    ///
    /// A pass that gave up on the first bad object would let one damaged file
    /// stop an archive from being loaded, and the object it stopped at is the
    /// one closest to eviction.
    #[test]
    fn a_damaged_object_is_left_unloaded_and_the_pass_carries_on() {
        let archive = archive(3, 50);
        // The middle object, appended to: it still replays whole, and it is no
        // longer the object its manifest describes.
        let objects = sorted_objects(&archive);
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&objects[1])
            .expect("the object is writable");
        std::io::Write::write_all(&mut file, b"not the described bytes").expect("append");
        drop(file);

        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let (pass, errors) = pass(&archive, &mut ledger, &metrics);

        assert_eq!(pass.loaded, 2, "the other two loaded");
        assert_eq!(pass.failed, 1);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("hashes to"), "{}", errors[0]);
        assert_eq!(pass.unloaded, 1, "and it is still waiting");
        assert!(pass.oldest_unloaded_age_seconds > 0, "which is lag");
        assert!(metrics.render().contains("kind=\"digest\",recorder"));
        assert_eq!(ledger.entries(), 2, "no entry for the object that failed");
    }

    /// A bound on the pass, so a loader eight hours behind still publishes its
    /// lag every poll interval instead of after an unbounded catch-up.
    #[test]
    fn a_pass_stops_at_its_bound_and_reports_what_is_still_waiting() {
        let archive = archive(4, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = never();
        let (pass, _) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 2,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);

        assert_eq!(pass.loaded, 2);
        assert_eq!(
            pass.unloaded, 2,
            "and the pass says so rather than looking done"
        );
        assert!(pass.oldest_unloaded_age_seconds > 0);
    }

    /// A signal finishes the object it arrived in and then stops.
    #[test]
    fn a_stop_signal_ends_the_pass_between_objects() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = || true;
        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);

        assert!(errors.is_empty());
        assert_eq!(pass.loaded, 0, "it stopped before the first object");
        assert_eq!(pass.unloaded, 3);
    }

    /// A directory that is not there is one counted error and an empty pass,
    /// not a crash: the recorder may not have created it yet.
    #[test]
    fn a_missing_objects_directory_is_one_counted_error() {
        let archive = archive(1, 10);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = never();
        let missing = archive.completed.join("not-here");
        let (pass, errors) = Loader {
            objects_dir: &missing,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);

        assert_eq!(pass.loaded, 0);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("not-here"), "{}", errors[0]);
        assert!(metrics.render().contains("kind=\"io\",recorder"));
    }

    /// Oldest first, because the oldest object is the one closest to eviction.
    #[test]
    fn the_walk_takes_the_oldest_object_first() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let loader = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut Vec::new(),
        };
        let mut errors = Vec::new();
        let candidates = loader.candidates(&mut errors);
        assert!(errors.is_empty());
        assert_eq!(candidates.len(), 3);
        assert!(
            candidates
                .windows(2)
                .all(|w| w[0].start_ns <= w[1].start_ns),
            "{candidates:?}"
        );
    }

    fn sorted_objects(archive: &Archive) -> Vec<PathBuf> {
        let mut objects: Vec<PathBuf> = std::fs::read_dir(&archive.completed)
            .expect("the completed directory exists")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .is_some_and(|n| n.to_string_lossy().ends_with(".pcapng.zst"))
            })
            .collect();
        objects.sort();
        objects
    }
}

#[cfg(test)]
mod deferred_ledger_tests {
    use dz_recorder_rows::{Accepted, RowBatch, RowSinkError, Written};

    use super::pass_tests::*;
    use super::*;

    /// A sink that holds everything until it is told to post.
    ///
    /// The column store's sink coalesces across objects, so `write_batch`
    /// returning `Ok` does not mean the rows are in the store. `FileSink` lands
    /// immediately and cannot exercise that, which is why this exists: what is
    /// under test is the *loader's* discipline, not a sink's.
    #[derive(Debug, Default)]
    struct HoldingSink {
        held: Vec<ObjectId>,
        posts: usize,
        /// When set, the next post fails and holds nothing — the shape a
        /// refused insert takes.
        refuse: bool,
        /// Every batch handed over, in order: the object it came from and the
        /// `anchor_certain` of its era rows. `held` cannot show a re-derivation
        /// — the object is already in it — and this can.
        taken: Vec<(String, Vec<u8>)>,
    }

    impl HoldingSink {
        fn post(&mut self) -> Result<Vec<ObjectId>, RowSinkError> {
            if self.refuse {
                let held = std::mem::take(&mut self.held);
                return Err(RowSinkError::Rejected {
                    object_key: format!("{} objects", held.len()),
                    attempts: 1,
                    last: "the destination refused it".to_owned(),
                });
            }
            self.posts += 1;
            Ok(std::mem::take(&mut self.held))
        }
    }

    impl RowSink for HoldingSink {
        fn write_batch(&mut self, rows: RowBatch, _now_ns: u64) -> Result<Accepted, RowSinkError> {
            let id = ObjectId::of(&rows);
            self.taken.push((
                rows.object_key.clone(),
                rows.era.iter().map(|e| e.anchor_certain).collect(),
            ));
            if !self.held.contains(&id) {
                self.held.push(id);
            }
            Ok(Accepted {
                accepted: Written::of(&rows, 0),
                landed: Vec::new(),
                bytes_posted: 0,
            })
        }

        /// Never due on its own: a test says when.
        fn post_if_due(&mut self, _now_ns: u64) -> Result<Vec<ObjectId>, RowSinkError> {
            Ok(Vec::new())
        }

        fn flush(&mut self, _now_ns: u64) -> Result<Vec<ObjectId>, RowSinkError> {
            self.post()
        }
    }

    /// **An object whose rows are still held is not loaded.**
    ///
    /// The ledger's whole meaning is "the rows for this object are in the
    /// store". An entry written when the sink *accepted* the batch would mark an
    /// object loaded whose rows are in memory, and a crash would lose them with
    /// nothing recording that it did — the object would never be derived again.
    #[test]
    fn an_object_the_sink_is_still_holding_gets_no_ledger_entry() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink::default();
        let mut pending = Vec::new();

        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut pending,
        }
        .run_once(&|| false);

        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.derived, 3, "all three were derived and handed over");
        assert_eq!(pass.loaded, 0, "and none of them is loaded");
        assert_eq!(ledger.entries(), 0, "so the ledger is empty");
        assert_eq!(pass.held, 3);
        assert_eq!(
            pass.unloaded, 3,
            "rows in memory are not loaded, and lag has to say so"
        );
        assert!(pass.oldest_unloaded_age_seconds > 0);
        assert_eq!(pending.len(), 3, "the loader is holding their trailers");
    }

    /// And they are recorded on the pass the insert lands, all of them.
    #[test]
    fn the_ledger_records_every_object_the_insert_carried() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink::default();
        let mut pending = Vec::new();

        let pass = |ledger: &mut Ledger, sink: &mut HoldingSink, pending: &mut Vec<Pending>| {
            Loader {
                objects_dir: &archive.completed,
                site: SITE,
                recorder: RECORDER,
                max_objects: 0,
                ledger,
                sink,
                metrics: &metrics,
                pending,
            }
            .run_once(&|| false)
            .0
        };

        pass(&mut ledger, &mut sink, &mut pending);
        assert_eq!(ledger.entries(), 0);

        // The insert goes. Nothing new is in the directory, so this is the case
        // the age bound exists for: a pass that derives nothing and lands
        // everything.
        let landed = sink.flush(0).expect("posted");
        assert_eq!(landed.len(), 3);
        for id in &landed {
            let done = pending
                .iter()
                .position(|p| &p.id == id)
                .expect("the loader was holding it");
            let done = pending.remove(done);
            ledger
                .record(Entry {
                    object_key: done.id.key,
                    object_sha256: done.id.sha256,
                    loaded_at_ns: 0,
                    trailer: done.trailer,
                })
                .expect("recordable");
        }
        assert_eq!(ledger.entries(), 3);
        assert!(pending.is_empty());

        // And now the next pass has nothing to do, which is what says the
        // entries are real.
        let second = pass(&mut ledger, &mut sink, &mut pending);
        assert_eq!(second.derived, 0);
        assert_eq!(second.skipped, 3);
        assert_eq!(second.unloaded, 0, "nothing is waiting any more");
    }

    /// A refused insert leaves every object it carried unloaded, and the loader
    /// stops holding their trailers.
    ///
    /// All of them, because they were one insert: the tables are
    /// `ReplacingMergeTree` and the rows are a pure function of
    /// `(object key, sha256)`, so re-deriving them all is a replace. Keeping the
    /// trailers would be worse than dropping them — the objects are still on
    /// disk, so the next pass derives them again from the evidence rather than
    /// from a memory of it.
    #[test]
    fn a_refused_insert_leaves_every_object_it_carried_unloaded() {
        let archive = archive(2, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink {
            refuse: true,
            ..HoldingSink::default()
        };
        let mut pending = Vec::new();

        // The first pass accepts both and posts nothing, so nothing fails yet.
        let (first, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut pending,
        }
        .run_once(&|| false);
        assert!(errors.is_empty());
        assert_eq!(first.held, 2);

        // The insert is refused.
        let error = sink.flush(0).expect_err("the destination refused it");
        assert!(matches!(error, RowSinkError::Rejected { .. }), "{error}");

        // Nothing was recorded, and a later pass derives both again.
        assert_eq!(ledger.entries(), 0);
        pending.clear();
        let mut sink = HoldingSink::default();
        let (second, _) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut pending,
        }
        .run_once(&|| false);
        assert_eq!(second.derived, 2, "both are derived again");
        assert_eq!(second.skipped, 0, "neither was recorded as loaded");
    }

    /// **An object the sink is holding is not derived again.**
    ///
    /// It has no ledger entry by design — the entry is written when the rows
    /// land — so the walk's own skip cannot see it, and without this the
    /// deployed configuration re-derives every held object about thirty times
    /// (a 30s poll against a 900s `insert_max_delay`). The cost is not the
    /// work. The second copy consults a pending trailer that now includes the
    /// object's *own* segment, so the adjacency check fails and it writes as
    /// unverified the era boundaries the first copy settled — and the copy the
    /// insert carries is the last one written.
    #[test]
    fn an_object_the_sink_is_holding_is_not_derived_again_next_pass() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink::default();
        let mut pending = Vec::new();

        let mut pass = |ledger: &mut Ledger, sink: &mut HoldingSink, pending: &mut Vec<Pending>| {
            Loader {
                objects_dir: &archive.completed,
                site: SITE,
                recorder: RECORDER,
                max_objects: 0,
                ledger,
                sink,
                metrics: &metrics,
                pending,
            }
            .run_once(&|| false)
            .0
        };

        let first = pass(&mut ledger, &mut sink, &mut pending);
        assert_eq!(first.derived, 3);
        let second = pass(&mut ledger, &mut sink, &mut pending);
        let third = pass(&mut ledger, &mut sink, &mut pending);

        assert_eq!(second.derived, 0, "all three are already with the sink");
        assert_eq!(third.derived, 0);
        assert_eq!(second.skipped, 3);
        assert!(metrics.render().contains("reason=\"held\",recorder"));
        assert_eq!(
            sink.taken.len(),
            3,
            "one batch per object over three passes, not three: {:?}",
            sink.taken
        );
        assert_eq!(
            pending.len(),
            3,
            "and the held list is the same three, not nine"
        );
        // The boundaries the first pass settled are still settled, because
        // nothing derived them a second time.
        let certain: Vec<u8> = sink
            .taken
            .iter()
            .map(|(_, eras)| *eras.first().expect("one boundary per object"))
            .collect();
        assert_eq!(certain, vec![0, 1, 1], "only the first is unsettled");
        // Still nothing loaded: what changed is the deriving, not the ledger.
        assert_eq!(ledger.entries(), 0);
        assert_eq!(third.unloaded, 3);
    }

    /// In-order certainty survives the coalescing.
    ///
    /// Within one pass object *n* is still pending when object *n+1* is
    /// derived, so a loader that consulted only the ledger for the adjacency
    /// check would write an uncertain era boundary for every object after the
    /// first — and every gap inside those objects would read `unverifiable`.
    /// The pending trailer is what prevents that.
    #[test]
    fn a_pending_trailer_still_settles_the_next_objects_era_boundary() {
        let archive = archive(3, 40);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        // A file sink beside the holding one, so the era rows are readable while
        // the ledger stays empty: what is asserted is the *boundary*, and it is
        // decided at derive time.
        let mut sink =
            dz_recorder_rows::FileSink::create(&archive.rows).expect("the directory is writable");
        let mut pending = Vec::new();
        Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            pending: &mut pending,
        }
        .run_once(&|| false);
        RowSink::flush(&mut sink, 0).expect("flush");

        let text = std::fs::read_to_string(dz_recorder_rows::FileSink::path_in(
            &archive.rows,
            dz_recorder_rows::Grain::Era,
        ))
        .expect("era rows were written");
        let certain: Vec<i64> = text
            .lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str::<serde_json::Value>(l).expect("an object")["anchor_certain"]
                    .as_i64()
                    .expect("a flag")
            })
            .collect();
        assert_eq!(certain, vec![0, 1, 1], "only the first is unsettled");
    }
}
