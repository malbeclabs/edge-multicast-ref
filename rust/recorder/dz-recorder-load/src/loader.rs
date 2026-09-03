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
use dz_recorder_rows::{derive_object, DeriveError, RowSink, RowSinkError, Written};

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
    pub loaded: u64,
    pub failed: u64,
    pub skipped: u64,
    /// Objects with no ledger entry when the pass ended, which is half of lag.
    pub unloaded: u64,
    /// How old the oldest of those is, from its own receive window.
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
}

impl<S: RowSink> Loader<'_, S> {
    /// Walks the directory once.
    ///
    /// Never returns an error for an object: every failure is counted, named on
    /// the returned list of messages, and left unloaded. A pass that gave up on
    /// the first bad object would let one damaged file stop an archive from being
    /// loaded.
    pub fn run_once(&mut self, stop: &dyn Fn() -> bool) -> (Pass, Vec<String>) {
        let mut pass = Pass::default();
        let mut errors = Vec::new();
        let candidates = self.candidates(&mut errors);
        let mut present: HashSet<(String, String)> = HashSet::new();
        let mut unloaded: Vec<u64> = Vec::new();
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
            unloaded.push(manifest.end_ns);

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

            match self.load(candidate, &manifest) {
                Ok(written) => {
                    pass.loaded += 1;
                    pass.written.add(written);
                    // This object's own entry, which the push above added a
                    // moment ago: it is the last one, and the scan has not
                    // moved on yet.
                    unloaded.pop();
                }
                Err(failure) => {
                    let (kind, message) = failure;
                    self.fail(kind, message, &mut pass, &mut errors);
                }
            }
        }

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
            pass.oldest_unloaded_age_seconds,
            self.ledger.entries() as i64,
            now_unix_seconds(),
        );
        (pass, errors)
    }

    /// Derives one object, writes it, and records it — in that order.
    fn load(
        &mut self,
        candidate: &Candidate,
        manifest: &SegmentManifest,
    ) -> Result<Written, (ErrorKind, String)> {
        let bytes_read = std::fs::metadata(&candidate.object)
            .map(|m| m.len())
            .unwrap_or(manifest.byte_count);

        let derived = derive_object(&candidate.object, manifest, self.ledger.trailer())
            .map_err(|e| (kind_of(&e), e.to_string()))?;

        let written = self
            .sink
            .write_batch(derived.rows)
            .map_err(|e| (kind_of_sink(&e), e.to_string()))?;

        // After the rows are in, and never before.
        self.ledger
            .record(Entry {
                object_key: manifest.object_key.clone(),
                object_sha256: manifest.sha256.clone(),
                loaded_at_ns: now_unix_nanos(),
                trailer: derived.trailer,
            })
            .map_err(|e: LedgerError| (ErrorKind::Ledger, e.to_string()))?;

        self.metrics.object_loaded(&written, bytes_read);
        Ok(written)
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

fn now_unix_nanos() -> u64 {
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

    const SITE: &str = "site-1";
    const RECORDER: &str = "recorder-1";

    /// A recorder host's completed directory, with `segments` objects in it.
    struct Archive {
        _dir: TempDir,
        completed: PathBuf,
        rows: PathBuf,
        ledger: PathBuf,
    }

    fn archive(segments: usize, per_segment: usize) -> Archive {
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
        }
        .run_once(&stop);
        sink.flush().expect("flush");
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
