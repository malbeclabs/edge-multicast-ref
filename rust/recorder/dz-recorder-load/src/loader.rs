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

use crate::config::MarketDataFeed;
use crate::ledger::{Entry, Ledger};
use crate::market_data::{derivation_for, derive_market_data, extend_batch, refusals};
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
    /// The same two numbers over the objects of feeds whose market data
    /// derivation is on — a subset, and never the same number.
    ///
    /// Separate because the two tiers are behind on two different sets: every
    /// object feeds the transport tables, and derivation is per feed and off by
    /// default. On a host where no feed derives these stay at zero however far
    /// behind the load is, which is the honest statement that no market data is
    /// at risk.
    pub market_data_unloaded: u64,
    pub market_data_oldest_unloaded_age_seconds: i64,
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
    /// The feeds whose objects also become market data rows.
    ///
    /// Empty is the default and empty is off: an object whose manifest names no
    /// feed in here is derived into the transport tables exactly as before, and
    /// `event`, `instrument` and `book_top` stay empty for it.
    pub market_data: &'a [MarketDataFeed],
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

/// Why one object's load failed, and what became of the rows the sink was
/// holding when it did.
///
/// The second half is the part a caller cannot infer from the first: a digest
/// mismatch and a refused insert are both an [`ErrorKind::Io`] away from each
/// other in the worst case, and the answers are opposite. A failure at the sink
/// takes every object it was holding with it — [`post`] empties the buffer
/// before it sends, so a refusal leaves nothing held — and a failure anywhere
/// else leaves those objects exactly where they were, waiting for an insert
/// that is still going to land.
///
/// [`post`]: dz_recorder_rows::RowSink::write_batch
struct Failed {
    kind: ErrorKind,
    message: String,
    sink_lost_its_rows: bool,
}

impl Failed {
    /// A failure before the sink was touched, or after it had done its work:
    /// what it holds is untouched, and forgetting it would leave the insert
    /// that carries it to land with no ledger entry behind it.
    const fn with_rows_intact(kind: ErrorKind, message: String) -> Self {
        Self {
            kind,
            message,
            sink_lost_its_rows: false,
        }
    }

    /// The sink could not send the batch, and everything it was holding went
    /// with it.
    const fn with_rows_lost(kind: ErrorKind, message: String) -> Self {
        Self {
            kind,
            message,
            sink_lost_its_rows: true,
        }
    }
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
        // The flag is whether this object's feed derives market data, read from
        // the manifest that named the feed. Kept per object rather than looked
        // up again afterwards, because the answer is a property of the object
        // and the configuration is a slice somebody may one day reload.
        let mut seen: Vec<(ObjectId, u64, bool)> = Vec::new();
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
            seen.push((
                id.clone(),
                manifest.end_ns,
                derivation_for(self.market_data, &manifest.feed).is_some(),
            ));

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
            // Against what the pass *derived*, which is the work it did. Loading
            // is not that number any more: a sink that coalesces lands nothing
            // until an insert goes, so a bound on `loaded` stays at 0 through a
            // whole catch-up and bounds nothing — which is the unbounded pass
            // the key exists to prevent.
            if self.max_objects != 0 && pass.derived >= self.max_objects as u64 {
                loading = false;
            }
            if !loading {
                continue;
            }

            match self.load(candidate, &manifest, now_ns) {
                Ok((recorded, rows)) => {
                    pass.derived += 1;
                    pass.loaded += recorded.recorded;
                    // Rows accepted, which is what this pass derived and handed
                    // over. Whether they are in the store yet is `loaded`.
                    pass.written.add(rows);
                    for message in recorded.failures {
                        self.fail(ErrorKind::Ledger, message, &mut pass, &mut errors);
                    }
                }
                Err(failure) => {
                    if failure.sink_lost_its_rows {
                        // Every object the sink was holding failed with this
                        // one, so none of them is loaded and all of them are
                        // re-derived next pass. Only then: a derivation that
                        // failed never reached the sink, and forgetting the
                        // objects it is still holding would leave their insert
                        // to land with no ledger entry written for any of them.
                        self.pending.clear();
                    }
                    self.fail(failure.kind, failure.message, &mut pass, &mut errors);
                }
            }
        }

        // Once a pass, including a pass that found no new object: a lane quiet
        // enough to produce nothing would otherwise hold its last rows until
        // something else arrived, which is the opposite of what the age bound is
        // for.
        match self.sink.post_if_due(now_ns) {
            Ok(landed) => {
                self.metrics.bytes_posted(landed.bytes_posted);
                let recorded = self.record_landed(&landed.objects);
                pass.loaded += recorded.recorded;
                for message in recorded.failures {
                    self.fail(ErrorKind::Ledger, message, &mut pass, &mut errors);
                }
            }
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
        let now_seconds = now_unix_seconds();
        let unloaded: Vec<(u64, bool)> = seen
            .iter()
            .filter(|(id, _, _)| !self.ledger.is_loaded(&id.key, &id.sha256))
            .map(|(_, end_ns, derives)| (*end_ns, *derives))
            .collect();
        pass.unloaded = unloaded.len() as u64;
        pass.oldest_unloaded_age_seconds =
            oldest_age(unloaded.iter().map(|(end, _)| *end), now_seconds);
        // The derivation's own lag, over the objects it was turned on for.
        // A separate number and not a filter a reader applies afterwards: on a
        // host where no feed derives this is 0 while the load's is whatever it
        // is, and that difference is the whole point of publishing two.
        let derived_unloaded = unloaded.iter().filter(|(_, derives)| *derives);
        pass.market_data_unloaded = derived_unloaded.clone().count() as u64;
        pass.market_data_oldest_unloaded_age_seconds =
            oldest_age(derived_unloaded.map(|(end, _)| *end), now_seconds);

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
            now_seconds,
        );
        self.metrics.market_data_pass_finished(
            pass.market_data_unloaded as i64,
            pass.market_data_oldest_unloaded_age_seconds,
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
    ) -> Result<(Recorded, Written), Failed> {
        let bytes_read = std::fs::metadata(&candidate.object)
            .map(|m| m.len())
            .unwrap_or(manifest.byte_count);

        // The latest trailer, pending or recorded. Pending first: within one
        // pass the previous object is still pending when this one is derived,
        // and consulting only the ledger would write an uncertain boundary for
        // every object after the first.
        let trailer = self.trailer();
        // Before the sink is touched, which is what makes this failure one the
        // held rows survive.
        let mut derived = derive_object(&candidate.object, manifest, trailer.as_ref())
            .map_err(|e| Failed::with_rows_intact(kind_of(&e), e.to_string()))?;

        // And only then, and only for a feed somebody named. The digest has been
        // verified and the archive has been walked to its end by the call above,
        // so what this second walk adds is the codec and nothing else.
        //
        // A failure here fails the whole object, batch included. Keeping the
        // datagram rows and dropping the market data would land an object whose
        // `event` table is empty — which is what a feed nobody published on looks
        // like, and it would look like that for ever, because the ledger entry
        // would say the object is loaded.
        if let Some(feed) = derivation_for(self.market_data, &manifest.feed) {
            let events = derive_market_data(&candidate.object, manifest, feed)
                .map_err(|e| Failed::with_rows_intact(ErrorKind::Replay, e.to_string()))?;
            self.metrics.market_data_refused(&refusals(&events));
            extend_batch(&mut derived.rows, events);
        }

        let accepted = self
            .sink
            .write_batch(derived.rows, now_ns)
            .map_err(|e| Failed::with_rows_lost(kind_of_sink(&e), e.to_string()))?;
        // What this call actually sent, which is `0` unless the batch it took
        // made the buffer due. Counted here rather than against the object,
        // because a request that carried four objects has one length.
        self.metrics.bytes_posted(accepted.bytes_posted);
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
        // A ledger that will not write is not a failure of *this* object, and
        // never a lost sink: the rows are in the store, and what is still held
        // is still held. It comes back on the recording, one message per entry.
        Ok((self.record_landed(&accepted.landed), rows))
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
    fn record_landed(&mut self, landed: &[ObjectId]) -> Recorded {
        record_landed(landed, self.pending, self.ledger, self.metrics)
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

/// What one recording did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Recorded {
    /// Objects whose entry is now in the ledger.
    pub recorded: u64,
    /// One message per object whose entry could not be written. Their rows are
    /// in the store with nothing recording it, so the next pass derives and
    /// re-inserts them — a replace, because the tables are `ReplacingMergeTree`
    /// and the rows are a pure function of `(object key, sha256)`.
    pub failures: Vec<String>,
}

/// Writes a ledger entry for every object whose rows have landed, and stops
/// holding them.
///
/// A free function because the way out needs it too: a `--once` pass and a
/// shutdown both end in a flush, and the objects that flush lands have to be
/// recorded by the same code that records them mid-pass. Two copies of this
/// had already drifted — one bailing on a ledger failure and one logging and
/// carrying on, one counting a landing the loader was not holding and one
/// dropping it silently — and neither copy was the one the tests exercised.
///
/// # Every landing is recorded independently, and none of them is a `?`
///
/// **The insert is over.** These objects' rows are in the store and the sink
/// has forgotten them, so the only question left is which of them the ledger
/// gets to hear about — and a return on the first failure answers it for one
/// object and abandons the rest still holding a `pending` entry for rows
/// nobody holds any more. The walk skips what is pending, so those objects are
/// skipped on every later pass: a single transient append failure freezes their
/// lag until the process is restarted, with the ledger writable again and the
/// sink holding nothing.
///
/// So each entry is attempted, each failure is counted, and every landing
/// leaves `pending` whatever the ledger did. An object whose entry did not get
/// written has no entry, which is exactly what makes the next pass derive it
/// again.
pub(crate) fn record_landed(
    landed: &[ObjectId],
    pending: &mut Vec<Pending>,
    ledger: &mut Ledger,
    metrics: &LoaderMetrics,
) -> Recorded {
    let mut out = Recorded::default();
    for id in landed {
        let Some(index) = pending.iter().position(|p| &p.id == id) else {
            // The sink named an object this loader is not holding. It cannot
            // happen — a sink only ever lands what it was given — and if it
            // did, recording an entry for an object with no trailer would put a
            // boundary check on evidence nobody derived.
            continue;
        };
        // Out of `pending` before the ledger is touched, and whatever it says:
        // the rows have gone, and an object left pending over rows the sink no
        // longer holds is one the walk will skip for ever.
        let done = pending.remove(index);
        match ledger.record(Entry {
            object_key: done.id.key.clone(),
            object_sha256: done.id.sha256.clone(),
            loaded_at_ns: now_unix_nanos(),
            trailer: done.trailer,
        }) {
            Ok(()) => {
                metrics.object_loaded(&done.written, done.bytes_read);
                out.recorded += 1;
            }
            Err(e) => out.failures.push(e.to_string()),
        }
    }
    out
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

/// The age of the oldest window in a set, and `0` for an empty one.
///
/// Zero for empty is what makes "nothing is waiting" and "the oldest thing
/// waiting ended a moment ago" the same reading, which is the reading a lag
/// alert wants: both are a loader that is not behind.
fn oldest_age(ends: impl Iterator<Item = u64>, now_unix_seconds: i64) -> i64 {
    ends.min()
        .map_or(0, |end_ns| age_seconds(end_ns, now_unix_seconds))
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
            market_data: &[],
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
            market_data: &[],
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
            market_data: &[],
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
            market_data: &[],
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
            market_data: &[],
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
            market_data: &[],
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

    pub(super) fn sorted_objects(archive: &Archive) -> Vec<PathBuf> {
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
    use dz_recorder_rows::{Accepted, Landed, RowBatch, RowSinkError, Written};

    /// What this sink pretends one object's rows cost on the wire, so a test
    /// can name the total a request should have counted.
    const BYTES_PER_OBJECT: u64 = 1_000;

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
        /// When set, the end-of-pass `post_if_due` posts, which is the shape a
        /// pass takes when the age bound comes due in it.
        due: bool,
        /// Every batch handed over, in order: the object it came from and the
        /// `anchor_certain` of its era rows. `held` cannot show a re-derivation
        /// — the object is already in it — and this can.
        taken: Vec<(String, Vec<u8>)>,
    }

    impl HoldingSink {
        fn post(&mut self) -> Result<Landed, RowSinkError> {
            if self.refuse {
                let held = std::mem::take(&mut self.held);
                return Err(RowSinkError::Rejected {
                    object_key: format!("{} objects", held.len()),
                    attempts: 1,
                    last: "the destination refused it".to_owned(),
                });
            }
            self.posts += 1;
            let objects = std::mem::take(&mut self.held);
            Ok(Landed {
                bytes_posted: objects.len() as u64 * BYTES_PER_OBJECT,
                objects,
            })
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

        /// Due only when a test says so, and never on a clock: what is under
        /// test is the loader's discipline, not a sink's timing.
        fn post_if_due(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
            if self.due {
                self.post()
            } else {
                Ok(Landed::default())
            }
        }

        fn flush(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
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
            market_data: &[],
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
                market_data: &[],
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
        assert_eq!(landed.objects.len(), 3);
        // Through the recording the binary's own way out calls, rather than a
        // hand-rolled copy of it: a test that reimplements the code under test
        // passes over exactly the drift it exists to catch.
        let recorded = record_landed(&landed.objects, &mut pending, &mut ledger, &metrics);
        assert_eq!(recorded.recorded, 3);
        assert!(recorded.failures.is_empty(), "{:?}", recorded.failures);
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
            market_data: &[],
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
            market_data: &[],
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

        let pass = |ledger: &mut Ledger, sink: &mut HoldingSink, pending: &mut Vec<Pending>| {
            Loader {
                objects_dir: &archive.completed,
                site: SITE,
                recorder: RECORDER,
                max_objects: 0,
                ledger,
                sink,
                metrics: &metrics,
                market_data: &[],
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

    /// **A damaged object does not make the loader forget what the sink holds.**
    ///
    /// The derivation fails before the sink is touched, so the objects it is
    /// holding are still going to land — and if the loader has dropped their
    /// trailers by then, the insert lands with no ledger entry written for any
    /// of them: the rows are in the store, nothing records it, and they are
    /// re-inserted every pass until the objects are evicted while their lag
    /// never falls.
    #[test]
    fn a_derive_failure_does_not_drop_the_objects_the_sink_is_holding() {
        let archive = archive(3, 20);
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
        // Due, so the insert goes at the end of this same pass: the age bound
        // coming due in a pass that also hit a damaged object is the whole
        // shape under test.
        let mut sink = HoldingSink {
            due: true,
            ..HoldingSink::default()
        };
        let mut pending = Vec::new();

        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            market_data: &[],
            pending: &mut pending,
        }
        .run_once(&|| false);

        assert_eq!(pass.failed, 1, "the damaged object, and only it");
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("hashes to"), "{}", errors[0]);
        assert_eq!(pass.derived, 2, "the other two reached the sink");
        assert_eq!(
            pass.loaded, 2,
            "and the insert that carried them was recorded"
        );
        assert_eq!(
            ledger.entries(),
            2,
            "rows in the store with no ledger entry are an object the next pass \
             derives again for nothing"
        );
        assert!(pending.is_empty(), "nothing is still waiting on an insert");
        assert_eq!(pass.unloaded, 1, "the damaged object, still unloaded");
    }

    /// **`dz_loader_bytes_written_total` moves.**
    ///
    /// It is fed from the bytes a *request* sent, because a sink that coalesces
    /// reports none per object — the rows of four objects in one body have one
    /// length between them. A counter fed from the per-object number therefore
    /// stayed at 0 for ever, while its own help text and `bytes_read_total`'s
    /// both tell an operator to compare the two.
    #[test]
    fn the_bytes_counter_follows_the_request_that_carried_the_rows() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink {
            due: true,
            ..HoldingSink::default()
        };
        let mut pending = Vec::new();

        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            market_data: &[],
            pending: &mut pending,
        }
        .run_once(&|| false);

        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.loaded, 3, "one request carried all three");
        let text = metrics.render();
        assert!(
            text.contains(&format!(
                "dz_loader_bytes_written_total{{recorder=\"{RECORDER}\",site=\"{SITE}\"}} {}",
                3 * BYTES_PER_OBJECT
            )),
            "the request's bytes were dropped on the way to the counter:\n{text}"
        );
        // And the number it is meant to be read against is there too, because
        // the ratio is the reason the rows travel and the objects stay local.
        assert!(
            !text.contains(&format!(
                "dz_loader_bytes_read_total{{recorder=\"{RECORDER}\",site=\"{SITE}\"}} 0"
            )),
            "{text}"
        );
    }

    /// The pass bound bounds a pass under a sink that coalesces.
    ///
    /// It used to be tested against what the pass *loaded*, and loading stays
    /// at 0 until an insert goes — so a catch-up pass against the deployed sink
    /// derived every object in the directory, which is the unbounded pass the
    /// key exists to prevent. The `FileSink` the other bound test uses lands
    /// every object as it takes it, so it cannot tell the two numbers apart.
    #[test]
    fn the_pass_bound_counts_what_was_derived_because_nothing_has_landed_yet() {
        let archive = archive(4, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = HoldingSink::default();
        let mut pending = Vec::new();

        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 2,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            market_data: &[],
            pending: &mut pending,
        }
        .run_once(&|| false);

        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(pass.derived, 2, "the bound is a bound");
        assert_eq!(pass.loaded, 0, "and nothing landed, which is the point");
        assert_eq!(pending.len(), 2);
        // The scan carried on past the bound, so the lag it publishes is the
        // whole backlog and not the prefix it got through.
        assert_eq!(pass.unloaded, 4);
        assert!(pass.oldest_unloaded_age_seconds > 0);
    }

    /// **A ledger failure part-way through an insert's landings strands
    /// nothing.**
    ///
    /// The insert is over by the time these entries are written: the rows are
    /// in the store and the sink has forgotten them. A recording that returned
    /// on the first failure left every object behind it holding a `pending`
    /// entry for rows nobody holds any more — and since the walk skips what is
    /// pending, those objects were skipped on every later pass, their lag
    /// frozen, with the ledger writable again and the sink empty. Only a
    /// restart cleared it.
    ///
    /// A transient fault is enough, so the fault here is transient: the ledger
    /// path is a directory for one pass and a writable file after it.
    #[test]
    fn a_ledger_failure_mid_landing_leaves_every_object_recoverable() {
        let archive = archive(3, 20);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        // Due, so the insert lands inside the pass and the entries are written
        // while the walk is still the thing running.
        let mut sink = HoldingSink {
            due: true,
            ..HoldingSink::default()
        };
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
                market_data: &[],
                pending,
            }
            .run_once(&|| false)
        };

        // Every append fails: a directory is not a file to append to.
        std::fs::create_dir(&archive.ledger).expect("the parent is writable");
        let (first, errors) = pass(&mut ledger, &mut sink, &mut pending);
        assert_eq!(first.derived, 3);
        assert_eq!(first.loaded, 0, "not one entry was written");
        assert_eq!(
            first.failed, 3,
            "and each one is counted, not just the first"
        );
        assert!(errors.iter().any(|e| e.contains("ledger")), "{errors:?}");
        assert!(
            pending.is_empty(),
            "the sink has sent and forgotten these rows, so nothing may still \
             be waiting on them: {pending:?}"
        );
        assert_eq!(ledger.entries(), 0);

        // The fault clears.
        std::fs::remove_dir(&archive.ledger).expect("the directory is removable");

        let (second, errors) = pass(&mut ledger, &mut sink, &mut pending);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(
            second.derived, 3,
            "all three are derived again, which is a replace"
        );
        assert_eq!(second.skipped, 0, "and none of them is stranded as held");
        assert_eq!(second.loaded, 3);
        assert_eq!(ledger.entries(), 3);
        assert_eq!(second.unloaded, 0, "the lag falls to nothing");
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
            market_data: &[],
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

/// The switch, over an archive the real encoder wrote.
///
/// **The assertion that matters here is the one about the datagram rows.** This
/// tier is being added to a loader already in production shape, so the claim
/// being tested is not that derivation works — the events crate's own suite says
/// that — but that a loader with no `[[market_data]]` section writes byte for
/// byte what it wrote before.
#[cfg(test)]
mod market_data_tests {
    use std::net::SocketAddrV4;
    use std::time::Duration;

    use dz_edge_core::{
        ChannelSequence, DatagramBuilder, Feed, PortRole, ResetCount, MAX_DATAGRAM_SIZE,
    };
    use dz_edge_mbp::{
        LevelUpdate, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel, ACTION_NEW,
        SIDE_ASK, SIDE_BID,
    };
    use dz_edge_refdata::{InstrumentDefinition, ASSET_CLASS_CRYPTO_SPOT, SYMBOL_LEN};
    use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
    use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
    use dz_recorder_archive::Compression;
    use dz_recorder_core::{CaptureDropScope, RecorderIdentity, RecvTsKind, Sink};
    use dz_recorder_replay::synthetic::{port_for, GROUP, PRIMARY_SOURCE};
    use dz_recorder_replay::OwnedDatagram;
    use dz_recorder_rows::{FileSink, Grain};
    use tempfile::TempDir;

    use crate::config::MarketDataFeed;

    use super::*;

    const SITE: &str = "site-1";
    const RECORDER: &str = "recorder-1";
    /// One `Channel ID` across all three port roles.
    ///
    /// Reference data is keyed on `(source address, Channel ID)` and never on
    /// the channel instance, because definitions arrive on `refdata` and prices
    /// on `mktdata`: a fixture that gave the roles different channels would file
    /// the definitions where the prices could never find them, and every event
    /// row would be refused for an unresolved instrument.
    const CHANNEL: u8 = 3;
    const INSTRUMENT: u32 = 4_242;
    const SOURCE_ID: u16 = 2;
    const SNAPSHOT_ID: u32 = 77;
    /// Levels the cycle promises and carries. Two, so the count is not one.
    const TOTAL_LEVELS: u32 = 2;
    const FIRST_RECV_TS_NS: u64 = 1_700_000_000_123_456_789;

    struct Archive {
        _dir: TempDir,
        completed: PathBuf,
        rows: PathBuf,
        ledger: PathBuf,
    }

    fn symbol() -> [u8; SYMBOL_LEN] {
        let mut out = [b' '; SYMBOL_LEN];
        out[..8].copy_from_slice(b"BTC-USDT");
        out
    }

    fn definition() -> InstrumentDefinition {
        InstrumentDefinition {
            instrument_id: INSTRUMENT,
            source_id: SOURCE_ID,
            symbol: symbol(),
            leg1: *b"BTC     ",
            leg2: *b"USDT    ",
            asset_class: ASSET_CLASS_CRYPTO_SPOT,
            price_exponent: -2,
            qty_exponent: -8,
            market_model: 1,
            tick_size: 1,
            lot_size: 1,
            contract_value: 1,
            expiry_ns: 0,
            settle_type: 1,
            price_bound: 1,
            manifest_seq: 1,
        }
    }

    fn level(seq: u32, price_raw: i64, qty_raw: u64, side: u8) -> LevelUpdate {
        LevelUpdate {
            instrument_id: INSTRUMENT,
            source_id: SOURCE_ID,
            side,
            action: ACTION_NEW,
            per_instrument_seq: seq,
            price_raw,
            qty_raw,
            timestamp_ns: 1_700_000_000_000_000_000 + u64::from(seq),
            order_count: 3,
            level_index: 0,
            update_reason: 0,
            level_flags: 0,
        }
    }

    /// One datagram of this feed, framed by the real builder.
    fn datagram(
        sequence: ChannelSequence,
        role: PortRole,
        recv_ts_ns: u64,
        push: impl FnOnce(&mut DatagramBuilder<MarketByPrice>),
    ) -> OwnedDatagram {
        let mut builder = DatagramBuilder::<MarketByPrice>::new(
            sequence,
            role,
            u16::try_from(MAX_DATAGRAM_SIZE).expect("the mandated cap fits a u16"),
        );
        push(&mut builder);
        let payload = builder
            .finish(recv_ts_ns - 1_000)
            .expect("a datagram with at least one message is emittable");
        let wire_payload_len = u32::try_from(payload.len()).expect("a datagram is small");
        OwnedDatagram {
            payload,
            src: SocketAddrV4::new(PRIMARY_SOURCE, 50_000),
            dst: SocketAddrV4::new(GROUP, port_for(role)),
            role,
            recv_ts_ns,
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta: 0,
            ttl: Some(4),
            link_headers: None,
            wire_payload_len,
        }
    }

    /// A depth feed's three roles, in arrival order.
    ///
    /// The definition arrives first because a statement is in force from the
    /// instant it was received: a fixture that priced before it defined would be
    /// asserting the fold's refusal rather than its output.
    fn depth_stream() -> Vec<OwnedDatagram> {
        let mut out = Vec::new();
        let mut recv_ts = FIRST_RECV_TS_NS;
        let mut stamp = || {
            recv_ts += 7_654_321;
            recv_ts
        };

        let refdata = ChannelSequence::new(CHANNEL, ResetCount::NEVER_RESET);
        out.push(datagram(refdata, PortRole::Refdata, stamp(), |b| {
            b.push(&definition())
                .expect("refdata carries an instrument definition");
        }));

        let mut mktdata = ChannelSequence::new(CHANNEL, ResetCount::NEVER_RESET);
        for (seq, price, qty, side) in [
            (1u32, 9_999_500i64, 12_500u64, SIDE_BID),
            (2, 10_000_500, 7_250, SIDE_ASK),
            (3, 9_999_600, 11_000, SIDE_BID),
        ] {
            out.push(datagram(mktdata, PortRole::Mktdata, stamp(), |b| {
                b.push(&level(seq, price, qty, side))
                    .expect("mktdata carries a level update");
            }));
            mktdata.advance();
        }

        // One complete cycle: a begin, its levels, an end. Split across two
        // datagrams, which is the ordinary case for a real book.
        let mut snapshot = ChannelSequence::new(CHANNEL, ResetCount::NEVER_RESET);
        out.push(datagram(snapshot, PortRole::Snapshot, stamp(), |b| {
            b.push(&SnapshotBegin {
                instrument_id: INSTRUMENT,
                anchor_seq: 3,
                total_levels: TOTAL_LEVELS,
                snapshot_id: SNAPSHOT_ID,
                last_instrument_seq: 3,
                timestamp_ns: 1_700_000_000_000_000_300,
                depth_bound: 50,
            })
            .expect("snapshot carries a begin");
            b.push(&SnapshotLevel {
                snapshot_id: SNAPSHOT_ID,
                price_raw: 9_999_600,
                qty_raw: 11_000,
                order_count: 1,
                side: SIDE_BID,
                level_flags: 0,
            })
            .expect("and a level");
        }));
        snapshot.advance();
        out.push(datagram(snapshot, PortRole::Snapshot, stamp(), |b| {
            b.push(&SnapshotLevel {
                snapshot_id: SNAPSHOT_ID,
                price_raw: 10_000_500,
                qty_raw: 7_250,
                order_count: 1,
                side: SIDE_ASK,
                level_flags: 0,
            })
            .expect("the second level");
            b.push(&SnapshotEnd {
                instrument_id: INSTRUMENT,
                anchor_seq: 3,
                snapshot_id: SNAPSHOT_ID,
            })
            .expect("and the end that closes it");
        }));

        out
    }

    /// A completed directory with one object of `feed` in it.
    fn archive(feed: &str) -> Archive {
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
            feed: feed.to_owned(),
            roles_joined: [PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot]
                .into_iter()
                .map(|role| RoleJoin::on(role, GROUP, port_for(role)))
                .collect(),
            link_headers: LinkHeaders::Synthesised,
            capture_drop_scope: CaptureDropScope::PortRole,
        };
        let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
        for dg in depth_stream() {
            Sink::write(&mut writer, &dg.as_recorded()).expect("the write path never fails");
        }
        assert_eq!(writer.datagrams_dropped_total(), 0);
        writer
            .rotate_at(FIRST_RECV_TS_NS + 1_000_000_000)
            .expect("rotation")
            .expect("a segment that held datagrams produces an object");
        writer
            .wait_completed()
            .expect("the compressor publishes exactly one object")
            .expect("publication");
        Archive {
            completed,
            rows: dir.path().join("rows"),
            ledger: dir.path().join("ledger.jsonl"),
            _dir: dir,
        }
    }

    fn derives(persist_snapshot_levels: bool) -> Vec<MarketDataFeed> {
        vec![MarketDataFeed {
            feed: MarketByPrice::NAME.to_owned(),
            magic: MarketByPrice::MAGIC,
            persist_snapshot_levels,
        }]
    }

    /// One pass over `archive`, with `market_data` in force.
    fn pass(archive: &Archive, market_data: &[MarketDataFeed], metrics: &LoaderMetrics) -> Pass {
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stop = || false;
        let (pass, errors) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics,
            market_data,
            pending: &mut Vec::new(),
        }
        .run_once(&stop);
        sink.flush(now_unix_nanos()).expect("flush");
        assert!(errors.is_empty(), "{errors:?}");
        pass
    }

    fn rows(archive: &Archive, grain: Grain) -> Vec<serde_json::Value> {
        std::fs::read_to_string(FileSink::path_in(&archive.rows, grain))
            .map(|text| {
                text.lines()
                    .filter(|l| !l.is_empty())
                    .map(|l| serde_json::from_str(l).expect("one JSON object per line"))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn message_types(archive: &Archive) -> Vec<String> {
        rows(archive, Grain::Event)
            .iter()
            .map(|row| row["message_type"].as_str().unwrap_or_default().to_owned())
            .collect()
    }

    /// **The switch off writes no market data row and changes no datagram row.**
    ///
    /// The second half is the one that matters. Everything about this tier is
    /// additive to a loader that is already in production shape, so what has to
    /// be true is that a host which turned nothing on gets the file it got
    /// before — not similar rows, the same bytes.
    #[test]
    fn the_switch_off_writes_no_market_data_and_leaves_the_datagram_rows_alone() {
        let off = archive(MarketByPrice::NAME);
        let on = archive(MarketByPrice::NAME);
        let metrics = LoaderMetrics::new(SITE, RECORDER);

        let without = pass(&off, &[], &metrics);
        let with = pass(&on, &derives(false), &metrics);

        for grain in [Grain::Event, Grain::Instrument, Grain::BookTop] {
            assert!(
                rows(&off, grain).is_empty(),
                "{grain} rows were written for a feed nobody named"
            );
            assert!(
                !rows(&on, grain).is_empty(),
                "{grain} rows are what the switch turns on"
            );
        }

        // And the transport tier is untouched: same rows, same order, same
        // values, in all four of its tables.
        for grain in [
            Grain::Datagram,
            Grain::Era,
            Grain::SegmentCoverage,
            Grain::SequenceGap,
        ] {
            assert_eq!(
                rows(&off, grain),
                rows(&on, grain),
                "{grain} differs with the switch on"
            );
        }
        assert_eq!(without.loaded, 1);
        assert_eq!(with.loaded, 1);
        assert_eq!(
            without.written.rows(Grain::Datagram),
            with.written.rows(Grain::Datagram)
        );
        assert_eq!(without.written.rows(Grain::Event), 0);
        assert!(with.written.rows(Grain::Event) > 0);
    }

    /// A feed the configuration does not name derives nothing, even when
    /// another one does.
    ///
    /// The switch is per feed, so a host carrying two feeds into one completed
    /// directory turns derivation on for one of them and not for the other —
    /// which is the whole reason it is not a global flag.
    #[test]
    fn a_feed_the_configuration_does_not_name_derives_nothing() {
        let other = archive("top-of-book");
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let pass = pass(&other, &derives(false), &metrics);

        assert_eq!(pass.loaded, 1, "it still loads");
        assert!(!rows(&other, Grain::Datagram).is_empty());
        assert!(
            rows(&other, Grain::Event).is_empty(),
            "the entry names market-by-price and this object says top-of-book"
        );
    }

    /// The level switch decides rows and never state.
    ///
    /// A cycle is always visible as a row, because begin and end are always
    /// written and the end carries `levels_seen` against the begin's
    /// `total_levels`. Persisting the levels is what makes each one its own row,
    /// and the book anchors either way — which is what makes the count on the
    /// end row trustworthy in the case where the levels are not there to count.
    #[test]
    fn levels_feed_the_book_always_and_become_rows_only_when_asked() {
        let consumed = archive(MarketByPrice::NAME);
        let persisted = archive(MarketByPrice::NAME);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        pass(&consumed, &derives(false), &metrics);
        pass(&persisted, &derives(true), &metrics);

        assert_eq!(
            message_types(&consumed)
                .iter()
                .filter(|t| *t == "SnapshotLevel")
                .count(),
            0,
            "levels are consumed, not persisted: {:?}",
            message_types(&consumed)
        );
        assert_eq!(
            message_types(&persisted)
                .iter()
                .filter(|t| *t == "SnapshotLevel")
                .count(),
            TOTAL_LEVELS as usize
        );

        // The cycle is a row either way, and the end says how many levels were
        // actually seen — which is what makes "was the snapshot complete"
        // answerable from the rows that are there.
        for archive in [&consumed, &persisted] {
            let events = rows(archive, Grain::Event);
            let begin = events
                .iter()
                .find(|row| row["message_type"] == "SnapshotBegin")
                .expect("a begin row");
            let end = events
                .iter()
                .find(|row| row["message_type"] == "SnapshotEnd")
                .expect("an end row");
            assert_eq!(begin["total_levels"], TOTAL_LEVELS);
            assert_eq!(end["levels_seen"], TOTAL_LEVELS);
        }

        // And the book anchored, which is the thing consuming every level is
        // for: a level skipped before the book saw it leaves a cycle that never
        // completes and nothing that ever anchors.
        let anchored = rows(&consumed, Grain::BookTop);
        assert!(
            anchored.iter().any(|row| row["from_anchor"] == 1),
            "no top came from applying the snapshot: {anchored:?}"
        );
        assert_eq!(
            rows(&consumed, Grain::BookTop).len(),
            rows(&persisted, Grain::BookTop).len(),
            "the switch is about rows, not about state"
        );
    }

    /// The rows carry the recorder that observed the bytes, not the loader.
    #[test]
    fn every_market_data_row_is_signed_by_the_manifests_recorder() {
        let archive = archive(MarketByPrice::NAME);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        pass(&archive, &derives(false), &metrics);

        for row in rows(&archive, Grain::Event) {
            assert_eq!(row["site"], SITE);
            assert_eq!(row["recorder"], RECORDER);
            assert_eq!(row["feed"], MarketByPrice::NAME);
            assert_eq!(row["instrument_id"], INSTRUMENT);
            assert_eq!(row["source_id"], SOURCE_ID);
        }
        for row in rows(&archive, Grain::BookTop) {
            // `site` names a recorder, and this is what names an observation of
            // one book among several.
            assert_eq!(row["observation"], format!("{SITE}/{RECORDER}"));
        }
    }

    /// Two lags, and the derivation's counts only the objects it is on for.
    ///
    /// With nothing named, the load has an object waiting and the derivation has
    /// none — because that object was never going to produce a row about an
    /// instrument, and a page about it would be a page about nothing.
    #[test]
    fn the_derivations_backlog_is_only_the_objects_it_derives() {
        let archive = archive(MarketByPrice::NAME);
        let metrics = LoaderMetrics::new(SITE, RECORDER);

        // A pass that loads nothing, so both backlogs are non-trivial: the
        // ledger is thrown away and the sink never posts.
        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let mut sink = FileSink::create(&archive.rows).expect("the directory is writable");
        let stopped = || true;
        let (off, _) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            market_data: &[],
            pending: &mut Vec::new(),
        }
        .run_once(&stopped);
        assert_eq!(off.unloaded, 1, "the load is behind");
        assert_eq!(
            off.market_data_unloaded, 0,
            "and no market data is at risk, because no feed derives"
        );
        assert_eq!(off.market_data_oldest_unloaded_age_seconds, 0);

        let mut ledger = Ledger::open(&archive.ledger).expect("a new ledger");
        let (on, _) = Loader {
            objects_dir: &archive.completed,
            site: SITE,
            recorder: RECORDER,
            max_objects: 0,
            ledger: &mut ledger,
            sink: &mut sink,
            metrics: &metrics,
            market_data: &derives(false),
            pending: &mut Vec::new(),
        }
        .run_once(&stopped);
        assert_eq!(on.unloaded, 1);
        assert_eq!(on.market_data_unloaded, 1, "now it is");
        assert!(
            on.market_data_oldest_unloaded_age_seconds > 0,
            "an object recorded in 2023 is not zero seconds behind"
        );
        assert!(metrics
            .render()
            .contains("dz_loader_market_data_unloaded_objects"));
    }

    /// Nothing resolved is not the same as nothing published.
    ///
    /// A `Magic` that matches no datagram in the object is the shape a typo
    /// takes, and it writes an empty `event` table — which reads exactly like a
    /// feed nobody published on. The refusal counters are what tell the two
    /// apart, and here the walk skips every datagram as foreign so even those
    /// stay at zero: the row counts are the only evidence, and they are counted
    /// per grain already.
    #[test]
    fn a_magic_that_matches_nothing_derives_nothing_rather_than_guessing() {
        let archive = archive(MarketByPrice::NAME);
        let metrics = LoaderMetrics::new(SITE, RECORDER);
        let wrong = vec![MarketDataFeed {
            feed: MarketByPrice::NAME.to_owned(),
            magic: 0x445A,
            persist_snapshot_levels: false,
        }];
        let pass = pass(&archive, &wrong, &metrics);

        assert_eq!(pass.loaded, 1, "the object still loads");
        assert!(!rows(&archive, Grain::Datagram).is_empty());
        assert!(
            rows(&archive, Grain::Event).is_empty(),
            "a datagram at the wrong Magic is refused, never parsed at the wrong layout"
        );
    }
}
