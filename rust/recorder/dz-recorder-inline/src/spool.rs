//! Rows on disk between the derivation and the column store: one window, one
//! directory.
//!
//! # Why the rows go to disk on every window, and not only during an outage
//!
//! **A recovery path that runs only during an incident is a recovery path
//! nobody has tested.** If the disk were the exception, this code would first
//! be exercised on the day it is most needed.
//!
//! **A crash otherwise loses what no archive can return.** The column store's
//! sink coalesces rows across units deliberately, to keep merge pressure a
//! function of rows per part, which means it holds rows in memory for as long as
//! its age bound allows. In archive mode that costs nothing — the objects are on
//! disk and the next pass re-derives them. Inline, that memory is the only copy,
//! and an out-of-memory kill, an uncaught panic or a host reboot takes it with
//! nothing recording that it did. With the rows on disk first, a crash costs the
//! open window.
//!
//! **It brings the ledger back, and with it idempotence.** `Accepted` and
//! `Landed` are distinct in the row-sink trait precisely because a sink that has
//! taken rows has not necessarily sent them. With windows on disk there is
//! something to retry and something to record: a window whose insert was never
//! acknowledged is replayed, `ReplacingMergeTree` makes the replay a replace,
//! and the ledger entry is written when the rows land and never when they are
//! accepted.
//!
//! # It never blocks, and that is not a performance choice
//!
//! When the byte budget is full the oldest window is evicted and counted, and
//! the derivation is never made to wait. A spool that applied backpressure would
//! stall the derivation, fill the ring behind it, overflow the receive queue,
//! and convert a column-store outage into feed loss — plus a false publisher-loss
//! finding in every window derived during it. Losing bounded history is
//! recoverable; contaminating live data is not. This is the archive's staging
//! rule, restated for a different unit.
//!
//! **Alert on [`oldest_age_seconds`](Spool::oldest_age_seconds), never on the
//! eviction counter.** A full budget evicts on every window at steady state by
//! design, so the counter rises whether or not anything is wrong, while one
//! window older than the eviction horizon is history already gone.
//!
//! # The insert happens with no lock held, which is why there is no `post`
//!
//! The derivation thread calls [`Spool::store`] and the posting thread drains
//! the spool, so this sits behind a mutex. A method that took `&mut self` and
//! made the insert inside it would hold that lock for the length of an HTTP
//! request — and a slow or hung destination would then block `store`, block the
//! derivation, fill the ring, and drop datagrams. That is the same backpressure
//! chain the byte budget exists to prevent, arriving by the lock rather than by
//! the disk, and by a route nobody would think to look at.
//!
//! So posting is three calls with the insert between them, and the middle one
//! touches no spool state:
//!
//! ```text
//! lock ─► take_oldest ─► unlock ─► write_batch ─► lock ─► record_landed / release
//! ```
//!
//! [`take_oldest`](Spool::take_oldest) marks the window in flight, so nothing
//! offers it twice while the insert is out; [`record_landed`](Spool::record_landed)
//! takes the ids the sink says are durable — a list, because a sink that
//! coalesces lands earlier windows together with the current one — and
//! [`release`](Spool::release) puts everything in flight back when the
//! destination refuses.
//!
//! # What one window directory holds
//!
//! A newline-delimited JSON file per grain, written through [`FileSink`], so
//! the spool holds exactly the bytes the column-store sink will send rather than
//! a second serialisation of the same rows — and beside them one sidecar,
//! [`CLOSED`], carrying the window's identity, its trailer and a digest per
//! grain file.
//!
//! The sidecar is renamed into place last and is the window's commit point. A
//! directory without one is a window whose close never finished, and it is
//! discarded rather than loaded: the alternative is inserting the part of a
//! window that reached the disk and recording the whole of it as loaded.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use dz_recorder_load::ledger::{Entry, Ledger};
use dz_recorder_load::now_unix_nanos;
use dz_recorder_rows::{
    BookTop, ConformanceFinding, Datagram, Derivation, Era, Event, FileSink, Grain, Instrument,
    ObjectId, RowBatch, RowSink as _, RowSinkError, SegmentCoverage, SegmentTrailer, SequenceGap,
};

/// What one call to [`Spool::record_landed`] did.
///
/// The loader's type, not a second one of the same shape: "which units did the
/// ledger get to hear about, and which could it not be told about" is one
/// question, and two answers to it would drift.
pub use dz_recorder_load::Recorded;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// What every window directory is named with.
///
/// The budget can only be enforced over names it can classify, and eviction may
/// only reach what this module wrote: a spool directory is a directory on
/// somebody's host, and a budget that doubles as a licence to delete another
/// program's files is worse than an unbounded one.
const WINDOW_PREFIX: &str = "window-";

/// Nanoseconds zero-padded to the width `u64::MAX` needs, so that ordering the
/// names lexicographically is ordering the windows by start stamp.
const STAMP_WIDTH: usize = 20;

/// The sidecar, renamed into place once every grain file is durable.
pub const CLOSED: &str = "window.json";

/// The sidecar before the rename. A window still under this name is one whose
/// close did not finish.
const CLOSING: &str = "window.json.closing";

/// A window the spool would not load, and why.
///
/// Every variant names the window directory, because the answer to all of them
/// is the same — the window is deleted and its rows are gone — and the only
/// thing an operator needs is which window and what was wrong with it.
#[derive(Debug, Error)]
pub enum SpoolError {
    #[error("spool {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// The bytes on disk are not the bytes the close hashed: a torn write, a
    /// truncation, or a file something else edited.
    #[error("window {window}: {grain}.jsonl does not match the digest written at close")]
    Digest { window: String, grain: Grain },
    /// A grain file the close did not hash. It cannot have been written by this
    /// module, and inserting rows nothing vouched for is the one thing the
    /// digest exists to prevent.
    #[error("window {window}: {grain}.jsonl is on disk and the close recorded no digest for it")]
    Unrecorded { window: String, grain: Grain },
    #[error("window {window}: {grain}.jsonl was recorded at close and is not on disk")]
    Missing { window: String, grain: Grain },
    /// The bytes match their digest and no longer parse, which is a row type
    /// that changed shape under a spool a previous build wrote.
    #[error("window {window}: a {grain} row will not parse: {source}")]
    Unreadable {
        window: String,
        grain: Grain,
        #[source]
        source: serde_json::Error,
    },
    #[error("window {window} was never closed: {reason}")]
    Unclosed { window: String, reason: String },
    #[error(transparent)]
    Sink(#[from] RowSinkError),
}

/// One window's rows, off the disk and handed out for an insert.
///
/// It exists so the insert can happen with the spool's lock released. The window
/// stays on disk and stays in flight — nothing else will offer it — until the
/// caller reports what landed to [`Spool::record_landed`] or gives it back with
/// [`Spool::release`]. Dropping one without doing either leaves it in flight for
/// the life of the process, which is a window nothing posts and nothing evicts
/// until the budget reaches it.
#[derive(Debug)]
pub struct InFlightWindow {
    id: ObjectId,
    batch: RowBatch,
}

impl InFlightWindow {
    /// Which window this is, for a log line or a counter label.
    #[must_use]
    pub const fn id(&self) -> &ObjectId {
        &self.id
    }

    /// The rows, for [`RowSink::write_batch`](dz_recorder_rows::RowSink::write_batch).
    #[must_use]
    pub fn into_rows(self) -> RowBatch {
        self.batch
    }
}

/// One window's rows on their way to the column store.
#[derive(Debug)]
struct Window {
    dir: PathBuf,
    /// From the window key, which carries the window's start in wall-clock
    /// nanoseconds and therefore orders windows across recorder runs — a
    /// per-run sequence number restarts at 0 and cannot.
    start_ns: u64,
    bytes: u64,
    id: ObjectId,
    sidecar: Sidecar,
    /// Rows the sink has taken and not yet acknowledged.
    ///
    /// Not re-posted while it is set: a sink that coalesces will land these rows
    /// on a later call, and sending them again in the meantime is one insert's
    /// worth of work for a row already in flight.
    in_flight: bool,
    /// The rows are in the store and the ledger entry could not be written.
    ///
    /// **What is owed is the entry, not the insert.** Re-posting these rows
    /// would be a replace of rows that are already there, so the window is not
    /// offered again — and it is not forgotten either, because an entry nobody
    /// retries is a window whose trailer the next era anchor will never see.
    /// [`Spool::record_landed`] retries it, which is why the pipeline calls that
    /// once a pass even with nothing landed.
    entry_owed: bool,
}

/// What the close writes beside the rows.
///
/// The identity is here rather than re-read from the rows because a `RowBatch`
/// carries it once and the per-grain files carry it per row: reconstructing the
/// batch from the files alone would mean trusting one row to speak for the
/// window.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sidecar {
    object_key: String,
    object_sha256: String,
    derivation: Derivation,
    start_ns: u64,
    /// What the next window's adjacency check consults, carried so that the
    /// ledger entry written when these rows land is the loader's entry and not
    /// a weaker one.
    trailer: SegmentTrailer,
    /// One entry per grain file the close actually wrote. A grain that produced
    /// no rows has no file and no entry — an empty `conformance_finding.jsonl`
    /// beside a real one reads as a rule set that ran and found nothing.
    digests: Vec<GrainDigest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GrainDigest {
    /// The table name, which is what [`FileSink`] names the file: one spelling,
    /// so a sidecar and a directory listing cannot disagree.
    grain: String,
    sha256: String,
    bytes: u64,
}

/// Windows on disk, oldest first, under a byte budget.
///
/// Two threads reach it — the derivation stores and the posting stage drains —
/// so it lives behind a mutex, and every method here is short and touches no
/// network. The one call that does is the caller's, between
/// [`take_oldest`](Self::take_oldest) and [`record_landed`](Self::record_landed),
/// with the lock released.
#[derive(Debug)]
pub struct Spool {
    dir: PathBuf,
    budget_bytes: u64,
    /// Keyed on the directory name, which begins with a zero-padded start
    /// stamp: iteration order is consumption order, and it is the same order
    /// after a restart as before one.
    windows: BTreeMap<String, Window>,
    windows_evicted_total: u64,
    bytes_evicted_total: u64,
    windows_discarded_total: u64,
    unreclaimable_bytes: u64,
    /// Discards since the caller last drained them, so that a window given up on
    /// is named somewhere an operator can read and not only counted.
    discarded: Vec<SpoolError>,
}

impl Spool {
    /// Opens the directory and adopts what a previous run left in it.
    ///
    /// **This is the replay, and it happens before any new window is written.**
    /// A run that started writing new windows first would post them ahead of
    /// older ones still on the disk, which is the one ordering that makes an era
    /// anchor uncertain when the evidence for it was there all along.
    ///
    /// A window this run cannot load is discarded here rather than at post time:
    /// the budget is enforced immediately afterwards, and bytes that cannot be
    /// classified are bytes eviction cannot reach.
    ///
    /// # Errors
    ///
    /// [`SpoolError::Io`] if the directory cannot be created or read. A spool
    /// directory that cannot be read is not the same as an empty one: starting
    /// on the second is resuming from nothing, and starting on the first is
    /// running with a disk full of windows nothing will ever post or evict.
    pub fn open(dir: impl Into<PathBuf>, budget_bytes: u64) -> Result<Self, SpoolError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|source| SpoolError::Io {
            path: dir.clone(),
            source,
        })?;
        let mut spool = Self {
            dir: dir.clone(),
            budget_bytes,
            windows: BTreeMap::new(),
            windows_evicted_total: 0,
            bytes_evicted_total: 0,
            windows_discarded_total: 0,
            unreclaimable_bytes: 0,
            discarded: Vec::new(),
        };

        let entries = fs::read_dir(&dir).map_err(|source| SpoolError::Io {
            path: dir.clone(),
            source,
        })?;
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(start_ns) = start_ns_in(&name) else {
                // Counted and left alone. A name this module did not write is
                // not ours to delete, and bytes nothing accounts for are bytes
                // nothing bounds — so the number is published rather than acted
                // on.
                spool.unreclaimable_bytes += tree_bytes(&entry.path());
                continue;
            };
            match spool.adopt(&name, start_ns) {
                Ok(window) => {
                    spool.windows.insert(name, window);
                }
                Err(e) => spool.discard(&name, e),
            }
        }

        // A budget lowered between runs is still a budget, and the previous
        // run's windows are what it applies to first.
        spool.enforce();
        Ok(spool)
    }

    /// Writes one derived window to disk and closes it.
    ///
    /// `start_ns` is the stamp the window key carries; it is what orders this
    /// window against every other, including a previous run's.
    ///
    /// The `fsync` is here, once, and never per batch: syncing per insert would
    /// pay a sync for durability the window bound already provides, and syncing
    /// not at all would make the whole spool a page cache a host reboot empties.
    /// What this bounds is a crash to the open window.
    ///
    /// # Errors
    ///
    /// [`SpoolError::Io`] or [`SpoolError::Sink`] if the rows cannot be written
    /// or made durable. The caller has lost this window and no other: nothing
    /// partial is left under a name a later run would load, because the sidecar
    /// that marks a window loadable is renamed into place last.
    ///
    /// **And nothing partial is left on the disk either.** A failure after the
    /// directory was created used to leave it there and out of `self.windows`,
    /// so its bytes sat outside [`bytes`](Self::bytes), [`enforce`](Self::enforce)
    /// and [`unreclaimable_bytes`](Self::unreclaimable_bytes) alike — the spool
    /// reporting itself empty while orphans accumulated, one per failed window,
    /// until a restart adopted or discarded them. The byte count is a claim
    /// about the disk, and a claim with an exception is not one. So the fallible
    /// part is [`write_window`](Self::write_window) and this is its error path:
    /// what it created is removed, and a removal that itself fails moves those
    /// bytes to `unreclaimable_bytes` rather than forgetting them.
    pub fn store(
        &mut self,
        start_ns: u64,
        batch: RowBatch,
        trailer: SegmentTrailer,
    ) -> Result<(), SpoolError> {
        let id = ObjectId::of(&batch);
        let name = window_name(start_ns, &id.key);
        let dir = self.dir.join(&name);

        match self.write_window(start_ns, batch, trailer, &name, &dir, id) {
            Ok(()) => {
                // After the window is on disk and never before it: eviction is
                // what keeps this from blocking, and a spool that made room
                // first would be a spool that decided what to give up before it
                // knew what it had.
                self.enforce();
                Ok(())
            }
            Err(e) => {
                // Counted where it can still be seen. `remove_tree` on a
                // directory that was never created succeeds, so this is the one
                // path for every failure above — and a removal that fails leaves
                // bytes this module can no longer reach, which is what
                // `unreclaimable_bytes` is.
                if remove_tree(&dir).is_err() {
                    self.unreclaimable_bytes =
                        self.unreclaimable_bytes.saturating_add(tree_bytes(&dir));
                }
                Err(e)
            }
        }
    }

    /// Everything `store` does that can fail, so that one error path can undo
    /// all of it.
    ///
    /// Separated for that reason alone: a `?` in the middle of a function that
    /// has already created a directory is a `?` that leaks it, and there were
    /// six of them.
    fn write_window(
        &mut self,
        start_ns: u64,
        batch: RowBatch,
        trailer: SegmentTrailer,
        name: &str,
        dir: &Path,
        id: ObjectId,
    ) -> Result<(), SpoolError> {
        let derivation = batch.derivation;
        let dir = dir.to_path_buf();

        // `FileSink` appends, deliberately, so that a double load shows rather
        // than hides. Here that would double a window's rows and hash the
        // doubling, so the directory starts empty whatever a previous attempt
        // left in it.
        self.windows.remove(name);
        remove_tree(&dir)?;

        let mut sink = FileSink::create(&dir)?;
        sink.write_batch(batch, start_ns)?;
        // Explicit, and not left to the `Drop` impl: what follows hashes these
        // files, and hashing a buffer that has not reached the file yet is a
        // digest of nothing anybody can read back.
        sink.flush(start_ns)?;
        drop(sink);

        let digests = sync_grain_files(&dir)?;
        let sidecar = Sidecar {
            object_key: id.key.clone(),
            object_sha256: id.sha256.clone(),
            derivation,
            start_ns,
            trailer,
            digests,
        };
        write_sidecar(&dir, &sidecar)?;

        self.windows.insert(
            name.to_owned(),
            Window {
                bytes: tree_bytes(&dir),
                dir,
                start_ns,
                id,
                sidecar,
                in_flight: false,
                entry_owed: false,
            },
        );
        Ok(())
    }

    /// The oldest window that is due to be posted, read off the disk and marked
    /// so that nothing offers it again.
    ///
    /// Due means neither in flight nor already in the store — a window whose
    /// rows landed and whose ledger entry could not be written owes an entry and
    /// not an insert, and offering it here would be a second insert of rows that
    /// are already there and a loop that never ends.
    ///
    /// Oldest by the start stamp in the window key, which orders windows across
    /// runs where a per-run sequence number cannot — and in-order posting is
    /// what keeps an era anchor certain when the evidence for it is there.
    ///
    /// `None` when every window on disk is already in flight, or there are none.
    ///
    /// # Errors
    ///
    /// A window that will not load is discarded, counted and named *before* this
    /// returns, so a caller that asks again gets the next one. One damaged
    /// directory must not stop a spool from draining.
    pub fn take_oldest(&mut self) -> Option<Result<InFlightWindow, SpoolError>> {
        let name = self
            .windows
            .iter()
            .find(|(_, w)| !w.in_flight && !w.entry_owed)
            .map(|(name, _)| name.clone())?;
        match self.load(&name) {
            Ok(batch) => {
                let window = self.windows.get_mut(&name)?;
                window.in_flight = true;
                Some(Ok(InFlightWindow {
                    id: window.id.clone(),
                    batch,
                }))
            }
            Err(e) => {
                let named = SpoolError::Unclosed {
                    window: name.clone(),
                    reason: e.to_string(),
                };
                self.discard(&name, e);
                Some(Err(named))
            }
        }
    }

    /// Writes a ledger entry for every window whose rows are now durable, and
    /// then deletes it.
    ///
    /// **The order is the whole point: the rows land, then the entry is written,
    /// then the directory goes.** An entry written when the sink merely
    /// *accepted* the rows marks a window loaded whose rows are still in the
    /// sink's memory, and a crash then loses them with nothing recording that it
    /// did.
    ///
    /// A list and never one id, because a sink that coalesces lands earlier
    /// windows together with the current one: the insert that carried window *c*
    /// is the insert that made *a* and *b* durable, and a caller that could only
    /// report *c* would leave the other two on disk for ever.
    ///
    /// **Each entry is attempted independently and none of them is a `?`.** The
    /// insert is over and the sink has forgotten these rows, so the only
    /// question left is which windows the ledger gets to hear about — and a
    /// return on the first failure would leave the rest marked in flight for rows
    /// nobody holds any more, which is a window skipped on every later pass until
    /// the process restarts.
    ///
    /// **Call it once a pass, including a pass where nothing landed.** An entry
    /// that could not be written is retried here and nowhere else: the rows are
    /// already in the store, so nothing will hand this spool that window's id a
    /// second time.
    pub fn record_landed(&mut self, landed: &[ObjectId], ledger: &mut Ledger) -> Recorded {
        let mut out = Recorded::default();
        let owed: Vec<String> = self
            .windows
            .iter()
            .filter(|(_, w)| w.entry_owed)
            .map(|(name, _)| name.clone())
            .collect();
        for name in owed {
            self.write_entry(&name, ledger, &mut out);
        }
        for id in landed {
            let Some(name) = self
                .windows
                .iter()
                .find(|(_, w)| &w.id == id)
                .map(|(name, _)| name.clone())
            else {
                // The sink named a window this spool is not holding. It cannot
                // happen — a sink only ever lands what it was given — and if it
                // did, an entry with no trailer behind it would put the next
                // window's boundary check on evidence nobody derived.
                continue;
            };
            let Some(window) = self.windows.get_mut(&name) else {
                continue;
            };
            // Whatever the ledger says, these rows have gone: a window left in
            // flight over rows the sink no longer holds is one nothing will ever
            // post again.
            window.in_flight = false;
            self.write_entry(&name, ledger, &mut out);
        }
        out
    }

    /// Writes one window's ledger entry and then deletes it.
    ///
    /// The two happen here and only here, in this order, so that the rule has
    /// one implementation: a window is deleted because its entry is written, and
    /// never the other way round.
    fn write_entry(&mut self, name: &str, ledger: &mut Ledger, out: &mut Recorded) {
        let Some(window) = self.windows.get_mut(name) else {
            return;
        };
        let entry = Entry {
            object_key: window.id.key.clone(),
            object_sha256: window.id.sha256.clone(),
            // Read here rather than taken as a parameter, exactly as the loader
            // reads it: the field is when this process wrote the rows, which is
            // not when the traffic passed and is not a quantity any caller has a
            // better answer for.
            loaded_at_ns: now_unix_nanos(),
            trailer: window.sidecar.trailer.clone(),
        };
        match ledger.record(entry) {
            Ok(()) => {
                out.recorded += 1;
                if let Err(message) = self.delete(name) {
                    out.failures.push(message);
                }
            }
            // The rows are in the store and nothing records it. The window stays
            // and the entry is owed — deleting it here would give up the trailer
            // the next era anchor needs, and re-posting it would insert rows that
            // are already there.
            Err(e) => {
                window.entry_owed = true;
                out.failures.push(e.to_string());
            }
        }
    }

    /// Puts every window in flight back, to be posted again.
    ///
    /// Called when the sink refuses: [`RowSink::write_batch`] documents that a
    /// failure takes every unit the sink was holding with it, so none of them is
    /// loaded and all of them are due again. Also the way out of a posting stage
    /// that panicked mid-insert — the windows it was holding are on disk, and
    /// only this mark says otherwise.
    ///
    /// A window that owes a ledger entry is not released: its rows are in the
    /// store already, so what it needs is
    /// [`record_landed`](Self::record_landed) and not a second insert.
    ///
    /// [`RowSink::write_batch`]: dz_recorder_rows::RowSink::write_batch
    pub fn release(&mut self) {
        for window in self.windows.values_mut() {
            window.in_flight = false;
        }
    }

    /// Drops every window the ledger already accounts for, without posting it
    /// again.
    ///
    /// Called once, after [`open`](Self::open) and before the first
    /// [`take_oldest`](Self::take_oldest). A crash between a ledger entry and the
    /// delete that follows it leaves a directory whose rows are already in the
    /// store, and re-posting it is an insert for rows that are already there.
    /// `ReplacingMergeTree` would make that a replace rather than a duplication,
    /// so this is not what keeps the store correct — but the ledger's whole
    /// meaning is *these rows are in the store*, and a spool that consulted it
    /// nowhere would be asking the question and throwing the answer away.
    ///
    /// Returns how many, which is a number that should be `0` on every start
    /// that followed a clean shutdown.
    pub fn forget_loaded(&mut self, ledger: &Ledger) -> u64 {
        let names: Vec<String> = self
            .windows
            .iter()
            .filter(|(_, w)| ledger.is_loaded(&w.id.key, &w.id.sha256))
            .map(|(name, _)| name.clone())
            .collect();
        let dropped = names.len() as u64;
        for name in names {
            let _ = self.delete(&name);
        }
        dropped
    }

    /// What eviction governs: every byte of every window directory.
    ///
    /// The sidecars are in here with the rows. They are small and they are what
    /// makes a window loadable, so a budget that excluded them would be a budget
    /// over part of its own disk.
    #[must_use]
    pub fn bytes(&self) -> u64 {
        self.windows.values().map(|w| w.bytes).sum()
    }

    /// Windows on disk that the ledger does not yet record.
    ///
    /// A window the sink is holding is one of these, deliberately: rows in
    /// memory are not loaded, and a lag that counted them as loaded would report
    /// a recorder caught up while its last insert sat unsent.
    #[must_use]
    pub fn windows(&self) -> usize {
        self.windows.len()
    }

    /// How far behind the oldest window on disk is, in seconds, and `0` when
    /// there is none.
    ///
    /// **This is the number to alert on.** A full budget evicts on every window
    /// at steady state by design, so the eviction counter rises whether or not
    /// anything is wrong; one window older than the eviction horizon is history
    /// already given up. `0` rather than absent when the spool is empty, so that
    /// a rule written over it does not silence itself on the healthy case.
    ///
    /// From the window's start stamp rather than a file's modification time: the
    /// stamp is a property of the traffic and the mtime is a property of the
    /// copy, and it is the same clock the window key is built from.
    #[must_use]
    pub fn oldest_age_seconds(&self, now_ns: u64) -> u64 {
        self.windows
            .values()
            .next()
            .map_or(0, |w| now_ns.saturating_sub(w.start_ns) / 1_000_000_000)
    }

    /// Windows given up under the budget. **Never the number to alert on** — see
    /// [`oldest_age_seconds`](Self::oldest_age_seconds).
    #[must_use]
    pub const fn windows_evicted_total(&self) -> u64 {
        self.windows_evicted_total
    }

    #[must_use]
    pub const fn bytes_evicted_total(&self) -> u64 {
        self.bytes_evicted_total
    }

    /// Windows deleted because they could not be loaded, which is a different
    /// loss from an eviction and is counted separately.
    ///
    /// An eviction is history the budget decided to give up; a discard is a
    /// window that was damaged, and one of those is a bug or a disk and the
    /// other is a configuration. A counter that added them could not say which.
    #[must_use]
    pub const fn windows_discarded_total(&self) -> u64 {
        self.windows_discarded_total
    }

    /// Bytes in the spool directory that eviction will not touch, because this
    /// module did not write them.
    ///
    /// Counted so that a disk this spool cannot bound is visible as such, and
    /// left alone because a budget is not a licence to delete somebody else's
    /// data. It is also why the ledger may not live in here.
    #[must_use]
    pub const fn unreclaimable_bytes(&self) -> u64 {
        self.unreclaimable_bytes
    }

    /// Takes the discards accumulated since the last call, for a caller that
    /// logs them.
    ///
    /// Drained rather than read, so that a window is named once and the list
    /// does not grow for the life of the process.
    pub fn take_discarded(&mut self) -> Vec<SpoolError> {
        std::mem::take(&mut self.discarded)
    }

    /// Reads one window back into the batch the sink will be given.
    ///
    /// **Every grain is verified before any grain is parsed.** A window is one
    /// unit of idempotence, and one whose datagram rows loaded while its gap
    /// rows did not is a window that reads as a clean feed.
    fn load(&self, name: &str) -> Result<RowBatch, SpoolError> {
        let window = self.windows.get(name).ok_or_else(|| SpoolError::Unclosed {
            window: name.to_owned(),
            reason: "no longer on disk".to_owned(),
        })?;
        let sidecar = &window.sidecar;

        // Keyed on the grain rather than indexed by it, because the file sink's
        // own index is private to its crate and a second copy of that mapping
        // here is one more place two grains can end up sharing a slot.
        let mut bytes: BTreeMap<Grain, Vec<u8>> = BTreeMap::new();
        for grain in Grain::ALL {
            let path = FileSink::path_in(&window.dir, grain);
            let recorded = sidecar.digests.iter().find(|d| d.grain == grain.table());
            let on_disk = match fs::read(&path) {
                Ok(content) => Some(content),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(source) => return Err(SpoolError::Io { path, source }),
            };
            match (on_disk, recorded) {
                (None, None) => {}
                (Some(_), None) => {
                    return Err(SpoolError::Unrecorded {
                        window: name.to_owned(),
                        grain,
                    })
                }
                (None, Some(_)) => {
                    return Err(SpoolError::Missing {
                        window: name.to_owned(),
                        grain,
                    })
                }
                (Some(content), Some(digest)) => {
                    if content.len() as u64 != digest.bytes || sha256_hex(&content) != digest.sha256
                    {
                        return Err(SpoolError::Digest {
                            window: name.to_owned(),
                            grain,
                        });
                    }
                    bytes.insert(grain, content);
                }
            }
        }

        let rows = |grain: Grain| bytes.get(&grain).map_or(&[][..], Vec::as_slice);
        Ok(RowBatch {
            object_key: sidecar.object_key.clone(),
            object_sha256: sidecar.object_sha256.clone(),
            derivation: sidecar.derivation,
            datagram: parse_rows::<Datagram>(name, Grain::Datagram, rows(Grain::Datagram))?,
            era: parse_rows::<Era>(name, Grain::Era, rows(Grain::Era))?,
            segment_coverage: parse_rows::<SegmentCoverage>(
                name,
                Grain::SegmentCoverage,
                rows(Grain::SegmentCoverage),
            )?,
            sequence_gap: parse_rows::<SequenceGap>(
                name,
                Grain::SequenceGap,
                rows(Grain::SequenceGap),
            )?,
            conformance_finding: parse_rows::<ConformanceFinding>(
                name,
                Grain::ConformanceFinding,
                rows(Grain::ConformanceFinding),
            )?,
            event: parse_rows::<Event>(name, Grain::Event, rows(Grain::Event))?,
            instrument: parse_rows::<Instrument>(name, Grain::Instrument, rows(Grain::Instrument))?,
            book_top: parse_rows::<BookTop>(name, Grain::BookTop, rows(Grain::BookTop))?,
        })
    }

    /// Deletes a window whose rows the ledger now accounts for.
    ///
    /// A directory that will not delete stops being a window whatever else
    /// happens: its rows are in the store, so leaving it counted as unposted
    /// would hold [`oldest_age_seconds`](Self::oldest_age_seconds) above zero for
    /// ever over a window nothing is waiting for — a lag alert that can never
    /// clear. Its bytes move to
    /// [`unreclaimable_bytes`](Self::unreclaimable_bytes), which is where bytes
    /// this module can no longer reach belong.
    fn delete(&mut self, name: &str) -> Result<(), String> {
        let Some(window) = self.windows.remove(name) else {
            return Ok(());
        };
        match remove_tree(&window.dir) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.unreclaimable_bytes = self.unreclaimable_bytes.saturating_add(window.bytes);
                Err(e.to_string())
            }
        }
    }

    /// Gives up a window that could not be loaded: deletes it, counts it, and
    /// keeps the reason so it can be named.
    ///
    /// Never loaded in part. Rows nothing vouched for are worse than no rows:
    /// they are indistinguishable in the store from rows that were derived
    /// whole.
    fn discard(&mut self, name: &str, reason: SpoolError) {
        let dir = self.dir.join(name);
        // Adopted at `open` before it is in the map, so its size may have to be
        // read off the disk.
        let bytes = self
            .windows
            .remove(name)
            .map_or_else(|| tree_bytes(&dir), |w| w.bytes);
        if remove_tree(&dir).is_err() {
            self.unreclaimable_bytes = self.unreclaimable_bytes.saturating_add(bytes);
        }
        self.windows_discarded_total += 1;
        self.discarded.push(reason);
    }

    /// Deletes oldest-first until the spool is inside its budget.
    ///
    /// A window the sink is holding is evicted like any other. It is the oldest,
    /// which is the rule, and the cost is one ledger entry that will not be
    /// written for rows that may yet land — which leaves the next window's era
    /// anchor uncertain and says so, rather than claiming a continuity nothing
    /// on this host can still evidence.
    ///
    /// **One window is the exception, and it is the one whose rows have already
    /// landed.** A window owing a ledger entry is not rows that may yet land: the
    /// rows *are* in the store, and its directory is the only thing left that can
    /// record that they are, carrying the trailer the next era anchor is checked
    /// against. So it goes last, preferred against while anything else remains —
    /// and taken when nothing else does, because a budget that stopped bounding
    /// the disk in order to protect an entry would trade a bounded backlog for an
    /// unbounded one. `in_flight` is deliberately not in the preference: those
    /// rows may yet land and no entry has been earned, which is the paragraph
    /// above.
    ///
    /// A window larger than the whole budget is evicted the moment it is
    /// written, which is a budget too small for one window and shows as an
    /// eviction against every window rather than as a spool that quietly kept
    /// nothing.
    ///
    /// **An eviction that cannot delete stops the pass, and stops being a
    /// window.** The pass stops because retrying the same undeletable directory
    /// for ever would delete the whole spool behind it and leave the disk no
    /// emptier. It stops being a window because leaving it in the map holds
    /// [`bytes`](Self::bytes) over the budget for ever and makes it the choice on
    /// every later pass — a budget that stops bounding the disk from the first
    /// failure on. Its bytes move to
    /// [`unreclaimable_bytes`](Self::unreclaimable_bytes), where bytes this
    /// module can no longer reach belong, and the next pass tries the next
    /// window.
    fn enforce(&mut self) {
        while self.bytes() > self.budget_bytes {
            let Some(name) = self.oldest_evictable() else {
                return;
            };
            let bytes = self.windows.get(&name).map_or(0, |w| w.bytes);
            let undeletable = self.delete(&name).is_err();
            self.windows_evicted_total += 1;
            self.bytes_evicted_total = self.bytes_evicted_total.saturating_add(bytes);
            if undeletable {
                return;
            }
        }
    }

    /// The window the budget takes next: the oldest whose rows are not already
    /// in the store, and only then the oldest of those that are.
    fn oldest_evictable(&self) -> Option<String> {
        self.windows
            .iter()
            .find(|(_, w)| !w.entry_owed)
            .or_else(|| self.windows.iter().next())
            .map(|(name, _)| name.clone())
    }

    /// Reads back what a previous run left under one window name.
    fn adopt(&self, name: &str, start_ns: u64) -> Result<Window, SpoolError> {
        let dir = self.dir.join(name);
        if !dir.is_dir() {
            return Err(SpoolError::Unclosed {
                window: name.to_owned(),
                reason: "not a directory".to_owned(),
            });
        }
        let path = dir.join(CLOSED);
        let text = fs::read_to_string(&path).map_err(|e| SpoolError::Unclosed {
            window: name.to_owned(),
            reason: format!("{CLOSED}: {e}"),
        })?;
        let sidecar: Sidecar = serde_json::from_str(&text).map_err(|e| SpoolError::Unclosed {
            window: name.to_owned(),
            reason: format!("{CLOSED}: {e}"),
        })?;
        Ok(Window {
            bytes: tree_bytes(&dir),
            dir,
            // From the name and not from the sidecar: the name is what the map
            // is ordered by, so a disagreement between the two would be a window
            // consumed out of the order its own directory listing shows.
            start_ns,
            id: ObjectId {
                key: sidecar.object_key.clone(),
                sha256: sidecar.object_sha256.clone(),
            },
            sidecar,
            in_flight: false,
            entry_owed: false,
        })
    }
}

/// The directory one window is written into.
///
/// The stamp first, zero-padded, so the names sort into consumption order. The
/// key's digest after it, because two windows can share a start stamp and
/// because a window key is a path — putting it in a file name unedited would
/// have the spool write outside its own directory.
#[must_use]
fn window_name(start_ns: u64, object_key: &str) -> String {
    let digest = sha256_hex(object_key.as_bytes());
    format!(
        "{WINDOW_PREFIX}{start_ns:0width$}-{}",
        &digest[..16],
        width = STAMP_WIDTH
    )
}

/// The start stamp in a window directory's name, if the name is one.
#[must_use]
fn start_ns_in(name: &str) -> Option<u64> {
    name.strip_prefix(WINDOW_PREFIX)?
        .split('-')
        .next()?
        .parse()
        .ok()
}

/// Hashes and syncs every grain file the sink wrote, and reports what it found.
///
/// The digest is taken over the bytes as they now sit on the disk rather than
/// over the rows in memory, so it is a claim about what a later run can actually
/// read back.
fn sync_grain_files(dir: &Path) -> Result<Vec<GrainDigest>, SpoolError> {
    let mut digests = Vec::new();
    for grain in Grain::ALL {
        let path = FileSink::path_in(dir, grain);
        let content = match fs::read(&path) {
            Ok(content) => content,
            // A grain that produced no rows leaves no file, which is the file
            // sink's own rule and is why the sidecar lists what exists rather
            // than all eight.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(source) => return Err(SpoolError::Io { path, source }),
        };
        OpenOptions::new()
            .append(true)
            .open(&path)
            .and_then(|f| f.sync_all())
            .map_err(|source| SpoolError::Io {
                path: path.clone(),
                source,
            })?;
        digests.push(GrainDigest {
            grain: grain.table().to_owned(),
            sha256: sha256_hex(&content),
            bytes: content.len() as u64,
        });
    }
    Ok(digests)
}

/// Writes the sidecar and makes the whole window durable.
///
/// Written under a working name and renamed, so a crash leaves either a window
/// with a sidecar or a window without one and never a sidecar that describes
/// half a window. The directory itself is synced last: without that the rename
/// is durable in a page cache and gone after a power loss, which is a window
/// whose rows are on the disk and whose commit point is not.
fn write_sidecar(dir: &Path, sidecar: &Sidecar) -> Result<(), SpoolError> {
    let temp = dir.join(CLOSING);
    let io = |path: &Path| {
        let path = path.to_path_buf();
        move |source| SpoolError::Io { path, source }
    };
    let text = serde_json::to_vec(sidecar).map_err(|e| SpoolError::Io {
        path: temp.clone(),
        source: std::io::Error::other(e),
    })?;
    let mut file = File::create(&temp).map_err(io(&temp))?;
    file.write_all(&text).map_err(io(&temp))?;
    file.sync_all().map_err(io(&temp))?;
    drop(file);
    let closed = dir.join(CLOSED);
    fs::rename(&temp, &closed).map_err(io(&closed))?;
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(io(dir))?;
    Ok(())
}

fn parse_rows<T: DeserializeOwned>(
    window: &str,
    grain: Grain,
    bytes: &[u8],
) -> Result<Vec<T>, SpoolError> {
    let text = std::str::from_utf8(bytes).map_err(|_| SpoolError::Digest {
        window: window.to_owned(),
        grain,
    })?;
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line).map_err(|source| SpoolError::Unreadable {
                window: window.to_owned(),
                grain,
                source,
            })
        })
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let digest = Sha256::digest(bytes);
    digest.iter().fold(String::with_capacity(64), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

/// Every byte under a path, and `0` for anything that cannot be measured.
///
/// A path that will not stat is reported as nothing rather than refused: the
/// budget is a bound on a directory, and one unreadable entry must not stop the
/// other windows from being counted.
fn tree_bytes(path: &Path) -> u64 {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return 0;
    };
    if !meta.is_dir() {
        return meta.len();
    }
    let Ok(entries) = fs::read_dir(path) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|e| tree_bytes(&e.path()))
        .sum()
}

fn remove_tree(path: &Path) -> Result<(), SpoolError> {
    match fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(SpoolError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}
