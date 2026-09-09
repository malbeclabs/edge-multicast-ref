//! The spool's failures, which are the part worth testing.
//!
//! The happy path is one window in and one window out and it proves nothing:
//! every reason this module exists is a case where something went wrong — a
//! destination that is down, a budget that is full, a process that died between
//! writing a window and posting it, a file that came back damaged. Each of those
//! is a test here.
//!
//! No socket, no privileges and no server. The destination is a fake sink this
//! file writes, and it reads the ledger on every call so that the order of the
//! two — rows first, entry second — is something a test can assert rather than
//! something a reader has to take on trust.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

use dz_recorder_inline::spool::{Spool, SpoolError, CLOSED};
use dz_recorder_load::ledger::Ledger;
use dz_recorder_rows::{
    Accepted, Derivation, Era, FileSink, Grain, Landed, Nanos, ObjectId, RowBatch, RowSink,
    RowSinkError, SegmentTrailer, Written,
};

const SECOND: u64 = 1_000_000_000;

/// What the fake sink saw, in the order it saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Seen {
    /// One window's rows, and how many entries the ledger held at that moment.
    /// The second number is the assertion: a spool that recorded before it wrote
    /// would show a window's own entry already there.
    Rows {
        key: String,
        ledger_entries: usize,
    },
    Posted {
        keys: Vec<String>,
    },
}

/// A destination the test drives.
///
/// It refuses on demand, and it holds rows on demand — the column store's sink
/// coalesces across units, so a spool that treated acceptance as landing would
/// pass every test written against a sink that always lands.
struct FakeSink {
    ledger_path: PathBuf,
    seen: Vec<Seen>,
    refuse: bool,
    /// Take the rows and do not send them, the way a coalescing sink does.
    hold: bool,
    /// Send what is held on the next `post_if_due`.
    due: bool,
    held: Vec<ObjectId>,
}

impl FakeSink {
    fn new(ledger_path: &Path) -> Self {
        Self {
            ledger_path: ledger_path.to_path_buf(),
            seen: Vec::new(),
            refuse: false,
            hold: false,
            due: true,
            held: Vec::new(),
        }
    }

    fn rows_written(&self) -> Vec<String> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Rows { key, .. } => Some(key.clone()),
                Seen::Posted { .. } => None,
            })
            .collect()
    }
}

impl RowSink for FakeSink {
    fn write_batch(&mut self, rows: RowBatch, _now_ns: u64) -> Result<Accepted, RowSinkError> {
        if self.refuse {
            return Err(RowSinkError::Rejected {
                object_key: rows.object_key.clone(),
                attempts: 1,
                last: "the destination is down".to_owned(),
            });
        }
        self.seen.push(Seen::Rows {
            key: rows.object_key.clone(),
            ledger_entries: ledger_entries(&self.ledger_path),
        });
        let id = ObjectId::of(&rows);
        let accepted = Written::of(&rows, 0);
        if self.hold {
            self.held.push(id);
            return Ok(Accepted {
                accepted,
                landed: Vec::new(),
                bytes_posted: 0,
            });
        }
        Ok(Accepted {
            accepted,
            landed: vec![id],
            bytes_posted: 1,
        })
    }

    fn post_if_due(&mut self, now_ns: u64) -> Result<Landed, RowSinkError> {
        if !self.due {
            return Ok(Landed::default());
        }
        self.flush(now_ns)
    }

    fn flush(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
        let objects = std::mem::take(&mut self.held);
        if objects.is_empty() {
            return Ok(Landed::default());
        }
        self.seen.push(Seen::Posted {
            keys: objects.iter().map(|o| o.key.clone()).collect(),
        });
        Ok(Landed {
            objects,
            bytes_posted: 1,
        })
    }
}

fn ledger_entries(path: &Path) -> usize {
    std::fs::read_to_string(path).map_or(0, |t| t.lines().filter(|l| !l.trim().is_empty()).count())
}

/// One window's rows: `count` era rows, which is enough to make a file with
/// bytes in it that a digest can be wrong about.
fn batch(key: &str, count: usize) -> RowBatch {
    let mut rows = RowBatch {
        object_key: key.to_owned(),
        // Empty, as an inline window's manifest writes it: no datagrams were
        // kept, so nothing was hashed, and an invented digest would be a claim
        // that something was verified.
        object_sha256: String::new(),
        derivation: Derivation::Live,
        ..RowBatch::default()
    };
    for n in 0..count {
        rows.era.push(Era {
            site: "site".to_owned(),
            recorder: "recorder".to_owned(),
            feed: "feed".to_owned(),
            source_addr: Ipv4Addr::new(10, 0, 0, 1),
            channel_id: 1,
            dst_port: 4000,
            anchor_ts: Nanos(n as u64),
            anchor_seq: n as u64,
            reset_count: 0,
            segment_seq: 0,
            anchor_certain: 1,
            continuation: 0,
            object_key: key.to_owned(),
            object_sha256: String::new(),
            derivation: Derivation::Live,
        });
    }
    rows
}

fn trailer(segment_seq: u64) -> SegmentTrailer {
    SegmentTrailer {
        segment_seq,
        interface_drop_total: 0,
        instances: Vec::new(),
    }
}

/// The window directories on the disk, in name order, which is the order they
/// are consumed in.
fn window_dirs(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("the spool directory is there")
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("window-"))
        .collect();
    names.sort();
    names
}

#[test]
fn a_destination_that_refuses_leaves_the_window_on_disk_and_the_ledger_empty() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let ledger_path = root.path().join("ledger.jsonl");
    let spool_dir = root.path().join("spool");
    let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
    let mut ledger = Ledger::open(&ledger_path).expect("a ledger");
    let mut sink = FakeSink::new(&ledger_path);
    sink.refuse = true;

    spool
        .store(10 * SECOND, batch("window-a", 3), trailer(0))
        .expect("the window reaches the disk");

    let refused = spool
        .post(&mut sink, &mut ledger, 11 * SECOND)
        .expect_err("a destination that is down is an error");
    assert!(
        matches!(refused, SpoolError::Sink(_)),
        "the destination's refusal is what comes back: {refused}"
    );

    // The rows are still where a later pass can find them, and nothing claims
    // they are in the store.
    assert_eq!(spool.windows(), 1);
    assert_eq!(window_dirs(&spool_dir).len(), 1);
    assert_eq!(ledger.entries(), 0);
    assert_eq!(ledger_entries(&ledger_path), 0);
    assert!(spool.bytes() > 0, "the window's bytes are still counted");

    // And the window is still due: a refusal must not leave it marked as
    // something the sink is holding.
    sink.refuse = false;
    let posted = spool
        .post(&mut sink, &mut ledger, 12 * SECOND)
        .expect("the destination is back");
    assert_eq!(posted.recorded, 1);
    assert_eq!(spool.windows(), 0);
}

#[test]
fn a_destination_that_recovers_lands_the_windows_oldest_first_and_records_each_after_its_rows() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let ledger_path = root.path().join("ledger.jsonl");
    let mut spool = Spool::open(root.path().join("spool"), 1 << 20).expect("a spool");
    let mut ledger = Ledger::open(&ledger_path).expect("a ledger");
    let mut sink = FakeSink::new(&ledger_path);
    sink.refuse = true;

    // Stored out of order, so that passing this cannot be an accident of the
    // order they were handed over in.
    spool
        .store(30 * SECOND, batch("window-c", 1), trailer(2))
        .expect("the third window reaches the disk");
    spool
        .store(10 * SECOND, batch("window-a", 1), trailer(0))
        .expect("the first window reaches the disk");
    spool
        .store(20 * SECOND, batch("window-b", 1), trailer(1))
        .expect("the second window reaches the disk");
    spool
        .post(&mut sink, &mut ledger, 31 * SECOND)
        .expect_err("the destination is down");

    sink.refuse = false;
    let posted = spool
        .post(&mut sink, &mut ledger, 40 * SECOND)
        .expect("the destination is back");

    assert_eq!(posted.recorded, 3);
    assert!(posted.failures.is_empty(), "{:?}", posted.failures);
    assert_eq!(
        sink.rows_written(),
        vec!["window-a", "window-b", "window-c"],
        "oldest first, by the start stamp in the window key"
    );
    // The assertion the whole ordering rests on: when each window's rows were
    // written, the ledger held an entry for every window before it and none for
    // this one. A spool that recorded first would show 1, 2, 3.
    assert_eq!(
        sink.seen,
        vec![
            Seen::Rows {
                key: "window-a".to_owned(),
                ledger_entries: 0
            },
            Seen::Rows {
                key: "window-b".to_owned(),
                ledger_entries: 1
            },
            Seen::Rows {
                key: "window-c".to_owned(),
                ledger_entries: 2
            },
        ]
    );
    assert_eq!(ledger_entries(&ledger_path), 3);
    assert_eq!(spool.windows(), 0);
    assert_eq!(spool.bytes(), 0);
    assert_eq!(spool.oldest_age_seconds(40 * SECOND), 0);
    // The trailer travelled with the window, so a restart resumes with the
    // certainty this run had.
    assert_eq!(
        ledger.trailer().map(|t| t.segment_seq),
        Some(2),
        "the highest window's trailer is what the next adjacency check consults"
    );
}

#[test]
fn rows_the_sink_has_taken_and_not_sent_are_not_recorded_in_the_ledger() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let ledger_path = root.path().join("ledger.jsonl");
    let spool_dir = root.path().join("spool");
    let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
    let mut ledger = Ledger::open(&ledger_path).expect("a ledger");
    let mut sink = FakeSink::new(&ledger_path);
    // A sink that coalesces: it takes the rows and sends nothing.
    sink.hold = true;
    sink.due = false;

    spool
        .store(10 * SECOND, batch("window-a", 2), trailer(0))
        .expect("the window reaches the disk");
    let posted = spool
        .post(&mut sink, &mut ledger, 11 * SECOND)
        .expect("the sink took the rows");

    assert_eq!(posted.recorded, 0, "accepted is not landed");
    assert_eq!(ledger.entries(), 0);
    assert_eq!(spool.windows(), 1, "the window stays until its rows land");
    assert_eq!(window_dirs(&spool_dir).len(), 1);

    // A second pass must not hand the same rows over again while the sink is
    // still holding them.
    spool
        .post(&mut sink, &mut ledger, 12 * SECOND)
        .expect("nothing new is due");
    assert_eq!(sink.rows_written(), vec!["window-a"]);

    sink.due = true;
    let posted = spool
        .post(&mut sink, &mut ledger, 13 * SECOND)
        .expect("the insert goes out");
    assert_eq!(posted.recorded, 1);
    assert_eq!(ledger_entries(&ledger_path), 1);
    assert_eq!(spool.windows(), 0);
    assert!(window_dirs(&spool_dir).is_empty(), "the directory is gone");
    assert_eq!(
        sink.rows_written(),
        vec!["window-a"],
        "the rows were handed over once"
    );
}

#[test]
fn a_full_budget_evicts_the_oldest_window_and_the_reported_age_is_the_oldest_left() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let spool_dir = root.path().join("spool");

    // Sized from a window that is already on disk, so the budget holds two and
    // not three whatever the rows happen to serialise to.
    let mut measure = Spool::open(root.path().join("measure"), u64::MAX).expect("a spool");
    measure
        .store(0, batch("window-measure", 4), trailer(0))
        .expect("one window, to measure");
    let budget = measure.bytes() * 2 + measure.bytes() / 2;

    let mut spool = Spool::open(&spool_dir, budget).expect("a spool");
    for (n, key) in ["window-a", "window-b"].iter().enumerate() {
        spool
            .store(
                (n as u64 + 1) * 10 * SECOND,
                batch(key, 4),
                trailer(n as u64),
            )
            .expect("the window reaches the disk");
    }
    assert_eq!(spool.windows(), 2);
    assert_eq!(spool.windows_evicted_total(), 0);
    assert_eq!(
        spool.oldest_age_seconds(100 * SECOND),
        90,
        "the age is the first window's"
    );

    // The third window does not wait for room and does not refuse: the oldest
    // goes.
    spool
        .store(30 * SECOND, batch("window-c", 4), trailer(2))
        .expect("a full spool still takes the window");

    assert_eq!(spool.windows(), 2);
    assert_eq!(spool.windows_evicted_total(), 1);
    assert!(spool.bytes_evicted_total() > 0);
    assert!(spool.bytes() <= budget);
    assert_eq!(
        window_dirs(&spool_dir).len(),
        2,
        "the evicted window's directory is gone from the disk too"
    );
    assert_eq!(
        spool.oldest_age_seconds(100 * SECOND),
        80,
        "the age comes from the oldest window that is left, not the one evicted"
    );
}

#[test]
fn a_spool_abandoned_without_posting_is_replayed_on_the_next_open_and_lands() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let ledger_path = root.path().join("ledger.jsonl");
    let spool_dir = root.path().join("spool");

    // The run that died: two windows on disk, nothing posted, no ledger.
    {
        let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
        spool
            .store(10 * SECOND, batch("window-a", 2), trailer(0))
            .expect("the window reaches the disk");
        spool
            .store(20 * SECOND, batch("window-b", 2), trailer(1))
            .expect("the window reaches the disk");
    }
    assert_eq!(ledger_entries(&ledger_path), 0);

    // The next run adopts them before it derives anything of its own.
    let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
    assert_eq!(spool.windows(), 2, "the previous run's windows are adopted");
    assert_eq!(
        spool.oldest_age_seconds(30 * SECOND),
        20,
        "the age survives the restart, because it comes from the window key"
    );

    let mut ledger = Ledger::open(&ledger_path).expect("a ledger");
    let mut sink = FakeSink::new(&ledger_path);
    let posted = spool
        .post(&mut sink, &mut ledger, 30 * SECOND)
        .expect("the destination is up");

    assert_eq!(posted.recorded, 2);
    assert_eq!(sink.rows_written(), vec!["window-a", "window-b"]);
    assert_eq!(
        posted.written.rows(Grain::Era),
        4,
        "the rows came back whole"
    );
    assert_eq!(spool.windows(), 0);
    assert!(window_dirs(&spool_dir).is_empty());
}

#[test]
fn a_corrupted_grain_file_is_discarded_by_name_and_the_windows_around_it_still_load() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let ledger_path = root.path().join("ledger.jsonl");
    let spool_dir = root.path().join("spool");

    {
        let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
        for (n, key) in ["window-a", "window-b", "window-c"].iter().enumerate() {
            spool
                .store(
                    (n as u64 + 1) * 10 * SECOND,
                    batch(key, 4),
                    trailer(n as u64),
                )
                .expect("the window reaches the disk");
        }
    }

    // The middle window's rows are torn, as a crash mid-write leaves them.
    let names = window_dirs(&spool_dir);
    assert_eq!(names.len(), 3);
    let damaged = names[1].clone();
    let torn = FileSink::path_in(&spool_dir.join(&damaged), Grain::Era);
    let mut content = std::fs::read(&torn).expect("the grain file is there");
    content.truncate(content.len() / 2);
    std::fs::write(&torn, &content).expect("the truncation is written");
    // Its sidecar is untouched, so what the load finds is a disagreement
    // between the bytes and the digest and not a missing window.
    assert!(spool_dir.join(&damaged).join(CLOSED).is_file());

    let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
    let mut ledger = Ledger::open(&ledger_path).expect("a ledger");
    let mut sink = FakeSink::new(&ledger_path);
    let posted = spool
        .post(&mut sink, &mut ledger, 100 * SECOND)
        .expect("one damaged window is not a failed pass");

    assert_eq!(posted.recorded, 2, "the windows around it still load");
    assert_eq!(
        sink.rows_written(),
        vec!["window-a", "window-c"],
        "not one row of the damaged window was inserted"
    );
    assert_eq!(spool.windows_discarded_total(), 1);
    assert!(
        !spool_dir.join(&damaged).exists(),
        "a window that cannot be loaded is deleted, not left to be retried for ever"
    );

    let discarded = spool.take_discarded();
    assert_eq!(discarded.len(), 1);
    let named = discarded[0].to_string();
    assert!(
        named.contains(&damaged) && named.contains("era"),
        "the error names the window and the grain: {named}"
    );
    assert!(
        spool.take_discarded().is_empty(),
        "a window is named once, not for the life of the process"
    );
}

#[test]
fn a_window_whose_close_never_finished_is_discarded_rather_than_loaded_in_part() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let spool_dir = root.path().join("spool");
    {
        let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
        spool
            .store(10 * SECOND, batch("window-a", 2), trailer(0))
            .expect("the window reaches the disk");
        spool
            .store(20 * SECOND, batch("window-b", 2), trailer(1))
            .expect("the window reaches the disk");
    }

    // The rows reached the disk and the sidecar did not, which is what a crash
    // between the two leaves.
    let names = window_dirs(&spool_dir);
    std::fs::remove_file(spool_dir.join(&names[0]).join(CLOSED)).expect("the sidecar goes");

    let mut spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");
    assert_eq!(spool.windows(), 1, "only the window that was closed");
    assert_eq!(spool.windows_discarded_total(), 1);
    assert!(!spool_dir.join(&names[0]).exists());
    let named = spool.take_discarded()[0].to_string();
    assert!(
        named.contains(&names[0]),
        "the error names the window: {named}"
    );
}

#[test]
fn a_name_the_spool_did_not_write_is_counted_and_left_alone() {
    let root = tempfile::tempdir().expect("a temporary directory");
    let spool_dir = root.path().join("spool");
    std::fs::create_dir_all(&spool_dir).expect("the spool directory");
    let theirs = spool_dir.join("somebody-elses.db");
    std::fs::write(&theirs, b"not ours to delete").expect("their file is written");

    let spool = Spool::open(&spool_dir, 1 << 20).expect("a spool");

    assert_eq!(spool.windows(), 0);
    assert_eq!(spool.unreclaimable_bytes(), 18);
    assert_eq!(spool.windows_discarded_total(), 0);
    assert!(
        theirs.is_file(),
        "a budget is not a licence to delete somebody else's data"
    );
}
