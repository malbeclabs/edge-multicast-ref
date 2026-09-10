//! The three stages wired together: what reaches the store, and what happens
//! when a stage does not survive its own bug.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dz_recorder_core::{CaptureDropScope, OwnedDatagram, RecorderIdentity};
use dz_recorder_inline::pipeline::{start, DerivationConfig};
use dz_recorder_inline::ring::{ring, Offered};
use dz_recorder_inline::spool::Spool;
use dz_recorder_inline::window::WindowBound;
use dz_recorder_load::Ledger;
use dz_recorder_replay::synthetic::SyntheticPublisher;
use dz_recorder_rows::{Accepted, Landed, ObjectId, RowBatch, RowSink, RowSinkError, Written};
use tempfile::TempDir;

/// A destination that records what it was given, and can be told to panic.
///
/// Shared through an `Arc` so the test can read it after the pipeline has taken
/// ownership of its half.
#[derive(Debug, Default)]
struct Store {
    landed_keys: Vec<String>,
    datagram_rows: usize,
    /// Every batch, kept whole rather than counted, so a test can ask what a
    /// row says and not only how many there were.
    batches: Vec<RowBatch>,
    /// Writes remaining before this one panics. `None` never panics.
    panic_after: Option<usize>,
}

#[derive(Clone)]
struct FakeSink(Arc<Mutex<Store>>);

impl RowSink for FakeSink {
    fn write_batch(&mut self, rows: RowBatch, _now_ns: u64) -> Result<Accepted, RowSinkError> {
        let mut store = self.0.lock().expect("the store is not poisoned");
        if let Some(remaining) = store.panic_after.as_mut() {
            if *remaining == 0 {
                store.panic_after = None;
                drop(store);
                panic!("the destination client has a bug");
            }
            *remaining -= 1;
        }
        store.datagram_rows += rows.datagram.len();
        store.landed_keys.push(rows.object_key.clone());
        store.batches.push(rows.clone());
        let accepted = Written::of(&rows, 0);
        Ok(Accepted {
            accepted,
            landed: vec![ObjectId::of(&rows)],
            bytes_posted: 0,
        })
    }

    fn post_if_due(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
        Ok(Landed::default())
    }

    fn flush(&mut self, _now_ns: u64) -> Result<Landed, RowSinkError> {
        Ok(Landed::default())
    }
}

struct Fixture {
    _dir: TempDir,
    spool: Spool,
    ledger: Ledger,
    ledger_path: std::path::PathBuf,
}

fn fixture() -> Fixture {
    let dir = TempDir::new().expect("a temporary directory");
    let spool_dir = dir.path().join("spool");
    // Beside the spool and never inside it: a file the budget cannot classify
    // is a file eviction cannot reach.
    let ledger_path = dir.path().join("ledger.jsonl");
    let spool = Spool::open(&spool_dir, 64 * 1024 * 1024).expect("the spool opens");
    let ledger = Ledger::open(&ledger_path).expect("the ledger opens");
    Fixture {
        _dir: dir,
        spool,
        ledger,
        ledger_path,
    }
}

fn config() -> DerivationConfig {
    DerivationConfig {
        identity: RecorderIdentity {
            site: "site-1".to_owned(),
            recorder: "recorder-1".to_owned(),
            env: "test".to_owned(),
            build_version: "0.1.0".to_owned(),
            build_commit: "0000000".to_owned(),
            config_hash: "a".repeat(64),
        },
        feed: "top-of-book".to_owned(),
        roles_joined: Vec::new(),
        drop_scope: CaptureDropScope::PortRole,
        link_headers_captured: false,
        // One window for the whole fixture: these tests are about the wiring,
        // and the window bound has its own.
        bound: WindowBound {
            bytes: u64::MAX,
            interval: Duration::from_millis(250),
        },
    }
}

fn ledger_entries(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count()
}

/// One whole run of the pipeline over `sent`, and the batches it posted.
///
/// A run of its own each time — its own spool, its own ledger, its own window
/// sequence starting at zero — because that is what a process restart is, and
/// two runs of one recorder are what the window key has to keep apart.
fn run_over(sent: &[OwnedDatagram]) -> Vec<RowBatch> {
    let store = Arc::new(Mutex::new(Store::default()));
    let fixture = fixture();
    let (mut tx, rx) = ring(256);
    let mut cfg = config();
    // Far longer than the run, so the only thing that closes the window is the
    // capture ending: one window for the whole stream.
    cfg.bound.interval = Duration::from_secs(3_600);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        cfg,
    );
    for dg in sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    pipeline.stop(tx);
    let posted = std::mem::take(&mut store.lock().expect("the store is not poisoned").batches);
    posted
}

/// The manifest describes the window the derivation walked, not an empty one.
///
/// **`recorder.segment_coverage` is a `ReplacingMergeTree` whose sort key ends
/// in `start_ts`, and `window_seq` restarts at zero on every run.** The stamp is
/// therefore the only thing in that key separating the second run's window *k*
/// from the first run's window *k*, which is why the window key carries it. A
/// manifest built before the window has been walked carries `start_ns = 0`,
/// `end_ns = 0` and no per-instance coverage at all: every window of every run
/// then shares one sort key and the second run replaces the first, and the rows
/// that would have said which datagrams were covered are not written.
#[test]
fn two_runs_of_one_recorder_do_not_describe_one_window_twice() {
    let first: Vec<OwnedDatagram> = SyntheticPublisher::clean(40).datagrams();
    // The same recorder, the same feed and the same window sequence a minute
    // later, over datagrams the first run never saw.
    let mut second = first.clone();
    for dg in &mut second {
        dg.recv_ts_ns += 60_000_000_000;
    }

    let runs = [(run_over(&first), first), (run_over(&second), second)];
    for (batches, sent) in &runs {
        let batch = batches.first().expect("the run posted its window");
        let first_ts = sent.first().expect("the stream is not empty").recv_ts_ns;
        let last_ts = sent.last().expect("the stream is not empty").recv_ts_ns;

        assert!(
            !batch.segment_coverage.is_empty(),
            "a window that walked {} datagrams describes no channel instance, so the \
             manifest it was derived against saw nothing",
            sent.len()
        );
        for row in &batch.segment_coverage {
            assert_eq!(
                row.start_ts.0, first_ts,
                "the coverage row is stamped with something other than the window's first \
                 receive timestamp"
            );
            assert_eq!(row.end_ts.0, last_ts, "and its end likewise");
        }
        assert!(
            batch.object_key.ends_with(&format!("/{first_ts}-0")),
            "the window key must carry the window's start stamp: {}",
            batch.object_key
        );
    }

    let [(one, _), (two, _)] = &runs;
    let keys_one: Vec<&str> = one.iter().map(|b| b.object_key.as_str()).collect();
    let keys_two: Vec<&str> = two.iter().map(|b| b.object_key.as_str()).collect();
    assert_ne!(
        keys_one, keys_two,
        "two runs of one recorder produced one another's window keys, so the second run's \
         rows replace the first run's"
    );
}

/// The window sequence number each posted batch carries.
fn window_sequence(batches: &[RowBatch]) -> Vec<u64> {
    batches
        .iter()
        .map(|b| {
            b.segment_coverage
                .first()
                .expect("a posted window describes what it covered")
                .segment_seq
        })
        .collect()
}

/// A quiet stretch leaves no hole in the window sequence.
///
/// A hole in `segment_seq` is how a reader learns the derivation had one, which
/// is the whole of what distinguishes a recorder that was down from a feed that
/// was quiet. A feed that goes silent closes windows on age as a matter of
/// course, so a window that saw nothing must leave the sequence where it found
/// it — otherwise a silent feed states *the derivation was down* once a window
/// bound for as long as the silence lasts.
///
/// The era anchor is the same assertion from the other end. The predecessor test
/// is `segment_seq + 1`, so a spent number would leave the window after the
/// silence two ahead of its trailer and its anchor uncertain.
#[test]
fn a_quiet_window_spends_no_window_sequence_number() {
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(20).datagrams();
    let store = Arc::new(Mutex::new(Store::default()));
    let fixture = fixture();

    let (mut tx, rx) = ring(256);
    let mut cfg = config();
    // Short, so that the silence below is several windows long rather than a
    // fraction of one.
    cfg.bound.interval = Duration::from_millis(120);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        cfg,
    );

    for dg in &sent[..10] {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    // Waited for rather than slept through. A fixed sleep is a margin against
    // this pipeline's own per-window latency and not a guarantee of one: a
    // window's deadline is set when it opens, and deriving the burst's window,
    // storing it to the spool and posting it all sit between its close and the
    // next window's open. On a host slow enough that span covers the sleep, no
    // window both opens and closes inside the silence, and the assertion below
    // fails on a precondition the fixture never established rather than on the
    // property it is about.
    //
    // Two waits, in that order, because the window this fixture needs is one
    // that closed empty *after* the burst. A wait on `windows_empty()` alone is
    // also satisfied by the window that was open when `start` returned: that
    // one closes on age an `interval` later whether or not the offers above have
    // run yet, so the very slow host this wait exists for is the host that
    // leaves it empty — and the wait would then fall through before the burst
    // had a window of its own, both bursts landing in one window with no
    // silence between them. `windows_derived()` rising is the burst's own window
    // closed and derived; only from there does a rise in `windows_empty()`
    // describe the silence rather than what preceded the burst.
    {
        let counters = pipeline.counters();
        let wait_began = Instant::now();
        let settle = |unmet: &str| {
            assert!(
                wait_began.elapsed() < Duration::from_secs(10),
                "{unmet} in 10s, which is this fixture failing to arrange itself rather \
                 than the property under test"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        while counters.windows_derived() == 0 {
            settle("the burst's own window did not close and derive");
        }
        let empty_before_silence = counters.windows_empty();
        while counters.windows_empty() == empty_before_silence {
            settle("no window closed empty after the burst's own window");
        }
    }
    for dg in &sent[10..] {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    let counters = pipeline.stop(tx);

    assert!(
        counters.windows_empty() >= 1,
        "the fixture is meant to leave a window with nothing in it"
    );

    let batches = std::mem::take(&mut store.lock().expect("the store is not poisoned").batches);
    let seqs = window_sequence(&batches);
    assert!(
        seqs.len() >= 2,
        "the fixture is meant to derive a window either side of the silence: {seqs:?}"
    );
    assert_eq!(
        seqs,
        (0..seqs.len() as u64).collect::<Vec<u64>>(),
        "a window that saw nothing spent a sequence number, so the derivation reports a \
         hole where a feed was merely quiet"
    );

    for batch in &batches[1..] {
        for row in &batch.era {
            assert_eq!(
                row.anchor_certain, 1,
                "the window after the silence cannot see its predecessor: {row:?}"
            );
        }
    }
}

/// One run of the pipeline over `sent`, spooling and recording where the run
/// after it will find them again.
///
/// The paths are the caller's, which is the whole difference from `run_over`: a
/// restart is a second process over one spool directory and one ledger, and a
/// test that gave each run its own would be testing two recorders.
fn run_at(
    spool_dir: &std::path::Path,
    ledger_path: &std::path::Path,
    sent: &[OwnedDatagram],
) -> Vec<RowBatch> {
    let store = Arc::new(Mutex::new(Store::default()));
    let spool = Spool::open(spool_dir, 64 * 1024 * 1024).expect("the spool opens");
    let ledger = Ledger::open(ledger_path).expect("the ledger opens");
    let (mut tx, rx) = ring(256);
    let mut cfg = config();
    // Far longer than the run, so the only thing that closes the window is the
    // capture ending: one window per run.
    cfg.bound.interval = Duration::from_secs(3_600);
    let pipeline = start(rx, spool, ledger, FakeSink(Arc::clone(&store)), cfg);
    for dg in sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    pipeline.stop(tx);
    let posted = std::mem::take(&mut store.lock().expect("the store is not poisoned").batches);
    posted
}

/// A restart does not anchor its first window on the ledger's trailer, and the
/// trailer is there to be read.
///
/// **This is the decision, and not an omission.** The ledger holds the trailer
/// of the last window whose rows landed, and reading it back at startup looks
/// like the fix for the first window of every run writing an uncertain era
/// anchor. It is not one. `window_seq` restarts at zero on every run and the
/// predecessor test is `segment_seq + 1`, so a trailer left by the previous run
/// precedes nothing in this one: handed to `derive` it is filtered out, and the
/// anchor is uncertain exactly as it is without it.
///
/// The only wiring that would change the answer is one that also continues the
/// window sequence across the restart, and that says *the derivation was not
/// down* over the interval in which it was. `007_recorder_cross_site.sql`'s
/// `segment_overflow` is what pays for it: nearest earlier segment,
/// `p.segment_seq + 1 = c.segment_seq`, and a counter that went backwards
/// clamped to zero — so the first window of the new run would report a
/// capture-drop delta of zero over a capture handle opened seconds earlier, and
/// a host that admitted nothing is a host whose absences may be used against a
/// publisher.
///
/// So the assertions are two and they belong together: the ledger **does** hold
/// a trailer, and the run beginning under it still numbers its first window
/// zero and still writes an uncertain anchor. Without the first assertion this
/// test would pass over an empty ledger and say nothing at all.
#[test]
fn a_restart_does_not_anchor_its_first_window_on_the_ledgers_trailer() {
    // One instance, one era, contiguous sequence numbers split across the
    // restart — the case where a continuation is at its most tempting, because
    // it happens to be true and the recorder is in no position to know it.
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(40).datagrams();
    let dir = TempDir::new().expect("a temporary directory");
    let spool_dir = dir.path().join("spool");
    let ledger_path = dir.path().join("ledger.jsonl");

    let first = run_at(&spool_dir, &ledger_path, &sent[..20]);
    assert!(!first.is_empty(), "the first run posted no window");

    // What the restart inherits, read the way a startup would read it.
    let inherited = Ledger::open(&ledger_path).expect("the ledger opens");
    let trailer = inherited
        .trailer()
        .expect("the first run landed a window, so its trailer is in the ledger");
    assert_eq!(
        trailer.segment_seq, 0,
        "the fixture is meant to land exactly one window in the first run"
    );

    let second = run_at(&spool_dir, &ledger_path, &sent[20..]);
    let batch = second.first().expect("the second run posted its window");

    let seq = batch
        .segment_coverage
        .first()
        .expect("a posted window describes what it covered")
        .segment_seq;
    assert_eq!(
        seq, 0,
        "the run after a restart continued the window sequence, so a reader is told the \
         derivation was not down over the interval in which it was"
    );

    assert!(
        !batch.era.is_empty(),
        "the fixture is meant to write an era row for the instance it replayed"
    );
    for row in &batch.era {
        assert_eq!(
            row.anchor_certain, 0,
            "the first window of a run declared a certain era anchor on a trailer from the \
             run before it: {row:?}"
        );
        assert_eq!(
            row.continuation, 0,
            "and called itself a continuation of a window the capture stopped after: {row:?}"
        );
    }
}

/// A window the spool refused hands its trailer to nobody.
///
/// The trailer is true — the derivation did read those datagrams — but the rows
/// it describes are not in the store. Handing it on would let the next window
/// declare a certain anchor and a continuation, and a reader would join two eras
/// as one continuous sequence space across a hole nothing in the rows can
/// explain. `None` there means *unknown*, and never *there was none*.
#[cfg(unix)]
#[test]
fn a_window_the_spool_refused_leaves_the_next_window_uncertain() {
    use std::os::unix::fs::PermissionsExt;

    fn mode(dir: &std::path::Path, bits: u32) {
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(bits))
            .expect("the mode can be set");
    }

    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(20).datagrams();
    let store = Arc::new(Mutex::new(Store::default()));
    let dir = TempDir::new().expect("a temporary directory");
    let spool_dir = dir.path().join("spool");
    let ledger_path = dir.path().join("ledger.jsonl");
    let spool = Spool::open(&spool_dir, 64 * 1024 * 1024).expect("the spool opens");
    let ledger = Ledger::open(&ledger_path).expect("the ledger opens");

    let (mut tx, rx) = ring(256);
    let mut cfg = config();
    cfg.bound.interval = Duration::from_millis(120);
    let pipeline = start(rx, spool, ledger, FakeSink(Arc::clone(&store)), cfg);

    // Readable and listable, not writable: the first window derives and then
    // cannot be written down, which is the case under test and not a spool bug.
    mode(&spool_dir, 0o500);
    for dg in &sent[..10] {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    std::thread::sleep(Duration::from_millis(400));
    // And writable again, so that there is a later window to read the anchor
    // off. A run where nothing lands proves nothing about what landed.
    mode(&spool_dir, 0o700);
    for dg in &sent[10..] {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    pipeline.stop(tx);

    let batches = std::mem::take(&mut store.lock().expect("the store is not poisoned").batches);
    let batch = batches
        .first()
        .expect("the window after the refusal reached the store");
    assert!(
        !batch.era.is_empty(),
        "the window after the refusal derived no era, so this asserts nothing"
    );
    for row in &batch.era {
        assert_eq!(
            row.anchor_certain, 0,
            "the window after a refused one anchored on a trailer whose rows were lost: {row:?}"
        );
    }
}

/// Datagrams offered to the ring reach the store, and the ledger records them.
///
/// The whole path in one test: a capture offering into the ring, a derivation
/// turning windows into rows, a spool holding them on disk, and a posting stage
/// that records only what the destination acknowledged.
#[test]
fn what_the_capture_offers_reaches_the_store_and_is_recorded() {
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(120).datagrams();
    let store = Arc::new(Mutex::new(Store::default()));
    let fixture = fixture();

    let (mut tx, rx) = ring(256);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        config(),
    );

    for dg in &sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    // The capture stops first, which is what ends the open window: the
    // datagrams already in the ring are derived rather than abandoned.
    let counters = pipeline.stop(tx);

    let store = store.lock().expect("the store is not poisoned");
    assert_eq!(
        store.datagram_rows,
        sent.len(),
        "every datagram offered became a row"
    );
    assert!(!store.landed_keys.is_empty());
    assert_eq!(
        ledger_entries(&fixture.ledger_path),
        store.landed_keys.len(),
        "one ledger entry per window the destination acknowledged, and no more"
    );
    assert_eq!(counters.windows_landed(), store.landed_keys.len() as u64);
    assert_eq!(counters.posts_failed(), 0);
    assert_eq!(counters.stage_restarts(), 0);
}

/// Shutdown derives the window that was open, rather than abandoning it.
///
/// The datagrams in the ring at shutdown were received, and the publisher will
/// not send them again. A pipeline that stopped without draining would leave a
/// hole in the rows that nothing in them could explain — and the verdict on a
/// gap with nothing admitted behind it is `publisher`.
#[test]
fn the_window_that_was_open_at_shutdown_is_derived_and_not_abandoned() {
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(40).datagrams();
    let store = Arc::new(Mutex::new(Store::default()));
    let fixture = fixture();

    let (mut tx, rx) = ring(256);
    let mut cfg = config();
    // Far longer than this test runs, so the window can only be closed by the
    // capture ending — which is exactly the case under test.
    cfg.bound.interval = Duration::from_secs(3_600);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        cfg,
    );

    for dg in &sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    pipeline.stop(tx);

    assert_eq!(
        store
            .lock()
            .expect("the store is not poisoned")
            .datagram_rows,
        sent.len(),
        "the window open at shutdown was derived"
    );
    assert_eq!(
        ledger_entries(&fixture.ledger_path),
        1,
        "and recorded, so a restart does not derive it again"
    );
}

/// A stage that panics is counted and begun again, and the capture is untouched.
///
/// The alternative is a recorder that keeps capturing into a ring nobody drains
/// and reports itself healthy on every other series it publishes. The counter is
/// what makes the restart visible: a stage that quietly came back is worse than
/// one that crashed, because nothing says a datagram was ever at risk.
#[test]
fn a_stage_that_panics_is_restarted_and_counted() {
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(40).datagrams();
    let store = Arc::new(Mutex::new(Store {
        // The first insert panics; the stage has to come back and try again.
        panic_after: Some(0),
        ..Store::default()
    }));
    let fixture = fixture();

    let (mut tx, rx) = ring(256);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        config(),
    );

    for dg in &sent {
        assert_eq!(
            tx.offer(&dg.as_recorded()),
            Offered::Accepted,
            "the capture keeps going while a stage is down"
        );
    }
    let counters = pipeline.stop(tx);

    assert!(
        counters.stage_restarts() >= 1,
        "the panic was not counted as a restart"
    );
    assert_eq!(
        store
            .lock()
            .expect("the store is not poisoned")
            .datagram_rows,
        sent.len(),
        "the window survived the panic and landed on the retry: the spool held it"
    );
}

/// The lag gauge reports the oldest unposted window, and zero when there is none.
///
/// This is the number an operator alerts on, so it has to mean the same thing on
/// a quiet host as on a backed-up one: zero because nothing is waiting, never
/// zero because nothing was measured.
#[test]
fn the_lag_gauge_is_zero_when_every_window_has_landed() {
    let sent: Vec<OwnedDatagram> = SyntheticPublisher::clean(40).datagrams();
    let store = Arc::new(Mutex::new(Store::default()));
    let fixture = fixture();

    let (mut tx, rx) = ring(256);
    let pipeline = start(
        rx,
        fixture.spool,
        fixture.ledger,
        FakeSink(Arc::clone(&store)),
        config(),
    );
    for dg in &sent {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
    let counters = pipeline.stop(tx);

    assert_eq!(
        counters.oldest_unposted_age_seconds(),
        0,
        "nothing is waiting, and the gauge says so"
    );
    assert_eq!(counters.spool_bytes(), 0, "and the spool is empty");
}

/// A scrape samples and renders alone, and a second one waits.
///
/// The endpoint serves every request on its own thread, and a Prometheus
/// counter cannot be assigned — only advanced — so a counter mirroring a total
/// the stages keep is sampled by reading it, subtracting, and adding the
/// difference. Two scrapes landing together would each read the same value and
/// each add the same difference, leaving a `*_total` permanently wrong by an
/// amount nothing records.
///
/// **Asserted on the exclusion rather than on the inflation.** A test that runs
/// many scrapes and checks the total afterwards is a test that has to lose a
/// race to fail: the sampling is a dozen atomic reads, so the window is narrow
/// enough that such a test passes against an implementation with no lock at all
/// — which is exactly what it did when it was written that way. This one holds
/// the first scrape open and requires the second to have waited, which either
/// happens or does not.
#[test]
fn a_second_scrape_waits_for_the_first_to_finish_sampling() {
    use dz_recorder_inline::metrics::InlineMetrics;
    use dz_recorder_inline::pipeline::InlineCounters;
    use dz_recorder_inline::ring::ring;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let metrics = Arc::new(InlineMetrics::new("site-1", "recorder-1"));
    let fixture = fixture();
    let spool = Arc::new(Mutex::new(fixture.spool));

    let (mut tx, _rx) = ring(1);
    for dg in &SyntheticPublisher::clean(8).datagrams() {
        let _ = tx.offer(&dg.as_recorded());
    }
    let dropped = tx.counters().dropped();
    assert!(dropped > 0, "the fixture is meant to overrun the ring");
    let ring_counters = Arc::clone(tx.counters());
    let counters = Arc::new(InlineCounters::default());

    let inside = Arc::new(AtomicBool::new(false));
    let first_finished = Arc::new(AtomicBool::new(false));

    let holder = {
        let metrics = Arc::clone(&metrics);
        let inside = Arc::clone(&inside);
        let first_finished = Arc::clone(&first_finished);
        let ring_counters = Arc::clone(&ring_counters);
        let counters = Arc::clone(&counters);
        let spool = Arc::clone(&spool);
        std::thread::spawn(move || {
            metrics.scrape(|m| {
                inside.store(true, Ordering::SeqCst);
                {
                    // Scoped, so the wait below is on the sampling lock and not
                    // on the spool's. Holding both would serialise the second
                    // scrape for the wrong reason and the test would pass
                    // against an implementation that takes no sampling lock at
                    // all — which is what it did when it was written that way.
                    let held = spool.lock().expect("the spool is not poisoned");
                    m.observe("top-of-book", &ring_counters, &counters, &held, 0);
                }
                // Long enough that a second scrape which did not wait would
                // have finished several times over.
                std::thread::sleep(Duration::from_millis(300));
                first_finished.store(true, Ordering::SeqCst);
            });
        })
    };

    while !inside.load(Ordering::SeqCst) {
        std::thread::sleep(Duration::from_millis(1));
    }
    let rendered = metrics.scrape(|m| {
        let held = spool.lock().expect("the spool is not poisoned");
        m.observe("top-of-book", &ring_counters, &counters, &held, 0);
    });
    assert!(
        first_finished.load(Ordering::SeqCst),
        "the second scrape sampled while the first was still inside its own"
    );
    holder.join().expect("the holding thread does not panic");

    // And the consequence the exclusion protects: two samples of a total that
    // never moved leave the counter reporting that total, not twice it.
    let line = rendered
        .lines()
        .find(|l| l.starts_with("dz_recorder_inline_ring_dropped_total"))
        .unwrap_or_else(|| panic!("the series is not in the exposition:\n{rendered}"));
    let reported: u64 = line
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("unreadable sample: {line}"));
    assert_eq!(reported, dropped, "the counter was advanced twice: {line}");
}
