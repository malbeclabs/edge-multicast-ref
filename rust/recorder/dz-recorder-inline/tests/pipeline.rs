//! The three stages wired together: what reaches the store, and what happens
//! when a stage does not survive its own bug.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

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
