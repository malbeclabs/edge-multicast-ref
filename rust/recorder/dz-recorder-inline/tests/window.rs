//! Where a window ends, and what its manifest is allowed to claim.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

use std::time::Duration;

use dz_edge_core::DatagramHeader;
use dz_recorder_archive::JoinedRole;
use dz_recorder_core::{CaptureDropScope, OwnedDatagram, RecorderIdentity, Source as _};
use dz_recorder_inline::manifest::{window_key, window_manifest, WindowIdentity};
use dz_recorder_inline::ring::{ring, Offered, RingSender};
use dz_recorder_inline::window::{Closed, HeldWindow, WindowBound, WindowSource};
use dz_recorder_replay::synthetic::{SyntheticPublisher, GROUP};

const FEED: &str = "top-of-book";

/// Generous, and never actually waited on: the producer has already offered
/// everything before a window opens in these tests.
const LONG: Duration = Duration::from_secs(30);

fn identity() -> RecorderIdentity {
    RecorderIdentity {
        site: "site-1".to_owned(),
        recorder: "recorder-1".to_owned(),
        env: "test".to_owned(),
        build_version: "0.1.0".to_owned(),
        build_commit: "0000000".to_owned(),
        config_hash: "a".repeat(64),
    }
}

fn roles() -> Vec<JoinedRole> {
    vec![JoinedRole {
        role: "mktdata".to_owned(),
        group: GROUP,
        port: 40_000,
        interface: None,
        source: None,
    }]
}

fn window_identity<'a>(id: &'a RecorderIdentity, roles: &'a [JoinedRole]) -> WindowIdentity<'a> {
    WindowIdentity {
        identity: id,
        feed: FEED,
        roles_joined: roles,
        drop_scope: CaptureDropScope::PortRole,
        link_headers_captured: false,
    }
}

/// A real stream, from the encoder the publisher tests use.
///
/// Hand-built datagrams were tried first and are the wrong fixture here: the
/// coverage tracker reads the channel, the sequence number and the reset count
/// at fixed offsets, so a test that writes those offsets itself is a test of
/// the test's idea of the layout. These carry headers the codec produced.
fn stream(count: usize) -> Vec<OwnedDatagram> {
    SyntheticPublisher::clean(count).datagrams()
}

fn offer_all(tx: &mut RingSender, datagrams: &[OwnedDatagram]) {
    for dg in datagrams {
        assert_eq!(tx.offer(&dg.as_recorded()), Offered::Accepted);
    }
}

/// The sequence numbers a window handed to the derivation, in arrival order.
fn sequence_numbers(window: &mut WindowSource<'_>) -> Vec<u64> {
    let mut out = Vec::new();
    while let Some(dg) = window.next().expect("the ring does not fail") {
        let header = DatagramHeader::peek(dg.payload).expect("the fixture carries a header");
        out.push(header.sequence_number);
    }
    out
}

/// A window closes when its payload bound is reached.
#[test]
fn a_window_closes_on_its_byte_bound() {
    let datagrams = stream(10);
    let each = datagrams[0].payload.len() as u64;
    let (mut tx, mut rx) = ring(64);
    offer_all(&mut tx, &datagrams);

    // Four datagrams' worth, so the fourth reaches the bound and the fifth is
    // left where the next window will find it.
    let bound = WindowBound {
        bytes: each * 4,
        interval: LONG,
    };
    let mut window = WindowSource::open(&mut rx, bound);
    let seen = sequence_numbers(&mut window);

    assert_eq!(window.closed(), Closed::Bytes);
    assert_eq!(seen.len(), 4);
    assert_eq!(window.tally().payload_byte_count, each * 4);
    assert_eq!(window.tally().datagram_count, 4);
}

/// The datagram that would have crossed the bound is the next window's first.
///
/// It is left in the ring rather than taken and held over, which is why the
/// bound is checked before the receive: a datagram taken out and put aside
/// belongs to no window's tally, and a reader would find a count that does not
/// match the rows.
#[test]
fn the_datagram_that_would_cross_the_bound_opens_the_next_window() {
    let datagrams = stream(6);
    let each = datagrams[0].payload.len() as u64;
    let (mut tx, mut rx) = ring(64);
    offer_all(&mut tx, &datagrams);

    let bound = WindowBound {
        bytes: each * 2,
        interval: LONG,
    };

    let first = {
        let mut window = WindowSource::open(&mut rx, bound);
        let seen = sequence_numbers(&mut window);
        assert_eq!(window.closed(), Closed::Bytes);
        seen
    };
    let second = {
        let mut window = WindowSource::open(&mut rx, bound);
        sequence_numbers(&mut window)
    };

    assert_eq!(first.len(), 2, "two datagrams reach a two-datagram bound");
    assert_eq!(second.len(), 2);
    assert_eq!(
        second[0],
        first[1] + 1,
        "the next window opens on the very next sequence number: nothing was \
         taken out of the ring and put aside"
    );
}

/// A feed that has gone quiet still closes its window, on age.
///
/// The case the age bound exists for. A window that waited for traffic would
/// hold a silent channel's last rows until something else arrived.
#[test]
fn a_quiet_feed_closes_its_window_on_age() {
    let (_tx, mut rx) = ring(4);
    let bound = WindowBound {
        bytes: u64::MAX,
        interval: Duration::from_millis(150),
    };

    let start = std::time::Instant::now();
    let mut window = WindowSource::open(&mut rx, bound);
    assert!(window.next().expect("the ring does not fail").is_none());

    assert_eq!(window.closed(), Closed::Age);
    assert!(window.is_empty(), "nothing arrived, so nothing is tallied");
    assert!(
        start.elapsed() >= Duration::from_millis(150),
        "it closed before its own interval"
    );
}

/// A capture that stops ends the window rather than timing it out.
///
/// The pipeline needs the difference: an age close means open another window, an
/// ending means this was the last one of the run.
#[test]
fn a_capture_that_stops_ends_the_window() {
    let datagrams = stream(1);
    let (mut tx, mut rx) = ring(8);
    offer_all(&mut tx, &datagrams);
    drop(tx);

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: LONG,
        },
    );
    let mut seen = 0;
    while window.next().expect("the ring does not fail").is_some() {
        seen += 1;
    }
    assert_eq!(seen, 1, "what was already in the ring is still derived");
    assert_eq!(window.closed(), Closed::CaptureEnded);
}

/// The manifest states what the window observed and refuses to invent the rest.
///
/// The digest and the byte count are the two fields a window cannot honestly
/// fill: nothing was written and nothing was hashed. An invented digest is worse
/// than an absent one, because it is a claim that something was verified.
#[test]
fn a_window_manifest_leaves_the_digest_and_the_size_empty() {
    let datagrams = stream(5);
    let each = datagrams[0].payload.len() as u64;
    let first_ts = datagrams[0].recv_ts_ns;
    let last_ts = datagrams[4].recv_ts_ns;
    let (mut tx, mut rx) = ring(64);
    offer_all(&mut tx, &datagrams);
    drop(tx);

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: LONG,
        },
    );
    while window.next().expect("the ring does not fail").is_some() {}

    let id = identity();
    let roles = roles();
    let wid = window_identity(&id, &roles);
    let manifest = window_manifest(&wid, window.tally(), 7);

    assert_eq!(
        manifest.sha256, "",
        "no datagram was kept, so nothing was hashed"
    );
    assert_eq!(
        manifest.byte_count, 0,
        "and there is no object to have a size"
    );
    assert_eq!(
        manifest.interface_drop_total, 0,
        "loss upstream of the capture point is not in either mode's manifest"
    );

    // Everything else is observed, and is what the archive path would write.
    assert_eq!(manifest.site, "site-1");
    assert_eq!(manifest.recorder, "recorder-1");
    assert_eq!(manifest.feed, FEED);
    assert_eq!(manifest.config_hash, "a".repeat(64));
    assert_eq!(manifest.segment_seq, 7);
    assert_eq!(manifest.datagram_count, 5);
    assert_eq!(manifest.payload_byte_count, each * 5);
    assert_eq!(manifest.start_ns, first_ts);
    assert_eq!(manifest.end_ns, last_ts);
    assert_eq!(
        manifest.capture_drop_total, 0,
        "this stream declares no drops, and the window may not invent any"
    );
    assert_eq!(manifest.capture_drop_scope, "port-role");
    assert_eq!(manifest.link_headers, "synthesised");
    assert_eq!(manifest.roles_joined.len(), 1);
    assert!(
        !manifest.object_key.is_empty(),
        "the key identifies the window even though it names no object"
    );
}

/// The window key orders across process runs, which a sequence number cannot.
///
/// `segment_seq` restarts at zero on every run, so two runs of one recorder
/// would produce the same key for different datagrams. The wall-clock start is
/// what makes the key unique and sortable without a coordinator — and it says
/// `live/`, so nobody goes looking for an object to fetch.
#[test]
fn a_window_key_carries_the_start_stamp_and_says_it_names_no_object() {
    let id = identity();
    let roles = roles();
    let wid = window_identity(&id, &roles);

    let first_run = window_key(&wid, 1_700_000_000_000_000_000, 0);
    let second_run = window_key(&wid, 1_700_000_600_000_000_000, 0);

    assert_ne!(
        first_run, second_run,
        "two runs both start at window zero and must not collide"
    );
    assert!(first_run < second_run, "and the earlier one sorts first");
    assert!(first_run.starts_with("live/"), "{first_run}");
    assert!(first_run.contains("site=site-1"), "{first_run}");
    assert!(first_run.contains(&format!("feed={FEED}")), "{first_run}");
}

/// The capture loss a window reports is the host's total, and it never restarts.
///
/// **The one column here whose wrong value is an accusation.**
/// `recorder.segment_overflow` subtracts `capture_drop_total` over consecutive
/// windows and reads a zero difference as `overflow_free = 1`, which is one of
/// the conditions admitting a site's absence as evidence *about the publisher*.
/// A site that dropped datagrams itself may not contribute an absence, so a
/// total pinned at zero — or restarted per window — certifies this host clean
/// and turns its own receive-queue overflow into a finding against somebody
/// else.
///
/// Two windows, because one window cannot tell the two shapes apart: a
/// per-window figure passes every assertion the first window can make. The
/// second is where it dies.
#[test]
fn the_capture_loss_a_window_reports_is_cumulative_and_not_its_own() {
    let mut datagrams = stream(6);
    // Declared by the capture, before the ring: the kernel lost four datagrams
    // before this one, and two before that one.
    datagrams[1].drop_delta = 4;
    datagrams[4].drop_delta = 2;
    let each = datagrams[0].payload.len() as u64;

    // Three datagrams each, so the two declarations fall in different windows.
    // Offered a window at a time and not all at once, because a cumulative
    // counter is advanced by the capture and a fixture that offered everything
    // first would have both declarations in hand before either window opened.
    let bound = WindowBound {
        bytes: each * 3,
        interval: LONG,
    };
    let id = identity();
    let roles = roles();
    let (mut tx, mut rx) = ring(64);

    offer_all(&mut tx, &datagrams[..3]);
    let first = {
        let mut window = WindowSource::open(&mut rx, bound);
        while window.next().expect("the ring does not fail").is_some() {}
        window_manifest(&window_identity(&id, &roles), window.tally(), 0)
    };
    offer_all(&mut tx, &datagrams[3..]);
    drop(tx);
    let second = {
        let mut window = WindowSource::open(&mut rx, bound);
        while window.next().expect("the ring does not fail").is_some() {}
        window_manifest(&window_identity(&id, &roles), window.tally(), 1)
    };

    assert_eq!(
        first.capture_drop_total, 4,
        "the first window's three datagrams declared four lost"
    );
    assert_eq!(
        second.capture_drop_total, 6,
        "the total restarted, so the delta between these two windows is what the second \
         window dropped rather than what the host has dropped since it started — and a \
         window that dropped less than its predecessor then reads as provably clean"
    );
}

/// A datagram the ring refused is in the total, because the ring is ours.
///
/// The ring is inline mode's own place to lose a datagram and archive mode has
/// no equivalent, so a datagram it dropped is exactly the loss `overflow_free`
/// must not certify away. Row-level attribution already travels on `drop_delta`
/// through the debt; this is the segment-level counter the cross-site machinery
/// reads.
#[test]
fn a_datagram_the_ring_refused_is_in_the_capture_loss_the_window_reports() {
    let datagrams = stream(12);
    // Two slots against twelve datagrams: the ring cannot take them and every
    // one it refuses is loss before the derivation.
    let (mut tx, mut rx) = ring(2);
    let mut refused = 0u64;
    for dg in &datagrams {
        if tx.offer(&dg.as_recorded()) == Offered::Dropped {
            refused += 1;
        }
    }
    drop(tx);
    assert!(refused > 0, "the fixture is meant to overrun the ring");

    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: LONG,
        },
    );
    while window.next().expect("the ring does not fail").is_some() {}

    assert_eq!(
        window.tally().capture_drop_total,
        refused,
        "the ring refused {refused} datagrams and the window reports {} lost before the \
         derivation",
        window.tally().capture_drop_total
    );
}

/// A held window hands the same datagrams back, so the second pass sees them.
///
/// The manifest describes what the window saw, and `derive` stamps it onto every
/// row as it reads — so the window has to be readable twice. This is the half of
/// that which is not the derivation: what goes in comes out, in arrival order,
/// carrying the delta the ring wrote on it.
#[test]
fn a_held_window_replays_what_it_drained_in_arrival_order() {
    let mut datagrams = stream(6);
    datagrams[2].drop_delta = 3;

    let (mut tx, mut rx) = ring(64);
    offer_all(&mut tx, &datagrams);
    drop(tx);

    let mut held = HeldWindow::new();
    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: LONG,
        },
    );
    held.fill(&mut window).expect("the ring does not fail");
    assert_eq!(window.tally().datagram_count, 6, "the window was drained");

    let mut replayed = Vec::new();
    let mut source = held.replay();
    while let Some(dg) = source.next().expect("a buffer does not fail") {
        replayed.push((dg.recv_ts_ns, dg.drop_delta, dg.payload.to_vec()));
    }

    let expected: Vec<_> = datagrams
        .iter()
        .map(|dg| (dg.recv_ts_ns, dg.drop_delta, dg.payload.clone()))
        .collect();
    assert_eq!(replayed, expected, "the second pass is the first pass");

    // And the buffer is reused rather than appended to: a shorter window after a
    // longer one replays its own length, not the tail of its predecessor.
    let (mut tx, mut rx) = ring(64);
    offer_all(&mut tx, &datagrams[..2]);
    drop(tx);
    let mut window = WindowSource::open(
        &mut rx,
        WindowBound {
            bytes: u64::MAX,
            interval: LONG,
        },
    );
    held.fill(&mut window).expect("the ring does not fail");
    let mut source = held.replay();
    let mut count = 0;
    while source.next().expect("a buffer does not fail").is_some() {
        count += 1;
    }
    assert_eq!(
        count, 2,
        "the second window replayed its predecessor's slots"
    );
}
