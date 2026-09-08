//! The two guards, and the distinction that decides what each one measures.
//!
//! There are two silences and neither guard here measures the first one.
//! Upstream silence is `[ingress] idle_timeout`, the driver measures it, and its
//! answer is a reconnect. What is left is *upstream in, nothing out* — which is
//! genuinely a publisher defect — and a publisher that can no longer describe
//! itself truthfully.
//!
//! Every test in here states the time. Nothing sleeps: a 60-second guard tested
//! by waiting would cost the suite a minute and be asserted by nobody.

mod harness;

use std::time::Duration;

use dz_adapter_core::EventSink;
use dz_publisher_metrics::ExitReason;
use dz_publisher_runtime::{Exit, FeedSpec, Inconsistency};
use harness::{feed, harness, FakeAdapter};

/// The window every test here uses, so the arithmetic is readable.
const WINDOW: Duration = Duration::from_secs(60);

fn guarded() -> harness::Harness {
    let mut feed = feed();
    feed.idle_guard = WINDOW;
    harness(feed)
}

#[test]
fn the_idle_guard_fires_with_the_idle_guard_exit_reason() {
    let mut h = guarded();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);

    // The upstream is delivering: the adapter recognised a message. Nothing
    // follows it, because whatever the mapping was supposed to produce, it
    // produced nothing.
    h.publisher.upstream_message("quote");

    // One nanosecond short of the window.
    h.clock.advance(WINDOW - Duration::from_nanos(1));
    assert!(
        h.publisher.tick().is_none(),
        "the guard fired before its window elapsed"
    );

    h.clock.advance(Duration::from_nanos(1));
    h.publisher.upstream_message("quote");
    let exit = h.publisher.tick().expect("the window has elapsed");
    assert_eq!(exit, Exit::IdleGuard);
    // The label the exit is counted under, which is what a dashboard groups by.
    assert_eq!(exit.reason(), ExitReason::IdleGuard);
}

#[test]
fn the_idle_guard_stays_quiet_when_the_upstream_is_silent_too() {
    // A venue that has gone quiet overnight. Silent and healthy: the published
    // set is dormant, the heartbeats say the channel is alive, and whether the
    // *connection* is still there is `[ingress] idle_timeout`'s question and not
    // this guard's. A guard that fired here would restart every busy feed
    // because one venue stopped quoting.
    let mut h = guarded();
    h.publisher.upstream_message("quote");
    h.clock.advance(WINDOW * 10);
    assert!(
        h.publisher.tick().is_none(),
        "silence with no upstream traffic is not a publisher defect"
    );
}

#[test]
fn the_idle_guard_stays_quiet_before_the_first_upstream_message() {
    // Startup is not silence. An adapter waiting on its first connect has
    // published nothing and owes nothing, and a guard that counted from process
    // start would end a publisher whose venue opens in an hour.
    let mut h = guarded();
    h.clock.advance(WINDOW * 10);
    assert!(h.publisher.tick().is_none());
}

#[test]
fn publishing_resets_the_idle_guards_window() {
    let mut h = guarded();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    for _ in 0..3 {
        h.publisher.upstream_message("quote");
        h.publisher.event(harness::quote(instrument, 1));
        h.clock.advance(WINDOW - Duration::from_secs(1));
        assert!(
            h.publisher.tick().is_none(),
            "a publisher that is publishing is not idle"
        );
        h.clock.advance(Duration::from_secs(1));
    }
}

#[test]
fn a_dark_transmitter_fires_the_consistency_guard() {
    // The mktdata fan-out's one member starts refusing non-transiently, which
    // is what a socket whose route has gone does. The fan-out absorbs the
    // failure - it must, because above it sits the only code that advances
    // `Sequence Number` - and exposes the dropped member for a guard to read
    // between ticks.
    let mut h = guarded();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);

    assert!(
        h.publisher.tick().is_none(),
        "nothing is wrong with this publisher yet"
    );

    h.mktdata_refusal().set(true);
    // A tick sends a heartbeat, which is the send that discovers the socket.
    h.clock.advance(Duration::from_secs(2));
    let exit = h.publisher.tick().expect("the mktdata transmitter is gone");

    match &exit {
        Exit::ConsistencyGuard(Inconsistency::EgressDark { sink }) => {
            assert_eq!(sink, "mktdata");
        }
        other => panic!("expected a dark transmitter, got {other:?}"),
    }
    assert_eq!(exit.reason(), ExitReason::ConsistencyGuard);
}

#[test]
fn the_consistency_guard_is_reported_ahead_of_the_idle_guard() {
    // Both are true: the transmitter is gone, and therefore nothing is reaching
    // the wire. Reporting the idle guard would send an operator to look at the
    // mapping when the socket is the answer.
    let mut h = guarded();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.mktdata_refusal().set(true);
    h.publisher.upstream_message("quote");
    h.publisher.event(harness::quote(instrument, 1));
    h.clock.advance(WINDOW * 2);
    h.publisher.upstream_message("quote");

    let exit = h
        .publisher
        .tick()
        .expect("both guards have something to say");
    assert!(
        matches!(exit, Exit::ConsistencyGuard(_)),
        "the idle guard reported a socket failure: {exit:?}"
    );
}

#[test]
fn an_unpersistable_state_directory_fires_the_consistency_guard() {
    // The registry stops minting on its own and says so; whether the process
    // should end is documented there as the runtime's decision, and this is the
    // decision. A publisher that cannot persist an `Instrument ID` publishes
    // definitions whose IDs resolve to nothing after the next restart.
    //
    // Reaching it needs a store whose writes fail, which `MemoryStore` states
    // rather than a test arranging a full disk.
    let mut h = harness::harness_with_broken_writes(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    assert_eq!(
        adapter.handles().len(),
        0,
        "nothing may be admitted while the record cannot be written"
    );

    let exit = h
        .publisher
        .tick()
        .expect("the state directory is unwritable");
    match &exit {
        Exit::ConsistencyGuard(Inconsistency::StateUnpersistable { .. }) => {}
        other => panic!("expected an unpersistable state directory, got {other:?}"),
    }
    assert_eq!(exit.reason(), ExitReason::ConsistencyGuard);
}

#[test]
fn every_exit_this_crate_decides_maps_onto_a_reason_the_metrics_crate_defines() {
    // Three of the four normative values. The fourth, `panic`, is not a guard:
    // nothing here decides it, and an exit recorded under it is one a panic hook
    // records on the way out. Transcribed rather than derived, so a mapping that
    // changed would fail here.
    assert_eq!(Exit::IdleGuard.reason(), ExitReason::IdleGuard);
    assert_eq!(
        Exit::ConsistencyGuard(Inconsistency::EgressDark {
            sink: "mktdata".to_owned()
        })
        .reason(),
        ExitReason::ConsistencyGuard
    );
    assert_eq!(
        Exit::ConsistencyGuard(Inconsistency::UpstreamUnusable {
            detail: "not a websocket endpoint".to_owned()
        })
        .reason(),
        ExitReason::ConsistencyGuard
    );
    assert_eq!(Exit::Signal.reason(), ExitReason::Signal);
}

#[test]
fn a_dropped_reference_stream_is_named_and_darkens_nothing() {
    // **The other half of the scope distinction, and the half that was
    // unobservable.** A `Channel`-scope member that fails non-transiently is
    // counted, dropped and absorbed — the send returns `Ok`, because propagating
    // it would put a decision about `Sequence Number` in the hands of one
    // auxiliary consumer's socket. So the fan-out goes quiet with nothing in the
    // send path saying so, and `Tee::dropped` had no caller at all: an archive
    // stopped being written and the publisher reported itself healthy.
    let mut h = guarded();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    // The first tick sends a heartbeat and the first manifest, so both fan-outs
    // are known to be working before anything is broken.
    assert!(h.publisher.tick().is_none());
    assert!(h.publisher.dropped_sinks().is_empty());

    h.only().reference_refusal.set(true);
    h.only().refdata_reference_refusal.set(true);
    h.clock.advance(Duration::from_secs(2));

    assert!(
        h.publisher.tick().is_none(),
        "a reference stream must never be able to end the process",
    );
    assert_eq!(
        h.publisher.feeds().dark_transmitter(),
        None,
        "nothing that darkens this publisher has failed",
    );

    let dropped = h.publisher.dropped_sinks();
    let named: Vec<(&str, &str, usize)> = dropped
        .iter()
        .map(|d| (d.name, d.port_role.as_str(), d.live))
        .collect();
    assert_eq!(
        named,
        [
            ("mktdata-reference", "mktdata", 1),
            ("refdata-reference", "refdata", 1),
        ],
        "both dropped members are named, with the transmitter still live beside \
         each: {dropped:?}",
    );
    assert!(dropped.iter().all(|d| d.spec == FeedSpec::TopOfBook));
    // The transmitters are untouched: what a dropped auxiliary member costs is
    // the copy and nothing else.
    assert!(h.mktdata().len() > 1);
}
