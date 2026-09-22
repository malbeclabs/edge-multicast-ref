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
use harness::{feed, harness, FakeAdapter, CHANNEL_ID, DEPTH_CHANNEL_ID};

/// The window every test here uses, so the arithmetic is readable.
const WINDOW: Duration = Duration::from_secs(60);

/// The Unix second the harness clock starts at, which is the value a gauge set
/// before the clock moves renders.
const START_UNIX_SECONDS: f64 = 1_700_000_000.0;

const CHANNEL_LAST_PUBLISHED: &str = "dz_publisher_channel_last_published_timestamp_seconds";
const IDLE_GUARD_LAST_UPDATE: &str = "dz_publisher_idle_guard_last_update_timestamp_seconds";

fn guarded() -> harness::Harness {
    let mut feed = feed();
    feed.idle_guard = WINDOW;
    harness(feed)
}

/// One gauge sample's value, found by family name and an optional label
/// fragment.
///
/// Read off the rendered exposition rather than through an accessor, because
/// the rendered series is the whole of what an operator can write an alert
/// against: a value correct in a field and absent from the scrape is the
/// failure these series exist to end.
fn gauge(exposition: &str, name: &str, label: Option<&str>) -> f64 {
    let line = exposition
        .lines()
        .find(|line| {
            line.starts_with(&format!("{name}{{")) && label.is_none_or(|label| line.contains(label))
        })
        .unwrap_or_else(|| panic!("no {name} sample matching {label:?} in:\n{exposition}"));
    line.rsplit(' ')
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("not a gauge sample: {line}"))
}

/// When the upstream's activity last put a message on `channel_id`.
fn last_published(exposition: &str, channel_id: u8) -> f64 {
    gauge(
        exposition,
        CHANNEL_LAST_PUBLISHED,
        Some(&format!("channel_id=\"{channel_id}\"")),
    )
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

// --- What the idle guard is the wrong instrument for -------------------------
//
// One feed of several going permanently silent. The guard measures a
// process-wide conjunction and must keep doing so: a guard that ended the
// process over one channel's silence would restart every other channel with
// it. So the condition is made *visible* per channel instead, on
// `dz_publisher_channel_last_published_timestamp_seconds`, and these are the
// tests of that.

#[test]
fn one_feed_going_silent_is_visible_while_its_sibling_publishes() {
    // The incident. Everything upstream of the depth feed has failed: the
    // top-of-book feed keeps publishing, the shared guard keeps being fed, the
    // exit never fires — correctly — and nothing else notices, because
    // connection state is per connection, the aggregate is healthy, and the
    // manifest still lists the depth feed's instruments as published.
    let mut h = harness::harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    // A quote is a top-of-book message and the specification carries it
    // nowhere else, so this is one feed publishing and its sibling not.
    h.publisher.upstream_message("quote");
    h.publisher.event(harness::quote(instrument, 1));

    let exposition = h.metrics.render();
    assert_eq!(
        last_published(&exposition, CHANNEL_ID),
        START_UNIX_SECONDS,
        "the channel that published is not reporting when it did:\n{exposition}"
    );
    assert_eq!(
        last_published(&exposition, DEPTH_CHANNEL_ID),
        0.0,
        "the silent channel is reporting a publication it never made:\n{exposition}"
    );
    // The process-wide gauge the playbook carries, held at *now* by the busy
    // feed. This is the series that cannot answer the question, which is why
    // there is a second one.
    assert_eq!(
        gauge(&exposition, IDLE_GUARD_LAST_UPDATE, None),
        START_UNIX_SECONDS
    );

    // And the exit stays where it was. Ten windows of one feed publishing and
    // the other silent is not a process to end.
    h.clock.advance(WINDOW * 10);
    h.publisher.upstream_message("quote");
    h.publisher.event(harness::quote(instrument, 2));
    assert!(
        h.publisher.tick().is_none(),
        "one silent feed ended the process, taking the busy one with it"
    );
}

#[test]
fn traffic_this_publisher_paces_itself_refreshes_no_channel() {
    // The whole reason this is a series of its own rather than a reading off
    // the egress counters. A heartbeat, a definition, a manifest and a
    // snapshot are all paced by this publisher, so a channel whose upstream
    // has died goes on sending every one of them: anything that counted them
    // would report a dead channel as fresh for as long as the process lives.
    //
    // All four, and the snapshot is the one that needs asking for: the depth
    // block configures no rotation unless a test states a cycle, so without
    // one this asserts nothing about the kind of traffic that is a *derived*
    // copy of a book rather than a fixed-size message on a timer.
    let mut h = harness::harness_both_with_rotation(Duration::from_secs(2));
    let mut adapter =
        FakeAdapter::new(&["A-B"]).with_book(&[(dz_adapter_core::Side::Bid, "100.25", "2.500")]);
    h.publisher.poll_listings(&mut adapter);

    // A tick sends heartbeats on both channels, the definition cycle and the
    // first manifest. The rotation schedules on its first tick and snapshots
    // from the second.
    assert!(h.publisher.tick().is_none());
    h.clock.advance(Duration::from_secs(30));
    assert!(h.publisher.tick().is_none());
    for _ in 0..2 {
        h.clock.advance(Duration::from_secs(2));
        let _ = h.publisher.periodic_snapshot(&adapter);
        assert!(h.publisher.tick().is_none());
    }
    assert!(
        h.tob.as_ref().expect("a top-of-book feed").mktdata.len() > 0,
        "nothing reached the wire, so this test is asserting nothing"
    );
    assert!(
        h.mbp
            .as_ref()
            .expect("a market-by-price feed")
            .snapshot
            .as_ref()
            .expect("a depth feed has a snapshot port")
            .len()
            > 0,
        "no snapshot was sent, so this test is not asserting anything about one"
    );
    assert!(
        h.tob.as_ref().expect("a top-of-book feed").refdata.len() > 0
            && h.mbp
                .as_ref()
                .expect("a market-by-price feed")
                .refdata
                .len()
                > 0,
        "no definition and no manifest reached a refdata port role, so this test is \
         asserting nothing about the two kinds that go there"
    );

    let exposition = h.metrics.render();
    for channel_id in [CHANNEL_ID, DEPTH_CHANNEL_ID] {
        assert_eq!(
            last_published(&exposition, channel_id),
            0.0,
            "traffic this publisher paced itself reported Channel ID {channel_id} as having \
             published:\n{exposition}"
        );
    }
}

#[test]
fn an_instrument_reset_reports_the_channel_it_was_announced_on() {
    // The one message that is neither a lowered price event nor paced by this
    // publisher, and it counts: an adapter announces a discard on discovering
    // its own book has stopped being right, which is upstream activity
    // reaching the wire and is what feeds the idle guard too.
    //
    // It is the sharp edge of the series and is written down rather than left
    // to be found: an adapter that announced a discard on every reconnect
    // would hold a dead channel's gauge at *now* for the life of the process.
    // Whether it should is a question about that adapter's contract, and
    // deciding it here would change what the venue-wide guard measures.
    let mut h = harness::harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher
        .desynchronised(instrument, dz_adapter_core::Desync::UpstreamGap);

    let exposition = h.metrics.render();
    assert_eq!(
        last_published(&exposition, DEPTH_CHANNEL_ID),
        START_UNIX_SECONDS,
        "the reset reached the depth channel and it is not reporting it:\n{exposition}"
    );
    // And nowhere else. `0x14` is a market-by-price message; the top-of-book
    // channel carried nothing.
    assert_eq!(last_published(&exposition, CHANNEL_ID), 0.0);
}

#[test]
fn a_trade_reports_both_of_the_channels_it_reached() {
    // `0x04` is the one message both specifications carry, lowered once and
    // handed to both send paths. Two channel instances took it, so both
    // report it: a trade recorded against one of them would leave the other
    // reading as stale while its subscribers were being served.
    let mut h = harness::harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.upstream_message("trade");
    h.publisher.event(harness::trade(instrument, 1));

    let exposition = h.metrics.render();
    for channel_id in [CHANNEL_ID, DEPTH_CHANNEL_ID] {
        assert_eq!(
            last_published(&exposition, channel_id),
            START_UNIX_SECONDS,
            "Channel ID {channel_id} carried the trade and is not reporting it:\n{exposition}"
        );
    }
}

#[test]
fn a_trade_one_feed_refused_refreshes_only_the_channel_that_took_it() {
    // Why the send paths are asked *which* of them took the trade rather than
    // whether either did. One lowered value, two channel instances, and one of
    // them cannot put it on the wire: a runtime that recorded the trade instead
    // of the sends would report a channel whose every datagram is being refused
    // as freshly published, which is the reading this series exists to end.
    //
    // Both members of top-of-book's mktdata fan-out refuse, because that is the
    // only shape in which a send fails: `Tee::send` absorbs a member's failure
    // and reports `NotRegistered` only once no live member is left. So the first
    // trade is the send that collapses the fan-out and the second is the one
    // refused.
    let mut h = harness::harness_both();
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    let tob = h.tob.as_ref().expect("a top-of-book feed");
    tob.mktdata_refusal.set(true);
    tob.reference_refusal.set(true);

    h.publisher.upstream_message("trade");
    h.publisher.event(harness::trade(instrument, 1));

    h.clock.advance(Duration::from_secs(5));
    h.publisher.upstream_message("trade");
    h.publisher.event(harness::trade(instrument, 2));

    let exposition = h.metrics.render();
    assert_eq!(
        last_published(&exposition, DEPTH_CHANNEL_ID),
        START_UNIX_SECONDS + 5.0,
        "the depth channel took the second trade and is not reporting it:\n{exposition}"
    );
    assert_eq!(
        last_published(&exposition, CHANNEL_ID),
        START_UNIX_SECONDS,
        "the top-of-book channel refused the second trade and is reporting it as \
         published:\n{exposition}"
    );
    assert!(
        h.mbp
            .as_ref()
            .expect("a market-by-price feed")
            .mktdata
            .len()
            > 0,
        "nothing reached the depth feed's wire, so this test is asserting nothing"
    );
}
