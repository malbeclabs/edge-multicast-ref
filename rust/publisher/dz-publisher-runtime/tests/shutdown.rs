//! Shutdown: the order, and why anything else is wrong.
//!
//! `EndOfSession` is the terminal statement on the mktdata port, so it goes
//! last there; the final manifest carrying `Valid = 0` is a statement about the
//! reference-data set, so it goes on the refdata port and it goes *before*
//! `EndOfSession` — a subscriber that stops reading at the terminal statement
//! would never see it otherwise. Admissions close before either, because an
//! `Instrument ID` minted during shutdown is persisted and never published.

mod harness;

use dz_adapter_core::EventSink;
use dz_edge_core::{AppMessage, EndOfSession, Heartbeat};
use dz_edge_refdata::ManifestSummary;
use dz_edge_tob::Quote;
use dz_publisher_metrics::ExitReason;
use dz_publisher_runtime::{Exit, TeardownStep};
use harness::{feed, harness, FakeAdapter};

#[test]
fn shutdown_tears_down_in_the_stated_order() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let _ = h.publisher.tick();

    let teardown = h.publisher.shut_down(Exit::Signal);

    // Asserted against the transcribed list rather than against whatever the
    // code did, so reordering the implementation fails here.
    assert_eq!(teardown.steps(), TeardownStep::ORDER);
    assert_eq!(teardown.exit().reason(), ExitReason::Signal);
}

#[test]
fn the_last_message_on_the_mktdata_port_is_end_of_session() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    h.publisher.upstream_message("quote");
    h.publisher.event(harness::quote(instrument, 7));
    // Past the heartbeat interval, so a heartbeat is due: a heartbeat is sent
    // when there is *no other traffic*, and the quote above is other traffic.
    h.clock.advance(std::time::Duration::from_secs(1));
    let _ = h.publisher.tick();
    h.publisher.shut_down(Exit::Signal);

    let type_ids = h.mktdata.type_ids();
    // Transcribed from the specifications' own message tables, not read off the
    // codec: `0x01` heartbeat, `0x03` quote, `0x06` end of session.
    assert!(type_ids.contains(&0x03), "the quote never reached the wire");
    assert!(type_ids.contains(&0x01), "no heartbeat was sent");
    assert_eq!(
        type_ids.last(),
        Some(&0x06),
        "something followed EndOfSession on the mktdata port: {type_ids:?}"
    );
    assert_eq!(
        type_ids.iter().filter(|id| **id == 0x06).count(),
        1,
        "EndOfSession is sent once"
    );
    // The type ids match the codec's own constants, which is what makes the
    // literals above readable rather than magic.
    assert_eq!(EndOfSession::TYPE_ID, 0x06);
    assert_eq!(Heartbeat::TYPE_ID, 0x01);
    assert_eq!(Quote::TYPE_ID, 0x03);
}

#[test]
fn a_heartbeat_is_suppressed_while_market_data_is_flowing() {
    // The heartbeat exists so a subscriber can tell a quiet channel from a dead
    // one, so a channel that is not quiet does not need one — and a heartbeat
    // sent alongside traffic on a timer costs a datagram and a sequence number
    // per interval on the busiest feeds.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let instrument = adapter.handles()[0];

    for step in 0..5 {
        h.clock.advance(std::time::Duration::from_millis(900));
        h.publisher.event(harness::quote(instrument, step));
        let _ = h.publisher.tick();
    }

    let type_ids = h.mktdata.type_ids();
    assert!(
        !type_ids.contains(&0x01),
        "a heartbeat was sent on a channel that was publishing: {type_ids:?}"
    );
    assert_eq!(type_ids.iter().filter(|id| **id == 0x03).count(), 5);
}

#[test]
fn the_final_manifest_carries_valid_zero_and_is_the_last_thing_on_the_refdata_port() {
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B", "C-D"]);
    h.publisher.poll_listings(&mut adapter);
    // A tick, so that a manifest with `Valid = 1` has already gone out and the
    // final one is a change rather than the only one.
    let _ = h.publisher.tick();

    let before: Vec<ManifestSummary> = h
        .refdata
        .messages()
        .into_iter()
        .filter(|(type_id, _)| *type_id == 0x07)
        .map(|(_, bytes)| ManifestSummary::decode(&bytes).expect("composed by this publisher"))
        .collect();
    assert!(
        before.iter().any(|manifest| manifest.valid == 1),
        "the published set was never advertised as established"
    );

    h.publisher.shut_down(Exit::Signal);

    let manifests: Vec<ManifestSummary> = h
        .refdata
        .messages()
        .into_iter()
        .filter(|(type_id, _)| *type_id == 0x07)
        .map(|(_, bytes)| ManifestSummary::decode(&bytes).expect("composed"))
        .collect();
    let last = manifests.last().expect("a manifest was sent");
    assert_eq!(
        last.valid, 0,
        "the last manifest must say the set is no longer established"
    );
    // Still describing the set it described: the published set is not withdrawn
    // on the way down, it is that nothing new joins it.
    assert_eq!(last.instrument_count, 2);
    assert_eq!(last.channel_id, harness::CHANNEL_ID);

    // And it is the last thing on that port role.
    let refdata_ids = h.refdata.type_ids();
    assert_eq!(refdata_ids.last(), Some(&0x07));
    assert_eq!(ManifestSummary::TYPE_ID, 0x07);
}

#[test]
fn nothing_is_admitted_after_admissions_close() {
    // An `Instrument ID` minted during shutdown is persisted and never
    // published, so a subscriber that later resolves it finds nothing.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    assert_eq!(h.publisher.refdata().published(), 1);

    h.publisher.shut_down(Exit::Signal);

    let mut latecomer = FakeAdapter::new(&["E-F"]);
    // The poll cadence has not elapsed, so ask for the state directly: what is
    // under test is the registry's phase and not the poll timer.
    h.clock.advance(dz_publisher_runtime::LISTING_POLL);
    h.publisher.poll_listings(&mut latecomer);
    assert_eq!(
        latecomer.declined(),
        1,
        "an offer was admitted after shutdown"
    );
    assert_eq!(h.publisher.refdata().published(), 1);
    assert!(!h.publisher.refdata().is_valid());
}

#[test]
fn the_exit_reason_reaches_the_metric_before_the_last_scrape() {
    let mut h = harness(feed());
    let exposition_before = h.metrics.render();
    assert!(
        exposition_before.contains("dz_publisher_exit_reason_total"),
        "the family is pre-created, so an alert on it can fire"
    );

    h.publisher.shut_down(Exit::IdleGuard);

    let exposition = h.metrics.render();
    assert!(
        exposition
            .lines()
            .any(|line| line.contains("exit_reason_total")
                && line.contains("idle_guard")
                && line.trim_end().ends_with(" 1")),
        "the exit was not recorded under `idle_guard`:\n{exposition}"
    );
}

#[test]
fn an_era_that_survived_a_restart_is_on_every_datagram() {
    // Not part of the teardown, and it is the other half of the same
    // subscriber-facing statement: a publisher whose sequence series restarts
    // at 0 without its era changing has told its subscribers nothing. The
    // harness begins in era 2, which the store could only have got from a
    // previous run.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut adapter);
    let _ = h.publisher.tick();
    h.publisher.shut_down(Exit::Signal);

    let headers = h.mktdata.headers();
    assert!(!headers.is_empty());
    for (index, (sequence, era)) in headers.iter().enumerate() {
        assert_eq!(*era, 2, "the era changed mid-run");
        // Dense, from zero, in this era: the series is per channel instance and
        // it is not persisted, because a subscriber decides *this publisher
        // restarted* from the era and not from the sequence number.
        assert_eq!(*sequence, index as u64);
    }
}
