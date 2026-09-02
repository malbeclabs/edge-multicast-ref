//! `InstrumentReset` end to end: announced on the wire, and the snapshot it
//! obliges.
//!
//! The capability the boundary was missing. An adapter owns its book, so it is
//! the only layer that can tell it has stopped being right — a delta it could
//! not route, a size it could not read, an upstream that resynchronised
//! underneath it. Before `EventSink::desynchronised` existed there was nowhere
//! to say so, and all three things it could do instead were wrong: publish on
//! and every later absolute quantity at that price is wrong for the rest of the
//! era, clear and it has told subscribers the levels are gone when they are
//! not, or drop the event and it has published on with less evidence.
//!
//! The wire values are transcribed from the specification rather than read off
//! the encoder: `0x14`, 28 bytes, and reset reason `3` for an upstream gap.

mod harness;

use dz_adapter_core::{Desync, EventSink};
use dz_publisher_runtime::Exit;
use harness::{depth_feed, feed, harness, FakeAdapter};

/// The type id and reason this file asserts, from the specification's tables.
const TYPE_INSTRUMENT_RESET: u8 = 0x14;
const RESET_UPSTREAM_GAP: u8 = 3;
const TYPE_SNAPSHOT_BEGIN: u8 = 0x20;
const TYPE_SNAPSHOT_END: u8 = 0x22;

#[test]
fn a_desynchronised_instrument_is_announced_on_the_market_data_port() {
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["ONE", "TWO"]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    h.publisher.desynchronised(one, Desync::UpstreamGap);

    let messages = h.only().mktdata.messages();
    let (type_id, body) = messages
        .iter()
        .find(|(type_id, _)| *type_id == TYPE_INSTRUMENT_RESET)
        .expect("the reset reached the market-data port");
    assert_eq!(*type_id, TYPE_INSTRUMENT_RESET);
    assert_eq!(body.len(), 28, "the specification's length");
    assert_eq!(body[8], RESET_UPSTREAM_GAP, "the reason the venue gave");

    // The instrument it names, and not the other one this publisher carries.
    let instrument_id = u32::from_le_bytes(body[4..8].try_into().expect("four bytes"));
    assert_eq!(instrument_id, 1, "the first admitted instrument");
}

#[test]
fn the_anchor_is_the_sequence_number_of_the_datagram_that_carries_it() {
    // **The rule the specification's own conformance subscriber grades.** The
    // reset takes effect immediately, so the anchor is where the stream is
    // *now* — and the off-by-one it catches is reading the number off the last
    // delta instead, which is one behind.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["ONE"]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    h.publisher.desynchronised(one, Desync::VenueResync);

    let headers = h.only().mktdata.headers();
    let messages = h.only().mktdata.messages();
    let position = messages
        .iter()
        .position(|(type_id, _)| *type_id == TYPE_INSTRUMENT_RESET)
        .expect("the reset reached the wire");
    let (sequence, _) = headers[position];
    let body = &messages[position].1;
    let anchor = u64::from_le_bytes(body[12..20].try_into().expect("eight bytes"));
    assert_eq!(
        anchor, sequence,
        "New Anchor Seq must equal the Sequence Number of its own datagram"
    );
}

#[test]
fn the_recovery_snapshot_is_owed_and_the_caller_drains_it() {
    // Owed rather than sent inside the adapter's callback: capturing a book is
    // a walk of it, and a snapshot has to be captured *after* the reset that
    // announced it, because a subscriber discards any snapshot for the
    // instrument with an older anchor.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["ONE"]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    assert!(
        h.publisher.owed_snapshots().is_empty(),
        "nothing is owed before anything went wrong"
    );

    h.publisher.desynchronised(one, Desync::UpstreamGap);
    let owed = h.publisher.owed_snapshots();
    assert_eq!(owed.len(), 1, "the instrument owes a recovery snapshot");
    assert_eq!(owed[0].0, one);
    assert!(
        h.publisher.owed_snapshots().is_empty(),
        "draining it twice would capture the same book twice"
    );
}

#[test]
fn an_instrument_owed_twice_is_owed_once() {
    // The second reset's anchor supersedes the first, and a subscriber discards
    // the older snapshot anyway - so two captures of one book is work with no
    // reader.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["ONE"]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    h.publisher.desynchronised(one, Desync::UpstreamGap);
    h.publisher.desynchronised(one, Desync::VenueResync);
    let owed = h.publisher.owed_snapshots();
    assert_eq!(owed.len(), 1, "owed twice is owed once");
    assert_eq!(owed[0].0, one);
    // And it is the *later* anchor that survives: the second reset has already
    // told subscribers to discard a snapshot at the first one.
    assert!(owed[0].1 > 0, "the later reset's anchor");

    // Both announcements still reached the wire, because each is a statement
    // about a different moment in the stream.
    let resets = h
        .only()
        .mktdata
        .type_ids()
        .into_iter()
        .filter(|type_id| *type_id == TYPE_INSTRUMENT_RESET)
        .count();
    assert_eq!(resets, 2);
}

#[test]
fn the_snapshot_that_follows_is_anchored_where_the_reset_promised() {
    // **The obligation, closed, and the reason it needed its own path.** A
    // subscriber records the reset's anchor as the minimum it will accept for
    // that instrument. The reset's own datagram advances the sequence, so a
    // snapshot captured afterwards and anchored where the stream has *since*
    // reached is at least one number later - and is one the subscriber
    // discards, leaving the instrument waiting for something that already went
    // past. This test found that: the reset promised 0 and a routine capture
    // produced 1.
    let mut h = harness(depth_feed());
    let mut adapter = FakeAdapter::new(&["ONE"]).with_book(&[
        (dz_adapter_core::Side::Bid, "0.41", "5"),
        (dz_adapter_core::Side::Ask, "0.43", "7"),
    ]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    h.publisher.desynchronised(one, Desync::UpstreamGap);

    let messages = h.only().mktdata.messages();
    let reset = messages
        .iter()
        .find(|(type_id, _)| *type_id == TYPE_INSTRUMENT_RESET)
        .expect("announced");
    let promised = u64::from_le_bytes(reset.1[12..20].try_into().expect("eight bytes"));

    for (instrument, anchor) in h.publisher.owed_snapshots() {
        assert_eq!(anchor, promised, "the debt carries the promised anchor");
        h.publisher
            .snapshot_anchored_at(&adapter, instrument, anchor, 0)
            .expect("the book is there to capture");
    }

    let snapshot = h.only().snapshot.as_ref().expect("a depth feed has one");
    let ids = snapshot.type_ids();
    assert_eq!(
        ids.first().copied(),
        Some(TYPE_SNAPSHOT_BEGIN),
        "the group opens"
    );
    assert_eq!(ids.last().copied(), Some(TYPE_SNAPSHOT_END), "and closes");

    let begin = snapshot
        .messages()
        .into_iter()
        .find(|(type_id, _)| *type_id == TYPE_SNAPSHOT_BEGIN)
        .expect("a begin");
    let anchor = u64::from_le_bytes(begin.1[8..16].try_into().expect("eight bytes"));
    assert_eq!(
        anchor, promised,
        "the snapshot must be anchored where the reset said it would be"
    );
}

#[test]
fn a_top_of_book_publisher_has_nowhere_to_announce_it() {
    // `0x14` is a market-by-price message and top-of-book's table does not
    // carry it, so a publisher emitting only that feed has nothing to send and
    // nothing to recover. Counted as unroutable, like every other event no
    // enabled feed carries — not refused, because the adapter was right to say
    // it.
    let mut h = harness(feed());
    let mut adapter = FakeAdapter::new(&["ONE"]);
    h.publisher.poll_listings(&mut adapter);

    let one = dz_adapter_core::InstrumentRef::from_admission(0);
    h.publisher.desynchronised(one, Desync::UpstreamGap);

    assert!(
        !h.only().mktdata.type_ids().contains(&TYPE_INSTRUMENT_RESET),
        "a feed that does not carry it must not carry it"
    );
    assert!(h.publisher.owed_snapshots().is_empty());
    let _ = h.publisher.shut_down(Exit::Signal);
}
