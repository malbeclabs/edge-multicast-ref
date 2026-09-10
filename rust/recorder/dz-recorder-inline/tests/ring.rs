//! What the ring owes, and what it allocates.
//!
//! Both are properties nothing else in the system can check for it. The archive
//! path has no ring between capture and derivation, so there is no existing test
//! that would notice either going wrong — and the first of them going wrong is
//! not a crash or a wrong number in a metric, it is a `publisher` verdict on a
//! gap this recorder caused.
//!
//! Nothing here needs a socket, a privilege or a server.
#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_core::{RecordedDatagram, RecvTsKind};
use dz_recorder_inline::ring::{ring, Offered, Waited};

const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 10);
const SOURCE: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);

/// Long enough that a timeout means the ring is genuinely empty rather than
/// that a thread had not run yet — and this is all one thread, so it never
/// actually waits.
const WAIT: Duration = Duration::from_millis(100);

fn datagram(payload: &[u8], drop_delta: u32) -> RecordedDatagram<'_> {
    RecordedDatagram {
        payload,
        src: SocketAddrV4::new(SOURCE, 50_000),
        dst: SocketAddrV4::new(GROUP, 40_000),
        role: PortRole::Mktdata,
        recv_ts_ns: 1_700_000_000_000_000_000,
        recv_ts_kind: RecvTsKind::KernelSoftware,
        drop_delta,
        ttl: Some(64),
        link_headers: None,
        wire_payload_len: payload.len() as u32,
    }
}

/// The delta the next datagram out of the ring declares.
fn delta_of(waited: Waited<'_>) -> u32 {
    match waited {
        Waited::Datagram(dg) => dg.drop_delta,
        other => panic!("expected a datagram, found {other:?}"),
    }
}

/// **The test the whole module exists for.**
///
/// A datagram the derivation never saw is a sequence value nobody delivered. If
/// its loss is not charged to the next datagram that gets through, the rows show
/// a gap with nothing admitted behind it — and the verdict on a gap with nothing
/// admitted behind it is `publisher`. So this asserts that the recorder's own
/// drop is admitted as the recorder's, which is the difference between a correct
/// finding and an accusation against somebody else.
///
/// Revert the charge in `offer` and this fails.
#[test]
fn a_datagram_the_ring_could_not_take_is_admitted_by_the_next_one_that_gets_through() {
    let (mut tx, mut rx) = ring(2);

    assert_eq!(tx.offer(&datagram(b"one", 0)), Offered::Accepted);
    assert_eq!(tx.offer(&datagram(b"two", 0)), Offered::Accepted);
    // Nothing has drained, so there is no slot left and this one is lost.
    assert_eq!(tx.offer(&datagram(b"three", 0)), Offered::Dropped);
    assert_eq!(tx.owed(), 1, "the lost datagram is owed");

    assert_eq!(
        delta_of(rx.recv_within(WAIT)),
        0,
        "the first admitted nothing"
    );
    assert_eq!(
        delta_of(rx.recv_within(WAIT)),
        0,
        "and neither did the second: the loss happened after them"
    );

    assert_eq!(tx.offer(&datagram(b"four", 0)), Offered::Accepted);
    assert_eq!(tx.owed(), 0, "the debt travelled with it");
    assert_eq!(
        delta_of(rx.recv_within(WAIT)),
        1,
        "the datagram after the loss is the one that admits it"
    );
}

/// A run of drops is charged once, in full, to the datagram that ends it.
///
/// Not once per drop and not once at all: `drop_delta` is defined as the count
/// lost between the previous datagram and this one, so a run of three is a three
/// on the next one that gets through. A test that only dropped one datagram
/// would pass against an implementation that set the delta to `1` rather than
/// accumulating.
#[test]
fn a_run_of_drops_is_charged_in_full_to_the_datagram_that_ends_it() {
    let (mut tx, mut rx) = ring(2);

    assert_eq!(tx.offer(&datagram(b"one", 0)), Offered::Accepted);
    assert_eq!(tx.offer(&datagram(b"two", 0)), Offered::Accepted);
    for _ in 0..3 {
        assert_eq!(tx.offer(&datagram(b"lost", 0)), Offered::Dropped);
    }
    assert_eq!(tx.owed(), 3);

    assert_eq!(delta_of(rx.recv_within(WAIT)), 0);
    // The second call is what puts the first slot back: the deriver holds the
    // one it handed out until it is asked for another.
    assert_eq!(delta_of(rx.recv_within(WAIT)), 0);
    assert_eq!(tx.offer(&datagram(b"next", 0)), Offered::Accepted);
    assert_eq!(
        delta_of(rx.recv_within(WAIT)),
        3,
        "all three, on one datagram"
    );
}

/// A datagram lost at the ring takes its own admission with it.
///
/// It arrives already declaring what the capture lost before it. Dropping it
/// without owing that forward loses the capture's admission as well as the
/// datagram — the archive path's own rule, one layer up, and the reason `offer`
/// owes before it tries rather than after it succeeds.
#[test]
fn a_dropped_datagram_owes_what_it_was_already_declaring() {
    let (mut tx, mut rx) = ring(2);

    assert_eq!(tx.offer(&datagram(b"one", 0)), Offered::Accepted);
    assert_eq!(tx.offer(&datagram(b"two", 0)), Offered::Accepted);
    // This one arrives admitting five losses of the capture's own, and then
    // does not get through either.
    assert_eq!(tx.offer(&datagram(b"lost", 5)), Offered::Dropped);
    assert_eq!(
        tx.owed(),
        6,
        "five it was carrying, and itself: dropping it must not discard the five"
    );

    assert_eq!(delta_of(rx.recv_within(WAIT)), 0);
    assert_eq!(delta_of(rx.recv_within(WAIT)), 0);
    assert_eq!(tx.offer(&datagram(b"next", 0)), Offered::Accepted);
    assert_eq!(delta_of(rx.recv_within(WAIT)), 6);
}

/// The slots are reused, so a steady feed allocates nothing per datagram.
///
/// Asserted on the buffer addresses the derivation is handed, which is the only
/// observable this can be checked on without an allocator hook: over many more
/// datagrams than there are slots, the payloads must come back in the same few
/// buffers. An implementation that built an owned datagram per arrival would
/// hand out a fresh address almost every time.
#[test]
fn a_steady_feed_reuses_the_slots_rather_than_allocating_one_per_datagram() {
    const CAPACITY: usize = 4;
    const DATAGRAMS: usize = 200;

    let (mut tx, mut rx) = ring(CAPACITY);
    let mut buffers: BTreeSet<usize> = BTreeSet::new();

    for i in 0..DATAGRAMS {
        let payload = format!("datagram-{i}");
        assert_eq!(
            tx.offer(&datagram(payload.as_bytes(), 0)),
            Offered::Accepted
        );
        match rx.recv_within(WAIT) {
            Waited::Datagram(dg) => {
                buffers.insert(dg.payload.as_ptr() as usize);
                assert_eq!(
                    dg.payload,
                    payload.as_bytes(),
                    "and the bytes are the ones sent"
                );
            }
            other => panic!("expected a datagram, found {other:?}"),
        }
    }

    assert!(
        buffers.len() <= CAPACITY,
        "{DATAGRAMS} datagrams came back in {} distinct buffers, and the ring has {CAPACITY} slots",
        buffers.len()
    );
}

/// A ring nobody is draining refuses rather than waits.
///
/// The capture thread calls this. A version that blocked would stop draining the
/// receive queue behind it, and a slow derivation would become feed loss — plus
/// a false publisher-loss finding in every window derived while it lasted.
#[test]
fn a_full_ring_refuses_at_once_rather_than_waiting_for_room() {
    let (mut tx, _rx) = ring(2);

    let start = std::time::Instant::now();
    assert_eq!(tx.offer(&datagram(b"one", 0)), Offered::Accepted);
    assert_eq!(tx.offer(&datagram(b"two", 0)), Offered::Accepted);
    for _ in 0..50 {
        assert_eq!(tx.offer(&datagram(b"lost", 0)), Offered::Dropped);
    }
    assert!(
        start.elapsed() < Duration::from_millis(50),
        "offering into a full ring waited: {:?}",
        start.elapsed()
    );
    assert_eq!(tx.counters().dropped(), 50);
    assert_eq!(tx.counters().accepted(), 2);
}

/// A quiet feed times out, and a timeout is not an ending.
///
/// The window bound needs the difference: a feed that has gone quiet must still
/// let a window close on age, and a feed whose capture has stopped must end the
/// derivation instead.
#[test]
fn a_quiet_ring_times_out_and_a_closed_one_ends() {
    let (tx, mut rx) = ring(1);
    assert!(matches!(
        rx.recv_within(Duration::from_millis(5)),
        Waited::TimedOut
    ));
    drop(tx);
    assert!(matches!(rx.recv_within(WAIT), Waited::Ended));
}

/// A derivation that has gone is reported as gone, and never as an endless drop.
///
/// **The two look identical on every counter and mean opposite things.** A full
/// ring is ordinary and self-correcting: the deriver catches up and the next
/// datagram is accepted. A deriver that is not there produces the same drop and
/// the same counter for ever, and it took its slots with it — so a sender
/// waiting for a free slot in order to notice would be waiting for one that is
/// never coming back.
///
/// Both ends of the ring have to say so, because which one answers depends on
/// whether a slot happened to be free when the deriver went. Give the sender a
/// sending end of the free list — so that the free list stays connected for as
/// long as the sender lives — and the first half of this fails with `Dropped`,
/// for ever.
#[test]
fn a_ring_whose_deriver_is_gone_says_so_from_either_end() {
    // Both slots spent before the deriver goes, so the free list is empty and
    // its own disconnection is what has to answer.
    let (mut tx, rx) = ring(2);
    assert_eq!(tx.offer(&datagram(b"one", 0)), Offered::Accepted);
    assert_eq!(tx.offer(&datagram(b"two", 0)), Offered::Accepted);
    drop(rx);

    assert_eq!(tx.offer(&datagram(b"lost", 7)), Offered::Disconnected);
    assert_eq!(
        tx.owed(),
        8,
        "the datagram and the seven it declared are still owed"
    );
    assert_eq!(tx.counters().dropped(), 1, "and it is counted as a drop");
    assert_eq!(
        tx.offer(&datagram(b"lost", 0)),
        Offered::Disconnected,
        "and it keeps saying so"
    );

    // A slot free when the deriver goes, so the full list is what answers.
    let (mut tx, rx) = ring(2);
    drop(rx);
    assert_eq!(tx.offer(&datagram(b"lost", 3)), Offered::Disconnected);
    assert_eq!(tx.owed(), 4);
    assert_eq!(tx.counters().dropped(), 1);
}
