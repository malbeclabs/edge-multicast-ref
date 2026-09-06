//! The four messages the walk used to only count.
//!
//! A re-lowering excludes all of them and is right to: each is the publisher's
//! own statement about its own book, lowered from no upstream payload, so a
//! re-lowering has nothing to produce them from. A consumer that *builds* a book
//! needs exactly these four — a cycle is the only anchor a delta book has, and a
//! reset is the only statement that what precedes it is not to be trusted.
//!
//! So they are surfaced beside the comparison rather than inside it, and the
//! tests here are as much about what did not change as about what did.

mod common;

use common::{pack, DatagramLog, Framing, Msg};
use dz_edge_core::PortRole;
use dz_edge_mbp::{
    InstrumentReset, MarketByPrice, SnapshotBegin, SnapshotEnd, SnapshotLevel, MAGIC_MBP, SIDE_BID,
    U16_UNAVAILABLE,
};
use dz_recorder_relower::{StateBody, WireCapture};

const INSTRUMENT: u32 = 11;
const SNAPSHOT: u32 = 7;
const ANCHOR_SEQ: u64 = 4_242;
const RESET_UPSTREAM_GAP: u8 = 3;

fn begin(total_levels: u32) -> Msg {
    Msg::SnapshotBegin(SnapshotBegin {
        instrument_id: INSTRUMENT,
        anchor_seq: ANCHOR_SEQ,
        total_levels,
        snapshot_id: SNAPSHOT,
        last_instrument_seq: 900,
        timestamp_ns: 1_000_000_001,
        depth_bound: 10,
    })
}

fn level(price_raw: i64) -> Msg {
    Msg::SnapshotLevel(SnapshotLevel {
        snapshot_id: SNAPSHOT,
        price_raw,
        qty_raw: 5,
        order_count: U16_UNAVAILABLE,
        side: SIDE_BID,
        level_flags: 0,
    })
}

fn end() -> Msg {
    Msg::SnapshotEnd(SnapshotEnd {
        instrument_id: INSTRUMENT,
        anchor_seq: ANCHOR_SEQ,
        snapshot_id: SNAPSHOT,
    })
}

fn absorb(messages: &[Msg], role: PortRole) -> WireCapture {
    let mut log = DatagramLog::new(pack::<MarketByPrice>(messages, role, Framing::tight()));
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut log, MAGIC_MBP)
        .expect("the log does not fail");
    capture
}

#[test]
fn a_reset_is_not_a_message_this_build_cannot_read() {
    let reset = InstrumentReset {
        instrument_id: INSTRUMENT,
        reason: RESET_UPSTREAM_GAP,
        new_anchor_seq: ANCHOR_SEQ,
        timestamp_ns: 1_000_000_002,
    };

    let capture = absorb(&[Msg::Reset(reset)], PortRole::Mktdata);

    // It went to `unknown_type` before, which claimed the codec had no decoder
    // for it. The codec has had one all along.
    assert_eq!(capture.skipped().unknown_type, 0);
    assert_eq!(capture.skipped().reset, 1);

    let [only] = capture.state_messages() else {
        panic!("one state message, got {}", capture.state_messages().len());
    };
    assert_eq!(only.body, StateBody::Reset(reset));
}

#[test]
fn the_recovery_anchor_survives_the_walk() {
    let capture = absorb(
        &[Msg::Reset(InstrumentReset {
            instrument_id: INSTRUMENT,
            reason: RESET_UPSTREAM_GAP,
            new_anchor_seq: ANCHOR_SEQ,
            timestamp_ns: 1_000_000_002,
        })],
        PortRole::Mktdata,
    );

    // Dropping this field is not lossy, it is unsafe: a consumer without it
    // accepts a snapshot that was already in flight when the reset was
    // published — a book the publisher had disowned — and rebuilds from it.
    let StateBody::Reset(reset) = capture.state_messages()[0].body else {
        panic!("a reset");
    };
    assert_eq!(reset.new_anchor_seq, ANCHOR_SEQ);
}

#[test]
fn a_complete_cycle_appears_in_the_order_the_archive_holds_it() {
    let capture = absorb(
        &[begin(3), level(100), level(99), level(98), end()],
        PortRole::Snapshot,
    );

    let kinds: Vec<&str> = capture
        .state_messages()
        .iter()
        .map(|message| message.body.message_type())
        .collect();
    assert_eq!(
        kinds,
        [
            "SnapshotBegin",
            "SnapshotLevel",
            "SnapshotLevel",
            "SnapshotLevel",
            "SnapshotEnd"
        ]
    );
    assert_eq!(capture.skipped().snapshot, 5);
}

#[test]
fn an_incomplete_cycle_is_surfaced_as_one_rather_than_repaired() {
    // The begin says three and two arrive. Nothing here decides what that means
    // — a consumer must not apply it, and saying so is the consumer's judgement,
    // not the walk's.
    let capture = absorb(
        &[begin(3), level(100), level(99), end()],
        PortRole::Snapshot,
    );

    let StateBody::SnapshotBegin(opened) = capture.state_messages()[0].body else {
        panic!("a begin");
    };
    let levels = capture
        .state_messages()
        .iter()
        .filter(|message| matches!(message.body, StateBody::SnapshotLevel(_)))
        .count();

    assert_eq!(opened.total_levels, 3);
    assert_eq!(levels, 2);
}

#[test]
fn none_of_them_enters_the_comparison() {
    let capture = absorb(&[begin(1), level(100), end()], PortRole::Snapshot);

    // The invariant the widening had to preserve: `messages()` is what a
    // re-lowering compares, and a snapshot is still not in it.
    assert!(capture.messages().is_empty());
    assert_eq!(capture.skipped().snapshot, 3);
}

#[test]
fn a_state_message_states_no_publisher_identity() {
    let capture = absorb(&[begin(1), level(100), end()], PortRole::Snapshot);

    // None of the four carries a `Source ID`. An archive holding only these
    // cannot name its publisher, and a capture that claimed one would be
    // claiming a value it invented — which is what refuses an archive holding
    // two publishers, so it has to rest on what was actually read.
    assert!(capture.source_id().is_err());
}
