//! Provenance carries the channel instance, not just the position in it.
//!
//! A sequence number is only meaningful under `(source address, Channel ID,
//! destination port)`. The walk had the first and third on every datagram and
//! dropped them, which is fine for a comparison — it never asks where a message
//! came from — and is not fine for anything deriving rows, which cannot name the
//! instance a row belongs to without them.

mod common;

use std::net::{Ipv4Addr, SocketAddrV4};

use common::{pack, DatagramLog, Framing, Msg, SOURCE_ID};
use dz_edge_core::PortRole;
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_core::RecvTsKind;
use dz_recorder_relower::WireCapture;

const INSTRUMENT: u32 = 11;
const BOTH_UPDATED: u8 = 0x03;

/// The second path serving one channel. Same `Channel ID`, same group, same
/// port — a different publisher, and nothing but the address says so.
const SECOND_PATH: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);

fn quote(ts: u64) -> Msg {
    Msg::Quote(Quote {
        instrument_id: INSTRUMENT,
        source_id: SOURCE_ID,
        update_flags: BOTH_UPDATED,
        source_timestamp_ns: ts,
        bid_price: 9_950,
        bid_qty: 12,
        ask_price: 10_050,
        ask_qty: 7,
        bid_source_count: 0,
        ask_source_count: 0,
    })
}

fn absorb(datagrams: Vec<common::OwnedDatagram>) -> WireCapture {
    let mut log = DatagramLog::new(datagrams);
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut log, MAGIC_TOB)
        .expect("the log does not fail");
    capture
}

#[test]
fn provenance_carries_the_address_and_port_the_datagram_arrived_on() {
    let datagrams = pack::<TopOfBook>(&[quote(1_000_000_001)], PortRole::Mktdata, Framing::tight());
    let expected_src = datagrams[0].src;
    let expected_dst = datagrams[0].dst;

    let capture = absorb(datagrams);
    let provenance = capture.messages()[0].provenance;

    assert_eq!(provenance.src, expected_src);
    assert_eq!(provenance.dst, expected_dst);
    assert_eq!(provenance.role, PortRole::Mktdata);
}

#[test]
fn two_paths_serving_one_channel_are_told_apart_by_provenance() {
    let mut first = pack::<TopOfBook>(&[quote(1_000_000_001)], PortRole::Mktdata, Framing::tight());
    let mut second =
        pack::<TopOfBook>(&[quote(1_000_000_001)], PortRole::Mktdata, Framing::tight());
    for datagram in &mut second {
        datagram.src = SocketAddrV4::new(SECOND_PATH, datagram.src.port());
    }
    first.append(&mut second);

    let capture = absorb(first);
    let [one, two] = [
        capture.messages()[0].provenance,
        capture.messages()[1].provenance,
    ];

    // The whole point: identical in everything a sequence number is read
    // against, and different in the one field that says they are two publishers
    // rather than one publisher going backwards.
    assert_eq!(one.channel_id, two.channel_id);
    assert_eq!(one.dst, two.dst);
    assert_eq!(one.sequence_number, two.sequence_number);
    assert_ne!(one.src, two.src);
    assert_eq!(*two.src.ip(), SECOND_PATH);
}

#[test]
fn a_fallback_receive_stamp_stays_visible_as_one() {
    let mut datagrams =
        pack::<TopOfBook>(&[quote(1_000_000_001)], PortRole::Mktdata, Framing::tight());
    datagrams[0].recv_ts_kind = RecvTsKind::ApplicationFallback;

    let capture = absorb(datagrams);

    // A latency taken from a stamp this process wrote measures this process, so
    // a consumer reporting one has to be able to say which kind it had. Losing
    // the distinction turns an unusable number into a plausible one.
    assert_eq!(
        capture.messages()[0].provenance.recv_ts_kind,
        RecvTsKind::ApplicationFallback
    );
}

#[test]
fn the_kernel_stamp_is_what_a_recorded_datagram_normally_carries() {
    let datagrams = pack::<TopOfBook>(&[quote(1_000_000_001)], PortRole::Mktdata, Framing::tight());

    let capture = absorb(datagrams);

    assert_eq!(
        capture.messages()[0].provenance.recv_ts_kind,
        RecvTsKind::KernelSoftware
    );
}
