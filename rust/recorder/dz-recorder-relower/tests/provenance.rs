//! Provenance carries the channel instance, not just the position in it.
//!
//! A sequence number is only meaningful under `(source address, Channel ID,
//! destination port)`. The walk had the first and third on every datagram and
//! dropped them, which is fine for a comparison — it never asks where a message
//! came from — and is not fine for anything deriving rows, which cannot name the
//! instance a row belongs to without them.

mod common;

use std::net::{Ipv4Addr, SocketAddrV4};

use common::{
    pack, port_for, refdata_datagrams, DatagramLog, Framing, Listed, Msg, CHANNEL_ID,
    PRIMARY_SOURCE, SOURCE_ID,
};
use dz_edge_core::{Heartbeat, PortRole};
use dz_edge_mbp::MAGIC_MBP;
use dz_edge_tob::{Quote, TopOfBook, MAGIC_TOB};
use dz_recorder_core::{ChannelInstance, RecvTsKind};
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
    absorb_as(datagrams, MAGIC_TOB)
}

fn absorb_as(datagrams: Vec<common::OwnedDatagram>, magic: u16) -> WireCapture {
    let mut log = DatagramLog::new(datagrams);
    let mut capture = WireCapture::new();
    capture
        .absorb(&mut log, magic)
        .expect("the log does not fail");
    capture
}

fn instance(role: PortRole) -> ChannelInstance {
    ChannelInstance::new(PRIMARY_SOURCE, CHANNEL_ID, port_for(role))
}

fn heartbeat(ts: u64) -> Msg {
    Msg::Heartbeat(Heartbeat {
        channel_id: CHANNEL_ID,
        timestamp_ns: ts,
    })
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

/// A datagram that yielded no message is still a datagram.
///
/// The tally cannot be reconstructed from the messages' own provenance, and this
/// is the case that says so: a `Heartbeat` datagram carries the header the
/// transport tier writes a row from and produces nothing here. A consumer
/// dividing by the datagrams the messages named would divide by the busy ones
/// only.
#[test]
fn a_datagram_that_yielded_no_message_is_still_counted() {
    let capture = absorb(pack::<TopOfBook>(
        &[heartbeat(1), heartbeat(2), heartbeat(3)],
        PortRole::Mktdata,
        Framing::tight(),
    ));

    assert!(capture.messages().is_empty());
    assert_eq!(capture.skipped().control, 3);
    assert_eq!(
        capture
            .datagrams_by_instance()
            .get(&instance(PortRole::Mktdata)),
        Some(&3)
    );
}

/// A datagram of another feed is counted against no instance of this one.
///
/// `datagrams()` counts everything the sources yielded, which is right for a
/// report about the read; this counts what carried the magic asked for, which is
/// the only denominator a per-feed number can have. An archive holding two feeds
/// would otherwise make each of them look half as dense as it is.
#[test]
fn a_foreign_magic_is_counted_against_no_instance() {
    let mut datagrams = pack::<TopOfBook>(
        &[quote(1_000_000_001), quote(1_000_000_002)],
        PortRole::Mktdata,
        Framing::tight(),
    );
    datagrams.append(&mut pack::<TopOfBook>(
        &[quote(1_000_000_003)],
        PortRole::Mktdata,
        Framing::tight(),
    ));

    let capture = absorb_as(datagrams, MAGIC_MBP);

    assert_eq!(capture.datagrams(), 3, "three datagrams were read");
    assert_eq!(capture.skipped().foreign_magic, 3);
    assert!(
        capture.datagrams_by_instance().is_empty(),
        "none of them was this feed's"
    );
}

/// One channel across two port roles is two instances in the tally.
///
/// The walk files a datagram under the instance its header and its destination
/// port name, and does not decide how a consumer groups them: a sizing
/// measurement folds the roles into a feed and a sequence tracker must not.
#[test]
fn two_port_roles_are_two_instances() {
    let mut datagrams = pack::<TopOfBook>(
        &[quote(1_000_000_001), quote(1_000_000_002)],
        PortRole::Mktdata,
        Framing::tight(),
    );
    // One definition and the manifest that declares it: two datagrams on the
    // role a feed's reference data actually arrives on.
    datagrams.append(&mut refdata_datagrams::<TopOfBook>(
        &[Listed::new("AAA", INSTRUMENT, -2, 0)],
        3,
    ));

    let capture = absorb(datagrams);

    assert_eq!(capture.datagrams_by_instance().len(), 2);
    assert_eq!(
        capture
            .datagrams_by_instance()
            .get(&instance(PortRole::Mktdata)),
        Some(&2)
    );
    assert_eq!(
        capture
            .datagrams_by_instance()
            .get(&instance(PortRole::Refdata)),
        Some(&2)
    );
}
