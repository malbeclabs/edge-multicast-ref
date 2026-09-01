//! The real thing: a publisher's encoder sends over a UDP multicast socket,
//! [`SocketSource`] receives it, the archive keeps it, and replay proves the
//! bytes that left the encoder are the bytes that came back.
//!
//! Everything else in this crate proves that two functions agree in memory. This
//! is the suite that proves the datagram went out and was captured — the kernel
//! stamped it, the receive path carried it, and no field was invented on the way
//! through.
//!
//! Behind the `socket-e2e` feature, which is off by default so that the default
//! build asks nothing of the host: it needs `IP_MULTICAST_LOOP` delivery to
//! this host on a documentation-range group, and a workstation or container
//! without a multicast route must still be able to test this workspace. CI
//! turns the feature on in its own job, behind a probe that proves the runner
//! can deliver multicast to itself, so this suite runs on every pull request.
#![cfg(feature = "socket-e2e")]
#![forbid(unsafe_code)]

mod common;

use std::net::{Ipv4Addr, SocketAddrV4, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use common::{encode, fresh, record_joined, replay, Msg, RawHeader, Recorded, GROUP};
use dz_edge_core::{
    Datagram, DatagramHeader, DecodeError, Feed, PortRole, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE,
};
use dz_edge_tob::TopOfBook;
use dz_recorder_archive::writer::RoleJoin;
use dz_recorder_archive::ArchiveWriter;
use dz_recorder_capture::{PortBinding, SocketSource, SocketSourceConfig};
use dz_recorder_core::{ChannelInstance, RecvTsKind, Sink, Source};
use dz_recorder_replay::OwnedDatagram;

/// The multicast path this host has to itself. `lo` carries a documentation-range
/// group under `IP_MULTICAST_LOOP` without any route being configured for it,
/// which is what makes this runnable by hand on a workstation.
const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;
const LOOPBACK_INTERFACE: &str = "lo";

/// A port per test, because the tests in this binary run in parallel and two
/// sockets joined to the same group and port would each receive the other's
/// traffic.
const ROUND_TRIP_MKTDATA_PORT: u16 = 41971;
const ROUND_TRIP_REFDATA_PORT: u16 = 41972;
const OVER_CAP_PORT: u16 = 41973;
const KERNEL_STAMP_PORT: u16 = 41974;
/// One port, two groups: the layout where a recorder that filters by port alone
/// records the wrong feed.
const SHARED_PORT: u16 = 41975;

/// How long a datagram sent on the loopback path is given to arrive.
///
/// This is a timeout and not a synchronisation: nothing here sleeps for a
/// datagram, the capture is waited on by count, and this bound only decides how
/// long a failure takes to report itself.
const ARRIVAL_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a recorder is watched for datagrams that must never arrive.
///
/// Unlike [`ARRIVAL_TIMEOUT`] this one is always spent: the test asks each
/// recorder for both streams and the correct answer is that half of them never
/// come. Short enough to keep the suite quick, long enough that a datagram
/// looping back on this host has had many times the time it needs.
const MIXING_TIMEOUT: Duration = Duration::from_millis(750);

const UNKNOWN_SCHEMA_CHANNEL: u8 = 11;
const FOREIGN_MAGIC_CHANNEL: u8 = 15;
const SHORT_CHANNEL: u8 = 17;
const MKTDATA_CHANNEL: u8 = 7;
const REFDATA_CHANNEL: u8 = 9;
const OVER_CAP_CHANNEL: u8 = 31;
const UNIMPLEMENTED_SCHEMA_VERSION: u8 = 9;
const ANOTHER_FEEDS_MAGIC: u16 = 0x445B;

/// A second group, on the same port as [`GROUP`]. Documentation range, like
/// every other address in this suite.
const OTHER_GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 11);
const OURS_CHANNEL: u8 = 41;
const THEIRS_CHANNEL: u8 = 42;

/// A datagram about to go over the wire, and a name for it, so a datagram that
/// does not arrive can be named in the failure rather than counted.
struct Outgoing {
    label: &'static str,
    role: PortRole,
    port: u16,
    payload: Vec<u8>,
}

fn binding(role: PortRole, port: u16) -> PortBinding {
    PortBinding::new(role, GROUP, port)
}

fn joins(bindings: &[PortBinding]) -> Vec<RoleJoin> {
    bindings
        .iter()
        .map(|b| RoleJoin {
            role: b.role,
            group: b.group,
            port: b.port,
            interface: Some(LOOPBACK_INTERFACE.to_owned()),
            // Observed rather than configured: this is the address the join was
            // actually made on.
            source: Some(LOOPBACK),
        })
        .collect()
}

/// Binds every port role before anything is sent.
///
/// The receiver exists first because a multicast datagram sent before the join
/// is a datagram nothing asked for, and a test that raced the two would be
/// flaky in exactly the direction that looks like loss.
fn receiver(bindings: &[PortBinding]) -> SocketSource {
    let mut config = SocketSourceConfig::new(LOOPBACK, bindings.to_vec());
    config.read_timeout = Duration::from_millis(10);
    // No rejoin cadence, and so no deferral of a failed bind either: a host with
    // no multicast path must fail here, naming the errno, rather than retrying
    // quietly and leaving the test to time out with nothing to say.
    config.stale_after = None;
    SocketSource::bind(&config).unwrap_or_else(|e| {
        panic!(
            "no multicast socket on {LOOPBACK} for {GROUP}: {e}. This suite needs \
             IP_MULTICAST_LOOP delivery to this host; it is behind a feature for that reason."
        )
    })
}

fn sender() -> UdpSocket {
    let socket = UdpSocket::bind((LOOPBACK, 0)).expect("a sending socket on the loopback address");
    // The datagram has to come back to this host, which is the whole mechanism
    // this suite rests on.
    socket
        .set_multicast_loop_v4(true)
        .expect("IP_MULTICAST_LOOP");
    // One hop: the traffic is for this host and must not leave it.
    socket.set_multicast_ttl_v4(1).expect("IP_MULTICAST_TTL");
    socket
}

/// Receives until `want` datagrams have arrived or the timeout expires, writing
/// each one into the archive as it comes.
///
/// [`Source::next`] blocks until a datagram or the stop flag — a live feed may be
/// quiet, and nothing else may end the wait — so the timeout arrives through the
/// flag. The write happens inside the loop because that is the loop a recorder
/// host runs: capture, then sink, with nothing buffered in between.
fn capture(
    source: &mut SocketSource,
    writer: &mut ArchiveWriter,
    want: usize,
    timeout: Duration,
) -> Vec<OwnedDatagram> {
    let stop = source.stop_flag();
    let finished = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let finished = Arc::clone(&finished);
        thread::spawn(move || {
            let deadline = Instant::now() + timeout;
            while Instant::now() < deadline {
                if finished.load(Ordering::Relaxed) {
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
            stop.store(true, Ordering::Relaxed);
        })
    };

    let mut received = Vec::with_capacity(want);
    while received.len() < want {
        match Source::next(source) {
            Ok(Some(dg)) => {
                Sink::write(writer, &dg).expect("the write path never fails the caller");
                received.push(OwnedDatagram::from_recorded(&dg));
            }
            // The stop flag: the timeout expired, and the caller reports what
            // arrived.
            Ok(None) => break,
            Err(e) => panic!(
                "the capture handle failed after {} datagrams: {e}",
                received.len()
            ),
        }
    }

    finished.store(true, Ordering::Relaxed);
    watchdog.join().expect("the watchdog thread");
    received
}

/// Sends everything, captures it, and archives it.
fn round_trip(outgoing: &[Outgoing], bindings: &[PortBinding]) -> (Vec<OwnedDatagram>, Recorded) {
    let mut source = receiver(bindings);
    let sender = sender();
    for dg in outgoing {
        sender
            .send_to(&dg.payload, SocketAddrV4::new(GROUP, dg.port))
            .unwrap_or_else(|e| panic!("sending {}: {e}", dg.label));
    }

    let mut captured = Vec::new();
    let archive = record_joined(joins(bindings), |writer| {
        captured = capture(&mut source, writer, outgoing.len(), ARRIVAL_TIMEOUT);
        // Inside the closure, because record_joined rotates on the way out and
        // an empty segment produces no object — so a run where nothing looped
        // back fails on "a segment that held datagrams produces an object",
        // which names neither the datagrams that went missing nor the capture
        // stats that would say why. The first failure a reader sees should be
        // the one that happened.
        assert_arrived(outgoing, &captured, &source);
        captured.len() as u64
    });

    (captured, archive)
}

/// Names what arrived and what did not, because a bare count says nothing about
/// which datagram the path lost.
fn assert_arrived(outgoing: &[Outgoing], captured: &[OwnedDatagram], source: &SocketSource) {
    let missing: Vec<&str> = outgoing
        .iter()
        .filter(|dg| !captured.iter().any(|back| back.payload == dg.payload))
        .map(|dg| dg.label)
        .collect();
    let arrived: Vec<&str> = outgoing
        .iter()
        .filter(|dg| captured.iter().any(|back| back.payload == dg.payload))
        .map(|dg| dg.label)
        .collect();
    assert!(
        missing.is_empty(),
        "{} of {} datagrams arrived within {ARRIVAL_TIMEOUT:?}: {arrived:?}; missing: {missing:?}; \
         capture stats {:?}",
        captured.len(),
        outgoing.len(),
        source.stats()
    );
    assert_eq!(captured.len(), outgoing.len());
}

/// The correct half of the stream: an advancing sequence, an era that begins
/// again, more than one message per datagram, and two port roles.
fn correct(outgoing: &mut Vec<Outgoing>) {
    let mut mktdata = fresh(MKTDATA_CHANNEL);
    for (label, msgs) in [
        ("mktdata era 0 seq 0", &[Msg::Quote(1), Msg::Trade(1)][..]),
        (
            "mktdata era 0 seq 1",
            &[Msg::Quote(1), Msg::Quote(2), Msg::Heartbeat][..],
        ),
        ("mktdata era 0 seq 2", &[Msg::Trade(2), Msg::Heartbeat][..]),
    ] {
        outgoing.push(Outgoing {
            label,
            role: PortRole::Mktdata,
            port: ROUND_TRIP_MKTDATA_PORT,
            payload: encode(mktdata, PortRole::Mktdata, msgs),
        });
        mktdata.advance();
    }
    mktdata.begin_era();
    for (label, msgs) in [
        ("mktdata era 1 seq 0", &[Msg::Quote(1), Msg::Trade(1)][..]),
        ("mktdata era 1 seq 1", &[Msg::Heartbeat, Msg::Quote(2)][..]),
    ] {
        outgoing.push(Outgoing {
            label,
            role: PortRole::Mktdata,
            port: ROUND_TRIP_MKTDATA_PORT,
            payload: encode(mktdata, PortRole::Mktdata, msgs),
        });
        mktdata.advance();
    }

    let mut refdata = fresh(REFDATA_CHANNEL);
    for (label, msgs) in [
        (
            "refdata seq 0",
            &[Msg::ManifestSummary(1), Msg::InstrumentDefinition(1)][..],
        ),
        (
            "refdata seq 1",
            &[Msg::InstrumentDefinition(2), Msg::InstrumentDefinition(3)][..],
        ),
    ] {
        outgoing.push(Outgoing {
            label,
            role: PortRole::Refdata,
            port: ROUND_TRIP_REFDATA_PORT,
            payload: encode(refdata, PortRole::Refdata, msgs),
        });
        refdata.advance();
    }
}

/// The wrong half. Each of these is a datagram every conformant subscriber
/// discards, which is exactly why it has to survive the socket, the archive and
/// the replay unchanged.
fn malformed(outgoing: &mut Vec<Outgoing>) {
    let mut unknown_schema = encode(
        fresh(UNKNOWN_SCHEMA_CHANNEL),
        PortRole::Mktdata,
        &[Msg::Quote(1)],
    );
    unknown_schema[2] = UNIMPLEMENTED_SCHEMA_VERSION;
    outgoing.push(Outgoing {
        label: "unknown schema version",
        role: PortRole::Mktdata,
        port: ROUND_TRIP_MKTDATA_PORT,
        payload: unknown_schema,
    });

    let mut foreign_magic = encode(
        fresh(FOREIGN_MAGIC_CHANNEL),
        PortRole::Mktdata,
        &[Msg::Quote(1)],
    );
    foreign_magic[0..2].copy_from_slice(&ANOTHER_FEEDS_MAGIC.to_le_bytes());
    outgoing.push(Outgoing {
        label: "another feed's magic",
        role: PortRole::Mktdata,
        port: ROUND_TRIP_MKTDATA_PORT,
        payload: foreign_magic,
    });

    outgoing.push(Outgoing {
        label: "shorter than the 24-byte header",
        role: PortRole::Mktdata,
        port: ROUND_TRIP_MKTDATA_PORT,
        payload: encode(fresh(SHORT_CHANNEL), PortRole::Mktdata, &[Msg::Quote(1)])[..12].to_vec(),
    });
}

#[test]
fn a_datagram_the_encoder_sends_over_a_real_socket_is_the_datagram_the_archive_replays() {
    let mut outgoing = Vec::new();
    correct(&mut outgoing);
    malformed(&mut outgoing);
    let bindings = [
        binding(PortRole::Mktdata, ROUND_TRIP_MKTDATA_PORT),
        binding(PortRole::Refdata, ROUND_TRIP_REFDATA_PORT),
    ];

    let (captured, archive) = round_trip(&outgoing, &bindings);

    // One drain thread per port role, so order is preserved within a role and
    // nothing promises it across two.
    for role in [PortRole::Mktdata, PortRole::Refdata] {
        let sent: Vec<&Vec<u8>> = outgoing
            .iter()
            .filter(|dg| dg.role == role)
            .map(|dg| &dg.payload)
            .collect();
        let back: Vec<&Vec<u8>> = captured
            .iter()
            .filter(|dg| dg.role == role)
            .map(|dg| &dg.payload)
            .collect();
        assert_eq!(back, sent, "the {} stream, in order", role.as_str());
    }

    let sender_port = captured[0].src.port();
    for dg in &captured {
        assert_eq!(*dg.src.ip(), LOOPBACK, "the sender is this host");
        assert_eq!(dg.src.port(), sender_port, "one sending socket");
        assert_eq!(*dg.dst.ip(), GROUP);
        assert_eq!(
            dg.dst.port(),
            match dg.role {
                PortRole::Mktdata => ROUND_TRIP_MKTDATA_PORT,
                PortRole::Refdata => ROUND_TRIP_REFDATA_PORT,
                PortRole::Snapshot => unreachable!("no snapshot port was joined"),
            },
            "IP_PKTINFO and the binding agree about where it was sent"
        );
        assert_eq!(
            dg.recv_ts_kind,
            RecvTsKind::KernelSoftware,
            "SCM_TIMESTAMPNS, not our own clock"
        );
        assert!(dg.ttl.is_some(), "IP_RECVTTL reported it");
        assert_eq!(dg.drop_delta, 0, "nothing was lost on the loopback path");
        assert!(
            dg.link_headers.is_none(),
            "socket mode sees a payload, and the headers in the archive are synthesised"
        );
        assert_eq!(
            dg.wire_payload_len as usize,
            dg.payload.len(),
            "nothing here is over the mandated cap"
        );
    }

    let replayed = replay(&archive.object);
    assert_eq!(
        replayed, captured,
        "the archive did not replay what came off the socket"
    );

    assert_eq!(archive.manifest.datagram_count, captured.len() as u64);
    assert_eq!(
        archive.manifest.short_datagrams, 1,
        "the datagram with no header is counted rather than skipped"
    );
    assert_eq!(archive.manifest.capture_drop_total, 0);
    assert_eq!(archive.manifest.link_headers, "synthesised");

    let coverage = archive
        .manifest
        .instances
        .get(&ChannelInstance::new(
            LOOPBACK,
            MKTDATA_CHANNEL,
            ROUND_TRIP_MKTDATA_PORT,
        ))
        .expect("the mktdata channel instance is described");
    assert_eq!(
        (coverage.first_seq, coverage.last_seq, coverage.count),
        (0, 1, 5)
    );
    assert_eq!(
        coverage.reset_counts_seen,
        vec![0, 1],
        "the era that began again crossed the socket intact"
    );
    assert!(
        archive
            .manifest
            .instances
            .contains_key(&ChannelInstance::new(
                LOOPBACK,
                UNKNOWN_SCHEMA_CHANNEL,
                ROUND_TRIP_MKTDATA_PORT
            )),
        "a datagram whose schema version is unknown is still described"
    );

    // A decoder refuses three of them, which is the point: what reached the
    // archive is traffic no subscriber would have passed on.
    let refused = replayed
        .iter()
        .filter(|dg| Datagram::decode(&dg.payload, TopOfBook::MAGIC).is_err())
        .count();
    assert_eq!(
        refused, 3,
        "the three malformed datagrams are all still there"
    );
}

#[test]
fn a_datagram_over_the_mandated_cap_is_archived_truncated_and_declares_what_arrived() {
    // 1300 bytes on the wire: past the 1232-byte cap every feed spec mandates,
    // which `DatagramBuilder` clamps its capacity to and therefore cannot
    // produce. Archiving its first 1232 bytes as though that were the whole
    // datagram would turn the violation into a clean one, and discarding it
    // would turn the violation into a sequence gap the publisher is blamed for.
    let over_cap: u16 = 1300;
    let body_len = usize::from(over_cap) - DATAGRAM_HEADER_SIZE;
    let mut body = vec![0u8; body_len];
    for (index, byte) in body.iter_mut().enumerate() {
        // A pattern, so a truncation at the wrong offset is visible rather than
        // hidden in a field of zeros.
        *byte = u8::try_from(index % 251).expect("a modulus below 256");
    }
    let mut header = RawHeader::conformant(OVER_CAP_CHANNEL, 0, body_len);
    header.datagram_len = over_cap;
    let payload = header.followed_by(&body);
    assert_eq!(payload.len(), usize::from(over_cap));

    let bindings = [binding(PortRole::Mktdata, OVER_CAP_PORT)];
    let mut source = receiver(&bindings);
    let sender = sender();
    sender
        .send_to(&payload, SocketAddrV4::new(GROUP, OVER_CAP_PORT))
        .expect("sending a datagram over the cap");

    let mut captured = Vec::new();
    let archive = record_joined(joins(&bindings), |writer| {
        captured = capture(&mut source, writer, 1, ARRIVAL_TIMEOUT);
        captured.len() as u64
    });
    assert_eq!(
        captured.len(),
        1,
        "no datagram arrived within {ARRIVAL_TIMEOUT:?}; capture stats {:?}",
        source.stats()
    );

    let arrived = &captured[0];
    assert_eq!(
        arrived.payload.len(),
        MAX_DATAGRAM_SIZE,
        "the receive buffer is the mandated cap, so this is all there was room for"
    );
    assert_eq!(arrived.payload, payload[..MAX_DATAGRAM_SIZE]);
    assert_eq!(
        arrived.wire_payload_len,
        u32::from(over_cap),
        "MSG_TRUNC reported the whole length, which is the violation"
    );
    assert_eq!(
        source.stats().truncated_datagrams,
        1,
        "a publisher over the cap is a counter and not a curiosity"
    );

    let replayed = replay(&archive.object);
    assert_eq!(replayed, captured, "including the length that arrived");
    assert_eq!(replayed[0].wire_payload_len, u32::from(over_cap));

    // The header is readable — it has to be, or the violation could not be
    // attributed — and a conformant subscriber still refuses the datagram.
    let peeked = DatagramHeader::peek(&replayed[0].payload).expect("the header is present");
    assert_eq!(peeked.datagram_len, over_cap);
    assert!(!peeked.declared_len_is_in_range());
    assert_eq!(
        Datagram::decode(&replayed[0].payload, TopOfBook::MAGIC).err(),
        Some(DecodeError::DeclaredLengthOutOfRange {
            declared: over_cap,
            min: DATAGRAM_HEADER_SIZE,
            max: MAX_DATAGRAM_SIZE,
        })
    );
}

#[test]
fn every_datagram_off_the_socket_carries_a_kernel_stamp_and_not_the_application_fallback() {
    // A socket test that silently exercised the fallback path would be proving
    // less than it looks: a latency computed from an application stamp measures
    // the recorder's own scheduler, and `RecvTsKind` exists so that nothing
    // downstream can mistake one for the other.
    let bindings = [binding(PortRole::Mktdata, KERNEL_STAMP_PORT)];
    let mut source = receiver(&bindings);
    let sender = sender();

    let mut sequence = fresh(MKTDATA_CHANNEL);
    let mut payloads = Vec::new();
    let before_ns = realtime_ns();
    for _ in 0..3 {
        let payload = encode(sequence, PortRole::Mktdata, &[Msg::Quote(1), Msg::Trade(1)]);
        sender
            .send_to(&payload, SocketAddrV4::new(GROUP, KERNEL_STAMP_PORT))
            .expect("sending");
        payloads.push(payload);
        sequence.advance();
    }

    let mut captured = Vec::new();
    let archive = record_joined(joins(&bindings), |writer| {
        captured = capture(&mut source, writer, payloads.len(), ARRIVAL_TIMEOUT);
        captured.len() as u64
    });
    let after_ns = realtime_ns();
    assert_eq!(
        captured.len(),
        payloads.len(),
        "{} of {} datagrams arrived within {ARRIVAL_TIMEOUT:?}; capture stats {:?}",
        captured.len(),
        payloads.len(),
        source.stats()
    );

    for (index, dg) in captured.iter().enumerate() {
        assert_eq!(
            dg.recv_ts_kind,
            RecvTsKind::KernelSoftware,
            "datagram {index} fell back to an application stamp"
        );
        assert!(
            (before_ns..=after_ns).contains(&dg.recv_ts_ns),
            "datagram {index}'s stamp {} is outside the window this test ran in \
             ({before_ns}..={after_ns})",
            dg.recv_ts_ns
        );
    }
    assert!(
        captured
            .windows(2)
            .all(|pair| pair[0].recv_ts_ns <= pair[1].recv_ts_ns),
        "the stamps do not advance: {:?}",
        captured.iter().map(|dg| dg.recv_ts_ns).collect::<Vec<_>>()
    );
    assert_eq!(
        source.stats().cmsg_truncations,
        0,
        "a control buffer too small for what the kernel sent is what explains an archive full of \
         fallback stamps"
    );

    // And the kind survives into the archive, which is where a reader asks
    // whether a latency number is measuring the network or our own scheduler.
    let replayed = replay(&archive.object);
    assert_eq!(replayed, captured);
    assert!(replayed
        .iter()
        .all(|dg| dg.recv_ts_kind == RecvTsKind::KernelSoftware));
}

fn realtime_ns() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).expect("a nanosecond count since 1970 fits a u64"))
        .expect("the clock is set after 1970")
}

#[test]
fn two_groups_on_one_port_each_record_their_own_and_nothing_of_the_other() {
    // Two feeds, one port, two groups — a layout the configuration blesses and
    // an operator will eventually deploy. Each recorder must come back with its
    // own datagrams and none of the other's, end to end: encoder, socket,
    // archive, replay, and the manifest's coverage rows.
    //
    // What makes this worth an end-to-end test rather than a unit one is the
    // part no unit test can reach. The kernel decides what a handle is given,
    // and with a wildcard bind it gives it every group any socket on the host
    // has joined — so a recorder can be entirely correct about the datagrams it
    // was handed and still archive a feed nobody asked it to keep. And it fails
    // quietly: ChannelInstance is (source, channel id, port), with no group in
    // it, so the two sequence spaces merge into one coverage row and the
    // archive that results reads as a single feed with impossible gaps.
    let ours_binding = PortBinding::new(PortRole::Mktdata, GROUP, SHARED_PORT);
    let theirs_binding = PortBinding::new(PortRole::Mktdata, OTHER_GROUP, SHARED_PORT);

    // Both recorders before either sender: a datagram sent before the join is a
    // datagram nothing asked for.
    let mut ours_source = receiver(&[ours_binding]);
    let mut theirs_source = receiver(&[theirs_binding]);

    let mut ours_seq = fresh(OURS_CHANNEL);
    let mut theirs_seq = fresh(THEIRS_CHANNEL);
    let mut ours_sent = Vec::new();
    let mut theirs_sent = Vec::new();
    let sender = sender();
    for _ in 0..4 {
        let ours = encode(ours_seq, PortRole::Mktdata, &[Msg::Quote(1), Msg::Trade(1)]);
        let theirs = encode(
            theirs_seq,
            PortRole::Mktdata,
            &[Msg::Quote(2), Msg::Heartbeat],
        );
        sender
            .send_to(&ours, SocketAddrV4::new(GROUP, SHARED_PORT))
            .expect("sending to our group");
        sender
            .send_to(&theirs, SocketAddrV4::new(OTHER_GROUP, SHARED_PORT))
            .expect("sending to the other group");
        ours_sent.push(ours);
        theirs_sent.push(theirs);
        ours_seq.advance();
        theirs_seq.advance();
    }

    // Each recorder is asked for *both* streams. A recorder that filters
    // correctly returns its own four and waits out the timeout for the rest,
    // which is the only shape of this test that can fail when the filtering
    // stops working: asking for four would be satisfied by the wrong four.
    let want = ours_sent.len() + theirs_sent.len();
    let mut ours_captured = Vec::new();
    let ours_archive = record_joined(joins(&[ours_binding]), |writer| {
        ours_captured = capture(&mut ours_source, writer, want, MIXING_TIMEOUT);
        ours_captured.len() as u64
    });
    let mut theirs_captured = Vec::new();
    let theirs_archive = record_joined(joins(&[theirs_binding]), |writer| {
        theirs_captured = capture(&mut theirs_source, writer, want, MIXING_TIMEOUT);
        theirs_captured.len() as u64
    });

    for (name, archive, mine, other) in [
        ("ours", &ours_archive, &ours_sent, &theirs_sent),
        ("theirs", &theirs_archive, &theirs_sent, &ours_sent),
    ] {
        let replayed: Vec<Vec<u8>> = replay(&archive.object)
            .iter()
            .map(|dg| dg.payload.clone())
            .collect();
        assert_eq!(
            replayed, *mine,
            "{name}: the archive is not exactly this group's datagrams, in order"
        );
        for stray in other.iter() {
            assert!(
                !replayed.contains(stray),
                "{name}: a datagram addressed to the other group reached the archive"
            );
        }
    }

    // And the coverage rows, which is where the mixing would survive a byte
    // comparison: one instance each, its own Channel ID, four datagrams.
    for (name, archive, channel) in [
        ("ours", &ours_archive, OURS_CHANNEL),
        ("theirs", &theirs_archive, THEIRS_CHANNEL),
    ] {
        let instances = &archive.manifest.instances;
        assert_eq!(
            instances.len(),
            1,
            "{name}: two channel instances in a manifest that saw one feed"
        );
        let (instance, coverage) = instances.iter().next().expect("the one instance");
        assert_eq!(instance.channel_id, channel, "{name}: the wrong feed");
        assert_eq!(instance.dst_port, SHARED_PORT, "{name}");
        assert_eq!(coverage.count, 4, "{name}");
    }

    assert_eq!(
        ours_source.stats().foreign_group_datagrams,
        0,
        "the bind refuses them, so nothing should reach the check behind it"
    );
}
