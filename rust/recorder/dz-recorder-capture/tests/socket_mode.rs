//! Socket mode's decisions, tested as the pure logic they are: no privileges,
//! no network, no waiting.
//!
//! Addresses are documentation-range (RFC 5737) and MCAST-TEST-NET (RFC 5771)
//! placeholders throughout.

use dz_edge_core::PortRole;
use dz_recorder_capture::rejoin::{can_defer_to_cadence, Rejoiner};
use dz_recorder_capture::socket::{
    is_reprovision_error, ArrivalMetadata, BindPlan, LastSeen, PortBinding, Sighting, SocketSource,
    SocketSourceConfig, SourceGate, SourceKey, SourceVerdict, Synthesiser,
};
use dz_recorder_capture::{bind_or_retry, OverflowTracker, Waited};
use dz_recorder_core::{ChannelInstance, RecvTsKind, Source};
use nix::errno::Errno;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread;
use std::time::{Duration, Instant};

const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 1);
const PORT: u16 = 40000;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

fn stale_after(after: Duration) -> Option<Duration> {
    Some(after)
}

fn no_stale_cadence() -> Option<Duration> {
    None
}

fn silent_for(silence: Duration) -> Duration {
    silence
}

fn joined() -> SocketAddrV4 {
    SocketAddrV4::new(GROUP, PORT)
}

fn source(last_octet: u8) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::new(192, 0, 2, last_octet), 41000)
}

fn synthesiser() -> Synthesiser {
    Synthesiser::new(joined(), PortRole::Mktdata)
}

/// An interface address no host has assigned, so `IP_ADD_MEMBERSHIP` fails the
/// way it does mid-reprovision. Port 0 so the test cannot collide with anything
/// else bound on the machine.
fn absent_interface() -> BindPlan {
    BindPlan {
        binding: PortBinding::new(PortRole::Mktdata, GROUP, 0),
        interface: Ipv4Addr::new(192, 0, 2, 7),
        recv_buffer_bytes: 1 << 20,
        read_timeout: Duration::from_millis(50),
    }
}

#[test]
fn the_first_datagram_on_a_handle_establishes_the_overflow_baseline() {
    // SO_RXQ_OVFL reports a running count, and both counters wrap. Reporting the
    // whole counter as a loss on the first datagram invents an outage.
    let mut t = OverflowTracker::new();
    assert_eq!(t.delta(1_000_000), 0);
    assert_eq!(t.delta(1_000_003), 3);
}

#[test]
fn the_overflow_delta_arithmetic_wraps() {
    let mut t = OverflowTracker::new();
    t.delta(u32::MAX - 1);
    assert_eq!(t.delta(2), 4);
}

#[test]
fn a_replaced_handle_starts_its_own_overflow_baseline() {
    // The counter is per handle. A rejoined or rebound socket inheriting the
    // previous total would report every datagram the old handle ever dropped as
    // one loss on its first datagram.
    let mut first = synthesiser();
    let mut second = synthesiser();
    let meta = ArrivalMetadata {
        overflow_total: Some(1_000_000),
        ..ArrivalMetadata::default()
    };
    assert_eq!(first.arrival(source(1), &meta, || 1).drop_delta, 0);
    assert_eq!(second.arrival(source(1), &meta, || 1).drop_delta, 0);
}

#[test]
fn the_overflow_delta_travels_on_the_datagram() {
    let mut synth = synthesiser();
    let baseline = ArrivalMetadata {
        overflow_total: Some(7),
        ..ArrivalMetadata::default()
    };
    let later = ArrivalMetadata {
        overflow_total: Some(11),
        ..ArrivalMetadata::default()
    };
    synth.arrival(source(1), &baseline, || 1);
    assert_eq!(synth.arrival(source(1), &later, || 1).drop_delta, 4);
}

#[test]
fn a_missing_overflow_control_message_admits_no_loss_rather_than_guessing() {
    let mut synth = synthesiser();
    assert_eq!(
        synth
            .arrival(source(1), &ArrivalMetadata::default(), || 1)
            .drop_delta,
        0
    );
}

#[test]
fn a_missing_timestamp_control_message_falls_back_and_says_so() {
    let mut synth = synthesiser();
    let dg = synth.arrival(source(1), &ArrivalMetadata::default(), || {
        1_700_000_000_000_000_000
    });
    assert_eq!(dg.recv_ts_kind, RecvTsKind::ApplicationFallback);
    assert!(dg.recv_ts_ns > 0);
}

#[test]
fn a_kernel_timestamp_is_carried_as_a_kernel_stamp() {
    // The fallback closure is not called at all: a stamp the kernel produced is
    // never replaced by one measuring our own scheduler.
    let mut synth = synthesiser();
    let meta = ArrivalMetadata {
        kernel_ts_ns: Some(1_700_000_000_123_456_789),
        ..ArrivalMetadata::default()
    };
    let dg = synth.arrival(source(1), &meta, || panic!("the kernel stamped it"));
    assert_eq!(dg.recv_ts_kind, RecvTsKind::KernelSoftware);
    assert_eq!(dg.recv_ts_ns, 1_700_000_000_123_456_789);
}

#[test]
fn an_unobserved_ttl_is_none_rather_than_zero() {
    // Zero is a TTL a datagram can actually carry, so it cannot double as
    // *not observed*.
    let mut synth = synthesiser();
    assert_eq!(
        synth
            .arrival(source(1), &ArrivalMetadata::default(), || 1)
            .ttl,
        None
    );
}

#[test]
fn an_observed_ttl_of_zero_is_recorded_as_zero() {
    let mut synth = synthesiser();
    let meta = ArrivalMetadata {
        ttl: Some(0),
        ..ArrivalMetadata::default()
    };
    assert_eq!(synth.arrival(source(1), &meta, || 1).ttl, Some(0));
}

#[test]
fn the_destination_address_is_the_one_the_kernel_reported() {
    // IP_PKTINFO's ipi_addr is where the datagram was actually sent. The group
    // we believe we joined is a guess by comparison.
    let mut synth = synthesiser();
    let meta = ArrivalMetadata {
        local_dst: Some(Ipv4Addr::new(233, 252, 0, 9)),
        ..ArrivalMetadata::default()
    };
    let dg = synth.arrival(source(1), &meta, || 1);
    assert_eq!(*dg.dst.ip(), Ipv4Addr::new(233, 252, 0, 9));
    assert_eq!(dg.dst.port(), PORT);
}

#[test]
fn the_joined_group_stands_in_when_the_kernel_reported_no_destination() {
    let mut synth = synthesiser();
    let dg = synth.arrival(source(1), &ArrivalMetadata::default(), || 1);
    assert_eq!(dg.dst, joined());
}

#[test]
fn a_synthesised_arrival_borrows_the_payload_it_is_attached_to() {
    let mut synth = synthesiser();
    let arrival = synth.arrival(source(1), &ArrivalMetadata::default(), || 1);
    let buf = vec![7u8; 24];
    let dg = arrival.attach(&buf, 24, None);
    assert_eq!(dg.payload.len(), 24);
    assert_eq!(dg.role.as_str(), "mktdata");
}

#[test]
fn socket_mode_carries_no_link_headers_rather_than_a_rebuilt_one() {
    // The mode saw only a payload. Handing the writer bytes it did not capture
    // is how a synthesised header is mistaken for an observed one.
    let mut synth = synthesiser();
    let arrival = synth.arrival(source(1), &ArrivalMetadata::default(), || 1);
    let buf = vec![7u8; 24];
    assert_eq!(arrival.attach(&buf, 24, None).link_headers, None);
}

#[test]
fn a_stranded_membership_is_rejoined_on_the_stale_cadence() {
    // A membership goes away with the interface it was joined on and nothing
    // reports it: the socket stays open, readable, and permanently silent.
    let r = Rejoiner::new(stale_after(secs(30)));
    assert!(!r.should_rejoin(silent_for(secs(29))));
    assert!(r.should_rejoin(silent_for(secs(31))));
}

#[test]
fn with_no_stale_cadence_silence_is_never_acted_on() {
    // A feed is allowed to be quiet, and a recorder told no cadence has said it
    // does not want silence acted on.
    let r = Rejoiner::new(no_stale_cadence());
    assert!(!r.should_rejoin(silent_for(secs(3600))));
    assert!(!can_defer_to_cadence(no_stale_cadence()));
}

#[test]
fn a_bind_that_fails_during_a_reprovision_retries_rather_than_ending_the_source() {
    // ENODEV from IP_ADD_MEMBERSHIP against an absent interface. Propagating it
    // ends the task before any drain thread exists, so nothing ever retries and
    // the source is dark until a human notices.
    let outcome = bind_or_retry(&absent_interface(), stale_after(secs(30)));
    assert!(matches!(outcome, Ok(None)));
    // With no cadence to retry on, failing loudly beats a thread that can only sleep.
    assert!(bind_or_retry(&absent_interface(), no_stale_cadence()).is_err());
}

#[test]
fn the_reprovision_errnos_are_the_ones_a_host_being_built_produces() {
    assert!(is_reprovision_error(Errno::ENODEV));
    assert!(is_reprovision_error(Errno::EADDRNOTAVAIL));
    // A permission problem or a group already joined is a configuration fault,
    // and waiting for a cadence would only hide it.
    assert!(!is_reprovision_error(Errno::EACCES));
    assert!(!is_reprovision_error(Errno::EADDRINUSE));
}

#[test]
fn a_datagram_from_an_unexpected_source_is_delivered() {
    let mut gate = SourceGate::with_expected_sources([Ipv4Addr::new(192, 0, 2, 1)], 64);
    let mut synth = synthesiser();
    let unexpected = SocketAddrV4::new(Ipv4Addr::new(198, 51, 100, 9), 41000);
    let verdict = gate.observe(SourceKey::new(*unexpected.ip(), PORT));
    let dg = synth.arrival(unexpected, &ArrivalMetadata::default(), || 1);
    assert_eq!(verdict, SourceVerdict::Unexpected);
    assert_eq!(
        dg.src.ip().to_string(),
        "198.51.100.9",
        "gated for counting, never for the archive"
    );
}

#[test]
fn an_expected_source_is_judged_expected() {
    let mut gate = SourceGate::with_expected_sources([Ipv4Addr::new(192, 0, 2, 1)], 64);
    assert_eq!(
        gate.observe(SourceKey::new(Ipv4Addr::new(192, 0, 2, 1), PORT)),
        SourceVerdict::Expected
    );
}

#[test]
fn an_empty_expected_list_is_not_an_expectation_and_cannot_be_violated() {
    let mut gate = SourceGate::with_expected_sources([], 64);
    assert_eq!(
        gate.observe(SourceKey::new(Ipv4Addr::new(198, 51, 100, 9), PORT)),
        SourceVerdict::Expected
    );
}

#[test]
fn per_source_state_is_bounded_and_evicts_the_least_recently_seen() {
    // An any-source join accepts datagrams from any sender, so the key space is
    // not ours to trust. The keys worth keeping are the ones still arriving.
    let mut seen: LastSeen<ChannelInstance> = LastSeen::with_capacity(2);
    let a = ChannelInstance::new(Ipv4Addr::new(192, 0, 2, 1), 1, PORT);
    let b = ChannelInstance::new(Ipv4Addr::new(192, 0, 2, 2), 1, PORT);
    let c = ChannelInstance::new(Ipv4Addr::new(192, 0, 2, 3), 1, PORT);
    assert_eq!(seen.observe(a), Sighting::First);
    assert_eq!(seen.observe(b), Sighting::First);
    assert_eq!(seen.observe(a), Sighting::Again);
    seen.observe(c);
    assert_eq!(seen.len(), 2);
    assert!(seen.contains(&a), "still arriving");
    assert!(seen.contains(&c));
    assert!(!seen.contains(&b), "least recently seen");
    assert_eq!(seen.take_evictions(), 1);
    assert_eq!(seen.take_evictions(), 0, "reported once");
}

#[test]
fn a_source_whose_interface_is_absent_keeps_retrying_instead_of_ending() {
    // The other half of the reprovision case, through the source rather than
    // the policy: the drain thread exists, it is retrying, and nothing has been
    // reported as a lost handle.
    let plan = absent_interface();
    let mut config = SocketSourceConfig::new(
        plan.interface,
        vec![PortBinding::new(PortRole::Mktdata, GROUP, 0)],
    );
    config.read_timeout = Duration::from_millis(5);
    config.stale_after = stale_after(Duration::from_millis(20));

    let mut source =
        SocketSource::bind(&config).expect("a host mid-reprovision is not a bind failure");
    thread::sleep(Duration::from_millis(80));
    assert!(source.stats().bind_retries >= 1);
    assert_eq!(source.stats().datagrams, 0);

    source.stop();
    assert!(matches!(source.next(), Ok(None)));
}

#[test]
fn a_capture_key_becomes_a_channel_instance_once_something_parses() {
    // The record path never parses, so the Channel ID shard is not available at
    // the capture point. The analysis tier supplies it offline, from the bytes.
    let key = SourceKey::new(Ipv4Addr::new(192, 0, 2, 1), PORT);
    assert_eq!(
        key.instance(3),
        ChannelInstance::new(Ipv4Addr::new(192, 0, 2, 1), 3, PORT)
    );
}

#[test]
fn a_bounded_wait_reports_a_quiet_feed_as_quiet_and_not_as_ended() {
    // Source::next has no end for a live source, so its Ok(None) means finished.
    // A caller that read a timeout as that would call a quiet feed a dead one —
    // and a test waiting on a count would hang on a lost datagram instead of
    // failing with what it did receive.
    let bindings = vec![PortBinding::new(
        PortRole::Mktdata,
        "233.252.0.10".parse().unwrap(),
        40_000,
    )];
    let cfg = SocketSourceConfig::new("192.0.2.1".parse().unwrap(), bindings);
    let Ok(mut src) = SocketSource::bind(&cfg) else {
        // No membership here means nothing to observe; the loopback feature
        // covers the path that needs one.
        return;
    };

    let started = Instant::now();
    let outcome = src.next_within(Duration::from_millis(150));
    let waited = started.elapsed();

    assert!(
        matches!(outcome, Ok(Waited::TimedOut)),
        "a quiet feed times out rather than ending"
    );
    assert!(
        waited >= Duration::from_millis(150),
        "the wait is at least the timeout, not a poll interval: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(2),
        "and it is bounded by it: {waited:?}"
    );

    src.stop();
    assert!(
        matches!(src.next_within(Duration::from_secs(5)), Ok(Waited::Ended)),
        "the stop flag ends the wait early rather than burning the timeout"
    );
}
