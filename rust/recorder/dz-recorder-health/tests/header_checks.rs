//! What the tier concludes from the 24 bytes themselves: latency and its
//! timestamp kind, the declared length against the cap and against the wire, and
//! the header values it counts rather than judges.

mod common;

use common::{
    has_sample, observer_with_limits, observer_with_sources, sample, Arrival, Datagram, FEED,
    MAGIC, PUBLISHER_A, SECOND_NS, T0,
};
use dz_edge_core::PortRole;
use dz_edge_core::{DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE, SCHEMA_VERSION, SIZE_HEARTBEAT};
use dz_recorder_core::{CaptureDropScope, Observer, RecvTsKind};

const MKTDATA: &[(&str, &str)] = &[("feed", FEED), ("port_role", "mktdata")];

fn with_role(extra: (&'static str, &str)) -> Vec<(&'static str, String)> {
    vec![
        ("feed", FEED.to_owned()),
        ("port_role", "mktdata".to_owned()),
        (extra.0, extra.1.to_owned()),
    ]
}

fn role_sample(rendered: &str, metric: &str, extra: (&'static str, &str)) -> f64 {
    let owned = with_role(extra);
    let labels: Vec<(&str, &str)> = owned
        .iter()
        .map(|(key, value)| (*key, value.as_str()))
        .collect();
    sample(rendered, metric, &labels)
}

/// A stamp the kernel did not produce measures the recorder's own scheduler.
/// Averaging the two kinds together measures neither, so the fallback is counted
/// and never observed.
#[test]
fn an_application_fallback_stamp_stays_out_of_the_latency_histogram() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.recv_ts_kind = RecvTsKind::ApplicationFallback;
    arrival.recv_ts_ns = T0 + SECOND_NS / 2;
    observer.on_datagram(&arrival.recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_count",
            MKTDATA
        ),
        0.0,
        "the histogram must hold no observation from an application stamp"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_sum",
            MKTDATA
        ),
        0.0
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_latency_samples_dropped_total",
            ("reason", "application_fallback")
        ),
        1.0,
        "counted separately instead, which is the histogram's missing denominator"
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_recv_timestamps_total",
            ("kind", "application_fallback")
        ),
        1.0
    );
    assert_eq!(
        sample(&rendered, "dz_recorder_datagrams_total", MKTDATA),
        1.0,
        "the datagram itself is still counted; only the latency is withheld"
    );
}

#[test]
fn a_kernel_stamp_enters_the_latency_histogram() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(
        &Arrival::from(PUBLISHER_A)
            .at(T0 + SECOND_NS / 1000)
            .recorded(&payload),
    );

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_count",
            MKTDATA
        ),
        1.0
    );
    assert!(
        (sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_sum",
            MKTDATA
        ) - 0.001)
            .abs()
            < 1e-9,
        "one millisecond, in seconds"
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_recv_timestamps_total",
            ("kind", "kernel_software")
        ),
        1.0
    );
}

/// A receive stamp before the send stamp is two clocks disagreeing about the
/// order of events. There is no non-negative duration to observe.
#[test]
fn a_receive_stamp_before_the_send_stamp_is_counted_and_not_observed() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.send_timestamp_ns = T0 + SECOND_NS;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_count",
            MKTDATA
        ),
        0.0
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_latency_samples_dropped_total",
            ("reason", "negative_interval")
        ),
        1.0
    );
}

#[test]
fn a_declared_length_over_the_cap_is_a_violation() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.declared_len = Some(u16::try_from(MAX_DATAGRAM_SIZE).unwrap() + 1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_violations_total",
            ("kind", "over_cap")
        ),
        1.0
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_violations_total",
            ("kind", "under_header")
        ),
        0.0
    );
    assert_eq!(
        sample(&rendered, "dz_recorder_datagrams_total", MKTDATA),
        1.0,
        "an oversized declaration is counted, never discarded: discarding it \
         would turn the violation into a sequence gap the publisher is blamed for"
    );
    assert_eq!(
        common::instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        1.0,
        "continuity accounting still uses the header a malformed datagram carries"
    );
}

#[test]
fn a_declared_length_below_the_header_is_the_other_violation() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.declared_len = Some(8);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_violations_total",
            ("kind", "under_header")
        ),
        1.0
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_violations_total",
            ("kind", "over_cap")
        ),
        0.0
    );
}

/// A separate violation from the one above, and asserted separately: a declared
/// length can be perfectly in range and still describe a different datagram than
/// the one delivered.
#[test]
fn a_declared_length_disagreeing_with_the_received_length_is_a_mismatch() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.declared_len = Some(u16::try_from(datagram.payload_len).unwrap() + 4);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_mismatch_total",
            ("kind", "declared_exceeds_received")
        ),
        1.0
    );
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_violations_total",
            ("kind", "over_cap")
        ),
        0.0,
        "the declared length is inside the mandated range; only the wire \
         disagrees with it"
    );

    let mut datagram = Datagram::seq(2);
    datagram.declared_len = Some(u16::try_from(datagram.payload_len).unwrap() - 4);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));
    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_mismatch_total",
            ("kind", "declared_below_received")
        ),
        1.0
    );
}

/// The capture length cuts a datagram short, and the wire length is what says so.
/// Comparing the declared length against the bytes that survived the capture
/// would report every truncated datagram as a publisher fault.
#[test]
fn a_truncated_capture_is_not_reported_as_a_length_mismatch() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.wire_payload_len = Some(u32::try_from(payload.len()).unwrap());
    let truncated = &payload[..32];
    observer.on_datagram(&arrival.recorded(truncated));

    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_declared_length_mismatch_total",
            ("kind", "declared_exceeds_received")
        ),
        0.0
    );
    assert_eq!(
        sample(&rendered, "dz_recorder_bytes_total", MKTDATA),
        payload.len() as f64,
        "the wire length is what the feed's rate is measured from"
    );
}

/// Required to be counted by value rather than judged. Through
/// `DatagramHeader::decode` this datagram is simply undecodable and the tier
/// learns nothing about exactly the traffic most worth knowing about.
#[test]
fn an_unknown_schema_version_is_counted_by_value() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.schema_version = 99;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_schema_version_total",
            &[("feed", FEED), ("schema_version", "99")]
        ),
        1.0,
        "the version is the label value, so an operator sees which one arrived"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_schema_version_total",
            &[("feed", FEED), ("schema_version", "other")]
        ),
        0.0,
        "one unknown version is inside the distinct-value budget"
    );
    assert_eq!(
        common::instance_sample(&rendered, "dz_recorder_sequence_current", PUBLISHER_A),
        1.0,
        "an unknown version does not stop continuity accounting"
    );
}

#[test]
fn a_known_schema_version_is_counted_under_its_own_value() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_schema_version_total",
            &[
                ("feed", FEED),
                ("schema_version", &SCHEMA_VERSION.to_string())
            ]
        ),
        1.0
    );
}

/// A datagram misrouted from another feed is exactly what counting `Magic` by
/// value answers.
#[test]
fn a_magic_from_another_feed_is_counted_by_value() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let mut datagram = Datagram::seq(1);
    datagram.magic = 0xbeef;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_magic_total",
            &[("feed", FEED), ("magic", "0xbeef")]
        ),
        1.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_magic_total",
            &[("feed", FEED), ("magic", &format!("0x{MAGIC:04x}"))]
        ),
        0.0
    );
}

/// `Magic` is 16 bits of sender-controlled label on an any-source join, so the
/// distinct values that get a series of their own are budgeted and the rest are
/// folded into one.
#[test]
fn magic_values_beyond_the_budget_are_folded_into_other() {
    let (metrics, mut observer) = observer_with_limits(
        &[PUBLISHER_A],
        dz_recorder_health::InstanceLimits {
            max_distinct_header_values: 2,
            ..dz_recorder_health::InstanceLimits::default()
        },
    );
    // The expected Magic already occupies one of the two slots.
    for magic in [0x0001_u16, 0x0002, 0x0003] {
        let mut datagram = Datagram::seq(1);
        datagram.magic = magic;
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));
    }

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_magic_total",
            &[("feed", FEED), ("magic", "0x0001")]
        ),
        1.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagram_magic_total",
            &[("feed", FEED), ("magic", "other")]
        ),
        2.0,
        "the budget was spent, so 0x0002 and 0x0003 share one series"
    );
    assert!(!has_sample(
        &rendered,
        "dz_recorder_datagram_magic_total",
        &[("magic", "0x0003")]
    ));
}

#[test]
fn a_datagram_shorter_than_the_header_is_counted_as_unreadable() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let payload = [0_u8; 8];
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        role_sample(
            &rendered,
            "dz_recorder_unreadable_datagrams_total",
            ("reason", "short_header")
        ),
        1.0
    );
    assert_eq!(
        sample(&rendered, "dz_recorder_datagrams_total", MKTDATA),
        1.0,
        "it arrived and it was archived; only this tier concluded nothing"
    );
    assert_eq!(observer.instances_tracked(), 0);
}

/// Our own losses, recorded before anything is concluded about the feed: a gap
/// covered by this is not a publisher finding.
#[test]
fn capture_drops_are_charged_to_the_recorder() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let datagram = Datagram::seq(6);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.drop_delta = 4;
    observer.on_datagram(&arrival.recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(&rendered, "dz_recorder_capture_drops_total", MKTDATA),
        4.0
    );
    assert_eq!(
        common::instance_sample(
            &rendered,
            "dz_recorder_missing_datagrams_on_arrival_total",
            PUBLISHER_A
        ),
        4.0,
        "the gap is still reported; it is the subtraction that makes it \
         attributable, and the subtraction is the analysis tier's"
    );
}

/// The recorder's own losses that no datagram carries, taken as explicit calls
/// rather than inferred from something.
#[test]
fn interface_drops_rejoins_and_evicted_segments_are_recorded_when_reported() {
    let (metrics, observer) = observer_with_sources(&[PUBLISHER_A]);
    observer.record_interface_drops(7);
    observer.record_rejoin(dz_edge_core::PortRole::Mktdata);
    observer.record_rejoin(dz_edge_core::PortRole::Snapshot);
    observer.record_segment_evicted();
    observer.record_segment_evicted();

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_interface_drops_total",
            &[("feed", FEED)]
        ),
        7.0
    );
    assert_eq!(sample(&rendered, "dz_recorder_rejoins_total", MKTDATA), 1.0);
    assert!(
        !has_sample(
            &rendered,
            "dz_recorder_rejoins_total",
            &[("port_role", "snapshot")]
        ),
        "a rejoin on a role this feed was not declared to carry has no series"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_segments_evicted_total",
            &[("site", common::SITE)]
        ),
        2.0,
        "recorder-wide, because the staging budget is"
    );
}

/// The header alone cannot read a message type, so cadence is measured from the
/// one shape the header does decide: a single message of the heartbeat's size.
#[test]
fn heartbeat_cadence_is_measured_between_heartbeat_shaped_datagrams() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    for (index, sequence_number) in [1_u64, 2, 3].into_iter().enumerate() {
        let datagram = Datagram::seq(sequence_number).heartbeat();
        let payload = datagram.payload();
        observer.on_datagram(
            &Arrival::from(PUBLISHER_A)
                .at(T0 + index as u64 * SECOND_NS)
                .recorded(&payload),
        );
    }

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_heartbeat_interval_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "0")]
        ),
        2.0,
        "three heartbeats are two intervals: the first establishes the baseline"
    );
    assert!(
        (sample(
            &rendered,
            "dz_recorder_heartbeat_interval_seconds_sum",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "0")]
        ) - 2.0)
            .abs()
            < 1e-9
    );
    assert_eq!(
        common::instance_sample(
            &rendered,
            "dz_recorder_heartbeat_last_timestamp_seconds",
            PUBLISHER_A
        ),
        (T0 + 2 * SECOND_NS) as f64 / 1e9
    );
}

#[test]
fn ordinary_traffic_does_not_count_as_a_heartbeat() {
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);
    for sequence_number in [1_u64, 2] {
        let datagram = Datagram::seq(sequence_number);
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));
    }

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_heartbeat_interval_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "0")]
        ),
        0.0
    );
    assert_eq!(
        common::instance_sample(
            &rendered,
            "dz_recorder_heartbeat_last_timestamp_seconds",
            PUBLISHER_A
        ),
        0.0
    );
}

#[test]
fn an_interval_longer_than_the_widest_bucket_is_counted_and_not_observed() {
    // Symmetric with the negative interval, and for the same reason. The send
    // timestamp is a field on the wire; a datagram carrying nearly zero
    // observes a billion-second interval into a histogram whose average the
    // help text points an operator at, and _sum does not come back down. The
    // series is (feed, role)-scoped, so the bounded instance map contains
    // nothing here.
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    let mut datagram = Datagram::seq(1);
    datagram.send_timestamp_ns = 1_000;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_latency_samples_dropped_total",
            &[
                ("feed", FEED),
                ("port_role", "mktdata"),
                ("reason", "implausible_interval")
            ]
        ),
        1.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_sum",
            &[("feed", FEED), ("port_role", "mktdata")]
        ),
        0.0,
        "one datagram poisoned the average for the life of the counter"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_send_to_recv_latency_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata")]
        ),
        0.0
    );
}

#[test]
fn a_heartbeat_is_recognised_by_its_size_on_the_wire_and_not_by_what_it_declares() {
    // Three lines above, the declared length is checked against the wire
    // because the wire is the truth. The cadence series carries no source
    // label, so a sender that merely claims the heartbeat shape writes
    // fabricated cadence into a declared channel's percentiles — which is how a
    // real silence is masked.
    let (metrics, mut observer) = observer_with_sources(&[PUBLISHER_A]);

    for at in [T0, T0 + 30 * SECOND_NS] {
        let mut datagram = Datagram::seq(1);
        datagram.msg_count = 1;
        // Ordinary traffic, declaring itself heartbeat-shaped.
        datagram.declared_len = Some(u16::try_from(DATAGRAM_HEADER_SIZE + SIZE_HEARTBEAT).unwrap());
        let payload = datagram.payload();
        observer.on_datagram(&Arrival::from(PUBLISHER_A).at(at).recorded(&payload));
    }

    assert_eq!(
        sample(
            &metrics.render(),
            "dz_recorder_heartbeat_interval_seconds_count",
            &[("feed", FEED), ("port_role", "mktdata"), ("channel", "0")]
        ),
        0.0,
        "a claim about a datagram's shape wrote a cadence the channel never had"
    );
}

#[test]
fn a_ring_wide_drop_is_not_charged_to_the_role_of_whatever_arrived_next() {
    // AF_PACKET is the default mode, and its ring counts frames dropped before
    // anything demultiplexed them into port roles. Charging that number to the
    // arriving datagram's role attributes our own loss to a feed that may have
    // lost nothing — and the archive this same process writes branches on the
    // scope, so the live metrics would contradict the object on disk. A
    // per-role subtraction of a handle-wide quantity manufactures a publisher
    // finding out of arithmetic.
    let limits = dz_recorder_health::InstanceLimits::default();
    let (metrics, mut observer) =
        common::observer_with_scope(&[PUBLISHER_A], limits, CaptureDropScope::CaptureHandle);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.drop_delta = 9;
    observer.on_datagram(&arrival.recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_capture_drops_handle_total",
            &[("feed", FEED)]
        ),
        9.0,
        "the quantity the capture actually reported"
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_capture_drops_total",
            &[("feed", FEED), ("port_role", "mktdata")]
        ),
        0.0,
        "a ring-wide drop was charged to one role"
    );
}

#[test]
fn a_per_role_drop_is_still_charged_to_its_role() {
    // The other scope, unchanged: socket mode keeps one accumulator per role,
    // and a per-instance subtraction of that number is a valid one.
    let limits = dz_recorder_health::InstanceLimits::default();
    let (metrics, mut observer) =
        common::observer_with_scope(&[PUBLISHER_A], limits, CaptureDropScope::PortRole);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.drop_delta = 4;
    observer.on_datagram(&arrival.recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_capture_drops_total",
            &[("feed", FEED), ("port_role", "mktdata")]
        ),
        4.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_capture_drops_handle_total",
            &[("feed", FEED)]
        ),
        0.0
    );
}

#[test]
fn our_own_loss_survives_a_datagram_on_a_role_this_feed_does_not_carry() {
    // The capture hands a delta over exactly once. Returning before it is
    // counted makes admitted loss exist nowhere in the exposition, and loss
    // that is admitted nowhere reads as a publisher gap. Under AF_PACKET, where
    // one handle carries every role, traffic on an undeclared role is exactly
    // what is most likely to be present.
    let limits = dz_recorder_health::InstanceLimits::default();
    let (metrics, mut observer) =
        common::observer_with_scope(&[PUBLISHER_A], limits, CaptureDropScope::CaptureHandle);

    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.role = PortRole::Snapshot;
    arrival.port = common::SNAPSHOT_PORT;
    arrival.drop_delta = 6;
    observer.on_datagram(&arrival.recorded(&payload));

    let rendered = metrics.render();
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_datagrams_unexpected_role_total",
            &[("feed", FEED)]
        ),
        1.0
    );
    assert_eq!(
        sample(
            &rendered,
            "dz_recorder_capture_drops_handle_total",
            &[("feed", FEED)]
        ),
        6.0,
        "the delta went nowhere and our loss reads as the publisher's"
    );
}
