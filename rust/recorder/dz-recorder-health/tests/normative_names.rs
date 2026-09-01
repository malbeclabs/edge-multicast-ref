//! Every family in the normative set must render under its exact name, reached
//! by the recording path rather than by pre-creation.
//!
//! This is the test that stops a rename, and it declares no sources so that
//! every channel-instance series here was opened by a datagram: pre-creation
//! could otherwise make a family that nothing can actually record to look
//! present.

mod common;

use std::time::Duration;

use common::{
    observer_with_limits, Arrival, Datagram, MKTDATA_PORT, NORMATIVE_NAMES, PUBLISHER_A,
    PUBLISHER_B, REFDATA_PORT, SECOND_NS, SNAPSHOT_PORT, STRANGER, T0,
};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_recorder_core::{Observer, RecvTsKind};
use dz_recorder_health::InstanceLimits;

#[test]
fn every_normative_family_renders_under_its_exact_name() {
    let (metrics, mut observer) = observer_with_limits(
        &[],
        InstanceLimits {
            max_instances: 2,
            min_evict_age: Duration::ZERO,
            ..InstanceLimits::default()
        },
    );

    // In order, then a gap, then a duplicate, then a reordering, then a reset,
    // then backward motion beyond the window.
    for (index, (sequence_number, reset_count)) in [
        (1_u64, 0_u8),
        (2, 0),
        (7, 0),
        (7, 0),
        (5, 0),
        (0, 1),
        (100_000, 1),
        (1, 1),
    ]
    .into_iter()
    .enumerate()
    {
        let datagram = Datagram::seq(sequence_number).with_reset(reset_count);
        let payload = datagram.payload();
        observer.on_datagram(
            &Arrival::from(PUBLISHER_A)
                .at(T0 + index as u64 * SECOND_NS)
                .recorded(&payload),
        );
    }

    // Two heartbeat-shaped datagrams, for the cadence histogram and its gauge.
    for (index, sequence_number) in [100_001_u64, 100_002].into_iter().enumerate() {
        let datagram = Datagram::seq(sequence_number).with_reset(1).heartbeat();
        let payload = datagram.payload();
        observer.on_datagram(
            &Arrival::from(PUBLISHER_A)
                .at(T0 + (20 + index as u64) * SECOND_NS)
                .recorded(&payload),
        );
    }

    // An application-level stamp, and a receive stamp before the send stamp.
    let datagram = Datagram::seq(100_003).with_reset(1);
    let payload = datagram.payload();
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.recv_ts_kind = RecvTsKind::ApplicationFallback;
    arrival.drop_delta = 3;
    observer.on_datagram(&arrival.recorded(&payload));

    let mut datagram = Datagram::seq(100_004).with_reset(1);
    datagram.send_timestamp_ns = T0 + 100 * SECOND_NS;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).at(T0).recorded(&payload));

    // A declared length over the cap, and one that disagrees with the wire.
    let mut datagram = Datagram::seq(100_005).with_reset(1);
    datagram.declared_len = Some(u16::try_from(MAX_DATAGRAM_SIZE).unwrap() + 1);
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    // An unknown schema version, and a Magic from another feed.
    let mut datagram = Datagram::seq(100_006).with_reset(1);
    datagram.schema_version = 99;
    datagram.magic = 0xbeef;
    let payload = datagram.payload();
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&payload));

    // Too short to hold a header.
    observer.on_datagram(&Arrival::from(PUBLISHER_A).recorded(&[0_u8; 4]));

    // A second and a third instance, so the map's bound is reached and an
    // eviction is counted.
    let datagram = Datagram::seq(1);
    let payload = datagram.payload();
    for (index, source) in [PUBLISHER_B, STRANGER].into_iter().enumerate() {
        observer.on_datagram(
            &Arrival::from(source)
                .at(T0 + (50 + index as u64) * SECOND_NS)
                .recorded(&payload),
        );
    }

    // The refdata role, so both declared roles carry traffic.
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.role = PortRole::Refdata;
    arrival.port = REFDATA_PORT;
    observer.on_datagram(&arrival.recorded(&payload));

    // The undeclared role.
    let mut arrival = Arrival::from(PUBLISHER_A);
    arrival.role = PortRole::Snapshot;
    arrival.port = SNAPSHOT_PORT;
    observer.on_datagram(&arrival.recorded(&payload));

    // The losses no datagram carries.
    observer.record_interface_drops(2);
    observer.record_rejoin(PortRole::Mktdata);
    observer.record_segment_evicted();

    assert_eq!(MKTDATA_PORT, 30001, "the fixture's port role mapping");

    let rendered = metrics.render();
    for name in NORMATIVE_NAMES {
        assert!(
            rendered.contains(&format!("# TYPE {name} ")),
            "missing normative metric family {name} in:\n{rendered}"
        );
    }
}

/// The label set is normative too. `site`, `recorder`, `feed`, `channel`, `role`
/// and `source` — and nothing that names a venue.
#[test]
fn no_family_carries_a_label_outside_the_normative_set() {
    let metrics = common::metrics_with_sources(&[PUBLISHER_A]);
    let rendered = metrics.render();

    const ALLOWED: &[&str] = &[
        "site",
        "recorder",
        "feed",
        "channel",
        "port_role",
        "source",
        // Taxonomy dimensions, each a closed Rust enum in this crate.
        "kind",
        "reason",
        "magic",
        "schema_version",
        // The histogram bucket boundary the exposition format itself adds.
        "le",
    ];

    for line in rendered
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
    {
        let labels = line
            .split_once('{')
            .and_then(|(_, rest)| rest.rsplit_once('}'))
            .map(|(labels, _)| labels)
            .unwrap_or_default();
        for pair in labels.split("\",") {
            let Some((key, _)) = pair.split_once('=') else {
                continue;
            };
            let key = key.trim().trim_start_matches(',');
            assert!(
                ALLOWED.contains(&key),
                "label `{key}` is outside the normative set: {line}"
            );
        }
    }
}
