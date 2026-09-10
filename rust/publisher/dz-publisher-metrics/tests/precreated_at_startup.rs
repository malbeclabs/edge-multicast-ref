//! Every family whose labels are entirely a closed set (an enum, or the
//! port roles a publisher was constructed with) must render at 0 the
//! instant `PublisherMetrics::new` returns, before anything has been
//! recorded. A dashboard panel or a `== 0` alert on one of these series
//! must see an actual zero, not "no data" - and an operator must be able
//! to tell "this publisher has sent nothing yet" from "this publisher
//! does not implement this metric".
//!
//! Families whose labels are open-ended (the upstream source's own
//! `message_type` vocabulary, a deployment-chosen `connection` or
//! `channel_id`, or caller-supplied `build_info` values) are the
//! deliberate opposite: they must stay absent until first touched, so
//! their absence keeps meaning something.

use std::collections::BTreeMap;

use dz_edge_core::PortRole;
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};

/// Finds the sample line for `metric` (an exact family name, or a
/// histogram sub-series such as `..._count`) carrying every label in
/// `labels`, regardless of the order Prometheus renders labels in.
fn find_sample<'a>(rendered: &'a str, metric: &str, labels: &[(&str, &str)]) -> &'a str {
    rendered
        .lines()
        .find(|line| {
            line.strip_prefix(metric)
                .is_some_and(|rest| rest.starts_with('{'))
                && labels
                    .iter()
                    .all(|(k, v)| line.contains(&format!("{k}=\"{v}\"")))
        })
        .unwrap_or_else(|| {
            panic!("no sample line for {metric} with labels {labels:?} in:\n{rendered}")
        })
}

fn assert_zero(rendered: &str, metric: &str, labels: &[(&str, &str)]) {
    let line = find_sample(rendered, metric, labels);
    assert!(
        line.ends_with(" 0"),
        "expected {metric}{labels:?} to render 0 at construction, got: {line}"
    );
}

#[test]
fn every_closed_label_family_renders_zero_at_construction() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for reason in ["schema", "unknown_field", "malformed", "truncated"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_parse_errors_total",
            &[("reason", reason)],
        );
    }

    for reason in ["timeout", "remote_close", "rate_limit", "auth_expired"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_reconnects_total",
            &[("reason", reason)],
        );
    }

    for reason in [
        "refused",
        "unresolved",
        "tls",
        "timeout",
        "unauthorized",
        "rate_limit",
        "rejected",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_connect_failures_total",
            &[("reason", reason)],
        );
    }

    for reason in ["not_ready", "unknown_instrument", "internal"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_adapter_errors_total",
            &[("reason", reason)],
        );
    }

    for reason in [
        "unknown_instrument",
        "inexact_contract",
        "too_precise",
        "malformed",
        "overflow",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_lowering_refusals_total",
            &[("reason", reason)],
        );
    }

    for kind in [
        "missing_level",
        "crossed_book",
        "snapshot_mismatch",
        "sequence_gap",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_book_inconsistency_total",
            &[("kind", kind)],
        );
    }

    for outcome in ["success", "failed"] {
        assert_zero(
            &rendered,
            "dz_publisher_book_recovery_total",
            &[("outcome", outcome)],
        );
    }

    for reason in ["timeout", "rate_limit", "schema", "unavailable"] {
        assert_zero(
            &rendered,
            "dz_publisher_refdata_load_errors_total",
            &[("reason", reason)],
        );
    }

    for reason in ["idle_guard", "consistency_guard", "signal", "panic"] {
        assert_zero(
            &rendered,
            "dz_publisher_exit_reason_total",
            &[("reason", reason)],
        );
    }

    for port_role in ["mktdata", "refdata"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_datagrams_total",
            &[("port_role", port_role)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_egress_bytes_total",
            &[("port_role", port_role)],
        );
        for reason in [
            "mtu_exceeded",
            "send_would_block",
            "socket_error",
            "not_registered",
            "wrong_port_role",
            "not_carried_by_feed",
            "malformed_message",
        ] {
            assert_zero(
                &rendered,
                "dz_publisher_egress_errors_total",
                &[("port_role", port_role), ("reason", reason)],
            );
        }
    }

    for timestamp_kind in [
        "exchange_recv",
        "matching_engine",
        "gateway_send",
        "block_time",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_venue_to_recv_latency_seconds_count",
            &[("timestamp_kind", timestamp_kind)],
        );
    }

    for event_kind in ["book_update", "trade"] {
        assert_zero(
            &rendered,
            "dz_publisher_recv_to_send_latency_seconds_count",
            &[("event_kind", event_kind)],
        );
    }
}

#[test]
fn egress_datagrams_total_only_covers_the_supplied_port_roles() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for port_role in ["mktdata", "refdata"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_datagrams_total",
            &[("port_role", port_role)],
        );
    }

    let has_snapshot = rendered.lines().any(|line| {
        line.starts_with("dz_publisher_egress_datagrams_total{")
            && line.contains("port_role=\"snapshot\"")
    });
    assert!(
        !has_snapshot,
        "snapshot was not supplied to PublisherMetrics::new and must not appear:\n{rendered}"
    );
}

#[test]
fn the_only_open_label_family_is_absent_until_touched_then_present() {
    // Everything else the config makes knowable. What is left is the build
    // labels, which only the caller can supply.
    const NAME: &str = "dz_publisher_build_info";

    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary"],
        channel_ids: &[1],
        ingress_message_types: &["trade"],
    });

    let rendered = metrics.render();
    assert!(
        !rendered.contains(&format!("# TYPE {NAME} ")),
        "{NAME} is labelled by caller-supplied values and must be absent until touched, \
         but rendered:\n{rendered}"
    );

    metrics
        .process()
        .set_build_info("1.0.0", "abc123", "1.88.0");

    let rendered = metrics.render();
    assert!(
        rendered.contains(&format!("# TYPE {NAME} ")),
        "{NAME} should render once touched, but did not appear in:\n{rendered}"
    );
}

#[test]
fn declared_ingress_message_types_render_at_zero_and_the_rest_fall_to_other() {
    // The label is the upstream source's vocabulary, so the crate cannot
    // enumerate it - but leaving it open puts an unbounded label on the
    // highest-frequency path. Sources that name a message after the
    // subscription that carried it would give one series per instrument.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary"],
        channel_ids: &[],
        ingress_message_types: &["trade", "book_delta"],
    });

    let rendered = metrics.render();
    for message_type in ["trade", "book_delta", "other"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_messages_total",
            &[("message_type", message_type), ("connection", "primary")],
        );
    }

    metrics.ingress().message("trade", "primary");
    metrics.ingress().message("trades.BTC-PERP", "primary");
    metrics.ingress().message("trades.ETH-PERP", "primary");

    let rendered = metrics.render();
    let has_instrument_series = rendered
        .lines()
        .any(|line| line.contains("message_type=\"trades."));
    assert!(
        !has_instrument_series,
        "an undeclared message type must not create a series of its own:\n{rendered}"
    );

    let other = rendered
        .lines()
        .find(|line| {
            line.starts_with("dz_publisher_ingress_messages_total{")
                && line.contains("message_type=\"other\"")
        })
        .unwrap_or_else(|| panic!("the other bucket must render:\n{rendered}"));
    assert!(
        other.ends_with(" 2"),
        "both undeclared messages count: {other}"
    );
}

#[test]
fn declared_connections_render_at_zero_from_startup() {
    // The alert that means "my feed is down" is `== 0` on this series. A
    // publisher whose upstream never came up would never touch it, so
    // without pre-creation the series would not exist and the alert could
    // not fire in the one case it exists for.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary", "backup"],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for connection in ["primary", "backup"] {
        assert_zero(
            &rendered,
            "dz_publisher_ingress_connection_state",
            &[("connection", connection)],
        );
    }
}

#[test]
fn declared_channel_ids_render_at_zero_from_startup() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[0, 7],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for channel_id in ["0", "7"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_sequence_current",
            &[("port_role", "mktdata"), ("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
            &[("port_role", "mktdata"), ("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_refdata_manifest_seq",
            &[("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_refdata_manifest_valid",
            &[("channel_id", channel_id)],
        );
        assert_zero(
            &rendered,
            "dz_publisher_refdata_instruments_current",
            &[("channel_id", channel_id)],
        );
    }
}

/// Every sample line carrying a `channel_id` label, counted per family.
///
/// Read off the exposition rather than worked out from the config, because a
/// test that computes the number the same way the code does passes against
/// both of them being wrong. It also names families rather than looking for
/// the ones it expects, so a fifth family that starts carrying `channel_id`
/// arrives here as a key nothing asserted.
fn channel_keyed_series(rendered: &str) -> BTreeMap<&str, usize> {
    let mut counts = BTreeMap::new();
    for line in rendered.lines() {
        if line.starts_with('#') || !line.contains("channel_id=\"") {
            continue;
        }
        let Some((family, _)) = line.split_once('{') else {
            continue;
        };
        *counts.entry(family).or_default() += 1;
    }
    counts
}

fn precreated_channel_keyed(channel_ids: &[u8]) -> BTreeMap<&'static str, usize> {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot],
        connections: &[],
        channel_ids,
        ingress_message_types: &[],
    });
    // The keys borrow the rendered exposition, which does not outlive this
    // call, so each is re-borrowed as the static name it matches. The lookup
    // is also the check: a family carrying `channel_id` that nobody sized
    // panics here instead of quietly inflating a total.
    channel_keyed_series(&metrics.render())
        .into_iter()
        .map(|(family, count)| {
            let family = CHANNEL_KEYED_FAMILIES
                .iter()
                .find(|known| **known == family)
                .unwrap_or_else(|| panic!("{family} carries channel_id and nothing sized it"));
            (*family, count)
        })
        .collect()
}

/// The families a declared Channel ID pre-creates a series on. Adding one is a
/// sizing decision, so it is written down rather than discovered.
const CHANNEL_KEYED_FAMILIES: &[&str] = &[
    "dz_publisher_egress_sequence_current",
    "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
    "dz_publisher_refdata_manifest_seq",
    "dz_publisher_refdata_manifest_valid",
    "dz_publisher_refdata_instruments_current",
];

#[test]
fn six_channel_keyed_series_and_one_instrument_count_are_precreated_per_channel_id() {
    // The pre-created surface is sized rather than discovered: it is entirely
    // built at startup, so a publisher that declares many channel instances
    // pays all of it before its first datagram. The two sizes below are a
    // publisher operating all three port roles, at two declared Channel IDs
    // and at a worked 62 — 31 shards of two feed specifications. Both totals
    // are counted off the gathered exposition; neither is arithmetic this test
    // performs, because arithmetic here would agree with the same mistake in
    // the code.
    let at_two = precreated_channel_keyed(&[0, 1]);
    assert_eq!(
        at_two,
        BTreeMap::from([
            ("dz_publisher_egress_sequence_current", 6),
            (
                "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
                2
            ),
            ("dz_publisher_refdata_manifest_seq", 2),
            ("dz_publisher_refdata_manifest_valid", 2),
            ("dz_publisher_refdata_instruments_current", 2),
        ]),
        "12 channel-keyed series plus 2 for the instrument count"
    );

    let sixty_two: Vec<u8> = (0..62).collect();
    let at_sixty_two = precreated_channel_keyed(&sixty_two);
    assert_eq!(
        at_sixty_two,
        BTreeMap::from([
            ("dz_publisher_egress_sequence_current", 186),
            (
                "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
                62
            ),
            ("dz_publisher_refdata_manifest_seq", 62),
            ("dz_publisher_refdata_manifest_valid", 62),
            ("dz_publisher_refdata_instruments_current", 62),
        ]),
        "372 channel-keyed series plus 62 for the instrument count"
    );

    // Stated as the two totals as well: the tables above are what a reader
    // checks line by line, and the totals are what an operator sizes a scrape
    // on.
    const INSTRUMENT_COUNT: &str = "dz_publisher_refdata_instruments_current";
    let four_families = |counts: &BTreeMap<&str, usize>| {
        counts
            .iter()
            .filter(|(family, _)| **family != INSTRUMENT_COUNT)
            .map(|(_, count)| *count)
            .sum::<usize>()
    };
    assert_eq!(four_families(&at_two), 12);
    assert_eq!(at_two[INSTRUMENT_COUNT], 2);
    assert_eq!(four_families(&at_sixty_two), 372);
    assert_eq!(at_sixty_two[INSTRUMENT_COUNT], 62);
}

#[test]
fn the_instrument_count_is_per_channel_and_a_write_to_one_leaves_the_others_alone() {
    // The gauge mirrors the wire's `Instrument Count`, which the specification
    // counts per channel. A process-wide gauge would report the whole
    // process's published set against every channel, and an operator reading
    // it as the channel's would see a number no datagram carries.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[4, 9],
        ingress_message_types: &[],
    });
    metrics.refdata().set_instruments_current(4, 3);

    let rendered = metrics.render();
    let line = find_sample(
        &rendered,
        "dz_publisher_refdata_instruments_current",
        &[("channel_id", "4")],
    );
    assert!(line.ends_with(" 3"), "channel 4 holds three: {line}");
    assert_zero(
        &rendered,
        "dz_publisher_refdata_instruments_current",
        &[("channel_id", "9")],
    );
}

#[test]
fn egress_message_types_are_precreated_only_on_the_port_roles_that_permit_them() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for message_type in ["heartbeat", "end_of_session", "quote", "trade"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_messages_total",
            &[("port_role", "mktdata"), ("message_type", message_type)],
        );
    }
    for message_type in ["instrument_definition", "manifest_summary"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_messages_total",
            &[("port_role", "refdata"), ("message_type", message_type)],
        );
    }

    // A quote on the refdata port is not a series that can ever be written
    // to, so pre-creation must not assert one.
    let has_impossible_pair = rendered.lines().any(|line| {
        line.starts_with("dz_publisher_egress_messages_total{")
            && line.contains("port_role=\"refdata\"")
            && line.contains("message_type=\"quote\"")
    });
    assert!(
        !has_impossible_pair,
        "the specification does not permit a quote on the refdata port:\n{rendered}"
    );
}

#[test]
fn every_egress_message_type_precreates_an_encode_duration_series() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for message_type in [
        "heartbeat",
        "end_of_session",
        "quote",
        "trade",
        "instrument_definition",
        "manifest_summary",
    ] {
        assert_zero(
            &rendered,
            "dz_publisher_encode_duration_seconds_count",
            &[("message_type", message_type)],
        );
    }
}

#[test]
fn set_build_info_renders_the_gauge_at_one_with_its_labels() {
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    metrics
        .process()
        .set_build_info("1.2.3", "deadbeef", "1.88.0");

    let rendered = metrics.render();
    let line = find_sample(
        &rendered,
        "dz_publisher_build_info",
        &[
            ("version", "1.2.3"),
            ("commit", "deadbeef"),
            ("toolchain", "1.88.0"),
        ],
    );
    assert!(line.ends_with(" 1"), "expected build_info to be 1: {line}");
}

#[test]
fn the_heartbeat_gauge_is_precreated_only_where_a_heartbeat_is_permitted() {
    // The gauge carries port_role so that a Channel ID heartbeating on two
    // port roles does not fold onto one series. Pre-creation still has to
    // respect where a heartbeat can go: on a role that never receives one
    // the gauge stays 0 forever, and the staleness rule in its own HELP
    // text would fire on that series permanently. The uptime guard
    // suppresses the startup window, not a series that is never set.
    //
    // The other half of why this gauge carries `port_role` - that two
    // roles on one Channel ID must not fold onto one series and hide the
    // staler port's age - has no test yet, because `is_valid_on` permits a
    // heartbeat on `mktdata` alone and the two-role case cannot be built
    // without going around the guard above. It comes back with the guard,
    // when a depth feed permits heartbeats on the snapshot role.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot],
        connections: &[],
        channel_ids: &[1],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    assert_zero(
        &rendered,
        "dz_publisher_egress_heartbeat_last_sent_timestamp_seconds",
        &[("port_role", "mktdata"), ("channel_id", "1")],
    );

    for port_role in ["refdata", "snapshot"] {
        let present = rendered.lines().any(|line| {
            line.starts_with("dz_publisher_egress_heartbeat_last_sent_timestamp_seconds{")
                && line.contains(&format!("port_role=\"{port_role}\""))
        });
        assert!(
            !present,
            "no heartbeat is permitted on {port_role}, so pre-creating the series would leave \
             it at 0 forever:\n{rendered}"
        );
    }
}

#[test]
fn the_manifest_gauges_are_absent_without_a_refdata_port() {
    // The manifest belongs to the refdata port. A publisher that does not
    // operate one has no manifest to report, and `manifest_valid`
    // pre-created at 0 is a wrong value rather than a missing one: `== 0`
    // on a gauge whose HELP reads "1 valid, 0 not" is the obvious alert,
    // and it would fire forever on a publisher with nothing to be invalid
    // about.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[3],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for name in [
        "dz_publisher_refdata_manifest_seq",
        "dz_publisher_refdata_manifest_valid",
    ] {
        let present = rendered
            .lines()
            .any(|line| line.starts_with(&format!("{name}{{")));
        assert!(!present, "{name} must not be pre-created here:\n{rendered}");
    }
}

#[test]
fn the_proposed_egress_reasons_are_precreated_only_on_the_supplied_port_roles() {
    // The two proposed values are pre-created on exactly the roles the
    // publisher operates, like the five before them. A `malformed_message` on
    // the snapshot port of a publisher with no snapshot port is a series
    // nothing can ever write to, and a panel that stays empty for a reason
    // nobody can find.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &[],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for reason in ["not_carried_by_feed", "malformed_message"] {
        assert_zero(
            &rendered,
            "dz_publisher_egress_errors_total",
            &[("port_role", "mktdata"), ("reason", reason)],
        );

        let has_snapshot = rendered.lines().any(|line| {
            line.starts_with("dz_publisher_egress_errors_total{")
                && line.contains("port_role=\"snapshot\"")
                && line.contains(&format!("reason=\"{reason}\""))
        });
        assert!(
            !has_snapshot,
            "snapshot was not supplied to PublisherMetrics::new, so {reason} on it can never be \
             written:\n{rendered}"
        );
    }
}

#[test]
fn the_proposed_families_carry_no_port_role_or_connection_label() {
    // Sized deliberately. A connect failure is a property of the upstream and
    // a lowering refusal is a property of an instrument's exponents; neither
    // is a property of a port role, and a label that is not a dimension of
    // what is being counted multiplies the series for nothing. `connection`
    // is left off `connect_failures_total` for the same reason it is left off
    // `reconnects_total`, which is its sibling: the two must aggregate the
    // same way.
    let metrics = PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 1,
        port_roles: &[PortRole::Mktdata],
        connections: &["primary"],
        channel_ids: &[],
        ingress_message_types: &[],
    });
    let rendered = metrics.render();

    for name in [
        "dz_publisher_ingress_connect_failures_total",
        "dz_publisher_ingress_adapter_errors_total",
        "dz_publisher_lowering_refusals_total",
    ] {
        for line in rendered
            .lines()
            .filter(|line| line.starts_with(&format!("{name}{{")))
        {
            for label in ["port_role=", "connection=", "channel_id=", "instrument_id="] {
                assert!(
                    !line.contains(label),
                    "{name} must not carry {label}: {line}"
                );
            }
        }
    }
}
