//! Fixtures shared by the health tier's tests.
//!
//! The whole tier is pure logic over a `RecordedDatagram`, so every test here
//! runs with no privileges, no network and no socket: the datagrams are 24 bytes
//! this module writes by hand, and the assertions read the rendered exposition.
//!
//! Every address is documentation-range — RFC 5737 or MCAST-TEST-NET — so
//! nothing here can be mistaken for a real host.
//!
//! Each test binary compiles this module in full and uses only part of it, so
//! the unused half is not dead code — it is another binary's fixture.
#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddrV4};

use dz_edge_core::{PortRole, DATAGRAM_HEADER_SIZE, SCHEMA_VERSION, SIZE_HEARTBEAT};
use dz_recorder_core::{CaptureDropScope, RecordedDatagram, RecvTsKind};
use dz_recorder_health::{
    FeedSeries, HealthMetrics, HealthMetricsConfig, HealthObserver, InstanceLimits,
};
use std::sync::Arc;

pub const SITE: &str = "test-site";
pub const RECORDER: &str = "test-recorder-1";
pub const FEED: &str = "test-feed";

pub const MKTDATA_PORT: u16 = 30001;
pub const REFDATA_PORT: u16 = 30002;
pub const SNAPSHOT_PORT: u16 = 30003;
pub const GROUP: Ipv4Addr = Ipv4Addr::new(233, 252, 0, 1);

/// An arbitrary `Magic`; nothing in this tier judges it.
pub const MAGIC: u16 = 0x4442;
pub const CHANNEL: u8 = 0;

pub const PUBLISHER_A: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 10);
pub const PUBLISHER_B: Ipv4Addr = Ipv4Addr::new(192, 0, 2, 11);
pub const STRANGER: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);

pub const SECOND_NS: u64 = 1_000_000_000;
/// A plausible unix nanosecond receive stamp, so that a gauge holding one reads
/// like a timestamp rather than like a small integer.
pub const T0: u64 = 1_800_000_000 * SECOND_NS;

/// Every `dz_recorder_*` family this crate is required to expose.
///
/// This list is the contract. Rename a family in the implementation without
/// changing this list and `normative_names.rs` fails; add one without adding it
/// here and `precreated_at_startup.rs` still holds it to rendering zero.
pub const NORMATIVE_NAMES: &[&str] = &[
    "dz_recorder_datagrams_total",
    "dz_recorder_bytes_total",
    "dz_recorder_send_to_recv_latency_seconds",
    "dz_recorder_recv_timestamps_total",
    "dz_recorder_latency_samples_dropped_total",
    "dz_recorder_declared_length_violations_total",
    "dz_recorder_declared_length_mismatch_total",
    "dz_recorder_unreadable_datagrams_total",
    "dz_recorder_capture_drops_total",
    "dz_recorder_capture_drops_handle_total",
    "dz_recorder_rejoins_total",
    "dz_recorder_foreign_group_datagrams_total",
    "dz_recorder_unexpected_source_datagrams_total",
    "dz_recorder_rejoin_failures_total",
    "dz_recorder_capture_rejoins_total",
    "dz_recorder_heartbeat_interval_seconds",
    "dz_recorder_datagram_magic_total",
    "dz_recorder_datagram_schema_version_total",
    "dz_recorder_interface_drops_total",
    "dz_recorder_instances_tracked",
    "dz_recorder_instances_opened_total",
    "dz_recorder_instances_evicted_total",
    "dz_recorder_instances_refused_total",
    "dz_recorder_declared_instances_evicted_total",
    "dz_recorder_datagrams_unexpected_role_total",
    "dz_recorder_segments_evicted_total",
    "dz_recorder_sequence_gaps_total",
    "dz_recorder_missing_datagrams_on_arrival_total",
    "dz_recorder_duplicate_datagrams_total",
    "dz_recorder_reordered_datagrams_total",
    "dz_recorder_resets_total",
    "dz_recorder_era_transitions_total",
    "dz_recorder_backward_sequence_total",
    "dz_recorder_forward_jump_total",
    "dz_recorder_sequence_current",
    "dz_recorder_era_ordinal",
    "dz_recorder_last_datagram_timestamp_seconds",
    "dz_recorder_heartbeat_last_timestamp_seconds",
];

/// A metric set for one feed carrying both non-snapshot roles and two channels.
pub fn metrics_with_sources(sources: &[Ipv4Addr]) -> HealthMetrics {
    HealthMetrics::new(&HealthMetricsConfig {
        site: SITE,
        recorder: RECORDER,
        feeds: &[FeedSeries {
            feed: FEED,
            port_roles: &[PortRole::Mktdata, PortRole::Refdata],
            channel_ids: &[0, 1],
            expected_sources: sources,
            expected_magic: Some(MAGIC),
        }],
    })
}

/// The same feed with no channel declaration at all, which is what most feeds
/// state today and a different fact from declaring none.
pub fn metrics_without_declared_channels(sources: &[Ipv4Addr]) -> HealthMetrics {
    HealthMetrics::new(&HealthMetricsConfig {
        site: SITE,
        recorder: RECORDER,
        feeds: &[FeedSeries {
            feed: FEED,
            port_roles: &[PortRole::Mktdata, PortRole::Refdata],
            channel_ids: &[],
            expected_sources: sources,
            expected_magic: Some(MAGIC),
        }],
    })
}

/// An observer over a feed that declared no channel ids.
#[must_use]
pub fn observer_with_undeclared_channels(
    sources: &[Ipv4Addr],
    limits: InstanceLimits,
) -> (Arc<HealthMetrics>, HealthObserver) {
    let metrics = Arc::new(metrics_without_declared_channels(sources));
    let observer = HealthObserver::new(
        Arc::clone(&metrics),
        FEED,
        limits,
        CaptureDropScope::PortRole,
    )
    .expect("the feed was declared to the metric set");
    (metrics, observer)
}

/// A metric set and one observer over it, with the declared sources a test
/// wants and the default bounds.
#[must_use]
pub fn observer_with_sources(sources: &[Ipv4Addr]) -> (Arc<HealthMetrics>, HealthObserver) {
    observer_with_limits(sources, InstanceLimits::default())
}

/// The same, with the bounds a test wants to reach.
#[must_use]
pub fn observer_with_limits(
    sources: &[Ipv4Addr],
    limits: InstanceLimits,
) -> (Arc<HealthMetrics>, HealthObserver) {
    observer_with_scope(sources, limits, CaptureDropScope::PortRole)
}

/// The same, at a chosen capture drop scope: where `drop_delta` is charged is a
/// property of the capture, and the two scopes put it in different series.
#[must_use]
pub fn observer_with_scope(
    sources: &[Ipv4Addr],
    limits: InstanceLimits,
    drop_scope: CaptureDropScope,
) -> (Arc<HealthMetrics>, HealthObserver) {
    let metrics = Arc::new(metrics_with_sources(sources));
    let observer = HealthObserver::new(Arc::clone(&metrics), FEED, limits, drop_scope)
        .expect("the feed was declared to the metric set");
    (metrics, observer)
}

/// One datagram's header fields, with the parts a test does not care about
/// already filled in.
#[derive(Debug, Clone, Copy)]
pub struct Datagram {
    pub magic: u16,
    pub schema_version: u8,
    pub channel_id: u8,
    pub sequence_number: u64,
    pub send_timestamp_ns: u64,
    pub msg_count: u8,
    pub reset_count: u8,
    /// What the header declares. `None` declares the encoded length, which is
    /// the conformant case.
    pub declared_len: Option<u16>,
    /// How many bytes the payload actually is. Defaults to two messages, so
    /// that the default datagram is deliberately *not* heartbeat-shaped and a
    /// cadence assertion cannot pass on ordinary traffic.
    pub payload_len: usize,
}

impl Default for Datagram {
    fn default() -> Self {
        Self {
            magic: MAGIC,
            schema_version: SCHEMA_VERSION,
            channel_id: CHANNEL,
            sequence_number: 1,
            send_timestamp_ns: T0,
            msg_count: 2,
            reset_count: 0,
            declared_len: None,
            payload_len: DATAGRAM_HEADER_SIZE + 2 * SIZE_HEARTBEAT,
        }
    }
}

impl Datagram {
    #[must_use]
    pub fn seq(sequence_number: u64) -> Self {
        Self {
            sequence_number,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn with_reset(mut self, reset_count: u8) -> Self {
        self.reset_count = reset_count;
        self
    }

    /// One message of the heartbeat's size, which is as far as the 24-byte
    /// header alone can identify a heartbeat.
    #[must_use]
    pub fn heartbeat(mut self) -> Self {
        self.msg_count = 1;
        self.payload_len = DATAGRAM_HEADER_SIZE + SIZE_HEARTBEAT;
        self
    }

    #[must_use]
    pub fn payload(&self) -> Vec<u8> {
        let mut buf = vec![0; self.payload_len.max(DATAGRAM_HEADER_SIZE)];
        let declared = self
            .declared_len
            .unwrap_or_else(|| u16::try_from(self.payload_len).expect("a test payload fits a u16"));
        buf[0..2].copy_from_slice(&self.magic.to_le_bytes());
        buf[2] = self.schema_version;
        buf[3] = self.channel_id;
        buf[4..12].copy_from_slice(&self.sequence_number.to_le_bytes());
        buf[12..20].copy_from_slice(&self.send_timestamp_ns.to_le_bytes());
        buf[20] = self.msg_count;
        buf[21] = self.reset_count;
        buf[22..24].copy_from_slice(&declared.to_le_bytes());
        buf.truncate(self.payload_len);
        buf
    }
}

/// Everything about a datagram's arrival that is not in its bytes.
#[derive(Debug, Clone, Copy)]
pub struct Arrival {
    pub source: Ipv4Addr,
    pub port: u16,
    pub role: PortRole,
    pub recv_ts_ns: u64,
    pub recv_ts_kind: RecvTsKind,
    pub drop_delta: u32,
    /// `None` means "the same as the payload's length", the untruncated case.
    pub wire_payload_len: Option<u32>,
}

impl Default for Arrival {
    fn default() -> Self {
        Self {
            source: PUBLISHER_A,
            port: MKTDATA_PORT,
            role: PortRole::Mktdata,
            recv_ts_ns: T0 + SECOND_NS / 1000,
            recv_ts_kind: RecvTsKind::KernelSoftware,
            drop_delta: 0,
            wire_payload_len: None,
        }
    }
}

impl Arrival {
    #[must_use]
    pub fn from(source: Ipv4Addr) -> Self {
        Self {
            source,
            ..Self::default()
        }
    }

    #[must_use]
    pub fn at(mut self, recv_ts_ns: u64) -> Self {
        self.recv_ts_ns = recv_ts_ns;
        self
    }

    #[must_use]
    pub fn recorded<'a>(&self, payload: &'a [u8]) -> RecordedDatagram<'a> {
        RecordedDatagram {
            payload,
            src: SocketAddrV4::new(self.source, 40000),
            dst: SocketAddrV4::new(GROUP, self.port),
            role: self.role,
            recv_ts_ns: self.recv_ts_ns,
            recv_ts_kind: self.recv_ts_kind,
            drop_delta: self.drop_delta,
            ttl: Some(8),
            link_headers: None,
            wire_payload_len: self.wire_payload_len.unwrap_or_else(|| {
                u32::try_from(payload.len()).expect("a test payload fits a u32")
            }),
        }
    }
}

/// The value of the sample line for `metric` carrying every label in `labels`.
///
/// Panics when no such line exists, rather than returning zero: a test that
/// cannot tell "the series is absent" from "the series is zero" is the test this
/// crate's pre-creation work exists to make impossible.
#[must_use]
pub fn sample(rendered: &str, metric: &str, labels: &[(&str, &str)]) -> f64 {
    find_sample(rendered, metric, labels)
        .rsplit(' ')
        .next()
        .expect("a sample line ends in a value")
        .parse()
        .expect("a sample value parses as a float")
}

fn find_sample<'a>(rendered: &'a str, metric: &str, labels: &[(&str, &str)]) -> &'a str {
    rendered
        .lines()
        .find(|line| {
            line.strip_prefix(metric)
                .is_some_and(|rest| rest.starts_with('{'))
                && labels
                    .iter()
                    .all(|(key, value)| line.contains(&format!("{key}=\"{value}\"")))
        })
        .unwrap_or_else(|| {
            panic!("no sample line for {metric} with labels {labels:?} in:\n{rendered}")
        })
}

/// Whether any sample line for `metric` carries every label in `labels`.
#[must_use]
pub fn has_sample(rendered: &str, metric: &str, labels: &[(&str, &str)]) -> bool {
    rendered.lines().any(|line| {
        line.strip_prefix(metric)
            .is_some_and(|rest| rest.starts_with('{'))
            && labels
                .iter()
                .all(|(key, value)| line.contains(&format!("{key}=\"{value}\"")))
    })
}

/// The labels of one channel instance's series, in the order the assertions
/// below want them.
#[must_use]
pub fn instance_labels(source: Ipv4Addr) -> Vec<(&'static str, String)> {
    vec![
        ("feed", FEED.to_owned()),
        ("port_role", PortRole::Mktdata.as_str().to_owned()),
        ("channel", CHANNEL.to_string()),
        ("source", source.to_string()),
    ]
}

/// The value of a channel-instance series for `source`.
#[must_use]
pub fn instance_sample(rendered: &str, metric: &str, source: Ipv4Addr) -> f64 {
    let owned = instance_labels(source);
    let labels: Vec<(&str, &str)> = owned
        .iter()
        .map(|(key, value)| (*key, value.as_str()))
        .collect();
    sample(rendered, metric, &labels)
}
