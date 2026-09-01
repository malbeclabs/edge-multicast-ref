//! The normative `dz_recorder_*` metric set.
//!
//! This mirrors the decision the `dz_publisher_*` set made: the names are
//! normative, the crate that owns the hot path records them internally, and a
//! recorder emits them whether or not anyone thought about it. Nothing outside
//! this module constructs a metric, so there is no path to a series that skips
//! the `site` and `recorder` labels or invents a name.
//!
//! # Pre-creation, and the one family class that cannot be pre-created
//!
//! Every family whose label values are knowable at startup is created here, so
//! it renders at 0 before the first datagram: a metric that first appears after
//! the event it counts is a metric no dashboard can chart, and a panel that is
//! blank because nothing has happened yet is indistinguishable from one that is
//! blank because the recorder is dead.
//!
//! The channel-instance families carry `source`, and a source address is not
//! knowable at startup in general — an any-source join accepts datagrams from
//! any sender. What *is* knowable is the set an operator declared, which is
//! what `FeedSeries::expected_sources` is for, and those children are
//! pre-created here. A source outside that set opens its series in silence when
//! its first datagram arrives, which is the rule the design requires and the
//! reason a tunnel address being reassigned under a live host does not page.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::LazyLock;

use dz_edge_core::{PortRole, SUPPORTED_SCHEMA_VERSIONS};
use prometheus::{
    Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder,
};

/// Buckets for `dz_recorder_send_to_recv_latency_seconds`.
///
/// The span is wider than a publisher's internal latencies because this one
/// crosses a network and two clocks: it has to resolve a same-site path in tens
/// of microseconds and still show a wide-area path, or a clock that has drifted
/// by seconds, rather than piling both into `+Inf`.
pub const SEND_TO_RECV_BUCKETS: &[f64] = &[
    0.000_010, 0.000_025, 0.000_050, 0.000_100, 0.000_250, 0.000_500, 0.001, 0.0025, 0.005, 0.010,
    0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0, 10.0,
];

/// Buckets for `dz_recorder_heartbeat_interval_seconds`.
///
/// Heartbeat cadence is a seconds-scale question — an idle channel's keepalive,
/// not a per-datagram path — so the microsecond end of [`SEND_TO_RECV_BUCKETS`]
/// would spend every bucket on a range this series never visits.
pub const HEARTBEAT_INTERVAL_BUCKETS: &[f64] = &[
    0.100, 0.250, 0.500, 1.0, 2.0, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 300.0,
];

/// How `recv_ts_ns` was obtained, as a label value.
///
/// This is a label rather than a judgement because the two kinds are not
/// comparable and the ratio between them is the thing an operator needs: it is
/// the denominator the latency histogram is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvTimestampKind {
    KernelSoftware,
    ApplicationFallback,
}

impl RecvTimestampKind {
    pub const ALL: &'static [Self] = &[Self::KernelSoftware, Self::ApplicationFallback];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KernelSoftware => "kernel_software",
            Self::ApplicationFallback => "application_fallback",
        }
    }
}

/// Why a datagram produced no latency observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyDropReason {
    /// The receive stamp was ours, not the kernel's. Such a stamp measures the
    /// recorder's own scheduler, and averaging it together with a kernel stamp
    /// measures neither.
    ApplicationFallback,
    /// `recv_ts_ns` precedes `send_timestamp_ns`, so the two clocks disagree
    /// about the order of events. There is no non-negative duration to observe
    /// and a histogram cannot represent the disagreement.
    NegativeInterval,
    /// The interval is longer than the histogram's widest bucket, so it is a
    /// clock that disagrees rather than a path that is slow. Counted rather
    /// than observed: `+Inf` would hold it either way, but `_sum` would carry
    /// it for the life of the process and the average the help text points at
    /// would never recover.
    ImplausibleInterval,
}

impl LatencyDropReason {
    pub const ALL: &'static [Self] = &[
        Self::ApplicationFallback,
        Self::NegativeInterval,
        Self::ImplausibleInterval,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApplicationFallback => "application_fallback",
            Self::NegativeInterval => "negative_interval",
            Self::ImplausibleInterval => "implausible_interval",
        }
    }
}

/// A declared datagram length outside the mandated range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredLengthViolation {
    /// Above the 1232-byte cap every feed specification mandates.
    OverCap,
    /// Below the 24-byte header, so the field cannot describe a datagram at
    /// all.
    UnderHeader,
}

impl DeclaredLengthViolation {
    pub const ALL: &'static [Self] = &[Self::OverCap, Self::UnderHeader];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OverCap => "over_cap",
            Self::UnderHeader => "under_header",
        }
    }
}

/// A declared datagram length that disagrees with the length that arrived.
///
/// A separate family from [`DeclaredLengthViolation`] because they are separate
/// violations: a declared length can be perfectly in range and still describe a
/// different datagram than the one delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredLengthMismatch {
    /// The header claims more bytes than arrived — a reader that trusts it
    /// walks off the end.
    DeclaredExceedsReceived,
    /// Fewer bytes are claimed than arrived, so something trails the datagram.
    DeclaredBelowReceived,
}

impl DeclaredLengthMismatch {
    pub const ALL: &'static [Self] = &[Self::DeclaredExceedsReceived, Self::DeclaredBelowReceived];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DeclaredExceedsReceived => "declared_exceeds_received",
            Self::DeclaredBelowReceived => "declared_below_received",
        }
    }
}

/// Why the health tier could conclude nothing about a datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnreadableReason {
    /// Fewer than 24 bytes arrived, so there is no header to read. Note that
    /// the datagram is still archived: this tier concluding nothing is not the
    /// record path dropping anything.
    ShortHeader,
}

impl UnreadableReason {
    pub const ALL: &'static [Self] = &[Self::ShortHeader];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ShortHeader => "short_header",
        }
    }
}

/// The label value a header value beyond the distinct-value budget is counted
/// under. See [`crate::observer::InstanceLimits::max_distinct_header_values`].
pub const OTHER_VALUE: &str = "other";

/// Decimal label values for every `u8`, built once.
///
/// `channel` and `schema_version` both label per-datagram paths, and formatting
/// one per call would put a heap allocation on the drain thread.
static U8_LABELS: LazyLock<[String; 256]> =
    LazyLock::new(|| std::array::from_fn(|value| value.to_string()));

/// The decimal label value for a `u8` — a Channel ID or a Schema Version —
/// without allocating.
#[must_use]
pub fn u8_label(value: u8) -> &'static str {
    &U8_LABELS[value as usize]
}

/// The label value for a `Magic` counted by value.
///
/// Hex because that is how every feed specification writes it: a decimal 21_573
/// is not a value anyone can match against a specification by eye. This
/// allocates, so it is called when a value is first admitted and never per
/// datagram.
#[must_use]
pub fn magic_label(magic: u16) -> String {
    format!("0x{magic:04x}")
}

/// One feed's declared series: what makes this feed's label values knowable
/// before its first datagram.
///
/// Everything here narrows pre-creation rather than widening it. A port role
/// this feed does not carry, or a Channel ID it does not shard on, would assert
/// a channel instance that cannot exist.
#[derive(Debug, Clone)]
pub struct FeedSeries<'a> {
    /// The feed specification's name — `FeedConfig::spec`. This is the `feed`
    /// label on every series below.
    pub feed: &'a str,
    /// Exactly the port roles this feed is recorded on.
    pub port_roles: &'a [PortRole],
    /// Every `Channel ID` this feed shards on.
    ///
    /// These are the channel-instance series pre-created for each declared
    /// source, and — with `expected_sources` — what makes an instance a
    /// declared one: series that survive eviction and an admission that
    /// displaces a stranger. Empty means no declaration was made, and then
    /// every channel from a declared source counts as declared, because
    /// "unstated" must not silently mean "none".
    pub channel_ids: &'a [u8],
    /// The source addresses an operator declared — `FeedConfig::expected_sources`.
    ///
    /// These are the channel-instance series that exist from startup, and the
    /// ones whose series survive eviction: an operator's own publisher must not
    /// disappear from a dashboard because a flood of unknown senders pushed it
    /// out of a bounded map. Empty means no expectation was stated, and then no
    /// channel-instance series exists until one is seen.
    pub expected_sources: &'a [Ipv4Addr],
    /// The `Magic` this feed's datagrams carry, if the caller knows it.
    ///
    /// Only pre-creation uses it. Nothing here judges `Magic`, because a health
    /// tier is required to count it by value: a datagram misrouted from another
    /// feed is exactly the traffic worth seeing, and a tier that discards it
    /// reports nothing about it.
    pub expected_magic: Option<u16>,
}

/// One feed's declaration, owned, so an observer can be built from the feed's
/// name alone rather than by restating what this crate already holds.
#[derive(Debug, Clone)]
pub(crate) struct FeedDefinition {
    pub(crate) feed: String,
    pub(crate) port_roles: Vec<PortRole>,
    /// Every `Channel ID` an operator declared this feed shards on. Empty means
    /// no declaration was made, which is not the same as declaring none.
    pub(crate) channel_ids: Vec<u8>,
    pub(crate) expected_sources: Vec<Ipv4Addr>,
    pub(crate) expected_magic: Option<u16>,
}

/// What one recorder process exposes, and the sets that make its label values
/// knowable at startup.
#[derive(Debug, Clone)]
pub struct HealthMetricsConfig<'a> {
    /// `RecorderIdentity::site`. A constant label on every series.
    pub site: &'a str,
    /// `RecorderIdentity::recorder`, unique within the site. A constant label on
    /// every series.
    pub recorder: &'a str,
    /// Every feed this recorder is configured to record.
    pub feeds: &'a [FeedSeries<'a>],
}

/// The complete normative metric set for one recorder process.
///
/// Shared across every feed's observer — one registry, one `/metrics`, and
/// `feed` as a label rather than a second exposition to scrape.
pub struct HealthMetrics {
    registry: Registry,
    pub(crate) feeds: Vec<FeedDefinition>,

    datagrams_total: IntCounterVec,
    bytes_total: IntCounterVec,
    send_to_recv_latency_seconds: HistogramVec,
    recv_timestamps_total: IntCounterVec,
    latency_samples_dropped_total: IntCounterVec,
    declared_length_violations_total: IntCounterVec,
    declared_length_mismatch_total: IntCounterVec,
    unreadable_datagrams_total: IntCounterVec,
    capture_drops_total: IntCounterVec,
    rejoins_total: IntCounterVec,

    heartbeat_interval_seconds: HistogramVec,

    datagram_magic_total: IntCounterVec,
    datagram_schema_version_total: IntCounterVec,

    interface_drops_total: IntCounterVec,
    instances_tracked: IntGaugeVec,
    instances_opened_total: IntCounterVec,
    instances_evicted_total: IntCounterVec,
    instances_refused_total: IntCounterVec,
    declared_instances_evicted_total: IntCounterVec,
    capture_drops_handle_total: IntCounterVec,
    datagrams_unexpected_role_total: IntCounterVec,

    segments_evicted_total: IntCounter,

    sequence_gaps_total: IntCounterVec,
    missing_datagrams_total: IntCounterVec,
    duplicate_datagrams_total: IntCounterVec,
    reordered_datagrams_total: IntCounterVec,
    resets_total: IntCounterVec,
    era_transitions_total: IntCounterVec,
    backward_sequence_total: IntCounterVec,
    forward_jump_total: IntCounterVec,
    sequence_current: IntGaugeVec,
    era_ordinal: IntGaugeVec,
    last_datagram_timestamp_seconds: GaugeVec,
    heartbeat_last_timestamp_seconds: GaugeVec,
}

/// The children for one feed, resolved once so nothing on the drain thread
/// looks a label up.
pub(crate) struct FeedChildren {
    pub(crate) interface_drops: IntCounter,
    pub(crate) instances_tracked: IntGauge,
    pub(crate) instances_opened: IntCounter,
    pub(crate) instances_evicted: IntCounter,
    pub(crate) instances_refused: IntCounter,
    pub(crate) declared_evicted: IntCounter,
    pub(crate) capture_drops_handle: IntCounter,
    pub(crate) unexpected_role: IntCounter,
}

/// The children for one `(feed, role)`, resolved once for the same reason.
pub(crate) struct RoleChildren {
    pub(crate) datagrams: IntCounter,
    pub(crate) bytes: IntCounter,
    pub(crate) latency: Histogram,
    pub(crate) recv_ts: [IntCounter; 2],
    pub(crate) latency_dropped: [IntCounter; 3],
    pub(crate) declared_violation: [IntCounter; 2],
    pub(crate) declared_mismatch: [IntCounter; 2],
    pub(crate) unreadable: [IntCounter; 1],
    pub(crate) capture_drops: IntCounter,
    pub(crate) rejoins: IntCounter,
}

/// The children for one channel instance, resolved when the instance opens.
///
/// Held by the instance entry, so the per-datagram path is an `inc()` on an
/// already-resolved counter: no label formatting, no map lookup, no allocation.
pub(crate) struct InstanceChildren {
    pub(crate) gaps: IntCounter,
    pub(crate) missing: IntCounter,
    pub(crate) duplicates: IntCounter,
    pub(crate) reordered: IntCounter,
    pub(crate) resets: IntCounter,
    pub(crate) era_transitions: IntCounter,
    pub(crate) backward: IntCounter,
    pub(crate) forward_jump: IntCounter,
    pub(crate) sequence_current: IntGauge,
    pub(crate) era_ordinal: IntGauge,
    pub(crate) last_datagram_timestamp: Gauge,
    pub(crate) heartbeat_last_timestamp: Gauge,
    /// Labelled by channel and not by source, so a cadence percentile does not
    /// cost one histogram per sender; held here because the interval it
    /// observes can only be measured per instance.
    pub(crate) heartbeat_interval: Histogram,
}

fn const_labels(site: &str, recorder: &str) -> HashMap<String, String> {
    let mut labels = HashMap::with_capacity(2);
    labels.insert("site".to_owned(), site.to_owned());
    labels.insert("recorder".to_owned(), recorder.to_owned());
    labels
}

fn opts(name: &str, help: &str, labels: &HashMap<String, String>) -> Opts {
    Opts::new(name, help).const_labels(labels.clone())
}

fn histogram_opts(
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    buckets: &[f64],
) -> HistogramOpts {
    HistogramOpts::new(name, help)
        .const_labels(labels.clone())
        .buckets(buckets.to_vec())
}

fn counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    variable: &[&str],
) -> IntCounterVec {
    let metric =
        IntCounterVec::new(opts(name, help, labels), variable).expect("static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

fn int_gauge_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    variable: &[&str],
) -> IntGaugeVec {
    let metric =
        IntGaugeVec::new(opts(name, help, labels), variable).expect("static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

fn gauge_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    variable: &[&str],
) -> GaugeVec {
    let metric =
        GaugeVec::new(opts(name, help, labels), variable).expect("static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &HashMap<String, String>,
    buckets: &[f64],
    variable: &[&str],
) -> HistogramVec {
    let metric = HistogramVec::new(histogram_opts(name, help, labels, buckets), variable)
        .expect("static metric definition");
    registry
        .register(Box::new(metric.clone()))
        .expect("static metric registration");
    metric
}

impl HealthMetrics {
    /// Builds and pre-creates the whole set.
    ///
    /// `site` and `recorder` become constant labels on every series, applied
    /// once here rather than threaded through call sites, so there is no path to
    /// a `dz_recorder_*` series that cannot say which capture point produced it.
    #[must_use]
    pub fn new(config: &HealthMetricsConfig<'_>) -> Self {
        let registry = Registry::new();
        let labels = const_labels(config.site, config.recorder);

        let metrics = Self {
            datagrams_total: counter_vec(
                &registry,
                "dz_recorder_datagrams_total",
                "Datagrams received, by feed and port role.",
                &labels,
                &["feed", "port_role"],
            ),
            bytes_total: counter_vec(
                &registry,
                "dz_recorder_bytes_total",
                "Payload bytes received on the wire, by feed and port role. This is the length \
                 the datagram had on the wire, not the length that survived the capture length, \
                 so a truncating capture does not understate the feed's rate.",
                &labels,
                &["feed", "port_role"],
            ),
            send_to_recv_latency_seconds: histogram_vec(
                &registry,
                "dz_recorder_send_to_recv_latency_seconds",
                "Header send timestamp to receive timestamp, by feed and port role. Only \
                 kernel receive stamps are observed here; an application-level fallback stamp \
                 measures this recorder's own scheduler and is counted on \
                 dz_recorder_latency_samples_dropped_total instead. Compare the count here \
                 against dz_recorder_recv_timestamps_total before trusting a percentile.",
                &labels,
                SEND_TO_RECV_BUCKETS,
                &["feed", "port_role"],
            ),
            recv_timestamps_total: counter_vec(
                &registry,
                "dz_recorder_recv_timestamps_total",
                "Receive timestamps by their kind, per feed and port role. A rising \
                 application_fallback share means the latency histogram is measuring a \
                 shrinking fraction of the traffic.",
                &labels,
                &["feed", "port_role", "kind"],
            ),
            latency_samples_dropped_total: counter_vec(
                &registry,
                "dz_recorder_latency_samples_dropped_total",
                "Datagrams that produced no latency observation, by reason. These are not lost \
                 datagrams and not a feed fault; they are the histogram's missing denominator.",
                &labels,
                &["feed", "port_role", "reason"],
            ),
            declared_length_violations_total: counter_vec(
                &registry,
                "dz_recorder_declared_length_violations_total",
                "Datagrams whose declared length is outside the mandated 24..=1232 range, by \
                 feed, port role and which end of the range. Distinct from \
                 dz_recorder_declared_length_mismatch_total: this is the field disagreeing with \
                 the specification, that is the field disagreeing with the wire.",
                &labels,
                &["feed", "port_role", "kind"],
            ),
            declared_length_mismatch_total: counter_vec(
                &registry,
                "dz_recorder_declared_length_mismatch_total",
                "Datagrams whose declared length disagrees with the length that arrived, by \
                 feed, port role and direction.",
                &labels,
                &["feed", "port_role", "kind"],
            ),
            unreadable_datagrams_total: counter_vec(
                &registry,
                "dz_recorder_unreadable_datagrams_total",
                "Datagrams this tier could conclude nothing about, by reason. The datagram is \
                 still archived: the health tier declining to read a header is never the record \
                 path dropping bytes.",
                &labels,
                &["feed", "port_role", "reason"],
            ),
            capture_drops_total: counter_vec(
                &registry,
                "dz_recorder_capture_drops_total",
                "Datagrams this recorder's own capture handle lost, by feed and port role, \
                 summed from the per-datagram drop delta. Rising means the archive is becoming \
                 less trustworthy, which is a fact you want before you rely on it. A sequence \
                 gap covered by this is not a publisher finding.",
                &labels,
                &["feed", "port_role"],
            ),
            rejoins_total: counter_vec(
                &registry,
                "dz_recorder_rejoins_total",
                "Group memberships this recorder replaced, by feed and port role. A membership \
                 goes away with the interface it was joined on and nothing reports it: the \
                 socket stays open, readable and permanently silent.",
                &labels,
                &["feed", "port_role"],
            ),
            heartbeat_interval_seconds: histogram_vec(
                &registry,
                "dz_recorder_heartbeat_interval_seconds",
                "Interval between successive heartbeat-shaped datagrams, measured per channel \
                 instance and aggregated by feed, port role and channel. The interval can only \
                 be measured per instance — two publishers on one channel each keep their own \
                 cadence — while the source address is left off this series so that a cadence \
                 percentile does not cost one histogram per sender.",
                &labels,
                HEARTBEAT_INTERVAL_BUCKETS,
                &["feed", "port_role", "channel"],
            ),
            datagram_magic_total: counter_vec(
                &registry,
                "dz_recorder_datagram_magic_total",
                "Datagrams by the Magic they carry, counted by value and never judged: a \
                 datagram misrouted from another feed is exactly what this answers. Values \
                 beyond this recorder's distinct-value budget are counted under `other`, \
                 because Magic is 16 bits of sender-controlled label on an any-source join.",
                &labels,
                &["feed", "magic"],
            ),
            datagram_schema_version_total: counter_vec(
                &registry,
                "dz_recorder_datagram_schema_version_total",
                "Datagrams by the Schema Version they carry, counted by value and never \
                 judged. A version this build does not implement is still counted, and still \
                 archived; values beyond the distinct-value budget are counted under `other`.",
                &labels,
                &["feed", "schema_version"],
            ),
            interface_drops_total: counter_vec(
                &registry,
                "dz_recorder_interface_drops_total",
                "Drops the arrival interface reported, per feed. This is the loss upstream of \
                 the capture point that a capture handle's own counter cannot see; `gap, no \
                 capture drops, interface drops rising` is its own category and not publisher \
                 loss.",
                &labels,
                &["feed"],
            ),
            instances_tracked: int_gauge_vec(
                &registry,
                "dz_recorder_instances_tracked",
                "Channel instances currently in the bounded map, per feed. At the map's \
                 capacity, look at dz_recorder_instances_evicted_total.",
                &labels,
                &["feed"],
            ),
            instances_opened_total: counter_vec(
                &registry,
                "dz_recorder_instances_opened_total",
                "Channel instances opened, per feed. A source address not seen before opens a \
                 series silently — no gap, no loss, no alert — because a tunnel address is a \
                 lease and a reassignment must not page. This counter is where that silence is \
                 still visible.",
                &labels,
                &["feed"],
            ),
            instances_evicted_total: counter_vec(
                &registry,
                "dz_recorder_instances_evicted_total",
                "Channel instances evicted from the bounded map, least-recently-seen first, \
                 per feed. An evicted instance loses its sequence state, so its next datagram \
                 opens a new series rather than reporting a gap.",
                &labels,
                &["feed"],
            ),
            instances_refused_total: counter_vec(
                &registry,
                "dz_recorder_instances_refused_total",
                "Datagrams from a new channel instance that was not admitted, per feed, \
                 because every instance in a full map had been seen too recently to evict. \
                 This bounds the rate at which an unknown sender can make this recorder create \
                 series; the datagram is still archived.",
                &labels,
                &["feed"],
            ),
            declared_instances_evicted_total: counter_vec(
                &registry,
                "dz_recorder_declared_instances_evicted_total",
                "Evictions whose victim was an instance of a declared publisher, per feed. \
                 Strangers are evicted first and a declared source is admitted over one of any \
                 age, so this only moves when every instance in a full map is a declared one — \
                 which is a configuration that has outgrown max_instances rather than a flood, \
                 and the only case where this tier's own bound costs the loss accounting of a \
                 publisher an operator named.",
                &labels,
                &["feed"],
            ),
            capture_drops_handle_total: counter_vec(
                &registry,
                "dz_recorder_capture_drops_handle_total",
                "Datagrams this recorder itself lost, per feed, where the capture counts them \
                 for the whole handle rather than per port role — a ring drops frames before \
                 anything has demultiplexed them. It is the same quantity as \
                 dz_recorder_capture_drops_total and never both: which one a feed populates \
                 follows the capture mode, and it matches the drop scope the archive declares \
                 in its segments. Subtracting this from one role's gaps is not a valid \
                 operation; subtracting it from the feed's is.",
                &labels,
                &["feed"],
            ),
            datagrams_unexpected_role_total: counter_vec(
                &registry,
                "dz_recorder_datagrams_unexpected_role_total",
                "Datagrams that arrived on a port role this feed was not declared to carry, \
                 per feed. Nothing else is concluded from them, because every other series is \
                 keyed on a role this recorder was told about.",
                &labels,
                &["feed"],
            ),
            segments_evicted_total: {
                let metric = IntCounter::with_opts(opts(
                    "dz_recorder_segments_evicted_total",
                    "Completed archive segments deleted to stay inside the staging budget. \
                     Every one is an hour of evidence that no longer exists, and the capture \
                     path is never blocked in exchange — a writer that blocked on a full disk \
                     would convert a storage outage into feed loss.",
                    &labels,
                ))
                .expect("static metric definition");
                registry
                    .register(Box::new(metric.clone()))
                    .expect("static metric registration");
                metric
            },
            sequence_gaps_total: counter_vec(
                &registry,
                "dz_recorder_sequence_gaps_total",
                "Forward sequence discontinuities, per channel instance. Subtract \
                 dz_recorder_capture_drops_total before reading one as publisher or network \
                 loss.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            missing_datagrams_total: counter_vec(
                &registry,
                "dz_recorder_missing_datagrams_on_arrival_total",
                "Sequence numbers that looked absent when they should have arrived, per \
                 channel instance — the size of the gaps rather than their count, because one \
                 gap of a thousand and a thousand gaps of one are different faults. \
                 \
                 An upper bound, and structurally so: a counter cannot decrement, so a datagram \
                 counted absent and then delivered late stays counted here. The set-truth \
                 figure is this less dz_recorder_reordered_datagrams_total, which is what an \
                 offline pass over the archive reports. A panel showing this beside that one \
                 must subtract, or it shows two missing counts for one feed whenever the \
                 network reorders.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            duplicate_datagrams_total: counter_vec(
                &registry,
                "dz_recorder_duplicate_datagrams_total",
                "Datagrams whose sequence number was already seen within the reordering \
                 window, per channel instance.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            reordered_datagrams_total: counter_vec(
                &registry,
                "dz_recorder_reordered_datagrams_total",
                "Datagrams that arrived after a higher sequence number but inside the \
                 reordering window, per channel instance. These fill a gap already counted, so \
                 a gap count is an upper bound on loss until reordering is subtracted.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            resets_total: counter_vec(
                &registry,
                "dz_recorder_resets_total",
                "Reset Count transitions, per channel instance. Counted as the transition \
                 happens, in receive order: Reset Count is a u8 and it wraps, so comparing the \
                 wire value would merge two eras and hide the loss between them.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            era_transitions_total: counter_vec(
                &registry,
                "dz_recorder_era_transitions_total",
                "Era boundaries crossed, per channel instance, including the era the first \
                 datagram opened. Always dz_recorder_resets_total + 1 for a live instance; the \
                 pair is what makes dz_recorder_era_ordinal readable without the history.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            backward_sequence_total: counter_vec(
                &registry,
                "dz_recorder_backward_sequence_total",
                "Backward sequence motion that is not a reset and not inside the reordering \
                 window, per channel instance. A publisher that restarted its sequence space \
                 without advancing Reset Count lands here, and it is a finding: nothing else \
                 in this tier would notice it.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            forward_jump_total: counter_vec(
                &registry,
                "dz_recorder_forward_jump_total",
                "Datagrams whose sequence number was too far ahead to be loss, per channel \
                 instance. Nothing is credited to \
                 dz_recorder_missing_datagrams_on_arrival_total for one of these and the tracker does not adopt the number, so the instance's \
                 accounting survives it. A non-zero rate here is a sender fabricating sequence \
                 numbers on a channel this recorder joined, not a publisher losing datagrams.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            sequence_current: int_gauge_vec(
                &registry,
                "dz_recorder_sequence_current",
                "Highest sequence number seen in the current era, per channel instance.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            era_ordinal: int_gauge_vec(
                &registry,
                "dz_recorder_era_ordinal",
                "Monotonic era ordinal, per channel instance, counting from 1 at the first \
                 datagram. This is the value to group by, never the wire Reset Count: the wire \
                 value is a u8 that wraps, and two eras sharing one Reset Count are two eras.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            last_datagram_timestamp_seconds: gauge_vec(
                &registry,
                "dz_recorder_last_datagram_timestamp_seconds",
                "Unix receive timestamp of the last datagram, per channel instance. This is \
                 the channel-silence signal: alert on `time() - this`, guarded on the recorder \
                 having run long enough, since a pre-created series for a declared source that \
                 has never sent renders 0 and `time() - 0` is an age of decades.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            heartbeat_last_timestamp_seconds: gauge_vec(
                &registry,
                "dz_recorder_heartbeat_last_timestamp_seconds",
                "Unix receive timestamp of the last heartbeat-shaped datagram, per channel \
                 instance. Guard any staleness rule the same way \
                 dz_recorder_last_datagram_timestamp_seconds asks.",
                &labels,
                &["feed", "port_role", "channel", "source"],
            ),
            registry,
            feeds: config
                .feeds
                .iter()
                .map(|feed| FeedDefinition {
                    feed: feed.feed.to_owned(),
                    port_roles: feed.port_roles.to_vec(),
                    channel_ids: feed.channel_ids.to_vec(),
                    expected_sources: feed.expected_sources.to_vec(),
                    expected_magic: feed.expected_magic,
                })
                .collect(),
        };

        for feed in config.feeds {
            metrics.precreate_feed(feed);
        }
        metrics
    }

    /// Creates every child whose label values this feed's declaration makes
    /// knowable, so each renders 0 before the first datagram.
    fn precreate_feed(&self, feed: &FeedSeries<'_>) {
        let name = feed.feed;

        self.interface_drops_total.with_label_values(&[name]);
        self.instances_tracked.with_label_values(&[name]);
        self.instances_opened_total.with_label_values(&[name]);
        self.instances_evicted_total.with_label_values(&[name]);
        self.instances_refused_total.with_label_values(&[name]);
        self.declared_instances_evicted_total
            .with_label_values(&[name]);
        self.capture_drops_handle_total.with_label_values(&[name]);
        self.datagrams_unexpected_role_total
            .with_label_values(&[name]);

        // `other` exists from startup so that the panel for "traffic whose
        // header value this recorder has never been told to expect" is a zero
        // rather than a blank.
        self.datagram_magic_total
            .with_label_values(&[name, OTHER_VALUE]);
        if let Some(magic) = feed.expected_magic {
            self.datagram_magic_total
                .with_label_values(&[name, &magic_label(magic)]);
        }
        self.datagram_schema_version_total
            .with_label_values(&[name, OTHER_VALUE]);
        for version in SUPPORTED_SCHEMA_VERSIONS {
            self.datagram_schema_version_total
                .with_label_values(&[name, u8_label(version)]);
        }

        for role in feed.port_roles {
            let role = role.as_str();
            self.datagrams_total.with_label_values(&[name, role]);
            self.bytes_total.with_label_values(&[name, role]);
            self.send_to_recv_latency_seconds
                .with_label_values(&[name, role]);
            self.capture_drops_total.with_label_values(&[name, role]);
            self.rejoins_total.with_label_values(&[name, role]);
            for kind in RecvTimestampKind::ALL {
                self.recv_timestamps_total
                    .with_label_values(&[name, role, kind.as_str()]);
            }
            for reason in LatencyDropReason::ALL {
                self.latency_samples_dropped_total.with_label_values(&[
                    name,
                    role,
                    reason.as_str(),
                ]);
            }
            for kind in DeclaredLengthViolation::ALL {
                self.declared_length_violations_total.with_label_values(&[
                    name,
                    role,
                    kind.as_str(),
                ]);
            }
            for kind in DeclaredLengthMismatch::ALL {
                self.declared_length_mismatch_total
                    .with_label_values(&[name, role, kind.as_str()]);
            }
            for reason in UnreadableReason::ALL {
                self.unreadable_datagrams_total
                    .with_label_values(&[name, role, reason.as_str()]);
            }
            for channel_id in feed.channel_ids {
                let channel = u8_label(*channel_id);
                self.heartbeat_interval_seconds
                    .with_label_values(&[name, role, channel]);
                for source in feed.expected_sources {
                    self.instance_children(name, role, channel, &source.to_string());
                }
            }
        }
    }

    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Renders the Prometheus text exposition.
    ///
    /// A family with no children is dropped rather than encoded: the text
    /// encoder rejects an empty family, and with no declared sources the
    /// channel-instance families are legitimately empty until a datagram
    /// arrives. Encoding them would turn "nothing has been seen yet" into a
    /// failed scrape of everything.
    #[must_use]
    pub fn render(&self) -> String {
        let families: Vec<_> = self
            .registry
            .gather()
            .into_iter()
            .filter(|family| !family.get_metric().is_empty())
            .collect();
        TextEncoder::new()
            .encode_to_string(&families)
            .expect("text encoding of well-formed metric families cannot fail")
    }

    pub(crate) fn feed_definition(&self, feed: &str) -> Option<&FeedDefinition> {
        self.feeds.iter().find(|held| held.feed == feed)
    }

    pub(crate) fn feed_children(&self, feed: &str) -> FeedChildren {
        FeedChildren {
            interface_drops: self.interface_drops_total.with_label_values(&[feed]),
            instances_tracked: self.instances_tracked.with_label_values(&[feed]),
            instances_opened: self.instances_opened_total.with_label_values(&[feed]),
            instances_evicted: self.instances_evicted_total.with_label_values(&[feed]),
            instances_refused: self.instances_refused_total.with_label_values(&[feed]),
            declared_evicted: self
                .declared_instances_evicted_total
                .with_label_values(&[feed]),
            capture_drops_handle: self.capture_drops_handle_total.with_label_values(&[feed]),
            unexpected_role: self
                .datagrams_unexpected_role_total
                .with_label_values(&[feed]),
        }
    }

    pub(crate) fn role_children(&self, feed: &str, role: PortRole) -> RoleChildren {
        let role_label = role.as_str();
        // Built from each taxonomy's `ALL`, in `ALL` order, so that indexing a
        // slot by the variant's own discriminant is correct by construction
        // rather than by two lists happening to agree. `enum_slots` holds that.
        fn by_kind<const N: usize>(
            vec: &IntCounterVec,
            feed: &str,
            role_label: &str,
            tokens: [&str; N],
        ) -> [IntCounter; N] {
            std::array::from_fn(|slot| vec.with_label_values(&[feed, role_label, tokens[slot]]))
        }
        RoleChildren {
            datagrams: self.datagrams_total.with_label_values(&[feed, role_label]),
            bytes: self.bytes_total.with_label_values(&[feed, role_label]),
            latency: self
                .send_to_recv_latency_seconds
                .with_label_values(&[feed, role_label]),
            recv_ts: by_kind(
                &self.recv_timestamps_total,
                feed,
                role_label,
                [
                    RecvTimestampKind::ALL[0].as_str(),
                    RecvTimestampKind::ALL[1].as_str(),
                ],
            ),
            latency_dropped: by_kind(
                &self.latency_samples_dropped_total,
                feed,
                role_label,
                [
                    LatencyDropReason::ALL[0].as_str(),
                    LatencyDropReason::ALL[1].as_str(),
                    LatencyDropReason::ALL[2].as_str(),
                ],
            ),
            declared_violation: by_kind(
                &self.declared_length_violations_total,
                feed,
                role_label,
                [
                    DeclaredLengthViolation::ALL[0].as_str(),
                    DeclaredLengthViolation::ALL[1].as_str(),
                ],
            ),
            declared_mismatch: by_kind(
                &self.declared_length_mismatch_total,
                feed,
                role_label,
                [
                    DeclaredLengthMismatch::ALL[0].as_str(),
                    DeclaredLengthMismatch::ALL[1].as_str(),
                ],
            ),
            unreadable: [self.unreadable_datagrams_total.with_label_values(&[
                feed,
                role_label,
                UnreadableReason::ALL[0].as_str(),
            ])],
            capture_drops: self
                .capture_drops_total
                .with_label_values(&[feed, role_label]),
            rejoins: self.rejoins_total.with_label_values(&[feed, role_label]),
        }
    }

    pub(crate) fn magic_child(&self, feed: &str, label: &str) -> IntCounter {
        self.datagram_magic_total.with_label_values(&[feed, label])
    }

    pub(crate) fn schema_version_child(&self, feed: &str, label: &str) -> IntCounter {
        self.datagram_schema_version_total
            .with_label_values(&[feed, label])
    }

    pub(crate) fn segments_evicted(&self) -> &IntCounter {
        &self.segments_evicted_total
    }

    pub(crate) fn instance_children(
        &self,
        feed: &str,
        role: &str,
        channel: &str,
        source: &str,
    ) -> InstanceChildren {
        let key = [feed, role, channel, source];
        InstanceChildren {
            gaps: self.sequence_gaps_total.with_label_values(&key),
            missing: self.missing_datagrams_total.with_label_values(&key),
            duplicates: self.duplicate_datagrams_total.with_label_values(&key),
            reordered: self.reordered_datagrams_total.with_label_values(&key),
            resets: self.resets_total.with_label_values(&key),
            era_transitions: self.era_transitions_total.with_label_values(&key),
            backward: self.backward_sequence_total.with_label_values(&key),
            forward_jump: self.forward_jump_total.with_label_values(&key),
            sequence_current: self.sequence_current.with_label_values(&key),
            era_ordinal: self.era_ordinal.with_label_values(&key),
            last_datagram_timestamp: self.last_datagram_timestamp_seconds.with_label_values(&key),
            heartbeat_last_timestamp: self
                .heartbeat_last_timestamp_seconds
                .with_label_values(&key),
            heartbeat_interval: self
                .heartbeat_interval_seconds
                .with_label_values(&[feed, role, channel]),
        }
    }

    /// Drops one channel instance's series.
    ///
    /// Called on eviction, and only for a source the operator did not declare.
    /// Without this the label vectors would keep every instance the bounded map
    /// ever held, which is the unbounded growth the map exists to prevent,
    /// moved one layer down. Errors are ignored: the only failure is a series
    /// that is already gone.
    pub(crate) fn remove_instance_children(
        &self,
        feed: &str,
        role: &str,
        channel: &str,
        source: &str,
        channel_is_now_empty: bool,
    ) {
        let key = [feed, role, channel, source];
        let _ = self.sequence_gaps_total.remove_label_values(&key);
        let _ = self.missing_datagrams_total.remove_label_values(&key);
        let _ = self.duplicate_datagrams_total.remove_label_values(&key);
        let _ = self.reordered_datagrams_total.remove_label_values(&key);
        let _ = self.resets_total.remove_label_values(&key);
        let _ = self.era_transitions_total.remove_label_values(&key);
        let _ = self.backward_sequence_total.remove_label_values(&key);
        let _ = self.forward_jump_total.remove_label_values(&key);
        let _ = self.sequence_current.remove_label_values(&key);
        let _ = self.era_ordinal.remove_label_values(&key);
        let _ = self
            .last_datagram_timestamp_seconds
            .remove_label_values(&key);
        let _ = self
            .heartbeat_last_timestamp_seconds
            .remove_label_values(&key);
        // Keyed (feed, role, channel) and shared by every instance on that
        // channel, so it goes only when the last of them does. Left behind it
        // is the one series an evicted instance keeps, which is the bound this
        // removal exists to hold moved one layer down.
        if channel_is_now_empty {
            let _ = self
                .heartbeat_interval_seconds
                .remove_label_values(&[feed, role, channel]);
        }
    }
}
