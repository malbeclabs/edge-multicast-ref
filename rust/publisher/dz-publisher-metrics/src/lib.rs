//! The normative set of Prometheus metrics a publisher in the DoubleZero Edge
//! feed family must emit.
//!
//! A governing playbook declares a fixed set of metric names, labels, and
//! label values normative so that one dashboard works across every
//! publisher. This crate is how that set is inherited rather than
//! reimplemented: a publisher never constructs a metric. It calls a typed
//! method on [`PublisherMetrics`], and this crate owns every name, every
//! label, and every allowed label value.
//!
//! # No `instrument_id` label, anywhere
//!
//! A consumer publishes hundreds of instruments. A per-instrument label on
//! any of these series would multiply every series by the instrument count,
//! and the governing playbook forbids a label with that cardinality above
//! 100 instruments. This crate enforces that by never accepting an
//! `instrument_id` parameter on any method, on any metric family - not by
//! documenting a limit a caller could exceed.
//!
//! # Enum label values
//!
//! Every `reason`, `kind`, and `outcome` label value is a Rust enum (see
//! [`labels`]), not a string, so the taxonomy a dashboard groups by cannot
//! drift one call site at a time.
//!
//! # Proposed additions the playbook does not yet carry
//!
//! Three families and two label values here are **not** normative. Each exists
//! because a piece of work in this workspace produced a number with nowhere to
//! go and refused to invent a series for it, leaving the count exposed on a
//! struct or in a log line instead. They are marked as proposals in their own
//! documentation and in their `HELP` text, and each one states what it counts
//! and why no existing family could hold it, so that whoever updates the
//! playbook has the argument rather than only the name:
//!
//! - `dz_publisher_lowering_refusals_total{reason}` - see
//!   [`LoweringRefusalReason`].
//! - `dz_publisher_ingress_connect_failures_total{reason}` - see
//!   [`ConnectFailureReason`].
//! - `dz_publisher_ingress_adapter_errors_total{reason}` - see
//!   [`AdapterErrorReason`].
//! - `not_carried_by_feed` and `malformed_message` on the normative
//!   `dz_publisher_egress_errors_total{port_role,reason}` - see
//!   [`EgressErrorReason`].
//!
//! # Venue-specific metrics
//!
//! A publisher's own venue integration may need series the normative set
//! does not cover. [`PublisherMetrics::venue_registry`] gives it a second,
//! separate registry for exactly that, and refuses any name beginning
//! `dz_publisher_` so a venue cannot shadow the normative contract.

#![forbid(unsafe_code)]

mod buckets;
mod error;
mod labels;
mod metrics;
mod opts;
mod server;
mod venue_registry;

use prometheus::proto::LabelPair;
use prometheus::{Registry, TextEncoder};

pub use buckets::{LATENCY_BUCKETS, REFDATA_LOAD_DURATION_BUCKETS};
pub use error::MetricsError;
pub use labels::{
    AdapterErrorReason, ConnectFailureReason, EgressErrorReason, EgressMessageType, EventKind,
    ExitReason, InconsistencyKind, LoweringRefusalReason, ParseErrorReason, ReconnectReason,
    RecoveryOutcome, RefdataLoadErrorReason, TimestampKind,
};
pub use metrics::{
    BookMetrics, EgressMetrics, IngressMetrics, LatencyMetrics, LoweringMetrics, ProcessMetrics,
    RefdataMetrics,
};
pub use server::{serve, MetricsServer};
pub use venue_registry::VenueRegistry;

/// What one publisher process operates: the identity that labels every
/// series, and the sets that make its label values knowable at startup.
///
/// This is a struct rather than a positional argument list because every
/// field here exists to pre-create series, and a reader of the call site
/// should be able to see which set is which.
pub struct PublisherMetricsConfig<'a> {
    /// The venue this publisher sources from. Applied as a constant label
    /// to every series, normative and venue-specific alike.
    pub venue: &'a str,
    /// This publisher's source identifier, applied as a constant label
    /// alongside `venue`.
    pub source_id: u16,
    /// Exactly the port roles this publisher operates. Passing a role it
    /// does not operate asserts a channel that does not exist.
    pub port_roles: &'a [PortRole],
    /// The names of every ingress connection this publisher opens.
    ///
    /// `dz_publisher_ingress_connection_state` is pre-created at 0 for
    /// each, so the `== 0` alert that means "my feed is down" can fire on
    /// a publisher whose upstream never came up at all - the case the
    /// metric most exists for. Leaving this empty leaves that alert unable
    /// to fire until the first successful connection.
    pub connections: &'a [&'a str],
    /// Every Channel ID this publisher sends on, so the sequence,
    /// heartbeat and manifest gauges exist from startup.
    pub channel_ids: &'a [u8],
    /// The upstream source's own message-type names that this publisher
    /// counts individually on `dz_publisher_ingress_messages_total`.
    ///
    /// Anything the upstream sends that is not named here is counted under
    /// `other`. The label is the source's vocabulary and this crate cannot
    /// enumerate it, but an unbounded label on the highest-frequency path
    /// is the cardinality blow-up the crate refuses elsewhere: many
    /// upstream APIs name a message after the subscription that carried
    /// it, which is one series per instrument.
    pub ingress_message_types: &'a [&'a str],
}

/// The complete normative metric set for one publisher process.
///
/// Every series this type exposes carries `venue` and `source_id` as
/// constant labels, applied once here rather than threaded through every
/// call site - there is no path to a metric that omits them.
pub struct PublisherMetrics {
    registry: Registry,
    ingress: IngressMetrics,
    lowering: LoweringMetrics,
    book: BookMetrics,
    refdata: RefdataMetrics,
    egress: EgressMetrics,
    latency: LatencyMetrics,
    process: ProcessMetrics,
    venue_registry: VenueRegistry,
}

impl PublisherMetrics {
    /// Builds the full normative metric set for one publisher process.
    ///
    /// `venue` and `source_id` are applied as constant labels to every
    /// series this crate exposes, both the normative set and anything
    /// registered through [`Self::venue_registry`].
    ///
    /// Every family whose label values the config makes knowable is
    /// pre-created here, so it renders at 0 from startup rather than
    /// appearing only once first touched: absence in the exposition should
    /// mean "this publisher has emitted nothing on this series yet", not
    /// "this publisher's build does not know about this series", and an
    /// alert on `== 0` cannot fire on a series that does not exist.
    ///
    /// The only families still left uncreated are those labelled by a
    /// value no one can enumerate at startup: the upstream source's own
    /// `message_type` vocabulary on ingress, and caller-supplied
    /// `build_info` labels.
    #[must_use]
    pub fn new(config: &PublisherMetricsConfig<'_>) -> Self {
        let registry = Registry::new();
        let labels = opts::const_labels(config.venue, config.source_id);

        let ingress = IngressMetrics::new(
            &registry,
            &labels,
            config.connections,
            config.ingress_message_types,
        );
        let lowering = LoweringMetrics::new(&registry, &labels);
        let book = BookMetrics::new(&registry, &labels);
        let refdata =
            RefdataMetrics::new(&registry, &labels, config.channel_ids, config.port_roles);
        let egress = EgressMetrics::new(&registry, &labels, config.port_roles, config.channel_ids);
        let latency = LatencyMetrics::new(&registry, &labels);
        let process = ProcessMetrics::new(&registry, &labels);

        Self {
            registry,
            ingress,
            lowering,
            book,
            refdata,
            egress,
            latency,
            process,
            venue_registry: VenueRegistry::new(&labels),
        }
    }

    #[must_use]
    pub fn ingress(&self) -> &IngressMetrics {
        &self.ingress
    }

    /// The families for the step between ingress and egress. Every family
    /// here is a proposed addition to the normative set rather than one the
    /// governing playbook carries; see [`LoweringMetrics`].
    #[must_use]
    pub fn lowering(&self) -> &LoweringMetrics {
        &self.lowering
    }

    #[must_use]
    pub fn book(&self) -> &BookMetrics {
        &self.book
    }

    #[must_use]
    pub fn refdata(&self) -> &RefdataMetrics {
        &self.refdata
    }

    #[must_use]
    pub fn egress(&self) -> &EgressMetrics {
        &self.egress
    }

    #[must_use]
    pub fn latency(&self) -> &LatencyMetrics {
        &self.latency
    }

    #[must_use]
    pub fn process(&self) -> &ProcessMetrics {
        &self.process
    }

    /// A second registry for venue-specific series, separate from the
    /// normative set above. Rejects any name beginning `dz_publisher_` so a
    /// venue cannot shadow the shared contract.
    #[must_use]
    pub fn venue_registry(&self) -> &VenueRegistry {
        &self.venue_registry
    }

    /// Renders the Prometheus text exposition of both the normative set and
    /// the venue registry.
    /// Venue families are filtered before encoding, and a family that
    /// fails the filter is dropped rather than propagated.
    ///
    /// Registration can only inspect what a collector *describes*; nothing
    /// binds a `Collector` to gather what it described. Three things a
    /// venue collector can gather would otherwise take down every metric
    /// this publisher exposes, not just its own:
    ///
    /// - a name in the normative namespace, which produces two `# TYPE`
    ///   blocks for one family and makes the text parser reject the scrape;
    /// - a family with no name or no metrics, which the encoder rejects,
    ///   turning the whole render into an error;
    /// - an `UNTYPED` family, which the text encoder does not implement and
    ///   panics on, unwinding whichever thread called `render`.
    #[must_use]
    pub fn render(&self) -> String {
        // Owned here rather than left to a caller's ticker. Three HELP
        // strings in this crate tell an operator to guard a staleness rule
        // on `dz_publisher_uptime_seconds`, and a publisher that never
        // wired that ticker would leave the guard false forever and every
        // one of those alerts silently unable to fire - the failure this
        // crate's pre-creation work exists to eliminate, arriving through
        // the guard it recommends.
        self.process.refresh_uptime();

        let mut families = self.registry.gather();
        families.extend(
            self.venue_registry
                .gather()
                .into_iter()
                .filter(is_encodable_venue_family),
        );

        let encoder = TextEncoder::new();
        encoder
            .encode_to_string(&families)
            .expect("text encoding of well-formed metric families cannot fail")
    }
}

/// Whether a venue-registry family can be emitted without taking the rest
/// of the exposition with it. See [`PublisherMetrics::render`].
fn is_encodable_venue_family(family: &prometheus::proto::MetricFamily) -> bool {
    is_valid_metric_name(family.name())
        && !venue_registry::is_reserved_name(family.name())
        && !family.get_metric().is_empty()
        && family.get_field_type() != prometheus::proto::MetricType::UNTYPED
        // `register` rejects a *described* reserved label, but nothing
        // requires a collector to gather what it described. This registry
        // appends its constant labels without deduplicating, so a metric
        // that gathers a `venue` of its own renders carrying `venue`
        // twice, and the text parser rejects a sample with a repeated
        // label name - taking the whole scrape with it, which is what
        // every other condition here exists to prevent. Testing for the
        // duplicate rather than for the reserved name is what keeps the
        // constant labels this registry legitimately adds from tripping
        // it, and catches any other repeated name at the same time.
        && !family.get_metric().iter().any(has_duplicate_label_name)
}

/// Whether a metric carries the same label name twice, which makes the
/// text parser reject the entire scrape rather than only this sample.
fn has_duplicate_label_name(metric: &prometheus::proto::Metric) -> bool {
    let mut names: Vec<&str> = metric.get_label().iter().map(LabelPair::name).collect();
    names.sort_unstable();
    names.windows(2).any(|pair| pair[0] == pair[1])
}

/// Whether a name is one Prometheus will accept: `[a-zA-Z_:][a-zA-Z0-9_:]*`.
///
/// A family whose gathered name is not a valid metric name is the same
/// "the collector did not gather what it described" class as the rest, and
/// renders an exposition no parser will read.
fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

// Re-exported so a caller doesn't need a direct dependency just to name a
// type this crate's methods already take. For `prometheus` that is not only
// convenience: `VenueRegistry::register` takes a `Box<dyn Collector>`, and a
// caller whose own manifest resolved a different major would hit the
// famously opaque "expected `Box<dyn Collector>`, found `Box<dyn Collector>`".
// Going through this re-export makes the version this crate links reachable.
pub use dz_edge_core::PortRole;
pub use prometheus;
