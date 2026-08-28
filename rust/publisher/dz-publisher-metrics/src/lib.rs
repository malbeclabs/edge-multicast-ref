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

use prometheus::{Registry, TextEncoder};

pub use buckets::{LATENCY_BUCKETS, REFDATA_LOAD_DURATION_BUCKETS};
pub use error::MetricsError;
pub use labels::{
    EgressErrorReason, EgressMessageType, EventKind, ExitReason, InconsistencyKind,
    ParseErrorReason, ReconnectReason, RecoveryOutcome, RefdataLoadErrorReason, TimestampKind,
};
pub use metrics::{
    BookMetrics, EgressMetrics, IngressMetrics, LatencyMetrics, ProcessMetrics, RefdataMetrics,
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
}

/// The complete normative metric set for one publisher process.
///
/// Every series this type exposes carries `venue` and `source_id` as
/// constant labels, applied once here rather than threaded through every
/// call site - there is no path to a metric that omits them.
pub struct PublisherMetrics {
    registry: Registry,
    ingress: IngressMetrics,
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

        let ingress = IngressMetrics::new(&registry, &labels, config.connections);
        let book = BookMetrics::new(&registry, &labels);
        let refdata = RefdataMetrics::new(&registry, &labels, config.channel_ids);
        let egress = EgressMetrics::new(&registry, &labels, config.port_roles, config.channel_ids);
        let latency = LatencyMetrics::new(&registry, &labels);
        let process = ProcessMetrics::new(&registry, &labels);

        Self {
            registry,
            ingress,
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
    /// A venue family whose gathered name lands in the normative
    /// namespace is dropped here rather than emitted. Registration already
    /// rejects those names, but it can only inspect what a collector
    /// *describes*; nothing binds a `Collector` to emit the same names it
    /// describes. Emitting one would produce two `# TYPE` blocks for a
    /// single family, which makes Prometheus reject the whole scrape - so
    /// one misbehaving venue collector would take every metric down with
    /// it, not just its own.
    #[must_use]
    pub fn render(&self) -> String {
        let mut families = self.registry.gather();
        families.extend(
            self.venue_registry
                .gather()
                .into_iter()
                .filter(|family| !venue_registry::is_reserved_name(family.name())),
        );

        let encoder = TextEncoder::new();
        encoder
            .encode_to_string(&families)
            .expect("text encoding of well-formed metric families cannot fail")
    }
}

// Re-exported so a caller doesn't need a direct dependency just to name a
// type this crate's methods already take. For `prometheus` that is not only
// convenience: `VenueRegistry::register` takes a `Box<dyn Collector>`, and a
// caller whose own manifest resolved a different major would hit the
// famously opaque "expected `Box<dyn Collector>`, found `Box<dyn Collector>`".
// Going through this re-export makes the version this crate links reachable.
pub use dz_edge_core::PortRole;
pub use prometheus;
