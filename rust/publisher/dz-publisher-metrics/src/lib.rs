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
    EgressErrorReason, EventKind, ExitReason, InconsistencyKind, ParseErrorReason, ReconnectReason,
    RecoveryOutcome, RefdataLoadErrorReason, TimestampKind,
};
pub use metrics::{
    BookMetrics, EgressMetrics, IngressMetrics, LatencyMetrics, ProcessMetrics, RefdataMetrics,
};
pub use server::{serve, MetricsServer};
pub use venue_registry::VenueRegistry;

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
    /// `port_roles` is exactly the set of port roles this publisher
    /// operates (for example `&[PortRole::Mktdata, PortRole::Refdata]`).
    /// Every family labelled by `port_role` - and, crossed with it,
    /// `egress_errors_total` - is pre-created for exactly those roles at
    /// construction, so it renders at 0 from startup instead of only
    /// appearing once first touched. Passing a role this publisher does
    /// not operate would assert a channel that does not exist, so pass
    /// only the roles actually in use.
    ///
    /// Every other family whose labels are entirely closed enums is also
    /// pre-created here, for the same reason: absence in the exposition
    /// should mean "this publisher has emitted nothing on this series
    /// yet", not "this publisher's build does not know about this
    /// series". Families labelled by an open-ended value - the upstream
    /// source's own `message_type` vocabulary, a `connection` or
    /// `channel_id` chosen by deployment, or caller-supplied `build_info`
    /// labels - are deliberately left uncreated until first touched; see
    /// the per-field comments in `metrics/*.rs`.
    #[must_use]
    pub fn new(venue: &str, source_id: u16, port_roles: &[PortRole]) -> Self {
        let registry = Registry::new();
        let labels = opts::const_labels(venue, source_id);

        let ingress = IngressMetrics::new(&registry, &labels);
        let book = BookMetrics::new(&registry, &labels);
        let refdata = RefdataMetrics::new(&registry, &labels);
        let egress = EgressMetrics::new(&registry, &labels, port_roles);
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
            venue_registry: VenueRegistry::new(),
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
    #[must_use]
    pub fn render(&self) -> String {
        let mut families = self.registry.gather();
        families.extend(self.venue_registry.gather());

        let encoder = TextEncoder::new();
        encoder
            .encode_to_string(&families)
            .expect("text encoding of well-formed metric families cannot fail")
    }
}

// Re-exported so a caller doesn't need a direct `prometheus` dependency just
// to name the type `PortRole` methods already take.
pub use dz_edge_core::PortRole;
