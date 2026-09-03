//! Where the ingress metric families finally meet a registry.
//!
//! `dz-ingress-core` records every `dz_publisher_ingress_*` family through
//! [`IngressObserver`], which is a trait rather than a call into the metrics
//! crate so that a venue linking a transport does not inherit a Prometheus
//! client to be told a socket closed. This is the implementation the runtime
//! owes it, and every mapping in it is one-to-one by name.
//!
//! The two taxonomies the boundary declares a second time — [`ParseError`] and
//! [`DisconnectReason`] — are mapped here by **exhaustive match**. A variant
//! added on either side fails to compile here rather than producing a reason no
//! dashboard groups by, which is the same arrangement the metrics crate already
//! uses to hold its copy of the taxonomies to the boundary's.

use std::sync::Arc;

use dz_adapter_core::{AdapterError, DisconnectReason, ParseError};
use dz_ingress_core::{ConnectFailureReason as IngressConnectFailureReason, IngressObserver};
use dz_publisher_metrics::{
    AdapterErrorReason, ConnectFailureReason, ParseErrorReason, PublisherMetrics, ReconnectReason,
};

/// [`IngressObserver`] over the normative registry.
pub struct MetricsObserver {
    metrics: Arc<PublisherMetrics>,
    /// Adapter failures the closed family set has no series for. See
    /// [`IngressObserver::adapter_error`] and
    /// [`Self::adapter_errors`].
    adapter_errors: std::cell::Cell<u64>,
}

impl MetricsObserver {
    #[must_use]
    pub fn new(metrics: Arc<PublisherMetrics>) -> Self {
        Self {
            metrics,
            adapter_errors: std::cell::Cell::new(0),
        }
    }

    /// How many times an adapter method failed where nothing counts it.
    ///
    /// [`IngressObserver::adapter_error`] has a caller and no family: an
    /// adapter that cannot compose its own subscription is a real, retried
    /// failure that is not a parse error and is not one of the four reconnect
    /// reasons, all of which describe a session that ended rather than one that
    /// never got going. `dz_publisher_ingress_adapter_errors_total{reason}` now
    /// exists and every failure reaches it; the number stays because the exit
    /// report prints it and because a count a process can read back out of
    /// itself is what lets a test assert one without scraping.
    #[must_use]
    pub fn adapter_errors(&self) -> u64 {
        self.adapter_errors.get()
    }
}

impl IngressObserver for MetricsObserver {
    fn message(&self, message_type: &'static str, connection: &'static str) {
        self.metrics.ingress().message(message_type, connection);
    }

    fn bytes(&self, count: u64) {
        self.metrics.ingress().bytes(count);
    }

    fn duplicate(&self) {
        self.metrics.ingress().duplicate();
    }

    fn parse_error(&self, error: ParseError) {
        // Exhaustive, so a fifth reason on either side is a build failure.
        let reason = match error {
            ParseError::Schema { .. } => ParseErrorReason::Schema,
            ParseError::UnknownField { .. } => ParseErrorReason::UnknownField,
            ParseError::Malformed { .. } => ParseErrorReason::Malformed,
            ParseError::Truncated { .. } => ParseErrorReason::Truncated,
        };
        self.metrics.ingress().parse_error(reason);
    }

    fn connection_state(&self, connection: &'static str, connected: bool) {
        self.metrics
            .ingress()
            .set_connection_state(connection, connected);
    }

    fn reconnect(&self, reason: DisconnectReason) {
        let reason = match reason {
            DisconnectReason::Timeout => ReconnectReason::Timeout,
            DisconnectReason::RemoteClose => ReconnectReason::RemoteClose,
            DisconnectReason::RateLimit => ReconnectReason::RateLimit,
            DisconnectReason::AuthExpired => ReconnectReason::AuthExpired,
        };
        self.metrics.ingress().reconnect(reason);
    }

    fn connect_failure(&self, reason: IngressConnectFailureReason) {
        // Two names for one taxonomy — the transport's and the registry's — and
        // an exhaustive match so an eighth reason on either side is a build
        // failure rather than a series nobody declared.
        let reason = match reason {
            IngressConnectFailureReason::Refused => ConnectFailureReason::Refused,
            IngressConnectFailureReason::Unresolved => ConnectFailureReason::Unresolved,
            IngressConnectFailureReason::Tls => ConnectFailureReason::Tls,
            IngressConnectFailureReason::Timeout => ConnectFailureReason::Timeout,
            IngressConnectFailureReason::Unauthorized => ConnectFailureReason::Unauthorized,
            IngressConnectFailureReason::RateLimit => ConnectFailureReason::RateLimit,
            IngressConnectFailureReason::Rejected => ConnectFailureReason::Rejected,
        };
        self.metrics.ingress().connect_failure(reason);
    }

    fn rate_limited(&self) {
        self.metrics.ingress().rate_limited();
    }

    fn adapter_error(&self, error: AdapterError) {
        // Exhaustive, so a fourth `AdapterError` is a build failure here rather
        // than a refusal counted under whichever bucket a fallback arm named.
        let reason = match error {
            AdapterError::NotReady { .. } => AdapterErrorReason::NotReady,
            AdapterError::UnknownInstrument => AdapterErrorReason::UnknownInstrument,
            AdapterError::Internal { .. } => AdapterErrorReason::Internal,
        };
        self.metrics.ingress().adapter_error(reason);
        self.adapter_errors.set(self.adapter_errors.get() + 1);
    }
}
