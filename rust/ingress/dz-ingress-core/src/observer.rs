//! Where the ingress metric families are recorded from, without a metrics
//! client.

use dz_adapter_core::{AdapterError, DisconnectReason, ParseError};

use crate::ConnectFailureReason;

/// The normative `dz_publisher_ingress_*` families, one method each.
///
/// # Why a trait and not the metrics crate
///
/// This is the crate a venue links to get a transport, and
/// `dz-publisher-metrics` pulls in a Prometheus client and a registry. A venue
/// must not inherit those to be told a socket closed — the same argument that
/// keeps the boundary crate down to two dependencies, one level up. The runtime
/// owns the registry, so the runtime implements this by forwarding to
/// `IngressMetrics`, and every mapping it has to make is one-to-one by name.
///
/// # Why one method per family and no defaults
///
/// The metric name set is closed. Asking publishers for common metrics did not
/// produce them, so the shape here is the one that does not permit an omission:
/// there is a method for every family in the ingress group, none of them
/// defaulted, so an implementation that forgets one does not compile. And there
/// is no method that is not a family, so nothing recorded from this crate can be
/// a series nobody has a panel for.
///
/// Two of the methods are exceptions worth stating rather than hiding, because
/// each is a gap between this crate and the closed family set:
///
/// - [`duplicate`](Self::duplicate) has a family and no caller here. The
///   boundary gives an adapter no way to report the upstream's own sequence
///   number or to declare a payload a repeat of one already published, so the
///   driver cannot tell. It is declared because the runtime's implementation
///   should be a complete mapping of the group, and because this is the place
///   the missing channel will be noticed.
/// - [`adapter_error`](Self::adapter_error) is counted, and not by the closed
///   set. An adapter that cannot compose its own subscription is a real,
///   retried failure that none of the normative families can hold: it is not a
///   parse error, and the four reconnect reasons all describe a session that
///   ended rather than one that never got going. So it has a family of its own,
///   `dz_publisher_ingress_adapter_errors_total{reason}`, which this repository
///   *proposes* rather than transcribes — the distinction that matters to
///   anybody comparing a publisher's exposition against the playbook's list.
///   It had no family at all when this trait was written, and the sentence
///   saying so outlived that by one change.
pub trait IngressObserver {
    /// `dz_publisher_ingress_messages_total{message_type,connection}`.
    ///
    /// One call per upstream message the adapter recognised — which is what the
    /// adapter reports through
    /// [`EventSink::upstream_message`](dz_adapter_core::EventSink::upstream_message),
    /// not one call per payload: a payload carrying a batch is several
    /// messages. A `message_type` the publisher did not declare is folded to
    /// `other` by the metrics crate, so this does no filtering of its own.
    fn message(&self, message_type: &'static str, connection: &'static str);

    /// `dz_publisher_ingress_bytes_total`.
    ///
    /// Payload bytes as the adapter sees them, not bytes off the socket:
    /// transport headers, keepalives and TLS overhead are not the venue's data
    /// and counting them would make the series disagree with the message rate
    /// in a way nobody could reconcile.
    fn bytes(&self, count: u64);

    /// `dz_publisher_ingress_duplicates_total`. See the trait note: nothing in
    /// this crate can call it yet.
    fn duplicate(&self);

    /// `dz_publisher_ingress_parse_errors_total{reason}`.
    ///
    /// Takes the adapter's own error, because its variants *are* the label
    /// values. The implementation maps [`ParseError`] to the metrics crate's
    /// `ParseErrorReason`, which is the same taxonomy declared twice so that
    /// this crate need not depend on that one.
    fn parse_error(&self, error: ParseError);

    /// `dz_publisher_ingress_connection_state{connection}`.
    ///
    /// Set true only once the adapter's subscriptions have been sent, not when
    /// the socket came up. A connection that is open and subscribed to nothing
    /// is not connected in any sense a subscriber benefits from, and this is
    /// the series an operator alerts on.
    fn connection_state(&self, connection: &'static str, connected: bool);

    /// `dz_publisher_ingress_reconnects_total{reason}`.
    ///
    /// Recorded when an established, subscribed connection ends, labelled by
    /// why. Not recorded for a connect attempt that never succeeded: the label
    /// set has no value for that, and folding it into one of the four would
    /// make the counter mean two things.
    fn reconnect(&self, reason: DisconnectReason);

    /// `dz_publisher_ingress_connect_failures_total{reason}`.
    ///
    /// A connection that was never established, labelled by why — the case
    /// [`reconnect`](Self::reconnect) deliberately does not cover. The two
    /// families answer different questions: a reconnect counter rising is a
    /// flapping session, and this rising with the state gauge stuck at 0 is a
    /// publisher that never came up, which is the outage that needs the reason
    /// most and had nowhere to put it.
    fn connect_failure(&self, reason: ConnectFailureReason);

    /// `dz_publisher_ingress_rate_limited_total`.
    ///
    /// The venue rate-limiting us. Not this publisher's own configured pacing,
    /// which is expected behaviour — see [`RateLimiter`](crate::RateLimiter).
    fn rate_limited(&self);

    /// `dz_publisher_ingress_adapter_errors_total{reason}`, which is a family
    /// this repository proposes rather than one the playbook lists. See the
    /// trait note for why none of the normative ones can hold it.
    fn adapter_error(&self, error: AdapterError);
}
