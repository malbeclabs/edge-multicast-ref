//! The taxonomies of this crate's ingress half against the label enums they
//! are recorded as.
//!
//! [`IngressObserver`](dz_ingress_core::IngressObserver) hands the runtime a
//! [`DisconnectReason`] and a [`ParseError`], and
//! [`Adapter::source_timestamp_kind`](dz_adapter_core::Adapter::source_timestamp_kind)
//! hands it a [`VenueTimestampKind`]; the runtime has to turn each into a label
//! value. Those mappings exist because this crate does not depend on the metrics
//! crate — a venue must not inherit a Prometheus client to be told a socket
//! closed, or to say which of its own clocks it read — and a mapping nobody
//! compiles is a mapping that drifts.
//!
//! So the mappings are written here, over a dev-dependency: dev-dependencies are
//! not inherited by a venue, which is what makes it affordable. Every match is
//! exhaustive on both sides, so a variant added to either taxonomy without a
//! partner fails to compile rather than producing a series no dashboard groups
//! by. The tokens are then compared as strings, because two enums can be the
//! same arity and still disagree about a spelling — and the label value is what
//! reaches the dashboard.
//!
//! One of the four is this crate's own rather than the boundary's:
//! [`ConnectFailureReason`](dz_ingress_core::ConnectFailureReason) is declared
//! here because a connect failure is the transport's to observe and an adapter
//! owns no transport — the boundary must not gain a word no implementor of it
//! can say. It is mirrored the same way regardless, and in both directions,
//! because the argument that a mapping nobody compiles is a mapping that
//! drifts does not care which crate the original lives in.
//!
//! The timestamp mapping is here rather than in the boundary crate for the
//! reason the boundary crate exists: `dz-adapter-core` has one dependency, and
//! holding its own mirror to the original would mean naming the metrics crate —
//! and its Prometheus client — in the graph every venue resolves. This crate
//! already carries that dev-dependency for the other two.

use dz_adapter_core::{DisconnectReason, ParseError, VenueTimestampKind};
use dz_ingress_core::ConnectFailureReason;
use dz_publisher_metrics::{
    ConnectFailureReason as ConnectFailureLabel, ParseErrorReason, ReconnectReason, TimestampKind,
};

/// The mapping the runtime performs for
/// `dz_publisher_ingress_reconnects_total{reason}`.
fn as_label(reason: DisconnectReason) -> ReconnectReason {
    match reason {
        DisconnectReason::Timeout => ReconnectReason::Timeout,
        DisconnectReason::RemoteClose => ReconnectReason::RemoteClose,
        DisconnectReason::RateLimit => ReconnectReason::RateLimit,
        DisconnectReason::AuthExpired => ReconnectReason::AuthExpired,
    }
}

/// The mapping the runtime performs for
/// `dz_publisher_ingress_parse_errors_total{reason}`.
fn as_parse_label(error: ParseError) -> ParseErrorReason {
    match error {
        ParseError::Schema { .. } => ParseErrorReason::Schema,
        ParseError::UnknownField { .. } => ParseErrorReason::UnknownField,
        ParseError::Malformed { .. } => ParseErrorReason::Malformed,
        ParseError::Truncated { .. } => ParseErrorReason::Truncated,
    }
}

#[test]
fn every_disconnect_reason_the_driver_can_reach_is_a_reconnect_label() {
    for reason in DisconnectReason::ALL {
        assert_eq!(
            reason.as_str(),
            as_label(reason).as_str(),
            "the two spellings of {reason:?} have diverged"
        );
    }
}

#[test]
fn every_parse_error_the_driver_can_record_is_a_parse_error_label() {
    for error in ParseError::ALL {
        assert_eq!(
            error.as_str(),
            as_parse_label(error).as_str(),
            "the two spellings of {error:?} have diverged"
        );
    }
}

/// The mapping the runtime performs for
/// `dz_publisher_venue_to_recv_latency_seconds{timestamp_kind}`.
///
/// Exhaustive on the boundary's enum: a fifth kind there fails to compile until
/// somebody decides which label it is observed under.
fn as_timestamp_label(kind: VenueTimestampKind) -> TimestampKind {
    match kind {
        VenueTimestampKind::ExchangeRecv => TimestampKind::ExchangeRecv,
        VenueTimestampKind::MatchingEngine => TimestampKind::MatchingEngine,
        VenueTimestampKind::GatewaySend => TimestampKind::GatewaySend,
        VenueTimestampKind::BlockTime => TimestampKind::BlockTime,
    }
}

/// Exhaustive on the metrics crate's enum, in the other direction: a fifth
/// label there fails to compile until an adapter can declare it. A pre-created
/// child series that no adapter can ever name is a panel that stays empty for a
/// reason nobody can find.
fn declarable(kind: TimestampKind) -> VenueTimestampKind {
    match kind {
        TimestampKind::ExchangeRecv => VenueTimestampKind::ExchangeRecv,
        TimestampKind::MatchingEngine => VenueTimestampKind::MatchingEngine,
        TimestampKind::GatewaySend => VenueTimestampKind::GatewaySend,
        TimestampKind::BlockTime => VenueTimestampKind::BlockTime,
    }
}

#[test]
fn every_venue_timestamp_kind_an_adapter_can_declare_is_a_timestamp_kind_label() {
    for kind in VenueTimestampKind::ALL {
        assert_eq!(
            kind.as_str(),
            as_timestamp_label(kind).as_str(),
            "the two spellings of {kind:?} have diverged"
        );
    }
}

#[test]
fn every_timestamp_kind_label_can_be_declared_at_the_boundary() {
    // The metrics crate's own `ALL` is private to it, so the four are listed
    // here and `declarable` is what keeps the list honest: a fifth label makes
    // that match fail to compile.
    for kind in [
        TimestampKind::ExchangeRecv,
        TimestampKind::MatchingEngine,
        TimestampKind::GatewaySend,
        TimestampKind::BlockTime,
    ] {
        assert_eq!(declarable(kind).as_str(), kind.as_str());
    }
}

#[test]
fn the_two_timestamp_taxonomies_are_the_same_size() {
    // `VenueTimestampKind::ALL` is the boundary's own statement of its arity;
    // the list above is the metrics crate's, and the exhaustive match in
    // `declarable` is what holds the two together.
    assert_eq!(VenueTimestampKind::ALL.len(), 4);
}

/// The mapping the runtime performs for
/// `dz_publisher_ingress_connect_failures_total{reason}`.
///
/// Exhaustive on this crate's enum: an eighth reason the transport can classify
/// fails to compile until somebody decides which label it is counted under.
fn as_connect_failure_label(reason: ConnectFailureReason) -> ConnectFailureLabel {
    match reason {
        ConnectFailureReason::Refused => ConnectFailureLabel::Refused,
        ConnectFailureReason::Unresolved => ConnectFailureLabel::Unresolved,
        ConnectFailureReason::Tls => ConnectFailureLabel::Tls,
        ConnectFailureReason::Timeout => ConnectFailureLabel::Timeout,
        ConnectFailureReason::Unauthorized => ConnectFailureLabel::Unauthorized,
        ConnectFailureReason::RateLimit => ConnectFailureLabel::RateLimit,
        ConnectFailureReason::Rejected => ConnectFailureLabel::Rejected,
    }
}

/// Exhaustive on the metrics crate's enum, in the other direction: an eighth
/// label there fails to compile until the transport can classify it. A
/// pre-created child series nothing can ever reach is a panel that stays empty
/// for a reason nobody can find.
fn classifiable(reason: ConnectFailureLabel) -> ConnectFailureReason {
    match reason {
        ConnectFailureLabel::Refused => ConnectFailureReason::Refused,
        ConnectFailureLabel::Unresolved => ConnectFailureReason::Unresolved,
        ConnectFailureLabel::Tls => ConnectFailureReason::Tls,
        ConnectFailureLabel::Timeout => ConnectFailureReason::Timeout,
        ConnectFailureLabel::Unauthorized => ConnectFailureReason::Unauthorized,
        ConnectFailureLabel::RateLimit => ConnectFailureReason::RateLimit,
        ConnectFailureLabel::Rejected => ConnectFailureReason::Rejected,
    }
}

#[test]
fn every_connect_failure_reason_the_transport_can_classify_is_a_label() {
    for reason in ConnectFailureReason::ALL {
        assert_eq!(
            reason.as_str(),
            as_connect_failure_label(reason).as_str(),
            "the two spellings of {reason:?} have diverged"
        );
    }
}

#[test]
fn every_connect_failure_label_can_be_classified_by_the_transport() {
    // The metrics crate's own `ALL` is private to it, so the seven are listed
    // here and `classifiable` is what keeps the list honest.
    for reason in [
        ConnectFailureLabel::Refused,
        ConnectFailureLabel::Unresolved,
        ConnectFailureLabel::Tls,
        ConnectFailureLabel::Timeout,
        ConnectFailureLabel::Unauthorized,
        ConnectFailureLabel::RateLimit,
        ConnectFailureLabel::Rejected,
    ] {
        assert_eq!(classifiable(reason).as_str(), reason.as_str());
    }
    assert_eq!(ConnectFailureReason::ALL.len(), 7);
}
