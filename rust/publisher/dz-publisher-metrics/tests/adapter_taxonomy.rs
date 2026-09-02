//! The two taxonomies `dz-adapter-core` mirrors, held to the label enums here.
//!
//! An adapter returns a parse failure, and is told why a connection ended, in
//! vocabularies that are also metric labels: this crate's `ParseErrorReason` is
//! what `dz_publisher_ingress_parse_errors_total{reason}` counts by, and
//! `ReconnectReason` is what `dz_publisher_ingress_reconnects_total{reason}`
//! counts by. So an adapter cannot fail to parse without the right series
//! moving, and cannot invent a fifth reason no dashboard groups by.
//!
//! That requires two declarations of each taxonomy, because `dz-adapter-core`
//! must depend on nothing: a venue implementing the trait would otherwise
//! inherit a Prometheus client in order to name a parse error. This is the same
//! arrangement, for the same reason, that `EgressMessageType::port_roles` and
//! `AppMessage::PORT_ROLES` already sit in — *a metric label is not a wire
//! concern* — and it is safe only because the copies are held to each other
//! rather than kept in step by hand.
//!
//! A third taxonomy joins them here: `AdapterErrorReason`, which is what the
//! proposed `dz_publisher_ingress_adapter_errors_total{reason}` counts by. It
//! mirrors `dz_adapter_core::AdapterError` for the same reason and is held to it
//! the same way, so that an adapter cannot fail a request the runtime made
//! without the right series moving.
//!
//! Both directions are exhaustive matches rather than lists, so a variant added
//! on either side fails to compile here rather than passing a comparison of two
//! lists that were each edited separately.

use dz_adapter_core::{AdapterError, DisconnectReason, ParseError};
use dz_publisher_metrics::{AdapterErrorReason, ParseErrorReason, ReconnectReason};

/// The label a parse failure is counted under.
///
/// Exhaustive on the adapter's enum: a fifth reason there fails to compile
/// until somebody decides which label it is counted as, which is the decision
/// worth forcing.
fn label_for(error: ParseError) -> ParseErrorReason {
    match error {
        ParseError::Schema { .. } => ParseErrorReason::Schema,
        ParseError::UnknownField { .. } => ParseErrorReason::UnknownField,
        ParseError::Malformed { .. } => ParseErrorReason::Malformed,
        ParseError::Truncated { .. } => ParseErrorReason::Truncated,
    }
}

/// Exhaustive on this crate's enum, in the other direction: a fifth label here
/// fails to compile until the adapter boundary can express it. A reason a
/// dashboard has a panel for that no adapter can report is a panel that stays
/// empty for a reason nobody can find.
fn reportable(reason: ParseErrorReason) -> ParseError {
    match reason {
        ParseErrorReason::Schema => ParseError::schema(""),
        ParseErrorReason::UnknownField => ParseError::unknown_field(""),
        ParseErrorReason::Malformed => ParseError::malformed(""),
        ParseErrorReason::Truncated => ParseError::truncated(""),
    }
}

#[test]
fn every_parse_error_carries_its_own_label_token() {
    for error in ParseError::ALL {
        assert_eq!(
            error.as_str(),
            label_for(error).as_str(),
            "the adapter's `{}` and its label disagree",
            error.as_str()
        );
    }
}

#[test]
fn every_parse_label_is_reachable_from_the_adapter_boundary() {
    for reason in [
        ParseErrorReason::Schema,
        ParseErrorReason::UnknownField,
        ParseErrorReason::Malformed,
        ParseErrorReason::Truncated,
    ] {
        assert_eq!(reportable(reason).as_str(), reason.as_str());
    }
}

#[test]
fn the_two_parse_taxonomies_are_the_same_size() {
    // `ParseError::ALL` is the adapter's own statement of its arity. The list
    // above is this crate's, and the exhaustive match in `reportable` is what
    // keeps it honest.
    assert_eq!(ParseError::ALL.len(), 4);
}

/// The label a disconnect is counted under. Exhaustive on the adapter's enum.
fn reconnect_label(reason: DisconnectReason) -> ReconnectReason {
    match reason {
        DisconnectReason::Timeout => ReconnectReason::Timeout,
        DisconnectReason::RemoteClose => ReconnectReason::RemoteClose,
        DisconnectReason::RateLimit => ReconnectReason::RateLimit,
        DisconnectReason::AuthExpired => ReconnectReason::AuthExpired,
    }
}

/// Exhaustive on this crate's enum, in the other direction.
fn tellable(reason: ReconnectReason) -> DisconnectReason {
    match reason {
        ReconnectReason::Timeout => DisconnectReason::Timeout,
        ReconnectReason::RemoteClose => DisconnectReason::RemoteClose,
        ReconnectReason::RateLimit => DisconnectReason::RateLimit,
        ReconnectReason::AuthExpired => DisconnectReason::AuthExpired,
    }
}

#[test]
fn every_disconnect_reason_carries_its_own_label_token() {
    for reason in DisconnectReason::ALL {
        assert_eq!(
            reason.as_str(),
            reconnect_label(reason).as_str(),
            "the adapter's `{}` and its label disagree",
            reason.as_str()
        );
    }
}

#[test]
fn every_reconnect_label_can_be_told_to_an_adapter() {
    for reason in [
        ReconnectReason::Timeout,
        ReconnectReason::RemoteClose,
        ReconnectReason::RateLimit,
        ReconnectReason::AuthExpired,
    ] {
        assert_eq!(tellable(reason).as_str(), reason.as_str());
    }
}

#[test]
fn the_two_reconnect_taxonomies_are_the_same_size() {
    assert_eq!(DisconnectReason::ALL.len(), 4);
}

/// The label an adapter failure is counted under.
///
/// Exhaustive on the adapter's enum: a fourth variant there fails to compile
/// until somebody decides which label it is counted as. `AdapterError` declares
/// no `ALL` of its own — unlike `ParseError` — so this match and the one below
/// are the whole of what holds the two in step.
fn adapter_label(error: AdapterError) -> AdapterErrorReason {
    match error {
        AdapterError::NotReady { .. } => AdapterErrorReason::NotReady,
        AdapterError::UnknownInstrument => AdapterErrorReason::UnknownInstrument,
        AdapterError::Internal { .. } => AdapterErrorReason::Internal,
    }
}

/// Exhaustive on this crate's enum, in the other direction: a fourth label here
/// fails to compile until an adapter can report it. A reason a dashboard has a
/// panel for that no adapter can produce is a panel that stays empty for a
/// reason nobody can find.
fn raisable(reason: AdapterErrorReason) -> AdapterError {
    match reason {
        AdapterErrorReason::NotReady => AdapterError::NotReady { detail: "" },
        AdapterErrorReason::UnknownInstrument => AdapterError::UnknownInstrument,
        AdapterErrorReason::Internal => AdapterError::Internal { detail: "" },
    }
}

#[test]
fn every_adapter_error_carries_its_own_label_token() {
    // The tokens are literals here rather than read off the enum under test.
    for (error, token) in [
        (AdapterError::NotReady { detail: "x" }, "not_ready"),
        (AdapterError::UnknownInstrument, "unknown_instrument"),
        (AdapterError::Internal { detail: "x" }, "internal"),
    ] {
        assert_eq!(adapter_label(error).as_str(), token);
    }
}

#[test]
fn every_adapter_label_is_reachable_from_the_adapter_boundary() {
    for reason in [
        AdapterErrorReason::NotReady,
        AdapterErrorReason::UnknownInstrument,
        AdapterErrorReason::Internal,
    ] {
        assert_eq!(adapter_label(raisable(reason)), reason);
    }
}

#[test]
fn every_adapter_label_has_a_distinct_token() {
    let mut tokens = [
        AdapterErrorReason::NotReady.as_str(),
        AdapterErrorReason::UnknownInstrument.as_str(),
        AdapterErrorReason::Internal.as_str(),
    ];
    tokens.sort_unstable();
    let count = tokens.len();
    let mut unique = tokens.to_vec();
    unique.dedup();
    assert_eq!(
        unique.len(),
        count,
        "two adapter reasons share a label value"
    );
}
