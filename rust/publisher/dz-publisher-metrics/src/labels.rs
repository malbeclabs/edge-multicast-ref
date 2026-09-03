//! Closed label-value vocabularies.
//!
//! Every `reason`, `kind`, and `outcome` label in this crate is one of these
//! enums rather than a free-form string, so the taxonomy a dashboard groups
//! by cannot drift one call site at a time.

use std::sync::LazyLock;

use dz_edge_core::PortRole;

/// Why an ingress message failed to parse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ParseErrorReason {
    Schema,
    UnknownField,
    Malformed,
    Truncated,
}

impl ParseErrorReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::Schema,
        Self::UnknownField,
        Self::Malformed,
        Self::Truncated,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Schema => "schema",
            Self::UnknownField => "unknown_field",
            Self::Malformed => "malformed",
            Self::Truncated => "truncated",
        }
    }
}

/// Why an ingress connection reconnected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReconnectReason {
    Timeout,
    RemoteClose,
    RateLimit,
    AuthExpired,
}

impl ReconnectReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::Timeout,
        Self::RemoteClose,
        Self::RateLimit,
        Self::AuthExpired,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::RemoteClose => "remote_close",
            Self::RateLimit => "rate_limit",
            Self::AuthExpired => "auth_expired",
        }
    }
}

/// Why an ingress connection was never established.
///
/// # A proposed addition to the normative set
///
/// The governing playbook does not carry this taxonomy yet. It exists because
/// [`ReconnectReason`]'s four values all describe *a session that existed and
/// then stopped*, and none of them describes one that never started: a connect
/// the far side refused, a host that did not resolve, a TLS negotiation that
/// failed, and a handshake answered with a rejection are all invisible to
/// `dz_publisher_ingress_reconnects_total`, and folding any of them into
/// `remote_close` would make that counter mean two things in exactly the
/// incident where it is being read. Until this is added, the only series that
/// says anything about a publisher whose upstream never came up is
/// `dz_publisher_ingress_connection_state` staying at 0, which says *that* it
/// is down and nothing about *why*.
///
/// The seven values are the cases an operator acts differently on, not a
/// transcription of one transport's error enumeration. The split between the
/// three handshake rejections is the sharpest of them: a rejection for bad
/// credentials is a secret to replace, one for too many connections is a
/// deployment to pace, and putting both under one value is what currently
/// leaves the status a venue gave in a log line where no dashboard reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectFailureReason {
    /// The far side refused the connection, or the path to it was
    /// unreachable.
    Refused,
    /// The endpoint's host did not resolve.
    Unresolved,
    /// TLS negotiation failed: an untrusted chain, a name that did not match,
    /// no shared cipher suite.
    Tls,
    /// No connection within the configured connect budget.
    Timeout,
    /// The handshake was answered with a credential rejection.
    Unauthorized,
    /// The handshake was answered with a refusal to accept another
    /// connection right now.
    ///
    /// Distinct from `dz_publisher_ingress_rate_limited_total`, which counts
    /// the venue pacing an *established* connection.
    RateLimit,
    /// The handshake was answered with any other refusal.
    ///
    /// The bucket that keeps this vocabulary closed, in the same way `other`
    /// bounds the ingress `message_type` label: a status nobody enumerated is
    /// counted here rather than creating a series per status code.
    Rejected,
}

impl ConnectFailureReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 7] = [
        Self::Refused,
        Self::Unresolved,
        Self::Tls,
        Self::Timeout,
        Self::Unauthorized,
        Self::RateLimit,
        Self::Rejected,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refused => "refused",
            Self::Unresolved => "unresolved",
            Self::Tls => "tls",
            Self::Timeout => "timeout",
            Self::Unauthorized => "unauthorized",
            Self::RateLimit => "rate_limit",
            Self::Rejected => "rejected",
        }
    }
}

/// Why an adapter method failed when the driver called it.
///
/// # A proposed addition to the normative set
///
/// The governing playbook does not carry this taxonomy yet. An adapter that
/// cannot answer a connect — a subscription it could not compose, a credential
/// it could not read yet — is a real failure that is retried under the
/// reconnect backoff and counted nowhere: it is not a parse error, because no
/// payload was read, and it is not a reconnect, because the connection is torn
/// down before it was ever subscribed. Until this is added the number lives in
/// a counter on the observer and reaches an operator only as a log line.
///
/// These three values mirror `dz_adapter_core::AdapterError`, whose variants
/// *are* the three different actions an operator takes. The mirror is a second
/// declaration rather than a dependency, for the same reason
/// [`ParseErrorReason`] is: the boundary crate a venue links must not inherit a
/// Prometheus client to name a failure. The copies are held to each other by
/// `tests/adapter_taxonomy.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdapterErrorReason {
    /// The adapter cannot answer yet and expects to be able to later. Retried,
    /// and not a fault on its own — a rate that stays flat here is the signal,
    /// not a single count.
    NotReady,
    /// The adapter was asked about an instrument it does not hold: the
    /// runtime's admitted set and the adapter's own disagree.
    UnknownInstrument,
    /// The adapter failed at something it should have been able to do.
    Internal,
}

impl AdapterErrorReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 3] = [Self::NotReady, Self::UnknownInstrument, Self::Internal];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NotReady => "not_ready",
            Self::UnknownInstrument => "unknown_instrument",
            Self::Internal => "internal",
        }
    }
}

/// The kind of book inconsistency detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InconsistencyKind {
    MissingLevel,
    CrossedBook,
    SnapshotMismatch,
    SequenceGap,
}

impl InconsistencyKind {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::MissingLevel,
        Self::CrossedBook,
        Self::SnapshotMismatch,
        Self::SequenceGap,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingLevel => "missing_level",
            Self::CrossedBook => "crossed_book",
            Self::SnapshotMismatch => "snapshot_mismatch",
            Self::SequenceGap => "sequence_gap",
        }
    }
}

/// The outcome of a book recovery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RecoveryOutcome {
    Success,
    Failed,
}

impl RecoveryOutcome {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 2] = [Self::Success, Self::Failed];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }
}

/// Why a reference-data load failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RefdataLoadErrorReason {
    Timeout,
    RateLimit,
    Schema,
    Unavailable,
}

impl RefdataLoadErrorReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::Timeout,
        Self::RateLimit,
        Self::Schema,
        Self::Unavailable,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::RateLimit => "rate_limit",
            Self::Schema => "schema",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Why a normalized event could not be lowered to the wire.
///
/// # A proposed addition to the normative set
///
/// The governing playbook carries no family for a lowering refusal, and every
/// existing family is a worse home than none:
/// `dz_publisher_ingress_parse_errors_total` is about reading an upstream
/// payload and its four reasons name none of these, and
/// `dz_publisher_egress_errors_total`'s reasons are about a datagram, a port
/// role and a socket — a value the wire cannot state exactly never reached a
/// datagram at all. Folding a lowering refusal into either makes a panel an
/// operator is already reading mean two things.
///
/// The five values are kept apart because each is a different operator action,
/// and the distinction is lost the moment they are merged: too much precision
/// means the instrument's exponent is wrong, a value that is not a decimal
/// means the upstream changed its format, a value that does not fit means the
/// wire field is too narrow for what the venue quoted, a contract size that
/// does not divide means the size is wrong or the venue has started quoting on
/// a finer grid than its own contract admits, and an unknown handle means the
/// adapter is carrying one the instrument table does not hold.
///
/// Every one of them is per-event: the event is counted, dropped, and the next
/// one taken. One instrument whose exponent is wrong must not darken a feed,
/// which is why this is a counter and not a guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LoweringRefusalReason {
    /// The event names an instrument the table does not hold: a handle that was
    /// forged, or one that outlived its instrument's withdrawal.
    UnknownInstrument,
    /// The instrument's declared contract size does not divide the value the
    /// venue quoted exactly.
    InexactContract,
    /// More precision than the instrument's exponent can state.
    TooPrecise,
    /// Not a decimal number in the accepted grammar.
    Malformed,
    /// Exact, and past what the wire's integer can hold.
    Overflow,
}

impl LoweringRefusalReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 5] = [
        Self::UnknownInstrument,
        Self::InexactContract,
        Self::TooPrecise,
        Self::Malformed,
        Self::Overflow,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownInstrument => "unknown_instrument",
            Self::InexactContract => "inexact_contract",
            Self::TooPrecise => "too_precise",
            Self::Malformed => "malformed",
            Self::Overflow => "overflow",
        }
    }
}

/// Why an egress send failed.
///
/// # Two of these are proposed additions to the normative set
///
/// The governing playbook fixes five values here.
/// [`NotCarriedByFeed`](Self::NotCarriedByFeed) and
/// [`MalformedMessage`](Self::MalformedMessage) are proposed sixth and seventh:
/// both are the codec refusing a message on this send path, per-message and
/// recoverable exactly like the other five, and both are counted with the
/// `port_role` the send was on — so they belong on this family rather than on
/// one of their own, which would split "a message did not reach the wire"
/// across two families and make every panel and alert sum both.
///
/// What neither could be is an existing value. Each is a *different mistake*
/// from [`WrongPortRole`](Self::WrongPortRole), which is the nearest, and an
/// operator acts differently on each: a wrong port role is a send path wired to
/// the wrong socket, a message the feed does not carry is a publisher composing
/// for a feed it is not emitting, and a malformed message is a field
/// combination its own specification forbids. Folding either into
/// `wrong_port_role` would make the value an operator reads as "this went to
/// the wrong port" mean three things.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EgressErrorReason {
    MtuExceeded,
    SendWouldBlock,
    SocketError,
    NotRegistered,
    WrongPortRole,
    /// The feed this datagram carries does not define this message type. A
    /// proposed addition; see the enum documentation.
    NotCarriedByFeed,
    /// The message's fields are individually representable and their
    /// combination is one its own specification forbids. A proposed addition;
    /// see the enum documentation.
    MalformedMessage,
}

impl EgressErrorReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 7] = [
        Self::MtuExceeded,
        Self::SendWouldBlock,
        Self::SocketError,
        Self::NotRegistered,
        Self::WrongPortRole,
        Self::NotCarriedByFeed,
        Self::MalformedMessage,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MtuExceeded => "mtu_exceeded",
            Self::SendWouldBlock => "send_would_block",
            Self::SocketError => "socket_error",
            Self::NotRegistered => "not_registered",
            Self::WrongPortRole => "wrong_port_role",
            Self::NotCarriedByFeed => "not_carried_by_feed",
            Self::MalformedMessage => "malformed_message",
        }
    }
}

/// Which upstream timestamp a venue-to-recv latency observation is measured
/// against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampKind {
    ExchangeRecv,
    MatchingEngine,
    GatewaySend,
    BlockTime,
}

impl TimestampKind {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::ExchangeRecv,
        Self::MatchingEngine,
        Self::GatewaySend,
        Self::BlockTime,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ExchangeRecv => "exchange_recv",
            Self::MatchingEngine => "matching_engine",
            Self::GatewaySend => "gateway_send",
            Self::BlockTime => "block_time",
        }
    }
}

/// Which kind of event a recv-to-send latency observation covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventKind {
    BookUpdate,
    Trade,
}

impl EventKind {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 2] = [Self::BookUpdate, Self::Trade];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BookUpdate => "book_update",
            Self::Trade => "trade",
        }
    }
}

/// Why the process exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExitReason {
    IdleGuard,
    ConsistencyGuard,
    Signal,
    Panic,
}

impl ExitReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 4] = [
        Self::IdleGuard,
        Self::ConsistencyGuard,
        Self::Signal,
        Self::Panic,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IdleGuard => "idle_guard",
            Self::ConsistencyGuard => "consistency_guard",
            Self::Signal => "signal",
            Self::Panic => "panic",
        }
    }
}

/// A message type this feed family defines.
///
/// Unlike the ingress side, where `message_type` is whatever the upstream
/// source calls its messages, an outbound message type is this family's own
/// vocabulary: the set is fixed by the wire specifications, so it is an enum
/// here rather than a string a call site could spell three ways.
///
/// A variant exists for every message type these crates can encode. A new
/// feed's message types are added here in the same change that adds them to
/// the codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EgressMessageType {
    Heartbeat,
    EndOfSession,
    Quote,
    Trade,
    InstrumentDefinition,
    ManifestSummary,
    LevelUpdate,
    BookClear,
    SnapshotBegin,
    SnapshotLevel,
    SnapshotEnd,
    InstrumentReset,
}

impl EgressMessageType {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 12] = [
        Self::Heartbeat,
        Self::EndOfSession,
        Self::Quote,
        Self::Trade,
        Self::InstrumentDefinition,
        Self::ManifestSummary,
        Self::LevelUpdate,
        Self::BookClear,
        Self::SnapshotBegin,
        Self::SnapshotLevel,
        Self::SnapshotEnd,
        Self::InstrumentReset,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Heartbeat => "heartbeat",
            Self::EndOfSession => "end_of_session",
            Self::Quote => "quote",
            Self::Trade => "trade",
            Self::InstrumentDefinition => "instrument_definition",
            Self::ManifestSummary => "manifest_summary",
            Self::LevelUpdate => "level_update",
            Self::BookClear => "book_clear",
            Self::SnapshotBegin => "snapshot_begin",
            Self::SnapshotLevel => "snapshot_level",
            Self::SnapshotEnd => "snapshot_end",
            Self::InstrumentReset => "instrument_reset",
        }
    }

    /// The port roles the specification permits this message type on.
    ///
    /// This is a second copy of what `AppMessage::PORT_ROLES` declares in
    /// the codec crates, kept because a metric label is not a wire concern
    /// and this crate does not otherwise depend on the message types. The
    /// copy is held to the original by `port_roles_match_the_codec` in
    /// `tests/enum_tokens.rs`, which fails if the two disagree or if a
    /// message type is added to the codec without an entry here.
    ///
    /// It keeps pre-creation honest: a `quote` on the refdata port is not
    /// a series that can ever be written to.
    pub(crate) const fn port_roles(self) -> &'static [PortRole] {
        match self {
            Self::Heartbeat
            | Self::EndOfSession
            | Self::Quote
            | Self::Trade
            | Self::LevelUpdate
            | Self::BookClear
            | Self::InstrumentReset => &[PortRole::Mktdata],
            Self::InstrumentDefinition | Self::ManifestSummary => &[PortRole::Refdata],
            // The snapshot port carries one book state cut across datagrams,
            // and nothing else does. A snapshot message on the market-data
            // port is a series that can never be written to.
            Self::SnapshotBegin | Self::SnapshotLevel | Self::SnapshotEnd => &[PortRole::Snapshot],
        }
    }

    pub(crate) fn is_valid_on(self, port_role: PortRole) -> bool {
        self.port_roles().contains(&port_role)
    }
}

/// Decimal label values for every `u8` Channel ID, built once.
///
/// `channel_id` is a label on per-datagram code paths this crate measures in
/// microseconds; formatting it per call put a heap allocation on that path.
static CHANNEL_ID_LABELS: LazyLock<[String; 256]> =
    LazyLock::new(|| std::array::from_fn(|id| id.to_string()));

pub(crate) fn channel_id_label(channel_id: u8) -> &'static str {
    &CHANNEL_ID_LABELS[channel_id as usize]
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This crate keeps its own copy of each message type's permitted port
    /// roles, because a metric label is not a wire concern and the library
    /// does not otherwise depend on the feed crates. This holds the copy to
    /// the original: it fails if the two disagree, and it fails if a message
    /// type is added to the codec without an entry here.
    #[test]
    fn port_roles_match_the_codec() {
        use dz_edge_core::{AppMessage, EndOfSession, Heartbeat};
        use dz_edge_mbp::{
            BookClear, InstrumentReset, LevelUpdate, SnapshotBegin, SnapshotEnd, SnapshotLevel,
        };
        use dz_edge_refdata::{InstrumentDefinition, ManifestSummary};
        use dz_edge_tob::{Quote, Trade};

        fn check<M: AppMessage>(message_type: EgressMessageType) {
            assert_eq!(
                message_type.port_roles(),
                M::PORT_ROLES,
                "{} disagrees with the codec about its port roles",
                message_type.as_str()
            );
        }

        check::<Heartbeat>(EgressMessageType::Heartbeat);
        check::<EndOfSession>(EgressMessageType::EndOfSession);
        check::<Quote>(EgressMessageType::Quote);
        check::<Trade>(EgressMessageType::Trade);
        check::<InstrumentDefinition>(EgressMessageType::InstrumentDefinition);
        check::<ManifestSummary>(EgressMessageType::ManifestSummary);
        check::<LevelUpdate>(EgressMessageType::LevelUpdate);
        check::<BookClear>(EgressMessageType::BookClear);
        check::<SnapshotBegin>(EgressMessageType::SnapshotBegin);
        check::<SnapshotLevel>(EgressMessageType::SnapshotLevel);
        check::<SnapshotEnd>(EgressMessageType::SnapshotEnd);
        check::<InstrumentReset>(EgressMessageType::InstrumentReset);

        // Every variant is covered above. A message type added to the codec
        // with no entry here would be pre-created nowhere, silently.
        assert_eq!(
            EgressMessageType::ALL.len(),
            12,
            "a new message type needs a port-role entry and a line in this test"
        );
    }
}
