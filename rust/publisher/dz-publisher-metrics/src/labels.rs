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

/// Why an egress send failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EgressErrorReason {
    MtuExceeded,
    SendWouldBlock,
    SocketError,
    NotRegistered,
    WrongPortRole,
}

impl EgressErrorReason {
    /// Every variant, in no particular order. Used to pre-create every
    /// child series of this closed-label family at construction.
    pub(crate) const ALL: [Self; 5] = [
        Self::MtuExceeded,
        Self::SendWouldBlock,
        Self::SocketError,
        Self::NotRegistered,
        Self::WrongPortRole,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MtuExceeded => "mtu_exceeded",
            Self::SendWouldBlock => "send_would_block",
            Self::SocketError => "socket_error",
            Self::NotRegistered => "not_registered",
            Self::WrongPortRole => "wrong_port_role",
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
