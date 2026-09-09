//! The configuration a venue's constructor reads, and none of the document it
//! came out of.
//!
//! Every type here is one an [`AdapterContext`](crate::AdapterContext) accessor
//! hands out, which is the only reason any of them is in this crate: the
//! accessors *are* the boundary, so a type a venue reads through one cannot
//! live above the crate the venue links. What stayed behind in
//! `dz-publisher-runtime` is everything the context does not expose — the
//! document, the sections it parses, the egress policy, and the startup error
//! that names them.
//!
//! # Two `resolve` functions report a small error rather than a startup one
//!
//! [`FeedSpec::resolve`] and [`SourceRole::resolve`] refused with the
//! publisher's own `StartupError`, which names the egress, the reference-data
//! registry and the metrics registry in other variants and cannot come here.
//! Each returns a struct of its own instead, carrying exactly the fields the
//! variant carried, and the runtime maps it into the variant it always
//! produced — so the operator-facing message is unchanged and so is the shape
//! anything matching on it sees.

use std::path::PathBuf;

use dz_adapter_core::ConnectionId;
use dz_edge_core::{Feed as WireFeed, PortRole};
use dz_edge_mbp::MarketByPrice;
use dz_edge_tob::TopOfBook;
use dz_ingress_core::Kind;
use serde::Deserialize;

/// What a publisher does with one source.
///
/// A closed set of tokens, so a value outside it is a load error naming what
/// would have been accepted rather than a role nothing implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceRole {
    /// The source this publisher publishes from.
    ///
    /// **Exactly one enabled `primary`, publisher-wide** — not one per feed.
    /// Every source's payloads reach one adapter, the adapter emits events, and
    /// no event carries the source it came from, so nothing here can confine one
    /// source's data to one feed; a per-feed rule would describe routing the
    /// runtime does not do. The check, and what a document that breaks the rule
    /// is told, are the runtime's: see `dz_publisher_runtime::config` and
    /// `StartupError::SourcePrimaries`.
    #[default]
    Primary,
    /// Connected, driven and counted, and carried for the race comparison
    /// against the primary — *which one saw a given state first*.
    ///
    /// Not an event-for-event diff, and the design says why: two connections to
    /// one venue do not deliver identical streams, so what is comparable is
    /// state at aligned instants plus the distributions of first observation.
    Comparison,
}

impl SourceRole {
    /// Every role, in the order the tokens below are listed.
    pub const ALL: [Self; 2] = [Self::Primary, Self::Comparison];

    /// The token a document states, and the metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Comparison => "comparison",
        }
    }

    /// The tokens, for an error message.
    pub const TOKEN_LIST: &'static str = "primary, comparison";

    /// Resolve a token.
    ///
    /// # Errors
    ///
    /// [`UnknownSourceRole`] naming the token and the set. The runtime turns it
    /// into `StartupError::UnknownSourceRole`, which is the variant with these
    /// two fields and always was.
    pub fn resolve(token: &str) -> Result<Self, UnknownSourceRole> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.as_str() == token)
            .ok_or_else(|| UnknownSourceRole {
                token: token.to_owned(),
                supported: Self::TOKEN_LIST,
            })
    }
}

/// A `[[source]] role` token naming no role this build implements.
///
/// The fields are the ones `StartupError::UnknownSourceRole` carries, because
/// that is what this becomes; the message here is what a caller with no
/// startup error of its own would print.
#[derive(Debug, Clone, thiserror::Error)]
#[error("`[[source]] role = \"{token}\"` names no role: the roles are {supported}")]
pub struct UnknownSourceRole {
    /// What the document said.
    pub token: String,
    /// What it could have said.
    pub supported: &'static str,
}

/// `[adapter.replay]`: a fixture directory for an offline run.
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ReplayConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub path: Option<PathBuf>,
}

/// A feed specification this build can emit.
///
/// A closed set and a total match, for the same reason
/// [`dz_ingress_core::Kind`] is: what makes a feed emittable is something being
/// able to compose, count and transmit its messages, and a value a
/// configuration can name that nothing composes is a value that resolves to
/// nothing at startup. Not `#[non_exhaustive]`, so a feed added here breaks
/// every match over this type — including `dz_publisher_runtime::run()`'s,
/// which is where the composing happens.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedSpec {
    /// `dz-edge-tob`: `Quote` and `Trade`, on the mktdata and refdata port
    /// roles.
    TopOfBook,
    /// `dz-edge-mbp`: `LevelUpdate` and `BookClear` on mktdata, the three
    /// snapshot message types on the snapshot port role, and `Trade` — which
    /// is byte-identical to top-of-book's, per the wire's cross-specification
    /// policy for `0x04`.
    MarketByPrice,
}

impl FeedSpec {
    /// Every specification, in the order an error message names them.
    pub const ALL: [Self; 2] = [Self::TopOfBook, Self::MarketByPrice];

    /// The specifications this build can emit, for an error message.
    ///
    /// A literal so that it is a `&'static str` usable in a `thiserror` format
    /// string; held to [`ALL`](Self::ALL) by `dz-publisher-runtime`'s
    /// `tests/feed_specs.rs::the_supported_list_is_the_specification_set`.
    pub const SUPPORTED: &'static str = "top-of-book, market-by-price";

    /// The configuration token, which is the codec crate's own `Feed::NAME`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TopOfBook => <TopOfBook as WireFeed>::NAME,
            Self::MarketByPrice => <MarketByPrice as WireFeed>::NAME,
        }
    }

    /// The port roles a feed of this specification operates.
    ///
    /// Handed to the metrics crate, which pre-creates one child series per role
    /// — so passing a role this publisher does not operate would assert a
    /// channel that does not exist, and omitting one it does operate would
    /// leave a panel blank until the first datagram.
    #[must_use]
    pub const fn port_roles(self) -> &'static [PortRole] {
        match self {
            Self::TopOfBook => &[PortRole::Mktdata, PortRole::Refdata],
            // The third role is the whole difference at this level: a
            // subscriber to a depth feed holds a book that only exists because
            // it applied every message in order, so it needs somewhere to
            // recover from.
            Self::MarketByPrice => &[PortRole::Mktdata, PortRole::Refdata, PortRole::Snapshot],
        }
    }

    /// Whether this specification carries a snapshot port role.
    #[must_use]
    pub const fn has_snapshot_port(self) -> bool {
        match self {
            Self::TopOfBook => false,
            Self::MarketByPrice => true,
        }
    }

    /// Resolve a `[[feed]] spec` token.
    ///
    /// The tokens are the codec crates' own `Feed::NAME` constants rather than
    /// literals here, so a configuration names a feed by the name the crate
    /// that implements it gives it, and the two cannot drift.
    ///
    /// # Errors
    ///
    /// [`UnsupportedFeedSpec`], naming what this build can emit. There is no
    /// default: a feed is not a thing to guess at, and the audit's misspelled
    /// section became the wrong transport precisely because something
    /// defaulted. The runtime turns it into
    /// `StartupError::UnsupportedFeedSpec`, which is the variant with these two
    /// fields and always was.
    pub fn resolve(token: &str) -> Result<Self, UnsupportedFeedSpec> {
        Self::ALL
            .into_iter()
            .find(|spec| spec.as_str() == token)
            .ok_or_else(|| UnsupportedFeedSpec {
                spec: token.to_owned(),
                supported: Self::SUPPORTED.to_owned(),
            })
    }
}

/// A `[[feed]] spec` token naming no feed specification this build can emit.
///
/// The fields are the ones `StartupError::UnsupportedFeedSpec` carries, because
/// that is what this becomes.
#[derive(Debug, Clone, thiserror::Error)]
#[error("`[[feed]] spec = \"{spec}\"` is not a feed this build emits: {supported}")]
pub struct UnsupportedFeedSpec {
    /// What the document said.
    pub spec: String,
    /// What it could have said.
    pub supported: String,
}

/// One upstream connection, resolved.
pub struct Source {
    /// The name, as every metric label carries it.
    ///
    /// # Why this is leaked, once, at startup
    ///
    /// [`ConnectionId`] holds a `&'static str` on purpose:
    /// `dz_publisher_ingress_connection_state` is pre-created at 0 for each
    /// declared name, which is what lets the `== 0` alert fire for a publisher
    /// whose upstream never came up at all — the case the metric most exists
    /// for. A name that only became known when a connection first succeeded
    /// would have no series until then, which is exactly the case that has to
    /// alert.
    ///
    /// The name now comes from the document, so that the file an operator reads
    /// and the label a dashboard groups by are one string. Reconciling those two
    /// facts costs one leak per configured source, before the metric registry
    /// exists and never again: it is bounded by the document, it happens once,
    /// and the alternatives are a label the file cannot state or a series that
    /// appears too late to be alerted on.
    pub connection: ConnectionId,
    /// Which transport carries it.
    pub kind: Kind,
    /// What this publisher does with it. Consumed at runtime: only a
    /// `primary`'s fatal error ends the process. See the runtime's
    /// `SourceSection`.
    pub role: SourceRole,
    /// The venue's own endpoint keys.
    pub upstream: toml::Table,
    /// The venue's own credential paths.
    pub credentials: toml::Table,
}

impl std::fmt::Debug for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Source")
            .field("connection", &self.connection.as_str())
            .field("kind", &self.kind)
            .field("role", &self.role.as_str())
            .finish_non_exhaustive()
    }
}

impl Source {
    /// Whether a fatal error from this source ends the process.
    ///
    /// Only a primary's does. Everything else is dropped, named and left with
    /// its `connection_state` at 0.
    #[must_use]
    pub const fn is_primary(&self) -> bool {
        matches!(self.role, SourceRole::Primary)
    }
}
