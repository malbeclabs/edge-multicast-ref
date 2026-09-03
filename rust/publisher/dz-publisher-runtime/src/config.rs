//! The configuration document, composed here and parsed section by section by
//! whichever crate owns the section.
//!
//! # The rule, and the failure it comes from
//!
//! Six values appear in every existing publisher and most are spelled two or
//! three ways each. The rule that ends that is *each shared crate parses its own
//! section*: `[ingress]` is [`dz_ingress_core::IngressConfig`], and the keys,
//! types and defaults of a transport cannot drift between venues because there
//! is one implementation of them. What this module owns is the **document** —
//! the sections whose owner is the runtime, and the composition of the rest.
//!
//! `[egress]`, `[refdata.selection]` and `[[feed]] source_id` are the awkward
//! cases and are handled the same way: the owning crate holds the *checked*
//! type ([`EgressPolicy`], [`SelectionPolicy`], [`SourceId`]) and not a
//! deserializer, so this module deserializes the keys and hands the values to
//! that crate's constructor, which is where the invariant lives.
//!
//! # `deny_unknown_fields`, everywhere, and why it is the load-bearing attribute
//!
//! One publisher had a misspelled section parse cleanly, fall back to a
//! default, and run the wrong transport while the operator believed otherwise.
//! Every table in this document that has a known key set therefore refuses one
//! it does not know, including the document's own root — so a venue-specific key
//! written at the top level is a load error rather than a key nobody reads.
//!
//! Two tables deliberately have no known key set: [`AdapterConfig::upstream`]
//! and [`AdapterConfig::credentials`]. An adapter reading a local directory, one
//! holding two credentialed APIs and one reading a chain RPC plus a local socket
//! have nothing useful in common, and forcing a shape on them would move the
//! sprawl up a level. They are free *below* `[adapter.upstream]`, and the name
//! `upstream` itself is checked — which is exactly what makes
//! `[adapter.upstrem]` a refusal instead of an empty table.

use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

use dz_adapter_core::ConnectionId;
use dz_edge_core::{Feed as WireFeed, PortRole};
use dz_edge_mbp::MarketByPrice;
use dz_edge_tob::TopOfBook;
use dz_ingress_core::{IngressConfig, Kind, Policy};
use dz_publisher_egress::{EgressPolicy, Ipv4Prefix, DEFAULT_TTL};
use dz_publisher_lowering::SourceId;
use dz_publisher_refdata::SelectionPolicy;
use serde::Deserialize;

use crate::duration::{de_duration, de_optional_duration};
use crate::error::StartupError;

/// The whole document.
///
/// `deny_unknown_fields` on the root is what makes the design's fourth adapter
/// rule — *a top-level venue key is a load error* — a mechanism rather than a
/// request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    /// The label on every `dz_publisher_*` series this process emits, applied
    /// as a constant label by the metrics crate's own constructor. There is no
    /// path to a series without it.
    pub venue: String,

    /// Absent means the default policy: discover the source address from the
    /// route, assert no invariant on it, one hop. That is the policy of a host
    /// whose route is right, which is the normal case — see [`EgressPolicy`].
    #[serde(default)]
    pub egress: EgressSection,

    /// One per feed emitted. An array because a publisher may emit several,
    /// which one existing publisher expresses as repeated blocks and another as
    /// four differently-named sections.
    #[serde(default, rename = "feed")]
    pub feeds: Vec<FeedSection>,

    pub refdata: RefdataSection,

    #[serde(default)]
    pub metrics: MetricsSection,

    /// Owned by `dz-ingress-core`. This crate holds the document; that crate
    /// holds the shape of this section, so nothing there needs a parser and
    /// nothing here needs to know what a backoff is.
    pub ingress: IngressConfig,

    /// One per upstream connection this publisher opens.
    ///
    /// **Absent is one source, named by the transport the venue builds**, which
    /// is what every document said before this array existed and what a
    /// publisher with one upstream still says. See [`SourceSection`].
    #[serde(default, rename = "source")]
    pub sources: Vec<SourceSection>,

    pub adapter: AdapterConfig,
}

/// `[[source]]`: one upstream connection, and what this publisher does with it.
///
/// # Why a feed has more than one source
///
/// A venue often publishes the same book twice by different paths — a websocket
/// and a FIX session, a local socket and a remote stream, two validators of one
/// chain. They are not the same stream: conflation differs, per-connection
/// sequencing differs, and each arrives at its own moment. So which one a
/// publisher publishes from is a decision, and it is one an operator has to be
/// able to change without a rebuild.
///
/// Both shipped publishers already live this. One has two adapters for one
/// product line, over a websocket and over FIX, and picks between them by which
/// binary it runs. The other takes two validator streams and reconciles them
/// inside its own listener, with a reorder window and a grace fallback.
///
/// # What the runtime does, and what it deliberately does not
///
/// It opens every enabled source, drives each with its own connection, backoff
/// and rate limit, and hands every payload to **one** adapter, which tells them
/// apart by [`Payload::connection`](dz_adapter_core::Payload::connection).
///
/// It does not merge them. Merging two views of one book is the venue's, for the
/// same reason the book state machine is: which of two prices is current, and
/// when to fail over, follows the venue's microstructure and nothing here can
/// know it. The consequence is worth stating plainly — **the runtime cannot
/// enforce that a `comparison` source stays off the wire**, because the adapter
/// emits events and no event carries the source it came from. [`SourceRole`] is
/// therefore a declaration, a metric label and what an analysis tier reads; it
/// is not a gate.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSection {
    /// This connection's name, and the `connection` label on every
    /// `dz_publisher_ingress_*` series it moves.
    ///
    /// From configuration rather than from the venue's code, so that the file an
    /// operator reads and the label a dashboard groups by are the same string.
    /// It has to outlive the process to be a label — see [`Source::connection`].
    pub name: String,

    /// Which transport carries it, by [`Kind`]'s own token.
    ///
    /// Named here rather than at `[ingress]` when there are several sources, and
    /// naming it in both places is refused.
    pub ingress: String,

    /// `false` keeps the block and opens nothing.
    ///
    /// A disabled source is not opened, is not handed to the adapter and is
    /// **not declared to the metrics registry** — a connection-state series
    /// pre-created at 0 for a connection nobody meant to open is an alert that
    /// fires for a decision somebody took on purpose.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Which feeds this source's data reaches, by `[[feed]] spec`.
    ///
    /// The grouping that makes [`SourceRole`] checkable: *exactly one primary
    /// per feed* is a rule about alternatives for the same data, and this is
    /// what says which sources are alternatives. Empty means every enabled
    /// feed, which is the ordinary single-source case.
    #[serde(default)]
    pub carries: Vec<String>,

    /// What this publisher does with it. See [`SourceRole`].
    #[serde(default)]
    pub role: Option<String>,

    /// The venue's own endpoint keys for this source, deserialized by the
    /// venue's own code.
    #[serde(default)]
    pub upstream: toml::Table,

    /// Paths, never secrets — checked exactly as `[adapter.credentials]` is.
    #[serde(default)]
    pub credentials: toml::Table,
}

/// What a publisher does with one source.
///
/// A closed set of tokens, so a value outside it is a load error naming what
/// would have been accepted rather than a role nothing implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum SourceRole {
    /// The source this publisher publishes from. Exactly one per feed.
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
    /// [`StartupError::UnknownSourceRole`] naming the token and the set.
    pub fn resolve(token: &str) -> Result<Self, StartupError> {
        Self::ALL
            .iter()
            .copied()
            .find(|role| role.as_str() == token)
            .ok_or_else(|| StartupError::UnknownSourceRole {
                token: token.to_owned(),
                supported: Self::TOKEN_LIST,
            })
    }
}

/// `[egress]`: how the source address is chosen, and the TTL.
///
/// There is no `mtu` key, in this section or anywhere else. The 1,232-byte cap
/// is mandated and lives in `DatagramBuilder::new`, which is where a
/// configuration key cannot reach it — one publisher shipped 1448 to production
/// from a key exactly like the one that is missing here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressSection {
    /// An invariant on the discovered address, not a source of one.
    ///
    /// Checked at startup and never used as a value. A source address from the
    /// wrong interface produces datagrams that are well formed, densely
    /// numbered, and read by every subscriber as a *different channel instance*
    /// from the one they were told to expect.
    #[serde(default)]
    pub expected_prefix: Option<String>,

    /// An operator's override of route discovery, for a host where discovery is
    /// wrong. An escape hatch, never the normal path: one publisher read its
    /// source address from configuration, met a tunnel address that had moved,
    /// and crash-looped tens of thousands of times over two days.
    #[serde(default)]
    pub pin: Option<String>,

    #[serde(default = "default_ttl")]
    pub ttl: u8,
}

const fn default_ttl() -> u8 {
    DEFAULT_TTL
}

/// One `[[feed]]` block.
///
/// The four durations carry the defaults the design's own configuration block
/// states, transcribed rather than chosen. They are defaults and not
/// requirements because they are spec-timed values with one right answer, which
/// is the opposite of `[adapter] kind` — that one has no default because a
/// wrong guess is invisible, and these have one because a missing value would
/// otherwise leave a publisher with no heartbeat at all.
///
/// Two of the four are `Option` here even so, and it is not a change of that
/// rule: they still default, once, in [`Document::resolve`] rather than per
/// block. See [`definition_cycle`](Self::definition_cycle) for the failure that
/// distinction fixes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeedSection {
    /// The feed specification this block emits, by the codec crate's own
    /// `Feed::NAME`. Resolved against a closed set; see [`FeedSpec`].
    pub spec: String,

    #[serde(default = "default_true")]
    pub enabled: bool,

    /// The `Channel ID` shard. `channel` means this and nothing else.
    pub channel_id: u8,

    /// This publisher's registered identity. Checked against the source
    /// registry's reserved ranges at startup rather than per message; see
    /// [`SourceId`].
    pub source_id: u16,

    /// **One group.** The supplement specifies one multicast group with two
    /// destination ports and rejects a second group by name, so there is one
    /// key here and not one per port role.
    pub multicast_group: String,

    pub mktdata_port: u16,
    pub refdata_port: u16,

    /// Depth feeds only, and **required** for one.
    ///
    /// Both directions are refused rather than shrugged at. A depth feed
    /// without one publishes a book a subscriber that lost a datagram can never
    /// resynchronise, which is the failure the port exists for; a top-of-book
    /// feed with one names a port nothing will ever send on, and an operator
    /// who wrote it believes something is listening there. See
    /// [`StartupError::SnapshotPortRequired`] and
    /// [`StartupError::SnapshotPortNotCarried`].
    #[serde(default)]
    pub snapshot_port: Option<u16>,

    /// One full pass of the snapshot rotation. Depth feeds only, and optional.
    ///
    /// # Why the key exists, and why it is a cycle
    ///
    /// A recovery snapshot answers a reset the publisher announced; it does
    /// nothing for the subscriber that joins mid-session, and that subscriber
    /// cannot build a book without one. Both shipped publishers carry a periodic
    /// snapshot for that reason and both set it to five seconds — one of them
    /// under this exact name and this exact meaning, a full round-robin pass
    /// with one instrument per tick.
    ///
    /// A *cycle* and not an interval, for the reason `definition_cycle` is one:
    /// an interval per instrument has the whole published set falling due
    /// together, and a snapshot is several datagrams per instrument. See
    /// [`SnapshotRotation`](crate::rotation::SnapshotRotation).
    ///
    /// Absent means recovery snapshots and nothing else, which is what this
    /// runtime did before the key existed. It is refused on a feed with no
    /// snapshot port role rather than ignored: see
    /// [`StartupError::SnapshotCycleWithoutPort`].
    #[serde(default, deserialize_with = "de_optional_duration")]
    pub snapshot_cycle: Option<Duration>,

    #[serde(default = "default_heartbeat", deserialize_with = "de_duration")]
    pub heartbeat_interval: Duration,

    /// A **maximum on the interval between retransmissions of any single
    /// definition**, not a lap target. `dz-publisher-refdata` paces one lap
    /// across 80% of it, which is what stops the burst the reference-data
    /// specification forbids.
    ///
    /// # Why this one is an `Option` and the two beside it are not
    ///
    /// It is per-feed in the document and **single in the publisher**: it paces
    /// one reference-data registry, and there is one because `Instrument ID`
    /// identity can only be one thing. Two enabled blocks stating different
    /// values is therefore a document that cannot be obeyed and is refused —
    /// see [`StartupError::FeedsDisagree`] — and that refusal is only about the
    /// operator's own keys if a stated value is distinguishable from an absent
    /// one. Serde-defaulted, a document stating this on its depth feed and
    /// omitting it on its top-of-book feed was refused for a conflict between
    /// the value they typed and a default they never did.
    ///
    /// So absent is absent, and the default is applied once after the check.
    #[serde(default, deserialize_with = "de_optional_duration")]
    pub definition_cycle: Option<Duration>,

    #[serde(default = "default_manifest_cadence", deserialize_with = "de_duration")]
    pub manifest_cadence: Duration,

    /// Feed silence, which is not upstream silence. See
    /// [`IdleGuard`](crate::IdleGuard) for what this measures and, more to the
    /// point, what it refuses to measure — `[ingress] idle_timeout` is the
    /// other one, and the two are deliberately spelled differently so that
    /// neither can be read as the other.
    ///
    /// An `Option` for the reason
    /// [`definition_cycle`](Self::definition_cycle) is one: there is a single
    /// guard, because the silence it measures is the publisher's, so two stated
    /// values are refused — and an omission is not one of the two.
    #[serde(default, deserialize_with = "de_optional_duration")]
    pub idle_guard: Option<Duration>,
}

const fn default_true() -> bool {
    true
}

const fn default_heartbeat() -> Duration {
    Duration::from_secs(1)
}

const fn default_definition_cycle() -> Duration {
    Duration::from_secs(30)
}

const fn default_manifest_cadence() -> Duration {
    Duration::from_secs(1)
}

const fn default_idle_guard() -> Duration {
    Duration::from_secs(60)
}

/// `[refdata]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefdataSection {
    /// Durable state, not a cache. It holds the `Instrument ID` minting record
    /// and takes exactly one writer; clearing it restarts the feed's identity
    /// history.
    pub state_dir: PathBuf,
    pub selection: SelectionSection,
}

/// `[refdata.selection]`: the playbook's policy, stated rather than defaulted.
///
/// All three keys are required. A default cap would be a number this crate
/// chose for a venue's universe, and the failure it produces is a publisher
/// that starts, stays up, reports a valid manifest of nothing, and declines
/// every instrument the venue offers.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelectionSection {
    pub bootstrap_top_n: usize,
    pub max_published: usize,
    pub warn_published_above: usize,
}

/// `[metrics]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsSection {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// **Bind this to a non-public interface.** The exposition describes a live
    /// trading data path, including its instrument set and its timing. The
    /// default is loopback for that reason and not for convenience.
    #[serde(default = "default_listen_addr")]
    pub listen_addr: SocketAddr,
}

impl Default for MetricsSection {
    fn default() -> Self {
        Self {
            enabled: true,
            listen_addr: default_listen_addr(),
        }
    }
}

fn default_listen_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 9100))
}

/// `[adapter]`: the one section whose contents this crate cannot know.
///
/// `deny_unknown_fields` here and on every table under it that has a known key
/// set. That is task 7's own requirement and it is the audit's failure in
/// miniature: the four names below are the whole of what `[adapter]` may
/// contain, so a fifth is a refusal rather than a table nobody reads.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    /// Required, and resolved against the registry the venue's own `main`
    /// populated. There is no default and no fallback; see
    /// [`AdapterRegistry`](crate::AdapterRegistry).
    pub kind: String,

    /// Optional; off when absent. See [`TeeConfig`].
    #[serde(default)]
    pub tee: TeeConfig,

    /// Endpoints. **Keys defined by the adapter**, so this table is free.
    #[serde(default)]
    pub upstream: toml::Table,

    /// Optional; **paths only, never inline secrets**. Free in its keys and
    /// checked in the shape of its values: see
    /// [`StartupError::NotACredentialPath`].
    #[serde(default)]
    pub credentials: toml::Table,

    /// Uniform, because publishers already carry a live-versus-fixture switch
    /// under different spellings.
    #[serde(default)]
    pub replay: ReplayConfig,
}

/// `[adapter.tee]`: the reference stream, and why it sits here.
///
/// The tee is a second [`DatagramSink`](dz_publisher_egress::DatagramSink)
/// carrying byte-identical copies of every datagram to a local socket a
/// recorder archives, so that a subscriber-site archive can be diffed against a
/// reference archive datagram for datagram — network loss, reordering, MTU
/// drops and one-way latency measured rather than inferred.
///
/// **It sits under `[adapter]` rather than `[egress]` deliberately.** It is not
/// a transmitter: it darkens nothing when it fails, and it must never be able to
/// end a send. Putting it in `[egress]` would put it beside the keys that decide
/// what reaches subscribers, which is the section an operator reads as *this can
/// take the feed down*.
///
/// # One socket per feed *and* port role, and no framing at all
///
/// `path` is a **prefix**: the feed's own `spec` token and the role's are
/// appended, so a `path` of `/run/a-publisher/tee` on a publisher emitting both
/// feeds is written to as `tee.top-of-book.mktdata`, `tee.top-of-book.refdata`,
/// `tee.market-by-price.mktdata`, `tee.market-by-price.refdata` and
/// `tee.market-by-price.snapshot`.
///
/// Both halves of that name are load-bearing, for one reason: **a Unix datagram
/// carries neither a destination port nor a group**, and the diff this stream
/// exists for is keyed on both. A recorder handed two roles on one socket, or
/// two feeds' copies of one role on one socket, cannot attribute a datagram
/// without decoding it — and decoding is the one thing a record path does not
/// do. `[[feed]]` is an array, so a publisher emitting two feeds is the ordinary
/// case rather than the exception; a name keyed on the role alone is right only
/// for the publisher that happens to emit one feed. The shape mirrors the
/// recorder's own configuration, which keys its ports per feed.
///
/// The socket is `SOCK_DGRAM`, so one datagram in is one datagram out and there
/// is no framing to invent, agree on or get wrong. See
/// [`ReferenceStream`](dz_publisher_egress::ReferenceStream).
#[derive(Debug, Clone, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TeeConfig {
    /// `false` when the section is absent, and `false` is the default when it is
    /// present without this key.
    #[serde(default)]
    pub enabled: bool,

    /// The Unix socket the publisher fans encoded datagrams out to.
    #[serde(default)]
    pub path: Option<PathBuf>,
}

impl TeeConfig {
    /// The socket one feed's one port role is copied to.
    ///
    /// `<path>.<feed spec>.<port role>`, in the tokens the document itself
    /// states — the `spec` an operator wrote in the `[[feed]]` block and the
    /// role's own name — so the file, the socket and the recorder's
    /// configuration all spell the same two things the same way.
    ///
    /// # Errors
    ///
    /// [`StartupError::TeeWithoutPath`] when the section is on and names no
    /// path. Checked again here as well as at load, because a prefix is not
    /// something to default: a tee that quietly wrote to a relative path would
    /// have an operator believing copies were being archived.
    pub fn destination(
        &self,
        spec: FeedSpec,
        port_role: PortRole,
    ) -> Result<PathBuf, StartupError> {
        let prefix = self.path.as_deref().ok_or(StartupError::TeeWithoutPath)?;
        // Built on the `OsString` rather than with `join` or `set_extension`:
        // the suffix is appended to the last component, and `join` would make it
        // a child directory instead.
        let mut destination = prefix.as_os_str().to_owned();
        destination.push(".");
        destination.push(spec.as_str());
        destination.push(".");
        destination.push(port_role.as_str());
        Ok(PathBuf::from(destination))
    }
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
/// every match over this type — including [`crate::run()`]'s, which is where the
/// composing happens.
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
    /// string; held to [`ALL`](Self::ALL) by
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
    /// [`StartupError::UnsupportedFeedSpec`], naming what this build can emit.
    /// There is no default: a feed is not a thing to guess at, and the audit's
    /// misspelled section became the wrong transport precisely because
    /// something defaulted.
    pub fn resolve(token: &str) -> Result<Self, StartupError> {
        Self::ALL
            .into_iter()
            .find(|spec| spec.as_str() == token)
            .ok_or_else(|| StartupError::UnsupportedFeedSpec {
                spec: token.to_owned(),
                supported: Self::SUPPORTED.to_owned(),
            })
    }
}

/// A wire feed this crate can compose a send path for, as a type-level fact.
///
/// # Why this exists rather than a field
///
/// [`ChannelEgress`](dz_publisher_egress::ChannelEgress) is generic over the
/// feed, because `Magic` belongs to the feed and is what rejects a datagram
/// misrouted from a sibling. So a send path is
/// `FeedPipeline<TopOfBook>` or `FeedPipeline<MarketByPrice>` and the feed is
/// known at compile time — but the *routing* has to know which specification it
/// is holding, because the codec will not stop a `Quote` being pushed into a
/// market-by-price datagram: `DatagramBuilder::push` checks `PORT_ROLES` and
/// nothing checks feed membership.
///
/// Carrying the specification as an associated constant rather than a runtime
/// field is what makes the two unable to disagree. A `FeedPipeline` built over
/// `MarketByPrice` cannot be told it is a top-of-book feed, because there is
/// nothing to tell.
pub trait EmittedFeed: WireFeed {
    /// The `[[feed]] spec` this wire feed answers to.
    const SPEC: FeedSpec;
}

impl EmittedFeed for TopOfBook {
    const SPEC: FeedSpec = FeedSpec::TopOfBook;
}

impl EmittedFeed for MarketByPrice {
    const SPEC: FeedSpec = FeedSpec::MarketByPrice;
}

/// One feed's configuration, checked.
#[derive(Debug, Clone)]
pub struct Feed {
    pub spec: FeedSpec,
    pub channel_id: u8,
    pub source_id: SourceId,
    pub group: Ipv4Addr,
    pub mktdata_port: u16,
    pub refdata_port: u16,
    pub snapshot_port: Option<u16>,
    /// One full pass of the snapshot rotation; `None` for recovery snapshots
    /// only. See [`FeedSection::snapshot_cycle`].
    pub snapshot_cycle: Option<Duration>,
    pub heartbeat_interval: Duration,
    /// The publisher-wide value, which every feed carries identically: it paces
    /// one reference-data registry. Either the one an enabled `[[feed]]` block
    /// stated, or the default — see [`one_stated`].
    pub definition_cycle: Duration,
    pub manifest_cadence: Duration,
    /// The publisher-wide value, as [`definition_cycle`](Self::definition_cycle)
    /// is: there is one guard, and the silence it measures is the publisher's.
    pub idle_guard: Duration,
}

/// `[refdata]`, checked.
#[derive(Debug, Clone)]
pub struct Refdata {
    pub state_dir: PathBuf,
    pub selection: SelectionPolicy,
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
    /// What this publisher does with it.
    pub role: SourceRole,
    /// The feeds this source's data reaches; empty means every enabled feed.
    pub carries: Vec<FeedSpec>,
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
            .field("carries", &self.carries)
            .finish_non_exhaustive()
    }
}

impl Source {
    /// Whether this source's data reaches `spec`.
    #[must_use]
    pub fn carries(&self, spec: FeedSpec) -> bool {
        self.carries.is_empty() || self.carries.contains(&spec)
    }
}

/// The whole document, checked, with every section handed to its owner's
/// constructor.
///
/// Separate from [`Document`] for the reason [`dz_ingress_core::Policy`] is
/// separate from [`IngressConfig`]: what runs takes what has been checked, not
/// what was written, so there is no case in the running publisher for a
/// `Source ID` of zero or a backoff pair the wrong way round.
#[derive(Debug)]
pub struct Config {
    pub venue: String,
    pub egress: EgressPolicy,
    pub feeds: Vec<Feed>,
    pub refdata: Refdata,
    pub metrics: MetricsSection,
    /// The document-level `[ingress] kind`, for a publisher with one source.
    ///
    /// `None` when the document names its transports per `[[source]]` instead,
    /// which is the case [`Config::sources`] is non-empty for. The two are
    /// mutually exclusive by construction: naming a transport in both places is
    /// refused at load.
    pub ingress_kind: Option<Kind>,
    pub ingress: Policy,
    /// Every enabled `[[source]]`, resolved. Empty means one source, named by
    /// the transport the venue builds — see [`SourceSection`].
    pub sources: Vec<Source>,
    pub adapter: AdapterConfig,
}

impl Document {
    /// Parse a document from TOML text.
    ///
    /// Text and not a path, so that every property of the document is testable
    /// without a filesystem. [`Config::load`] is the one function here that
    /// reads a file.
    ///
    /// # Errors
    ///
    /// [`StartupError::Document`] for anything that does not parse, **including
    /// a key nobody reads**.
    pub fn parse(text: &str) -> Result<Self, StartupError> {
        Ok(toml::from_str(text)?)
    }

    /// Check every section, through the constructor of whichever crate owns it.
    ///
    /// # Errors
    ///
    /// Every [`StartupError`] that is not about reading a file or opening a
    /// socket.
    pub fn resolve(self) -> Result<Config, StartupError> {
        let egress = self.egress.resolve()?;

        // The feeds first and the adapter last, because the order decides which
        // failure an operator sees and a wrong `Channel ID` is worth hearing
        // about before a misspelled adapter: the second is a typo in one line
        // and the first is a conversation with subscribers.
        // **Two keys are per-feed in the document and not per-feed in the
        // publisher, so a document that states two answers is refused rather
        // than silently given the first feed's.**
        //
        // `definition_cycle` paces one registry, and one registry is deliberate:
        // `Instrument ID` identity is the one thing there can only be one of,
        // so every feed publishes the same set from the same table and a second
        // cadence over it would emit the same definition at two rates. See
        // `Publisher::new`.
        //
        // `idle_guard` is one guard because the silence it measures is the
        // publisher's — upstream delivering and nothing reaching any wire. The
        // shipped publisher that once had one guard per feed now has exactly one
        // venue-wide guard, with a fallback to its first feed's key; the
        // fallback is the part that is a trap, and this is where it is refused
        // instead.
        //
        // **Only two stated values are a disagreement**, which is why both keys
        // are `Option` in the section and defaulted once here: a document
        // stating `idle_guard` on its depth feed and omitting it on its
        // top-of-book feed states one answer, and refusing it for a conflict
        // with a default the operator never typed is a refusal to start over a
        // key the file does not contain.
        let enabled: Vec<FeedSection> =
            self.feeds.into_iter().filter(|feed| feed.enabled).collect();
        let definition_cycle = one_stated(
            "[[feed]] definition_cycle",
            enabled.iter().map(|feed| feed.definition_cycle),
            default_definition_cycle(),
        )?;
        let idle_guard = one_stated(
            "[[feed]] idle_guard",
            enabled.iter().map(|feed| feed.idle_guard),
            default_idle_guard(),
        )?;

        let mut feeds = Vec::new();
        let mut seen: BTreeMap<&'static str, ()> = BTreeMap::new();
        for section in enabled {
            let feed = section.resolve(definition_cycle, idle_guard)?;
            if seen.insert(feed.spec.as_str(), ()).is_some() {
                return Err(StartupError::DuplicateFeedSpec {
                    spec: feed.spec.as_str().to_owned(),
                });
            }
            feeds.push(feed);
        }
        if feeds.is_empty() {
            return Err(StartupError::NoEnabledFeed);
        }
        // One `Source ID` per process, because that is what a `Source ID` is:
        // the lowering takes it once and every message a process sends carries
        // it, so there is no per-message decision and no per-feed one either.
        // Two feeds naming different ids is a configuration that cannot be
        // obeyed, and picking one of them would put an identity on one feed's
        // wire that its own block did not ask for.
        let first = feeds[0].source_id;
        if let Some(other) = feeds.iter().find(|feed| feed.source_id != first) {
            return Err(StartupError::SeveralSourceIds {
                one: first.get(),
                another: other.source_id.get(),
            });
        }

        let selection = SelectionPolicy::new(
            self.refdata.selection.bootstrap_top_n,
            self.refdata.selection.max_published,
            self.refdata.selection.warn_published_above,
        )?;

        // **The transport is named once**, either at `[ingress]` for a publisher
        // with one source or once per `[[source]]`, and never in both places. A
        // key that is read only when another is absent is a key an operator
        // cannot reason about from the file in front of them.
        let (ingress_kind, ingress) = if self.sources.is_empty() {
            let (kind, policy) = self.ingress.resolve()?;
            (Some(kind), policy)
        } else {
            if let Some(document) = self.ingress.kind.clone() {
                return Err(StartupError::Ingress {
                    source: dz_ingress_core::ConfigError::KindNamedTwice {
                        document,
                        sources: self.sources.len(),
                    },
                });
            }
            (None, self.ingress.policy()?)
        };

        let sources = resolve_sources(self.sources, &feeds)?;

        check_credentials(&self.adapter.credentials)?;
        for source in &sources {
            check_credentials(&source.credentials)?;
        }

        // Checked at load rather than where the socket is opened: a section
        // switched on and left incomplete is an operator who believes copies
        // are being archived, and there is no reason to open a multicast socket
        // before saying so.
        if self.adapter.tee.enabled && self.adapter.tee.path.is_none() {
            return Err(StartupError::TeeWithoutPath);
        }

        Ok(Config {
            venue: self.venue,
            egress,
            feeds,
            refdata: Refdata {
                state_dir: self.refdata.state_dir,
                selection,
            },
            metrics: self.metrics,
            ingress_kind,
            ingress,
            sources,
            adapter: self.adapter,
        })
    }
}

impl Config {
    /// Read and check a document from a file.
    ///
    /// # Errors
    ///
    /// [`StartupError::Read`], then everything [`Document::parse`] and
    /// [`Document::resolve`] can return.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self, StartupError> {
        let path = path.into();
        let text = std::fs::read_to_string(&path).map_err(|source| StartupError::Read {
            path: path.clone(),
            source,
        })?;
        Document::parse(&text)?.resolve()
    }

    /// Every `Channel ID` this publisher sends on, so the sequence, heartbeat
    /// and manifest gauges exist from startup rather than appearing once
    /// something has already gone wrong.
    #[must_use]
    pub fn channel_ids(&self) -> Vec<u8> {
        let mut ids: Vec<u8> = self.feeds.iter().map(|feed| feed.channel_id).collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Every enabled feed's specification, in the document's own order.
    ///
    /// Handed to a venue's constructor through
    /// [`AdapterContext::feeds`](crate::AdapterContext::feeds), which is where
    /// the reason it exists is written down.
    #[must_use]
    pub fn feed_specs(&self) -> Vec<FeedSpec> {
        self.feeds.iter().map(|feed| feed.spec).collect()
    }

    /// Exactly the port roles this publisher operates, across every enabled
    /// feed.
    #[must_use]
    pub fn port_roles(&self) -> Vec<PortRole> {
        let mut roles = Vec::new();
        for feed in &self.feeds {
            for role in feed.spec.port_roles() {
                if !roles.contains(role) {
                    roles.push(*role);
                }
            }
        }
        roles
    }
}

impl EgressSection {
    fn resolve(&self) -> Result<EgressPolicy, StartupError> {
        let expected_prefix = match &self.expected_prefix {
            None => None,
            Some(text) => {
                Some(Ipv4Prefix::parse(text).map_err(|source| StartupError::BadPrefix { source })?)
            }
        };
        let pin = match &self.pin {
            None => None,
            Some(text) => Some(text.parse().map_err(|_| StartupError::NotAnAddress {
                key: "[egress] pin",
                value: text.clone(),
            })?),
        };
        Ok(EgressPolicy {
            pin,
            expected_prefix,
            ttl: self.ttl,
        })
    }
}

/// The one value an enabled `[[feed]]` block set states for a key the publisher
/// holds once, or the default if none of them states one.
///
/// # Absent is absent, and that is the whole point of the function
///
/// Both callers' keys used to be serde-defaulted, so every block carried a
/// value whether or not it stated one — and the disagreement check then read a
/// document that set `idle_guard = "300s"` on its depth feed and omitted it on
/// its top-of-book feed as a conflict between 300s and a 60s default the
/// operator never typed. A publisher that started yesterday would refuse to
/// start today, naming two values, one of which is not in the file.
///
/// So the sections carry `Option`, only two stated values are a disagreement,
/// and the default is applied once — here, after the check, so that a single
/// stated value governs every feed rather than only the block it appears in.
///
/// The zero check is here too, for the same reason it is a refusal at all: zero
/// is what an unset key reads as in a document that spells its durations as
/// bare numbers, and a cadence of zero is not a slower cadence.
///
/// # Errors
///
/// [`StartupError::ZeroDuration`] for a stated zero, and
/// [`StartupError::FeedsDisagree`] naming both values when two blocks state
/// different ones.
fn one_stated(
    key: &'static str,
    stated: impl Iterator<Item = Option<Duration>>,
    default: Duration,
) -> Result<Duration, StartupError> {
    let mut settled: Option<Duration> = None;
    for value in stated.flatten() {
        if value.is_zero() {
            return Err(StartupError::ZeroDuration { key });
        }
        match settled {
            None => settled = Some(value),
            // Named in the document's own order, so the two values in the
            // message are the first and the one that disagreed with it.
            Some(one) if one != value => {
                return Err(StartupError::FeedsDisagree {
                    key,
                    one,
                    another: value,
                })
            }
            Some(_) => {}
        }
    }
    Ok(settled.unwrap_or(default))
}

impl FeedSection {
    /// Check one block, with the two publisher-wide cadences already settled.
    ///
    /// They are arguments rather than fields of the block because they are not
    /// per-feed values: see [`one_stated`]. Every resolved [`Feed`] carries the
    /// same pair by construction.
    fn resolve(
        self,
        definition_cycle: Duration,
        idle_guard: Duration,
    ) -> Result<Feed, StartupError> {
        let spec = FeedSpec::resolve(&self.spec)?;
        let source_id = SourceId::new(self.source_id).ok_or(StartupError::BadSourceId {
            source_id: self.source_id,
        })?;
        let group: Ipv4Addr =
            self.multicast_group
                .parse()
                .map_err(|_| StartupError::NotAnAddress {
                    key: "[[feed]] multicast_group",
                    value: self.multicast_group.clone(),
                })?;
        if !group.is_multicast() {
            return Err(StartupError::NotMulticast { group });
        }

        for (key, port) in [
            ("mktdata_port", self.mktdata_port),
            ("refdata_port", self.refdata_port),
        ] {
            if port == 0 {
                return Err(StartupError::ZeroPort { key });
            }
        }
        if self.snapshot_port == Some(0) {
            return Err(StartupError::ZeroPort {
                key: "snapshot_port",
            });
        }
        match (spec.has_snapshot_port(), self.snapshot_port) {
            (true, None) => {
                return Err(StartupError::SnapshotPortRequired {
                    spec: spec.as_str(),
                })
            }
            (false, Some(port)) => {
                return Err(StartupError::SnapshotPortNotCarried {
                    spec: spec.as_str(),
                    port,
                })
            }
            _ => {}
        }
        if self.mktdata_port == self.refdata_port {
            return Err(StartupError::PortsCollide {
                left: "mktdata_port",
                right: "refdata_port",
                port: self.mktdata_port,
            });
        }
        if let Some(snapshot) = self.snapshot_port {
            for (key, port) in [
                ("mktdata_port", self.mktdata_port),
                ("refdata_port", self.refdata_port),
            ] {
                if snapshot == port {
                    return Err(StartupError::PortsCollide {
                        left: key,
                        right: "snapshot_port",
                        port,
                    });
                }
            }
        }

        // A cadence for a port role this feed does not carry is a key nobody
        // reads, which is the failure the whole document is checked against.
        if self.snapshot_cycle.is_some() && self.snapshot_port.is_none() {
            return Err(StartupError::SnapshotCycleWithoutPort {
                spec: spec.as_str(),
            });
        }

        // `definition_cycle` and `idle_guard` are not here: they are checked
        // once, across every enabled block, by `one_stated`.
        for (key, value) in [
            ("[[feed]] heartbeat_interval", self.heartbeat_interval),
            ("[[feed]] manifest_cadence", self.manifest_cadence),
        ]
        .into_iter()
        .chain(
            self.snapshot_cycle
                .map(|cycle| ("[[feed]] snapshot_cycle", cycle)),
        ) {
            if value.is_zero() {
                return Err(StartupError::ZeroDuration { key });
            }
        }

        Ok(Feed {
            spec,
            channel_id: self.channel_id,
            source_id,
            group,
            mktdata_port: self.mktdata_port,
            refdata_port: self.refdata_port,
            snapshot_port: self.snapshot_port,
            snapshot_cycle: self.snapshot_cycle,
            heartbeat_interval: self.heartbeat_interval,
            definition_cycle,
            manifest_cadence: self.manifest_cadence,
            idle_guard,
        })
    }
}

/// Resolve `[[source]]`, and refuse every document that names alternatives
/// without saying which one publishes.
///
/// # The one rule that has to be a startup error
///
/// **Exactly one enabled `primary` per enabled feed.** Two primaries carrying
/// one feed are two publishers' worth of events on one channel instance: the
/// `Sequence Number` series is per channel instance, so a subscriber's gap
/// detection reads the two interleaved as its own losses and cannot tell which.
/// None is a feed whose block is enabled and whose data has no path to the wire,
/// which is a publisher heartbeating a channel it never fills.
///
/// A `comparison` source is refused nothing else: several are fine, and one
/// arriving on a feed that has a primary is the whole point of the role.
fn resolve_sources(
    sections: Vec<SourceSection>,
    feeds: &[Feed],
) -> Result<Vec<Source>, StartupError> {
    let mut sources: Vec<Source> = Vec::new();
    let mut seen: BTreeMap<String, ()> = BTreeMap::new();

    for section in sections {
        if section.name.trim().is_empty() {
            return Err(StartupError::UnnamedSource);
        }
        // Checked across every block rather than only the enabled ones: two
        // blocks sharing a name are two descriptions of one connection, and
        // which of them is in force would depend on which was enabled today.
        if seen.insert(section.name.clone(), ()).is_some() {
            return Err(StartupError::DuplicateSourceName { name: section.name });
        }
        if !section.enabled {
            continue;
        }

        let kind =
            Kind::resolve(&section.ingress).map_err(|source| StartupError::Ingress { source })?;
        let role = match section.role.as_deref() {
            Some(token) => SourceRole::resolve(token)?,
            None => SourceRole::default(),
        };
        let mut carries = Vec::new();
        for spec in &section.carries {
            let spec = FeedSpec::resolve(spec)?;
            // A source carrying a feed this publisher does not emit is a key
            // nobody reads, and an operator who wrote it believes that feed is
            // being served from it.
            if !feeds.iter().any(|feed| feed.spec == spec) {
                return Err(StartupError::SourceCarriesUnknownFeed {
                    name: section.name.clone(),
                    spec: spec.as_str(),
                });
            }
            carries.push(spec);
        }

        sources.push(Source {
            // Leaked once, here, before the metric registry exists. See
            // `Source::connection` for why a label cannot be anything else.
            connection: ConnectionId::new(Box::leak(section.name.into_boxed_str())),
            kind,
            role,
            carries,
            upstream: section.upstream,
            credentials: section.credentials,
        });
    }

    if !seen.is_empty() && sources.is_empty() {
        return Err(StartupError::NoEnabledSource);
    }
    // Only when the array is in use: a document with no `[[source]]` block has
    // one implicit source and nothing to disambiguate.
    if seen.is_empty() {
        return Ok(sources);
    }

    for feed in feeds {
        let primaries: Vec<&str> = sources
            .iter()
            .filter(|source| source.role == SourceRole::Primary && source.carries(feed.spec))
            .map(|source| source.connection.as_str())
            .collect();
        if primaries.len() != 1 {
            return Err(StartupError::FeedPrimaries {
                spec: feed.spec.as_str(),
                primaries: if primaries.is_empty() {
                    "none".to_owned()
                } else {
                    primaries.join(", ")
                },
            });
        }
    }

    Ok(sources)
}

/// The checkable half of *paths only, never inline secrets*.
///
/// Whether a string is a secret is not decidable, so this checks the two shapes
/// that are: a value that is not a string at all, and a string carrying a line
/// break — which is a private key or a certificate somebody pasted in, and the
/// one case worth failing a startup over.
fn check_credentials(credentials: &toml::Table) -> Result<(), StartupError> {
    for (key, value) in credentials {
        let what = match value {
            toml::Value::String(text) if text.contains('\n') || text.contains('\r') => {
                "several lines of text"
            }
            toml::Value::String(_) => continue,
            _ => "not a string",
        };
        return Err(StartupError::NotACredentialPath {
            key: key.clone(),
            what,
        });
    }
    Ok(())
}
