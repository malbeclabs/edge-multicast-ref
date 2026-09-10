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
use dz_venue_composition::AdapterSection;
use serde::Deserialize;

use crate::duration::{de_duration, de_optional_duration};
use crate::error::StartupError;

/// The four types a venue's constructor is handed by value, which left with the
/// context that hands them out.
///
/// They are re-exported at the paths they had, so `crate::config::FeedSpec` is
/// still `crate::config::FeedSpec` for every module here and for every venue.
/// What stayed is everything the context does not expose: the document, the
/// sections that parse it, and the startup error that names them.
///
/// The two `resolve` functions among them now refuse with a small error of that
/// crate's own, because [`StartupError`] names the egress and the
/// reference-data registry in other variants and cannot go there. Each is
/// mapped back into the variant it always produced, at the one call site each
/// has, so both the message and the variant's fields are unchanged.
pub use dz_venue_composition::{FeedSpec, ReplayConfig, Source, SourceRole};

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
    ///
    /// **Defaultable, because every key in it has a default and `kind` is
    /// optional.** A publisher that names its transport once per `[[source]]`
    /// has nothing to state here, and required this failed at parse with
    /// `missing field `ingress`` at line 1, column 1 — an error pointing at the
    /// whole file rather than at the section nobody wrote. A document that
    /// names a transport in *neither* place still reaches
    /// [`ConfigError::NoKind`], which names both ways of stating it, so the
    /// default cannot make a publisher with no transport start.
    #[serde(default)]
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
/// know it.
///
/// # What `role` is, and the one thing it decides
///
/// **The runtime cannot enforce that a `comparison` source stays off the wire.**
/// The adapter emits events and no event carries the source it came from, so
/// there is no seam at which one source's data could be held back from a feed.
/// So `role` is a declaration and a metric label, and it is *not* a gate on
/// what reaches the wire. Nothing here pretends otherwise, and the one rule that
/// depends on it — exactly one enabled `primary` — is publisher-wide for exactly
/// that reason: a per-feed rule would describe routing the runtime does not do.
///
/// What it does decide, and the reason it is not decoration:
/// **only a `primary`'s fatal error ends the process.** `Driver::run` returns
/// only on [`IngressError::Fatal`](dz_ingress_core::IngressError::Fatal), whose
/// documented causes are the per-source configuration faults found at connect —
/// an invalid endpoint, a missing credential path, an unsupported scheme. A
/// mistyped URL on a source that by design must not reach the wire took the
/// healthy primary down and kept it down across restarts. Now that source's
/// driver is dropped and named, its `connection_state` stays at 0 — which is
/// the alert for exactly this case — and the primary carries on.
///
/// # There is no `carries`
///
/// There was, and it declared which feeds a source's data reached. It could not
/// be honoured: every payload reaches one adapter and every event it emits
/// reaches every feed the event belongs to, so a source cannot be confined to a
/// subset of feeds by anything in this crate. A key that reads as a partition
/// while nothing partitions is worse than no key — it made two primaries with
/// disjoint declarations resolve cleanly while both upstreams' events landed on
/// one channel instance under one `Sequence Number` series.
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

    /// Which shard of the instrument set this block carries, in the venue's own
    /// word. Absent resolves to the default shard, which is what a publisher
    /// with one channel per specification has always been.
    ///
    /// Naming the default explicitly is refused rather than accepted: two
    /// spellings of one shard would be two era files. See
    /// [`StartupError::ReservedShardName`].
    #[serde(default)]
    pub shard: Option<String>,

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

/// The four values a venue's constructor reads out of this section.
///
/// The section is a publisher's — the fifth field is a second send path, and
/// its one method returns a [`StartupError`] — so the type stays here and the
/// four names travel. What that buys is the claim
/// [`AdapterContext`](crate::AdapterContext) makes: the document a section came
/// out of is not one of the four, so a process with a configuration of its own
/// can build the same context for the same venue's constructor.
impl AdapterSection for AdapterConfig {
    fn kind(&self) -> &str {
        &self.kind
    }

    fn upstream(&self) -> &toml::Table {
        &self.upstream
    }

    fn credentials(&self) -> &toml::Table {
        &self.credentials
    }

    fn replay(&self) -> &ReplayConfig {
        &self.replay
    }
}

/// `[adapter.tee]`: the reference copy, and why it sits here.
///
/// The section names a second
/// [`DatagramSink`](dz_publisher_egress::DatagramSink) carrying byte-identical
/// copies of every datagram to a local socket a recorder archives, so that a
/// subscriber-site archive can be diffed against a reference archive datagram
/// for datagram — network loss, reordering, MTU drops and one-way latency
/// measured rather than inferred.
///
/// **It sits under `[adapter]` rather than `[egress]` deliberately.** It is not
/// a transmitter: it darkens nothing when it fails, and it must never be able to
/// end a send. Putting it in `[egress]` would put it beside the keys that decide
/// what reaches subscribers, which is the section an operator reads as *this can
/// take the feed down*.
///
/// # One socket per feed, *shard* and port role, and no framing at all
///
/// `path` is a **prefix**: the feed's own `spec` token, the shard's name where
/// it is not the default, and the role's are appended, so a `path` of
/// `/run/a-publisher/fan-out` on a publisher emitting both feeds of the default
/// shard is written to as `fan-out.top-of-book.mktdata`,
/// `fan-out.top-of-book.refdata`, `fan-out.market-by-price.mktdata`,
/// `fan-out.market-by-price.refdata` and `fan-out.market-by-price.snapshot`,
/// and the same publisher carrying a shard the document named `alpha` writes
/// that shard's copies to `fan-out.top-of-book.alpha.mktdata` and the rest of
/// the five alongside.
///
/// All three parts of that name are load-bearing, for one reason: **a Unix
/// datagram carries neither a destination port nor a group**, and the diff this
/// fan-out exists for is keyed on both. A recorder handed two roles on one
/// socket, two feeds' copies of one role on one socket, or **two shards' copies
/// of one feed's role on one socket**, cannot attribute a datagram without
/// decoding it — and decoding is the one thing a record path does not do.
/// `[[feed]]` is an array, so a publisher emitting two feeds is the ordinary
/// case rather than the exception, and two channel instances of one
/// specification are the ordinary case now too; a name keyed on the role alone
/// is right only for the publisher that happens to emit one feed, and one keyed
/// on the feed and the role alone only for the one that carries a single shard.
/// The shape mirrors the recorder's own configuration, which keys its ports per
/// feed.
///
/// The default shard is spelled by its **absence**, as its era file is. A shard
/// segment for it would rename the socket every existing deployment's recorder
/// is bound to, and the fan-out would then write to a path with nobody on it —
/// every datagram dropped and counted, or worse, an operator who believes
/// copies are still being archived.
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
    /// The socket one channel instance's one port role is copied to.
    ///
    /// `<path>.<feed spec>.<shard>.<port role>`, and
    /// `<path>.<feed spec>.<port role>` for the default shard, in the tokens the
    /// document itself states — the `spec` and the `shard` an operator wrote in
    /// the `[[feed]]` block and the role's own name — so the file, the socket
    /// and the recorder's configuration all spell the same things the same way.
    ///
    /// The default shard's absence from the name is [`ShardName::era_shard`]'s
    /// reasoning applied to a socket, and it is here rather than at the call
    /// site for the same reason: what a caller would naturally write is
    /// `shard.as_str()`, which moves every existing deployment's fan-out to a
    /// path its recorder is not bound to.
    ///
    /// # Errors
    ///
    /// [`StartupError::TeeWithoutPath`] when the section is on and names no
    /// path. Checked again here as well as at load, because a prefix is not
    /// something to default: a fan-out that quietly wrote to a relative path
    /// would have an operator believing copies were being archived.
    pub fn destination(
        &self,
        spec: FeedSpec,
        shard: &ShardName,
        port_role: PortRole,
    ) -> Result<PathBuf, StartupError> {
        let prefix = self.path.as_deref().ok_or(StartupError::TeeWithoutPath)?;
        // Built on the `OsString` rather than with `join` or `set_extension`:
        // the suffix is appended to the last component, and `join` would make it
        // a child directory instead.
        let mut destination = prefix.as_os_str().to_owned();
        destination.push(".");
        destination.push(spec.as_str());
        if !shard.is_default() {
            destination.push(".");
            destination.push(shard.as_str());
        }
        destination.push(".");
        destination.push(port_role.as_str());
        Ok(PathBuf::from(destination))
    }
}

/// A wire feed this crate can compose a send path for, as a type-level fact.
///
/// # Why this exists rather than a field
///
/// [`ChannelEgress`](dz_publisher_egress::ChannelEgress) is generic over the
/// feed, because `Magic` belongs to the feed and is what rejects a datagram
/// misrouted from another feed in the family. So a send path is
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

/// Which shard of the instrument set a block carries, as a venue names it.
///
/// A channel *is* "a logical shard of the instrument set, named by `Channel
/// ID`", and this is the venue's word for that shard while `channel_id` is the
/// configuration's number for it. The mapping between them is the document's,
/// which is what keeps a venue unable to name a `Channel ID` — the constraint
/// the whole adapter boundary is built on.
///
/// # Checked here because it becomes a path component later
///
/// The era store already refuses a feed name that is not one lowercase path
/// component, and for the same reason: a name with a slash in it writes
/// somewhere nobody configured. A shard name reaches a path in two places, so
/// it is checked once, at load, in the one place the value enters the process —
/// rather than at each use, where the third use is the one that forgets.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardName(String);

impl ShardName {
    /// The longest name a shard may have, in bytes.
    ///
    /// The era store's own bound. A longer one is refused rather than truncated:
    /// two shards whose names differ past the cut would share an era file.
    const MAX: usize = 64;

    /// A shard name, or `None` for one that cannot be a path component.
    ///
    /// `None` is a startup error for the caller to report against its own
    /// configuration key — the same shape as `SourceId::new`, and for the same
    /// reason: a publisher with a name it cannot write must not start, and must
    /// not discover it later when the first era file is written.
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        let safe = !value.is_empty()
            && value.len() <= Self::MAX
            && value
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        safe.then(|| Self(value.to_owned()))
    }

    /// The default shard, for a document that names none.
    ///
    /// The token is `dz-adapter-core`'s so that the boundary and the
    /// configuration cannot spell it differently — two spellings of one shard
    /// are two era files and two published sets, for one channel.
    #[must_use]
    pub fn default_shard() -> Self {
        Self(dz_adapter_core::DEFAULT_SHARD.to_owned())
    }

    /// Whether this is the default, which a block may not spell explicitly.
    #[must_use]
    pub fn is_default(&self) -> bool {
        self.0 == dz_adapter_core::DEFAULT_SHARD
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// This shard as the era store keys its files on.
    ///
    /// **The one place the mapping is made, and it exists because the obvious
    /// version is wrong.** A document that names no shard resolves to the
    /// default token, so a caller reaching for `Shard::named(shard.as_str())` —
    /// the natural thing to write — renames every existing deployment's era
    /// file. A renamed file reads as *no file*, which resolves to the first
    /// era: a publisher on era 7 restarts on era 1 and announces nothing.
    ///
    /// The default shard therefore keeps `<spec>.era`, and it is this method's
    /// job to know that rather than each call site's.
    #[must_use]
    pub fn era_shard(&self) -> dz_publisher_egress::Shard<'_> {
        // Delegated, not reimplemented. `Shard::resolve` is the mapping and it
        // takes the token as an argument because `dz-publisher-egress` does not
        // depend on the boundary crate that owns the constant — so the decision
        // lives once, in the crate that owns `Shard`, and this method's job is
        // to supply the token from the one place it is spelled.
        dz_publisher_egress::Shard::resolve(&self.0, dz_adapter_core::DEFAULT_SHARD)
    }
}

impl std::fmt::Display for ShardName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One feed's configuration, checked.
#[derive(Debug, Clone)]
pub struct Feed {
    pub spec: FeedSpec,
    /// The shard this block carries. A document naming none resolves every
    /// block to [`ShardName::default_shard`].
    pub shard: ShardName,
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

        let mut feeds: Vec<Feed> = Vec::new();
        // Keyed on the pair. Two blocks of one specification are ordinary now —
        // on different shards — and it is the shard that carries the meaning.
        let mut seen: BTreeMap<(&'static str, String), ()> = BTreeMap::new();
        for section in enabled {
            let feed = section.resolve(definition_cycle, idle_guard)?;
            if seen
                .insert((feed.spec.as_str(), feed.shard.as_str().to_owned()), ())
                .is_some()
            {
                return Err(StartupError::DuplicateFeedShard {
                    spec: feed.spec.as_str().to_owned(),
                    shard: feed.shard.as_str().to_owned(),
                });
            }
            // Checked here rather than in `channel_ids`, which sorts and dedups
            // and would therefore make a collision disappear on its way to the
            // metrics that would have shown it.
            if let Some(first) = feeds
                .iter()
                .find(|earlier| earlier.channel_id == feed.channel_id)
            {
                return Err(StartupError::DuplicateChannelId {
                    channel_id: feed.channel_id,
                    first_spec: first.spec.as_str().to_owned(),
                    first_shard: first.shard.as_str().to_owned(),
                    second_spec: feed.spec.as_str().to_owned(),
                    second_shard: feed.shard.as_str().to_owned(),
                });
            }
            feeds.push(feed);
        }
        if feeds.is_empty() {
            return Err(StartupError::NoEnabledFeed);
        }
        check_shards_carry_the_same_specifications(&feeds)?;
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

        let sources = resolve_sources(self.sources)?;

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
        // Distinct, because the question it answers is *which feeds does this
        // publisher emit* and an adapter handed one specification once per
        // shard is being told something about the deployment rather than about
        // the feeds. Document order, so the answer is stable and readable.
        let mut specs: Vec<FeedSpec> = Vec::new();
        for feed in &self.feeds {
            if !specs.contains(&feed.spec) {
                specs.push(feed.spec);
            }
        }
        specs
    }

    /// The distinct shards this publisher carries, in the document's own order.
    ///
    /// One entry however many specifications carry it: a shard is a partition of
    /// the instrument set, and a block of each specification for one shard is
    /// two channel instances of one partition. It is the unit the reference-data
    /// registry publishes a set for.
    #[must_use]
    pub fn shards(&self) -> Vec<ShardName> {
        let mut shards: Vec<ShardName> = Vec::new();
        for feed in &self.feeds {
            if !shards.contains(&feed.shard) {
                shards.push(feed.shard.clone());
            }
        }
        shards
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
        // Mapped rather than converted with `?`: the variant keeps its two
        // fields and its own message, so nothing matching on it or reading it
        // can tell that the token set moved crates.
        let spec =
            FeedSpec::resolve(&self.spec).map_err(|refused| StartupError::UnsupportedFeedSpec {
                spec: refused.spec,
                supported: refused.supported,
            })?;
        // Before anything that could fail on a different key, because a
        // document with a bad shard name and a bad port should be told about
        // the shard: it is the one that decides where files are written.
        let shard = match &self.shard {
            None => ShardName::default_shard(),
            Some(stated) => {
                let named =
                    ShardName::new(stated).ok_or_else(|| StartupError::UnsafeShardName {
                        spec: self.spec.clone(),
                        shard: stated.clone(),
                    })?;
                if named.is_default() {
                    return Err(StartupError::ReservedShardName {
                        spec: self.spec.clone(),
                        shard: stated.clone(),
                    });
                }
                named
            }
        };
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
            shard,
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

/// Every shard carries the same specifications, or the publisher refuses.
///
/// **This is what makes [`ListingSink::list_on`] total.** A venue admits an
/// instrument to a shard; if that shard has no block for a specification another
/// shard has, the instrument's messages for that specification reach no wire and
/// are counted only as unroutable. The venue did exactly what the interface
/// asked, the configuration is the thing that is wrong, and nothing at run time
/// can tell that apart from an instrument that simply never traded.
///
/// So it is a startup refusal, and it names the shard and the specification it
/// has no block for — the two things an operator has to edit.
///
/// [`ListingSink::list_on`]: dz_adapter_core::ListingSink::list_on
fn check_shards_carry_the_same_specifications(feeds: &[Feed]) -> Result<(), StartupError> {
    let mut by_shard: BTreeMap<&str, Vec<FeedSpec>> = BTreeMap::new();
    for feed in feeds {
        by_shard
            .entry(feed.shard.as_str())
            .or_default()
            .push(feed.spec);
    }
    // The union, because the question is not what the first shard carries but
    // what any of them does: a specification one shard has is one every shard
    // must have, whichever shard the document happens to list first.
    let mut every: Vec<FeedSpec> = Vec::new();
    for specs in by_shard.values() {
        for spec in specs {
            if !every.contains(spec) {
                every.push(*spec);
            }
        }
    }
    for (shard, specs) in &by_shard {
        for spec in &every {
            if !specs.contains(spec) {
                return Err(StartupError::ShardSpecsDisagree {
                    shard: (*shard).to_owned(),
                    spec: spec.as_str().to_owned(),
                });
            }
        }
    }
    Ok(())
}

/// Resolve `[[source]]`, and refuse every document that names alternatives
/// without saying which one publishes.
///
/// # The one rule that has to be a startup error
///
/// **Exactly one enabled `primary`, publisher-wide.** Two primaries are two
/// publishers' worth of events on the channel instances they reach: the
/// `Sequence Number` series is per channel instance, so a subscriber's gap
/// detection reads the two interleaved as its own losses and cannot tell which.
/// None is a publisher whose data has no path to the wire, heartbeating channels
/// it never fills.
///
/// **Publisher-wide, and not per feed.** A per-feed rule would be a statement
/// about routing this runtime does not do: every source's payloads reach one
/// adapter, the adapter emits events, and no event carries the source it came
/// from — so nothing here can confine one source's data to one feed. The rule
/// as written is the one the runtime upholds, and it is checkable from the
/// document alone.
///
/// # The rule that makes the session count the operator's statement
///
/// **One driver is opened per enabled `[[source]]`**, so on a session
/// transport sessions are per source: not per `[[feed]]`, not per shard, not
/// per channel instance. A publisher carrying sixty-two channel instances of
/// one feed specification over one source opens one session, because a shard
/// is a partition of the *published set* and has nothing to do with how many
/// upstream connections exist.
///
/// That is worth stating because a venue may permit one session per credential
/// and answer a second logon by evicting the first, which makes the count a
/// thing an operator has to be able to read off the document. What this
/// function refuses is the copy-paste failure — two enabled blocks whose
/// `credentials` tables are equal — and
/// [`StartupError::SourceCredentialsShared`](crate::StartupError) states both
/// why and what it cannot see.
///
/// A `comparison` source is refused nothing else: several are fine, and one
/// arriving beside the primary is the whole point of the role.
fn resolve_sources(sections: Vec<SourceSection>) -> Result<Vec<Source>, StartupError> {
    let mut sources: Vec<Source> = Vec::new();
    let mut seen: BTreeMap<String, ()> = BTreeMap::new();

    for section in sections {
        // Trimmed once, and then held against what was written: the name is the
        // `connection` metric label, and a name that differs from its trim
        // would be one string in the file and another in a dashboard.
        let name = section.name.trim();
        if name.is_empty() {
            return Err(StartupError::UnnamedSource);
        }
        if name != section.name {
            return Err(StartupError::SourceNameNotTrimmed {
                trimmed: name.to_owned(),
                name: section.name,
            });
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
            // Mapped for the reason the feed specification above is: the
            // variant keeps both fields and its own message.
            Some(token) => {
                SourceRole::resolve(token).map_err(|refused| StartupError::UnknownSourceRole {
                    token: refused.token,
                    supported: refused.supported,
                })?
            }
            None => SourceRole::default(),
        };

        sources.push(Source {
            // Leaked once, here, before the metric registry exists. See
            // `Source::connection` for why a label cannot be anything else.
            connection: ConnectionId::new(Box::leak(section.name.into_boxed_str())),
            kind,
            role,
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

    // Two enabled sources with one credential, refused naming both. See
    // `StartupError::SourceCredentialsShared` for the failure it prevents and
    // for the case it cannot see.
    //
    // Over the *enabled* sources only, unlike the name check: a disabled block
    // opens no session, so it cannot be one of two logons. And an empty
    // credentials table is not a shared credential — a venue that needs none
    // leaves it unwritten, and several sources doing so is not two logons with
    // one credential.
    for (index, source) in sources.iter().enumerate() {
        if source.credentials.is_empty() {
            continue;
        }
        if let Some(other) = sources[..index]
            .iter()
            .find(|earlier| earlier.credentials == source.credentials)
        {
            return Err(StartupError::SourceCredentialsShared {
                one: other.connection.as_str().to_owned(),
                another: source.connection.as_str().to_owned(),
            });
        }
    }

    let primaries: Vec<&str> = sources
        .iter()
        .filter(|source| source.is_primary())
        .map(|source| source.connection.as_str())
        .collect();
    if primaries.len() != 1 {
        return Err(StartupError::SourcePrimaries {
            primaries: if primaries.is_empty() {
                "none".to_owned()
            } else {
                primaries.join(", ")
            },
        });
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
