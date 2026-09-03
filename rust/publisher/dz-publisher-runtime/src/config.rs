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

    pub adapter: AdapterConfig,
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
    #[serde(default = "default_definition_cycle", deserialize_with = "de_duration")]
    pub definition_cycle: Duration,

    #[serde(default = "default_manifest_cadence", deserialize_with = "de_duration")]
    pub manifest_cadence: Duration,

    /// Feed silence, which is not upstream silence. See
    /// [`IdleGuard`](crate::IdleGuard) for what this measures and, more to the
    /// point, what it refuses to measure — `[ingress] idle_timeout` is the
    /// other one, and the two are deliberately spelled differently so that
    /// neither can be read as the other.
    #[serde(default = "default_idle_guard", deserialize_with = "de_duration")]
    pub idle_guard: Duration,
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
/// # One socket per port role, and no framing at all
///
/// `path` is a **prefix**: the role's own token is appended, so a `path` of
/// `/run/a-publisher/tee` is written to as `tee.mktdata`, `tee.refdata` and
/// `tee.snapshot`. The diff this stream exists for is keyed on the destination
/// port among other things, and a Unix datagram carries no UDP header — so one
/// socket for three roles would hand a recorder datagrams it could not
/// attribute without decoding them, and decoding is the one thing a record path
/// does not do. The shape mirrors the recorder's own configuration, which
/// already names a port per role.
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
    pub definition_cycle: Duration,
    pub manifest_cadence: Duration,
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
    pub ingress_kind: Kind,
    pub ingress: Policy,
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
        let mut feeds = Vec::new();
        let mut seen: BTreeMap<&'static str, ()> = BTreeMap::new();
        for section in self.feeds {
            if !section.enabled {
                continue;
            }
            let feed = section.resolve()?;
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
        for (key, value, of_other) in [
            (
                "[[feed]] definition_cycle",
                feeds[0].definition_cycle,
                feeds
                    .iter()
                    .find(|feed| feed.definition_cycle != feeds[0].definition_cycle)
                    .map(|feed| feed.definition_cycle),
            ),
            (
                "[[feed]] idle_guard",
                feeds[0].idle_guard,
                feeds
                    .iter()
                    .find(|feed| feed.idle_guard != feeds[0].idle_guard)
                    .map(|feed| feed.idle_guard),
            ),
        ] {
            if let Some(another) = of_other {
                return Err(StartupError::FeedsDisagree {
                    key,
                    one: value,
                    another,
                });
            }
        }

        let selection = SelectionPolicy::new(
            self.refdata.selection.bootstrap_top_n,
            self.refdata.selection.max_published,
            self.refdata.selection.warn_published_above,
        )?;

        let (ingress_kind, ingress) = self.ingress.resolve()?;

        check_credentials(&self.adapter.credentials)?;

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

impl FeedSection {
    fn resolve(self) -> Result<Feed, StartupError> {
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

        for (key, value) in [
            ("[[feed]] heartbeat_interval", self.heartbeat_interval),
            ("[[feed]] definition_cycle", self.definition_cycle),
            ("[[feed]] manifest_cadence", self.manifest_cadence),
            ("[[feed]] idle_guard", self.idle_guard),
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
            definition_cycle: self.definition_cycle,
            manifest_cadence: self.manifest_cadence,
            idle_guard: self.idle_guard,
        })
    }
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
