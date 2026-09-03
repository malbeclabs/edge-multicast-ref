//! Why a publisher did not start.
//!
//! Every variant here is a refusal to start, and every one of them names the
//! values that *would* have been accepted wherever there is a set to name. That
//! is the audit's own lesson rather than a style rule: a publisher had a
//! misspelled section parse cleanly, fall back to a default, and run a
//! transport its operator did not believe it was running. An error that says
//! only what was wrong invites the same guess a second time.
//!
//! There is deliberately nothing in here that a publisher can continue past. A
//! configuration that is wrong about the wire — the source identity, the group,
//! the ports, the adapter — produces datagrams nobody is subscribed to, which
//! is indistinguishable at a subscriber from a publisher that is simply down,
//! and takes far longer to diagnose.

use std::io;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use dz_publisher_egress::{EraError, OpenError, PrefixError};
use dz_publisher_refdata::{PolicyError, RefdataError};

/// What the constructor a venue registered may fail with.
///
/// Boxed rather than a type of this crate's own, because the failure belongs to
/// the venue: a credential file that is not there, an endpoint that does not
/// parse, an upstream section missing a key only the adapter knows the name of.
/// A closed enumeration here would have to anticipate all of them, and a venue
/// whose failure did not fit would be pushed into whichever variant was nearest.
pub type AdapterInitError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Why a publisher did not start.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("the configuration file {path:?} could not be read: {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: io::Error,
    },

    /// The document did not parse, which includes every key nobody reads.
    ///
    /// `deny_unknown_fields` is on every section this crate owns, so a
    /// misspelled section name arrives here rather than as a section that
    /// quietly took its defaults. The message serde produces names the key it
    /// did not recognise and lists the keys it would have.
    #[error("the configuration document is not one this publisher can run: {source}")]
    Document {
        #[from]
        source: toml::de::Error,
    },

    /// `[adapter] kind` names no adapter this binary was linked with.
    ///
    /// **Never a fallback and never a default.** The message lists the registry
    /// because that is the operator's next action: the set of adapters in a
    /// binary is a property of the build, so being told only that a value was
    /// refused leaves the question of whether to fix a spelling or redo a build
    /// unanswered.
    #[error(
        "`[adapter] kind = \"{token}\"` names no adapter this binary registered; \
         registered in this binary: {registered}"
    )]
    UnknownAdapterKind { token: String, registered: String },

    /// The adapter was registered and would not construct.
    #[error("the adapter registered as `{kind}` could not be constructed: {source}")]
    AdapterInit {
        kind: &'static str,
        #[source]
        source: AdapterInitError,
    },

    /// `[ingress]` could not be resolved. See `dz_ingress_core::ConfigError`,
    /// which names the built-in transports and the ones this binary links.
    #[error(transparent)]
    Ingress {
        #[from]
        source: dz_ingress_core::ConfigError,
    },

    /// A `[[feed]] spec` this build cannot emit.
    ///
    /// The tokens are the codec crates' own `Feed::NAME` constants, so this is
    /// a spelling mistake or a feed whose codec crate is not in this workspace
    /// — `dz-edge-mbo` and `dz-edge-perp-stats` being the ones that are not.
    /// The message names what this build can emit, because being told only
    /// that a value was refused leaves an operator guessing.
    #[error(
        "`[[feed]] spec = \"{spec}\"` is not a feed this build can emit; it can emit: {supported}"
    )]
    UnsupportedFeedSpec { spec: String, supported: String },

    /// A depth feed with no `snapshot_port`.
    ///
    /// Refused rather than run without one. A subscriber to a depth feed holds
    /// a book that exists only because it applied every message in order, so
    /// one that lost a datagram has nowhere to recover from — and the publisher
    /// looks healthy the whole time.
    #[error(
        "`[[feed]] spec = \"{spec}\"` carries a snapshot port role and `snapshot_port` is not set"
    )]
    SnapshotPortRequired { spec: &'static str },

    /// A feed with a `snapshot_port` its specification does not carry.
    ///
    /// Refused rather than ignored: an operator who wrote a port believes
    /// something is listening on it, and a key nobody reads is the audit's own
    /// failure one level down.
    #[error(
        "`[[feed]] spec = \"{spec}\"` carries no snapshot port role, so \
         `snapshot_port = {port}` names a port nothing will send on"
    )]
    SnapshotPortNotCarried { spec: &'static str, port: u16 },

    /// A feed with a `snapshot_cycle` and no snapshot port role.
    ///
    /// The same rule as [`Self::SnapshotPortNotCarried`], one key along: a
    /// cadence for a port role the feed does not carry is a key nobody reads,
    /// and an operator who wrote it believes snapshots are going out.
    #[error(
        "`[[feed]] spec = \"{spec}\"` carries no snapshot port role, so \
         `snapshot_cycle` would pace snapshots nothing sends"
    )]
    SnapshotCycleWithoutPort { spec: &'static str },

    /// Two enabled feeds stating different values for a key the publisher holds
    /// once.
    ///
    /// **Refused rather than resolved to the first feed's**, which is what this
    /// runtime silently did. Both keys are per-feed in the document because they
    /// are properties of a feed, and both are single in the publisher because of
    /// what they drive: `definition_cycle` paces one reference-data registry,
    /// which is one because `Instrument ID` identity can only be one thing, and
    /// `idle_guard` measures one publisher's silence. Taking the first block's
    /// answer means an operator who set the second is obeyed on paper and
    /// ignored in fact.
    #[error(
        "two enabled `[[feed]]` blocks state different `{key}`, {one:?} and \
         {another:?}, and this publisher holds one"
    )]
    FeedsDisagree {
        key: &'static str,
        one: std::time::Duration,
        another: std::time::Duration,
    },

    /// The venue's constructor handed back no transport at all.
    ///
    /// A publisher with nothing to read from would come up, publish nothing,
    /// and look like a quiet venue.
    #[error("the adapter's constructor returned no source: this publisher would read nothing")]
    NoVenueSource,

    /// The venue built several transports and the document declares none.
    ///
    /// Refused rather than run: with no `[[source]]` block there is nothing
    /// that says what the second connection is, which feed it carries or
    /// whether it is meant to publish — and its name would be the venue's
    /// rather than the operator's.
    #[error(
        "the adapter's constructor returned {built} sources and the document declares none; \
         state one `[[source]]` block per connection"
    )]
    SourcesUndeclared { built: usize },

    /// The document's sources and the venue's transports are not the same set.
    ///
    /// Every way they can disagree is silent. A transport the document did not
    /// declare moves traffic under a `connection` label the metric registry
    /// never pre-created, so it is counted under nothing; a declared source the
    /// venue did not build is a series sitting at zero, which reads exactly
    /// like an upstream that is down.
    #[error(
        "the `[[source]]` blocks and the adapter's transports are different sets: declared \
         {declared}; built {built}"
    )]
    SourcesDisagree { declared: String, built: String },

    /// A `[[source]]` block with an empty `name`.
    ///
    /// The name is the `connection` metric label, so an empty one is a series
    /// nobody can group by and a log line nobody can read.
    #[error("a `[[source]]` block has an empty `name`")]
    UnnamedSource,

    /// A `name` with leading or trailing whitespace.
    ///
    /// Refused rather than trimmed, because the name is used three times and
    /// trimming it in one of them is worse than either. The emptiness check
    /// above reads the trimmed string; the duplicate check and the leak that
    /// produces the `connection` label read what was written. So `"ws"` and
    /// `"ws "` resolve as two distinct sources carrying two label values a
    /// dashboard cannot tell apart, and an error listing them renders them as
    /// `ws, ws `. Trimming silently would fix the label and leave an operator
    /// reading a file that does not say what the label says; refusing names the
    /// typo where it was made.
    #[error(
        "`[[source]] name = \"{name}\"` has leading or trailing whitespace: the name is the \
         `connection` metric label, and `\"{trimmed}\"` and `\"{name}\"` would be two series a \
         dashboard cannot tell apart"
    )]
    SourceNameNotTrimmed { name: String, trimmed: String },

    /// Two `[[source]]` blocks sharing a name.
    ///
    /// Checked across every block and not only the enabled ones: two blocks
    /// with one name are two descriptions of a single connection, and which of
    /// them is in force would depend on which happened to be enabled today.
    #[error("two `[[source]]` blocks are named `{name}`")]
    DuplicateSourceName { name: String },

    /// `[[source]] role` is a token the closed set does not carry.
    #[error("`[[source]] role = \"{token}\"` is not a role; the roles are {supported}")]
    UnknownSourceRole {
        token: String,
        supported: &'static str,
    },

    /// Every `[[source]]` block is disabled.
    #[error("every `[[source]]` block is disabled: this publisher would connect to nothing")]
    NoEnabledSource,

    /// No enabled `primary` source, or more than one.
    ///
    /// **The rule the whole array exists to make checkable, and it is
    /// publisher-wide rather than per feed** — because that is the rule the
    /// runtime actually upholds. Every source's payloads reach one adapter, the
    /// adapter emits events, and no event carries the source it came from, so
    /// the runtime cannot route one source's data to one feed and another's to
    /// another. A per-feed rule would have accepted two primaries with disjoint
    /// declarations and let both upstreams' events land on one channel instance
    /// under one `Sequence Number` series, which a subscriber reads as its own
    /// gap-detection losses and cannot attribute.
    ///
    /// Two primaries are therefore two publishers' worth of events wherever
    /// they land. None is a publisher whose data has no path to the wire at all,
    /// heartbeating channels it never fills.
    #[error(
        "exactly one enabled `[[source]]` must have `role = \"primary\"`; there {} {primaries}. \
         Every source's payloads reach one adapter and no event carries the source it came \
         from, so this is a publisher-wide rule and not a per-feed one",
        if primaries.contains(',') { "are" } else { "is" }
    )]
    SourcePrimaries { primaries: String },

    /// Two enabled feeds naming different `source_id`s.
    ///
    /// A `Source ID` is the publisher's registered identity and is the same for
    /// every message a process sends — the lowering takes it once, for that
    /// reason. Two feeds asking for different ones is a configuration that
    /// cannot be obeyed, and obeying either would put an identity on one feed's
    /// wire that its own block did not ask for.
    #[error("two enabled `[[feed]]` blocks name different source ids, {one} and {another}")]
    SeveralSourceIds { one: u16, another: u16 },

    /// Two `[[feed]]` blocks name the same specification.
    ///
    /// Refused rather than merged: each block carries its own `Channel ID`,
    /// ports and era, so two blocks for one feed are two channel instances of
    /// the same feed — and a subscriber tracking either one sees the other's
    /// numbering as its own gaps.
    #[error("two `[[feed]]` blocks name `spec = \"{spec}\"`")]
    DuplicateFeedSpec { spec: String },

    /// Every `[[feed]]` block is disabled, or there are none.
    #[error("no `[[feed]]` block is enabled: this publisher would emit nothing")]
    NoEnabledFeed,

    /// `[[feed]] source_id` is outside the ranges the source registry admits.
    ///
    /// `0` is reserved and MUST NOT reach the wire, and it is the value a
    /// half-read configuration file hands you. `1024`–`32767` are reserved for
    /// future assignment.
    #[error(
        "`[[feed]] source_id = {source_id}` is not a Source ID the registry admits; \
         assigned production ids are 1-1023 and private or experimental ids are 32768-65535"
    )]
    BadSourceId { source_id: u16 },

    /// A value that should have been an address and was not.
    #[error("`{key} = \"{value}\"` is not an IPv4 address")]
    NotAnAddress { key: &'static str, value: String },

    /// `[[feed]] multicast_group` is not a multicast address.
    #[error("`[[feed]] multicast_group = \"{group}\"` is not a multicast address")]
    NotMulticast { group: Ipv4Addr },

    /// `[egress] expected_prefix` is not a prefix.
    #[error("`[egress] expected_prefix`: {source}")]
    BadPrefix {
        #[source]
        source: PrefixError,
    },

    /// A port that is zero.
    ///
    /// Zero is the wildcard, which for a destination port is not a port at all;
    /// it is also what an unset integer key reads as.
    #[error("`[[feed]] {key}` is 0, which is not a destination port")]
    ZeroPort { key: &'static str },

    /// Two port roles of one feed share a destination port.
    ///
    /// The port is what separates the roles — the specification mandates one
    /// group with distinct destination ports — and the channel instance a
    /// subscriber tracks is keyed on it. Two roles on one port interleave two
    /// independent sequence series into one that goes backwards on every
    /// alternation.
    #[error("`[[feed]] {left}` and `{right}` are both port {port}")]
    PortsCollide {
        left: &'static str,
        right: &'static str,
        port: u16,
    },

    /// A duration key that is zero.
    #[error("`{key}` must be greater than zero")]
    ZeroDuration { key: &'static str },

    /// `[refdata.selection]` is not a coherent policy.
    #[error("`[refdata.selection]`: {source}")]
    Selection {
        #[from]
        source: PolicyError,
    },

    /// A `[adapter.credentials]` value that is not a path.
    ///
    /// The rule is *paths only, never inline secrets*, and this is the half of
    /// it that is mechanically checkable: a value that is not a string, or one
    /// carrying a line break, is a key or a certificate somebody pasted into
    /// the configuration file.
    #[error("`[adapter.credentials] {key}` is {what}; credentials are paths to files")]
    NotACredentialPath { key: String, what: &'static str },

    /// The reference-data owner would not open. See `RefdataError`; every case
    /// is a startup failure there too, including the single-writer guard
    /// refusing a second publisher on one state directory.
    #[error(transparent)]
    Refdata {
        #[from]
        source: RefdataError,
    },

    /// The era could not be read or recorded, so a restart would be invisible
    /// to every subscriber.
    #[error(transparent)]
    Era {
        #[from]
        source: EraError,
    },

    /// A transmitter would not open: no route to the group, a source address
    /// outside the declared prefix, or a socket that would not bind.
    #[error(transparent)]
    Transmitter {
        #[from]
        source: OpenError,
    },

    /// The metrics endpoint would not bind.
    ///
    /// A refusal to start rather than a warning: a publisher with no `/metrics`
    /// is a publisher no alert can fire on, and the whole point of the
    /// normative set is that a fleet dashboard works without anyone thinking
    /// about it.
    #[error("the metrics endpoint could not be bound to {addr}: {source}")]
    Metrics {
        addr: std::net::SocketAddr,
        #[source]
        source: io::Error,
    },

    /// The async runtime would not start.
    ///
    /// Its own variant rather than folded into [`Self::Metrics`]: they are the
    /// only two `io::Error`s at startup and they are opposite problems, so
    /// sharing a message would send an operator to look at a listen address
    /// when the host is out of file descriptors.
    #[error("the async runtime could not be started: {source}")]
    Runtime {
        #[source]
        source: io::Error,
    },

    /// No configuration file was named.
    #[error("no configuration file: {usage}")]
    NoConfigPath { usage: &'static str },

    /// `[adapter.tee] enabled = true` with no `path`.
    ///
    /// The same shape as [`Self::ReplayWithoutPath`]: a section switched on and
    /// left incomplete is an operator who believes copies are being archived.
    #[error("[adapter.tee] is enabled but names no path")]
    TeeWithoutPath,

    /// The reference stream's own socket would not open.
    ///
    /// Not the consumer's socket: nothing is connected there, so a recorder
    /// that has not started yet is the ordinary case and not a startup failure.
    /// This is a host on which an unbound datagram socket cannot be created at
    /// all.
    #[error("the reference stream for {path} would not open: {source}")]
    Tee {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// `[adapter.replay] enabled = true` with no `path`.
    ///
    /// Named rather than defaulted to a directory: a replay that silently read
    /// the working directory would publish whatever happened to be there, and a
    /// publisher's whole purpose is that what it sends is what a venue said.
    #[error("[adapter.replay] is enabled but names no path")]
    ReplayWithoutPath,

    /// The replay directory could not be read, or holds no payload.
    ///
    /// A startup error rather than a driver retry: no amount of reconnecting
    /// will put a file there.
    #[error("the replay directory cannot be used: {source}")]
    Replay {
        #[source]
        source: dz_ingress_core::IngressError,
    },
}
