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
    /// A separate error from a spelling mistake, and the message says which it
    /// is: the depth specs are named by the codec crates and lowered by
    /// `dz-publisher-lowering`, and what they lack is an
    /// `EgressMessageType` to be counted under — the metric name set is closed
    /// by a governing playbook, so the runtime cannot invent one and cannot
    /// push a message it has no label for. See the crate documentation.
    #[error(
        "`[[feed]] spec = \"{spec}\"` is not a feed this build can emit; it can emit: {supported}"
    )]
    UnsupportedFeedSpec { spec: String, supported: String },

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
}
