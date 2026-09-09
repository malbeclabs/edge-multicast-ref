//! What a venue's constructor is given, and the four values it is built from.

use dz_ingress_core::Kind;
use serde::de::DeserializeOwned;

use crate::config::{FeedSpec, ReplayConfig, Source};

/// The `[adapter]` section, as a venue's constructor reads it.
///
/// # Why a trait and not the section's own type
///
/// [`AdapterContext::new`] reads exactly four values out of an `[adapter]`
/// section: the `kind` that selected the constructor, the two free tables whose
/// keys only the adapter knows, and whether this run reads a fixture directory.
/// It has never read anything else.
///
/// The type carrying those four in `dz-publisher-runtime` carries more, and the
/// rest is the publisher's: one of its sections has a method that returns the
/// publisher's own startup error and takes a `PortRole`, so moving the type
/// here would move the publisher's error here with it — which is the whole
/// thing this crate exists not to link. Naming the four instead leaves that
/// type where it belongs and makes the claim precise: **the document a section
/// came out of is not one of the four**, so a process with a configuration of
/// its own can build the same context for the same venue's constructor without
/// owning the publisher's document.
pub trait AdapterSection {
    /// `[adapter] kind`.
    fn kind(&self) -> &str;
    /// `[adapter.upstream]`: endpoints, keys defined by the adapter.
    fn upstream(&self) -> &toml::Table;
    /// `[adapter.credentials]`: paths, never inline secrets.
    fn credentials(&self) -> &toml::Table;
    /// `[adapter.replay]`.
    fn replay(&self) -> &ReplayConfig;
}

/// What a venue's constructor is given.
///
/// Everything about the configuration that is the venue's, and nothing that is
/// not. There is deliberately no `Channel ID`, `Source ID`, multicast group,
/// port or era in here: those are the values the boundary exists to keep out of
/// a venue's hands, and a context carrying them would hand them back.
pub struct AdapterContext<'a> {
    kind: &'a str,
    ingress_kind: Option<Kind>,
    venue: &'a str,
    upstream: &'a toml::Table,
    credentials: &'a toml::Table,
    replay: &'a ReplayConfig,
    sources: &'a [Source],
    feeds: &'a [FeedSpec],
}

impl<'a> AdapterContext<'a> {
    /// The context for one `[adapter]` section and one resolved `[ingress]
    /// kind`.
    #[must_use]
    pub fn new<S: AdapterSection + ?Sized>(
        adapter: &'a S,
        ingress_kind: Option<Kind>,
        venue: &'a str,
        sources: &'a [Source],
        feeds: &'a [FeedSpec],
    ) -> Self {
        Self {
            kind: adapter.kind(),
            ingress_kind,
            venue,
            upstream: adapter.upstream(),
            credentials: adapter.credentials(),
            replay: adapter.replay(),
            sources,
            feeds,
        }
    }

    /// The `[adapter] kind` that selected this constructor.
    ///
    /// Worth having even though the constructor was chosen by it: one closure
    /// may be registered under several names by a venue whose adapter covers
    /// several of its own product lines.
    #[must_use]
    pub const fn kind(&self) -> &'a str {
        self.kind
    }

    /// The transport the document-level `[ingress] kind` resolved to.
    ///
    /// `None` when the document names a transport per `[[source]]` instead, in
    /// which case [`sources`](Self::sources) carries one [`Kind`] each and
    /// there is no single answer to give. The two are mutually exclusive at
    /// load: naming a transport in both places is refused.
    #[must_use]
    pub const fn ingress_kind(&self) -> Option<Kind> {
        self.ingress_kind
    }

    /// Every enabled `[[source]]`, resolved.
    ///
    /// What a venue builds one [`Input`](dz_ingress_core::Input) from each of:
    /// the name to carry as its
    /// [`ConnectionId`](dz_adapter_core::ConnectionId), the transport to open,
    /// and its own `upstream` and `credentials` tables. Empty when the document
    /// declares no sources, which is the publisher with one upstream — see
    /// [`Venue::single`](crate::Venue::single).
    ///
    /// **The `ConnectionId` is handed over rather than invented.** It is the
    /// `connection` metric label, it is declared to the registry at startup so
    /// the `== 0` alert exists before anything connects, and it comes from the
    /// document so that the file an operator reads and the label a dashboard
    /// groups by are one string. A venue that named its own would be a second
    /// place for that string to live.
    #[must_use]
    pub const fn sources(&self) -> &'a [Source] {
        self.sources
    }

    /// Every enabled `[[feed]] spec` this process asks this adapter for.
    ///
    /// **Which normalized-event surface will be asked for, and nothing about
    /// the wire.** No `Channel ID`, `Source ID`, group, port or era comes with
    /// it — those are the values this boundary exists to keep out of a venue's
    /// hands. A feed specification is the opposite kind of fact: it is what the
    /// runtime will ask this adapter *for*, and an adapter is the only thing
    /// that knows whether it can answer.
    ///
    /// **Two runtimes read this the same way.** For a publisher it is *what
    /// this publisher publishes*; for a process that records rather than
    /// publishes it is *what this process is recording*. That is one question
    /// with two verbs: the same value, and the same refusal, because an adapter
    /// that cannot answer a depth feed's snapshot has to fail at startup either
    /// way. The name is the publisher's spelling of it and stays, because the
    /// two readings never disagree about what is in the slice.
    ///
    /// It is here for the refusal that needs it. A depth feed obliges
    /// [`Adapter::snapshot`](dz_adapter_core::Adapter::snapshot) — a subscriber
    /// that lost a datagram has nowhere else to recover from, and one joining
    /// mid-session has nowhere to start — so an adapter that holds no book must
    /// be able to refuse that combination at startup rather than publish deltas
    /// no subscriber can apply. See [`crate::builtin`].
    #[must_use]
    pub const fn feeds(&self) -> &'a [FeedSpec] {
        self.feeds
    }

    /// The `venue` label, for an adapter that wants its own log lines to carry
    /// the same identity the metrics do.
    #[must_use]
    pub const fn venue(&self) -> &'a str {
        self.venue
    }

    /// `[adapter.upstream]`, as the adapter's own type.
    ///
    /// # Errors
    ///
    /// [`toml::de::Error`], which names the key and what was expected of it.
    /// The adapter should return it as an [`AdapterInitError`], which is what
    /// makes a missing endpoint a startup failure naming a key rather than a
    /// connect that fails forever under a backoff.
    ///
    /// [`AdapterInitError`]: crate::AdapterInitError
    pub fn upstream<T: DeserializeOwned>(&self) -> Result<T, toml::de::Error> {
        self.upstream.clone().try_into()
    }

    /// `[adapter.credentials]`, as the adapter's own type.
    ///
    /// Every value here has already been checked to be a single-line string by
    /// whichever runtime loaded the document. What it points at is the
    /// adapter's to read, and reading it in the constructor is right: a
    /// credential file that is not there should stop a startup, not a
    /// reconnect.
    ///
    /// # Errors
    ///
    /// [`toml::de::Error`].
    pub fn credentials<T: DeserializeOwned>(&self) -> Result<T, toml::de::Error> {
        self.credentials.clone().try_into()
    }

    /// `[adapter.replay]`: whether this run reads a fixture directory instead
    /// of the live upstream, and which.
    #[must_use]
    pub const fn replay(&self) -> &'a ReplayConfig {
        self.replay
    }
}
