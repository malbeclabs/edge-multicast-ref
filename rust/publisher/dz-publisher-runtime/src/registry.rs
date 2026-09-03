//! `[adapter] kind`: a registry the venue's own `main` populates, and the one
//! thing this crate cannot know.
//!
//! # Why a registry and not a match
//!
//! The publisher crates design spells `[ingress] kind` and `[adapter] kind`
//! alike and says plainly that they are not the same mechanism.
//! [`dz_ingress_core::Kind`] is the other one: the family of transports is
//! fixed, lives in this repository, and is therefore a closed enum and a total
//! match. An adapter is the opposite. The set of adapters a binary contains is
//! a property of *that binary*, decided by whoever linked it, and this crate is
//! a library that is linked rather than a service that is deployed — so the
//! only place the set is knowable is the venue's own `main`:
//!
//! ```no_run
//! # use dz_publisher_runtime::{AdapterRegistry, Venue};
//! # struct VenueAdapter;
//! # impl VenueAdapter { fn new(_: &dz_publisher_runtime::AdapterContext<'_>)
//! #     -> Result<Self, std::io::Error> { Ok(Self) } }
//! # impl dz_adapter_core::Adapter for VenueAdapter {
//! #     fn message_types(&self) -> &[&'static str] { &[] }
//! #     fn poll_listings(&mut self, _: &mut dyn dz_adapter_core::ListingSink) {}
//! #     fn on_payload(&mut self, _: &dz_adapter_core::Payload<'_>,
//! #         _: &mut dyn dz_adapter_core::EventSink)
//! #         -> Result<(), dz_adapter_core::ParseError> { Ok(()) }
//! # }
//! # fn venue_input(_: &dz_publisher_runtime::AdapterContext<'_>)
//! #     -> Result<Box<dyn dz_ingress_core::Input>, std::io::Error> { unimplemented!() }
//! fn main() -> std::process::ExitCode {
//!     dz_publisher_runtime::run(AdapterRegistry::new().with("a-venue", |cx| {
//!         Ok(Venue {
//!             adapter: Box::new(VenueAdapter::new(cx)?),
//!             input: venue_input(cx)?,
//!         })
//!     }))
//! }
//! ```
//!
//! The runtime owns configuration loading, the guards, the signals, the
//! metrics, the egress and the reference data. The registry is the only thing
//! it cannot know. Static dispatch where it matters, `cargo` resolving
//! versions, no ABI, and a binary that cannot be pointed at an adapter it does
//! not contain.
//!
//! # An unregistered `kind` is a startup error naming the registry
//!
//! No default. No fallback. Not *the first registered one*, not *the only
//! registered one*, and not an empty adapter that would leave the process up
//! and publishing heartbeats over nothing. The audit's own lesson is the whole
//! reason: a publisher had a misspelled section parse cleanly, fall back to a
//! default, and run the wrong transport while the operator believed otherwise.
//! What the error names is the registry, because *what is in this binary* is
//! the question an operator cannot answer from the file in front of them.

use std::collections::BTreeSet;

use dz_adapter_core::Adapter;
use dz_ingress_core::{Input, Kind};
use serde::de::DeserializeOwned;

use crate::config::{AdapterConfig, ReplayConfig};
use crate::error::{AdapterInitError, StartupError};

/// What a venue's constructor hands back: the mapping, and the transport it
/// reads from.
///
/// # Why the transport comes from here too
///
/// The design's `main` shape returns only the adapter, and that is one piece
/// short of what a publisher needs — the gap is in the configuration document
/// rather than in the shape. `[adapter.upstream]` is *"endpoints; keys defined
/// by the adapter"*, so the endpoint a transport connects to is a value only the
/// adapter's own code knows the name of. A runtime that constructed the
/// transport itself would have to know that key, and it cannot; worse, it would
/// have to depend on every transport crate in the family, which is precisely
/// what [`Kind::is_linked`](dz_ingress_core::Kind::is_linked) exists to avoid —
/// a transport is linked when the crate implementing it is in the build, and a
/// runtime depending on all of them would make every one of them always linked.
///
/// So `[ingress] kind` is still resolved here, against the closed set, with the
/// two distinguishable failures that resolution already reports; the resolved
/// [`Kind`] is handed to the constructor in [`AdapterContext::ingress_kind`];
/// and the constructor builds the transport that kind names. What the runtime
/// cannot check is that it built the *matching* one, which is the honest cost of
/// this and is stated rather than hidden.
pub struct Venue {
    /// The venue's mapping from its upstream's payloads onto normalized events.
    pub adapter: Box<dyn Adapter>,
    /// The transport those payloads arrive on. See the type's own note.
    pub input: Box<dyn Input>,
}

/// Neither half is `Debug` and neither can be: `Adapter` and `Input` are
/// boundaries a venue implements, and requiring `Debug` of them would be this
/// crate asking every venue for something only its own error messages want.
impl std::fmt::Debug for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Venue").finish_non_exhaustive()
    }
}

/// What a venue's constructor is given.
///
/// Everything about the configuration that is the venue's, and nothing that is
/// not. There is deliberately no `Channel ID`, `Source ID`, multicast group,
/// port or era in here: those are the values the boundary exists to keep out of
/// a venue's hands, and a context carrying them would hand them back.
pub struct AdapterContext<'a> {
    kind: &'a str,
    ingress_kind: Kind,
    venue: &'a str,
    upstream: &'a toml::Table,
    credentials: &'a toml::Table,
    replay: &'a ReplayConfig,
}

impl<'a> AdapterContext<'a> {
    /// The context for one `[adapter]` section and one resolved `[ingress]
    /// kind`.
    #[must_use]
    pub fn new(adapter: &'a AdapterConfig, ingress_kind: Kind, venue: &'a str) -> Self {
        Self {
            kind: &adapter.kind,
            ingress_kind,
            venue,
            upstream: &adapter.upstream,
            credentials: &adapter.credentials,
            replay: &adapter.replay,
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

    /// The transport `[ingress] kind` resolved to. See [`Venue`].
    #[must_use]
    pub const fn ingress_kind(&self) -> Kind {
        self.ingress_kind
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
    pub fn upstream<T: DeserializeOwned>(&self) -> Result<T, toml::de::Error> {
        self.upstream.clone().try_into()
    }

    /// `[adapter.credentials]`, as the adapter's own type.
    ///
    /// Every value here has already been checked to be a single-line string;
    /// see [`StartupError::NotACredentialPath`]. What it points at is the
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

/// What a venue registers: a name, and something that builds the integration.
///
/// Boxed rather than an `fn` pointer, so that a `main` may close over what it
/// has already parsed or opened. `Fn` rather than `FnOnce`, so that the
/// registry stays inspectable — [`AdapterRegistry::kinds`] and the error
/// message have to be able to name every entry whether or not one has been
/// used.
type Constructor = Box<dyn Fn(&AdapterContext<'_>) -> Result<Venue, AdapterInitError>>;

/// The adapters this binary contains, by the name `[adapter] kind` selects them
/// with.
///
/// Built in `main` and handed to [`run()`](crate::run()). See the module
/// documentation for the whole argument; the two properties worth stating on the
/// type are that resolution has no default, and that a name registered twice is
/// a panic rather than a silent shadowing.
#[derive(Default)]
pub struct AdapterRegistry {
    /// Registration order, kept: it is the order a `main` reads in, which is
    /// what a reader comparing the file to the code needs. The error message
    /// sorts instead, so that what an operator is shown does not depend on the
    /// order somebody happened to write the calls in.
    entries: Vec<(&'static str, Constructor)>,
}

impl AdapterRegistry {
    /// An empty registry.
    ///
    /// Empty is a legitimate state to build and an illegitimate state to run:
    /// a binary that registered no adapter cannot resolve any `kind`, and the
    /// error says so in those words rather than printing an empty list.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one adapter under the name `[adapter] kind` selects it with.
    ///
    /// # Panics
    ///
    /// If `name` is already registered. A duplicate is a bug in `main`, and the
    /// two ways of absorbing it are both worse than a panic: keeping the first
    /// silently ignores the second, and keeping the second silently replaces
    /// the first — which is *an adapter shadowing another adapter*, the exact
    /// class of failure this registry exists to make impossible. The panic
    /// happens before anything is opened, before a socket exists, and before a
    /// single datagram, so it is a startup crash with a message and not an
    /// incident.
    #[must_use]
    pub fn with<F>(mut self, name: &'static str, constructor: F) -> Self
    where
        F: Fn(&AdapterContext<'_>) -> Result<Venue, AdapterInitError> + 'static,
    {
        assert!(
            !self.entries.iter().any(|(known, _)| *known == name),
            "the adapter `{name}` is registered twice; one would shadow the other"
        );
        self.entries.push((name, Box::new(constructor)));
        self
    }

    /// Every registered name, sorted.
    #[must_use]
    pub fn kinds(&self) -> Vec<&'static str> {
        let sorted: BTreeSet<&'static str> = self.entries.iter().map(|(name, _)| *name).collect();
        sorted.into_iter().collect()
    }

    /// How many adapters are registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The registry as an error message names it.
    ///
    /// A binary that registered nothing says so in words. Printing an empty
    /// list would read as a spelling problem, and the actual problem is a build
    /// that linked no adapter — a different action entirely, following
    /// [`Kind::linked_list`](dz_ingress_core::Kind::linked_list), which makes
    /// the same distinction for the same reason.
    #[must_use]
    pub fn registered_list(&self) -> String {
        let kinds = self.kinds();
        let built_in = format!("built in: {}", crate::builtin::BUILTIN_KINDS.join(", "));
        if kinds.is_empty() {
            // Still worth saying rather than printing only the built-in: a
            // build that linked no venue adapter is a different problem from a
            // misspelled name, and it wants a different action.
            format!("none registered by this binary ({built_in})")
        } else {
            format!("{} ({built_in})", kinds.join(", "))
        }
    }

    /// Resolve `[adapter] kind` and construct the integration it names.
    ///
    /// # Errors
    ///
    /// [`StartupError::UnknownAdapterKind`] for a name this binary did not
    /// register, **naming every name it did**; and
    /// [`StartupError::AdapterInit`] carrying whatever the venue's constructor
    /// refused with.
    pub fn open(&self, cx: &AdapterContext<'_>) -> Result<Venue, StartupError> {
        if let Some((name, constructor)) = self.entries.iter().find(|(name, _)| *name == cx.kind())
        {
            return constructor(cx)
                .map_err(|source| StartupError::AdapterInit { kind: name, source });
        }
        // **After the venue's own entries, and never instead of one.** A venue
        // that registers a name this crate also builds in gets its own — it is
        // the one that knows its upstream — and the built-in is what answers a
        // name nothing else does. See `crate::builtin` for why there is one at
        // all.
        if let Some(built_in) = crate::builtin::open(cx) {
            let kind = crate::builtin::BUILTIN_KINDS
                .iter()
                .find(|name| **name == cx.kind())
                .copied()
                .unwrap_or("built-in");
            return built_in.map_err(|source| StartupError::AdapterInit { kind, source });
        }
        Err(StartupError::UnknownAdapterKind {
            token: cx.kind().to_owned(),
            registered: self.registered_list(),
        })
    }
}
