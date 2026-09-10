//! `[adapter] kind`: a registry the venue's own `main` populates, and the one
//! thing a runtime cannot know.
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
//! use dz_venue_composition::{AdapterContext, AdapterRegistry, Venue};
//! # struct VenueAdapter;
//! # impl VenueAdapter { fn new(_: &AdapterContext<'_>)
//! #     -> Result<Self, std::io::Error> { Ok(Self) } }
//! # impl dz_adapter_core::Adapter for VenueAdapter {
//! #     fn message_types(&self) -> &[&'static str] { &[] }
//! #     fn poll_listings(&mut self, _: &mut dyn dz_adapter_core::ListingSink) {}
//! #     fn on_payload(&mut self, _: &dz_adapter_core::Payload<'_>,
//! #         _: &mut dyn dz_adapter_core::EventSink)
//! #         -> Result<(), dz_adapter_core::ParseError> { Ok(()) }
//! # }
//! # fn venue_input(_: &AdapterContext<'_>)
//! #     -> Result<Box<dyn dz_ingress_core::Input>, std::io::Error> { unimplemented!() }
//! /// The venue's one line, written once and handed to whichever runtime links
//! /// it: `E` is how *that* runtime reports a refusal, and nothing here names
//! /// it. A publisher's `main` calls this on the registry it is about to run.
//! fn register<E>(registry: AdapterRegistry<E>) -> AdapterRegistry<E> {
//!     registry.with("a-venue", |cx| {
//!         Ok(Venue::single(
//!             Box::new(VenueAdapter::new(cx)?),
//!             venue_input(cx)?,
//!         ))
//!     })
//! }
//! ```
//!
//! A runtime owns configuration loading, the guards, the signals, the metrics,
//! and — where it is a publisher — the egress and the reference data. The
//! registry is the only thing it cannot know. Static dispatch where it matters,
//! `cargo` resolving versions, no ABI, and a binary that cannot be pointed at
//! an adapter it does not contain.
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
//!
//! # Whose error the refusal is reported in
//!
//! [`AdapterRegistry`] is generic over that error, through
//! [`AdapterResolution`], and defaults to [`AdapterInitError`]. The two
//! failures it reports — *no adapter answers this `kind`* and *the adapter this
//! `kind` names refused* — are not about an egress, an era store or a
//! reference-data registry, so they must not oblige a caller to link one.
//! `dz-publisher-runtime` aliases the registry at its own `StartupError` and
//! the alias is what a venue's `main` names, so the parameter is invisible to
//! it; a venue's constructor never sees it at all, because that returns
//! [`AdapterInitError`] whichever runtime is going to report the refusal. What
//! the parameter buys is the third case: a venue can write
//! `fn register<E>(AdapterRegistry<E>) -> AdapterRegistry<E>` once and hand the
//! same adapters to two runtimes.
//!
//! # What the default does, and the one thing it is not
//!
//! It spares a caller with no error of its own from writing the parameter — in
//! **type position**, which is the only position Rust applies a parameter
//! default in:
//!
//! ```
//! # use dz_venue_composition::AdapterRegistry;
//! let registry: AdapterRegistry = AdapterRegistry::new();
//! assert!(registry.is_empty());
//! ```
//!
//! It does **not** make an unannotated `AdapterRegistry::new().open(&cx)`
//! resolve. A value's type is never inferred from a parameter's default, so
//! that spelling is `error[E0282]: type annotations needed`:
//!
//! ```compile_fail
//! # use dz_venue_composition::{AdapterContext, AdapterRegistry};
//! # fn cx() -> AdapterContext<'static> { unimplemented!() }
//! let venue = AdapterRegistry::new().open(&cx());
//! ```
//!
//! What makes that line compile in `dz-publisher-runtime`'s suite is that
//! crate's *alias*, which names the error; the default is doing nothing there.
//! Worth stating because it is the tempting reading of why the parameter has a
//! default at all, and a reader who believed it would remove the alias.
//!
//! The default's own user, and both message strings the fallback below formats,
//! are in `tests/reported_error.rs`.

use std::collections::BTreeSet;
use std::marker::PhantomData;

use dz_adapter_core::Adapter;
use dz_ingress_core::Input;
use prometheus::core::Collector;

use crate::context::AdapterContext;

/// What the constructor a venue registered may fail with.
///
/// Boxed rather than a type of this crate's own, because the failure belongs to
/// the venue: a credential file that is not there, an endpoint that does not
/// parse, an upstream section missing a key only the adapter knows the name of.
/// A closed enumeration here would have to anticipate all of them, and a venue
/// whose failure did not fit would be pushed into whichever variant was nearest.
pub type AdapterInitError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// How a caller's own error reports the two ways resolution fails.
///
/// One constructor per failure and nothing else, because that is the whole of
/// what [`AdapterRegistry::open`] can go wrong in. A publisher implements it on
/// the enumeration it already refuses startups with, so the message an operator
/// reads is the one that crate has always produced; anything with no error of
/// its own gets the blanket implementation on [`AdapterInitError`].
pub trait AdapterResolution {
    /// `[adapter] kind` named no adapter this binary registered.
    ///
    /// `registered` is [`AdapterRegistry::registered_list`], which is the
    /// operator's next action rather than decoration: the set of adapters in a
    /// binary is a property of the build, so being told only that a value was
    /// refused leaves *fix a spelling* and *redo a build* indistinguishable.
    fn unknown_kind(token: String, registered: String) -> Self;

    /// The adapter that `kind` named refused to be built.
    ///
    /// `kind` is the name the entry was registered under and not the token the
    /// document carried, so that a closure registered under several names
    /// reports against the one that reached it.
    fn adapter_init(kind: &'static str, source: AdapterInitError) -> Self;
}

/// The fallback for a caller with no startup error of its own.
///
/// It reports the same two failures, carrying the same values, in words of this
/// crate's own — and not in any runtime's. `dz-publisher-runtime`'s
/// `StartupError` spells an unregistered `kind` as `… this binary registered;
/// registered in this binary: <list>` where this spells it
/// `… this binary registered (<list>)`, and a constructor's refusal as *could
/// not be constructed* where this says *could not be built*. What the trait
/// fixes is the two failures and what each one names; the sentence around them
/// belongs to whoever reports it, which is the reason a runtime that refuses
/// startups through an enumeration should implement the trait on that instead
/// and keep its refusals in one type and one voice.
///
/// Both strings are read back **whole** in `tests/reported_error.rs`, because
/// [`AdapterResolution::unknown_kind`] takes the token and the registry as two
/// positional `String`s: nothing short of the finished message can tell a
/// transposition from the truth.
impl AdapterResolution for AdapterInitError {
    fn unknown_kind(token: String, registered: String) -> Self {
        format!(
            "`[adapter] kind = \"{token}\"` names no adapter this binary registered ({registered})"
        )
        .into()
    }

    fn adapter_init(kind: &'static str, source: AdapterInitError) -> Self {
        format!("the adapter `{kind}` could not be built: {source}").into()
    }
}

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
/// So `[ingress] kind` is still resolved by the runtime, against the closed set,
/// with the two distinguishable failures that resolution already reports; the
/// resolved [`Kind`](dz_ingress_core::Kind) is handed to the constructor in
/// [`AdapterContext::ingress_kind`]; and the constructor builds the transport
/// that kind names. What the runtime cannot check is that it built the
/// *matching* one, which is the honest cost of this and is stated rather than
/// hidden.
///
/// `#[non_exhaustive]`: this is the second breaking addition to this struct's
/// fields, and every field is public. Build one through [`Venue::single`] or
/// [`Venue::new`], and add to one through [`Venue::with_collectors`], so the
/// next field is additive rather than another break.
#[non_exhaustive]
pub struct Venue {
    /// The venue's mapping from its upstream's payloads onto normalized events.
    ///
    /// **One, however many sources there are.** A venue that reads the same
    /// book over a websocket and over a FIX session hands back one adapter that
    /// tells them apart by
    /// [`Payload::connection`](dz_adapter_core::Payload::connection), because
    /// merging two views of one book follows the venue's microstructure and is
    /// the same decision as the book state machine itself. The runtime drives
    /// the connections; it does not reconcile them.
    pub adapter: Box<dyn Adapter>,
    /// The transports those payloads arrive on, one per enabled `[[source]]`.
    ///
    /// See the type's own note, and [`Venue::single`] for the publisher with
    /// one upstream.
    pub sources: Vec<Box<dyn Input>>,
    /// The venue's own Prometheus collectors, for series the normative set does
    /// not describe.
    ///
    /// **They travel up, out of the constructor, because they cannot travel
    /// down into it.** A publisher's normative metric set is built from
    /// [`Adapter::message_types`], which needs the adapter, which this
    /// constructor is what returns — so there is no registry in existence at
    /// the moment a venue is asked to build itself, and an [`AdapterContext`]
    /// carrying one would be carrying a thing that does not yet exist. A venue
    /// therefore hands its collectors back and the runtime registers them once
    /// the normative set is there.
    ///
    /// **They are why this crate depends on `prometheus`.** The type is a
    /// Prometheus one by construction: the runtime hands these same objects to
    /// a Prometheus registry, so a trait object of our own could not be turned
    /// back into one, and a field the composition dropped would put one
    /// composition in two places. What is avoided is `dz-publisher-metrics`
    /// itself, which carries the exposition server a process that records has
    /// no port for. The client is re-exported as
    /// [`prometheus`](crate::prometheus) for the reason that crate re-exports
    /// it — see the crate documentation.
    ///
    /// **They go into a publisher's second registry, never the normative one.**
    /// That registry refuses any name beginning `dz_publisher_`, so a venue
    /// cannot shadow a series a subscriber's alert is written against — and the
    /// refusal is a startup failure rather than a warning, because a publisher
    /// that ran with a shadowed contract would be reporting one thing under the
    /// name of another.
    ///
    /// Empty is the ordinary case and states nothing: a venue with no
    /// microstructure worth counting is not a venue that failed to count it.
    ///
    /// [`Adapter::message_types`]: dz_adapter_core::Adapter::message_types
    pub collectors: Vec<Box<dyn Collector>>,
}

impl Venue {
    /// A venue with its adapter and its transports, named however many
    /// `[[source]]` entries the document resolved to.
    ///
    /// The general constructor: [`Venue::single`] is the one-upstream
    /// convenience built on top of it, and both leave `collectors` empty,
    /// which [`with_collectors`](Self::with_collectors) adds to.
    #[must_use]
    pub fn new(adapter: Box<dyn Adapter>, sources: Vec<Box<dyn Input>>) -> Self {
        Self {
            adapter,
            sources,
            collectors: Vec::new(),
        }
    }

    /// A venue with one upstream.
    ///
    /// The shape every publisher had before a feed could have several sources,
    /// and still the ordinary one. A document with no `[[source]]` block is
    /// exactly this.
    #[must_use]
    pub fn single(adapter: Box<dyn Adapter>, input: Box<dyn Input>) -> Self {
        Self::new(adapter, vec![input])
    }

    /// The same venue, with its own series to publish.
    ///
    /// A builder rather than a fourth argument to every constructor, because
    /// most venues have none and the ones that do are stating something extra
    /// rather than filling in something missing.
    #[must_use]
    pub fn with_collectors(mut self, collectors: Vec<Box<dyn Collector>>) -> Self {
        self.collectors = collectors;
        self
    }
}

/// Neither half is `Debug` and neither can be: `Adapter` and `Input` are
/// boundaries a venue implements, and requiring `Debug` of them would be this
/// crate asking every venue for something only its own error messages want.
impl std::fmt::Debug for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Venue").finish_non_exhaustive()
    }
}

/// What a venue registers: a name, and something that builds the integration.
///
/// Boxed rather than an `fn` pointer, so that a `main` may close over what it
/// has already parsed or opened. `Fn` rather than `FnOnce`, so that the
/// registry stays inspectable — [`AdapterRegistry::kinds`] and the error
/// message have to be able to name every entry whether or not one has been
/// used.
///
/// **Not generic over the reported error.** A constructor's own refusal is the
/// venue's, and it is an [`AdapterInitError`] whichever runtime is going to
/// carry it — so registering an adapter is one piece of code for both, and the
/// registry's type parameter never reaches a venue.
type Constructor = Box<dyn Fn(&AdapterContext<'_>) -> Result<Venue, AdapterInitError>>;

/// The adapters this binary contains, by the name `[adapter] kind` selects them
/// with.
///
/// Built in `main` and handed to the runtime. See the module documentation for
/// the whole argument; the three properties worth stating on the type are that
/// resolution has no default, that a name registered twice is a panic rather
/// than a silent shadowing, and that `E` is only how a refusal is *reported* —
/// nothing a venue writes mentions it.
pub struct AdapterRegistry<E = AdapterInitError> {
    /// Registration order, kept: it is the order a `main` reads in, which is
    /// what a reader comparing the file to the code needs. The error message
    /// sorts instead, so that what an operator is shown does not depend on the
    /// order somebody happened to write the calls in.
    entries: Vec<(&'static str, Constructor)>,
    /// `fn() -> E` rather than `E`, so that the parameter neither owns nor
    /// borrows anything: a registry holds constructors and no error, and the
    /// auto traits it gets should follow from the constructors alone.
    reported: PhantomData<fn() -> E>,
}

/// Hand-written rather than derived: `#[derive(Default)]` would demand
/// `E: Default`, and `E` is a type a refusal is built into rather than a value
/// a registry holds.
impl<E> Default for AdapterRegistry<E> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            reported: PhantomData,
        }
    }
}

impl<E> AdapterRegistry<E> {
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
}

impl<E: AdapterResolution> AdapterRegistry<E> {
    /// Resolve `[adapter] kind` and construct the integration it names.
    ///
    /// # Errors
    ///
    /// [`AdapterResolution::unknown_kind`] for a name this binary did not
    /// register, **naming every name it did**; and
    /// [`AdapterResolution::adapter_init`] carrying whatever the venue's
    /// constructor refused with.
    pub fn open(&self, cx: &AdapterContext<'_>) -> Result<Venue, E> {
        if let Some((name, constructor)) = self.entries.iter().find(|(name, _)| *name == cx.kind())
        {
            return constructor(cx).map_err(|source| E::adapter_init(name, source));
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
            return built_in.map_err(|source| E::adapter_init(kind, source));
        }
        Err(E::unknown_kind(
            cx.kind().to_owned(),
            self.registered_list(),
        ))
    }
}
