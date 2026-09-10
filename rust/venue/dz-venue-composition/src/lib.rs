//! How a venue is composed, so that two runtimes compose one the same way.
//!
//! A venue's integration is three values and one act. [`AdapterRegistry`] is
//! the set of adapters a binary contains, which only the `main` that linked
//! them knows. [`AdapterContext`] is what one of their constructors is given.
//! [`Venue`] is what it hands back: the adapter, the transports it reads from,
//! and any series of its own. The act is [`AdapterRegistry::open`], which
//! resolves `[adapter] kind` against the set with no default and no fallback.
//!
//! # Why this is not in the publisher's crate
//!
//! Because a publisher is not the only thing that composes a venue. A process
//! that captures the wire and records what an adapter derived from it needs
//! exactly these three values and none of the rest of a publisher — and while
//! they lived in `dz-publisher-runtime`, reaching them meant linking the
//! egress, the transmitters, the era store and the reference-data registry **in
//! order to publish nothing.** That dependency shape has been removed from this
//! tree once already.
//!
//! It is not either boundary crate either. `dz-adapter-core` is the trait a
//! venue implements and `dz-ingress-core` is the transport half of the same
//! boundary; a boundary crate that gained a registry would be a boundary crate
//! with a composition in it, and every venue repository would inherit the
//! composition in order to implement the trait.
//!
//! # What a venue writes, once
//!
//! ```no_run
//! # use dz_venue_composition::{AdapterContext, AdapterRegistry, Venue};
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
//! pub fn register<E>(registry: AdapterRegistry<E>) -> AdapterRegistry<E> {
//!     registry.with("a-venue", |cx| {
//!         Ok(Venue::single(
//!             Box::new(VenueAdapter::new(cx)?),
//!             venue_input(cx)?,
//!         ))
//!     })
//! }
//! ```
//!
//! `E` is how the runtime that ends up holding the registry reports a refusal —
//! see [`AdapterResolution`]. A publisher aliases it to its own startup error,
//! so `dz_publisher_runtime::AdapterRegistry` is spelled without a parameter and
//! a `main` written before this crate existed is unchanged.
//!
//! # What is here that a first reading would not expect, and why
//!
//! - **The configuration types the context hands out.** [`ReplayConfig`],
//!   [`Source`], [`SourceRole`] and [`FeedSpec`] are what
//!   [`AdapterContext::replay`], [`AdapterContext::sources`] and
//!   [`AdapterContext::feeds`] return, and an accessor is the boundary. A type
//!   a venue reads through one cannot live above the crate the venue links.
//!   What did *not* come is everything the context does not expose: the
//!   document, the sections that parse it, and the startup error.
//! - **The built-in record adapter**, [`builtin`], because
//!   [`AdapterRegistry::open`] resolves it and because it is the one adapter
//!   that is nobody's venue code — `dz-adapter-uds` is `dz-adapter-core` and
//!   `thiserror`.
//! - **A Prometheus client.** [`Venue::collectors`] is a Prometheus type by
//!   construction; see the field. What is avoided is `dz-publisher-metrics`,
//!   which carries the exposition server. The client is re-exported here as
//!   [`prometheus`] for the reason that crate re-exports it: a consumer whose
//!   manifest resolved a different major would otherwise hit the opaque
//!   `expected Box<dyn Collector>, found Box<dyn Collector>`.
//!
//! # What is deliberately not here
//!
//! The egress and the transmitters, the era store, and the reference-data
//! registry. That is the whole reason this crate exists, and it is asserted
//! against `cargo metadata`'s resolved graph in `tests/dependencies.rs` rather
//! than left to a reading of the manifest — a dependency shape is not
//! observable from behaviour, so the tempting non-move (leave the three types
//! in `dz-publisher-runtime`, re-export them from here) passes every
//! behavioural test in the workspace and fails exactly that one.
//!
//! # Vocabulary
//!
//! `venue` for the external exchange or market operator, which is what a
//! [`Venue`] value is one venue's integration with. `channel` for the
//! `Channel ID` shard and nothing else — and no `Channel ID` reaches an
//! [`AdapterContext`], which is the point of the boundary.

#![forbid(unsafe_code)]

pub mod builtin;
pub mod config;
pub mod context;
pub mod registry;

/// The metrics client [`Venue::collectors`] is typed in, re-exported so a
/// consumer links the copy this crate does.
///
/// The same re-export `dz-publisher-metrics` carries, and for the same reason:
/// one 0.14.x resolves for the whole graph, so a `Box<dyn Collector>` built
/// against either is the same type. A consumer whose manifest resolved a
/// different major without this would meet `expected Box<dyn Collector>, found
/// Box<dyn Collector>` and nothing naming the cause.
pub use prometheus;

pub use builtin::BUILTIN_KINDS;
pub use config::{
    FeedSpec, ReplayConfig, Source, SourceRole, UnknownSourceRole, UnsupportedFeedSpec,
};
pub use context::{AdapterContext, AdapterSection};
pub use registry::{AdapterInitError, AdapterRegistry, AdapterResolution, Venue};
