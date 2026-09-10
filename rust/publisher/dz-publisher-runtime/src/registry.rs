//! The composition seam's own module path here, kept.
//!
//! The types live in [`dz_venue_composition::registry`] now, and that module's
//! documentation is where the whole argument is — why a registry and not a
//! match, why an unregistered `kind` is a startup error naming the registry,
//! and what a venue's one registration line looks like. What is here is the
//! *path*, because an out-of-tree `main` that wrote
//! `use dz_publisher_runtime::registry::AdapterRegistry` is the `main` this
//! move promised not to change.
//!
//! # Why the path is not a module re-export
//!
//! `pub use dz_venue_composition::registry;` would compile and would not keep
//! that promise. [`dz_venue_composition::AdapterRegistry`] is generic over the
//! error a refusal is reported in and defaults to the boxed
//! [`AdapterInitError`], so `registry::AdapterRegistry` reached through a
//! module re-export names the *defaulted* type — a different type from the one
//! this crate's callers hold,
//! and one whose `open` hands back a boxed error where a publisher's `?`
//! expects a [`StartupError`](crate::StartupError). The name would resolve and
//! the code around it would not build.
//!
//! So [`AdapterRegistry`] here is the alias the crate root carries, at this
//! crate's own startup error, and the three names this module always exported —
//! [`AdapterRegistry`], [`AdapterContext`] and [`Venue`] — resolve to what they
//! always resolved to.

pub use dz_venue_composition::registry::{AdapterInitError, AdapterResolution, Venue};
pub use dz_venue_composition::AdapterContext;

/// The adapters this binary contains, reporting a refusal as a
/// [`StartupError`](crate::StartupError).
///
/// The same alias as [`crate::AdapterRegistry`], which is where the choice of
/// parameter is argued. A venue's `main` names one or the other and never the
/// parameter.
pub type AdapterRegistry = crate::AdapterRegistry;
