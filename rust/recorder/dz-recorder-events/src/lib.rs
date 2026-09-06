//! Market data rows, derived from an archive.
//!
//! The recorder decodes nothing while recording. This reads objects that are
//! already written, in a process that can be turned off, run late, or run twice
//! over the same object — which is the property that lets a derivation this
//! expensive exist at all.
//!
//! What is here so far is the reference data the rest of the derivation joins
//! against. See
//! `docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md`.

pub mod derive;
pub mod instruments;

pub use derive::{derive_events, DerivedEvents, EventInput, Refused};
pub use instruments::{At, Channel, InstrumentTable, Observed, Statement};
