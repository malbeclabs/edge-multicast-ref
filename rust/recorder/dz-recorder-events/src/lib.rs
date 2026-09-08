//! Market data rows, derived from an archive.
//!
//! The recorder decodes nothing while recording. This reads objects that are
//! already written, in a process that can be turned off, run late, or run twice
//! over the same object — which is the property that lets a derivation this
//! expensive exist at all.
//!
//! What is here so far is the reference data the rest of the derivation joins
//! against, the fold that turns an object into rows, the book that spans
//! objects, and [`sizing`] — the measurement that says what enabling any of it
//! for a given feed will cost. See
//! `docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md`.

pub mod book;
pub mod derive;
pub mod instruments;
pub mod sizing;

pub use book::{state_key, Book, BookRefused, Certainty, Change, Side, Top};
pub use derive::{derive_events, DerivedEvents, EventInput, Refused};
pub use instruments::{At, Channel, InstrumentTable, Observed, Statement};
pub use sizing::{FeedSizing, Incomplete, Sizing};
