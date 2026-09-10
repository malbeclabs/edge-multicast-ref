//! Market data rows, derived from recorded datagrams.
//!
//! The recorder decodes nothing while recording. The derivation reads what is
//! already written, in a process that can be turned off, run late, or run twice
//! over the same input — which is the property that lets a derivation this
//! expensive exist at all.
//!
//! What is here is the reference data the rest of the derivation joins against,
//! the fold that turns recorded datagrams into rows, the book, and [`sizing`] —
//! the measurement that says what enabling any of it for a given feed will
//! cost.
//!
//! There are two entry points and the difference is only where the state lives.
//! [`derive_events`] builds it, folds one object and ends it, which is what an
//! object is. [`derive_events_into`] is handed a [`Derivation`] that outlives
//! the call, which is what a caller cutting a live feed into windows needs: the
//! reference data, the book and the snapshot attribution have to cross the cut
//! or every window starts as a fresh recorder. See
//! `docs/superpowers/specs/2026-09-05-recorder-market-data-rows-design.md` and
//! `docs/superpowers/specs/2026-09-10-recorder-derivation-state-design.md`.

pub mod book;
pub mod derive;
pub mod instruments;
pub mod sizing;

pub use book::{state_key, Book, BookRefused, Certainty, Change, Side, Top};
pub use derive::{
    derive_events, derive_events_into, Derivation, DerivedEvents, EventInput, Refused,
};
pub use instruments::{At, Channel, InstrumentTable, Observed, Statement};
pub use sizing::{FeedSizing, Incomplete, Sizing};
