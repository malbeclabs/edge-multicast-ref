//! Reference data: who an instrument is, and how a subscriber comes to know it.
//!
//! This crate is the **reference-data owner** the boundary and the lowering
//! keep referring to. A venue offers an
//! [`InstrumentSpec`](dz_adapter_core::InstrumentSpec) through
//! [`ListingSink`](dz_adapter_core::ListingSink) and gets back a handle; on
//! this side of that call the selection policy is applied, an `Instrument ID`
//! is minted or recalled and persisted, an `InstrumentDefinition` is composed,
//! the lowering's [`InstrumentTable`](dz_publisher_lowering::InstrumentTable) is
//! populated, and `Manifest Seq` is advanced. None of that is expressible from
//! the venue's side, which is the whole design: an adapter that could name an
//! `Instrument ID` could name one that was never published.
//!
//! # The three things it exists to make unreachable
//!
//! **A published ID that resolves to nothing.** An `Instrument ID` is minted
//! only after a definition has composed, persisted before it is admitted, and
//! never re-issued — not even for a delisted instrument, since a subscriber
//! keys a book on one. [`Registry`] carries the full argument.
//!
//! **Two writers on one state directory.** The last flush wins and half the
//! published IDs resolve to nothing after the next restart. The claim is taken
//! before anything is read, the holder wins, and the newcomer does not start.
//! See [`Registry::open`] and [`FileStore`].
//!
//! **A definition cycle emitted as a burst.** The reference-data
//! specification's second rule forbids emitting the entire published set at
//! once, and one existing publisher does exactly that — its own comment calls
//! the emission a synchronized burst. Here the cycle is paced across
//! [`LAP_PERCENT`] of its period, capped per tick, with the definitions per
//! datagram derived from the sizes rather than configured. It is a property of
//! [`DefinitionPacer`] rather than a discipline asked of a caller, so there is
//! no loop anyone can write that gets the burst back.
//!
//! # No I/O except through a trait, and no clock except through a trait
//!
//! [`StateStore`] is the only way to a filesystem and [`Clock`] the only way to
//! a reading of time. So every property above is testable with no filesystem,
//! no privileges, no network and no sleeping: a claim that is already held, a
//! write that fails, a record that is damaged, and the pacing itself are all
//! stated rather than provoked. [`FileStore`] and [`SystemClock`] are the real
//! implementations; [`MemoryStore`] and [`ManualClock`] are the stated ones,
//! and the second pair is also what an offline re-run over an archive uses.
//!
//! # It constructs no metric
//!
//! The normative `dz_publisher_*` set is closed by the playbook, and inventing
//! a series is not this crate's to do. What it publishes instead is
//! [`Registry::counts`] and the accessors beside it, documented against the
//! family each one belongs to.

#![forbid(unsafe_code)]

pub mod clock;
pub mod definition;
pub mod error;
pub mod file_store;
pub mod pacer;
pub mod policy;
pub mod refusal;
pub mod registry;
pub mod state;
pub mod store;

pub use clock::{Clock, ManualClock, SystemClock};
pub use definition::{compose, symbol_field, Composition, Fits};
pub use error::RefdataError;
pub use file_store::FileStore;
pub use pacer::{definitions_per_datagram, CycleSchedule, DefinitionPacer, LAP_PERCENT};
pub use policy::{Phase, PolicyError, SelectionPolicy};
pub use refusal::Refusal;
pub use registry::{Counts, Registry, RegistryConfig};
pub use state::{Entry, RecordError, StateRecord, FIRST_INSTRUMENT_ID};
pub use store::{MemoryStore, StateError, StateStore};
