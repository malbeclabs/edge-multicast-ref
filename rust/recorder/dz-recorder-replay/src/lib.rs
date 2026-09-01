//! An archive, read back as a [`Source`](dz_recorder_core::Source).
//!
//! This is the other half of the symmetry the design rests on: replay must
//! yield the identical bytes, timestamps, source addresses, port roles and drop
//! deltas that were recorded. A truncated segment replays what survived and
//! says so, because a recorder killed mid-write leaves a partial block and
//! returning an error for the whole file would discard every datagram before
//! the tear.
//!
//! Nothing here decodes a datagram. The payload comes back verbatim and the
//! header fields the archive's own metadata needed are read as bare integers at
//! fixed offsets, so an archive of traffic this build does not understand
//! replays exactly as well as an archive of traffic it does.
//!
//! [`synthetic`] is the publisher the tests share: a known stream, written
//! straight into a `Sink`, with no socket and no privileges, so the whole record
//! path is exercised in CI.
#![forbid(unsafe_code)]

pub mod owned;
pub mod source;
pub mod synthetic;

pub use owned::OwnedDatagram;
pub use source::{ArchiveSource, LinkHeaderProvenance, PortRoles, Termination};
pub use synthetic::{Fault, StarvationWindow, SyntheticPublisher};
