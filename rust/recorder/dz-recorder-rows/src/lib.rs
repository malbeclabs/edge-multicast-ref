//! The rows an archive derives into, and the derivation that produces them.
//!
//! Four grains — [`Datagram`], [`SegmentCoverage`], [`SequenceGap`],
//! [`ConformanceFinding`] — plus [`Era`], which is where an era's *opening* is
//! recorded so that the monotonic index is a rank over openings rather than a
//! number written into the largest table. Field names are the column names,
//! exactly, and `tests/column_names.rs` holds every one of them against a
//! literal so that a rename cannot pass.
//!
//! # Nothing here knows what a column store is
//!
//! Derivation is pure and sink-agnostic: it reads a
//! [`Source`](dz_recorder_core::Source), which is the same trait a live capture
//! presents, and hands back a [`RowBatch`]. [`RowSink`] is the one seam a writer
//! implements — [`FileSink`] writes newline-delimited JSON and is what makes the
//! golden tests and `--dry-run` possible, and the column store is the other
//! implementation, in a crate of its own. That split is what lets the derivation
//! be exercised in CI against a synthetic publisher with no socket, no
//! privileges and no server.
//!
//! # One batch, one unit of idempotence
//!
//! [`RowSink::write_batch`] takes every grain at once rather than one method per
//! grain. Reprocessing is idempotent on `(object key, sha256)`, and an object
//! whose datagram rows landed while its gap rows did not is an object that reads
//! as a clean feed: partial credit is how a gap becomes invisible. The batch is
//! the unit that either lands or does not.
//!
//! # What is deliberately not decided here
//!
//! `publisher` is a verdict this crate never writes. It requires a datagram
//! absent from *every* site with no recorder overflow anywhere, which is a join
//! across sites that one object cannot answer; a row is written with
//! [`Verdict::Unverifiable`] and `seen_elsewhere` absent, and a later pass over
//! the rows upgrades it. Evidence arriving late upgrades a verdict; its absence
//! never blocks one.
//!
//! Nothing here decodes a payload. The 24-byte datagram header is read through
//! [`DatagramHeader::peek`](dz_edge_core::DatagramHeader::peek), which judges
//! nothing but the buffer's length: `decode` refuses an unsupported schema
//! version and an out-of-range declared length, and refusing the datagram that
//! carries the sequence number whose absence is the finding manufactures a gap
//! out of a datagram we hold.
#![forbid(unsafe_code)]

pub mod derive;
pub mod file;
pub mod rows;
pub mod sink;

pub use derive::{
    derive, derive_object, DeriveError, DeriveInput, Derived, InstanceReset, SegmentTrailer,
};
pub use file::FileSink;
pub use rows::{
    ConformanceFinding, Datagram, DropScope, Era, FindingVerdict, Grain, Nanos, PortRoleLabel,
    RecvTsKindLabel, RoleJoinRow, RowBatch, SegmentCoverage, SequenceGap, Verdict,
};
pub use sink::{RowSink, RowSinkError, Written};
