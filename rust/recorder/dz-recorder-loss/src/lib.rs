//! Sequence-loss detection: which sequence values nobody delivered, and whose
//! they are.
//!
//! This runs over a [`Source`](dz_recorder_core::Source), so the same code reads
//! a live capture and a replayed archive — which is the point of that symmetry.
//! It reads the 24-byte header and nothing else.
//!
//! **Loss is measured in sequence values, never in time.** A gap is a run of
//! sequence numbers nobody delivered and its size is how many of them there
//! were. At fifty datagrams a second a three-second gap is a hundred and fifty
//! missing, and on a channel that only heartbeats it is three, so a figure in
//! seconds says as much about how busy the feed was as about what was lost.
//! Timestamps here place a run against an incident; they never quantify it.
//!
//! # The live tier's rules, not a copy of them
//!
//! Continuity, reordering, duplication and the monotonic era ordinal are
//! [`SequenceTracker`](dz_recorder_core::SequenceTracker)'s, in
//! `dz-recorder-core`, and this crate drives that one implementation. The live
//! health tier drives the same one, so the two cannot reach different verdicts
//! about the same datagram.
//!
//! `dz-recorder-health` stays a dev-dependency and never a dependency: it links
//! a metrics registry and a Prometheus exposition into a recorder process, and
//! an offline loader has no business carrying either. That is why the shared
//! rules sit in core rather than being shared from there.
//!
//! What is this crate's own is the per-era delivered ranges: the tracker says
//! what a datagram meant on arrival, which is all a live tier can say, and the
//! ranges say which sequence values the archive does not hold once the late
//! arrivals are in. `tests/agreement.rs` drives one datagram stream through
//! both halves — through a real archive on the offline side — and holds the
//! numbers they build on top of the tracker against each other.
#![forbid(unsafe_code)]

pub mod deriver;
pub mod run;

pub use deriver::{
    DeriverLimits, EraCoverage, InstanceLoss, LossDeriver, LossError, LossReport, Unexplained,
};
pub use run::SequenceRun;
