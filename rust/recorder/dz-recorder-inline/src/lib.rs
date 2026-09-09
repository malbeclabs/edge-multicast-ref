//! Inline mode: capture, derive and load in one process, keeping no datagrams.
//!
//! Design:
//! [`2026-09-08-recorder-inline-mode-design.md`](../../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md).
//!
//! # The second arrangement, not a replacement
//!
//! Archive mode is the default and is unchanged: `dz-recorder` writes hashed,
//! manifested objects and `dz-recorder-load` derives rows from them. This crate
//! is the other arrangement — one process that derives rows from the live
//! capture and keeps none of the datagrams behind them.
//!
//! **That gives something up, and the rows say so.** Every row this path
//! produces carries [`Derivation::Live`](dz_recorder_rows::Derivation::Live):
//! nothing verified those bytes and nothing can derive them again, so a rule
//! written next month has nothing to run against and a derivation defect found
//! later can be stopped but not corrected.
//!
//! # Three stages, and the two rules that shape them
//!
//! ```text
//! capture ─► ring ─► derivation ─► spool ─► posting ─► ledger
//! ```
//!
//! **The capture path never blocks.** Inline mode adds two places it could. A
//! datagram that does not fit the ring is dropped and its loss charged to the
//! next one that gets through, through [`PendingLoss`]; a spool at its byte
//! budget evicts its oldest window and counts it. Neither ever applies
//! backpressure, because a recorder that waits on storage overflows its receive
//! queue and manufactures the very loss it exists to measure.
//!
//! **The derivation is called, never reimplemented.** [`derive`] is the same
//! function archive mode calls, over the same [`Source`] trait. What this crate
//! supplies is a `Source` over a live ring, a manifest for a window that has no
//! object, and somewhere to put the rows.
//!
//! [`PendingLoss`]: dz_recorder_capture::PendingLoss
//! [`derive`]: dz_recorder_rows::derive
//! [`Source`]: dz_recorder_core::Source
#![forbid(unsafe_code)]

pub mod manifest;
pub mod pipeline;
pub mod ring;
pub mod spool;
pub mod window;
