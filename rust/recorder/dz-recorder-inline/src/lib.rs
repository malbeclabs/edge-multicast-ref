//! Inline mode: capture, derive and load in one process, keeping no datagrams.
//!
//! Design:
//! [`2026-09-08-recorder-inline-mode-design.md`](../../../docs/superpowers/specs/2026-09-08-recorder-inline-mode-design.md).
//!
//! # The other arrangement, and what it gives up
//!
//! Archive mode is unchanged, and is selected by the two `[archive]`
//! directories it has always required: `dz-recorder` writes hashed, manifested
//! objects and `dz-recorder-load` derives rows from them. This crate is the
//! other arrangement, selected by the file `--inline-config` names — one
//! process that derives rows from the live capture and keeps none of the
//! datagrams behind them.
//!
//! **That gives something up, and the rows say so.** Every row this path
//! produces carries [`Derivation::Live`](dz_recorder_rows::Derivation::Live):
//! nothing verified those bytes and nothing can derive them again, so a rule
//! written next month has nothing to run against and a derivation defect found
//! later can be stopped but not corrected.
//!
//! Neither arrangement is a default, and that is because of how a default
//! fails either way round. A host that meant this mode and said nothing would
//! get a running recorder writing objects nobody derived — an empty table,
//! indistinguishable from a feed nobody published on. A host that meant archive
//! mode and said nothing would be refused, but for the wrong reason: told about
//! a spool directory it never wanted. So silence is neither arrangement. Each is
//! named by what it cannot run without, both key sets are required with no
//! default, and a configuration stating both or neither is refused. The
//! refusals are what hold it, and the binary owns them.
//!
//! **This crate derives the five transport grains and no market data rows.**
//! `event`, `instrument` and `book_top` come from a codec walk, and nothing in
//! the record path decodes a datagram — a rule that is why the transport grains
//! are trustworthy, since a message a decoder would reject still carries the
//! sequence number whose absence is the finding. Archive mode runs that walk in
//! the loader, over bytes already stored and already hashed. Asking for market
//! data rows here is refused by name rather than ignored.
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
//! [`PendingLoss`]: dz_recorder_core::PendingLoss
//! [`derive`]: dz_recorder_rows::derive
//! [`Source`]: dz_recorder_core::Source
#![forbid(unsafe_code)]

pub mod derivation;
pub mod manifest;
pub mod metrics;
pub mod pipeline;
pub mod ring;
pub mod spool;
pub mod window;
