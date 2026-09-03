//! Re-running a venue's mapping over an archive, and diffing it against the
//! wire: *did the publisher publish what the venue said?*
//!
//! The recorder archives bytes off the multicast wire and decodes nothing in
//! the record path. Given a second archive — the raw upstream payloads, exactly
//! as the transport yielded them — this crate answers the question a
//! subscriber's complaint always turns into: **was the message never sent, or
//! sent and lost?** It re-runs [`Adapter::on_payload`] over exactly those
//! bytes, lowers the events with the publisher's own lowering, and joins the
//! result against the messages decoded from the multicast archive.
//!
//! It works only because the boundary was defined so that it could: `on_payload`
//! is synchronous, does no I/O, and is a pure function of its payload and the
//! adapter's own state. An `async fn next_event()` would have made all of this
//! impossible.
//!
//! # The three things that make the comparison well-defined
//!
//! Each is a requirement rather than an observation, and each is enforced here
//! rather than documented.
//!
//! - **The comparison is at message grain, never datagram grain.** Datagram
//!   batching is time-dependent; the messages inside one are not. [`WireCapture`]
//!   strips the framing on the wire side, and the re-lowered side never has any,
//!   so nothing a batching or pacing decision can move is in the compared tuple.
//!   That is what makes the fourth finding class produce no finding.
//! - **`Per-Instrument Seq` is the join key for depth, and it is deterministic.**
//!   The runtime stamps it, from a counter keyed on the instrument and reset with
//!   the era, so both copies carry the same value for the same upstream event and
//!   the diff is a *join* rather than a heuristic alignment. Nothing here aligns
//!   by proximity, by order or by time.
//! - **Reference data comes from the archive.** `InstrumentDefinition` and
//!   `ManifestSummary` are on the wire, so the capture already carries the
//!   `Instrument ID` and the exponents the re-lowering needs. There is no
//!   entry point on this crate that accepts an
//!   [`InstrumentTable`](dz_publisher_lowering::InstrumentTable): reconstructing
//!   it from live registry state would re-run today's mapping over yesterday's
//!   bytes and report nothing, so the API does not offer the option.
//!
//! # The four findings, and the one that is not one
//!
//! | Class | Meaning | Reported as |
//! |---|---|---|
//! | In the re-lowered stream, not on the wire | the publisher dropped it: a full queue, a guard, a crash window | [`Finding::ReLoweredNotOnWire`] |
//! | On the wire, not in the re-lowered stream | the publisher invented it, or reference data diverged | [`Finding::OnWireNotReLowered`] |
//! | Both, fields differ | a lowering or scaling defect, **named by field** | [`Finding::FieldsDiffer`] |
//! | Both, identical, different timing | framing and pacing only — the healthy case | **nothing**; counted as [`Summary::identical`] |
//!
//! The fourth is why this is usable at all. A tool that reported the healthy
//! case would report every archive, and an operator would learn to close it.
//!
//! # What it does not do
//!
//! It does not validate the adapter against the venue: both sides run the same
//! mapping, so an adapter reading the wrong upstream field is invisible here.
//! That is what golden fixtures of upstream payloads are for, and they belong in
//! the venue's own repository beside the adapter.
//!
//! Nor does it compare what the publisher's own cadence produced rather than the
//! venue's events: heartbeats, `EndOfSession`, the definition cycle, the manifest
//! cadence and the snapshot rotation are all timed by the runtime, and the
//! payload archive does not say when the runtime asked. Those messages are
//! counted in [`Skipped`] and excluded from the join rather than reported as
//! findings the archive cannot support.
//!
//! # I/O
//!
//! Both archives arrive behind a trait: the multicast side as
//! [`Source`](dz_recorder_core::Source), which is how every other offline tier
//! reads an archive, and the upstream side as [`PayloadArchive`]. Nothing here
//! opens a file, a socket or a device, so every test of it runs unprivileged.
//!
//! [`Adapter::on_payload`]: dz_adapter_core::Adapter::on_payload

#![forbid(unsafe_code)]

pub mod archive;
pub mod compare;
pub mod diff;
pub mod error;
pub mod finding;
pub mod join;
pub mod refdata;
pub mod relower;
pub mod wire;

pub use archive::{ArchivedPayload, PayloadArchive, PayloadLog};
pub use compare::{compare, compare_archives, key_overlap, RelowerReport, Summary};
pub use diff::FieldDiff;
pub use error::RelowerError;
pub use finding::{Caveat, Finding, Outcome};
pub use join::{JoinKey, TopOfBookTie};
pub use refdata::{ArchivedInstrument, ArchivedRefdata, MissingDefinition};
pub use relower::{relower, LoweredMessage, ParseFailure, ReLowered, ReLoweredProvenance, Refusal};
pub use wire::{MessageBody, Skipped, WireCapture, WireMessage, WireProvenance};
