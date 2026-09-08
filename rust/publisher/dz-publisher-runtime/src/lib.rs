//! The publisher a venue links, and the one thing it cannot know: which
//! adapter.
//!
//! A venue repository owns `main`, and `main` is short. It says which adapters
//! this binary contains, and nothing else:
//!
//! ```no_run
//! # use dz_publisher_runtime::{AdapterRegistry, Venue};
//! fn main() -> std::process::ExitCode {
//!     dz_publisher_runtime::run(AdapterRegistry::new().with("a-venue", |_cx| {
//!         unimplemented!("the venue's adapter and the transport it reads from")
//!     }))
//! }
//! ```
//!
//! Everything else is here: the configuration document, the guards, the
//! signals, the metrics, the egress and the reference data. This crate is a
//! *library* rather than a service for that reason — Rust has no runtime
//! library loading, so the set of adapters a binary contains is decided by
//! whoever links it, and the only place that set is knowable is the `main` that
//! did.
//!
//! # The one refusal this crate exists for
//!
//! An `[adapter] kind` naming an adapter this binary did not register is a
//! **startup error naming every kind it did**. Never a default. Never a
//! fallback. Not the only registered one, not the first, and not an adapter
//! that publishes nothing while the process stays up looking healthy.
//!
//! The audit finding behind that: a publisher had a misspelled configuration
//! section parse cleanly, fall back to a default, and run the wrong transport
//! while its operator believed otherwise. So `deny_unknown_fields` is on every
//! table in this document that has a known key set — including `[adapter]` and
//! every table under it, and including the document's own root, which is what
//! makes a venue-specific key written at the top level a load error rather than
//! a key nobody reads.
//!
//! # What is wired
//!
//! Both feeds this workspace has a codec for, end to end. A normalized event
//! from a venue's adapter is lowered through [`dz_publisher_lowering`],
//! composed into a datagram by [`dz_publisher_egress`], and reaches a
//! [`DatagramSink`](dz_publisher_egress::DatagramSink) numbered, in an era that
//! survived the restart, on the port role its specification allows, under the
//! mandated cap:
//!
//! | Event | `top-of-book` | `market-by-price` |
//! |---|---|---|
//! | `Quote` | `0x03` mktdata | — |
//! | `Trade` | `0x04` mktdata | `0x04` mktdata |
//! | `Level` | — | `0x40` mktdata |
//! | `Clear` | — | `0x41` mktdata |
//! | a pulled snapshot | — | `0x20`/`0x42`/`0x22` snapshot |
//!
//! Plus heartbeats, the paced definition cycle, the manifest cadence, the two
//! guards, and an ordered shutdown that ends with `EndOfSession`.
//!
//! A publisher may emit **both** feeds from one process, which is what
//! `[[feed]]` being an array is for — and it is where the wire's
//! cross-specification policy for `0x04` stops being a doc comment. The trade
//! is lowered **once** and the same value is handed to both send paths, so the
//! two feeds do not carry two things that agree; they carry one thing.
//!
//! One registry serves every feed. `Instrument ID` identity is the one thing
//! there can only be one of, `Manifest Seq` describes the published set rather
//! than a channel, and the manifest's own redundant `Channel ID` is stamped by
//! the builder from the datagram that frames it — so one composed manifest is
//! truthful on every feed's refdata port. See [`Publisher::new`].
//!
//! # What is still a hole
//!
//! Two entries left this list. `dz_publisher_lowering_refusals_total{reason}`
//! and `dz_publisher_ingress_adapter_errors_total{reason}` now exist and every
//! refusal reaches one — as **proposed** additions to the normative set rather
//! than families the governing playbook already carries, which the metrics
//! crate keeps in a list of its own and says in each help text.
//!
//! Two things, and each is a missing piece elsewhere rather than an unfinished
//! one here:
//!
//! - **A lowering refusal has no series.** Five reasons stay distinguishable
//!   and none of them fits the closed set. See [`Refusals`] for why every
//!   candidate is worse than none, and for what a playbook addition would look
//!   like.
//! - **Two latency families cannot be observed.**
//!   `dz_publisher_recv_to_send_latency_seconds` and
//!   `dz_publisher_venue_to_recv_latency_seconds` both measure from a payload's
//!   arrival, and `EventSink` — the whole of what the composed publisher is
//!   handed — does not carry `Payload::recv_ts_ns`. The encode-duration family
//!   is observed instead.
//!
//! # A feed can have several sources
//!
//! `[[source]]` states one block per upstream — its name, its transport, and
//! whether it is the one that publishes. The runtime opens every enabled
//! source, drives each with its own [`Driver`](dz_ingress_core::Driver), and
//! hands every payload to **one** adapter, which tells them apart by
//! [`Payload::connection`](dz_adapter_core::Payload::connection).
//!
//! It does not merge them, and that is deliberate: which of two views of one
//! book is current, and when to fail over, follows the venue's microstructure —
//! the same argument that leaves the book state machine with the venue. See
//! [`SourceSection`] for the rule that makes the array checkable, and for what
//! [`SourceRole`] can and cannot enforce.
//!
//! Two entries that were on this list have closed, and both were closed by
//! evidence from the shipped publishers rather than by a decision here.
//!
//! - **A snapshot now has a cadence**, `[[feed]] snapshot_cycle`: one full pass
//!   over the published set, one instrument per derived tick. It was left out
//!   because the design names no key for it and inventing keys is how a
//!   configuration grows values nobody can set right — but both shipped
//!   publishers carry a periodic snapshot, both at five seconds, one of them
//!   under this name and with these semantics. Absent still means recovery
//!   snapshots and nothing else, which is what this runtime did before. See
//!   [`rotation`] and [`Publisher::periodic_snapshot`].
//! - **`[adapter.tee]` is plumbed**, as a second [`DatagramSink`](dz_publisher_egress::DatagramSink)
//!   per port role writing byte-identical copies to a Unix datagram socket.
//!   What it was waiting on was a framing, and the answer was that it needs
//!   none: `SOCK_DGRAM` preserves message boundaries, so one datagram in is one
//!   datagram out. See [`ReferenceStream`](dz_publisher_egress::ReferenceStream).
//!
//! Market-by-order is absent rather than a hole: `dz-edge-mbo` does not exist,
//! and the boundary's own event variants for it are absent for the same reason.
//!
//! # Nothing here needs a socket to be tested
//!
//! [`Publisher`] is composed from arguments and reads time through an injected
//! [`Clock`]: the reference-data registry arrives already holding its state
//! store, the send path arrives holding fan-outs whose members may be recording
//! sinks, and every cadence is a value a test states and reads back. [`run()`] is
//! the only function in this crate that opens a file, binds a socket, installs
//! a signal handler or starts a runtime, and it decides nothing the composed
//! publisher does not already decide.
//!
//! # Vocabulary
//!
//! `datagram`, never `frame`, for our own traffic. `era` for a `Reset Count`
//! generation. `channel` for the `Channel ID` shard and nothing else — the
//! three ports are *port roles*, spelled `mktdata`, `refdata` and `snapshot`.

#![forbid(unsafe_code)]

pub mod builtin;
pub mod clock;
pub mod config;
mod duration;
pub mod error;
pub mod guard;
pub mod observer;
pub mod pipeline;
pub mod publisher;
pub mod registry;
pub mod replay;
pub mod rotation;
pub mod run;

pub use builtin::BUILTIN_KINDS;
pub use clock::{Clock, ManualClock, SystemClock};
pub use config::{
    AdapterConfig, Config, Document, EgressSection, EmittedFeed, Feed, FeedSection, FeedSpec,
    MetricsSection, Refdata, RefdataSection, ReplayConfig, SelectionSection, Source, SourceRole,
    SourceSection, TeeConfig,
};
pub use error::{AdapterInitError, StartupError};
pub use guard::{ConsistencyGuard, Exit, IdleGuard, Inconsistency, Upstream};
pub use observer::MetricsObserver;
pub use pipeline::{DroppedSink, FeedPipeline, Port, Ports};
pub use publisher::{
    Feeds, Publisher, Refusals, SnapshotError, SnapshotRefusals, Teardown, TeardownStep,
    LISTING_POLL,
};
pub use registry::{AdapterContext, AdapterRegistry, Venue};
pub use replay::ReplayInput;
pub use rotation::SnapshotRotation;
pub use run::{check_sources, run};
