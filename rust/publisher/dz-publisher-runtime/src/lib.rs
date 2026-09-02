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
//! # What is wired, and what is a hole
//!
//! The top-of-book path is whole: a normalized event from a venue's adapter is
//! lowered through [`dz_publisher_lowering`], composed into a datagram by
//! [`dz_publisher_egress`], and reaches a
//! [`DatagramSink`](dz_publisher_egress::DatagramSink) numbered, in an era that
//! survived the restart, on the port role its specification allows, under the
//! cap. Heartbeats, the paced definition cycle, the manifest cadence, the two
//! guards and an ordered shutdown that ends with `EndOfSession` are all here.
//!
//! Four things are deliberately not, and each is a missing piece elsewhere
//! rather than an unfinished one here:
//!
//! - **The depth feeds cannot reach the wire.** `dz-publisher-lowering` lowers
//!   `LevelUpdate`, `BookClear` and the three snapshot messages correctly
//!   today. What none of them has is an
//!   [`EgressMessageType`](dz_publisher_metrics::EgressMessageType) to be
//!   counted under, and the metric name set is closed by a governing playbook —
//!   so this crate can neither invent a label nor push a message it has no
//!   label for. `[[feed]] spec = "market-by-price"` is therefore a startup
//!   error naming what this build can emit, and a depth event is counted and
//!   dropped *before* it is lowered, so that no `Per-Instrument Seq` is spent
//!   on a message that never left. See [`Publisher::unroutable`].
//! - **The snapshot has no cadence to be pulled on.** The design names
//!   `[[feed]] snapshot_port` and no snapshot interval, and inventing a key is
//!   the one thing this crate must not do. [`Publisher::snapshot`] frames one
//!   on demand and hands it back rather than sending it.
//! - **A lowering refusal has no series.** Five reasons stay distinguishable and
//!   none of them fits the closed set. See [`Refusals`] for why every candidate
//!   is worse than none, and for what a playbook addition would look like.
//! - **`[adapter.tee]` is parsed, defaults off, and is plumbed nowhere.** The
//!   framing it would write is the framing the offline comparison reads, and
//!   that framing does not exist yet. See [`TeeConfig`].
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

pub mod clock;
pub mod config;
mod duration;
pub mod error;
pub mod guard;
pub mod observer;
pub mod pipeline;
pub mod publisher;
pub mod registry;
pub mod run;

pub use clock::{Clock, ManualClock, SystemClock};
pub use config::{
    AdapterConfig, Config, Document, EgressSection, Feed, FeedSection, FeedSpec, MetricsSection,
    Refdata, RefdataSection, ReplayConfig, SelectionSection, TeeConfig,
};
pub use error::{AdapterInitError, StartupError};
pub use guard::{ConsistencyGuard, Exit, IdleGuard, Inconsistency, Upstream};
pub use observer::MetricsObserver;
pub use pipeline::FeedPipeline;
pub use publisher::{Publisher, Refusals, SnapshotError, Teardown, TeardownStep, LISTING_POLL};
pub use registry::{AdapterContext, AdapterRegistry, Venue};
pub use run::run;
