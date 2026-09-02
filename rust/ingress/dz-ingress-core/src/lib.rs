//! The transport half of the venue boundary, and the half that waits.
//!
//! [`dz_adapter_core`] is the half that decides what an upstream payload
//! *means*. It is synchronous, does no I/O, and names no transport, because
//! that is what lets a venue's mapping be re-run offline over an archive and
//! tested in CI against a committed payload. This crate is everything that
//! statement leaves out: the socket, the connect, the reconnect, the backoff,
//! the rate limit, and the guard that notices an upstream which is still
//! answering but has stopped saying anything.
//!
//! # Why this is a second crate and not two traits in one
//!
//! The two existing publishers' upstreams have nothing in common. One connects
//! out to a session-oriented API; the other tails a local process's output.
//! `Adapter` has to fit both, so it cannot assume a connection, a subscription
//! or a reconnect — and `Input`, which is nothing but those three, cannot live
//! beside it. A venue whose upstream is a directory would otherwise inherit a
//! TLS stack, an async runtime and a websocket client to be handed bytes.
//!
//! The same argument then applies to *this* crate, one level down, and it is
//! why [`Input`] is a trait here rather than a websocket in here:
//!
//! - **No async runtime.** Nothing in this crate names one. Waiting goes
//!   through [`Clock`], which is injected. A venue that links this crate and
//!   its own reader is not pinned to our tokio minor version, and — the reason
//!   that matters more — the driver's backoff sequence becomes a value a test
//!   can assert instead of a wait a test has to sit through.
//! - **No metrics client.** The normative `dz_publisher_ingress_*` families are
//!   recorded from here, but through [`IngressObserver`], which the runtime
//!   implements over its Prometheus registry. A venue must not inherit a
//!   Prometheus client to be told a socket closed, and the metric name set is
//!   closed: the observer has one method per family, so a series cannot be
//!   invented here and cannot be silently dropped there.
//!
//! # The piece that makes the other half possible
//!
//! [`Driver`] is where the two halves meet. It owns every await; the adapter
//! owns every decision. Concretely, it is the reason three of the adapter's
//! methods can have the signatures they do:
//!
//! - [`Adapter::on_connected`](dz_adapter_core::Adapter::on_connected) writes
//!   into a synchronous [`UpstreamSink`](dz_adapter_core::UpstreamSink) because
//!   the driver buffers what it wrote and sends it afterwards. The adapter says
//!   *what*; the driver owns *when*, and owns the reconnect that makes it
//!   happen again.
//! - It is called on **every** successful connect. A subscription that was
//!   silently lost — the socket alive, the venue's session gone — comes back
//!   only because something re-issues it, and that something is here.
//! - [`Adapter::on_payload`](dz_adapter_core::Adapter::on_payload) can be a
//!   pure function of its bytes because the bytes arrive from here, already
//!   stamped, already counted.
//! - [`EventSink::payload_scope`](dz_adapter_core::EventSink::payload_scope) is
//!   supplied from here, so a runtime can measure from a payload's arrival
//!   without an adapter passing its payload through to every event it emits.
//!   The driver holds the stamp; the adapter is never asked for it, and — since
//!   the wrapper it writes into does not forward that report — cannot state one
//!   of its own.
//!
//! # Vocabulary
//!
//! The wire's unit is a datagram and it does not appear in this crate: nothing
//! here has reached the encoder yet. What a transport delivers is a *payload*,
//! which is the boundary's own word for it.

#![forbid(unsafe_code)]

pub mod backoff;
pub mod clock;
pub mod config;
pub mod driver;
pub mod error;
pub mod input;
pub mod kind;
pub mod limit;
pub mod observer;

pub use backoff::{Backoff, BackoffPolicy};
#[cfg(feature = "tokio")]
pub use clock::TokioClock;
pub use clock::{BoxFuture, Clock};
pub use config::{IngressConfig, Policy};
pub use driver::{Driver, UpstreamQueue};
pub use error::{ConfigError, IngressError};
pub use input::{Input, Received, StampSource, UpstreamMessage};
pub use kind::Kind;
pub use limit::RateLimiter;
pub use observer::IngressObserver;
