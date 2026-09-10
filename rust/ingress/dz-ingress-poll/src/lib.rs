//! A polled request/response transport, as an [`Input`](dz_ingress_core::Input).
//!
//! `[ingress] kind = "poll"` resolves to this. A venue whose instrument
//! catalogue lives behind a request/response endpoint declares a second
//! `[[source]]` for it, the catalogue arrives as payloads, and `on_payload`
//! decodes it like anything else. What it means is the adapter's; when to ask
//! again is this crate's; what to do about a failure is
//! [`dz_ingress_core::Driver`]'s.
//!
//! # What it does not hold, which is the whole point
//!
//! **No second poller, no second backoff, no second failure count.** Before
//! this transport existed, a venue whose catalogue was a request wrote a poller
//! task in its own binary, holding its own timer, its own retry delay and its
//! own count of consecutive failures, and handed results across a queue it also
//! owned. The driver already has all four for every other transport, so this
//! transport has none of them:
//!
//! - **The cadence.** [`PollInput::recv`](poll::PollInput) is only ever inside
//!   one receive the driver asked for, and it returns. What it holds is when
//!   the next request falls due — the same thing the websocket transport holds
//!   for its ping — and not a loop of its own.
//! - **The retry delay.** A failed request ends the connection. The driver's
//!   delay sequence paces the next attempt, and it is the sequence an operator
//!   already knows from `[ingress] reconnect_backoff_initial`.
//! - **The failure count.** `dz_publisher_ingress_reconnects_total{reason}`
//!   counts them, by reason, because the ending is reported rather than
//!   swallowed. A transport that retried internally would report a healthy
//!   connection while nothing arrived.
//!
//! # The cost this crate is: a second HTTP client in this workspace
//!
//! [`Input`](dz_ingress_core::Input) is async and this workspace's other HTTP
//! client is blocking. That one is `ureq`, in the column-store writer, where
//! blocking is right: it is a separate process doing batched writes, one at a
//! time, with nothing else to get on with. Calling it from inside an async
//! `Input` would block the runtime every driver in the publisher shares — and
//! the ingress crates deliberately start no runtime and spawn nothing, so
//! `spawn_blocking` is not available to them either.
//!
//! So this is **the family's first HTTP client and the workspace's second**,
//! and the cost is named here rather than hidden. The two are not duplicates to
//! be consolidated: they belong to different processes, different failure
//! models and different tiers, and the one thing that must not happen is either
//! migrating toward the other because they look alike in a manifest. A change
//! that makes the loader async to share this one, or this one blocking to share
//! that one, has to answer for the runtime it blocks or the process it starts.
//!
//! An async client rather than HTTP over `tokio` by hand, for the reason the
//! websocket crate gives for its protocol: chunked transfer encoding,
//! connection reuse, timeouts and certificate verification are a
//! security-relevant surface with no upside in owning.
//!
//! **Redirects are not one of them, and are not followed.** `hyper` does not
//! implement them — that belongs to a higher-level client, and this crate does
//! not add one. So a `301` or a `302` is a status the endpoint should not have
//! answered with: `Rejected` on the connect probe, `remote_close` on an
//! established connection, and the connection ends. A venue that has moved its
//! catalogue is therefore an endpoint to change in the document rather than one
//! this transport quietly follows — which is the answer worth having, since
//! following a redirect is how a request carrying a key in its query string
//! reaches a host nobody configured.
//!
//! # TLS is a feature, not a default
//!
//! `https` needs the `tls` feature, and a build without it **refuses an
//! `https` endpoint at configuration load**, naming the scheme and the feature
//! — the shape the column-store writer already uses, and for the same reason:
//! an operator who wrote `https` asked for the wire to be encrypted, and a
//! transport that quietly spoke plain HTTP instead would send a credential in
//! the clear having been told not to. Learning at load is the difference
//! between a startup failure and a publisher that starts and fails on every
//! request.
//!
//! With the feature on it is `rustls` with the compiled-in webpki trust anchors
//! and `ring` as the provider, **named in code rather than discovered** — see
//! [`HttpClient::tls_config`](client::HttpClient) for why that matters, which
//! is the same reason the websocket transport names its own. This crate's
//! `Cargo.toml` states which backends are deliberately excluded and why a
//! default must not be able to pull one in.
//!
//! # Vocabulary
//!
//! A response body is a **payload**. It is never a datagram: nothing here has
//! been encoded yet, and the wire's unit is nowhere near this crate. What is
//! polled is an **endpoint**. The cadence is an **interval** and not a cycle —
//! a cycle is one full pass over a set divided by the set's size, which is what
//! `definition_cycle` and `snapshot_cycle` are, and there is no set here: one
//! tick is one request.

#![forbid(unsafe_code)]

pub mod client;
pub mod config;
pub mod poll;

pub use client::{Answer, HttpClient, PollClient, Request, RequestFailure};
pub use config::{ConfigError, PollConfig};
pub use poll::PollInput;
