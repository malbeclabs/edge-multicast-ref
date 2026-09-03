//! A websocket transport, as an [`Input`].
//!
//! `[ingress] kind = "websocket"` resolves to this. Everything venue-specific
//! is elsewhere: this crate knows how to hold a websocket open, how to notice
//! that it has stopped being one, and how to say why in the four words the
//! reconnect metric counts by. What the bytes mean is the adapter's, and when to
//! reconnect is [`dz_ingress_core::Driver`]'s.
//!
//! # The two dependencies that are choices, and why these
//!
//! **`tokio-tungstenite` over `tungstenite`, `soketto` or a hand-rolled client.**
//! `tungstenite` is the protocol implementation this repository would end up
//! using either way — it is the one with the Autobahn conformance suite run
//! against it — and `tokio-tungstenite` is that same state machine behind a
//! `Stream`/`Sink`. The property that decided it is not ergonomics: the
//! partial-read state lives in the stream rather than in the future returned by
//! a read, so abandoning a read at a timeout cannot leave half a message
//! behind. A client where that were untrue could not be given a receive budget
//! at all, and the symptom of getting it wrong — one corrupt payload after a
//! busy period — is close to undiagnosable.
//!
//! **`tokio` over `async-std` or `smol`.** It is what the rest of this
//! repository's async surface will be, and a second runtime in one binary is
//! two thread pools and two timer wheels. Note where it is *not*: nothing in
//! this crate starts a runtime or spawns a task. Starting one is the binary's
//! decision, and a transport that spawned its own reader would put the receive
//! path behind a channel whose depth nobody chose.
//!
//! # TLS
//!
//! `rustls` with the compiled-in webpki trust anchors, and `ring` as the
//! provider. Three consequences, each of them the point:
//!
//! - No OpenSSL, so a venue building this crate needs no system TLS library and
//!   there is no build that differs by host because a distribution shipped a
//!   different one.
//! - The trust anchors are in the binary, so a host with an empty or stale CA
//!   bundle connects exactly like every other host. A publisher that cannot
//!   verify a venue's certificate on one machine out of a fleet is a failure
//!   that looks like the venue's.
//! - `ring` rather than the default `aws-lc-rs`, which wants cmake and a C
//!   compiler at build time.
//!
//! The provider is named in code, in [`WebSocketInput::tls_connector`], rather
//! than left to whichever one a feature happens to have installed
//! process-wide. `rustls` panics when it has to choose and cannot, and that
//! panic would arrive on the first `wss://` connect — the one moment no test
//! that can run without a network reaches.
//!
//! # Vocabulary
//!
//! A websocket message is a message. The wire's unit is a datagram and it is
//! nowhere near this crate: nothing here has been encoded yet.

#![forbid(unsafe_code)]

pub mod websocket;

pub use websocket::WebSocketInput;
