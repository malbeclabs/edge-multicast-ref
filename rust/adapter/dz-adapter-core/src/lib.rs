//! The boundary a venue implements, and the only crate its repository compiles
//! against.
//!
//! A venue owns two things: its upstream protocol, and its own book state
//! machine. It owns nothing else. Everything a feed specification already
//! decided — the `Instrument ID`, the `Source ID`, the `Channel ID`, the
//! sequence numbers, the fixed-point scaling, the `Update Flags` byte, the
//! `Action`, the datagram and its 1,232-byte cap — belongs to the crates above
//! this one, and **none of it is expressible through any type here**. Not as a
//! convention a venue is asked to observe: there is no parameter to pass one
//! through.
//!
//! That is the whole design, and it comes from what the existing publishers
//! did. Every defect a fleet-wide audit found was a publisher re-deciding
//! something a specification had already decided, and the ones worth naming are
//! the ones that were bytes a venue was still allowed to author.
//!
//! **A level encoder numbered the `Action` table from `New`**, so every removal
//! went out as a `Change` carrying zero. That one reached live traffic; its
//! publisher's own design notes count it as one of two bugs that landed, and
//! both landed from the same cause — a wire value table transcribed wrongly and
//! then checked only against itself. It is fixed, and the fix was structural: a
//! test that transcribes the specification's tables as literals, which is the
//! same technique as `tests/wire_vocabularies.rs` here.
//!
//! **A scaling failure became a price of zero on the wire.** The other
//! publisher's live market-data path converts the venue's decimal through `f64`
//! and `.round()`, and takes the failure as `.unwrap_or(0)` — while the same
//! repository holds an exact, string-only, exact-or-refuse conversion that the
//! path does not call. Zero is an in-range price, and the same publisher's
//! quote sets the *updated* flag for a side it has a level for, so a conversion
//! that fails publishes a real-looking bid at nothing.
//!
//! Both are self-consistent, so both are invisible to any test that encodes and
//! then decodes. Neither is reachable from here: there is no `Action` to
//! author, and no integer at the instrument's exponent to arrive at.
//!
//! # Three properties, and what each one buys
//!
//! **No dependency but `thiserror`.** A venue inheriting our async runtime's
//! minor version, or our Prometheus client's, is a version conflict we caused.
//! The transport half of the boundary is async and inherently carries one, so
//! it lives in a different crate and a venue that does not need it does not
//! link it. `tests/dependencies.rs` fails the moment a second entry appears in
//! `[dependencies]`.
//!
//! **[`Adapter::on_payload`] is synchronous, does no I/O, and allocates
//! nothing.** This is not an ergonomic preference. It makes an adapter a pure
//! function of its input bytes and its own state, which is what lets the same
//! adapter be re-run offline over an archive of what the upstream actually
//! sent, and its output diffed against what was captured on the wire. An
//! `async fn` on this trait would pin every venue to one runtime version and
//! make that comparison impossible; both costs would be paid for nothing,
//! because the asynchronous work is the transport's.
//!
//! **Everything borrows.** An adapter reads out of the receive buffer and
//! writes into the encode buffer, and owns nothing in between.
//!
//! # What an adapter looks like
//!
//! ```
//! use dz_adapter_core::{Adapter, EventSink, ListingSink, ParseError, Payload};
//!
//! struct Quiet;
//!
//! impl Adapter for Quiet {
//!     fn message_types(&self) -> &[&'static str] {
//!         &["heartbeat"]
//!     }
//!
//!     fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}
//!
//!     fn on_payload(
//!         &mut self,
//!         payload: &Payload<'_>,
//!         out: &mut dyn EventSink,
//!     ) -> Result<(), ParseError> {
//!         if payload.bytes.is_empty() {
//!             return Err(ParseError::truncated("empty payload"));
//!         }
//!         out.upstream_message("heartbeat");
//!         Ok(())
//!     }
//! }
//! ```
//!
//! Nothing else is imported, because there is nothing else to import.

#![forbid(unsafe_code)]

pub mod adapter;
pub mod error;
pub mod event;
pub mod instrument;
pub mod payload;
pub mod scalar;
pub mod sink;
pub mod timestamp;

pub use adapter::Adapter;
pub use error::{AdapterError, ParseError};
pub use event::{Aggressor, ClearScope, Desync, Event, Presence, Side, SideUpdate, TradeFlags};
pub use instrument::{
    AssetClass, InstrumentRef, InstrumentSpec, MarketModel, PriceBound, SettleType,
};
pub use payload::{ConnectionId, DisconnectReason, Payload};
pub use scalar::Scalar;
pub use sink::{EventSink, ListingSink, SnapshotSink, UpstreamSink};
pub use timestamp::VenueTimestampKind;
