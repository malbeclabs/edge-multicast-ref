//! Normalized venue events to Edge wire messages: one implementation, shared
//! by every venue.
//!
//! [`dz_adapter_core`] is the boundary a venue implements, and it is defined so
//! that nothing a feed specification already decided can cross it. This is the
//! layer that makes the other half of that statement true — the one that
//! decides all of it, once.
//!
//! What is decided here rather than by a venue:
//!
//! - **The instrument's identity.** An adapter carries a dense handle; the
//!   `Instrument ID` on the wire comes from [`InstrumentTable`], which the
//!   reference-data owner populates. A handle that resolves to nothing is
//!   refused where the refusal can be counted, rather than resolving to
//!   whatever instrument now occupies that slot.
//! - **Fixed-point scaling.** One conversion, exact or refused, never rounded,
//!   with the three failure modes kept apart because each is a different
//!   operator action. See [`scale`].
//! - **`Update Flags`.** Derived from the pair of sides, where the *updated*
//!   and *gone* bits of a side are mutually exclusive. See
//!   [`Lowering::lower_quote`].
//! - **Trade qualifiers and the aggressor byte**, from the boundary's booleans
//!   and its three-case enum, so a bit nobody defined has no way to be set.
//!
//! # Why this is not part of the runtime
//!
//! The recorder answers *did the publisher publish what the venue said?* by
//! re-running a venue's adapter offline over an archive of what its upstream
//! actually sent, lowering the result with **this** code, and diffing against
//! the messages decoded from the multicast capture. That comparison is only
//! evidence if both sides lower identically, so the lowering must be linkable
//! without the runtime's egress socket, signal handling or async runtime.
//! Hence a crate of its own.
//!
//! # Scope
//!
//! Top-of-book (`0x03`, `0x04`) is here. Depth and its snapshot framing follow,
//! and market-by-order follows its codec crate.

#![forbid(unsafe_code)]

pub mod error;
pub mod instrument;
pub mod scale;
pub mod tob;

pub use error::LoweringError;
pub use instrument::{Instrument, InstrumentTable};
pub use scale::{price_at, qty_at};
pub use tob::Lowering;
