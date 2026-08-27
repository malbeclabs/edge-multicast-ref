//! Shared wire primitives for the DoubleZero Edge feed family.
//!
//! Venue-agnostic, zero-I/O, zero-async. Every feed in the family uses the
//! 24-byte datagram header and 4-byte message header defined here.

#![forbid(unsafe_code)]

pub mod constants;
pub mod datagram;
pub mod end_of_session;
pub mod error;
pub mod heartbeat;
pub mod message;
pub mod port_role;

pub use constants::*;
pub use datagram::{DatagramBuilder, DatagramHeader};
pub use end_of_session::EndOfSession;
pub use error::DecodeError;
pub use heartbeat::Heartbeat;
pub use message::AppMessage;
pub use port_role::PortRole;
