//! Shared wire primitives for the DoubleZero Edge feed family.
//!
//! Venue-agnostic, zero-I/O, zero-async. Every feed in the family uses the
//! 24-byte datagram header and 4-byte message header defined here.

#![forbid(unsafe_code)]

pub mod constants;
pub mod datagram;
pub mod error;
pub mod message;

pub use constants::*;
pub use datagram::{DatagramBuilder, DatagramHeader};
pub use error::DecodeError;
pub use message::AppMessage;
