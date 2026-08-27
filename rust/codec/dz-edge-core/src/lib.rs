//! Shared wire primitives for the DoubleZero Edge feed family.
//!
//! Venue-agnostic, zero-I/O, zero-async. Every feed in the family uses the
//! 24-byte datagram header and 4-byte message header defined here.

#![forbid(unsafe_code)]

pub mod constants;

pub use constants::*;
