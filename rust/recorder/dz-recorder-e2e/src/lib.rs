//! Tests only.
//!
//! The publisher side and the recorder side are separate crates that have never
//! met in one process: the encoder builds datagrams, the recorder keeps them,
//! and every test so far has exercised one side against a fixture of the other.
//! This crate is where they meet, so that a disagreement between a real encoder
//! and a real archive is a test failure rather than a discovery in a dashboard.
//!
//! It carries no library code on purpose — a crate that both sides depend on
//! would be a place for a shared assumption to hide, which is the very thing
//! these tests exist to rule out.
#![forbid(unsafe_code)]
