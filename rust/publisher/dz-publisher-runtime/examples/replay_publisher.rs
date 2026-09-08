//! `run()` end to end: the real entry point, offline.
//!
//! `examples/loopback_publisher.rs` composed a publisher by hand, which proved
//! the send path but left [`run`](dz_publisher_runtime::run) — the one function
//! a venue's `main` actually calls — with no live exercise at all. This closes
//! that: a real config document, the real registry, a real adapter reading real
//! recorded bytes, the real lowering, real sockets, and the real teardown.
//!
//! The adapter is the **built-in** one — `[adapter] kind = "uds"`, resolved by
//! [`dz_publisher_runtime::builtin`] rather than by anything here — so the
//! record encoding, the listing section and the built-in's own resolution are
//! all exercised. That is why this file is three lines long: a `main` that is
//! only a registry is the whole point of the design, and a venue's differs from
//! it by the one call this one does not need to make.
//!
//! ```text
//! cargo run -p dz-publisher-runtime --example replay_publisher -- <config.toml>
//! ```
//!
//! `examples/replay.sh` writes the config and the payloads, runs this, and
//! reads the other end with the repository's Go subscriber.
//!
//! # What this example deliberately does not show
//!
//! A venue's own adapter and its own transport, which is what
//! `AdapterRegistry::with` is for. Registering one here would mean writing a
//! venue, and the shape of that call is in [`dz_publisher_runtime::registry`]'s
//! own documentation where it can be read without a fixture directory.

use std::process::ExitCode;

use dz_publisher_runtime::AdapterRegistry;

/// A venue's `main`, minus the venue.
///
/// The registry is empty and that is not a mistake: `[adapter] kind = "uds"`
/// names the built-in record adapter, and an empty registry is what proves the
/// built-in resolves without a venue registering anything. A `kind` naming
/// neither is still a startup error listing both sets.
fn main() -> ExitCode {
    dz_publisher_runtime::run(AdapterRegistry::new())
}
