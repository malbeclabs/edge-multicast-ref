//! The loading half of a recorder host, as a library: the pass, the ledger and
//! the metrics.
//!
//! # Why this crate is a library as well as a binary
//!
//! There are two arrangements a recorder host can run, and they share this half.
//!
//! **Archive mode** is the binary beside this file: the recorder writes hashed,
//! manifested objects into a directory, and `dz-recorder-load` walks that
//! directory, derives rows and loads them. Two processes, one shared directory,
//! and nothing else between them.
//!
//! **Inline mode** is one process that captures a feed and derives its rows
//! directly, keeping no datagrams. It has no objects directory to walk, so it
//! does not want [`loader::Loader`] — but everything downstream of the
//! derivation is the same problem, and it was already solved here:
//!
//! - [`ledger`] — which unit's rows are in the store, keyed on the unit and its
//!   digest, carrying the trailer so a restart resumes with the certainty a
//!   continuous run had.
//! - [`loader::record_landed`] — the recording that happens when an insert is
//!   *acknowledged*, which is not when the sink accepted the rows. A sink that
//!   coalesces has taken rows it has not sent, so an entry written on acceptance
//!   marks a unit loaded whose rows a crash then loses.
//! - [`metrics`] — the `dz_loader_*` family, including the lag that this whole
//!   tier is gated on.
//!
//! A second copy of any of those would be a second answer to *is this loaded*,
//! and the two would disagree in exactly the case that matters: after a crash.
//!
//! # What stays in the binary
//!
//! The command line, the metrics endpoint and the build identity. Each is a
//! statement about *this* binary — inline mode serves its own endpoint on the
//! recorder's port and reports the recorder's build — so exporting them would
//! offer a caller the wrong one.
//!
//! [`config`] is here rather than there, and that is not the split this crate
//! first drew. `LoaderConfig` is the archive-mode binary's own file and inline
//! mode reads a different one, so on shape alone it belongs beside the command
//! line. But [`loader`] and [`metrics`] both reach into it for
//! [`MarketDataFeed`] and into [`market_data`] for the derivation and its
//! refusal kinds, and the pass cannot be a library while half of what it calls
//! is not. Splitting `MarketDataFeed` out of the configuration to restore the
//! tidier boundary would move a type away from the keys that give it meaning
//! for no gain a caller can see: a crate exporting a configuration nobody has
//! to read costs nothing.
//!
//! Nothing here changed behaviour when it moved. The binary's own test suite is
//! what says so.
#![forbid(unsafe_code)]

pub mod config;
pub mod ledger;
pub mod loader;
pub mod market_data;
pub mod metrics;

pub use config::{ConfigError, LoaderConfig, MarketDataFeed};
pub use ledger::{Entry, Ledger, LedgerError};
pub use loader::{now_unix_nanos, record_landed, Candidate, Loader, Pass, Pending, Recorded};
pub use metrics::{ErrorKind, LoaderMetrics, SkipReason};
