//! The column store, as one implementation of [`RowSink`].
//!
//! `JSONEachRow` over HTTP, batched by row count and by bytes, with **the batch
//! as the retry unit** and the object as the unit of credit. Nothing here knows
//! how a row is derived, and nothing in the derivation knows this crate exists.
//!
//! # A failed batch means the object is unloaded
//!
//! This is the property the whole tier rests on and the easiest one to give away
//! by accident. Reprocessing is idempotent on `(object key, sha256)` and the
//! tables are `ReplacingMergeTree`, so a retry after a failure is a *replace*
//! rather than a duplication — which means the safe answer to any failure is to
//! report it and let the object be loaded again. Reporting partial success is
//! how a gap becomes invisible: an object whose datagram rows landed and whose
//! gap rows did not reads as a clean feed for ever.
//!
//! # Nothing here may block the recorder
//!
//! The loader is a separate process that shares only a directory with the
//! recorder. A column store that is down, slow or full must cost loading
//! progress and nothing else, so every failure below is a counted refusal and
//! never a wait without a bound.
//!
//! # Credentials come from the environment, and are never logged
//!
//! [`Credentials::from_env`] is the only way one enters this process:
//! [`PASSWORD_FILE_ENV`] — which is what a systemd credential is — or
//! [`PASSWORD_ENV`] for a caller with nowhere to put a file. The configuration
//! file carries the endpoint, the database and the user, the things an operator
//! needs to read in a review, and no password key exists to be filled in by
//! somebody who then commits it. Nothing in this crate's `Debug` or `Display`
//! output can carry one: see [`Credentials`].
#![forbid(unsafe_code)]

pub mod config;
pub mod ddl;
pub mod sink;
pub mod transport;

pub use config::{ClickHouseConfig, ConfigError, Credentials, PASSWORD_ENV, PASSWORD_FILE_ENV};
pub use ddl::{migrations, schema, Migration};
pub use sink::{send_order, ClickHouseSink};
pub use transport::{HttpTransport, Response, Transport, TransportError};
