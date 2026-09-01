//! The two error types the record path can produce.

use thiserror::Error;

/// A [`Source`](crate::Source) failed to produce the next datagram.
#[derive(Debug, Error)]
pub enum SourceError {
    #[error("capture i/o: {0}")]
    Io(#[from] std::io::Error),
    /// The membership, the interface or the handle went away. The caller
    /// decides whether to rejoin or to end: a source that ends on its own
    /// leaves a recorder that looks exactly like a quiet feed.
    #[error("capture handle is no longer usable: {0}")]
    HandleLost(String),
    #[error("archive is not readable as pcapng: {0}")]
    MalformedArchive(String),
}

/// A [`Sink`](crate::Sink) failed to accept, rotate or flush.
///
/// Nothing in the record path may propagate one of these into the drain
/// thread. Every path that could is a counter and a drop instead — a writer
/// that blocks on a full disk stalls the drain thread, overflows the receive
/// queue, and converts a storage outage into false publisher-loss findings in
/// every archive written during it.
#[derive(Debug, Error)]
pub enum SinkError {
    #[error("archive i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("pcapng write: {0}")]
    Encode(String),
    #[error("compression: {0}")]
    Compress(String),
}
