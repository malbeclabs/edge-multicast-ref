//! The three traits that make a live capture and a replayed archive the same
//! thing to everything above them.
//!
//! This symmetry is the load-bearing property of the design: the analysis tier
//! runs unchanged over live traffic, the health tier runs unchanged over an
//! archive, and a recorder is testable end to end with no network.

use crate::{RecordedDatagram, SinkError, SourceError};
use std::path::PathBuf;

/// A stream of received datagrams: a live capture, or an archive read back.
pub trait Source {
    /// Blocking for a live source; `Ok(None)` is EOF for an archive.
    fn next(&mut self) -> Result<Option<RecordedDatagram<'_>>, SourceError>;
}

/// Somewhere datagrams are written byte for byte.
pub trait Sink {
    fn write(&mut self, dg: &RecordedDatagram<'_>) -> Result<(), SinkError>;

    /// `Ok(None)` means this call is handing nothing back — either the segment
    /// held nothing, or the implementation publishes asynchronously and no
    /// object has landed yet. Neither is an error and neither may be logged as
    /// one, and in particular `None` must not be read as *nothing was
    /// recorded*.
    ///
    /// An implementation whose publication is asynchronous cannot say more than
    /// that through this signature, because the object's path, size and digest
    /// exist only after it has been compressed and hashed — and waiting for
    /// that here would reintroduce the stall the design forbids. Such an
    /// implementation exposes its own precise form alongside this one, and a
    /// binary that wires up a recorder should prefer it.
    fn rotate(&mut self) -> Result<Option<CompletedSegment>, SinkError>;

    fn flush(&mut self) -> Result<(), SinkError>;
}

/// A header-only observer, run on the drain thread.
///
/// Implementations must be allocation-free per datagram, or run behind a
/// bounded channel that drops and counts. An observer that falls behind is not
/// allowed to slow the loop.
pub trait Observer {
    fn on_datagram(&mut self, dg: &RecordedDatagram<'_>);
}

/// A rotated, hashed segment, ready for whatever ships it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedSegment {
    pub path: PathBuf,
    /// Monotonic per recorder run. A gap in this sequence is a gap in the
    /// archive; without it, a recorder that was down for an hour is
    /// indistinguishable from a feed that was quiet for an hour.
    pub segment_seq: u64,
    pub start_ns: u64,
    pub end_ns: u64,
    pub datagram_count: u64,
    /// Of the object that lands, so that reprocessing keyed on
    /// `(object key, sha256)` is idempotent.
    pub byte_count: u64,
    pub sha256: [u8; 32],
}
