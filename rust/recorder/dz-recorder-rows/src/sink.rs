//! The one seam a writer implements, and the error it may fail with.
//!
//! # One method, every grain
//!
//! [`RowSink::write_batch`] takes a whole [`RowBatch`]. A method per grain was
//! refused: reprocessing is idempotent on `(object key, sha256)`, so the object
//! is the unit that either landed or did not, and an object whose datagram rows
//! landed while its gap rows did not is an object that reads as a clean feed.
//! Partial credit is how a gap becomes invisible.
//!
//! A sink that cannot accept the batch must say so, and the caller must treat
//! the object as unloaded. That is why there is no "accepted some" return: a
//! [`Written`] comes back only when the whole batch is in.

use crate::rows::{Grain, RowBatch};
use thiserror::Error;

/// A [`RowSink`] failed to accept the batch.
///
/// Every variant names the object or the grain, because the loader's answer to
/// all of them is the same — the object stays unloaded and is retried — and the
/// only thing an operator needs from the error is which one and why.
#[derive(Debug, Error)]
pub enum RowSinkError {
    #[error("writing rows: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialising a {grain} row: {source}")]
    Encode {
        grain: Grain,
        #[source]
        source: serde_json::Error,
    },
    /// The destination refused the batch, and the retries this sink was
    /// willing to make are spent. The last error is carried verbatim: a
    /// bounded retry that discards what the destination said leaves an operator
    /// with a count and no cause.
    #[error("{object_key} was refused after {attempts} attempts: {last}")]
    Rejected {
        object_key: String,
        attempts: u32,
        last: String,
    },
}

/// What a sink actually wrote, per grain.
///
/// Per grain rather than in total because the grains are orders of magnitude
/// apart in volume and a single number hides the interesting failure: a load
/// that wrote a hundred thousand datagram rows and no gap rows is not a load
/// that went well.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    rows: [u64; Grain::COUNT],
    bytes: u64,
}

impl Written {
    #[must_use]
    pub fn of(batch: &RowBatch, bytes: u64) -> Self {
        let mut written = Self::default();
        for grain in Grain::ALL {
            written.rows[grain.index()] = batch.rows(grain) as u64;
        }
        written.bytes = bytes;
        written
    }

    #[must_use]
    pub const fn rows(&self, grain: Grain) -> u64 {
        self.rows[grain.index()]
    }

    #[must_use]
    pub fn total(&self) -> u64 {
        self.rows.iter().sum()
    }

    /// Bytes handed to the destination, which is what a throughput question
    /// wants and what a batch-size bound is enforced on.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Folds another sink's report in, for a caller loading many objects.
    pub fn add(&mut self, other: Self) {
        for grain in Grain::ALL {
            self.rows[grain.index()] = self.rows[grain.index()].saturating_add(other.rows(grain));
        }
        self.bytes = self.bytes.saturating_add(other.bytes);
    }
}

/// Somewhere rows are written.
///
/// Implemented twice: [`FileSink`](crate::FileSink), which is the CI sink and
/// the `--dry-run` sink, and the column store, in a crate of its own. The trait
/// is what keeps the derivation from learning what a column store is.
pub trait RowSink {
    /// Writes every row in the batch, or fails having written none that the
    /// caller may count.
    ///
    /// # Errors
    ///
    /// [`RowSinkError`], and then the object is unloaded. An implementation that
    /// wrote some rows before failing must still return the error: the loader's
    /// ledger and `ReplacingMergeTree` together make the retry a replace, and a
    /// success reported over a partial write is a gap nothing will ever find.
    fn write_batch(&mut self, rows: RowBatch) -> Result<Written, RowSinkError>;

    /// Pushes whatever is buffered.
    ///
    /// # Errors
    ///
    /// [`RowSinkError`] if the buffered rows could not be handed over.
    fn flush(&mut self) -> Result<(), RowSinkError>;
}
