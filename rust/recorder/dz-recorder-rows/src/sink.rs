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

/// The pair an object is identified and deduplicated by.
///
/// `(object key, sha256)` and not the key alone: a key names a location, and a
/// *re-derived* object — same key, new bytes, because a recorder was restarted
/// or an object rebuilt — must not look loaded when its rows are not there.
/// This is the ledger's key and the `ReplacingMergeTree` key, and it is what a
/// sink hands back to say which objects are now durable.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId {
    pub key: String,
    pub sha256: String,
}

impl ObjectId {
    #[must_use]
    pub fn of(batch: &RowBatch) -> Self {
        Self {
            key: batch.object_key.clone(),
            sha256: batch.object_sha256.clone(),
        }
    }
}

/// What a post put in the store.
///
/// The two together, because a caller needs both and they are answers to the
/// same event: which objects are now durable, and what that request cost. A
/// method that returned only the objects left the byte count on the floor —
/// and `dz_loader_bytes_written_total` flat at 0 while its own help text told
/// an operator to compare it against the bytes read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Landed {
    /// The objects whose rows are now durable. Empty when nothing was due.
    pub objects: Vec<ObjectId>,
    /// Bytes this call sent, and `0` when it sent nothing.
    ///
    /// A property of the *request* and not of an object: the rows of four
    /// objects in one body have one length between them, and dividing it up
    /// would be inventing a number. See [`Accepted::bytes_posted`].
    pub bytes_posted: u64,
}

/// What a sink did with one batch.
///
/// **Accepted is not landed, and the difference is the whole reason this type
/// exists.** A sink that coalesces rows from several objects into one insert has
/// taken the batch without having sent it, so a caller that read
/// [`write_batch`](RowSink::write_batch) returning `Ok` as "the rows are in the
/// store" would mark an object loaded whose rows are still in memory — and a
/// crash then loses them with nothing recording that it did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Accepted {
    /// Rows the sink took. Counted here because they were derived and handed
    /// over, whether or not this call sent them.
    pub accepted: Written,
    /// The objects whose rows are now durable, if this call posted. Empty means
    /// the sink is still holding them.
    pub landed: Vec<ObjectId>,
    /// Bytes this call actually sent, and `0` when it held.
    ///
    /// Here rather than on [`Written`] because once an insert spans objects a
    /// byte count is a property of the *request* and not of an object: the rows
    /// of four objects in one body have one length between them, and dividing it
    /// up would be inventing a number. [`Written::bytes`] therefore stays `0`
    /// for a sink that coalesces, and this is what a throughput metric adds up.
    pub bytes_posted: u64,
}

/// Somewhere rows are written.
///
/// Implemented twice: [`FileSink`](crate::FileSink), which is the CI sink and
/// the `--dry-run` sink, and the column store, in a crate of its own. The trait
/// is what keeps the derivation from learning what a column store is.
/// # The clock is a parameter
///
/// Every method takes `now_ns`. A sink that coalesces has to know when its
/// oldest held row stopped being worth holding, and reading a clock inside would
/// make that untestable without sleeping. The archive writer's `rotate_at`
/// already takes time this way, for the same reason.
pub trait RowSink {
    /// Takes every row in the batch, and says whether anything landed.
    ///
    /// A sink may hold the rows and post them later together with rows from
    /// other objects — see [`Accepted`]. It must take all of them or none: a
    /// batch spanning grains is one unit of idempotence.
    ///
    /// # Errors
    ///
    /// [`RowSinkError`], and then every object the sink was holding is unloaded
    /// along with this one. An implementation that sent some rows before failing
    /// must still return the error: the loader's ledger and
    /// `ReplacingMergeTree` together make the retry a replace, and a success
    /// reported over a partial write is a gap nothing will ever find.
    fn write_batch(&mut self, rows: RowBatch, now_ns: u64) -> Result<Accepted, RowSinkError>;

    /// Posts what is held **if it is due**, by rows or by age, and says what
    /// landed.
    ///
    /// Called once a pass, including a pass that found no new object: without
    /// that, a lane quiet enough to produce nothing would hold its last rows
    /// until something else arrived, which is the opposite of what an age bound
    /// is for.
    ///
    /// # Errors
    ///
    /// [`RowSinkError`], and the held objects stay unloaded.
    fn post_if_due(&mut self, now_ns: u64) -> Result<Landed, RowSinkError>;

    /// Posts everything held, due or not, and says what landed.
    ///
    /// For the way out: a `--once` pass and a shutdown both end here, so no run
    /// leaves rows in memory that the ledger will never account for.
    ///
    /// # Errors
    ///
    /// [`RowSinkError`], and the held objects stay unloaded.
    fn flush(&mut self, now_ns: u64) -> Result<Landed, RowSinkError>;
}
