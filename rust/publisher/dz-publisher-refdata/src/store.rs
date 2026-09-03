//! Where the persisted state lives, behind a trait, and the two writers that
//! must never both be it.

use std::sync::{Arc, Mutex, MutexGuard};

/// The persisted state, as this crate reaches it.
///
/// Three operations, in the order they are called: claim the directory, read
/// what is in it, write it back. Everything about *what* is written is
/// [`StateRecord`](crate::StateRecord)'s; everything about *where* is an
/// implementation of this.
///
/// # Why this is a trait
///
/// So that every property this crate promises is testable without a
/// filesystem: a claim that is already held, a read that fails, a write that
/// fails, and a record that is present but damaged are four behaviours a real
/// directory will only produce if a test can arrange a broken one, and
/// arranging one costs privileges a test suite should not need. They are all
/// four reachable through [`MemoryStore`].
///
/// It is also what the recorder needs. Re-running a venue's listings offline
/// must mint the same `Instrument ID`s the capture carries, and must not write
/// a single byte into the live publisher's state directory while doing it.
///
/// # The order is not a suggestion
///
/// [`Registry::open`](crate::Registry::open) calls [`claim`](Self::claim)
/// before [`load`](Self::load), and never calls [`store`](Self::store) without
/// having claimed. An implementation may assume that order; it may not rely on
/// the caller for the exclusion itself, because the caller is what the
/// exclusion is protecting the directory from.
pub trait StateStore {
    /// Take exclusive use of the state directory for as long as this store
    /// lives.
    ///
    /// # Errors
    ///
    /// [`StateError::AlreadyHeld`] when another live writer holds it, which is
    /// the single-writer guard refusing to start a second publisher on one
    /// directory. Any other [`StateError`] is a directory that could not be
    /// claimed at all.
    fn claim(&mut self) -> Result<(), StateError>;

    /// The persisted record, or `Ok(None)` for a directory that holds none.
    ///
    /// `Ok(None)` and `Err` are kept apart deliberately: the first is a
    /// publisher that has never minted an ID, and the second is one whose
    /// minted IDs are unreadable. Those take opposite actions.
    ///
    /// # Errors
    ///
    /// [`StateError::Read`] for a record that exists and could not be read.
    fn load(&mut self) -> Result<Option<Vec<u8>>, StateError>;

    /// Replace the persisted record.
    ///
    /// Must be atomic against a reader: after this returns, whether it returned
    /// an error or not, a subsequent [`load`](Self::load) sees either the whole
    /// previous record or the whole new one. A partially written record would
    /// be read back as a damaged one, and a damaged one stops the publisher
    /// starting.
    ///
    /// # Errors
    ///
    /// [`StateError::Write`]. The caller treats this as a fault and mints
    /// nothing further: an `Instrument ID` that was published but not persisted
    /// is one that resolves to nothing after a restart.
    fn store(&mut self, record: &[u8]) -> Result<(), StateError>;
}

/// What the state directory can refuse.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Another live writer holds this directory.
    ///
    /// **The holder wins.** See
    /// [`Registry::open`](crate::Registry::open) for why the newcomer is the
    /// one that fails.
    #[error("the state directory is held by another live writer")]
    AlreadyHeld,

    #[error("the state directory could not be claimed")]
    Claim(#[source] std::io::Error),

    #[error("the persisted record could not be read")]
    Read(#[source] std::io::Error),

    #[error("the persisted record could not be written")]
    Write(#[source] std::io::Error),
}

/// A state directory that is not one.
///
/// Cloning hands back **another writer onto the same directory**, which is what
/// makes the single-writer guard testable: two clones are two publishers
/// pointed at one `state_dir`, and the second [`claim`](StateStore::claim) is
/// refused exactly as the filesystem would refuse it. Dropping the writer that
/// holds the claim releases it, which is what happens to a real claim when the
/// process holding it dies.
///
/// This is also the store an offline re-run uses, where nothing should reach a
/// disk at all.
#[derive(Debug, Default)]
pub struct MemoryStore {
    directory: Arc<Mutex<Directory>>,
    /// Whether this particular writer is the one holding the claim, so that
    /// dropping a refused writer does not release the incumbent's.
    holds_claim: bool,
}

impl Clone for MemoryStore {
    /// Another writer onto the same directory, holding no claim of its own.
    ///
    /// Written out rather than derived: a derived clone of the writer that
    /// holds the claim would hold it too, and dropping that clone would
    /// release the incumbent's claim without the incumbent stopping - which is
    /// the one state the guard exists to make impossible.
    fn clone(&self) -> Self {
        Self {
            directory: Arc::clone(&self.directory),
            holds_claim: false,
        }
    }
}

#[derive(Debug, Default)]
struct Directory {
    record: Option<Vec<u8>>,
    claimed: bool,
    read_fails: Option<String>,
    write_fails: Option<String>,
}

impl MemoryStore {
    /// An empty directory, unclaimed.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The record as it stands, for a caller checking what was persisted.
    #[must_use]
    pub fn record(&self) -> Option<Vec<u8>> {
        self.lock().record.clone()
    }

    /// Put bytes in the directory without going through a writer, to stand in
    /// for a record damaged by something outside this process.
    pub fn set_record(&self, record: Vec<u8>) {
        self.lock().record = Some(record);
    }

    /// Make every read fail, standing in for a record that exists and cannot be
    /// read.
    pub fn break_reads(&self, message: &str) {
        self.lock().read_fails = Some(message.to_owned());
    }

    /// Make every write fail, standing in for a full or read-only directory.
    pub fn break_writes(&self, message: &str) {
        self.lock().write_fails = Some(message.to_owned());
    }

    /// Let writes through again.
    pub fn repair_writes(&self) {
        self.lock().write_fails = None;
    }

    fn lock(&self) -> MutexGuard<'_, Directory> {
        self.directory
            .lock()
            .unwrap_or_else(|held| held.into_inner())
    }
}

impl StateStore for MemoryStore {
    fn claim(&mut self) -> Result<(), StateError> {
        let mut directory = self.lock();
        if directory.claimed {
            return Err(StateError::AlreadyHeld);
        }
        directory.claimed = true;
        drop(directory);
        self.holds_claim = true;
        Ok(())
    }

    fn load(&mut self) -> Result<Option<Vec<u8>>, StateError> {
        let directory = self.lock();
        if let Some(message) = &directory.read_fails {
            return Err(StateError::Read(std::io::Error::other(message.clone())));
        }
        Ok(directory.record.clone())
    }

    fn store(&mut self, record: &[u8]) -> Result<(), StateError> {
        let mut directory = self.lock();
        if let Some(message) = &directory.write_fails {
            return Err(StateError::Write(std::io::Error::other(message.clone())));
        }
        // Assigned whole, so a reader never sees half of it - the same property
        // the atomic rename buys on a real filesystem.
        directory.record = Some(record.to_vec());
        Ok(())
    }
}

impl Drop for MemoryStore {
    fn drop(&mut self) {
        if self.holds_claim {
            self.lock().claimed = false;
        }
    }
}
