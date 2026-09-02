//! The state directory on a real filesystem: an advisory lock and an atomic
//! rename.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use nix::fcntl::{Flock, FlockArg};

use crate::store::{StateError, StateStore};

/// The record itself.
const RECORD: &str = "instruments.state";
/// Where a new record is written before it replaces the old one.
const PENDING: &str = "instruments.state.pending";
/// The file the advisory lock is taken on.
///
/// A file of its own rather than the record: the record is replaced by rename,
/// and a lock held on the replaced inode guards nothing.
const LOCK: &str = "writer.lock";

/// The `[refdata] state_dir` as a store.
///
/// # The single-writer guard
///
/// [`claim`](StateStore::claim) takes a non-blocking exclusive `flock` on a
/// lock file in the directory and holds it for as long as this store lives.
/// Two publishers pointed at one `state_dir` therefore end with the first
/// running and the second failing to start, which is the outcome the design
/// asks for: two writers means the last flush wins, and every `Instrument ID`
/// the loser published resolves to nothing after a restart.
///
/// `flock` and not a file the claimer creates and deletes. A lock file created
/// with `O_EXCL` outlives the process that made it, so a publisher that
/// crash-loops — and one existing publisher crash-looped over thirty thousand
/// times in two days over a configuration change — would take its first crash
/// and then never start again, needing an operator to delete a file before
/// anything could recover. The kernel drops an `flock` when the last descriptor
/// on it closes, including when the process dies however it dies, so a stale
/// claim is not a state this can reach.
///
/// What it does not cover: two hosts writing one directory over a network
/// filesystem, where `flock` semantics are the filesystem's business rather
/// than the kernel's. That is a deployment to refuse rather than a guard to
/// write, and the [`StateRecord`](crate::StateRecord)'s `Source ID` check is
/// what would catch the ordinary version of it.
///
/// # The write
///
/// [`store`](StateStore::store) writes the whole record to a pending file,
/// flushes it to the device, renames it over the record, and then flushes the
/// directory. The rename is what makes a reader see either the old record whole
/// or the new one whole; the flush before it is what makes that true after a
/// power loss rather than only after a crash.
#[derive(Debug)]
pub struct FileStore {
    dir: PathBuf,
    /// Held, never read. Dropping it releases the claim, so this field is the
    /// claim's lifetime and removing it would silently remove the guard.
    claim: Option<Flock<File>>,
}

impl FileStore {
    /// A store over `state_dir`. Nothing is created and nothing is read until
    /// [`claim`](StateStore::claim).
    #[must_use]
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: state_dir.into(),
            claim: None,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

impl StateStore for FileStore {
    fn claim(&mut self) -> Result<(), StateError> {
        std::fs::create_dir_all(&self.dir).map_err(StateError::Claim)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(self.path(LOCK))
            .map_err(StateError::Claim)?;
        match Flock::lock(lock, FlockArg::LockExclusiveNonblock) {
            Ok(held) => {
                self.claim = Some(held);
                Ok(())
            }
            // The one errno that means "somebody else has it" rather than
            // "this could not be attempted", told apart because the two are
            // different operator actions: stop the other publisher, versus
            // look at the directory.
            Err((_, nix::errno::Errno::EWOULDBLOCK)) => Err(StateError::AlreadyHeld),
            Err((_, errno)) => Err(StateError::Claim(std::io::Error::from(errno))),
        }
    }

    fn load(&mut self) -> Result<Option<Vec<u8>>, StateError> {
        match std::fs::read(self.path(RECORD)) {
            Ok(bytes) => Ok(Some(bytes)),
            // A directory with no record is a publisher that has never minted
            // an ID. Every other error is a record that exists and cannot be
            // read, which is not the same thing and must not be treated as one.
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StateError::Read(error)),
        }
    }

    fn store(&mut self, record: &[u8]) -> Result<(), StateError> {
        let pending = self.path(PENDING);
        let mut file = File::create(&pending).map_err(StateError::Write)?;
        file.write_all(record).map_err(StateError::Write)?;
        // Before the rename, not after: a rename that reaches the directory
        // ahead of the bytes it names leaves a record that exists and is empty,
        // which reads back as damaged and stops the next start.
        file.sync_all().map_err(StateError::Write)?;
        drop(file);
        std::fs::rename(&pending, self.path(RECORD)).map_err(StateError::Write)?;
        sync_dir(&self.dir).map_err(StateError::Write)
    }
}

/// Flush the directory entry the rename created.
///
/// Opening a directory read-only and syncing it is how the rename itself is
/// made durable; without it the record can be the old one again after a power
/// loss, which is a stale ID map rather than a damaged one - and a stale map
/// mints an ID that is already published.
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}
