//! The state directory on a real filesystem: an advisory lock, an atomic
//! rename, and an append.

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
/// power loss rather than only after a crash. A failure before the rename
/// removes the pending file, so a record that could not be replaced on a full
/// disk leaves the room it had for appends.
///
/// [`append`](StateStore::append) writes through a handle opened with
/// `O_APPEND` on the record and flushes it with `sync_data`. The handle is
/// dropped by every `store`, because the rename leaves it on the replaced
/// inode, and a line appended there is a line no `load` will ever read.
///
/// [`truncate`](StateStore::truncate) shortens the record in place and flushes
/// it with `sync_data`, which is what lets a full disk still start: it
/// allocates nothing.
#[derive(Debug)]
pub struct FileStore {
    dir: PathBuf,
    /// Held, never read. Dropping it releases the claim, so this field is the
    /// claim's lifetime and removing it would silently remove the guard.
    claim: Option<Flock<File>>,
    /// The record, opened for appending, until the next `store` replaces it.
    appending: Option<File>,
}

impl FileStore {
    /// A store over `state_dir`. Nothing is created and nothing is read until
    /// [`claim`](StateStore::claim).
    #[must_use]
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: state_dir.into(),
            claim: None,
            appending: None,
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    /// Write `record` beside the record and rename it over it. An error leaves
    /// the record as it was.
    fn write_pending(&self, pending: &Path, record: &[u8]) -> std::io::Result<()> {
        let mut file = File::create(pending)?;
        file.write_all(record)?;
        // Before the rename, not after: a rename that reaches the directory
        // ahead of the bytes it names leaves a record that exists and is empty,
        // which reads back as damaged and stops the next start.
        file.sync_all()?;
        drop(file);
        std::fs::rename(pending, self.path(RECORD))
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
        // Dropped before the rename, so no path through here leaves an append
        // handle on an inode the rename is about to unlink.
        self.appending = None;
        let pending = self.path(PENDING);
        self.write_pending(&pending, record).map_err(|error| {
            // Removed, so a write that ran the disk out of room does not keep
            // that room from the appends that go on after it.
            let _ = std::fs::remove_file(&pending);
            StateError::NotReplaced(error)
        })?;
        // After the rename the record may be either one, and a directory sync
        // that fails does not say which.
        sync_dir(&self.dir).map_err(StateError::Write)
    }

    fn append(&mut self, bytes: &[u8]) -> Result<(), StateError> {
        let file = match &mut self.appending {
            Some(file) => file,
            // No `create`: the first write to a directory is a `store`, so a
            // record missing here is one somebody removed, and appending would
            // start a file that is not our format.
            None => self.appending.insert(
                OpenOptions::new()
                    .append(true)
                    .open(self.path(RECORD))
                    .map_err(StateError::Write)?,
            ),
        };
        file.write_all(bytes).map_err(StateError::Write)?;
        // `sync_data` and not `sync_all`: the size is the one piece of metadata
        // an append changes, and `fdatasync` flushes it.
        file.sync_data().map_err(StateError::Write)
    }

    fn truncate(&mut self, len: usize) -> Result<(), StateError> {
        let file = OpenOptions::new()
            .write(true)
            .open(self.path(RECORD))
            .map_err(StateError::Write)?;
        file.set_len(len as u64).map_err(StateError::Write)?;
        file.sync_data().map_err(StateError::Write)
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
