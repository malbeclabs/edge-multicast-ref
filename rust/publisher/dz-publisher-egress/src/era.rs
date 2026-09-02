//! `Reset Count` across restarts: the era store.
//!
//! The datagram header's `Reset Count` is how a publisher tells its
//! subscribers *forget what I told you*. A subscriber that sees it change
//! drops the book it had cached, the reference data it had accumulated and the
//! snapshot context it was assembling, and re-syncs from scratch. It is the
//! resolved shape for announcing a reset — the alternative, a message type
//! emitted at startup, is a thing three feeds answer three different ways and
//! two of them reserve the type ID it would need.
//!
//! Which makes this the whole point: **a publisher whose sequence series
//! restarts at 0 without its era changing has told subscribers nothing.** They
//! keep the stale book and apply fresh deltas onto it, and read the sequence
//! going backwards as reordering rather than as a restart. Since the series
//! restarts on every process start, the era must advance on every process
//! start, and that means it has to survive one.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use dz_edge_core::{Feed, ResetCount};

/// The era a feed that has no persisted history advertises.
///
/// **Not zero.** `ResetCount(0)` is what a channel that has never reset
/// advertises, and a publisher's first datagram has already reset any
/// subscriber that was listening to a previous incarnation of this feed. One is
/// also what a newly enabled feed advertises, which is the reason the store is
/// keyed per feed: a feed that has never published must not inherit an era from
/// another feed that has published for months, or its first datagram claims a
/// history it does not have.
pub const FIRST_ERA: u8 = 1;

/// The token every era file starts with. A version, so that a later format can
/// be told from a corrupt file of this one.
const FORMAT_TAG: &str = "era-v1";

/// The persisted era of each feed this host publishes.
///
/// One small file per feed under a state directory, written before the feed's
/// first datagram and never touched again while the process runs.
///
/// # Failure modes are the contract
///
/// The store exists to make a restart visible, so what it does when it cannot
/// tell is part of what it is for:
///
/// - **No file.** The feed has never published from this state directory, so no
///   subscriber holds an era for it, so nothing can collide: the era is
///   [`FIRST_ERA`]. This is also what a wiped state directory looks like, and
///   the collision that case risks is real but unresolvable — a store with no
///   record cannot distinguish *never ran* from *lost its memory*. An operator
///   who moves or clears the state directory has restarted the feed's history
///   and needs to know it.
/// - **A file that will not parse.** [`EraError::Corrupt`], and the publisher
///   does not start. This is the case where guessing is worst: a file exists,
///   so an era *was* in use, and picking one risks re-advertising the era
///   subscribers already hold state under. A subscriber's barrier fires on a
///   *change*, so re-using the previous era after a restart means no subscriber
///   ever drops its stale book — the exact silent corruption this file exists
///   to prevent, arrived at by the store that was meant to prevent it. Refusing
///   is loud, and an operator can repair it with one line or clear the
///   directory deliberately.
/// - **The write fails.** Also a refusal to start, and for the same reason: an
///   era that was not persisted is an era that will be re-used by the next
///   start.
///
/// The file is written and `fsync`ed, and the directory `fsync`ed after the
/// rename, *before* the era is handed out. A crash between publishing under an
/// era and recording it would re-use that era on the next start, which is the
/// failure above.
pub struct EraStore {
    dir: PathBuf,
}

impl EraStore {
    /// Open, creating the state directory if it is not there.
    ///
    /// # Errors
    ///
    /// [`EraError::Io`] if the directory cannot be created.
    pub fn open(dir: impl Into<PathBuf>) -> Result<Self, EraError> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|source| EraError::Io {
            path: dir.clone(),
            source,
        })?;
        Ok(Self { dir })
    }

    /// Advance this feed's era, persist it, and return it.
    ///
    /// Called once per feed at startup, before the feed's first datagram. The
    /// value is persisted before it is returned, so a crash immediately after
    /// this call cannot leave the next start re-using it.
    ///
    /// # Errors
    ///
    /// [`EraError`]. Every variant is a refusal to start; see [`EraStore`].
    pub fn begin_era<F: Feed>(&self) -> Result<ResetCount, EraError> {
        let path = self.path_for(F::NAME)?;
        let era = match read_era(&path)? {
            None => FIRST_ERA,
            // **The era after 255 is 0, and that is not a lie.** The
            // specification anticipates this exact wrap and settles it: a
            // subscriber detects a reset by testing its last-seen value for
            // *inequality*, "any change, including the 255 to 0 wrap, is a
            // reset; never compare for ordering". So 0 is not a claim about
            // history a subscriber could be misled by - it is only ever read
            // against what that subscriber last saw on that channel instance.
            //
            // Skipping 0 was tried here first, on the reasoning that 0 is the
            // value a channel advertises before it has ever reset. That
            // reasoning reads the field as ordered, which is the one thing the
            // specification forbids - and it would have made this store's
            // sequence disagree with `ChannelSequence::begin_era`, which wraps.
            // Two era sequences for one channel is worse than either.
            Some(previous) => previous.wrapping_add(1),
        };
        self.write_era(&path, era)?;
        Ok(ResetCount(era))
    }

    /// This feed's persisted era without advancing it, or `None` for a feed
    /// with no history here. For a diagnostic, and for a check mode.
    ///
    /// # Errors
    ///
    /// [`EraError::Corrupt`] or [`EraError::Io`], as [`Self::begin_era`].
    pub fn persisted_era<F: Feed>(&self) -> Result<Option<ResetCount>, EraError> {
        Ok(read_era(&self.path_for(F::NAME)?)?.map(ResetCount))
    }

    /// The file a feed's era lives in.
    ///
    /// The feed name becomes a path component, so it is checked rather than
    /// trusted. `Feed::NAME` is a compile-time constant in the codec crates
    /// today, but a name that is not one path component is a directory
    /// traversal from a constant nobody thought of as one, and the check costs
    /// nothing at startup.
    fn path_for(&self, name: &'static str) -> Result<PathBuf, EraError> {
        let safe = !name.is_empty()
            && name.len() <= 64
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !safe {
            return Err(EraError::UnsafeFeedName { name });
        }
        Ok(self.dir.join(format!("{name}.era")))
    }

    /// Write via a temporary file and a rename, so that a crash mid-write
    /// leaves either the previous era or the new one — never a half-written
    /// file, which is the [`EraError::Corrupt`] refusal above and would need an
    /// operator to start the publisher at all.
    fn write_era(&self, path: &Path, era: u8) -> Result<(), EraError> {
        let tmp = path.with_extension("era.tmp");
        let write = |path: &Path| -> io::Result<()> {
            let mut file = File::create(path)?;
            file.write_all(format!("{FORMAT_TAG} {era}\n").as_bytes())?;
            file.sync_all()
        };
        write(&tmp).map_err(|source| EraError::Io {
            path: tmp.clone(),
            source,
        })?;
        fs::rename(&tmp, path).map_err(|source| EraError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        // The rename itself has to reach the disk. Without this, a power loss
        // after a clean-looking startup leaves the directory entry pointing at
        // the *previous* era, and the next start re-uses an era that has
        // already published.
        File::open(&self.dir)
            .and_then(|dir| dir.sync_all())
            .map_err(|source| EraError::Io {
                path: self.dir.clone(),
                source,
            })
    }
}

/// Read a feed's era file: `None` for absent, `Err` for present and unreadable.
///
/// Read as bytes rather than as a string so that a file of arbitrary bytes is
/// diagnosed as corrupt — which is a refusal an operator must repair — instead
/// of as an I/O error, which reads like something a retry might fix.
fn read_era(path: &Path) -> Result<Option<u8>, EraError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(EraError::Io {
                path: path.to_path_buf(),
                source,
            })
        }
    };
    let corrupt = |what: &'static str| EraError::Corrupt {
        path: path.to_path_buf(),
        what,
    };
    let text = std::str::from_utf8(&bytes).map_err(|_| corrupt("not UTF-8"))?;
    let mut fields = text.split_whitespace();
    if fields.next() != Some(FORMAT_TAG) {
        return Err(corrupt("does not begin with the format tag"));
    }
    let era: u8 = fields
        .next()
        .ok_or_else(|| corrupt("holds no era"))?
        .parse()
        .map_err(|_| corrupt("holds an era that is not a number in 0..=255"))?;
    if fields.next().is_some() {
        return Err(corrupt("holds more than the format tag and an era"));
    }
    // A persisted 0 is ordinary: it is what a channel on its 256th era
    // recorded, and the next one after it is 1. Refusing it - which this did at
    // first - would turn a wrap into a refusal to start, once every 256
    // restarts, on a publisher that had done nothing wrong.
    Ok(Some(era))
}

/// Why an era could not be read or recorded. Every variant is a refusal to
/// start; see [`EraStore`].
#[derive(Debug, thiserror::Error)]
pub enum EraError {
    /// A feed name that is not one safe path component.
    #[error("feed name {name:?} is not a single lowercase path component")]
    UnsafeFeedName { name: &'static str },

    /// The file is there and does not say what era this feed is in.
    #[error("the era file {path:?} is corrupt: it {what}")]
    Corrupt { path: PathBuf, what: &'static str },

    #[error("{path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}
