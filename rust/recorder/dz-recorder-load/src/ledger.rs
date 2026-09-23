//! What has been loaded, keyed on `(object key, sha256)`.
//!
//! # Why the digest is part of the key
//!
//! Reprocessing is idempotent on the pair, so the pair is what the ledger
//! records. An object key alone would make a re-derived object — same key, new
//! bytes, because a recorder was restarted or an object was rebuilt — look
//! loaded when its rows are not there. The digest is what distinguishes *this
//! object* from *an object that was once at this key*.
//!
//! # Append-only, and compacted against the directory
//!
//! Each load appends one line. On a full pass the ledger is rewritten keeping
//! only the entries whose objects are still in the directory, plus the trailer:
//! an entry about an object nobody can present again is an entry nothing will
//! ever match, and objects here are evicted under a staging budget, so without
//! compaction the ledger grows without bound on a host whose archive does not.
//!
//! The rewrite is to a temporary file and then a rename, because a ledger
//! truncated by a crash mid-write is a loader that re-loads its whole archive —
//! which costs a replace and not a duplication, but also costs the eviction race
//! the lag metric exists to watch.

use std::collections::{HashMap, HashSet};
use std::io::Write as _;
use std::path::PathBuf;

use dz_recorder_rows::SegmentTrailer;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LedgerError {
    #[error("ledger {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// One object, loaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub object_key: String,
    pub object_sha256: String,
    /// When this loader wrote the rows, which is not when the traffic passed.
    pub loaded_at_ns: u64,
    /// What the object left for its successor's adjacency check. Carried in the
    /// ledger so that a restart resumes with the same certainty a continuous run
    /// had: without it the first object after every restart writes an uncertain
    /// era boundary, and every gap in it is reported `unverifiable`.
    pub trailer: SegmentTrailer,
    /// The feed the object belongs to, which is what makes the trailer above
    /// answerable.
    ///
    /// **`segment_seq` IS PER FEED AND RESTARTS AT 0 FOR EACH.** One loader
    /// reading `completed/<spec>/` sees several, so a trailer without this is a
    /// trailer whose `seq + 1` adjacency check consults whichever feed last
    /// wrote the highest number.
    ///
    /// `default` for the lines a ledger written before this field holds. They
    /// come back under the empty feed, so the first object of each real feed
    /// after the upgrade finds no trailer of its own and writes one uncertain
    /// boundary -- the same one-off cost a restart already pays, and stated
    /// here so it is not mistaken for a defect.
    #[serde(default)]
    pub feed: String,
}

/// The ledger, held in memory and appended to on disk.
#[derive(Debug)]
pub struct Ledger {
    path: PathBuf,
    loaded: HashSet<(String, String)>,
    /// The trailer of the highest `segment_seq` this ledger knows FOR EACH
    /// FEED, which is what that feed's next object consults.
    ///
    /// Keyed, because `segment_seq` restarts at 0 per feed: one map and not one
    /// trailer is the difference between an adjacency check that answers about
    /// its own feed and one that answers about whichever feed counted highest.
    trailers: HashMap<String, SegmentTrailer>,
    entries: usize,
}

impl Ledger {
    /// Reads what is there, and treats a line it cannot parse as absent.
    ///
    /// A ledger with a torn last line is what a crash mid-append leaves, and the
    /// entry it describes is one whose rows may or may not have landed. Dropping
    /// it re-loads that object, which `ReplacingMergeTree` makes a replace.
    /// Refusing to start over one bad line would be a loader that a single crash
    /// takes out permanently.
    ///
    /// # Errors
    ///
    /// [`LedgerError::Io`] if the file exists and cannot be read, or its
    /// directory cannot be created. A ledger that cannot be read is not the same
    /// as one that is not there: starting on the second is resuming from
    /// nothing, and starting on the first is loading an archive whose rows may
    /// already be in place while nothing records it.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, LedgerError> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|source| LedgerError::Io {
                    path: path.clone(),
                    source,
                })?;
            }
        }
        let mut ledger = Self {
            path: path.clone(),
            loaded: HashSet::new(),
            trailers: HashMap::new(),
            entries: 0,
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ledger),
            Err(source) => return Err(LedgerError::Io { path, source }),
        };
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let Ok(entry) = serde_json::from_str::<Entry>(line) else {
                continue;
            };
            ledger.remember(&entry);
        }
        Ok(ledger)
    }

    /// A ledger that records nothing anywhere, for `--dry-run`.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            loaded: HashSet::new(),
            trailers: HashMap::new(),
            entries: 0,
        }
    }

    #[must_use]
    pub fn is_loaded(&self, object_key: &str, object_sha256: &str) -> bool {
        self.loaded
            .contains(&(object_key.to_owned(), object_sha256.to_owned()))
    }

    /// The trailer the next object's adjacency check should consult.
    #[must_use]
    pub fn trailer(&self, feed: &str) -> Option<&SegmentTrailer> {
        self.trailers.get(feed)
    }

    #[must_use]
    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Records a load, appending to the file when there is one.
    ///
    /// Written *after* the rows are in, and never before: the ledger's whole
    /// meaning is "the rows for this object are in the store", and an entry
    /// written first would make a failed load look complete for ever.
    ///
    /// # Errors
    ///
    /// [`LedgerError::Io`]. The caller must treat this as a failed load even
    /// though the rows landed: the alternative is a loader that has forgotten an
    /// object it loaded, and the re-load that follows is a replace.
    pub fn record(&mut self, entry: Entry) -> Result<(), LedgerError> {
        if !self.path.as_os_str().is_empty() {
            let line = serde_json::to_string(&entry).map_err(|e| LedgerError::Io {
                path: self.path.clone(),
                source: std::io::Error::other(e),
            })?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(|source| LedgerError::Io {
                    path: self.path.clone(),
                    source,
                })?;
            let io = |source| LedgerError::Io {
                path: self.path.clone(),
                source,
            };
            file.write_all(line.as_bytes()).map_err(io)?;
            file.write_all(b"\n").map_err(io)?;
            // Durable before the caller is told the load is recorded: an entry
            // in a page cache the machine loses is an object the loader believes
            // it has done.
            file.sync_all().map_err(io)?;
        }
        self.remember(&entry);
        Ok(())
    }

    /// Drops every entry whose object is no longer in `objects_dir`, and
    /// rewrites the file.
    ///
    /// # Errors
    ///
    /// [`LedgerError::Io`]. A compaction that fails is not a failed load: the
    /// ledger is still correct, only longer than it needs to be, so a caller
    /// counts this and carries on.
    pub fn compact(&mut self, present: &HashSet<(String, String)>) -> Result<(), LedgerError> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(source) => {
                return Err(LedgerError::Io {
                    path: self.path.clone(),
                    source,
                })
            }
        };
        let kept: Vec<Entry> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str::<Entry>(l).ok())
            .filter(|e| {
                // The trailer's own entry is kept whatever became of its object:
                // it is the evidence the next object's adjacency check needs,
                // and it is one line.
                present.contains(&(e.object_key.clone(), e.object_sha256.clone()))
                    || self
                        .trailers
                        .get(&e.feed)
                        .is_some_and(|t| t.segment_seq == e.trailer.segment_seq)
            })
            .collect();
        if kept.len() == self.entries {
            return Ok(());
        }

        let temp = self.path.with_extension("compacting");
        let io = |source| LedgerError::Io {
            path: temp.clone(),
            source,
        };
        let mut file = std::fs::File::create(&temp).map_err(io)?;
        for entry in &kept {
            let line = serde_json::to_string(entry).map_err(|e| LedgerError::Io {
                path: temp.clone(),
                source: std::io::Error::other(e),
            })?;
            file.write_all(line.as_bytes()).map_err(io)?;
            file.write_all(b"\n").map_err(io)?;
        }
        file.sync_all().map_err(io)?;
        // Rename, so a crash leaves either the old ledger or the new one and
        // never half of either.
        std::fs::rename(&temp, &self.path).map_err(|source| LedgerError::Io {
            path: self.path.clone(),
            source,
        })?;

        self.loaded = kept
            .iter()
            .map(|e| (e.object_key.clone(), e.object_sha256.clone()))
            .collect();
        self.entries = kept.len();
        Ok(())
    }

    fn remember(&mut self, entry: &Entry) {
        self.loaded
            .insert((entry.object_key.clone(), entry.object_sha256.clone()));
        self.entries += 1;
        // The highest segment, not the last written: a pass that loaded an
        // out-of-order object must not leave the earlier object's trailer as the
        // evidence the next object consults.
        if self
            .trailers
            .get(&entry.feed)
            .is_none_or(|t| entry.trailer.segment_seq >= t.segment_seq)
        {
            self.trailers
                .insert(entry.feed.clone(), entry.trailer.clone());
        }
    }
}
