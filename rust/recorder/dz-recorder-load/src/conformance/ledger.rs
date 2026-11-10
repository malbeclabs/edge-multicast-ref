//! What the runner has judged, keyed on the object **and the rule set version**.
//!
//! The load ledger answers `is_loaded(object key, sha256)`, which is right for
//! the rows it guards: those are a pure function of the two. A finding is not —
//! it is a function of the object and of a rule set that moves independently of
//! this binary — so that boolean would answer *have I judged this* with a
//! confident yes on exactly the day a version bump means no. Hence a ledger of
//! its own, in a file of its own, following the load ledger's habits: one JSON
//! line per entry, appended and synced after the rows are in, a torn last line
//! read as absent, and compaction against the objects still present.
//!
//! **Nothing here reads or writes the load ledger.** The boolean that guards
//! datagram rows must not start depending on whether a Go binary was rebuilt.

use std::collections::HashSet;
use std::io::Write as _;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum JudgeLedgerError {
    #[error("conformance ledger {path}: {cause}")]
    Io {
        path: PathBuf,
        #[source]
        cause: std::io::Error,
    },
}

/// One object, judged under one rule set version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JudgeEntry {
    pub object_key: String,
    pub object_sha256: String,
    pub rule_set_version: String,
    /// The manifest's feed, so compaction can keep to the feeds a pass judged.
    pub feed: String,
    pub judged_at_ns: u64,
}

#[derive(Debug)]
pub struct JudgeLedger {
    path: PathBuf,
    judged: HashSet<(String, String, String)>,
    objects: HashSet<(String, String)>,
    entries: usize,
}

impl JudgeLedger {
    /// Opens the ledger at `path`, creating its directory. A missing file is an
    /// empty ledger; a line that does not parse — the torn last line a crash
    /// mid-append leaves — is read as absent, so its object is judged again,
    /// which the table makes a replace.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, JudgeLedgerError> {
        let path = path.into();
        let io = |cause| JudgeLedgerError::Io {
            path: path.clone(),
            cause,
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(io)?;
            }
        }
        let mut ledger = Self {
            path: path.clone(),
            judged: HashSet::new(),
            objects: HashSet::new(),
            entries: 0,
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ledger),
            Err(cause) => return Err(io(cause)),
        };
        // A torn tail — the unfinished line a crash mid-append leaves — is cut
        // off before anything is appended. Left in place, the next entry would
        // be written onto the end of it, becoming one unparseable line, and the
        // object it records would read as unjudged on the next open.
        let whole = text.rfind('\n').map_or(0, |at| at + 1);
        if whole < text.len() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .map_err(io)?;
            file.set_len(whole as u64).map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        for entry in parse(&text[..whole]) {
            ledger.remember(&entry);
        }
        Ok(ledger)
    }

    /// A ledger that keeps nothing between runs, for `--dry-run`.
    #[must_use]
    pub fn in_memory() -> Self {
        Self {
            path: PathBuf::new(),
            judged: HashSet::new(),
            objects: HashSet::new(),
            entries: 0,
        }
    }

    /// Whether this object has been judged under this rule set version.
    #[must_use]
    pub fn is_judged(&self, object_key: &str, object_sha256: &str, version: &str) -> bool {
        self.judged.contains(&(
            object_key.to_owned(),
            object_sha256.to_owned(),
            version.to_owned(),
        ))
    }

    /// Whether this object has been judged under any version at all.
    ///
    /// What a pass consults before judging, and the reason it is not
    /// [`is_judged`](Self::is_judged): a version bump must not re-judge the
    /// retained archive as a side effect of a deploy. That is the largest bill
    /// this tier can present, and it is to be a deliberate, bounded act over a
    /// chosen range of objects — which this ledger, keyed on the version, is
    /// what makes possible — and never the consequence of picking up a new pin.
    #[must_use]
    pub fn judged_under_any(&self, object_key: &str, object_sha256: &str) -> bool {
        self.objects
            .contains(&(object_key.to_owned(), object_sha256.to_owned()))
    }

    #[must_use]
    pub const fn entries(&self) -> usize {
        self.entries
    }

    /// Appends an entry, durably. Called only once the rows it vouches for
    /// have landed.
    pub fn record(&mut self, entry: JudgeEntry) -> Result<(), JudgeLedgerError> {
        if !self.path.as_os_str().is_empty() {
            let io = |cause| JudgeLedgerError::Io {
                path: self.path.clone(),
                cause,
            };
            let line = serde_json::to_string(&entry).map_err(|e| io(std::io::Error::other(e)))?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
                .map_err(io)?;
            file.write_all(line.as_bytes()).map_err(io)?;
            file.write_all(b"\n").map_err(io)?;
            file.sync_all().map_err(io)?;
        }
        self.remember(&entry);
        Ok(())
    }

    /// Drops the entries of objects no longer present, among the feeds `scope`
    /// names; an entry of a feed outside it is kept, because the pass that
    /// asked did not look there. Rewritten through a rename, so a crash leaves
    /// one ledger or the other and never half of either.
    pub fn compact(
        &mut self,
        present: &HashSet<(String, String)>,
        scope: &HashSet<String>,
    ) -> Result<(), JudgeLedgerError> {
        if self.path.as_os_str().is_empty() {
            return Ok(());
        }
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(cause) => {
                return Err(JudgeLedgerError::Io {
                    path: self.path.clone(),
                    cause,
                })
            }
        };
        let kept: Vec<JudgeEntry> = parse(&text)
            .filter(|e| {
                !scope.contains(&e.feed)
                    || present.contains(&(e.object_key.clone(), e.object_sha256.clone()))
            })
            .collect();
        if kept.len() == self.entries {
            return Ok(());
        }
        let temp = self.path.with_extension("compacting");
        let io = |cause| JudgeLedgerError::Io {
            path: temp.clone(),
            cause,
        };
        let mut file = std::fs::File::create(&temp).map_err(io)?;
        for entry in &kept {
            let line = serde_json::to_string(entry).map_err(|e| io(std::io::Error::other(e)))?;
            file.write_all(line.as_bytes()).map_err(io)?;
            file.write_all(b"\n").map_err(io)?;
        }
        file.sync_all().map_err(io)?;
        std::fs::rename(&temp, &self.path).map_err(|cause| JudgeLedgerError::Io {
            path: self.path.clone(),
            cause,
        })?;
        self.judged.clear();
        self.objects.clear();
        self.entries = 0;
        for entry in &kept {
            self.remember(entry);
        }
        Ok(())
    }

    fn remember(&mut self, entry: &JudgeEntry) {
        self.judged.insert((
            entry.object_key.clone(),
            entry.object_sha256.clone(),
            entry.rule_set_version.clone(),
        ));
        self.objects
            .insert((entry.object_key.clone(), entry.object_sha256.clone()));
        self.entries += 1;
    }
}

fn parse(text: &str) -> impl Iterator<Item = JudgeEntry> + '_ {
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<JudgeEntry>(l).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{Entry, Ledger};

    fn entry(key: &str, version: &str) -> JudgeEntry {
        JudgeEntry {
            object_key: key.to_owned(),
            object_sha256: format!("sha-{key}"),
            rule_set_version: version.to_owned(),
            feed: "top-of-book".to_owned(),
            judged_at_ns: 1,
        }
    }

    #[test]
    fn an_object_judged_under_one_version_reads_as_unjudged_under_the_next() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("judged.jsonl");
        let mut ledger = JudgeLedger::open(&path).expect("a new ledger");
        ledger.record(entry("a", "v1")).expect("recorded");
        assert!(ledger.is_judged("a", "sha-a", "v1"));
        assert!(
            !ledger.is_judged("a", "sha-a", "v2"),
            "a version bump means no"
        );
        assert!(ledger.judged_under_any("a", "sha-a"));
        assert!(
            !ledger.is_judged("a", "sha-other", "v1"),
            "other bytes, other object"
        );

        let reopened = JudgeLedger::open(&path).expect("reopened");
        assert!(reopened.is_judged("a", "sha-a", "v1"));
        assert_eq!(reopened.entries(), 1);
    }

    #[test]
    fn a_torn_last_line_is_absent_and_its_object_is_judged_again() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("judged.jsonl");
        let mut ledger = JudgeLedger::open(&path).expect("a new ledger");
        ledger.record(entry("a", "v1")).expect("recorded");
        ledger.record(entry("b", "v1")).expect("recorded");
        let text = std::fs::read_to_string(&path).expect("readable");
        // A crash part way through appending b's line.
        std::fs::write(&path, &text[..text.len() - 20]).expect("torn");
        let mut reopened = JudgeLedger::open(&path).expect("reopened");
        assert!(reopened.is_judged("a", "sha-a", "v1"));
        assert!(
            !reopened.is_judged("b", "sha-b", "v1"),
            "torn, so not judged"
        );
        assert_eq!(reopened.entries(), 1);

        // Judged again and recorded: the entry must survive the next open,
        // not be appended onto the torn fragment and lost with it.
        reopened.record(entry("b", "v1")).expect("recorded");
        let again = JudgeLedger::open(&path).expect("reopened again");
        assert!(again.is_judged("a", "sha-a", "v1"));
        assert!(
            again.is_judged("b", "sha-b", "v1"),
            "the re-judged entry is kept"
        );
        assert_eq!(again.entries(), 2);
        assert!(std::fs::read_to_string(&path)
            .expect("readable")
            .lines()
            .all(|l| serde_json::from_str::<JudgeEntry>(l).is_ok()));
    }

    #[test]
    fn compaction_drops_entries_for_objects_no_longer_present_within_its_scope() {
        let dir = tempfile::tempdir().expect("a directory");
        let path = dir.path().join("judged.jsonl");
        let mut ledger = JudgeLedger::open(&path).expect("a new ledger");
        ledger.record(entry("kept", "v1")).expect("recorded");
        ledger.record(entry("kept", "v2")).expect("recorded");
        ledger.record(entry("evicted", "v1")).expect("recorded");
        let mut elsewhere = entry("other-feed", "v1");
        elsewhere.feed = "market-by-price".to_owned();
        ledger.record(elsewhere).expect("recorded");

        let present = HashSet::from([("kept".to_owned(), "sha-kept".to_owned())]);
        let scope = HashSet::from(["top-of-book".to_owned()]);
        ledger.compact(&present, &scope).expect("compacted");
        assert_eq!(ledger.entries(), 3);
        assert!(ledger.is_judged("kept", "sha-kept", "v1"));
        assert!(
            ledger.is_judged("kept", "sha-kept", "v2"),
            "every version of a present object"
        );
        assert!(!ledger.judged_under_any("evicted", "sha-evicted"));
        assert!(
            ledger.judged_under_any("other-feed", "sha-other-feed"),
            "a feed this pass did not look at is not a feed whose objects have gone"
        );
        let reopened = JudgeLedger::open(&path).expect("reopened");
        assert_eq!(reopened.entries(), 3);
    }

    /// The thing a second ledger exists for: the load ledger's answer is about
    /// rows, and nothing in this file changes it.
    #[test]
    fn the_load_ledger_still_answers_only_about_rows() {
        let dir = tempfile::tempdir().expect("a directory");
        let load_path = dir.path().join("ledger.jsonl");
        let mut load = Ledger::open(&load_path).expect("the load ledger");
        load.record(Entry {
            object_key: "loaded".to_owned(),
            object_sha256: "sha-loaded".to_owned(),
            loaded_at_ns: 1,
            trailer: dz_recorder_rows::SegmentTrailer::default(),
            feed: "top-of-book".to_owned(),
        })
        .expect("recorded");
        let before = std::fs::read(&load_path).expect("readable");

        let mut judged = JudgeLedger::open(dir.path().join("judged.jsonl")).expect("ours");
        judged.record(entry("unloaded", "v1")).expect("recorded");
        judged
            .compact(&HashSet::new(), &HashSet::from(["top-of-book".to_owned()]))
            .expect("compacted");

        assert_eq!(
            std::fs::read(&load_path).expect("readable"),
            before,
            "byte for byte"
        );
        let load = Ledger::open(&load_path).expect("reopened");
        assert!(load.is_loaded("loaded", "sha-loaded"));
        assert!(
            !load.is_loaded("unloaded", "sha-unloaded"),
            "judging an object does not load it"
        );
        let judged = JudgeLedger::open(dir.path().join("judged.jsonl")).expect("ours");
        assert!(
            !judged.judged_under_any("loaded", "sha-loaded"),
            "and loading an object does not judge it"
        );
    }
}
