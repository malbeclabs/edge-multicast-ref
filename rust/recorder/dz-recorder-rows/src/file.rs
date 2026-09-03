//! Newline-delimited JSON, one file per grain, into a directory.
//!
//! This is the sink that makes the golden tests possible and the sink
//! `--dry-run` uses. It writes exactly the bytes the column-store sink would put
//! in a `JSONEachRow` body, which is the property that matters: a golden test
//! over this sink is a test of what the other one sends, not of a second
//! serialisation written for the test.
//!
//! It appends. A loader re-run against the same directory therefore accumulates,
//! and that is deliberate — the deduplication `ReplacingMergeTree` performs is a
//! property of the column store and not of a file, and a file sink that
//! truncated would quietly hide a double load rather than show it.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::rows::{Grain, RowBatch};
use crate::sink::{RowSink, RowSinkError, Written};

/// One file per grain under one directory, named `<grain>.jsonl`.
#[derive(Debug)]
pub struct FileSink {
    dir: PathBuf,
    /// Opened lazily, so a grain that produced no rows leaves no file: an empty
    /// `conformance_finding.jsonl` beside a real one reads as a runner that ran
    /// and found nothing, and no runner ran.
    files: [Option<BufWriter<File>>; Grain::COUNT],
}

impl FileSink {
    /// Creates the directory if it is not there.
    ///
    /// # Errors
    ///
    /// [`RowSinkError::Io`] if the directory cannot be created.
    pub fn create(dir: impl Into<PathBuf>) -> Result<Self, RowSinkError> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            files: [None, None, None, None, None],
        })
    }

    /// Where a grain's rows land, whether or not the file exists yet.
    #[must_use]
    pub fn path(&self, grain: Grain) -> PathBuf {
        Self::path_in(&self.dir, grain)
    }

    /// The same, for a caller holding only the directory — a test asserting
    /// against a `--dry-run` output, say.
    #[must_use]
    pub fn path_in(dir: &Path, grain: Grain) -> PathBuf {
        dir.join(format!("{}.jsonl", grain.table()))
    }

    fn writer(&mut self, grain: Grain) -> Result<&mut BufWriter<File>, RowSinkError> {
        let path = self.path(grain);
        let slot = &mut self.files[grain.index()];
        if slot.is_none() {
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            *slot = Some(BufWriter::new(file));
        }
        Ok(slot
            .as_mut()
            .expect("the slot was just filled if it was empty"))
    }

    fn write_rows<T: Serialize>(&mut self, grain: Grain, rows: &[T]) -> Result<u64, RowSinkError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let mut bytes = 0u64;
        let writer = self.writer(grain)?;
        for row in rows {
            let line = serde_json::to_string(row)
                .map_err(|source| RowSinkError::Encode { grain, source })?;
            writer.write_all(line.as_bytes())?;
            writer.write_all(b"\n")?;
            bytes += line.len() as u64 + 1;
        }
        Ok(bytes)
    }
}

impl RowSink for FileSink {
    fn write_batch(&mut self, rows: RowBatch) -> Result<Written, RowSinkError> {
        let mut bytes = 0u64;
        bytes += self.write_rows(Grain::Datagram, &rows.datagram)?;
        bytes += self.write_rows(Grain::Era, &rows.era)?;
        bytes += self.write_rows(Grain::SegmentCoverage, &rows.segment_coverage)?;
        bytes += self.write_rows(Grain::SequenceGap, &rows.sequence_gap)?;
        bytes += self.write_rows(Grain::ConformanceFinding, &rows.conformance_finding)?;
        Ok(Written::of(&rows, bytes))
    }

    fn flush(&mut self) -> Result<(), RowSinkError> {
        for writer in self.files.iter_mut().flatten() {
            writer.flush()?;
        }
        Ok(())
    }
}

impl Drop for FileSink {
    /// A buffered writer dropped without a flush loses whatever was in it, and
    /// a loader that reported rows written over a lost buffer is the same lie a
    /// partial write would be.
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
