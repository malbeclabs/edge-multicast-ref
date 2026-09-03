//! The file sink, which is the CI sink and the `--dry-run` sink.
//!
//! What it writes has to be exactly what the column-store sink sends, because a
//! golden test over this one is otherwise a test of a second serialisation
//! written for the test.
#![forbid(unsafe_code)]

mod common;

use common::record;
use dz_recorder_replay::synthetic::SyntheticPublisher;
use dz_recorder_replay::Fault;
use dz_recorder_rows::{FileSink, Grain, RowSink};

#[test]
fn every_grain_lands_in_its_own_file_named_for_its_table() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let recorded = record(&SyntheticPublisher::with_fault(100, Fault::SequenceGap));
    let batch = recorded.rows().rows;
    let expected: Vec<(Grain, usize)> = Grain::ALL.iter().map(|g| (*g, batch.rows(*g))).collect();

    let mut sink = FileSink::create(dir.path()).expect("the directory is writable");
    let written = sink.write_batch(batch).expect("the batch lands");
    sink.flush().expect("flush");

    for (grain, rows) in expected {
        assert_eq!(written.rows(grain), rows as u64, "{grain}");
        let path = FileSink::path_in(dir.path(), grain);
        if rows == 0 {
            // A grain that produced nothing leaves no file: an empty
            // `conformance_finding.jsonl` beside a real one reads as a runner
            // that ran and found nothing, and no runner ran.
            assert!(!path.exists(), "{grain} produced no rows and yet a file");
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("the file is readable");
        assert_eq!(text.lines().count(), rows, "{grain}");
        for line in text.lines() {
            let value: serde_json::Value =
                serde_json::from_str(line).expect("every line is one JSON object");
            assert!(value.is_object(), "{grain}: {line}");
        }
    }
    assert!(written.bytes() > 0);
    assert_eq!(
        written.total(),
        Grain::ALL.iter().map(|g| written.rows(*g)).sum::<u64>()
    );
}

/// It appends, so a double load is visible rather than hidden.
///
/// The deduplication `ReplacingMergeTree` performs is a property of the column
/// store and not of a file. A file sink that truncated would make a loader bug
/// that loads an object twice look exactly like one that loads it once.
#[test]
fn a_second_load_of_the_same_object_appends_rather_than_replacing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let recorded = record(&SyntheticPublisher::clean(20));

    let mut sink = FileSink::create(dir.path()).expect("the directory is writable");
    sink.write_batch(recorded.rows().rows).expect("first load");
    sink.write_batch(recorded.rows().rows).expect("second load");
    sink.flush().expect("flush");

    let text = std::fs::read_to_string(FileSink::path_in(dir.path(), Grain::Datagram))
        .expect("the file is readable");
    assert_eq!(text.lines().count(), 40);
}

/// Dropping the sink flushes it, because rows reported written over a lost
/// buffer are the same lie a partial write would be.
#[test]
fn a_dropped_sink_still_flushed_what_it_was_holding() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let recorded = record(&SyntheticPublisher::clean(20));
    {
        let mut sink = FileSink::create(dir.path()).expect("the directory is writable");
        sink.write_batch(recorded.rows().rows).expect("the batch");
    }
    let text = std::fs::read_to_string(FileSink::path_in(dir.path(), Grain::Datagram))
        .expect("the file is readable");
    assert_eq!(text.lines().count(), 20);
}
