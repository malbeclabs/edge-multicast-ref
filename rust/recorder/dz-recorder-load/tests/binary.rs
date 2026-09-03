//! The binary, run as a process.
//!
//! Everything below spawns the real executable, because the things being
//! asserted are properties of the whole: the exit codes a deployment pipeline
//! reads, that `--check` opens nothing, and that a `--dry-run` needs no
//! destination and touches no ledger.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use dz_edge_core::PortRole;
use dz_recorder_archive::rotate::{ArchiveWriter, ArchiveWriterConfig};
use dz_recorder_archive::writer::{LinkHeaders, RoleJoin};
use dz_recorder_archive::Compression;
use dz_recorder_core::{CaptureDropScope, RecorderIdentity};
use dz_recorder_replay::synthetic::{port_for, SyntheticPublisher, GROUP};

const BIN: &str = env!("CARGO_BIN_EXE_dz-recorder-load");
const SITE: &str = "site-1";
const RECORDER: &str = "recorder-1";

fn run(args: &[&str]) -> Output {
    Command::new(BIN)
        .args(args)
        // Nothing here contacts a destination, and an inherited password would
        // be a variable this test did not mean to set.
        .env_remove("DZ_LOADER_CLICKHOUSE_PASSWORD")
        .output()
        .expect("the binary runs")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A configuration file, with the metrics endpoint on port 0 so that two of
/// these can run at once.
fn config_at(dir: &Path, objects: &Path, endpoint: &str) -> PathBuf {
    let path = dir.join("loader.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[loader]
site = "{SITE}"
recorder = "{RECORDER}"
objects_dir = "{}"
ledger = "{}"
poll_interval = 1

[clickhouse]
endpoint = "{endpoint}"
database = "recorder"
user = "loader"
# A destination nothing answers on is the point of several of these tests, and
# the default half-minute wait would make the suite thirty times slower than the
# thing it is testing.
timeout = 1

[metrics]
listen_addr = "127.0.0.1:0"
"#,
            objects.display(),
            dir.join("ledger.jsonl").display(),
        ),
    )
    .expect("the configuration is writable");
    path
}

/// A completed directory with `segments` objects in it, written by the real
/// writer.
fn archive(dir: &Path, segments: usize) -> PathBuf {
    let completed = dir.join("completed");
    let cfg = ArchiveWriterConfig {
        staging_dir: dir.join("staging"),
        completed_dir: completed.clone(),
        rotate_bytes: 1 << 30,
        rotate_interval: Duration::from_secs(3600),
        staging_max: 1 << 40,
        compression: Compression::Zstd { level: 1 },
        identity: RecorderIdentity {
            site: SITE.to_owned(),
            recorder: RECORDER.to_owned(),
            env: "test".to_owned(),
            build_version: "0.1.0".to_owned(),
            build_commit: "0000000".to_owned(),
            config_hash: "a".repeat(64),
        },
        feed: "top-of-book".to_owned(),
        roles_joined: vec![RoleJoin::on(
            PortRole::Mktdata,
            GROUP,
            port_for(PortRole::Mktdata),
        )],
        link_headers: LinkHeaders::Synthesised,
        capture_drop_scope: CaptureDropScope::PortRole,
    };
    let mut writer = ArchiveWriter::new(cfg, 0).expect("the archive opens");
    for segment in 0..segments {
        SyntheticPublisher::clean(40)
            .publish_into(&mut writer)
            .expect("the write path never fails the caller");
        writer
            .rotate_at(1_000_000_000 * (segment as u64 + 1))
            .expect("rotation")
            .expect("a segment that held datagrams produces an object");
        writer
            .wait_completed()
            .expect("the compressor publishes exactly one object")
            .expect("publication");
    }
    completed
}

#[test]
fn help_and_version_succeed_and_say_where_it_runs() {
    let help = run(&["--help"]);
    assert!(help.status.success());
    let text = stdout(&help);
    assert!(text.contains("on the recorder host"), "{text}");
    assert!(text.contains("--dry-run"), "{text}");

    let version = run(&["--version"]);
    assert!(version.status.success());
    assert!(stdout(&version).starts_with("dz-recorder-load "));
}

/// A command line that could not be understood exits 2, which is a different
/// failure from a loader that refused to start: a pipeline distinguishes them.
#[test]
fn a_command_line_that_cannot_be_understood_exits_two() {
    for args in [
        vec!["--nope"],
        vec!["--once"],
        vec!["--config", "x.toml"],
        vec!["--config", "x.toml", "--check", "--once"],
        vec!["--config", "x.toml", "--once", "--watch"],
    ] {
        let output = run(&args);
        assert_eq!(
            output.status.code(),
            Some(2),
            "{args:?} exited {:?}: {}",
            output.status.code(),
            stderr(&output)
        );
        assert!(stderr(&output).contains("Usage:"), "{args:?}");
    }
}

/// A configuration that names a directory that is not there fails before
/// anything is opened, naming the key.
#[test]
fn a_configuration_that_names_nothing_real_is_refused_by_name() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let config = config_at(
        dir.path(),
        &dir.path().join("not-here"),
        "http://192.0.2.20:8123",
    );
    let output = run(&[
        "--config",
        &config.display().to_string(),
        "--once",
        "--dry-run",
        &dir.path().join("rows").display().to_string(),
    ]);
    assert_eq!(output.status.code(), Some(1));
    let text = stderr(&output);
    assert!(text.contains("objects_dir"), "{text}");
    assert!(text.contains("read-only"), "{text}");
}

/// `--check` validates and reaches for the destination, and loads nothing.
///
/// The destination here is a documentation address nothing answers on, so the
/// probe fails — which is the assertion: `--check` is a gate, and a gate that
/// passed without reaching the destination would let a pipeline restart a loader
/// that cannot write.
#[test]
fn check_reaches_for_the_destination_and_opens_no_object() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let objects = archive(dir.path(), 1);
    let config = config_at(dir.path(), &objects, "http://192.0.2.20:1");

    let output = run(&["--config", &config.display().to_string(), "--check"]);
    assert_eq!(output.status.code(), Some(1));
    let text = stderr(&output);
    assert!(text.contains("could not be reached"), "{text}");
    // And it says where the password comes from, because that is the other
    // half of what makes a destination unreachable.
    assert!(text.contains("DZ_LOADER_CLICKHOUSE_PASSWORD"), "{text}");
    // Nothing was loaded and no ledger was created.
    assert!(
        !dir.path().join("ledger.jsonl").exists(),
        "--check touched the ledger"
    );
    // What was read is printed, so an operator sees it rather than what they
    // believe they wrote.
    let printed = stdout(&output);
    assert!(printed.contains("site=site-1"), "{printed}");
    assert!(printed.contains("database=recorder"), "{printed}");
    assert!(!printed.to_lowercase().contains("password"), "{printed}");
}

/// A dry run writes the rows to a directory, contacts nothing, and records
/// nothing — so the same question can be asked twice.
#[test]
fn a_dry_run_writes_rows_contacts_nothing_and_records_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let objects = archive(dir.path(), 2);
    let rows = dir.path().join("rows");
    // A destination nothing is listening on: a dry run that contacted it would
    // fail here, which is the point of pointing it at one.
    let config = config_at(dir.path(), &objects, "http://192.0.2.20:1");
    let args = [
        "--config",
        &config.display().to_string(),
        "--once",
        "--dry-run",
        &rows.display().to_string(),
    ];

    let output = run(&args);
    assert!(
        output.status.success(),
        "exit {:?}: {}",
        output.status.code(),
        stderr(&output)
    );
    let report = stderr(&output);
    assert!(report.contains("loaded 2 object(s)"), "{report}");
    assert!(report.contains("0 unloaded"), "{report}");

    let datagrams = std::fs::read_to_string(rows.join("datagram.jsonl"))
        .expect("the datagram rows were written");
    assert_eq!(datagrams.lines().filter(|l| !l.is_empty()).count(), 80);
    for grain in ["era", "segment_coverage"] {
        assert!(
            rows.join(format!("{grain}.jsonl")).exists(),
            "{grain} rows are missing"
        );
    }
    // No runner ran, so there is no file at all rather than an empty one: an
    // empty `conformance_finding.jsonl` reads as a runner that found nothing.
    assert!(!rows.join("conformance_finding.jsonl").exists());
    // And nothing was recorded, so the same question can be asked again.
    assert!(!dir.path().join("ledger.jsonl").exists());

    let again = run(&args);
    assert!(again.status.success(), "{}", stderr(&again));
    assert!(
        stderr(&again).contains("loaded 2 object(s)"),
        "a dry run that recorded its work would answer this once"
    );
}

/// An object that is not the one its manifest describes fails the object and
/// not the pass, and the process exits non-zero to say so.
#[test]
fn a_damaged_object_fails_the_object_and_reports_it_in_the_exit_code() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let objects = archive(dir.path(), 2);
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&objects)
        .expect("the completed directory exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().ends_with(".pcapng.zst"))
        })
        .collect();
    paths.sort();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&paths[0])
        .expect("the object is writable");
    std::io::Write::write_all(&mut file, b"not the described bytes").expect("append");
    drop(file);

    let config = config_at(dir.path(), &objects, "http://192.0.2.20:1");
    let output = run(&[
        "--config",
        &config.display().to_string(),
        "--once",
        "--dry-run",
        &dir.path().join("rows").display().to_string(),
    ]);

    assert_eq!(output.status.code(), Some(1), "a failed object is reported");
    let text = stderr(&output);
    assert!(text.contains("hashes to"), "{text}");
    assert!(
        text.contains("loaded 1 object(s)"),
        "the other object still loaded: {text}"
    );
    assert!(text.contains("1 unloaded"), "{text}");
}
