//! `--check` as a deployment pipeline actually runs it: the binary, a file, and
//! the exit code and the message an operator gets back.
//!
//! The unit tests cover the refusals as values. This covers them as an
//! experience — that the process exits non-zero, that the message reaches
//! stderr and names the key, and that a valid configuration touches nothing.
//! Every address here is documentation-range: this repository is public.
//!
//! **Every check here passes `--archive`**, because that is now what archive
//! mode takes: inline mode is what a command line naming no mode is read as.
//! The one test that leaves the flag off is
//! [`an_archive_configuration_with_no_mode_named_is_refused_and_names_the_flag`],
//! and it asserts a refusal.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BINARY: &str = env!("CARGO_BIN_EXE_dz-recorder");

const VALID: &str = r#"
site     = "site-a"
recorder = "recorder-1"
env      = "test"

[[feed]]
spec            = "top-of-book"
multicast_group = "233.252.0.1"
interface       = "192.0.2.7"
mktdata_port    = 41000
refdata_port    = 41001

[capture]
mode   = "socket"
buffer = "8MiB"

[archive]
staging_dir     = "/var/lib/dz-recorder/staging"
completed_dir   = "/var/lib/dz-recorder/completed"
rotate_bytes    = "16MiB"
rotate_interval = "60s"
compression     = "zstd"
staging_max     = "1GiB"

[metrics]
listen_addr = "127.0.0.1:0"
"#;

struct Ran {
    output: Output,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn code(&self) -> i32 {
        self.output
            .status
            .code()
            .expect("the process was not signalled")
    }
}

fn run(args: &[&str]) -> Ran {
    let output = Command::new(BINARY)
        .args(args)
        .output()
        .expect("the binary under test runs");
    Ran {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        output,
    }
}

fn config_in(dir: &Path, text: &str) -> PathBuf {
    let path = dir.join("recorder.toml");
    std::fs::write(&path, text).expect("a configuration in a temporary directory");
    path
}

/// Runs `--check` over a configuration made by editing the valid one.
///
/// `--archive`, because the configuration is an archive-mode one: the flag is
/// how that arrangement is asked for, and without it every one of these
/// refusals would be replaced by the one about a configured staging directory.
fn check(edit: impl FnOnce(&str) -> String) -> Ran {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = config_in(dir.path(), &edit(VALID));
    run(&[
        "--config",
        path.to_str().expect("a utf-8 path"),
        "--archive",
        "--check",
    ])
}

/// **Archive mode takes a flag now, and the configuration that used to be
/// enough on its own is refused rather than reinterpreted.**
///
/// This is the whole safety argument of the inverted default, at the altitude
/// an operator meets it. The fixture describes an archive — it names
/// `staging_dir` and `completed_dir`, and with `--archive` it checks out clean
/// two tests below. Without the flag it must not start, must not print
/// `mode=archive`, and must say what to add: the host this happens to is one
/// recording a production feed for evidence, on the restart nobody was
/// watching, and a log line would have been read weeks later as a year of
/// retention that was never kept.
#[test]
fn an_archive_configuration_with_no_mode_named_is_refused_and_names_the_flag() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    let path = config_in(dir.path(), VALID);
    let ran = run(&["--config", path.to_str().expect("a utf-8 path"), "--check"]);

    assert_eq!(ran.code(), 1, "{}{}", ran.stdout, ran.stderr);
    assert!(
        !ran.stdout.contains("mode=archive"),
        "a command line naming no mode was read as archive mode: {}",
        ran.stdout
    );
    assert!(
        ran.stdout.is_empty(),
        "a refusal is not a result: {}",
        ran.stdout
    );
    // The flag to add. This message is the migration instruction as much as the
    // refusal, and it is the only place that instruction is certain to be read.
    assert!(ran.stderr.contains("--archive"), "{}", ran.stderr);

    if cfg!(feature = "inline") {
        // And the key the operator actually wrote: inline mode writes no
        // object, so a staging directory carrying a value is what identifies
        // this configuration as the other arrangement's.
        assert!(ran.stderr.contains("archive.staging_dir"), "{}", ran.stderr);
        assert!(ran.stderr.contains("never kept"), "{}", ran.stderr);
    } else {
        // A `--no-default-features` build never reaches the key: it refuses the
        // default mode by the feature's name first, which is the more useful
        // answer for a binary that cannot derive a row at all.
        assert!(ran.stderr.contains("--features inline"), "{}", ran.stderr);
    }
}

#[test]
fn a_valid_configuration_checks_out_and_creates_nothing() {
    let dir = tempfile::tempdir().expect("a temporary directory");
    // The archive directories are inside the temporary one and writable, so
    // that a check which created them would be visible here. Pointed somewhere
    // this process cannot write, the assertion below would hold whether or not
    // anything had been created.
    let staging = dir.path().join("staging");
    let completed = dir.path().join("completed");
    let text = VALID
        .replace("/var/lib/dz-recorder/staging", staging.to_str().unwrap())
        .replace(
            "/var/lib/dz-recorder/completed",
            completed.to_str().unwrap(),
        );
    let path = config_in(dir.path(), &text);

    let ran = run(&["--config", path.to_str().unwrap(), "--archive", "--check"]);
    assert_eq!(ran.code(), 0, "{}", ran.stderr);
    assert!(
        ran.stdout.contains("configuration is valid"),
        "{}",
        ran.stdout
    );
    // The first line, and asserted as the first line: inline mode's `--check`
    // prints its own mode line first too, and an operator scanning two hosts
    // must find the same statement in the same place. Spelled out here because
    // this crate has no library target to import the constant from.
    assert_eq!(
        ran.stdout.lines().next(),
        Some(
            "mode=archive: every datagram is written to an object, and the loader derives the rows"
        ),
        "the mode is not the first line of what --check printed:\n{}",
        ran.stdout
    );
    // The check runs in a pipeline, against a host that may already be
    // recording. Nothing it does may touch that host's disk.
    assert!(!staging.exists(), "--check created {}", staging.display());
    assert!(
        !completed.exists(),
        "--check created {}",
        completed.display()
    );
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .expect("the temporary directory")
        .filter_map(Result::ok)
        .map(|e| e.file_name())
        .collect();
    assert_eq!(entries.len(), 1, "{entries:?}");
}

#[test]
fn the_check_states_the_mode_the_drop_scope_and_the_provenance() {
    let ran = check(ToOwned::to_owned);
    assert!(ran.stdout.contains("capture mode=socket"), "{}", ran.stdout);
    assert!(
        ran.stdout.contains("drop scope=port-role"),
        "{}",
        ran.stdout
    );
    assert!(ran.stdout.contains("build version="), "{}", ran.stdout);
    assert!(ran.stdout.contains("config hash="), "{}", ran.stdout);
}

#[test]
fn gzip_is_refused_loudly_and_by_key() {
    let ran =
        check(|text| text.replace(r#"compression     = "zstd""#, r#"compression     = "gzip""#));
    assert_eq!(ran.code(), 1);
    assert!(ran.stderr.contains("archive.compression"), "{}", ran.stderr);
    assert!(
        ran.stderr.contains("no writer implements it"),
        "{}",
        ran.stderr
    );
    assert!(
        ran.stdout.is_empty(),
        "a refusal is not a result: {}",
        ran.stdout
    );
}

#[test]
fn an_empty_staging_or_completed_directory_is_refused_by_key() {
    let ran = check(|text| {
        text.replace(
            r#"staging_dir     = "/var/lib/dz-recorder/staging""#,
            r#"staging_dir     = """#,
        )
    });
    assert_eq!(ran.code(), 1);
    assert!(ran.stderr.contains("archive.staging_dir"), "{}", ran.stderr);

    let ran = check(|text| {
        text.replace(
            r#"completed_dir   = "/var/lib/dz-recorder/completed""#,
            r#"completed_dir   = """#,
        )
    });
    assert_eq!(ran.code(), 1);
    assert!(
        ran.stderr.contains("archive.completed_dir"),
        "{}",
        ran.stderr
    );
}

#[test]
fn a_misspelled_key_is_refused_rather_than_defaulted() {
    // `deny_unknown_fields` is the configuration crate's, and this is the
    // binary proving it survives the trip: a misspelled section that parsed
    // cleanly and fell back to a default is how a host records the wrong thing
    // while an operator believes otherwise.
    let ran = check(|text| text.replace("rotate_bytes", "rotate_byte"));
    assert_eq!(ran.code(), 1);
    assert!(ran.stderr.contains("rotate_byte"), "{}", ran.stderr);
}

#[test]
fn a_configuration_that_is_not_there_names_the_path() {
    let ran = run(&["--config", "/nonexistent/recorder.toml", "--check"]);
    assert_eq!(ran.code(), 1);
    assert!(
        ran.stderr.contains("/nonexistent/recorder.toml"),
        "{}",
        ran.stderr
    );
}

#[test]
fn a_command_line_that_cannot_be_understood_exits_differently_from_a_refusal() {
    // A deployment pipeline tells "I invoked it wrongly" from "the recorder
    // will not start on this configuration", and does something different
    // about each.
    let ran = run(&["--nope"]);
    assert_eq!(ran.code(), 2, "{}", ran.stderr);
    assert!(ran.stderr.contains("--nope"), "{}", ran.stderr);
    assert!(ran.stderr.contains("Usage:"), "{}", ran.stderr);

    let ran = run(&[]);
    assert_eq!(ran.code(), 2, "{}", ran.stderr);
    assert!(
        ran.stderr.contains("--config is required"),
        "{}",
        ran.stderr
    );
}

#[test]
fn the_version_names_the_build_and_admits_an_unknown_commit() {
    let ran = run(&["--version"]);
    assert_eq!(ran.code(), 0);
    assert!(ran.stdout.starts_with("dz-recorder "), "{}", ran.stdout);
    // Either the pipeline stamped a commit or the build says it does not know
    // one. What must never appear is an empty parenthesis, which reads as a
    // field somebody forgot rather than as an answer.
    assert!(!ran.stdout.contains("()"), "{}", ran.stdout);
}

#[test]
fn help_says_a_signal_stops_a_recorder_without_losing_the_open_segment() {
    let ran = run(&["--help"]);
    assert_eq!(ran.code(), 0);
    assert!(ran.stdout.contains("--run-for"), "{}", ran.stdout);
    assert!(ran.stdout.contains("Stopping it"), "{}", ran.stdout);
}
