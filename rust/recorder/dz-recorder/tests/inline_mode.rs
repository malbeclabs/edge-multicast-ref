//! Inline mode as a deployment pipeline meets it: the binary, two files, and
//! the exit code and the message an operator gets back.
//!
//! The unit tests cover the refusals as values. This covers them as an
//! experience — that the process exits non-zero, that the message reaches
//! stderr naming the key, that a build without the mode refuses the flag rather
//! than recording an archive, and that `--check` touches neither the spool nor
//! the ledger. Nothing here needs a socket, a privilege or a server: every
//! address is documentation-range, and the destination is one nothing answers
//! on.
#![forbid(unsafe_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const BINARY: &str = env!("CARGO_BIN_EXE_dz-recorder");

/// A recorder configuration for inline mode: no `[archive]` section at all,
/// because nothing in this mode writes an object.
const RECORDER: &str = r#"
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

[metrics]
listen_addr = "127.0.0.1:0"
"#;

/// Inline mode's own file. The destination is a documentation address nothing
/// answers on, and the timeout is a second: the default half-minute wait would
/// make this suite thirty times slower than the thing it is testing.
const INLINE: &str = r#"
[inline]
window_bytes    = "16MiB"
window_interval = "10s"
ring_datagrams  = 8192
spool_dir       = "SPOOL"
spool_max       = "8GiB"
ledger          = "LEDGER"

[clickhouse]
endpoint = "http://192.0.2.20:1"
database = "recorder"
user     = "dz_loader"
timeout  = 1
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
        // An inherited password would be a variable this suite did not mean to
        // set, and nothing here reaches a destination anyway.
        .env_remove("DZ_LOADER_CLICKHOUSE_PASSWORD")
        .env_remove("DZ_LOADER_CLICKHOUSE_PASSWORD_FILE")
        .output()
        .expect("the binary under test runs");
    Ran {
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        output,
    }
}

/// The two files, with a real spool directory beside them.
struct Host {
    dir: tempfile::TempDir,
    spool: PathBuf,
    ledger: PathBuf,
}

impl Host {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let spool = dir.path().join("spool");
        std::fs::create_dir(&spool).expect("the spool directory is creatable");
        let ledger = dir.path().join("ledger.jsonl");
        Self { dir, spool, ledger }
    }

    fn write(&self, name: &str, text: &str) -> PathBuf {
        let path = self.dir.path().join(name);
        std::fs::write(&path, text).expect("a configuration in a temporary directory");
        path
    }

    /// `--check` over the two files, each made by editing the valid one.
    fn check(
        &self,
        recorder: impl FnOnce(&str) -> String,
        inline: impl FnOnce(&str) -> String,
    ) -> Ran {
        let recorder = self.write("recorder.toml", &recorder(RECORDER));
        let inline_text = inline(
            &INLINE
                .replace("SPOOL", &self.spool.display().to_string())
                .replace("LEDGER", &self.ledger.display().to_string()),
        );
        let inline = self.write("inline.toml", &inline_text);
        run(&[
            "--config",
            path_str(&recorder),
            "--inline-config",
            path_str(&inline),
            "--check",
        ])
    }

    /// Nothing this mode would write at run time exists yet.
    fn nothing_was_written(&self) {
        assert!(!self.ledger.exists(), "--check created the ledger");
        let spooled: Vec<_> = std::fs::read_dir(&self.spool)
            .expect("the spool directory")
            .filter_map(Result::ok)
            .map(|e| e.file_name())
            .collect();
        assert!(
            spooled.is_empty(),
            "--check wrote into the spool: {spooled:?}"
        );
    }
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("a utf-8 path")
}

/// A build without the mode refuses the flag by name and never records an
/// archive instead.
///
/// The mode is behind a build feature so that a default recorder gains no
/// column-store crate, no HTTP client and no row crates. Falling back would put
/// a host in the arrangement nobody chose, keeping bytes where rows were asked
/// for — and it would look like a working recorder while doing it.
#[cfg(not(feature = "inline"))]
#[test]
fn a_build_without_the_mode_refuses_the_flag_and_names_the_feature() {
    let host = Host::new();
    let ran = host.check(ToOwned::to_owned, ToOwned::to_owned);
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(ran.stderr.contains("--features inline"), "{}", ran.stderr);
    assert!(
        ran.stdout.is_empty(),
        "a refusal is not a result: {}",
        ran.stdout
    );
    host.nothing_was_written();

    // And recording, not only checking: the refusal is what stops a supervisor
    // starting this unit and getting archive mode, which is the arrangement
    // nobody chose. A run that fell back would say `mode=archive` here.
    let recorder = host.write("recorder.toml", RECORDER);
    let ran = run(&[
        "--config",
        path_str(&recorder),
        "--inline-config",
        path_str(&host.dir.path().join("inline.toml")),
        "--run-for",
        "1s",
    ]);
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(ran.stderr.contains("--features inline"), "{}", ran.stderr);
    assert!(!ran.stderr.contains("mode=archive"), "{}", ran.stderr);
}

/// `--check` validates both files, says what it read, and reaches for the
/// destination.
///
/// The destination here is a documentation address nothing answers on, so the
/// probe fails — which is the assertion: `--check` is a gate, and a gate that
/// passed without reaching the destination would let a pipeline restart a
/// recorder that cannot write a row anywhere.
#[cfg(feature = "inline")]
#[test]
fn check_validates_both_files_reaches_for_the_destination_and_touches_nothing() {
    let host = Host::new();
    let ran = host.check(ToOwned::to_owned, ToOwned::to_owned);
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(
        ran.stderr.contains("could not be reached"),
        "{}",
        ran.stderr
    );
    // And it says where the password comes from, because that is the other half
    // of what makes a destination unreachable.
    assert!(
        ran.stderr.contains("DZ_LOADER_CLICKHOUSE_PASSWORD"),
        "{}",
        ran.stderr
    );

    // What was read is printed, so an operator sees it rather than what they
    // believe they wrote — starting with which arrangement this host is in.
    assert!(ran.stdout.contains("mode=inline"), "{}", ran.stdout);
    assert!(ran.stdout.contains("NO DATAGRAM IS KEPT"), "{}", ran.stdout);
    assert!(
        ran.stdout.contains("site=site-a recorder=recorder-1"),
        "{}",
        ran.stdout
    );
    assert!(ran.stdout.contains("config hash="), "{}", ran.stdout);
    assert!(ran.stdout.contains("database=recorder"), "{}", ran.stdout);
    assert!(
        !ran.stdout.to_lowercase().contains("password"),
        "{}",
        ran.stdout
    );

    host.nothing_was_written();
}

/// Nothing writes an object in inline mode, so a configured archive directory
/// is an operator expecting bytes they will not get.
#[cfg(feature = "inline")]
#[test]
fn an_archive_directory_in_inline_mode_is_refused_by_key() {
    for (key, section) in [
        (
            "archive.staging_dir",
            "[archive]\nstaging_dir = \"/var/lib/dz-recorder/staging\"\n",
        ),
        (
            "archive.completed_dir",
            "[archive]\ncompleted_dir = \"/var/lib/dz-recorder/completed\"\n",
        ),
    ] {
        let host = Host::new();
        let ran = host.check(|text| format!("{text}\n{section}"), ToOwned::to_owned);
        assert_eq!(ran.code(), 1, "{}", ran.stderr);
        assert!(ran.stderr.contains(key), "{}", ran.stderr);
        assert!(ran.stderr.contains("never kept"), "{}", ran.stderr);
        host.nothing_was_written();
    }
}

/// A file the spool's budget cannot classify is a file eviction cannot reach.
#[cfg(feature = "inline")]
#[test]
fn a_ledger_inside_the_spool_is_refused_by_key() {
    let host = Host::new();
    let inside = host.spool.join("ledger.jsonl");
    let ran = host.check(ToOwned::to_owned, |text| {
        text.replace(
            &host.ledger.display().to_string(),
            &inside.display().to_string(),
        )
    });
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(ran.stderr.contains("inline.ledger"), "{}", ran.stderr);
    assert!(ran.stderr.contains("inline.spool_dir"), "{}", ran.stderr);
    host.nothing_was_written();
}

/// The spool is this mode's durability, and a directory nobody created is a
/// recorder that would hold every row in memory and call itself healthy.
#[cfg(feature = "inline")]
#[test]
fn a_spool_directory_that_is_not_there_is_refused_by_key() {
    let host = Host::new();
    let ran = host.check(ToOwned::to_owned, |text| {
        text.replace(&host.spool.display().to_string(), "/nope/not/here")
    });
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(ran.stderr.contains("inline.spool_dir"), "{}", ran.stderr);
    assert!(ran.stderr.contains("/nope/not/here"), "{}", ran.stderr);
}

/// A misspelled section that parsed cleanly and fell back to a default is how a
/// host loads into the wrong database while an operator believes otherwise.
#[cfg(feature = "inline")]
#[test]
fn a_misspelled_key_in_the_second_file_is_refused_rather_than_defaulted() {
    let host = Host::new();
    let ran = host.check(ToOwned::to_owned, |text| {
        text.replace("spool_max", "spool_maxx")
    });
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(ran.stderr.contains("spool_maxx"), "{}", ran.stderr);
}

/// A second file that is not there names the path, rather than starting in the
/// mode that keeps bytes.
#[cfg(feature = "inline")]
#[test]
fn a_second_file_that_is_not_there_names_the_path() {
    let host = Host::new();
    let recorder = host.write("recorder.toml", RECORDER);
    let ran = run(&[
        "--config",
        path_str(&recorder),
        "--inline-config",
        "/nonexistent/inline.toml",
        "--check",
    ]);
    assert_eq!(ran.code(), 1, "{}", ran.stderr);
    assert!(
        ran.stderr.contains("/nonexistent/inline.toml"),
        "{}",
        ran.stderr
    );
}

/// A configuration valid for archive mode is still valid for archive mode, in
/// both builds, and the summary says which arrangement it is.
#[test]
fn a_configuration_valid_for_archive_mode_is_still_valid_and_says_so() {
    let host = Host::new();
    let text = format!(
        "{RECORDER}\n[archive]\nstaging_dir = \"{staging}\"\ncompleted_dir = \"{completed}\"\n\
         rotate_bytes = \"16MiB\"\nrotate_interval = \"60s\"\ncompression = \"zstd\"\n\
         staging_max = \"1GiB\"\n",
        staging = host.dir.path().join("staging").display(),
        completed = host.dir.path().join("completed").display(),
    );
    let recorder = host.write("recorder.toml", &text);
    let ran = run(&["--config", path_str(&recorder), "--check"]);
    assert_eq!(ran.code(), 0, "{}", ran.stderr);
    assert!(ran.stdout.contains("mode=archive"), "{}", ran.stdout);
    assert!(
        ran.stdout.contains("configuration is valid"),
        "{}",
        ran.stdout
    );
    assert!(
        !ran.stdout.contains("mode=inline"),
        "archive mode is not inline mode: {}",
        ran.stdout
    );
}
