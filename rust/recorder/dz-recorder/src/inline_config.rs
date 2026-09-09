//! Inline mode's second configuration file, and what the recorder refuses on
//! it.
//!
//! **The record path's own file gains no key from any of this.**
//! `RecorderConfig` documents the absence of an endpoint, a credential and a
//! database key, and its `config_hash` is written into every object and every
//! coverage row as provenance. A destination there would make rotating a
//! password change what an archive says produced it, though nothing about what
//! the recorder captured or wrote had changed — and adding any key at all would
//! change the hash of every configuration in the fleet. So the destination, the
//! window bound, the ring, the spool and the ledger live here, in the file the
//! loader's own configuration already sets the precedent for.
//!
//! **`site` and `recorder` are deliberately not keys here.** They come from the
//! recorder's own file and from nowhere else. That is the one thing the
//! two-process arrangement cannot guarantee: two files can name one host
//! differently, and then the live panel and the historical panel of one
//! dashboard describe two recorders that do not exist.
//!
//! Every struct carries `deny_unknown_fields`. A misspelled section that parses
//! cleanly and falls back to a default is how a host loads into the wrong
//! database while the operator believes otherwise.

use std::path::Path;
#[cfg(feature = "inline")]
use std::path::PathBuf;
#[cfg(feature = "inline")]
use std::time::Duration;

use dz_recorder_core::RecorderConfig;
use thiserror::Error;

#[cfg(feature = "inline")]
use dz_recorder_clickhouse::{ClickHouseConfig, ClickHouseSink};
#[cfg(feature = "inline")]
use serde::{Deserialize, Serialize};

/// What inline mode refuses to start on.
///
/// Each variant names the key an operator has to edit, because a refusal that
/// describes a state rather than a key leaves them reading the whole file.
#[derive(Debug, Error)]
pub enum InlineConfigError {
    /// Only a build that has to make this refusal carries it: with the feature
    /// compiled in there is no command line that can reach it.
    #[cfg(not(feature = "inline"))]
    #[error(
        "`--inline-config` was given, and this build has no inline mode compiled in. Rebuild with \
         `--features inline`, which brings in the column-store client and the row crates that a \
         default build deliberately does not carry. Recording an archive instead would leave this \
         host in the arrangement nobody chose, keeping bytes where rows were asked for."
    )]
    NotCompiledIn,

    /// A feed the plan refused. Inline mode joins what archive mode joins, so
    /// every feed refusal is made here too — and it is carried through rather
    /// than restated, so an operator reads one wording whichever arrangement
    /// they are running.
    #[cfg(feature = "inline")]
    #[error("{0}")]
    Startup(#[from] crate::startup::StartupError),

    /// The record path failed. Carried through rather than restated, so a
    /// failure reads the same whichever arrangement produced it.
    #[cfg(feature = "inline")]
    #[error("{0}")]
    Run(crate::runner::RunError),

    #[cfg(feature = "inline")]
    #[error("reading {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[cfg(feature = "inline")]
    #[error("{path}: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },

    #[cfg(feature = "inline")]
    #[error(
        "`archive.{key}` = `{path}`, and inline mode writes no object. Nothing would ever be \
         written there and nothing would ever read from it, so a host configured this way is \
         believed to be keeping bytes it never kept for a second. Remove the `[archive]` \
         directories, or drop `--inline-config` and record an archive."
    )]
    ArchiveDirectoryConfigured { key: &'static str, path: String },

    #[cfg(feature = "inline")]
    #[error(
        "`inline.{key}` is 0, and a bound of zero is not a bound: the window would close on the \
         first datagram it admitted, which posts an insert per datagram and turns this recorder's \
         output into the destination's merge work."
    )]
    WindowBoundIsZero { key: &'static str },

    #[cfg(feature = "inline")]
    #[error(
        "`inline.ring_datagrams` is 0. Every datagram the capture accepted would be dropped on \
         the way to the derivation and charged to this recorder as its own loss, and the rows \
         would describe a feed nobody published on."
    )]
    RingHoldsNothing,

    #[cfg(feature = "inline")]
    #[error(
        "`inline.spool_dir` is required and there is no defensible host path to invent for it. \
         It is where a window's rows land before the column store has taken them, and it is what \
         bounds a crash to the open window."
    )]
    NoSpoolDir,

    #[cfg(feature = "inline")]
    #[error(
        "`inline.spool_dir` = `{path}` is not a directory this process can open. The spool is \
         this mode's whole durability: a recorder that could not write it would hold every row it \
         derived in memory and call itself healthy, and a crash would take rows no archive can \
         return."
    )]
    SpoolDirUnusable { path: String },

    #[cfg(feature = "inline")]
    #[error(
        "`inline.spool_max` is 0 bytes. Every window would be evicted as soon as its rows were \
         written, and the destination would stay empty while the recorder looked healthy."
    )]
    SpoolBudgetIsZero,

    #[cfg(feature = "inline")]
    #[error(
        "`inline.ledger` is required. Without it a restart re-posts every window the spool still \
         holds, and loses the trailer that keeps the era anchor certain across the restart."
    )]
    NoLedger,

    #[cfg(feature = "inline")]
    #[error(
        "`inline.ledger` = `{ledger}` is inside `inline.spool_dir` = `{spool}`, for the reason \
         the loader's ledger may not live inside its objects directory: the spool's byte budget \
         classifies what it finds and evicts the oldest window, so a file it cannot classify is \
         a file eviction cannot reach — and the ledger would be counted as history or deleted as \
         it. Put it on this process's own writable path."
    )]
    LedgerInsideSpool { ledger: String, spool: String },

    #[cfg(feature = "inline")]
    #[error("{0}")]
    ClickHouse(#[from] dz_recorder_clickhouse::ConfigError),

    #[cfg(feature = "inline")]
    #[error(
        "the destination could not be reached, so no row derived here would land: {0}. The \
         password comes from DZ_LOADER_CLICKHOUSE_PASSWORD_FILE, or second best from \
         DZ_LOADER_CLICKHOUSE_PASSWORD, and from nowhere else."
    )]
    Unreachable(String),
}

/// One inline-mode host's second file.
#[cfg(feature = "inline")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InlineConfig {
    pub inline: Inline,
    /// The destination, reused verbatim from the crate the loader already
    /// points at its own: one type, one set of defaults, and one place a
    /// credential comes from. Two spellings of *where the rows go* is how two
    /// halves of one host end up loading into two databases.
    pub clickhouse: ClickHouseConfig,
}

/// What the mode itself is bounded by.
///
/// Every value here bounds something the archive's own keys bound for objects,
/// and none of them is an archive key: a window is a derivation and not a file,
/// so `[archive] rotate_bytes` could not be reused without making one number
/// govern two units.
#[cfg(feature = "inline")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Inline {
    /// The window closes on this many bytes of datagrams, or on
    /// [`window_interval`](Self::window_interval), whichever comes first —
    /// which is what `[archive] rotate_bytes` and `rotate_interval` decide for
    /// an object, and is written the same way, with a unit.
    ///
    /// Smaller than a rotation bound by default, because the two bounds pay for
    /// different things. An object's size is a disk-sizing question; a window's
    /// is how long a row waits to be queryable, which is the loop bringing up a
    /// feed runs. The destination coalesces across windows on its own, so a
    /// small window costs the column store no extra parts.
    #[serde(with = "byte_size")]
    pub window_bytes: u64,
    /// The age bound, so a quiet feed's rows keep moving. A feed nobody
    /// published on and a feed whose window never closed look identical in a
    /// dashboard.
    #[serde(with = "duration_unit")]
    pub window_interval: Duration,
    /// The ring between the capture and the derivation, in datagram slots.
    ///
    /// A count of datagrams and not a byte budget, and the key says so: the
    /// slots are pooled at the capture length, so a count is the number an
    /// operator can multiply out, and a value read as bytes would be a ring of
    /// a few datagrams on a host that asked for a large one.
    ///
    /// It is how far the derivation may fall behind the capture before a
    /// datagram is dropped and its loss charged to the next one admitted. The
    /// busiest feed measured on a live recorder is 224,000 datagrams a minute,
    /// so the default is on the order of two seconds of that feed.
    pub ring_datagrams: usize,
    /// Where a window's rows are written before the destination has taken them.
    ///
    /// Empty by default, which is not a usable path: rows reach disk on every
    /// window rather than only under an outage, so this directory is the
    /// mode's durability and there is no defensible host path to invent for it.
    pub spool_dir: PathBuf,
    /// The spool's byte budget. When it is full the oldest window is evicted
    /// and counted, and the derivation is never blocked — a spool that applied
    /// backpressure would stall the derivation, fill the ring, overflow the
    /// receive queue and convert a column-store outage into feed loss.
    ///
    /// Sized against the outage the host intends to survive. Rows are about two
    /// orders of magnitude smaller than the datagrams they describe, so this
    /// buys far more time per byte than the staging budget it replaces. Alert
    /// on the age of the oldest unposted window and never on the eviction
    /// count: a full budget evicts on every pass at steady state by design.
    #[serde(with = "byte_size")]
    pub spool_max: u64,
    /// What has landed, so that a restart resumes with the certainty a
    /// continuous run had. On this process's own writable path and never inside
    /// [`spool_dir`](Self::spool_dir).
    pub ledger: PathBuf,
}

#[cfg(feature = "inline")]
impl Default for Inline {
    fn default() -> Self {
        Self {
            window_bytes: 16 * 1024 * 1024,
            window_interval: Duration::from_secs(10),
            ring_datagrams: 8192,
            spool_dir: PathBuf::new(),
            spool_max: 8 * 1024 * 1024 * 1024,
            ledger: PathBuf::new(),
        }
    }
}

#[cfg(feature = "inline")]
impl InlineConfig {
    /// Load from TOML text.
    ///
    /// # Errors
    ///
    /// [`InlineConfigError::Parse`], naming the offending key.
    pub fn parse(text: &str, path: &Path) -> Result<Self, InlineConfigError> {
        toml::from_str(text).map_err(|source| InlineConfigError::Parse {
            path: path.to_path_buf(),
            source,
        })
    }

    /// Everything checkable without touching the network, the spool or the
    /// ledger.
    ///
    /// # Errors
    ///
    /// [`InlineConfigError`], naming the key. This is what `--check` runs,
    /// against a host that may already be recording.
    pub fn check(&self) -> Result<(), InlineConfigError> {
        if self.inline.window_bytes == 0 {
            return Err(InlineConfigError::WindowBoundIsZero {
                key: "window_bytes",
            });
        }
        if self.inline.window_interval.is_zero() {
            return Err(InlineConfigError::WindowBoundIsZero {
                key: "window_interval",
            });
        }
        if self.inline.ring_datagrams == 0 {
            return Err(InlineConfigError::RingHoldsNothing);
        }
        if self.inline.spool_dir.as_os_str().is_empty() {
            return Err(InlineConfigError::NoSpoolDir);
        }
        // What is proven here is that the directory exists and is this
        // process's to open. Writability is proven by writing, and `--check`
        // may not: in inline mode the directory it would write into is a spool
        // another process may be reading, and a probe file is exactly the file
        // the budget cannot classify. The spool's first window is what proves
        // the rest, and it fails loudly.
        if !self.inline.spool_dir.is_dir() || std::fs::read_dir(&self.inline.spool_dir).is_err() {
            return Err(InlineConfigError::SpoolDirUnusable {
                path: self.inline.spool_dir.display().to_string(),
            });
        }
        if self.inline.spool_max == 0 {
            return Err(InlineConfigError::SpoolBudgetIsZero);
        }
        if self.inline.ledger.as_os_str().is_empty() {
            return Err(InlineConfigError::NoLedger);
        }
        // Textual and not canonicalised, as the archive's own two-directories
        // check is: the ledger need not exist yet, and the case being guarded
        // is one configuration naming one path inside the other.
        if self.inline.ledger.starts_with(&self.inline.spool_dir) {
            return Err(InlineConfigError::LedgerInsideSpool {
                ledger: self.inline.ledger.display().to_string(),
                spool: self.inline.spool_dir.display().to_string(),
            });
        }
        self.clickhouse.check()?;
        Ok(())
    }

    /// What `--check` prints: which arrangement is running, what it keeps, and
    /// what was read rather than what an operator believes they wrote.
    #[must_use]
    /// The identity is not repeated here.
    ///
    /// The plan prints it, and `--check` prints the plan first: an operator
    /// reading two `site=` lines has to work out whether the two files disagree,
    /// and the answer is that they cannot — inline mode takes the identity from
    /// the recorder's own file and this one has no key for it.
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        // In the words the design uses: the two modes keep different things,
        // and this is the line that says which one this host is.
        let _ = writeln!(out, "{INLINE_MODE}");
        let _ = writeln!(
            out,
            "inline window={}B or {:?} ring={} datagrams",
            self.inline.window_bytes, self.inline.window_interval, self.inline.ring_datagrams,
        );
        let _ = writeln!(
            out,
            "spool dir={} budget={}B ledger={}",
            self.inline.spool_dir.display(),
            self.inline.spool_max,
            self.inline.ledger.display(),
        );
        let _ = writeln!(
            out,
            "destination={} database={} user={}",
            self.clickhouse.endpoint, self.clickhouse.database, self.clickhouse.user
        );
        out
    }
}

/// What the summary says inline mode is, wherever the summary is read.
///
/// The sentence an operator has to leave with is the one about datagrams: this
/// mode's rows cannot be re-derived and a rule written next month has nothing
/// to run against, and neither fact is visible in a list of keys.
#[cfg(feature = "inline")]
pub const INLINE_MODE: &str =
    "mode=inline: rows are derived from the live capture and NO DATAGRAM IS KEPT";

/// Inline mode, from the two files to the refusals to `--check`.
///
/// # Errors
///
/// [`InlineConfigError`], naming the key. A build without the feature refuses
/// here and never falls back to archive mode: a host silently recording bytes
/// where rows were asked for is in the arrangement nobody chose.
#[cfg(feature = "inline")]
pub fn run(
    recorder: &RecorderConfig,
    path: &Path,
    check: bool,
    run_for: Option<std::time::Duration>,
) -> Result<(), InlineConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| InlineConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let config = InlineConfig::parse(&text, path)?;

    // The other file's refusal first: an operator who left `[archive]` in place
    // has asked for two arrangements at once, and being told about the spool
    // before being told about that would answer the smaller question.
    check_archive_is_not_configured(recorder)?;
    config.check()?;

    // **The feeds are planned here, not only the two files.** Inline mode joins
    // exactly what archive mode joins, so every feed refusal archive mode makes
    // has to be made here too — a group that is not multicast, a port role
    // claimed twice, an interface that does not resolve. Letting one through
    // would let it through on the arrangement that leaves an operator the least
    // to diagnose it with: there is no archive to go back to.
    //
    // `for_inline` rather than `from_config`, because the archive directories
    // this refuses a value for are the ones that one requires one of.
    let plan = crate::startup::Plan::for_inline(recorder)?;
    // Where the recorder's own summary goes, and for the same reason: `--check`
    // is a result a pipeline reads on stdout, and a recording run's summary is
    // a log line beside the version it prints on startup.
    let summary = config.summary();
    if check {
        print!("{}", plan.summary());
        print!("{summary}");
    } else {
        eprintln!("dz-recorder: {}", crate::cli::version_line());
        eprint!("{}", plan.summary());
        eprint!("{summary}");
    }

    if check {
        // Nothing is bound, nothing is created and nothing is joined, and the
        // spool and the ledger are not touched: this runs in a deployment
        // pipeline, against a host that may already be recording.
        let sink = ClickHouseSink::over_http(config.clickhouse.clone());
        let probe = sink
            .statement("SELECT 1")
            .map_err(|e| InlineConfigError::Unreachable(e.to_string()))?;
        println!("destination answered: {}", probe.trim());
        println!("configuration is valid");
        return Ok(());
    }

    crate::inline_runner::run(&plan, &config, run_for).map_err(InlineConfigError::Run)
}

/// The refusal a build with no inline mode makes, in place of everything above.
///
/// # Errors
///
/// Always [`InlineConfigError::NotCompiledIn`]. A configuration asking for what
/// the binary cannot do fails at startup rather than falling back to archive
/// mode.
#[cfg(not(feature = "inline"))]
pub fn run(
    _recorder: &RecorderConfig,
    _path: &Path,
    _check: bool,
    _run_for: Option<std::time::Duration>,
) -> Result<(), InlineConfigError> {
    Err(InlineConfigError::NotCompiledIn)
}

/// An archive directory configured in the mode that writes no object.
///
/// Refused rather than ignored: nothing would ever be written there, and a host
/// whose configuration names a staging directory is a host somebody believes is
/// keeping bytes for a year that it never kept for a second.
#[cfg(feature = "inline")]
fn check_archive_is_not_configured(config: &RecorderConfig) -> Result<(), InlineConfigError> {
    for (key, dir) in [
        ("staging_dir", &config.archive.staging_dir),
        ("completed_dir", &config.archive.completed_dir),
    ] {
        if !dir.as_os_str().is_empty() {
            return Err(InlineConfigError::ArchiveDirectoryConfigured {
                key,
                path: dir.display().to_string(),
            });
        }
    }
    Ok(())
}

/// The identity every row carries, checked here because inline mode reads it
/// from the recorder's own file and builds no archive plan to check it for.
#[cfg(feature = "inline")]
/// Sizes carry a unit — `"16MiB"` — because both plausible readings of a bare
/// number are wrong, one of them by orders of magnitude.
///
/// Spelled exactly as `[archive] rotate_bytes` and `staging_max` are, so an
/// operator learns one syntax for both files. The configuration crate's own
/// parser is private to its deserializer, which is why this is not a call into
/// it.
#[cfg(feature = "inline")]
mod byte_size {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    const TIB: u64 = 1024 * GIB;

    pub fn serialize<S: Serializer>(value: &u64, ser: S) -> Result<S::Ok, S::Error> {
        // The base unit, so that two spellings of one size reach a reader as
        // the same bytes and the canonical form is still a configuration this
        // parser accepts.
        ser.serialize_str(&format!("{value}B"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<u64, D::Error> {
        let raw = String::deserialize(de)?;
        parse(&raw).map_err(D::Error::custom)
    }

    fn parse(raw: &str) -> Result<u64, String> {
        let (value, unit) = super::split_unit(raw)?;
        let scale = match unit {
            "B" => 1,
            "KiB" => KIB,
            "MiB" => MIB,
            "GiB" => GIB,
            "TiB" => TIB,
            _ => {
                return Err(format!(
                    "`{unit}` is not a size unit (B, KiB, MiB, GiB, TiB)"
                ))
            }
        };
        value
            .checked_mul(scale)
            .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit byte count"))
    }
}

/// Durations carry a unit — `"10s"` — for the same reason sizes do, and are
/// spelled as `[archive] rotate_interval` is.
#[cfg(feature = "inline")]
mod duration_unit {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&format!("{}ns", value.as_nanos()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
        let raw = String::deserialize(de)?;
        parse(&raw).map_err(D::Error::custom)
    }

    fn parse(raw: &str) -> Result<Duration, String> {
        let (value, unit) = super::split_unit(raw)?;
        let nanos = match unit {
            "ns" => 1,
            "us" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3_600 * 1_000_000_000,
            _ => {
                return Err(format!(
                    "`{unit}` is not a duration unit (ns, us, ms, s, m, h)"
                ))
            }
        };
        value
            .checked_mul(nanos)
            .map(Duration::from_nanos)
            .ok_or_else(|| format!("`{raw}` does not fit in a 64-bit nanosecond count"))
    }
}

/// Split a value written as digits immediately followed by a unit.
#[cfg(feature = "inline")]
fn split_unit(raw: &str) -> Result<(u64, &str), String> {
    let text = raw.trim();
    let boundary = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{text}` has no unit"))?;
    let (digits, unit) = text.split_at(boundary);
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a whole number followed by a unit"))?;
    Ok((value, unit))
}

#[cfg(all(test, feature = "inline"))]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The second file that every refusal below is a single edit away from.
    /// Documentation-range addresses only: this repository is public.
    const VALID: &str = r#"
[inline]
window_bytes    = "16MiB"
window_interval = "10s"
ring_datagrams  = 8192
spool_dir       = "SPOOL"
spool_max       = "8GiB"
ledger          = "LEDGER"

[clickhouse]
endpoint = "http://192.0.2.20:8123"
database = "recorder"
user     = "dz_loader"
"#;

    /// A recorder configuration with no `[archive]` section, which is what
    /// inline mode takes: the mode writes no object, so the directories an
    /// archive needs are refused when they carry a value.
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

    struct Fixture {
        _dir: tempfile::TempDir,
        spool: PathBuf,
        ledger: PathBuf,
    }

    impl Fixture {
        /// A real spool directory, because the refusal for one that is not
        /// there is itself under test and a fixture pointing at a path nobody
        /// created would pass every other assertion for the wrong reason.
        fn new() -> Self {
            let dir = tempfile::tempdir().expect("a temporary directory");
            let spool = dir.path().join("spool");
            std::fs::create_dir(&spool).expect("the spool directory is creatable");
            let ledger = dir.path().join("ledger.jsonl");
            Self {
                _dir: dir,
                spool,
                ledger,
            }
        }

        fn text(&self) -> String {
            VALID
                .replace("SPOOL", &self.spool.display().to_string())
                .replace("LEDGER", &self.ledger.display().to_string())
        }

        fn config(&self) -> InlineConfig {
            InlineConfig::parse(&self.text(), Path::new("inline.toml")).expect("the fixture parses")
        }

        /// The refusal, as an operator reads it.
        fn refusal(&self, edit: impl FnOnce(String) -> String) -> String {
            InlineConfig::parse(&edit(self.text()), Path::new("inline.toml"))
                .map_err(|e| e.to_string())
                .and_then(|config| config.check().map_err(|e| e.to_string()))
                .expect_err("this configuration must be refused")
        }
    }

    fn recorder_config(text: &str) -> RecorderConfig {
        RecorderConfig::parse(text).expect("the fixture parses")
    }

    #[test]
    fn a_complete_second_file_is_accepted() {
        let fixture = Fixture::new();
        let config = fixture.config();
        config
            .check()
            .expect("the fixture is one a recorder starts on");
        assert_eq!(config.inline.window_bytes, 16 * 1024 * 1024);
        assert_eq!(config.inline.window_interval, Duration::from_secs(10));
        assert_eq!(config.inline.ring_datagrams, 8192);
        assert_eq!(config.clickhouse.database, "recorder");
    }

    /// A misspelled key that parsed cleanly and fell back to a default is how a
    /// host loads into the wrong database while the operator believes
    /// otherwise.
    #[test]
    fn a_misspelled_key_is_refused_rather_than_defaulted() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| text.replace("spool_max", "spool_maxx"));
        assert!(message.contains("spool_maxx"), "{message}");
        let message = fixture.refusal(|text| text.replace("[inline]", "[inlnie]"));
        assert!(message.contains("inlnie"), "{message}");
    }

    /// The identity is the recorder's, and naming it here would be the second
    /// place a host can be named — which is the failure this file exists to
    /// prevent.
    #[test]
    fn site_and_recorder_are_not_keys_in_this_file() {
        let fixture = Fixture::new();
        for key in ["site", "recorder"] {
            let message = fixture
                .refusal(|text| text.replace("[inline]", &format!("[inline]\n{key} = \"x\"")));
            assert!(message.contains(key), "{message}");
        }
        // And they are absent from what this file serialises to, so a key added
        // later is covered without anybody remembering to come back here.
        let text = toml::to_string(&fixture.config()).expect("serialisable");
        assert!(!text.contains("site"), "{text}");
        assert!(!text.contains("recorder ="), "{text}");
    }

    /// The invariant the record path holds, held here too: no endpoint, no
    /// credential and no database key over there, and no password key here.
    #[test]
    fn there_is_no_password_key_anywhere_in_this_file() {
        let fixture = Fixture::new();
        let text = toml::to_string(&fixture.config()).expect("serialisable");
        for forbidden in ["password", "secret", "token", "credential"] {
            assert!(!text.contains(forbidden), "`{forbidden}` in: {text}");
        }
    }

    #[test]
    fn a_window_bound_of_zero_is_refused_by_key() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| text.replace(r#""16MiB""#, r#""0B""#));
        assert!(message.contains("inline.window_bytes"), "{message}");
        let message = fixture.refusal(|text| text.replace(r#""10s""#, r#""0s""#));
        assert!(message.contains("inline.window_interval"), "{message}");
    }

    #[test]
    fn a_ring_that_holds_nothing_is_refused_by_key() {
        let fixture = Fixture::new();
        let message =
            fixture.refusal(|text| text.replace("ring_datagrams  = 8192", "ring_datagrams  = 0"));
        assert!(message.contains("inline.ring_datagrams"), "{message}");
        assert!(message.contains("its own loss"), "{message}");
    }

    #[test]
    fn a_spool_directory_is_required_and_is_not_guessed_at() {
        let fixture = Fixture::new();
        let message =
            fixture.refusal(|text| text.replace(&fixture.spool.display().to_string(), ""));
        assert!(message.contains("inline.spool_dir"), "{message}");
        // The unstated case and the unusable one are different refusals, and an
        // operator who wrote nothing has to be told there is nothing to invent
        // rather than that a path they never wrote could not be opened.
        assert!(message.contains("no defensible host path"), "{message}");
    }

    #[test]
    fn a_spool_directory_that_is_not_there_is_refused_by_key() {
        let fixture = Fixture::new();
        let message = fixture
            .refusal(|text| text.replace(&fixture.spool.display().to_string(), "/nope/not/here"));
        assert!(message.contains("inline.spool_dir"), "{message}");
        assert!(message.contains("/nope/not/here"), "{message}");
    }

    #[test]
    fn a_spool_budget_of_zero_is_refused_by_key() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| text.replace(r#""8GiB""#, r#""0B""#));
        assert!(message.contains("inline.spool_max"), "{message}");
        assert!(message.contains("evicted as soon as"), "{message}");
    }

    #[test]
    fn a_ledger_is_required() {
        let fixture = Fixture::new();
        let message =
            fixture.refusal(|text| text.replace(&fixture.ledger.display().to_string(), ""));
        assert!(message.contains("inline.ledger"), "{message}");
    }

    /// A file the budget cannot classify is a file eviction cannot reach.
    #[test]
    fn a_ledger_inside_the_spool_is_refused_by_key() {
        let fixture = Fixture::new();
        let inside = fixture.spool.join("ledger.jsonl");
        let message = fixture.refusal(|text| {
            text.replace(
                &fixture.ledger.display().to_string(),
                &inside.display().to_string(),
            )
        });
        assert!(message.contains("inline.ledger"), "{message}");
        assert!(message.contains("inline.spool_dir"), "{message}");
        assert!(message.contains("eviction cannot reach"), "{message}");
    }

    /// The destination's own checks reach the same error path, so one `--check`
    /// covers both files' worth of keys.
    #[test]
    fn the_destinations_own_checks_are_part_of_this_one() {
        let fixture = Fixture::new();
        let message =
            fixture.refusal(|text| text.replace("http://192.0.2.20:8123", "192.0.2.20:8123"));
        assert!(message.contains("http://"), "{message}");
    }

    /// Nothing writes an object in inline mode, so a directory with a value in
    /// it is an operator expecting an archive they will not get.
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
            let config = recorder_config(&format!("{RECORDER}\n{section}"));
            let message = check_archive_is_not_configured(&config)
                .expect_err("an archive directory in inline mode is refused")
                .to_string();
            assert!(message.contains(key), "{message}");
            assert!(message.contains("never kept"), "{message}");
        }
    }

    /// A recorder configuration with no `[archive]` section is what inline mode
    /// takes, and the absence is the statement.
    #[test]
    fn a_recorder_file_with_no_archive_section_is_what_inline_mode_takes() {
        let config = recorder_config(RECORDER);
        check_archive_is_not_configured(&config).expect("no directory is configured");
        crate::startup::Plan::for_inline(&config)
            .expect("the identity is stated and the feed wires");
    }

    /// The other half of that: a configuration valid for archive mode is still
    /// valid for archive mode, and this module cannot have changed it.
    #[test]
    fn a_configuration_valid_for_archive_mode_is_still_valid() {
        let config = crate::startup::tests::valid_config();
        crate::startup::Plan::from_config(&config).expect("archive mode is unchanged");
        // And the same file is refused for inline mode, by the key that says
        // why: it names directories nothing would ever write to.
        let message = check_archive_is_not_configured(&config)
            .expect_err("an archive configuration is not an inline one")
            .to_string();
        assert!(message.contains("archive.staging_dir"), "{message}");
    }

    /// The identity is refused by the plan, in the wording archive mode uses.
    ///
    /// One refusal rather than two: inline mode takes `site`, `recorder` and
    /// `env` from the recorder's own file — which is what stops one host's live
    /// rows and archived rows naming two recorders — so the check belongs where
    /// that file is validated, and an operator reads the same sentence whichever
    /// arrangement they are running.
    #[test]
    fn the_identity_the_rows_carry_is_required_by_key() {
        for (key, line, blanked) in [
            ("site", r#"site     = "site-a""#, r#"site     = """#),
            ("recorder", r#"recorder = "recorder-1""#, r#"recorder = """#),
            ("env", r#"env      = "test""#, r#"env      = """#),
        ] {
            let config = recorder_config(&RECORDER.replace(line, blanked));
            let message = crate::startup::Plan::for_inline(&config)
                .expect_err("the identity every row carries is required")
                .to_string();
            assert!(message.contains(key), "{key}: {message}");
        }
    }

    #[test]
    fn the_summary_says_which_mode_is_running_and_that_no_datagram_is_kept() {
        let fixture = Fixture::new();
        let summary = fixture.config().summary();
        assert!(summary.contains("mode=inline"), "{summary}");
        assert!(summary.contains("NO DATAGRAM IS KEPT"), "{summary}");
        assert!(summary.contains("database=recorder"), "{summary}");
        assert!(summary.contains("user=dz_loader"), "{summary}");
        assert!(!summary.to_lowercase().contains("password"), "{summary}");
    }

    /// The identity appears once in what `--check` prints, and it is the plan's.
    ///
    /// Two `site=` lines would have an operator working out whether the two
    /// files disagree — and the answer is that they cannot, because this file
    /// has no key for it. A summary that repeated the identity would invent a
    /// question the configuration makes unaskable.
    #[test]
    fn the_inline_summary_does_not_repeat_the_identity_the_plan_prints() {
        let fixture = Fixture::new();
        let config = recorder_config(RECORDER);
        let inline = fixture.config().summary();
        let plan = crate::startup::Plan::for_inline(&config)
            .expect("the fixture is one inline mode can start on")
            .summary();

        assert!(plan.contains("site=site-a recorder=recorder-1"), "{plan}");
        assert!(
            !inline.contains("site=site-a"),
            "the identity is the plan's, and printing it twice is what this guards: {inline}"
        );
        assert!(
            !inline.contains("config hash="),
            "and so is the provenance hash: {inline}"
        );
    }

    /// `--check` in inline mode prints nothing about an archive.
    ///
    /// The keys are refused outright, so a rotation bound and a staging budget
    /// of zero would have an operator reading the output for a problem that is
    /// the absence of a thing this arrangement does not do.
    #[test]
    fn the_inline_plan_summary_names_no_archive() {
        let config = recorder_config(RECORDER);
        let summary = crate::startup::Plan::for_inline(&config)
            .expect("the fixture is one inline mode can start on")
            .summary();
        assert!(!summary.contains("archive rotate="), "{summary}");
        assert!(!summary.contains("staging budget="), "{summary}");
        assert!(!summary.contains("staging="), "{summary}");
        assert!(!summary.contains("completed="), "{summary}");
        // And the feed is still there: what is dropped is the archive, not the
        // thing an operator came to check.
        assert!(summary.contains("feed top-of-book"), "{summary}");
    }

    /// A size or a duration without a unit is refused rather than guessed at,
    /// as the recorder's own file refuses one.
    #[test]
    fn a_size_or_a_duration_without_a_unit_is_refused() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| text.replace(r#""16MiB""#, "16"));
        assert!(message.contains("window_bytes"), "{message}");
        let message = fixture.refusal(|text| text.replace(r#""10s""#, r#""10""#));
        assert!(message.contains("no unit"), "{message}");
    }

    /// The example ships with the crate and an operator copies it, so it has to
    /// be a file this binary accepts rather than prose that looks like one.
    #[test]
    fn the_example_is_a_configuration_this_binary_parses() {
        let config = example();
        assert!(config.inline.window_interval > Duration::ZERO);
        assert!(!config.inline.spool_dir.as_os_str().is_empty());
        assert!(
            !config.inline.ledger.starts_with(&config.inline.spool_dir),
            "the example puts the ledger where eviction cannot reach it"
        );
        // The two keys this file must not have, checked against the file an
        // operator copies rather than only against the struct.
        let text = std::fs::read_to_string(example_path()).expect("the example ships");
        for forbidden in ["password =", "site =", "recorder ="] {
            assert!(!text.contains(forbidden), "`{forbidden}` in the example");
        }
    }

    /// The example names the account the checked-in DDL creates.
    ///
    /// Two files that have to agree and nothing that would notice when they
    /// stop — the same trap the loader's example fell into. It is not a
    /// configuration error, so `check` passes it, and it surfaces as an
    /// authentication failure against a destination that has to be reachable
    /// before anything can say the name was wrong.
    #[test]
    fn the_example_names_the_account_the_ddl_creates() {
        let sql = dz_recorder_clickhouse::migrations()
            .into_iter()
            .find(|migration| migration.name == "004_recorder_loader_user.sql")
            .expect("the account migration ships with the client")
            .sql;
        let account = sql
            .lines()
            .map(str::trim)
            .filter(|line| !line.starts_with("--"))
            .find_map(|line| line.strip_prefix("CREATE USER "))
            .map(|rest| {
                rest.strip_prefix("IF NOT EXISTS ")
                    .unwrap_or(rest)
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_owned()
            })
            .expect("`004` creates an account");
        assert_eq!(
            example().clickhouse.user,
            account,
            "the example points at an account the DDL does not create"
        );
    }

    fn example_path() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("inline.example.toml")
    }

    fn example() -> InlineConfig {
        let path = example_path();
        let text = std::fs::read_to_string(&path).expect("the example ships with the crate");
        InlineConfig::parse(&text, &path).expect("the example parses")
    }
}

#[cfg(all(test, not(feature = "inline")))]
mod tests {
    use super::*;

    /// A build that cannot derive rows must not record an archive instead.
    ///
    /// The mode is behind a build feature so that a default recorder gains no
    /// HTTP client, no column-store crate and no row crates. A configuration
    /// asking for what this binary cannot do fails here, naming the feature.
    #[test]
    fn a_build_without_inline_mode_refuses_the_flag_and_names_the_feature() {
        let config =
            RecorderConfig::parse(crate::startup::tests::VALID).expect("the fixture parses");
        let message = run(&config, Path::new("inline.toml"), true, None)
            .expect_err("a build without the feature refuses inline mode")
            .to_string();
        assert!(message.contains("--features inline"), "{message}");
        assert!(message.contains("nobody chose"), "{message}");
    }
}
