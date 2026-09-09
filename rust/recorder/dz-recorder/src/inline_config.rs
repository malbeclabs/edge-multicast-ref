//! Inline mode's second configuration file, and what the recorder refuses on
//! it.
//!
//! **This file is what selects inline mode**, and the selection itself is not
//! here: it is [`crate::startup::Arrangement::selected_by`], which reads this
//! file's presence beside the recorder configuration's two archive directories.
//! The refusals for a configuration stating both arrangements or neither live
//! there with it, because the condition they read is the condition that selects
//! — a second reading of it downstream is a reading that can disagree.
//!
//! What is here is what inline mode refuses *once it is the arrangement*: a
//! spool it cannot open, a ledger it cannot separate from the spool, a budget
//! that divides to nothing, and a feed whose market data rows were asked for.
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
use dz_recorder_load::MarketDataFeed;
#[cfg(feature = "inline")]
use serde::{Deserialize, Serialize};

/// What inline mode refuses to start on.
///
/// Each variant names the key an operator has to edit, because a refusal that
/// describes a state rather than a key leaves them reading the whole file.
#[derive(Debug, Error)]
pub enum InlineConfigError {
    /// Only a build that has to make this refusal carries it: with the feature
    /// compiled in there is no configuration that can reach it.
    ///
    /// Reached by a configuration that *states* inline mode — a second file
    /// given and no archive directory — and not by silence, which is refused
    /// before this by [`crate::startup::StartupError::NoArrangementStated`]. So
    /// the operator reading it asked for this arrangement on purpose, and what
    /// they need is the feature rather than an alternative they did not choose.
    #[cfg(not(feature = "inline"))]
    #[error(
        "`--inline-config` states inline mode, and this build has no inline mode compiled in. \
         The `inline` feature is in the default set, so this binary was built without the default \
         features; rebuild with them, or with `--features inline`. A build that cannot derive \
         rows can still record an archive: drop `--inline-config` and state `[archive] \
         staging_dir` and `completed_dir` instead. Falling back to that unasked would leave this \
         host keeping bytes where rows were asked for, which is the arrangement nobody chose."
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

    /// The budget is not zero, but dividing it between the feeds makes it zero.
    ///
    /// The same failure as [`Self::SpoolBudgetIsZero`], reached by arithmetic
    /// rather than by a literal, and therefore the one an operator will
    /// actually hit: the host's budget is stated once and the feeds share the
    /// disk, so two feeds and a small budget give each of them nothing. Archive
    /// mode refuses the same shape with `StagingBudgetTooSmall`, and this is
    /// that refusal for the arrangement that stages rows instead of bytes.
    ///
    /// Zero is the floor this can check and not a chosen one. A window's rows
    /// are derived from datagrams, so how many bytes one costs is not knowable
    /// from the configuration — inventing a multiple of the window bound would
    /// be inventing a number. What is knowable is that a per-feed budget of
    /// zero evicts every window as it is written.
    #[cfg(feature = "inline")]
    #[error(
        "`inline.spool_max` is {spool_max} bytes across {feeds} feed(s), which is 0 bytes each. \
         Every window would be evicted as soon as its rows were written, and the destination \
         would stay empty while the recorder looked healthy. The budget is the host's and the \
         feeds share the disk, so state it for all of them together."
    )]
    SpoolBudgetTooSmall { spool_max: u64, feeds: usize },

    #[cfg(feature = "inline")]
    #[error(
        "`inline.ledger` is required. Without it a restart re-posts every window the spool still \
         holds, and every one of those is a replace paid for rows already in the store. It is \
         not what keeps the era anchor certain across a restart, and nothing is: a restart is a \
         run boundary, the window sequence begins again at zero, and the first window of a run \
         anchors on nothing."
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

    /// Market data rows asked for in the arrangement that derives none.
    ///
    /// **The entry exists so that this refusal can.** Without a
    /// `[[market_data]]` section there was no way to ask, and a feed pointed at
    /// inline mode left `event`, `instrument` and `book_top` empty —
    /// indistinguishable from a feed nobody published on, and with nothing
    /// anywhere saying it would be. Answering the ask is what this is for; the
    /// answer happens to be no.
    ///
    /// Refused rather than derived for two reasons, both in the design. Market
    /// data derivation is a codec walk, and nothing in the record path decodes a
    /// datagram — the rule that makes the transport grains trustworthy, since a
    /// message a decoder would reject still carries the sequence number whose
    /// absence is the finding. And a definition is in force from the instant it
    /// was received, so resolving a price message needs the definitions seen
    /// before it: per window, that is state spanning the unit the spool exists to
    /// bound.
    #[cfg(feature = "inline")]
    #[error(
        "`[[market_data]]` names feed `{feed}`, and inline mode derives no market data rows. \
         `event`, `instrument` and `book_top` would stay empty for it, which reads as a feed \
         nobody published on — so this is refused rather than accepted and ignored. Deriving them \
         is a codec walk, and nothing in this arrangement's record path decodes a datagram. Run \
         archive mode for this feed and name it in the loader's own `[[market_data]]`, or remove \
         the entry to derive the transport rows and no others."
    )]
    MarketDataNotDerived { feed: String },

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
    /// The feeds whose market data rows were asked for — **every one of which
    /// is refused**.
    ///
    /// A section that exists in order to be refused, which wants its reason
    /// stated rather than assumed. Inline mode derives the five transport
    /// grains and none of the three market data ones, and that emptiness is not
    /// what this refuses: archive mode leaves the same three tables empty for a
    /// feed with no entry in the *loader's* configuration, and defends it there.
    /// What inline mode had that archive mode does not is **no way to ask and
    /// nothing saying so** — three permanently empty tables, and a key an
    /// operator could not have written to find out.
    ///
    /// So the ask is spelled the way archive mode spells it, using the loader's
    /// own [`MarketDataFeed`] rather than a second type: one spelling of *which
    /// feeds derive market data*, so the two arrangements cannot grow two. An
    /// entry here is [`InlineConfigError::MarketDataNotDerived`], at `--check`,
    /// before a socket is bound.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub market_data: Vec<MarketDataFeed>,
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
        // Before the destination and after everything cheap, because it is a
        // refusal about what this host will *derive* rather than about whether a
        // path is usable — and an operator who asked for market data rows should
        // be told so whether or not the column store is up.
        if let Some(derived) = self.market_data.first() {
            return Err(InlineConfigError::MarketDataNotDerived {
                feed: derived.feed.clone(),
            });
        }
        self.clickhouse.check()?;
        Ok(())
    }

    /// The budget once it is divided between the feeds that share the disk.
    ///
    /// Separate from [`check`](Self::check) because it needs a number that file
    /// does not carry: how many feeds the recorder's own configuration enables.
    /// `spool_max` is the host's budget, stated once, exactly as `staging_max`
    /// is in archive mode — and divided the same way.
    ///
    /// # Errors
    ///
    /// [`InlineConfigError::SpoolBudgetTooSmall`] when the division leaves a
    /// feed nothing.
    pub fn check_budget_covers(&self, feeds: usize) -> Result<(), InlineConfigError> {
        if feeds > 0 && self.inline.spool_max / feeds as u64 == 0 {
            return Err(InlineConfigError::SpoolBudgetTooSmall {
                spool_max: self.inline.spool_max,
                feeds,
            });
        }
        Ok(())
    }

    /// What `--check` prints: what this arrangement keeps, and what was read
    /// rather than what an operator believes they wrote.
    ///
    /// **The mode line is not here, and that is deliberate.** It is printed
    /// before the plan by [`run`], because archive mode prints its own before
    /// the plan too and an operator comparing two hosts must find the same line
    /// in the same place. Written here it would arrive in the middle of one
    /// arrangement's output and at the top of the other's, or twice.
    ///
    /// The identity is not repeated here either. The plan prints it, and
    /// `--check` prints the plan first: an operator reading two `site=` lines
    /// has to work out whether the two files disagree, and the answer is that
    /// they cannot — inline mode takes the identity from the recorder's own file
    /// and this one has no key for it.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
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
        // Stated, and stated in both arrangements. Three tables that are empty
        // because of what this host derives are indistinguishable from three
        // tables that are empty because nobody published — so the arrangement
        // says which, beside the mode line it is read with, rather than leaving
        // an operator to find out by querying.
        let _ = writeln!(out, "{INLINE_MARKET_DATA}");
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

/// What the summary says about the three market data grains.
///
/// Printed in both arrangements, beside the mode line, because an empty table
/// says nothing about *why* it is empty: a feed nobody published on and a feed
/// this host derives no market data for look the same in the column store, and
/// only one of them is a finding. Archive mode's counterpart names the loader,
/// which is the process that decides there.
#[cfg(feature = "inline")]
pub const INLINE_MARKET_DATA: &str = "market_data=none: `event`, `instrument` and `book_top` are \
                                      not derived in this arrangement; archive mode derives them \
                                      in dz-recorder-load";

/// Inline mode, from the two files to the refusals to `--check`.
///
/// `path` is the file that selected this arrangement, so it is always `Some`
/// here: `Arrangement::selected_by` answered `Inline` because it was given, and
/// a configuration with no file selects archive mode or is refused. It is taken
/// as an `Option` because the no-feature form below has to accept the same
/// arguments, and asserted rather than assumed.
///
/// # Errors
///
/// [`InlineConfigError`], naming the key or the file. A build without the
/// feature refuses here and never falls back to archive mode: a host silently
/// recording bytes where rows were asked for is in the arrangement nobody
/// chose.
///
/// # Panics
///
/// If `path` is `None`, which is a caller that reached inline mode without the
/// file that selects it.
#[cfg(feature = "inline")]
pub fn run(
    recorder: &RecorderConfig,
    path: Option<&Path>,
    check: bool,
    run_for: Option<std::time::Duration>,
) -> Result<(), InlineConfigError> {
    // Not a refusal: the file is what selected this arrangement, so its absence
    // here is a dispatch that has stopped agreeing with the selection rather
    // than anything an operator can have written. The refusal for a
    // configuration that states no arrangement is `NoArrangementStated`, made
    // before this is reached.
    let path = path.expect("inline mode is selected by the file, so the file is in hand");

    let text = std::fs::read_to_string(path).map_err(|source| InlineConfigError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let config = InlineConfig::parse(&text, path)?;
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
    // After the plan, because it needs the feed count, and before anything is
    // opened, because `--check` is where a host learns this rather than after
    // a night of derived rows nobody kept.
    config.check_budget_covers(plan.feeds.len())?;
    // Where the recorder's own summary goes, and for the same reason: `--check`
    // is a result a pipeline reads on stdout, and a recording run's summary is
    // a log line beside the version it prints on startup.
    let summary = config.summary();
    // The mode first, in both arrangements. Archive mode prints its own line
    // before the plan, and the one thing an operator scanning two hosts must
    // not have to hunt for is which of them is keeping the bytes — which is
    // also the thing a command line can get wrong by saying nothing.
    if check {
        println!("{INLINE_MODE}");
        print!("{}", plan.summary());
        print!("{summary}");
    } else {
        eprintln!("dz-recorder: {}", crate::cli::version_line());
        eprintln!("{INLINE_MODE}");
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
/// Always [`InlineConfigError::NotCompiledIn`]. It fails at startup rather than
/// falling back to archive mode, which would put a host in the arrangement
/// nobody chose — and the message names the two directories, which are how the
/// arrangement this binary *can* run is stated.
#[cfg(not(feature = "inline"))]
pub fn run(
    _recorder: &RecorderConfig,
    _path: Option<&Path>,
    _check: bool,
    _run_for: Option<std::time::Duration>,
) -> Result<(), InlineConfigError> {
    Err(InlineConfigError::NotCompiledIn)
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

    /// **Asking for market data rows is answered, and the answer is a refusal.**
    ///
    /// The point of the section is that the question can be put at all. Before
    /// it, a feed pointed at this arrangement had `event`, `instrument` and
    /// `book_top` permanently empty, indistinguishable from a feed nobody
    /// published on, and no key an operator could have written to find out. The
    /// refusal names the feed, so an operator with several knows which entry to
    /// take out, and names archive mode, which is the arrangement that derives
    /// them.
    #[test]
    fn a_feed_whose_market_data_rows_were_asked_for_is_refused_by_name() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| {
            format!("{text}\n[[market_data]]\nfeed = \"top-of-book\"\nmagic = 62721\n")
        });
        assert!(message.contains("top-of-book"), "{message}");
        assert!(message.contains("book_top"), "{message}");
        assert!(message.contains("archive mode"), "{message}");
    }

    /// And the section is the loader's own type, so the two arrangements cannot
    /// grow two spellings of the same question.
    ///
    /// Asserted through `deny_unknown_fields`: `persist_snapshot_levels` is a
    /// `MarketDataFeed` key and parses, and a key that is not one is a parse
    /// error rather than a field silently dropped. A second type here would
    /// drift from the loader's one key at a time, and the first sign of it
    /// would be an operator copying a working entry between two files and
    /// having it rejected.
    #[test]
    fn the_market_data_entry_is_the_loaders_own_type() {
        let fixture = Fixture::new();
        let message = fixture.refusal(|text| {
            format!(
                "{text}\n[[market_data]]\nfeed = \"depth\"\nmagic = 62722\n\
                 persist_snapshot_levels = true\n"
            )
        });
        assert!(message.contains("depth"), "{message}");

        let message = fixture.refusal(|text| {
            format!("{text}\n[[market_data]]\nfeed = \"depth\"\nmagic = 62722\nlevels = true\n")
        });
        assert!(message.contains("levels"), "{message}");
    }

    /// **The summary says the tables are empty on purpose**, and says it whether
    /// or not anybody asked.
    ///
    /// A refusal only reaches an operator who tried. This line reaches the one
    /// who did not, which is the one who would otherwise find three empty tables
    /// and read them as a feed nobody published on.
    #[test]
    fn the_summary_states_that_no_market_data_rows_are_derived() {
        let fixture = Fixture::new();
        let summary = fixture.config().summary();
        assert!(summary.contains("market_data=none"), "{summary}");
        assert!(summary.contains("book_top"), "{summary}");
        assert!(summary.contains("dz-recorder-load"), "{summary}");
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
        // The reason that is true: a restart with no ledger re-posts what the
        // spool still holds.
        assert!(message.contains("re-posts every window"), "{message}");
        // And the one that is not. A run begins at `window_seq` zero and the
        // predecessor test is `segment_seq + 1`, so no ledger makes the first
        // window of a run certain of its era boundary — a refusal promising it
        // reads to an operator as a guarantee this arrangement keeps.
        assert!(
            !message.contains("keeps the era anchor certain across the restart"),
            "the refusal promises a certainty a restart cannot give: {message}"
        );
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

    /// A directory with a value in it beside a second file is two arrangements
    /// stated at once, and the refusal names both statements.
    ///
    /// The refusal lives in `startup` and not here, because the condition it
    /// reads is the condition that *selects* the arrangement — a second reading
    /// of the same two keys downstream of the selection is a reading that can
    /// disagree with it. What this asserts is that inline mode is reachable only
    /// through that selection.
    #[test]
    fn an_archive_directory_beside_the_second_file_is_refused_by_key() {
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
            let message = crate::startup::Arrangement::selected_by(
                &config,
                Some(std::path::Path::new("inline.toml")),
            )
            .expect_err("an archive directory beside a second file states both")
            .to_string();
            assert!(message.contains(key), "{message}");
            assert!(message.contains("inline.toml"), "{message}");
        }
    }

    /// A recorder configuration with no `[archive]` section, plus the second
    /// file, is what inline mode takes — and the file is what states it.
    #[test]
    fn a_recorder_file_with_no_archive_section_and_the_file_is_inline_mode() {
        let config = recorder_config(RECORDER);
        assert_eq!(
            crate::startup::Arrangement::selected_by(
                &config,
                Some(std::path::Path::new("inline.toml"))
            )
            .expect("no directory is configured and the file is given"),
            crate::startup::Arrangement::Inline
        );
        crate::startup::Plan::for_inline(&config)
            .expect("the identity is stated and the feed wires");
    }

    /// The other half of that: a configuration valid for archive mode is still
    /// valid for archive mode, and selects it with no flag at all.
    #[test]
    fn a_configuration_valid_for_archive_mode_is_still_valid_and_selects_it() {
        let config = crate::startup::tests::valid_config();
        crate::startup::Plan::from_config(&config).expect("archive mode is unchanged");
        assert_eq!(
            crate::startup::Arrangement::selected_by(&config, None)
                .expect("the two directories state archive mode"),
            crate::startup::Arrangement::Archive,
            "an archive host selects its arrangement by saying what it already said"
        );
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

    /// The summary says what was read, and leaves the mode line to its caller.
    ///
    /// `run` prints `INLINE_MODE` before the plan so that it is the first line
    /// in both arrangements, as archive mode's is. Written here as well it would
    /// appear twice, and written *only* here it appears in the middle of one
    /// arrangement's output and at the top of the other's — which is the shape
    /// this test exists to keep out.
    #[test]
    fn the_summary_leaves_the_mode_line_to_the_caller_that_prints_it_first() {
        let fixture = Fixture::new();
        let summary = fixture.config().summary();
        assert!(
            !summary.contains("mode=inline"),
            "the mode line is printed before the plan, and twice is worse than late: {summary}"
        );
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

    /// A budget that survives one feed and not two is refused, by arithmetic.
    ///
    /// The literal `spool_max = 0` was already refused, and it is the case
    /// nobody writes. This is the one they hit: a host budget that looked ample
    /// until a second feed was added to the same file, leaving each of them
    /// nothing and the destination empty while the recorder reported itself
    /// healthy.
    #[test]
    fn a_budget_that_divides_to_nothing_is_refused_and_names_the_arithmetic() {
        let fixture = Fixture::new();
        let config = fixture.config();

        config
            .check_budget_covers(1)
            .expect("the fixture's budget covers one feed");

        // A budget smaller than the number of feeds is the only shape that can
        // divide to zero, and it is reachable by adding feeds rather than by
        // editing the budget.
        let text = fixture
            .text()
            .replace(r#"spool_max       = "8GiB""#, r#"spool_max       = "1B""#);
        let tight = InlineConfig::parse(&text, std::path::Path::new("inline.toml"))
            .expect("a one-byte budget parses; it is the division that refuses it");
        tight
            .check_budget_covers(1)
            .expect("one feed still gets the whole byte");
        let message = tight
            .check_budget_covers(2)
            .expect_err("two feeds share it and neither gets a byte")
            .to_string();
        assert!(message.contains("2 feed"), "{message}");
        assert!(message.contains("0 bytes each"), "{message}");
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
    /// The mode is behind a build feature so that a `--no-default-features`
    /// recorder gains no HTTP client, no column-store crate and no row crates.
    /// What reaches this build is a configuration that *selects* inline mode —
    /// `--inline-config` given — so the refusal names the feature and points at
    /// the two directories that select the arrangement this binary can run.
    ///
    /// It is never reached by silence: a configuration stating no arrangement is
    /// refused earlier by `NoArrangementStated`, in this build as in every
    /// other. So the operator reading this asked for inline mode on purpose.
    #[test]
    fn a_build_without_inline_mode_refuses_a_configuration_selecting_it() {
        let config =
            RecorderConfig::parse(crate::startup::tests::VALID).expect("the fixture parses");
        // With the file, which is what selects inline mode and therefore the
        // only way this refusal is reached.
        let message = run(&config, Some(Path::new("inline.toml")), true, None)
            .expect_err("a build without the feature refuses inline mode")
            .to_string();
        assert!(message.contains("--features inline"), "{message}");
        assert!(message.contains("staging_dir"), "{message}");
        assert!(message.contains("nobody chose"), "{message}");

        // And the same without one, because this form of `run` cannot tell the
        // two apart and must not start either way: a build that cannot run the
        // mode cannot run it with a file or without.
        let message = run(&config, None, true, None)
            .expect_err("a build without the feature refuses inline mode")
            .to_string();
        assert!(message.contains("--features inline"), "{message}");
    }
}
