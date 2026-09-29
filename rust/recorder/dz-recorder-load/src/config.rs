//! The loader's own configuration, which is not the recorder's.
//!
//! **The record path gains no key from any of this.** `RecorderConfig`
//! documents the absence of an endpoint, a credential and a database key as an
//! invariant, because the recorder does not upload — `completed_dir` is the
//! whole interface to whatever reads from it. This is the file on the other side
//! of that directory: its own process, its own service user, its own metrics
//! port and its own configuration.
//!
//! Every struct carries `deny_unknown_fields`. A misspelled section that parses
//! cleanly and falls back to a default is how a host loads into the wrong
//! database while the operator believes otherwise.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use dz_recorder_clickhouse::ClickHouseConfig;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("configuration is not valid: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("{0}")]
    ClickHouse(#[from] dz_recorder_clickhouse::ConfigError),
    #[error(
        "[loader] objects_dir is required: there is no defensible host path to invent, and a \
         loader pointed somewhere surprising reports an empty archive as a quiet feed"
    )]
    NoObjectsDir,
    #[error(
        "[loader] objects_dir `{0}` is not a directory this process can read: the loader runs \
         on the recorder host against that host's own completed directory, opened read-only"
    )]
    ObjectsDirUnreadable(PathBuf),
    #[error("[loader] ledger is required: without it a restart re-loads the whole archive")]
    NoLedger,
    #[error("[loader] site and recorder are required, so that every dz_loader_* series can say which host produced it")]
    NoIdentity,
    #[error("[loader] poll_interval must be above zero in --watch: a loader that never waits is a loader that spins")]
    NoPollInterval,
    #[error(
        "[[market_data]] feed is required: an entry that names no feed turns derivation on for \
         nothing and reads as if it had turned it on for everything"
    )]
    NoDerivedFeed,
    #[error(
        "[[market_data]] magic is required and 0 is not one: it is the only thing that stops a \
         datagram misrouted from another feed in the family being parsed at the wrong layout, \
         and a feed whose Magic nothing matches derives an empty table that reads as a quiet \
         feed"
    )]
    NoMagic(String),
    #[error(
        "[[market_data]] names `{0}` twice: which entry is in force would be whichever the \
         parser saw last, and the switch an operator believes is off may be the other one"
    )]
    DuplicateDerivedFeed(String),
    #[error(
        "[[market_data]] feed `{0}` is padded with whitespace: the name is matched against the \
         manifest's exactly, so a padded one matches no feed and derives nothing, and the \
         failure looks like a switch that is off rather than a name that is wrong"
    )]
    PaddedDerivedFeed(String),
    #[error(
        "[loader] feeds is present but names nothing. **An empty list is not a way to turn \
         the loader off**, and it is not read as one: omitting the key means every feed, so \
         `feeds = []` would be the widest setting written in the words of the narrowest. \
         Remove the key to scan every feed, or stop running the loader on this host"
    )]
    EmptyScanSet,
    #[error(
        "[loader] feeds names an empty entry: it matches no subdirectory of objects_dir, so \
         it narrows the scan by one feed and adds nothing, and the list reads as though it \
         had named something"
    )]
    NoScannedFeed,
    #[error(
        "[loader] feeds `{0}` is padded with whitespace: the name is matched against the \
         subdirectory the recorder writes, so a padded one matches no feed, that feed loads \
         nothing, and the failure looks like a feed nobody published on rather than a name \
         that is wrong"
    )]
    PaddedScannedFeed(String),
    #[error(
        "[loader] feeds names `{0}` twice: a list that says the same thing twice is a list \
         somebody edited without reading it, and the duplicate stands where the name that \
         was meant to be there is not"
    )]
    DuplicateScannedFeed(String),
    #[error(
        "[loader] feeds `{0}` is not a name the recorder can have written: a spec is ASCII \
         letters, digits, `.`, `-` and `_`, and is neither `.` nor `..` -- `check_spec` in \
         dz-recorder refuses anything else, so a name outside that set cannot be a directory \
         under completed/ and can only ever scan nothing"
    )]
    ScannedFeedIsNotASpecName(String),
    #[error(
        "[[market_data]] feed `{0}` is not named in [loader] feeds, so its objects are never \
         scanned and it derives nothing at all -- not event, and not datagram either. A \
         derivation switch that is correct and never reached fills no table, which reads \
         exactly like a feed nobody published on: name the feed in the scan set, or remove \
         the entry that says it derives"
    )]
    DerivedFeedIsNotScanned(String),
}

/// One loader host's whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoaderConfig {
    pub loader: Loader,
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// The feeds whose objects also become market data rows.
    ///
    /// **Empty is the default, and empty means no feed derives.** Derivation is
    /// per feed and off at every stage: a feed with no entry here is loaded
    /// exactly as it is loaded today, into `datagram`, `era`,
    /// `segment_coverage` and `sequence_gap` and nothing else. A global switch
    /// was refused because the cost is per feed — a snapshot cycle is
    /// `total_levels` messages per instrument on the publisher's cadence — and
    /// a switch whose blast radius is every feed on the host is a switch nobody
    /// turns on.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub market_data: Vec<MarketDataFeed>,
}

/// One feed's derivation, and the two things it cannot be guessed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketDataFeed {
    /// The manifest's `feed`, matched exactly.
    ///
    /// The feed *specification's* name and never a venue's, which is what the
    /// recorder writes into every manifest and what the row's `feed` column
    /// holds. Matching on the manifest rather than on a directory or a port is
    /// what makes the switch survive a recorder that starts carrying a second
    /// feed into the same completed directory.
    pub feed: String,
    /// The feed's `Magic`, as a number.
    ///
    /// Required and with no default, for the reason the codec's own walk
    /// requires it: it is the only thing that stops a datagram misrouted from
    /// another feed in the family being parsed at the wrong layout. There is no
    /// registry here to look it up in — the recorder never decodes, so its own
    /// configuration does not carry one either — so the operator states it, and
    /// `--check` says back which value was read.
    pub magic: u16,
    /// Whether `SnapshotLevel` messages become `event` rows.
    ///
    /// **Off by default, and the book consumes every level either way.** A cycle
    /// is `total_levels` messages per instrument on the runtime's cadence, so
    /// persisting all of them puts the largest row count in the system on the
    /// port role with the least analytical value per row. `SnapshotBegin` and
    /// `SnapshotEnd` are always written, and `total_levels` on the begin against
    /// `levels_seen` on the end answers *was the cycle complete* from rows
    /// alone — which is the one question persisting the levels would otherwise
    /// have been the only way to ask.
    #[serde(default)]
    pub persist_snapshot_levels: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Loader {
    /// Labels on every `dz_loader_*` series, and the same words the health
    /// tier's labels and the row columns use for the same things. A dashboard
    /// where the live panel and the historical panel disagree about what a
    /// recorder is teaches nobody anything.
    pub site: String,
    pub recorder: String,
    /// The recorder's `completed_dir`, opened read-only. The loader and the
    /// recorder share this directory and nothing else.
    pub objects_dir: PathBuf,
    /// Which feeds under `objects_dir` are scanned, named by the subdirectory
    /// the recorder writes them into.
    ///
    /// **Empty is the default, and empty means every feed.** A host that
    /// upgrades this binary and changes no configuration scans exactly what it
    /// scanned before. A name here is the recorder's *spec*: the same string
    /// `[[market_data]] feed` is matched against in the manifest, and the same
    /// string `dz-recorder` joins onto its own `completed_dir`.
    ///
    /// **THIS GATES THE SCAN, WHICH IS NOT WHAT THE RECORDER'S
    /// `expected_sources` GATES.** That one gates counting and alerting and
    /// never the archive. This one decides whether a row exists at all:
    /// `datagram`, `era`, `segment_coverage` and `sequence_gap` are derived for
    /// every object a pass *scans*, whatever `[[market_data]]` names. So a feed
    /// left out of a non-empty list writes no row of any grain, and no
    /// `dz_loader_*` series says so — there is no "configured feed produced
    /// nothing" signal to say it with. That is why `check` refuses a
    /// `[[market_data]]` entry this list does not carry: the deploy pipeline is
    /// the only place the mistake is visible.
    ///
    /// **It exists because the alternative to narrowing is a back-load.**
    /// Widening a host whose scan was narrowed to one feed derives every object
    /// of every other feed still on disk, at the `datagram` grain, one row per
    /// datagram, into a destination that may be rationing writes — tens of
    /// gigabytes of archive nobody agreed to spend. Naming the feeds that are
    /// wanted costs those feeds and nothing else.
    ///
    /// **A NARROWED SCAN DOES NOT READ THE TOP LEVEL OF `objects_dir`.** A
    /// recorder configured without a spec writes its objects there, and their
    /// feed is knowable only from the manifest, which the walk does not open —
    /// so reading them would put objects in the pass that the compaction scope,
    /// which is keyed on the feed, does not cover. Their ledger entries would
    /// then be kept for ever once the objects were evicted, and the ledger
    /// would grow without bound. A narrowed scan is exactly the feeds it names;
    /// omit the key on a host that writes loose objects and everything is read,
    /// as it always was.
    ///
    /// **`Option`, SO THAT `feeds = []` IS NOT THE SAME SENTENCE AS SAYING NOTHING.**
    /// Absent is every feed. An empty list is refused, because an operator who
    /// writes one means "none for now" and would be handed the widest setting
    /// there is — a full-archive scan at the `datagram` grain, which is the
    /// exact cost this key exists to avoid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feeds: Option<Vec<String>>,
    /// Where the load ledger lives. On the loader's own writable path, never
    /// inside `objects_dir`: a loader that wrote into the recorder's directory
    /// would put a file the staging budget cannot classify next to the objects
    /// eviction has to reach.
    pub ledger: PathBuf,
    /// How long `--watch` waits between passes.
    #[serde(with = "duration_secs")]
    pub poll_interval: Duration,
    /// Objects one pass will derive, so that a pass has a bound and the metrics
    /// are published between passes rather than after an unbounded catch-up.
    /// Zero is no bound.
    ///
    /// Derived and not loaded, because they are different numbers under a sink
    /// that coalesces: a pass may derive sixty objects and load none, and a
    /// bound on the loading would not have bounded that pass at all.
    pub max_objects_per_pass: usize,
}

impl Default for Loader {
    fn default() -> Self {
        Self {
            site: String::new(),
            recorder: String::new(),
            objects_dir: PathBuf::new(),
            feeds: None,
            ledger: PathBuf::new(),
            poll_interval: Duration::from_secs(30),
            max_objects_per_pass: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct MetricsConfig {
    /// Its own port, and not the recorder's: two processes cannot share one.
    ///
    /// Bind it to a non-public interface. It describes a live data path — the
    /// feeds, the sites and the timing of an archive — and exposing it publicly
    /// leaks all of that.
    pub listen_addr: SocketAddr,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            // 9100 is the recorder's, so this is the next one: a loader that
            // defaulted onto the recorder's port would fail to bind on exactly
            // the hosts it is meant to run on.
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 9101)),
        }
    }
}

/// Which rule a feed name broke, so the two lists can share the rules and
/// still report their own errors.
///
/// The keys fail differently — one derives nothing, the other scans nothing —
/// and the message is what an operator acts on, so they do not share a
/// `ConfigError`. What they share is the definition of a name.
enum FeedNameError {
    Empty,
    Padded,
    Duplicate,
}

/// Empty, padded and duplicate, checked once for both lists.
fn check_feed_name(name: &str, earlier: &[String]) -> Result<(), FeedNameError> {
    if name.trim().is_empty() {
        return Err(FeedNameError::Empty);
    }
    // Refused rather than trimmed. Trimming would make the configuration and
    // the thing it configures disagree about what the operator wrote, and it
    // would make `"a"` and `"a "` one entry for the duplicate check while
    // `--check` still echoed two.
    if name.trim() != name {
        return Err(FeedNameError::Padded);
    }
    if earlier.iter().any(|e| e == name) {
        return Err(FeedNameError::Duplicate);
    }
    Ok(())
}

/// Whether the recorder could have written a directory of this name.
///
/// The same rule as `check_spec` in `dz-recorder`'s `startup.rs`, which is what
/// creates the directory this name has to match. Kept as a copy rather than
/// shared: the loader does not depend on the recorder's crate, and the
/// alternative to a copy is a dependency edge between two binaries that share
/// one directory and nothing else.
fn is_spec_name(name: &str) -> bool {
    name != "."
        && name != ".."
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
}

impl LoaderConfig {
    /// Load from TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError::Toml`], naming the offending key.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        Ok(toml::from_str(text)?)
    }

    /// Everything checkable without touching the network or opening an object.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], naming the key. This is what `--check` runs, against a
    /// host that may already be loading.
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.loader.site.trim().is_empty() || self.loader.recorder.trim().is_empty() {
            return Err(ConfigError::NoIdentity);
        }
        if self.loader.objects_dir.as_os_str().is_empty() {
            return Err(ConfigError::NoObjectsDir);
        }
        if !self.loader.objects_dir.is_dir() {
            return Err(ConfigError::ObjectsDirUnreadable(
                self.loader.objects_dir.clone(),
            ));
        }
        if self.loader.ledger.as_os_str().is_empty() {
            return Err(ConfigError::NoLedger);
        }
        if self.loader.poll_interval.is_zero() {
            return Err(ConfigError::NoPollInterval);
        }
        self.check_market_data()?;
        // After `check_market_data`, so the cross-check below compares a name
        // that has already been held to being unpadded and unique. A padded
        // derivation name would otherwise fail here, as a feed the scan set
        // does not carry, and the message would describe the wrong mistake.
        self.check_feeds()?;
        self.clickhouse.check()?;
        Ok(())
    }

    /// The derivation switches, checked where somebody is watching.
    ///
    /// A half-written entry parses — the recorder's own configuration takes the
    /// same line about a port of zero — and fails here, in a deployment
    /// pipeline, rather than in an editor. Every failure below is one that would
    /// otherwise present as an empty `event` table, which is indistinguishable
    /// from a feed nobody published on.
    fn check_market_data(&self) -> Result<(), ConfigError> {
        let names: Vec<String> = self
            .market_data
            .iter()
            .map(|derived| derived.feed.clone())
            .collect();
        for (index, derived) in self.market_data.iter().enumerate() {
            // Shared with the scan set's check, so the two lists cannot drift
            // apart on what a feed name is. They report different errors,
            // because the two keys fail differently and the message is what an
            // operator acts on -- but they agree on the rules.
            //
            //
            // The character set is NOT applied here. This name is matched
            // against the manifest's `feed` rather than against a directory;
            // the recorder writes both from one spec, but tightening a manifest
            // match to the directory character set is a separate decision, and
            // this is not the change that takes it.
            check_feed_name(&derived.feed, &names[..index]).map_err(|kind| match kind {
                FeedNameError::Empty => ConfigError::NoDerivedFeed,
                FeedNameError::Padded => ConfigError::PaddedDerivedFeed(derived.feed.clone()),
                FeedNameError::Duplicate => ConfigError::DuplicateDerivedFeed(derived.feed.clone()),
            })?;
            if derived.magic == 0 {
                return Err(ConfigError::NoMagic(derived.feed.clone()));
            }
        }
        Ok(())
    }

    /// The scan set, checked where somebody is watching.
    ///
    /// Every failure below is one that would otherwise present as a feed
    /// loading nothing — indistinguishable from a feed nobody published on, and
    /// the exact condition that ran for ten days on a host scanning one feed's
    /// directory out of thirteen with every counter reading healthy.
    fn check_feeds(&self) -> Result<(), ConfigError> {
        let Some(feeds) = self.loader.feeds.as_deref() else {
            // Absent is every feed, so there is nothing to narrow and nothing
            // for a derivation to fall outside of.
            return Ok(());
        };
        if feeds.is_empty() {
            return Err(ConfigError::EmptyScanSet);
        }
        for (index, feed) in feeds.iter().enumerate() {
            check_feed_name(feed, &feeds[..index]).map_err(|kind| match kind {
                FeedNameError::Empty => ConfigError::NoScannedFeed,
                FeedNameError::Padded => ConfigError::PaddedScannedFeed(feed.clone()),
                FeedNameError::Duplicate => ConfigError::DuplicateScannedFeed(feed.clone()),
            })?;
            // And this one only here: a scan-set entry becomes a directory
            // name, so the recorder's own rule for what a spec may contain is
            // the rule for what can possibly be found.
            if !is_spec_name(feed) {
                return Err(ConfigError::ScannedFeedIsNotASpecName(feed.clone()));
            }
        }
        for derived in &self.market_data {
            if !feeds.iter().any(|feed| feed == &derived.feed) {
                return Err(ConfigError::DerivedFeedIsNotScanned(derived.feed.clone()));
            }
        }
        Ok(())
    }

    /// The feeds named that have no directory under `objects_dir`.
    ///
    /// **Not a refusal, and deliberately not one.** A feed configured in the
    /// same change as the recorder that will write it has no directory until
    /// that recorder's first publication, so refusing here would fail every
    /// simultaneous deploy. But the other cause is a misspelling, and a
    /// misspelled entry scans nothing for ever while `--check` passes — the
    /// silent shape this whole key is meant to stop producing. So `--check`
    /// says which ones, and [`crate::metrics`] counts them every pass, and the
    /// operator decides which of the two it is.
    #[must_use]
    pub fn feeds_without_a_directory(&self) -> Vec<&str> {
        self.loader
            .feeds
            .iter()
            .flatten()
            .filter(|feed| !self.loader.objects_dir.join(feed).is_dir())
            .map(String::as_str)
            .collect()
    }

    /// What `--check` prints, so an operator can see what was read rather than
    /// what they believe they wrote.
    #[must_use]
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        // Writing to a String cannot fail.
        let _ = writeln!(
            out,
            "site={} recorder={}",
            self.loader.site, self.loader.recorder
        );
        let _ = writeln!(out, "objects={}", self.loader.objects_dir.display());
        // Printed only when the scan is narrowed, and the absence is the
        // statement -- the same line `market_data` takes below. An empty
        // `feeds=` would read as "no feeds", which is the opposite of what an
        // empty list means.
        if let Some(feeds) = &self.loader.feeds {
            let _ = writeln!(out, "feeds={}", feeds.join(" "));
        }
        let _ = writeln!(out, "ledger={}", self.loader.ledger.display());
        let _ = writeln!(
            out,
            "destination={} database={} user={}",
            self.clickhouse.endpoint, self.clickhouse.database, self.clickhouse.user
        );
        let _ = writeln!(out, "metrics={}", self.metrics.listen_addr);
        // Named one per line, and *nothing* printed when no feed derives. The
        // absence of a line is the statement: derivation is off by default, and
        // an operator reading this back is reading which feeds pay for it and at
        // what Magic rather than which feeds they meant to name.
        for derived in &self.market_data {
            let _ = writeln!(
                out,
                "market_data feed={} magic=0x{:04x} snapshot_levels={}",
                derived.feed,
                derived.magic,
                if derived.persist_snapshot_levels {
                    "persisted"
                } else {
                    "consumed"
                }
            );
        }
        out
    }
}

mod duration_secs {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Duration, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_u64(value.as_secs())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<Duration, D::Error> {
        Ok(Duration::from_secs(u64::deserialize(de)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
[loader]
site = "site-1"
recorder = "recorder-1"
objects_dir = "OBJECTS"
ledger = "/var/lib/dz-loader/ledger.jsonl"
poll_interval = 30

[clickhouse]
endpoint = "http://192.0.2.20:8123"
database = "recorder"
user = "loader"
"#;

    fn config_with_objects_dir(dir: &std::path::Path) -> LoaderConfig {
        LoaderConfig::parse(&VALID.replace("OBJECTS", &dir.display().to_string()))
            .expect("the fixture parses")
    }

    #[test]
    fn a_valid_configuration_checks_out() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = config_with_objects_dir(dir.path());
        config.check().expect("valid");
        assert_eq!(config.loader.poll_interval, Duration::from_secs(30));
        assert_eq!(config.metrics.listen_addr.port(), 9101);
    }

    /// The invariant the record path holds: no endpoint, no credential and no
    /// database key over there, and no password key over here either.
    #[test]
    fn there_is_no_password_key_anywhere_in_this_file() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = toml::to_string(&config_with_objects_dir(dir.path()))
            .expect("the configuration is serialisable");
        for forbidden in ["password", "secret", "token", "credential"] {
            assert!(!text.contains(forbidden), "`{forbidden}` in: {text}");
        }
    }

    /// A misspelled key that parsed cleanly and fell back to a default is how a
    /// host loads into the wrong database while the operator believes otherwise.
    #[test]
    fn a_misspelled_key_is_refused_rather_than_defaulted() {
        let text = VALID.replace("poll_interval", "pol_interval");
        let error = LoaderConfig::parse(&text).expect_err("an unknown key is refused");
        assert!(error.to_string().contains("pol_interval"), "{error}");
    }

    #[test]
    fn the_metrics_port_is_not_the_recorders() {
        // A loader that defaulted onto 9100 would fail to bind on exactly the
        // hosts it is meant to run on.
        assert_ne!(MetricsConfig::default().listen_addr.port(), 9100);
    }

    #[test]
    fn an_objects_directory_is_required_and_is_not_guessed_at() {
        let mut config = LoaderConfig::parse(&VALID.replace("OBJECTS", "/nope/not/here"))
            .expect("the fixture parses");
        assert!(matches!(
            config.check(),
            Err(ConfigError::ObjectsDirUnreadable(_))
        ));
        config.loader.objects_dir = PathBuf::new();
        assert!(matches!(config.check(), Err(ConfigError::NoObjectsDir)));
    }

    #[test]
    fn the_labels_every_series_carries_are_required() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.loader.site = String::new();
        assert!(matches!(config.check(), Err(ConfigError::NoIdentity)));
        config = config_with_objects_dir(dir.path());
        config.loader.recorder = "  ".to_owned();
        assert!(matches!(config.check(), Err(ConfigError::NoIdentity)));
    }

    #[test]
    fn a_ledger_and_a_wait_are_required() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.loader.ledger = PathBuf::new();
        assert!(matches!(config.check(), Err(ConfigError::NoLedger)));
        config = config_with_objects_dir(dir.path());
        config.loader.poll_interval = Duration::ZERO;
        assert!(matches!(config.check(), Err(ConfigError::NoPollInterval)));
    }

    /// The destination's own checks reach the same error path, so one `--check`
    /// covers both files' worth of keys.
    #[test]
    fn the_destinations_own_checks_are_part_of_this_one() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.clickhouse.endpoint = "192.0.2.20:8123".to_owned();
        let error = config.check().expect_err("not an http url");
        assert!(error.to_string().contains("http://"), "{error}");
    }

    const DERIVES: &str = r#"

[[market_data]]
feed = "market-by-price"
magic = 0x4442
persist_snapshot_levels = true
"#;

    /// The state this ships in: every feed loads datagram rows and no feed
    /// derives market data.
    ///
    /// Not a preference expressed in a comment — the key is absent from the
    /// example file and from every fixture here, and a default that turned it on
    /// would turn it on for a host whose operator never wrote the key at all.
    #[test]
    fn a_configuration_that_says_nothing_derives_no_market_data() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = config_with_objects_dir(dir.path());
        config.check().expect("valid");
        assert!(config.market_data.is_empty(), "off, and off by absence");
        assert!(
            !config.summary().contains("market_data"),
            "no line, because there is nothing to say: {}",
            config.summary()
        );
    }

    #[test]
    fn a_feed_that_derives_says_which_and_at_what_magic() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = VALID.replace("OBJECTS", &dir.path().display().to_string()) + DERIVES;
        let config = LoaderConfig::parse(&text).expect("the fixture parses");
        config.check().expect("valid");

        assert_eq!(config.market_data.len(), 1);
        assert_eq!(config.market_data[0].feed, "market-by-price");
        assert_eq!(config.market_data[0].magic, 0x4442);
        assert!(config.market_data[0].persist_snapshot_levels);
        let summary = config.summary();
        assert!(
            summary.contains("market_data feed=market-by-price magic=0x4442"),
            "{summary}"
        );
        assert!(summary.contains("snapshot_levels=persisted"), "{summary}");
    }

    /// Persisting levels is its own switch, and it is off unless it is asked
    /// for.
    ///
    /// The book consumes every level either way, so this key decides a row count
    /// and never a derivation — which is why a default of *on* would be the
    /// expensive one to discover.
    #[test]
    fn levels_are_consumed_and_not_persisted_unless_a_feed_asks() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = VALID.replace("OBJECTS", &dir.path().display().to_string())
            + "\n[[market_data]]\nfeed = \"top-of-book\"\nmagic = 0x445a\n";
        let config = LoaderConfig::parse(&text).expect("the fixture parses");
        config.check().expect("valid");
        assert!(!config.market_data[0].persist_snapshot_levels);
        assert!(
            config.summary().contains("snapshot_levels=consumed"),
            "{}",
            config.summary()
        );
    }

    /// A `Magic` of zero is a key somebody has yet to fill in.
    ///
    /// It parses, because a half-written file should fail where somebody is
    /// watching rather than in an editor, and it is refused here — where
    /// `--check` runs. Left alone it matches no datagram in the archive, and the
    /// feed derives an empty table that reads exactly like a feed nobody
    /// published on.
    #[test]
    fn a_magic_nobody_filled_in_is_refused_where_somebody_is_watching() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = VALID.replace("OBJECTS", &dir.path().display().to_string())
            + "\n[[market_data]]\nfeed = \"market-by-price\"\nmagic = 0\n";
        let config = LoaderConfig::parse(&text).expect("a half-written file parses");
        let error = config.check().expect_err("and is refused at --check");
        assert!(matches!(error, ConfigError::NoMagic(ref feed) if feed == "market-by-price"));
        assert!(error.to_string().contains("quiet feed"), "{error}");
    }

    /// Two entries for one feed: whichever the parser saw last would be in
    /// force, and the other is the one an operator believes is.
    #[test]
    fn one_feed_may_not_be_named_twice() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = VALID.replace("OBJECTS", &dir.path().display().to_string())
            + DERIVES
            + "\n[[market_data]]\nfeed = \"market-by-price\"\nmagic = 0x4442\n";
        let config = LoaderConfig::parse(&text).expect("the fixture parses");
        assert!(matches!(
            config.check(),
            Err(ConfigError::DuplicateDerivedFeed(_))
        ));
    }

    /// A padded name matches no feed, and the failure it produces is the one
    /// this whole section is arranged to prevent: an empty table that reads as a
    /// feed nobody published on.
    #[test]
    fn a_feed_name_may_not_be_padded_with_whitespace() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let text = VALID.replace("OBJECTS", &dir.path().display().to_string())
            + "\n[[market_data]]\nfeed = \"market-by-price \"\nmagic = 0x4442\n";
        let config = LoaderConfig::parse(&text).expect("the fixture parses");
        assert!(
            matches!(config.check(), Err(ConfigError::PaddedDerivedFeed(ref feed)) if feed == "market-by-price "),
            "a padded feed name is accepted and derives nothing"
        );
    }

    /// The same refusal the rest of the file makes, in the section that is
    /// newest and therefore the one most likely to be typed from memory.
    #[test]
    fn a_misspelled_derivation_key_is_refused_rather_than_defaulted() {
        let text = VALID.to_owned()
            + "\n[[market_data]]\nfeed = \"market-by-price\"\nmagic = 0x4442\npersist_snapshot_level = true\n";
        let error = LoaderConfig::parse(&text).expect_err("an unknown key is refused");
        assert!(
            error.to_string().contains("persist_snapshot_level"),
            "{error}"
        );
    }

    #[test]
    fn the_summary_says_what_was_read_and_never_a_password() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let summary = config_with_objects_dir(dir.path()).summary();
        assert!(summary.contains("site=site-1"), "{summary}");
        assert!(summary.contains("database=recorder"), "{summary}");
        assert!(summary.contains("user=loader"), "{summary}");
        assert!(!summary.to_lowercase().contains("password"), "{summary}");
    }

    /// The name of the account `004` creates, read out of the DDL.
    ///
    /// Parsed rather than written down here, because a constant would be a
    /// third copy of the same string and the one nothing checks.
    fn account_created_by(sql: &str) -> Option<String> {
        sql.lines()
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
            .filter(|name| !name.is_empty())
    }

    /// The example configuration names the account the checked-in DDL creates.
    ///
    /// Two files that have to agree and nothing that would notice when they
    /// stop: `004` provisions the account, and this is the file an operator
    /// copies in order to point at it. They had disagreed — the DDL creates
    /// `dz_loader` and the example said `loader` — and the symptom is the
    /// expensive shape. It is not a configuration error, so `check` passes it;
    /// it surfaces as an authentication failure against a destination that has
    /// to be reachable before anything can say the name was wrong, which is a
    /// provisioning mistake wearing a connectivity mistake's clothes.
    ///
    /// The expected name is read out of the DDL rather than written here, so
    /// what this holds is that the two files agree — not that both of them
    /// match a third copy of the string.
    #[test]
    fn the_example_names_the_account_the_ddl_creates() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("loader.example.toml");
        let example = std::fs::read_to_string(&path).expect("the example ships with the crate");
        // Parsed, not only scanned: an example an operator copies has to be a
        // configuration this binary accepts, and nothing had held it to that.
        let config = LoaderConfig::parse(&example).expect("the example parses");

        let sql = dz_recorder_clickhouse::migrations()
            .into_iter()
            .find(|migration| migration.name == "004_recorder_loader_user.sql")
            .expect("the account migration is one of the four")
            .sql;
        let account = account_created_by(sql).expect("`004` creates an account");

        assert_eq!(
            config.clickhouse.user, account,
            "the example points at `{}` and the DDL creates `{account}`",
            config.clickhouse.user
        );
    }

    /// The compatibility promise, and the one the deployed fleet relies on: a
    /// host that upgrades this binary and changes no configuration scans the
    /// whole archive exactly as it did before.
    #[test]
    fn a_configuration_that_names_no_feeds_scans_every_feed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = config_with_objects_dir(dir.path());

        assert!(
            config.loader.feeds.is_none(),
            "an absent `feeds` key must mean every feed, not no feed"
        );
        config.check().expect("it checks out");
    }

    /// A derivation the scan set does not carry derives nothing at all -- not
    /// `event`, and not `datagram` either -- and nothing on the host says so.
    /// So `--check` is where it is said.
    #[test]
    fn a_derived_feed_the_scan_set_omits_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.loader.feeds = Some(vec!["scanned".to_owned()]);
        config.market_data = vec![MarketDataFeed {
            feed: "derives-but-is-never-scanned".to_owned(),
            magic: 0x4442,
            persist_snapshot_levels: false,
        }];

        let message = config.check().expect_err("it is refused").to_string();
        assert!(
            message.contains("derives-but-is-never-scanned"),
            "the refusal must name the feed: {message}"
        );
    }

    /// And the same entry with no scan set is accepted, because empty is every
    /// feed. Paired with the test above deliberately: the refusal must come
    /// from the narrowing and not from the derivation.
    #[test]
    fn the_same_derived_feed_is_accepted_when_no_scan_set_is_named() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.market_data = vec![MarketDataFeed {
            feed: "derives-but-is-never-scanned".to_owned(),
            magic: 0x4442,
            persist_snapshot_levels: false,
        }];

        config
            .check()
            .expect("an unnarrowed scan reaches every feed");
    }

    #[test]
    fn a_scan_set_entry_that_names_nothing_usable_is_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        for (feeds, what) in [
            (vec![String::new()], "an empty name"),
            (vec![" padded".to_owned()], "a padded name"),
            (vec!["a".to_owned(), "a".to_owned()], "a duplicate name"),
            (vec!["a/b".to_owned()], "a separator"),
            (vec!["..".to_owned()], "a parent directory"),
            (vec![".".to_owned()], "the directory itself"),
            // The recorder's own `check_spec` refuses these, so no directory
            // of this name can exist and the entry could only ever scan
            // nothing. Same rule, restated where the name is read.
            (vec!["a b".to_owned()], "an internal space"),
            (vec!["a?b".to_owned()], "a shell metacharacter"),
            (vec!["a\\b".to_owned()], "a backslash"),
        ] {
            let mut config = config_with_objects_dir(dir.path());
            config.loader.feeds = Some(feeds);
            config
                .check()
                .expect_err(&format!("{what} must be refused"));
        }
    }

    /// Printed when the scan is narrowed and absent when it is not, because an
    /// empty `feeds=` line would read as "no feeds" and empty means every one.
    #[test]
    fn the_summary_names_the_scan_set_only_when_there_is_one() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        assert!(
            !config.summary().contains("feeds="),
            "an unnarrowed scan must say nothing: {}",
            config.summary()
        );

        config.loader.feeds = Some(vec!["one".to_owned(), "two".to_owned()]);
        let summary = config.summary();
        assert!(summary.contains("feeds=one two"), "{summary}");
    }

    /// **`feeds = []` IS NOT A WAY TO PAUSE THE LOADER, AND IT MUST NOT READ AS
    /// THE WIDEST SETTING THERE IS.**
    ///
    /// Absent means every feed. An operator who writes an empty list means the
    /// opposite, and with a plain `Vec` the two are the same value — so the
    /// narrowest thing anyone can write would have produced a full-archive
    /// scan at the `datagram` grain, which is the exact cost this key exists
    /// to avoid. `Option` is what keeps them different, and this is what holds
    /// it there.
    #[test]
    fn an_empty_scan_set_is_refused_rather_than_read_as_every_feed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let mut config = config_with_objects_dir(dir.path());
        config.loader.feeds = Some(Vec::new());

        config.check().expect_err("an empty list is refused");

        // And the distinction survives a round trip through TOML, which is
        // where the operator actually writes it.
        let text = toml::to_string(&config).expect("it serialises");
        assert!(text.contains("feeds = []"), "{text}");
        LoaderConfig::parse(&text)
            .expect("it parses")
            .check()
            .expect_err("and is still refused after a round trip");
    }

    /// The scan set is reported against the directory, so a misspelling has
    /// somewhere to show up.
    ///
    /// Not a refusal: a feed configured in the same change as the recorder that
    /// will write it has no directory until the first publication. The two
    /// cases are told apart by whether it clears.
    #[test]
    fn a_named_feed_with_no_directory_is_reported_but_not_refused() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        std::fs::create_dir(dir.path().join("written")).expect("creatable");
        let mut config = config_with_objects_dir(dir.path());
        config.loader.feeds = Some(vec!["written".to_owned(), "never-written".to_owned()]);

        config.check().expect("it is not a refusal");
        assert_eq!(config.feeds_without_a_directory(), vec!["never-written"]);
    }

    /// An unnarrowed configuration reports nothing, because it names nothing.
    #[test]
    fn an_unnarrowed_configuration_reports_no_missing_feeds() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let config = config_with_objects_dir(dir.path());
        assert!(config.feeds_without_a_directory().is_empty());
    }
}
