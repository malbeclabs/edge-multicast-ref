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
}

/// One loader host's whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoaderConfig {
    pub loader: Loader,
    pub clickhouse: ClickHouseConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
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
        self.clickhouse.check()?;
        Ok(())
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
        let _ = writeln!(out, "ledger={}", self.loader.ledger.display());
        let _ = writeln!(
            out,
            "destination={} database={} user={}",
            self.clickhouse.endpoint, self.clickhouse.database, self.clickhouse.user
        );
        let _ = writeln!(out, "metrics={}", self.metrics.listen_addr);
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

    #[test]
    fn the_summary_says_what_was_read_and_never_a_password() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let summary = config_with_objects_dir(dir.path()).summary();
        assert!(summary.contains("site=site-1"), "{summary}");
        assert!(summary.contains("database=recorder"), "{summary}");
        assert!(summary.contains("user=loader"), "{summary}");
        assert!(!summary.to_lowercase().contains("password"), "{summary}");
    }
}
