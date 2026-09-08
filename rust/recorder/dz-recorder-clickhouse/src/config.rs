//! What an operator states, and what only the environment may carry.
//!
//! The split is deliberate and it is the whole reason this is a separate module:
//! a configuration file is a thing people read in reviews, paste into tickets and
//! commit. It carries the endpoint, the database and the user. It has no password
//! key at all — not an empty one, not an optional one — because a key that exists
//! is a key somebody fills in.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The password, read as a value.
///
/// Second choice, and it exists because a container or a shell that has the
/// secret in hand has nowhere else to put it. A value in the environment is
/// readable through `/proc/<pid>/environ` by anything running as the same user
/// and is inherited by every child, so [`PASSWORD_FILE_ENV`] is preferred when
/// both are set.
pub const PASSWORD_ENV: &str = "DZ_LOADER_CLICKHOUSE_PASSWORD";

/// A path to read the password from, which is what a systemd credential is.
///
/// First choice. `LoadCredentialEncrypted=` hands a unit a file under
/// `$CREDENTIALS_DIRECTORY` rather than a variable, and that file is readable by
/// the service user alone and is not inherited by anything the process spawns.
pub const PASSWORD_FILE_ENV: &str = "DZ_LOADER_CLICKHOUSE_PASSWORD_FILE";

/// The cap on rows in one insert.
///
/// **Row volume is not the constraint; merge pressure is, and it is set by rows
/// per part rather than rows per day.** An insert is one atomic block and
/// becomes one part, so this is what decides how much merge work a day of
/// loading creates — and merge work does not appear in a query log at all, only
/// as the gap between a provider's CPU graph and query-attributed CPU. A chatty
/// inserter raises it silently.
///
/// A million rows lands an object's rows in one or two parts. The busiest lane
/// measured on a live recorder is 224,000 datagrams a minute, which is about 1.1
/// million rows in a time-rotated object.
pub const DEFAULT_INSERT_MAX_ROWS: usize = 1_000_000;

/// The number of rows below which the sink keeps holding.
///
/// **The bound that stops one part per object per lane**, which is the
/// pathological profile — and the reason a row *maximum* alone is not enough. The
/// quietest lanes measured run 130 to 150 datagrams a minute, about 700 rows in
/// a time-rotated object, so a sink that posted per object would write a
/// 700-row part per object per lane for ever. Rows from several objects coalesce
/// into one insert instead.
pub const DEFAULT_INSERT_MIN_ROWS: usize = 50_000;

/// How long the sink may hold rows short of [`DEFAULT_INSERT_MIN_ROWS`].
///
/// The bound on coalescing, so a quiet lane is **late rather than absent**. At
/// the rates above the worst case is roughly 2,000 rows a part, which is far
/// better than one part per object and is the price of not letting a quiet lane
/// go unqueryable indefinitely.
pub const DEFAULT_INSERT_MAX_DELAY: Duration = Duration::from_secs(15 * 60);

/// The cap on bytes in one insert.
///
/// Not in the design's table, and kept because a row count says nothing about a
/// row's width: the widest grain here carries an object key and two digests, and
/// a million of those is not the same request as a million gap rows. This is the
/// bound that keeps one request from being unreasonable whatever the row count
/// says; the row bounds above are what govern merge pressure.
pub const DEFAULT_INSERT_MAX_BYTES: u64 = 256 * 1024 * 1024;

/// The default number of attempts one batch is given./// The default number of attempts one batch is given.
///
/// Bounded, because the loader's answer to a destination that will not take a
/// batch is to leave the object unloaded and come back to it — and an unbounded
/// retry inside one object is a loader that stops making progress on every other
/// object while looking busy.
pub const DEFAULT_ATTEMPTS: u32 = 4;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("[clickhouse] endpoint is required: there is no defensible default host here")]
    NoEndpoint,
    #[error(
        "[clickhouse] endpoint `{0}` is https, and this build carries no TLS stack: build with \
         the `tls` feature of the http client, or point this at the private path the loader \
         actually runs on"
    )]
    TlsUnsupported(String),
    #[error("[clickhouse] endpoint `{0}` must start with http:// or https://")]
    NotAnHttpUrl(String),
    #[error("[clickhouse] database is required, and `default` is not a database to load into")]
    NoDatabase,
    #[error(
        "[clickhouse] insert_max_rows and insert_max_bytes must both be above zero: a batch of \
         nothing never lands"
    )]
    EmptyBatch,
    #[error(
        "[clickhouse] insert_min_rows ({min}) cannot exceed insert_max_rows ({max}): the floor \
         the sink holds to has to be reachable, or every insert waits for the delay"
    )]
    FloorAboveCap { min: usize, max: usize },
    #[error("[clickhouse] attempts must be at least 1: a batch nobody tries never lands")]
    NoAttempts,
}

/// Where the rows go, and how they are batched on the way.
///
/// There is no password field. See the module documentation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClickHouseConfig {
    /// `http://host:8123`, with no path: the path and the query are this crate's.
    pub endpoint: String,
    pub database: String,
    pub user: String,
    /// The cap on rows in one insert. See [`DEFAULT_INSERT_MAX_ROWS`].
    pub insert_max_rows: usize,
    /// The floor below which the sink keeps holding. See
    /// [`DEFAULT_INSERT_MIN_ROWS`].
    pub insert_min_rows: usize,
    /// How long it may hold short of that floor. See
    /// [`DEFAULT_INSERT_MAX_DELAY`].
    #[serde(with = "duration_secs")]
    pub insert_max_delay: Duration,
    /// The cap on bytes in one insert. See [`DEFAULT_INSERT_MAX_BYTES`].
    pub insert_max_bytes: u64,
    /// Attempts per batch, including the first.
    pub attempts: u32,
    #[serde(with = "duration_secs")]
    pub timeout: Duration,
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            endpoint: String::new(),
            database: String::new(),
            user: "default".to_owned(),
            insert_max_rows: DEFAULT_INSERT_MAX_ROWS,
            insert_min_rows: DEFAULT_INSERT_MIN_ROWS,
            insert_max_delay: DEFAULT_INSERT_MAX_DELAY,
            insert_max_bytes: DEFAULT_INSERT_MAX_BYTES,
            attempts: DEFAULT_ATTEMPTS,
            timeout: Duration::from_secs(30),
        }
    }
}

impl ClickHouseConfig {
    /// Everything checkable without touching the network.
    ///
    /// # Errors
    ///
    /// [`ConfigError`], naming the key. `--check` runs this against a host that
    /// may already be loading, before anything is restarted.
    pub fn check(&self) -> Result<(), ConfigError> {
        let endpoint = self.endpoint.trim();
        if endpoint.is_empty() {
            return Err(ConfigError::NoEndpoint);
        }
        if endpoint.starts_with("https://") && !cfg!(feature = "tls") {
            // Refused rather than downgraded. A loader that quietly spoke plain
            // HTTP to an endpoint an operator wrote as https would send a
            // password over the wire in the clear, having been told not to.
            return Err(ConfigError::TlsUnsupported(endpoint.to_owned()));
        }
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(ConfigError::NotAnHttpUrl(endpoint.to_owned()));
        }
        if self.database.trim().is_empty() {
            return Err(ConfigError::NoDatabase);
        }
        if self.insert_max_rows == 0 || self.insert_max_bytes == 0 {
            return Err(ConfigError::EmptyBatch);
        }
        if self.insert_min_rows > self.insert_max_rows {
            return Err(ConfigError::FloorAboveCap {
                min: self.insert_min_rows,
                max: self.insert_max_rows,
            });
        }
        if self.attempts == 0 {
            return Err(ConfigError::NoAttempts);
        }
        Ok(())
    }

    /// The URL one insert is posted to, with the statement in the query string.
    ///
    /// The statement rather than the body carries the target table, because the
    /// body is the rows: a `JSONEachRow` insert whose query and body were mixed
    /// in one stream would have to be re-serialised to be retried.
    #[must_use]
    pub fn insert_url(&self, table: &str) -> String {
        format!(
            "{}/?database={}&query={}",
            self.endpoint.trim_end_matches('/'),
            urlencode(self.database.trim()),
            urlencode(&format!(
                "INSERT INTO {}.{table} FORMAT JSONEachRow",
                self.database.trim()
            ))
        )
    }

    /// The URL an arbitrary statement is posted to — `--check`'s reachability
    /// probe, and the DDL.
    #[must_use]
    pub fn statement_url(&self) -> String {
        format!(
            "{}/?database={}",
            self.endpoint.trim_end_matches('/'),
            urlencode(self.database.trim())
        )
    }
}

/// A password, held so that nothing can print it.
///
/// `Debug` is implemented by hand and prints a fixed string. That is not
/// decoration: every error type in this crate derives `Debug`, a loader logs its
/// errors, and a struct that carried a `String` here would put the password in a
/// log the first time somebody added `{:?}` to a message.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    pub user: String,
    password: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("user", &self.user)
            .field(
                "password",
                &if self.password.is_some() {
                    "<set, from the environment>"
                } else {
                    "<unset>"
                },
            )
            .finish()
    }
}

impl Credentials {
    /// The user from the configuration, the password from the environment.
    #[must_use]
    pub fn new(user: impl Into<String>, password: Option<String>) -> Self {
        Self {
            user: user.into(),
            // Empty is unset, not a password: a unit file whose lookup failed
            // exports the variable with nothing in it, and sending that as a
            // password produces an authentication failure nobody can explain.
            password: password.filter(|p| !p.is_empty()),
        }
    }

    /// The user from the configuration, the password from the environment, and
    /// from nowhere else.
    ///
    /// [`PASSWORD_FILE_ENV`] wins over [`PASSWORD_ENV`] when both are set,
    /// because a file readable by the service user alone is a better place for a
    /// secret than a variable every child process inherits. A file that cannot
    /// be read is treated as no password rather than as a failure here: the
    /// destination's own refusal is a clearer error than this one could be, and
    /// it arrives with the server's own message.
    #[must_use]
    pub fn from_env(user: impl Into<String>) -> Self {
        let from_file = std::env::var(PASSWORD_FILE_ENV)
            .ok()
            .filter(|p| !p.is_empty())
            .and_then(|path| std::fs::read_to_string(path).ok())
            // Trailing newline stripped: a credential written by an editor or by
            // `echo` has one, and a password with a newline in it authenticates
            // nowhere while looking exactly right in a file listing.
            .map(|text| text.trim_end_matches(['\n', '\r']).to_owned());
        Self::new(user, from_file.or_else(|| std::env::var(PASSWORD_ENV).ok()))
    }

    #[must_use]
    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    #[must_use]
    pub fn is_authenticated(&self) -> bool {
        self.password.is_some()
    }
}

/// Seconds, as the recorder's own configuration spells a duration with a unit —
/// except that a timeout is a scalar an operator reads in seconds, so this is
/// the one place a bare number is unambiguous and therefore allowed.
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

/// Percent-encodes what a query string cannot carry raw.
///
/// By hand, because the alternative is a dependency for one function whose whole
/// input is SQL this crate wrote itself. Unreserved characters pass; everything
/// else becomes `%XX`, which is what a server's own decoder inverts.
fn urlencode(raw: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            // Writing to a String cannot fail.
            other => {
                let _ = write!(out, "%{other:02X}");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid() -> ClickHouseConfig {
        ClickHouseConfig {
            endpoint: "http://127.0.0.1:8123".to_owned(),
            database: "recorder".to_owned(),
            ..ClickHouseConfig::default()
        }
    }

    #[test]
    fn a_password_cannot_reach_a_log_through_debug() {
        // Every error type here derives Debug and a loader logs its errors. A
        // `String` in this field would put the password in a log the first time
        // somebody added `{:?}` to a message.
        let creds = Credentials::new("loader", Some("hunter2".to_owned()));
        let rendered = format!("{creds:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(rendered.contains("loader"), "{rendered}");
        assert!(rendered.contains("<set"), "{rendered}");
        assert_eq!(creds.password(), Some("hunter2"), "and it is still usable");
    }

    /// A file beats a variable, because a variable is inherited by every child
    /// and readable through `/proc`.
    ///
    /// The variables are set and removed inside one test rather than across
    /// several, because the environment is process-wide and `cargo test` runs
    /// tests in threads: two tests setting the same variable would race.
    #[test]
    fn a_credential_file_is_preferred_over_a_variable_and_is_trimmed() {
        let dir = tempfile::tempdir().expect("a temporary directory");
        let path = dir.path().join("clickhouse-password");
        // With the trailing newline an editor or `echo` leaves: a password with
        // a newline in it authenticates nowhere while looking exactly right in a
        // file listing.
        std::fs::write(&path, "from-the-file\n").expect("the file is writable");

        with_env(
            &[
                (PASSWORD_ENV, Some("from-the-variable")),
                (PASSWORD_FILE_ENV, Some(&path.display().to_string())),
            ],
            || {
                assert_eq!(
                    Credentials::from_env("loader").password(),
                    Some("from-the-file")
                );
            },
        );

        // The variable alone still works, for a container with nowhere to put a
        // file.
        with_env(
            &[
                (PASSWORD_ENV, Some("from-the-variable")),
                (PASSWORD_FILE_ENV, None),
            ],
            || {
                assert_eq!(
                    Credentials::from_env("loader").password(),
                    Some("from-the-variable")
                );
            },
        );

        // A file that is not there is no password, and not a failure: the
        // destination's own refusal is a clearer error, and it arrives with the
        // server's own message.
        with_env(
            &[
                (PASSWORD_ENV, None),
                (PASSWORD_FILE_ENV, Some("/nope/not/here")),
            ],
            || assert!(!Credentials::from_env("loader").is_authenticated()),
        );
    }

    /// Sets and restores process-wide variables around one closure.
    ///
    /// `std::env::set_var` is safe on this toolchain and still process-wide, so
    /// every environment assertion in this module runs inside one test.
    fn with_env(vars: &[(&str, Option<&str>)], body: impl FnOnce()) {
        let previous: Vec<(String, Option<String>)> = vars
            .iter()
            .map(|(k, _)| ((*k).to_owned(), std::env::var(k).ok()))
            .collect();
        for (key, value) in vars {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        body();
        for (key, value) in previous {
            match value {
                Some(value) => std::env::set_var(&key, value),
                None => std::env::remove_var(&key),
            }
        }
    }

    #[test]
    fn a_password_set_to_nothing_is_not_a_password() {
        // The shape a unit file produces when its secret lookup failed: the
        // variable is exported, and empty.
        assert!(!Credentials::new("loader", Some(String::new())).is_authenticated());
        assert!(!Credentials::new("loader", None).is_authenticated());
        assert!(Credentials::new("loader", Some("p".to_owned())).is_authenticated());
    }

    #[test]
    fn the_configuration_has_no_password_key_at_all() {
        // A key that exists is a key somebody fills in, and a configuration file
        // is a thing people commit. Serialising the whole struct is the check,
        // because a field added later would appear here.
        let text = toml_of(&valid());
        for forbidden in ["password", "secret", "token", "credential"] {
            assert!(
                !text.contains(forbidden),
                "`{forbidden}` appears in the configuration: {text}"
            );
        }
    }

    #[test]
    fn an_endpoint_is_required_and_is_not_guessed_at() {
        let mut config = valid();
        config.endpoint = String::new();
        assert_eq!(config.check(), Err(ConfigError::NoEndpoint));
        config.endpoint = "127.0.0.1:8123".to_owned();
        assert_eq!(
            config.check(),
            Err(ConfigError::NotAnHttpUrl("127.0.0.1:8123".to_owned()))
        );
    }

    /// An operator who wrote `https` asked for the wire to be encrypted. A build
    /// with no TLS stack must say so rather than send the password in the clear.
    #[test]
    fn an_https_endpoint_is_refused_rather_than_downgraded() {
        let mut config = valid();
        config.endpoint = "https://store.internal:8443".to_owned();
        let result = config.check();
        if cfg!(feature = "tls") {
            assert_eq!(result, Ok(()));
        } else {
            assert_eq!(
                result,
                Err(ConfigError::TlsUnsupported(
                    "https://store.internal:8443".to_owned()
                ))
            );
        }
    }

    #[test]
    fn a_batch_of_nothing_and_a_retry_of_nothing_are_refused() {
        let mut config = valid();
        config.insert_max_rows = 0;
        assert_eq!(config.check(), Err(ConfigError::EmptyBatch));
        config = valid();
        config.insert_max_bytes = 0;
        assert_eq!(config.check(), Err(ConfigError::EmptyBatch));
        config = valid();
        config.attempts = 0;
        assert_eq!(config.check(), Err(ConfigError::NoAttempts));
        assert_eq!(valid().check(), Ok(()));
    }

    #[test]
    fn a_database_is_required() {
        let mut config = valid();
        config.database = "  ".to_owned();
        assert_eq!(config.check(), Err(ConfigError::NoDatabase));
    }

    /// The statement goes in the query string and the rows go in the body, so a
    /// retry re-sends bytes it already has rather than re-serialising them.
    #[test]
    fn an_insert_url_carries_the_statement_and_the_database() {
        let url = valid().insert_url("sequence_gap");
        assert!(
            url.starts_with("http://127.0.0.1:8123/?database=recorder&query="),
            "{url}"
        );
        assert!(
            url.contains("INSERT%20INTO%20recorder.sequence_gap%20FORMAT%20JSONEachRow"),
            "{url}"
        );
        assert!(!url.contains(' '), "a URL with a raw space in it: {url}");
    }

    #[test]
    fn percent_encoding_leaves_the_unreserved_set_alone() {
        assert_eq!(urlencode("abcXYZ019-_.~"), "abcXYZ019-_.~");
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urlencode("a&b=c"), "a%26b%3Dc");
    }

    fn toml_of(config: &ClickHouseConfig) -> String {
        // A round trip through the serialiser rather than a literal, so a field
        // added later is covered without anybody remembering to add it here.
        format!(
            "{config:?}\n{}",
            serde_json::to_string(config).expect("serialisable")
        )
    }
}
