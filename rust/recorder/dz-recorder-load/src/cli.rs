//! The command line, parsed by hand as the recorder's is.
//!
//! Six options and a dependency to parse them is a dependency in a process that
//! runs beside a recorder.

use std::path::PathBuf;

use thiserror::Error;

use crate::identity::{build_commit, BUILD_VERSION};

pub const USAGE: &str = "\
dz-recorder-load — turns a directory of completed objects into rows a dashboard can ask.

Usage:
  dz-recorder-load --config <path> --once
  dz-recorder-load --config <path> --watch
  dz-recorder-load --config <path> --once --dry-run <dir>
  dz-recorder-load --config <path> --check
  dz-recorder-load --version
  dz-recorder-load --help

Options:
  --config <path>    The TOML configuration. Required.

  --once             Walk the objects directory once, oldest object first, and
                     exit. This is the mode a timer runs.

  --watch            The same walk, repeated on the configured interval, until a
                     signal arrives. A signal finishes the object being loaded
                     and then stops, so a restart never leaves an object
                     half-loaded — and because loading is idempotent on
                     (object key, sha256), a restart that re-loads one costs a
                     replace and nothing else.

  --dry-run <dir>    Write the rows as newline-delimited JSON into <dir> instead
                     of sending them anywhere, and record nothing in the ledger.
                     Nothing is contacted: this is what to run against a new
                     object when the question is what the rows would say.

  --check            Validate the configuration and the destination's
                     reachability, and load nothing. Nothing is written, no
                     object is opened and the ledger is not touched: this is
                     what a deployment pipeline runs before it restarts
                     anything, against a host that may already be loading.

  --version          The build version and the build commit. A build that was
                     not given DZ_LOADER_BUILD_COMMIT at compile time reports
                     `unknown` rather than claiming a commit.
  --help             This.

Exit codes:
  0  the pass finished, or the configuration checked out
  1  the loader refused to start, or failed while loading
  2  the command line could not be understood

Where it runs: on the recorder host, against that host's own completed
directory, opened read-only. Nothing ships objects off a recorder host, and
objects are evicted under the staging budget — so the rows travel and the bytes
stay local. The gate on that arrangement is dz_loader_oldest_unloaded_age_seconds
against the eviction window: a loader that falls behind eviction loses history
that no re-run can recover.

The password for the column store comes from DZ_LOADER_CLICKHOUSE_PASSWORD and
from nowhere else. There is no configuration key for it.";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CliError {
    #[error("`{0}` is not an option this binary knows")]
    Unknown(String),
    #[error("`{0}` needs a value")]
    MissingValue(&'static str),
    #[error("--config is required: a loader has nothing to load without one")]
    NoConfig,
    #[error(
        "one of --once, --watch or --check is required: a loader with no mode would do nothing \
         and report success"
    )]
    NoMode,
    #[error("--once and --watch cannot both be asked for: one pass is not every pass")]
    OnceAndWatch,
    #[error(
        "--check loads nothing, so it cannot be combined with {0}: a pipeline that asked to \
         validate must not start loading"
    )]
    CheckAndLoad(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Invocation {
    Help,
    Version,
    Run(Args),
}

/// What the loader was asked to do.
#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub config: PathBuf,
    pub mode: Mode,
    /// Rows to a directory instead of to the destination, and no ledger entry.
    pub dry_run: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Check,
    Once,
    Watch,
}

/// Parses the arguments after the program name.
///
/// # Errors
///
/// [`CliError`], and the caller exits 2: a command line that could not be
/// understood is a different failure from a loader that refused to start, and a
/// deployment pipeline distinguishes them.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Invocation, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut check = false;
    let mut once = false;
    let mut watch = false;
    let mut dry_run: Option<PathBuf> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Invocation::Help),
            "--version" => return Ok(Invocation::Version),
            "--check" => check = true,
            "--once" => once = true,
            "--watch" => watch = true,
            "--config" => {
                config = Some(PathBuf::from(
                    args.next().ok_or(CliError::MissingValue("--config"))?,
                ));
            }
            "--dry-run" => {
                dry_run = Some(PathBuf::from(
                    args.next().ok_or(CliError::MissingValue("--dry-run"))?,
                ));
            }
            other => return Err(CliError::Unknown(other.to_owned())),
        }
    }

    let config = config.ok_or(CliError::NoConfig)?;
    if once && watch {
        return Err(CliError::OnceAndWatch);
    }
    if check {
        if once || watch {
            return Err(CliError::CheckAndLoad("--once or --watch"));
        }
        if dry_run.is_some() {
            return Err(CliError::CheckAndLoad("--dry-run"));
        }
    }
    let mode = match (check, once, watch) {
        (true, _, _) => Mode::Check,
        (_, true, _) => Mode::Once,
        (_, _, true) => Mode::Watch,
        _ => return Err(CliError::NoMode),
    };
    Ok(Invocation::Run(Args {
        config,
        mode,
        dry_run,
    }))
}

#[must_use]
pub fn version_line() -> String {
    format!("dz-recorder-load {BUILD_VERSION} ({})", build_commit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_of(args: &[&str]) -> Result<Invocation, CliError> {
        parse(args.iter().map(|s| (*s).to_owned()))
    }

    fn args_of(args: &[&str]) -> Args {
        match parse_of(args) {
            Ok(Invocation::Run(args)) => args,
            other => panic!("expected an invocation, got {other:?}"),
        }
    }

    #[test]
    fn a_configuration_path_is_required() {
        assert_eq!(parse_of(&[]), Err(CliError::NoConfig));
        assert_eq!(parse_of(&["--once"]), Err(CliError::NoConfig));
    }

    /// A loader with no mode would walk nothing and exit 0, which a timer would
    /// report as a successful load for ever.
    #[test]
    fn a_mode_is_required_rather_than_defaulted() {
        assert_eq!(parse_of(&["--config", "l.toml"]), Err(CliError::NoMode));
    }

    #[test]
    fn one_pass_and_every_pass_are_not_asked_for_together() {
        assert_eq!(
            parse_of(&["--config", "l.toml", "--once", "--watch"]),
            Err(CliError::OnceAndWatch)
        );
    }

    /// A pipeline that asked to validate must not start loading, and this is
    /// where that is enforced rather than in a comment.
    #[test]
    fn checking_and_loading_are_not_asked_for_together() {
        assert_eq!(
            parse_of(&["--config", "l.toml", "--check", "--once"]),
            Err(CliError::CheckAndLoad("--once or --watch"))
        );
        assert_eq!(
            parse_of(&["--config", "l.toml", "--check", "--watch"]),
            Err(CliError::CheckAndLoad("--once or --watch"))
        );
        assert_eq!(
            parse_of(&["--config", "l.toml", "--check", "--dry-run", "/tmp/x"]),
            Err(CliError::CheckAndLoad("--dry-run"))
        );
    }

    #[test]
    fn every_mode_is_asked_for_by_name() {
        assert_eq!(args_of(&["--config", "l.toml", "--once"]).mode, Mode::Once);
        assert_eq!(
            args_of(&["--config", "l.toml", "--watch"]).mode,
            Mode::Watch
        );
        assert_eq!(
            args_of(&["--config", "l.toml", "--check"]).mode,
            Mode::Check
        );
    }

    #[test]
    fn a_dry_run_names_the_directory_it_writes_to() {
        let args = args_of(&["--config", "l.toml", "--once", "--dry-run", "/var/tmp/rows"]);
        assert_eq!(args.dry_run, Some(PathBuf::from("/var/tmp/rows")));
        assert_eq!(args_of(&["--config", "l.toml", "--once"]).dry_run, None);
    }

    #[test]
    fn an_option_with_no_value_is_named_in_the_error() {
        assert_eq!(
            parse_of(&["--config"]),
            Err(CliError::MissingValue("--config"))
        );
        assert_eq!(
            parse_of(&["--config", "l.toml", "--once", "--dry-run"]),
            Err(CliError::MissingValue("--dry-run"))
        );
    }

    #[test]
    fn an_unknown_option_is_refused_rather_than_ignored() {
        assert_eq!(
            parse_of(&["--config", "l.toml", "--once", "--nope"]),
            Err(CliError::Unknown("--nope".to_owned()))
        );
    }

    #[test]
    fn help_and_version_short_circuit_everything_else() {
        assert_eq!(parse_of(&["--help"]), Ok(Invocation::Help));
        assert_eq!(parse_of(&["--version"]), Ok(Invocation::Version));
        assert_eq!(
            parse_of(&["--config", "l.toml", "--version"]),
            Ok(Invocation::Version)
        );
    }

    /// Where an operator looks for how to run this is where they have to learn
    /// the two things that are not obvious: it runs on the recorder host, and
    /// the gate on that is lag against eviction.
    #[test]
    fn the_usage_says_where_it_runs_and_what_the_gate_on_that_is() {
        assert!(USAGE.contains("on the recorder host"), "{USAGE}");
        assert!(
            USAGE.contains("evicted under the staging budget"),
            "{USAGE}"
        );
        assert!(
            USAGE.contains("dz_loader_oldest_unloaded_age_seconds"),
            "{USAGE}"
        );
        // And that the password is not a configuration key.
        assert!(USAGE.contains("DZ_LOADER_CLICKHOUSE_PASSWORD"), "{USAGE}");
        assert!(USAGE.contains("no configuration key for it"), "{USAGE}");
    }

    #[test]
    fn the_version_line_carries_the_build_and_its_commit() {
        let line = version_line();
        assert!(line.contains(BUILD_VERSION), "{line}");
        assert!(line.contains(build_commit()), "{line}");
    }
}
