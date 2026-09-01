//! The command line, parsed by hand because there are four options and a
//! dependency to parse them is a dependency in the record path.

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::identity::{build_commit, BUILD_VERSION};

pub const USAGE: &str = "\
dz-recorder — captures an edge feed, archives the bytes, and says how it is doing.

Usage:
  dz-recorder --config <path> [--run-for <duration>]
  dz-recorder --config <path> --check
  dz-recorder --version
  dz-recorder --help

Options:
  --config <path>       The TOML configuration. Required.

  --check               Validate the configuration and exit, recording nothing.
                        Nothing is bound, nothing is created and nothing is
                        joined: this is what a deployment pipeline runs before
                        it restarts anything.

  --run-for <duration>  Record for this long, then shut down through the whole
                        sequence: drain what is in flight, stop the capture,
                        flush and rotate the open segment, and wait for the
                        compressor to publish it.

                        A bounded run for a test or a one-off capture; a
                        supervisor stops a recorder with a signal, and that
                        takes the same sequence.

                        Durations carry a unit, as the configuration's do:
                        `500ms`, `30s`, `5m`, `1h`.

  --version             The build version and the build commit. A build that
                        was not given DZ_RECORDER_BUILD_COMMIT at compile time
                        reports `unknown` rather than claiming a commit, and
                        that string is what every archive it writes carries.
  --help                This.

Exit codes:
  0  the run finished, or the configuration checked out
  1  the recorder refused to start, or failed while recording
  2  the command line could not be understood

Stopping it: SIGINT, SIGTERM or SIGHUP runs the whole shutdown sequence — drain,
stop the capture, rotate the open segment, wait for it to be published — so a
restart does not abandon the window an operator is most likely to be asking
about. A second signal exits at once, without waiting for the publication:
signalling twice says the graceful path is taking too long, and the right answer
then is to die.

If the handler cannot be installed the recorder still records and says so on
startup; stop that one with `--run-for`.";

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CliError {
    #[error("`{0}` is not an option this binary knows")]
    Unknown(String),
    #[error("`{0}` needs a value")]
    MissingValue(&'static str),
    #[error("--config is required: a recorder has nothing to record without one")]
    NoConfig,
    #[error("--run-for: {0}")]
    BadDuration(String),
    #[error("--check and --run-for cannot both be asked for: one records nothing and one records")]
    CheckAndRun,
}

/// What the command line asked for.
#[derive(Debug, PartialEq, Eq)]
pub enum Invocation {
    Help,
    Version,
    Run(Args),
}

#[derive(Debug, PartialEq, Eq)]
pub struct Args {
    pub config: PathBuf,
    pub check: bool,
    /// `None` records until a signal arrives, which runs the whole shutdown
    /// sequence and publishes the open segment. See [`USAGE`].
    pub run_for: Option<Duration>,
}

/// Parses the arguments after the program name.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Invocation, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut check = false;
    let mut run_for: Option<Duration> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Invocation::Help),
            "--version" => return Ok(Invocation::Version),
            "--check" => check = true,
            "--config" => {
                config = Some(PathBuf::from(
                    args.next().ok_or(CliError::MissingValue("--config"))?,
                ));
            }
            "--run-for" => {
                let raw = args.next().ok_or(CliError::MissingValue("--run-for"))?;
                run_for = Some(parse_duration(&raw).map_err(CliError::BadDuration)?);
            }
            other => return Err(CliError::Unknown(other.to_owned())),
        }
    }

    let config = config.ok_or(CliError::NoConfig)?;
    if check && run_for.is_some() {
        return Err(CliError::CheckAndRun);
    }
    Ok(Invocation::Run(Args {
        config,
        check,
        run_for,
    }))
}

#[must_use]
pub fn version_line() -> String {
    format!("dz-recorder {BUILD_VERSION} ({})", build_commit())
}

/// Durations carry a unit here for the same reason the configuration's do: both
/// plausible readings of a bare number are wrong, one of them by a factor of a
/// billion.
///
/// Spelled the same way as `[archive] rotate_interval`, deliberately, so an
/// operator learns one syntax. The configuration crate's parser is private to
/// its deserializer, which is why this is not a call into it.
fn parse_duration(raw: &str) -> Result<Duration, String> {
    let text = raw.trim();
    let boundary = text
        .find(|c: char| !c.is_ascii_digit())
        .ok_or_else(|| format!("`{text}` has no unit (ns, us, ms, s, m, h)"))?;
    let (digits, unit) = text.split_at(boundary);
    let value: u64 = digits
        .parse()
        .map_err(|_| format!("`{text}` is not a whole number followed by a unit"))?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_of(args: &[&str]) -> Result<Invocation, CliError> {
        parse(args.iter().map(|s| (*s).to_owned()))
    }

    #[test]
    fn a_configuration_path_is_required() {
        assert_eq!(parse_of(&[]), Err(CliError::NoConfig));
        assert_eq!(parse_of(&["--check"]), Err(CliError::NoConfig));
    }

    #[test]
    fn check_mode_is_asked_for_by_name() {
        let Ok(Invocation::Run(args)) = parse_of(&["--config", "r.toml", "--check"]) else {
            panic!("--check is an invocation");
        };
        assert!(args.check);
        assert_eq!(args.config, PathBuf::from("r.toml"));
        assert_eq!(args.run_for, None);
    }

    #[test]
    fn a_bounded_run_takes_a_duration_with_a_unit() {
        let Ok(Invocation::Run(args)) = parse_of(&["--config", "r.toml", "--run-for", "90s"])
        else {
            panic!("--run-for is an invocation");
        };
        assert_eq!(args.run_for, Some(Duration::from_secs(90)));
    }

    #[test]
    fn a_duration_without_a_unit_is_refused_rather_than_guessed_at() {
        let error = parse_of(&["--config", "r.toml", "--run-for", "90"]).unwrap_err();
        assert!(matches!(error, CliError::BadDuration(_)), "{error}");
        let error = parse_of(&["--config", "r.toml", "--run-for", "90 fortnights"]).unwrap_err();
        assert!(matches!(error, CliError::BadDuration(_)), "{error}");
    }

    #[test]
    fn checking_and_recording_are_not_asked_for_together() {
        assert_eq!(
            parse_of(&["--config", "r.toml", "--check", "--run-for", "1s"]),
            Err(CliError::CheckAndRun)
        );
    }

    #[test]
    fn an_option_with_no_value_is_named_in_the_error() {
        assert_eq!(
            parse_of(&["--config"]),
            Err(CliError::MissingValue("--config"))
        );
        assert_eq!(
            parse_of(&["--config", "r.toml", "--run-for"]),
            Err(CliError::MissingValue("--run-for"))
        );
    }

    #[test]
    fn an_unknown_option_is_refused_rather_than_ignored() {
        assert_eq!(
            parse_of(&["--config", "r.toml", "--nope"]),
            Err(CliError::Unknown("--nope".to_owned()))
        );
    }

    #[test]
    fn help_and_version_short_circuit_everything_else() {
        assert_eq!(parse_of(&["--help"]), Ok(Invocation::Help));
        assert_eq!(parse_of(&["--version"]), Ok(Invocation::Version));
        assert_eq!(
            parse_of(&["--config", "r.toml", "--version"]),
            Ok(Invocation::Version)
        );
    }

    #[test]
    fn the_version_line_carries_the_build_and_its_commit() {
        let line = version_line();
        assert!(line.contains(BUILD_VERSION), "{line}");
        assert!(line.contains(build_commit()), "{line}");
    }

    #[test]
    fn the_usage_says_a_signal_runs_the_whole_shutdown_sequence() {
        // Where an operator looks for how to stop a recorder is where they
        // have to learn that stopping it costs nothing — and that a second
        // signal will not wait.
        assert!(USAGE.contains("Stopping it"), "{USAGE}");
        assert!(USAGE.contains("second signal"), "{USAGE}");
    }
}
