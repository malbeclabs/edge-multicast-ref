//! The command line, parsed by hand because there are five options and a
//! dependency to parse them is a dependency in the record path.
//!
//! **No option here names an arrangement.** One of the five contributes to
//! selecting one — `--inline-config` names the file inline mode cannot run
//! without — and the other half of the selection is two keys in the
//! configuration file, which nothing here reads. So nothing here decides which
//! arrangement runs: that is [`crate::startup::Arrangement::selected_by`], and
//! the refusals for a configuration stating both arrangements or neither are
//! its own.

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::identity::{build_commit, BUILD_VERSION};

pub const USAGE: &str = "\
dz-recorder — captures an edge feed, derives its rows or archives its bytes, and
says how it is doing.

Two arrangements, and NEITHER IS A DEFAULT. Each is selected by the
configuration only it can run on:

  archive mode   `[archive] staging_dir` and `completed_dir` are stated, and no
                 --inline-config is given. Every datagram is written to a
                 hashed, manifested object and dz-recorder-load derives the rows
                 from it in a second process. The bytes are kept, so a
                 conformance rule written next month can be run against them
                 and a row can be re-derived from what was verified. THIS IS
                 WHAT A HOST RECORDING A PRODUCTION FEED FOR EVIDENCE RUNS, and
                 it runs on the configuration it has always run on.

  inline mode    --inline-config is given, and neither archive directory carries
                 a value. One process captures, derives and loads. NO DATAGRAM
                 IS KEPT.

  both stated    Refused, naming the archive key and the file. Two arrangements
                 that keep different things were stated at once, and there is no
                 reading of both that is what somebody meant.

  neither        Refused, naming both. Archive mode needs two directories,
                 inline mode needs a spool, a ledger and a destination, and not
                 one of the five has a defensible value to invent.

No flag names an arrangement. Each mode is named by what it cannot run without,
so there is nothing to keep in step with the configuration and nothing that can
disagree with it. --check is where a host learns which arrangement it is in: it
prints the mode on its first line and exits non-zero if the configuration states
none or both.

Usage:
  dz-recorder --config <path> [--inline-config <path>] [--run-for <duration>]
  dz-recorder --config <path> [--inline-config <path>] --check
  dz-recorder --version
  dz-recorder --help

Options:
  --config <path>       The TOML configuration. Required. Its `[archive]`
                        directories are what select archive mode.

  --inline-config <path>
                        Inline mode's own file, and what selects inline mode:
                        the window bound, the ring, the spool and its budget,
                        the ledger, and the destination.

                        NO DATAGRAM IS KEPT. There is no archive, no manifest
                        and no digest: a conformance rule written next month has
                        nothing to run against, a row cannot be re-derived, and
                        nothing verifies what the derivation read. Every row
                        this mode writes says so, in its `derivation` column.

                        It selects the mode because it is what the mode needs.
                        There is no defensible spool directory, ledger or
                        destination to guess at, so this is a file an operator
                        states rather than a switch they set, and a
                        configuration that states neither this nor an archive is
                        refused rather than started on a guess.

                        What it keeps is rows, spooled to disk under a byte
                        budget until the destination has taken them. What it
                        does not keep is the bytes they were derived from.

                        `[archive] staging_dir` and `completed_dir` must carry
                        no value beside it: nothing writes an object here, so a
                        configuration stating both is a contradiction rather
                        than a mode.

                        It derives the five transport grains and no market data
                        rows. A `[[market_data]]` entry in this file is refused
                        by name — asking is answered, rather than leaving
                        `event`, `instrument` and `book_top` empty for a feed
                        somebody expected them from. Archive mode derives them,
                        in the loader.

                        `site` and `recorder` are not in this file: they come
                        from --config, so the live rows and the archived rows of
                        one host cannot name two recorders that do not exist.
                        Neither file has a password key; the destination's comes
                        from DZ_LOADER_CLICKHOUSE_PASSWORD_FILE or
                        DZ_LOADER_CLICKHOUSE_PASSWORD, and from nowhere else.

                        The mode needs a build carrying `inline`, which is a
                        default feature because the arrangement is a property of
                        a host's configuration and the released binary is one
                        binary for the fleet. A build made with
                        `--no-default-features` can only record an archive: it
                        refuses a configuration that selects inline mode by the
                        feature's name, and the way back is the default features
                        or `--features inline`.

  --check               Validate the configuration and exit, recording nothing.
                        Nothing is bound, nothing is created and nothing is
                        joined: this is what a deployment pipeline runs before
                        it restarts anything.

                        In inline mode it validates both files and asks the
                        destination for `SELECT 1`, because a gate that passed
                        without reaching the destination would let a pipeline
                        restart a recorder that cannot write. The spool and the
                        ledger are not touched.

                        It is also where a configuration that states both
                        arrangements, or neither, is caught — before anything is
                        restarted, and with the arrangement on the first line of
                        its output so a pipeline can read which one it is
                        deploying. Run it as an ExecStartPre and an unclear
                        arrangement costs a failed pre-check rather than a
                        recorder that would not start.

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
    /// Inline mode's own file, and half of what selects the arrangement.
    ///
    /// It selects inline mode because it is what inline mode needs: a spool
    /// directory, a ledger and a destination, none of which has a defensible
    /// value to invent and none of which the recorder's own configuration
    /// carries. The other half is the two `[archive]` directories, read from
    /// that configuration — so this is not a mode switch and there is no mode
    /// switch. `None` beside a stated archive is archive mode; `None` beside no
    /// archive is a refusal naming both. See
    /// [`Arrangement::selected_by`](crate::startup::Arrangement::selected_by).
    pub inline_config: Option<PathBuf>,
    pub check: bool,
    /// `None` records until a signal arrives, which runs the whole shutdown
    /// sequence and publishes the open segment. See [`USAGE`].
    pub run_for: Option<Duration>,
}

/// Parses the arguments after the program name.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Invocation, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut inline_config: Option<PathBuf> = None;
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
            "--inline-config" => {
                inline_config = Some(PathBuf::from(
                    args.next()
                        .ok_or(CliError::MissingValue("--inline-config"))?,
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
    // No arrangement is decided here. Both-stated and neither-stated are
    // contradictions between a file and a command line rather than within the
    // command line, so they are refused where both are in hand — and refusing
    // one of them here would mean refusing it twice, in two wordings.
    Ok(Invocation::Run(Args {
        config,
        inline_config,
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

    /// The command line carries no arrangement, either way round.
    ///
    /// This is all this altitude can say: nothing here reads a file, so nothing
    /// here can select an arrangement. `--inline-config` is a path and not a
    /// switch, and its absence is not a mode — the selection is
    /// `Arrangement::selected_by`, asserted in `startup` and at binary
    /// altitude.
    #[test]
    fn the_command_line_names_no_arrangement() {
        let Ok(Invocation::Run(args)) = parse_of(&["--config", "r.toml"]) else {
            panic!("--config alone is an invocation");
        };
        assert_eq!(
            args.inline_config, None,
            "no second file, which is not yet a mode either way"
        );

        let Ok(Invocation::Run(args)) =
            parse_of(&["--config", "r.toml", "--inline-config", "i.toml"])
        else {
            panic!("--inline-config is an invocation");
        };
        assert_eq!(args.inline_config, Some(PathBuf::from("i.toml")));
        assert_eq!(args.config, PathBuf::from("r.toml"));
    }

    /// A flag naming an arrangement is not a flag this binary has.
    ///
    /// Asserted rather than left to the absence of code. `--archive` existed on
    /// this branch and was retired: it could only ever agree with the
    /// configuration or be refused by it, and the one power it added was the
    /// power to resolve one of the two refusals — which is starting a recorder
    /// in an arrangement its own configuration contradicts. A flag is cheap to
    /// add back and the argument against it is long, so this test is what
    /// carries the decision to whoever reaches for it next.
    #[test]
    fn a_flag_naming_an_arrangement_is_not_a_flag_this_binary_has() {
        assert_eq!(
            parse_of(&["--config", "r.toml", "--archive"]),
            Err(CliError::Unknown("--archive".to_owned())),
            "the arrangement is selected by the configuration, never by a flag"
        );
        assert_eq!(
            parse_of(&["--config", "r.toml", "--inline"]),
            Err(CliError::Unknown("--inline".to_owned())),
            "and there is no flag for the other one either"
        );
        // And `USAGE` does not teach one. A usage text that documented a flag
        // the parser refuses would send an operator to a failing command line
        // with the binary's own blessing.
        assert!(!USAGE.contains("--archive"), "{USAGE}");
    }

    /// Where an operator learns what each arrangement costs, and what selects
    /// it: the configuration, in both cases.
    #[test]
    fn the_usage_says_what_selects_each_arrangement_and_what_it_keeps() {
        assert!(USAGE.contains("--inline-config"), "{USAGE}");
        assert!(USAGE.contains("NO DATAGRAM IS KEPT"), "{USAGE}");
        assert!(USAGE.contains("--features inline"), "{USAGE}");
        // And that the identity is the recorder's own, which is what stops one
        // host being two recorders in one dashboard.
        assert!(USAGE.contains("are not in this file"), "{USAGE}");

        // The selection, in the four cases an operator can be in. Read as four
        // assertions rather than one, because the two that refuse are the two
        // that make the other two a reading rather than a guess.
        assert!(USAGE.contains("NEITHER IS A DEFAULT"), "{USAGE}");
        assert!(USAGE.contains("both stated    Refused"), "{USAGE}");
        assert!(USAGE.contains("neither        Refused"), "{USAGE}");
        assert!(USAGE.contains("No flag names an arrangement"), "{USAGE}");
        assert!(
            USAGE.contains("EVIDENCE RUNS, and\n                 it runs on the configuration it has always run on"),
            "{USAGE}"
        );
        // The market data gap, where an operator choosing an arrangement is.
        assert!(
            USAGE.contains("no market data\n                        rows"),
            "{USAGE}"
        );
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
        assert_eq!(
            parse_of(&["--config", "r.toml", "--inline-config"]),
            Err(CliError::MissingValue("--inline-config"))
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
