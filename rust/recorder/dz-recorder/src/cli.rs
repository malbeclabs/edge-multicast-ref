//! The command line, parsed by hand because there are six options and a
//! dependency to parse them is a dependency in the record path.
//!
//! Two of the six choose an arrangement, and only one of them has to be
//! written down: inline mode is what a command line naming no mode is read
//! as, and `--archive` is how the arrangement that keeps every datagram is
//! asked for. Nothing here reads a file, so nothing here can tell whether
//! the reading was right — that is what the refusals in
//! [`crate::inline_config`] and [`crate::startup`] are for.

use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;

use crate::identity::{build_commit, BUILD_VERSION};

pub const USAGE: &str = "\
dz-recorder — captures an edge feed, derives its rows or archives its bytes, and
says how it is doing.

Two arrangements, and the one a command line naming no mode is read as is inline
mode: rows, and no datagram kept. Archive mode keeps every datagram and is asked
for by name.

Usage:
  dz-recorder --config <path> --inline-config <path> [--run-for <duration>]
  dz-recorder --config <path> --archive [--run-for <duration>]
  dz-recorder --config <path> [--inline-config <path> | --archive] --check
  dz-recorder --version
  dz-recorder --help

Options:
  --config <path>       The TOML configuration. Required.

  --archive             Run archive mode: every datagram is written to a hashed,
                        manifested object, and dz-recorder-load derives the rows
                        from it in a second process. The bytes are kept, so a
                        conformance rule written next month can be run against
                        them and a row can be re-derived from what was verified.

                        THIS IS WHAT A HOST RECORDING A PRODUCTION FEED FOR
                        EVIDENCE RUNS, AND IT TAKES THIS FLAG. It used to be
                        what omitting a flag got, and it is not any more. A
                        configuration naming `[archive] staging_dir` or
                        `completed_dir` without this flag is refused at startup
                        naming the key, rather than read as a recorder that
                        quietly stopped keeping bytes.

                        Requires `[archive] staging_dir` and `completed_dir`,
                        which are exactly the keys inline mode refuses a value
                        for. That is why neither mode can be entered by
                        accident, and why the two cannot both be asked for.

  --inline-config <path>
                        Inline mode's own file: the window bound, the ring, the
                        spool and its budget, the ledger, and the destination.

                        INLINE MODE IS THE DEFAULT — it is what a command line
                        naming no mode is read as — AND NO DATAGRAM IS KEPT.
                        There is no archive, no manifest and no digest: a
                        conformance rule written next month has nothing to run
                        against, a row cannot be re-derived, and nothing
                        verifies what the derivation read. Every row this mode
                        writes says so, in its `derivation` column.

                        The default is a reading and not an invention. There is
                        no defensible spool directory, ledger or destination to
                        guess at, so a command line that names no mode and gives
                        no file here is refused naming this flag and --archive,
                        rather than started on a guess.

                        What it keeps is rows, spooled to disk under a byte
                        budget until the destination has taken them. What it
                        does not keep is the bytes they were derived from.

                        Because nothing writes an object, `[archive]
                        staging_dir` and `completed_dir` must carry no value: a
                        recorder that finds one refuses rather than leaving an
                        operator believing in bytes nobody kept.

                        `site` and `recorder` are not in this file: they come
                        from --config, so the live rows and the archived rows of
                        one host cannot name two recorders that do not exist.
                        Neither file has a password key; the destination's comes
                        from DZ_LOADER_CLICKHOUSE_PASSWORD_FILE or
                        DZ_LOADER_CLICKHOUSE_PASSWORD, and from nowhere else.

                        The mode needs a build carrying `inline`, which is a
                        default feature because the mode is the default mode. A
                        build made with `--no-default-features` can only record
                        an archive: it refuses a command line naming no mode by
                        the feature's name and by --archive, and the way back is
                        the default features or `--features inline`.

  --check               Validate the configuration and exit, recording nothing.
                        Nothing is bound, nothing is created and nothing is
                        joined: this is what a deployment pipeline runs before
                        it restarts anything.

                        In inline mode it validates both files and asks the
                        destination for `SELECT 1`, because a gate that passed
                        without reaching the destination would let a pipeline
                        restart a recorder that cannot write. The spool and the
                        ledger are not touched.

                        It is also where a command line that names the wrong
                        mode — or none — is caught, before anything is
                        restarted. Run it as an ExecStartPre and a missing
                        --archive costs a failed pre-check rather than a
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
    #[error(
        "--archive and --inline-config cannot both be asked for: one keeps every datagram and \
         one keeps none, and there is no reading of both that is what somebody meant"
    )]
    ArchiveAndInline,
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
    /// Archive mode, asked for by name.
    ///
    /// Its absence is inline mode. A default decides how a command line naming
    /// no mode is *read*, and this one is put on the reading that fails loudly:
    /// a configuration describing an archive read as inline mode is refused by
    /// key, because inline mode refuses the two directories archive mode
    /// requires — where an inline-mode configuration read as archive mode would
    /// have been a host that quietly stopped keeping bytes and looked healthy
    /// doing it. See [`USAGE`].
    pub archive: bool,
    /// Inline mode's own file, which the default mode needs and cannot invent.
    ///
    /// Not the mode switch, which it used to be. `None` without
    /// [`archive`](Self::archive) is still inline mode, and is a refusal naming
    /// this flag rather than a fallback to the arrangement nobody asked for.
    pub inline_config: Option<PathBuf>,
    pub check: bool,
    /// `None` records until a signal arrives, which runs the whole shutdown
    /// sequence and publishes the open segment. See [`USAGE`].
    pub run_for: Option<Duration>,
}

/// Parses the arguments after the program name.
pub fn parse<I: IntoIterator<Item = String>>(args: I) -> Result<Invocation, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut archive = false;
    let mut inline_config: Option<PathBuf> = None;
    let mut check = false;
    let mut run_for: Option<Duration> = None;

    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Ok(Invocation::Help),
            "--version" => return Ok(Invocation::Version),
            "--check" => check = true,
            "--archive" => archive = true,
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
    // Refused here rather than at startup, because it is a contradiction in the
    // command line itself and not in a file: the two arrangements keep different
    // things, and a recorder that picked one of them would be picking which of
    // an operator's two statements to ignore.
    if archive && inline_config.is_some() {
        return Err(CliError::ArchiveAndInline);
    }
    Ok(Invocation::Run(Args {
        config,
        archive,
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

    /// A command line naming no mode is inline mode, and archive mode takes a
    /// flag.
    ///
    /// This is the whole of the default at the command line's altitude: nothing
    /// here reads a file, so all it can say is which arrangement was asked for.
    /// What makes the reading safe is the refusals — an archive-shaped
    /// configuration reaching inline mode is refused by key — and those are
    /// asserted in `inline_config` and at binary altitude.
    #[test]
    fn a_command_line_naming_no_mode_is_inline_mode() {
        let Ok(Invocation::Run(args)) = parse_of(&["--config", "r.toml"]) else {
            panic!("--config alone is an invocation");
        };
        assert!(
            !args.archive,
            "inline mode is what a command line naming no mode is read as"
        );
        assert_eq!(
            args.inline_config, None,
            "and it has no file yet, which is a refusal and not a fallback"
        );

        let Ok(Invocation::Run(args)) =
            parse_of(&["--config", "r.toml", "--inline-config", "i.toml"])
        else {
            panic!("--inline-config is an invocation");
        };
        assert!(!args.archive);
        assert_eq!(args.inline_config, Some(PathBuf::from("i.toml")));
        assert_eq!(args.config, PathBuf::from("r.toml"));
    }

    /// Archive mode is asked for by name, and the flag carries no path: what it
    /// needs is already the `[archive]` section of the recorder's own file.
    #[test]
    fn archive_mode_is_asked_for_by_name() {
        let Ok(Invocation::Run(args)) = parse_of(&["--config", "r.toml", "--archive"]) else {
            panic!("--archive is an invocation");
        };
        assert!(args.archive);
        assert_eq!(args.inline_config, None);
        assert_eq!(args.config, PathBuf::from("r.toml"));
    }

    /// One host cannot be both arrangements, and the contradiction is in the
    /// command line rather than in a file.
    #[test]
    fn naming_both_modes_is_refused() {
        assert_eq!(
            parse_of(&[
                "--config",
                "r.toml",
                "--archive",
                "--inline-config",
                "i.toml"
            ]),
            Err(CliError::ArchiveAndInline)
        );
        // Either order, because an operator's argument order is not a
        // statement about which arrangement they meant.
        assert_eq!(
            parse_of(&[
                "--config",
                "r.toml",
                "--inline-config",
                "i.toml",
                "--archive"
            ]),
            Err(CliError::ArchiveAndInline)
        );
    }

    /// Where an operator learns what each arrangement costs is the flag that
    /// selects it — and, for inline mode, the flag they did not have to pass.
    #[test]
    fn the_usage_says_which_mode_is_the_default_and_what_it_keeps() {
        assert!(USAGE.contains("--inline-config"), "{USAGE}");
        assert!(USAGE.contains("NO DATAGRAM IS KEPT"), "{USAGE}");
        assert!(USAGE.contains("--features inline"), "{USAGE}");
        // And that the identity is the recorder's own, which is what stops one
        // host being two recorders in one dashboard.
        assert!(USAGE.contains("are not in this file"), "{USAGE}");

        // The inversion, in the two places an operator meets it: that saying
        // nothing is inline mode, and that the arrangement which keeps the
        // bytes now takes a flag it did not use to take.
        assert!(USAGE.contains("INLINE MODE IS THE DEFAULT"), "{USAGE}");
        assert!(USAGE.contains("--archive"), "{USAGE}");
        assert!(
            USAGE.contains("EVIDENCE RUNS, AND IT TAKES THIS FLAG"),
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
