//! `dz-recorder`: the binary that turns the recorder crates into something that
//! runs.
//!
//! It reads a configuration, joins what it was told to join, writes every
//! datagram it receives into a pcapng archive with the recorder's own losses
//! recorded inside it, says how it is doing on `/metrics`, and stops without
//! losing what it is holding.
//!
//! Three properties are worth stating here because everything below is arranged
//! around them.
//!
//! **It refuses rather than invents.** A configuration that is incomplete, that
//! contradicts itself, or that asks for something this build cannot do fails
//! before a single datagram is recorded, with a message naming the key. A
//! recorder that starts on a guess writes an archive of the wrong thing, and an
//! archive of the wrong thing is indistinguishable from an archive of the right
//! one until somebody draws a finding from it.
//!
//! **It records nothing it decodes.** The health tier reads the 24-byte
//! datagram header and the archive reads nothing at all. A message a decoder
//! rejects is a message the archive never holds, and the evidence needed to
//! diagnose that bug is what the bug destroyed.
//!
//! **It never blocks the record path.** Compression, hashing, publication and
//! the staging budget all happen off the loop that drains the capture. A writer
//! that blocked on a full disk would stall that loop, overflow the receive
//! queue, and convert a storage outage into feed loss plus a false
//! publisher-loss finding in every archive written during it.
#![forbid(unsafe_code)]

mod cli;
mod endpoint;
mod identity;
/// Declared in every build, feature or not: a build without inline mode still
/// has to refuse `--inline-config` by name, and a refusal that only exists in
/// the builds that do not need it is no refusal at all.
mod inline_config;
/// The record path for inline mode, which exists only where the mode does.
#[cfg(feature = "inline")]
mod inline_runner;
mod runner;
mod startup;

use std::fs;
use std::process::ExitCode;

use thiserror::Error;

use cli::{Args, CliError, Invocation};
use startup::{Plan, StartupError};

/// The command line could not be understood, which is a different failure from
/// a recorder that refused to start: a deployment pipeline distinguishes them.
const USAGE_EXIT: u8 = 2;

#[derive(Debug, Error)]
enum Failure {
    #[error("{0}")]
    Startup(#[from] StartupError),
    #[error("{0}")]
    /// Boxed: this is the widest variant by a long way — its refusals carry
    /// paths and a parser's own error — and an unboxed one makes every `run`
    /// result that size, including the overwhelmingly common `Ok`.
    Inline(#[from] Box<inline_config::InlineConfigError>),
    #[error("{0}")]
    Run(#[from] runner::RunError),
}

/// Which arrangement this invocation is running, printed where the summary is
/// read.
///
/// Two arrangements exist and they keep different things. A summary that named
/// neither would leave an operator to infer the mode from which keys were
/// echoed back, and the one they need to be sure of is whether this host is
/// keeping the bytes.
const ARCHIVE_MODE: &str =
    "mode=archive: every datagram is written to an object, and the loader derives the rows";

fn main() -> ExitCode {
    let invocation = match cli::parse(std::env::args().skip(1)) {
        Ok(invocation) => invocation,
        Err(e) => return usage_error(&e),
    };
    let args = match invocation {
        Invocation::Help => {
            println!("{}", cli::USAGE);
            return ExitCode::SUCCESS;
        }
        Invocation::Version => {
            println!("{}", cli::version_line());
            return ExitCode::SUCCESS;
        }
        Invocation::Run(args) => args,
    };

    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("dz-recorder: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage_error(error: &CliError) -> ExitCode {
    eprintln!("dz-recorder: {error}");
    eprintln!();
    eprintln!("{}", cli::USAGE);
    ExitCode::from(USAGE_EXIT)
}

fn run(args: &Args) -> Result<(), Failure> {
    let text = fs::read_to_string(&args.config).map_err(|source| StartupError::Read {
        path: args.config.clone(),
        source,
    })?;
    let config =
        dz_recorder_core::RecorderConfig::parse(&text).map_err(|source| StartupError::Config {
            path: args.config.clone(),
            source,
        })?;
    // Inline mode before the archive plan, and never after it: the mode writes
    // no object, so the archive directories it refuses are the ones an archive
    // plan requires. A build without the feature refuses here rather than
    // falling back to the arrangement nobody chose.
    if let Some(path) = &args.inline_config {
        return Ok(inline_config::run(&config, path, args.check, args.run_for).map_err(Box::new)?);
    }

    let plan = Plan::from_config(&config)?;

    if args.check {
        // Nothing is bound, nothing is created and nothing is joined: this runs
        // in a deployment pipeline, against a host that may already be
        // recording, before anything is restarted.
        println!("{ARCHIVE_MODE}");
        print!("{}", plan.summary());
        println!("configuration is valid");
        return Ok(());
    }

    eprintln!("dz-recorder: {}", cli::version_line());
    eprintln!("{ARCHIVE_MODE}");
    eprint!("{}", plan.summary());
    runner::run(&plan, args.run_for)?;
    Ok(())
}
