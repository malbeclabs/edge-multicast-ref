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
    Run(#[from] runner::RunError),
}

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
    let plan = Plan::from_config(&config)?;

    if args.check {
        // Nothing is bound, nothing is created and nothing is joined: this runs
        // in a deployment pipeline, against a host that may already be
        // recording, before anything is restarted.
        print!("{}", plan.summary());
        println!("configuration is valid");
        return Ok(());
    }

    eprintln!("dz-recorder: {}", cli::version_line());
    eprint!("{}", plan.summary());
    runner::run(&plan, args.run_for)?;
    Ok(())
}
