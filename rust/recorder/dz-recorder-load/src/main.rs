//! `dz-recorder-load`: turns a directory of completed objects into rows a
//! dashboard can ask.
//!
//! # Where it runs, and the prerequisite that is missing
//!
//! **On the recorder host, against that host's own completed directory, opened
//! read-only.** Nothing ships objects off a recorder host today, and objects are
//! evicted under the staging budget in about a day and a half on a busy one. The
//! rows are tens of bytes against a datagram's twelve hundred, so the small
//! thing travels and the bytes stay local.
//!
//! State that plainly, because it is the consequence: **this is what makes the
//! cross-site join available before a shipper exists**, since the join is over
//! rows and not over objects. Two sites' loaders write into one column store and
//! `(channel instance, sequence number)` identifies a datagram independently of
//! who received it, so the comparison the whole tier exists for needs no
//! shipper — only two loaders that keep up.
//!
//! **The gate on that arrangement is lag against eviction.** A loader that falls
//! behind eviction loses history that no re-run can recover, so
//! `dz_loader_oldest_unloaded_age_seconds` is a metric with an alert and not a
//! log line. It is the one number that has to be watched for this arrangement to
//! be sound.
//!
//! # Three properties everything below is arranged around
//!
//! **It cannot touch the recorder.** The two processes share one directory,
//! opened read-only here, and a ledger this one owns. A column store that is
//! down, slow or full costs loading progress and nothing else. The record path
//! gains no key from any of this: `RecorderConfig` documents the absence of an
//! endpoint, a credential and a database key as an invariant, and this binary
//! has its own configuration file, service user and metrics port.
//!
//! **It refuses rather than invents.** An object whose sha256 is not the one its
//! manifest states derives nothing, and neither does one whose archive will not
//! say at what scope its drop counts may be subtracted. A finding drawn from an
//! object nobody verified is a finding about a file, not about a feed.
//!
//! **A failed object stays unloaded.** The ledger entry is written after the
//! rows are in. Loading is idempotent on `(object key, sha256)` and the tables
//! are `ReplacingMergeTree`, so a retry is a replace — which makes leaving the
//! object unloaded the cheap answer and reporting partial success the expensive
//! one: an object whose datagram rows landed and whose gap rows did not reads as
//! a clean feed for ever.
#![forbid(unsafe_code)]

mod cli;
mod endpoint;
mod identity;

use std::collections::HashSet;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use dz_recorder_clickhouse::ClickHouseSink;
use dz_recorder_rows::{FileSink, RowSink};
use thiserror::Error;

use cli::{Args, CliError, Invocation, Mode};
use dz_recorder_conformance::{ConformanceTool, PinnedRuleSet};
use dz_recorder_load::config::{self, LoaderConfig};
use dz_recorder_load::conformance::ledger::{JudgeLedger, JudgeLedgerError};
use dz_recorder_load::conformance::stage::{record_judged, HeldJudgement, Judge, JudgedFeed};
// The pass, the ledger and the metrics are the library's, because inline mode
// needs them and a second copy of the ledger would be a second answer to
// *is this loaded*. See `lib.rs`.
use dz_recorder_load::ledger::{self, Ledger};
use dz_recorder_load::loader::{self, now_unix_nanos, Loader};
use dz_recorder_load::metrics::LoaderMetrics;

/// The command line could not be understood, which is a different failure from a
/// loader that refused to start: a deployment pipeline distinguishes them.
const USAGE_EXIT: u8 = 2;

#[derive(Debug, Error)]
enum Failure {
    #[error("reading {path}: {source}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Config {
        path: std::path::PathBuf,
        #[source]
        source: config::ConfigError,
    },
    #[error("{0}")]
    Ledger(#[from] ledger::LedgerError),
    #[error("the metrics endpoint could not bind {addr}: {source}")]
    Metrics {
        addr: std::net::SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "the destination could not be reached, so nothing here would land: {0}. \
         The password comes from DZ_LOADER_CLICKHOUSE_PASSWORD and from nowhere else."
    )]
    Unreachable(String),
    #[error("--dry-run {path}: {source}")]
    DryRun {
        path: std::path::PathBuf,
        #[source]
        source: dz_recorder_rows::RowSinkError,
    },
    #[error("{failed} object(s) could not be loaded; the first was: {first}")]
    Objects { failed: u64, first: String },
    #[error("{0}")]
    JudgeLedger(#[from] JudgeLedgerError),
    #[error("conformance scratch {path}: {cause}")]
    Scratch {
        path: std::path::PathBuf,
        #[source]
        cause: std::io::Error,
    },
    #[error("[[conformance]] `{feed}`: {detail}")]
    RuleSet { feed: String, detail: String },
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
            eprintln!("dz-recorder-load: {e}");
            ExitCode::FAILURE
        }
    }
}

fn usage_error(error: &CliError) -> ExitCode {
    eprintln!("dz-recorder-load: {error}");
    eprintln!();
    eprintln!("{}", cli::USAGE);
    ExitCode::from(USAGE_EXIT)
}

fn run(args: &Args) -> Result<(), Failure> {
    let text = std::fs::read_to_string(&args.config).map_err(|source| Failure::Read {
        path: args.config.clone(),
        source,
    })?;
    let config = LoaderConfig::parse(&text).map_err(|source| Failure::Config {
        path: args.config.clone(),
        source,
    })?;
    config.check().map_err(|source| Failure::Config {
        path: args.config.clone(),
        source,
    })?;

    if args.mode == Mode::Check {
        // Nothing is opened, nothing is written and the ledger is not touched:
        // this runs in a deployment pipeline, against a host that may already be
        // loading, before anything is restarted.
        print!("{}", config.summary());
        // **A WARNING AND NOT A REFUSAL, AND THE DIFFERENCE IS A DEPLOY.** A
        // feed configured in the same change as the recorder that will write it
        // has no directory until that recorder's first publication, so refusing
        // would fail every simultaneous deploy. The other cause is a
        // misspelling, which scans nothing for ever while everything reads
        // healthy -- so it is said here, on the one surface a pipeline reads,
        // and `dz_loader_scanned_feeds_without_objects` is what says it later.
        for feed in config.feeds_without_a_directory() {
            eprintln!(
                "dz-recorder-load: warning: [loader] feeds names `{feed}`, which has no \
                 directory under {}. If its recorder has not published yet this clears \
                 itself; if the name is wrong it scans nothing, for ever, with no error. \
                 dz_loader_scanned_feeds_without_objects counts these every pass.",
                config.loader.objects_dir.display()
            );
        }
        let sink = ClickHouseSink::over_http(config.clickhouse.clone());
        let probe = sink
            .statement("SELECT 1")
            .map_err(|e| Failure::Unreachable(e.to_string()))?;
        println!("destination answered: {}", probe.trim());
        // Each rule set asked which one it is, as a pass would ask it. A tool
        // that cannot say, or says something else, fails the check rather than
        // judging nothing quietly once the unit is running.
        for judged in &config.conformance {
            let scratch = config
                .loader
                .conformance_scratch
                .clone()
                .unwrap_or_default();
            let tool = judged.resolved_tool().map_err(|e| Failure::RuleSet {
                feed: judged.feed.clone(),
                detail: e.to_string(),
            })?;
            let pinned = PinnedRuleSet::resolve(
                ConformanceTool::new(tool, scratch),
                &judged.rule_set_version,
            )
            .map_err(|e| Failure::RuleSet {
                feed: judged.feed.clone(),
                detail: e.to_string(),
            })?;
            println!(
                "conformance feed={} rule set answered {}",
                judged.feed,
                pinned.version()
            );
        }
        println!("configuration is valid");
        return Ok(());
    }

    eprintln!("dz-recorder-load: {}", cli::version_line());
    eprint!("{}", config.summary());

    let metrics = Arc::new(LoaderMetrics::new(
        &config.loader.site,
        &config.loader.recorder,
    ));
    // Before the first pass, and it must succeed: a loader nobody can scrape is
    // a loader whose lag nobody can see, and lag is what this tier is gated on.
    let server =
        endpoint::serve(Arc::clone(&metrics), config.metrics.listen_addr).map_err(|source| {
            Failure::Metrics {
                addr: config.metrics.listen_addr,
                source,
            }
        })?;
    // What was actually bound, which is not what was configured when the
    // configuration asked for port 0 — and a loader nobody can find is a loader
    // whose lag nobody can see.
    if let Some(bound) = server.local_addr() {
        eprintln!("dz-recorder-load: metrics on http://{bound}/metrics");
    }

    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    if let Err(e) = ctrlc::set_handler(move || flag.store(true, Ordering::SeqCst)) {
        // It still loads, and it says so. A `--once` pass ends on its own, and a
        // `--watch` without a handler is stopped by killing it — which loses at
        // most the object in flight, and re-loading that is a replace.
        eprintln!("dz-recorder-load: no signal handler ({e}); stop this with --once");
    }

    match &args.dry_run {
        Some(dir) => {
            let mut sink = FileSink::create(dir).map_err(|source| Failure::DryRun {
                path: dir.clone(),
                source,
            })?;
            // In memory, so a dry run records nothing and can be repeated: the
            // question a dry run answers is what the rows would say, and a run
            // that marked the objects loaded would answer it exactly once.
            let mut ledger = Ledger::in_memory();
            // A sink of its own over the same directory: it writes only the
            // findings file, which the load's sink never opens.
            let mut conformance = conformance_of(&config, true, || {
                FileSink::create(dir).map_err(|cause| Failure::DryRun {
                    path: dir.clone(),
                    source: cause,
                })
            })?;
            drive(
                args,
                &config,
                &metrics,
                &stop,
                &mut ledger,
                &mut sink,
                conformance.as_mut(),
            )
        }
        None => {
            let mut sink = ClickHouseSink::over_http(config.clickhouse.clone());
            let mut ledger = Ledger::open(&config.loader.ledger)?;
            eprintln!(
                "dz-recorder-load: resuming with {} ledger entr{}",
                ledger.entries(),
                if ledger.entries() == 1 { "y" } else { "ies" }
            );
            let mut conformance = conformance_of(&config, false, || {
                Ok(ClickHouseSink::over_http(config.clickhouse.clone()))
            })?;
            drive(
                args,
                &config,
                &metrics,
                &stop,
                &mut ledger,
                &mut sink,
                conformance.as_mut(),
            )
        }
    }
}

/// The conformance runner's state, carried across passes. Exists only when a
/// `[[conformance]]` entry names a feed.
struct Conformance<S> {
    feeds: Vec<JudgedFeed<ConformanceTool>>,
    scratch: std::path::PathBuf,
    ledger: JudgeLedger,
    /// Its own: the objects a sink reports landed are what a ledger records,
    /// and one sink shared with the load would report each one to both.
    sink: S,
    held: Vec<HeldJudgement>,
    /// Failed attempts per object, so one that keeps failing is given up on
    /// rather than retried first in its feed on every pass.
    failures: std::collections::HashMap<dz_recorder_rows::ObjectId, u32>,
}

fn conformance_of<S>(
    config: &LoaderConfig,
    dry_run: bool,
    sink: impl FnOnce() -> Result<S, Failure>,
) -> Result<Option<Conformance<S>>, Failure> {
    if config.conformance.is_empty() {
        return Ok(None);
    }
    // `check` has already refused a configuration without these two.
    let scratch = config
        .loader
        .conformance_scratch
        .clone()
        .unwrap_or_default();
    std::fs::create_dir_all(&scratch).map_err(|cause| Failure::Scratch {
        path: scratch.clone(),
        cause,
    })?;
    let ledger = if dry_run {
        JudgeLedger::in_memory()
    } else {
        let ledger =
            JudgeLedger::open(config.loader.conformance_ledger.clone().unwrap_or_default())?;
        eprintln!(
            "dz-recorder-load: conformance resuming with {} ledger entr{}",
            ledger.entries(),
            if ledger.entries() == 1 { "y" } else { "ies" }
        );
        ledger
    };
    // The resolved path, so the file `check` looked at is the file that runs:
    // a bare name handed to the process launcher is looked up on PATH.
    let feeds = config
        .conformance
        .iter()
        .map(|c| {
            let tool = c.resolved_tool().map_err(|e| Failure::RuleSet {
                feed: c.feed.clone(),
                detail: e.to_string(),
            })?;
            Ok(JudgedFeed {
                feed: c.feed.clone(),
                rule_set_feed: c.rule_set_feed.clone(),
                rule_set_version: c.rule_set_version.clone(),
                rule_set: ConformanceTool::new(tool, &scratch),
                verdicts: c.verdicts,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(Some(Conformance {
        feeds,
        scratch,
        ledger,
        sink: sink()?,
        held: Vec::new(),
        failures: std::collections::HashMap::new(),
    }))
}

/// One pass, or every pass until a signal.
fn drive<S: RowSink, C: RowSink>(
    args: &Args,
    config: &LoaderConfig,
    metrics: &Arc<LoaderMetrics>,
    stop: &Arc<AtomicBool>,
    ledger: &mut Ledger,
    sink: &mut S,
    mut conformance: Option<&mut Conformance<C>>,
) -> Result<(), Failure> {
    let stopping = {
        let stop = Arc::clone(stop);
        move || stop.load(Ordering::SeqCst)
    };
    let mut first_failure: Option<String> = None;
    let mut failed = 0u64;
    // Carried across passes because the sink is: a quiet feed's rows may be
    // held for the whole `insert_max_delay`, which is several passes.
    let mut pending: Vec<loader::Pending> = Vec::new();
    // Built once and not per pass. It is the one representation of the scan
    // set, and both the walk's filter and the ledger's compaction scope are
    // spent from it -- two spellings is how those two come to disagree about
    // which feeds a pass was responsible for.
    let feeds: Option<HashSet<String>> = config
        .loader
        .feeds
        .as_ref()
        .map(|feeds| feeds.iter().cloned().collect());

    loop {
        let (pass, errors) = Loader {
            objects_dir: &config.loader.objects_dir,
            feeds: feeds.as_ref(),
            site: &config.loader.site,
            recorder: &config.loader.recorder,
            max_objects: config.loader.max_objects_per_pass,
            ledger,
            sink,
            metrics,
            market_data: &config.market_data,
            pending: &mut pending,
        }
        .run_once(&stopping);

        for message in &errors {
            eprintln!("dz-recorder-load: {message}");
        }
        failed += pass.failed;
        if first_failure.is_none() {
            first_failure = errors.first().cloned();
        }
        eprintln!(
            "dz-recorder-load: derived {} object(s), loaded {}, skipped {}, failed {}; \
             {} unloaded of which {} held by the sink, oldest {}s behind",
            pass.derived,
            pass.loaded,
            pass.skipped,
            pass.failed,
            pass.unloaded,
            pass.held,
            pass.oldest_unloaded_age_seconds
        );
        // Only where a feed derives. A line reading "0 unloaded" on every host
        // that turned nothing on is a line an operator learns to skip, and the
        // one host that did turn something on is the host whose line matters.
        if !config.market_data.is_empty() {
            eprintln!(
                "dz-recorder-load: market data: {} unloaded, oldest {}s behind",
                pass.market_data_unloaded, pass.market_data_oldest_unloaded_age_seconds
            );
        }

        // After the load, and only where a feed is judged. The two share the
        // directory and nothing else.
        if let Some(c) = conformance.as_deref_mut() {
            let (judged, errors) = Judge {
                objects_dir: &config.loader.objects_dir,
                site: &config.loader.site,
                recorder: &config.loader.recorder,
                max_objects: config.loader.max_objects_per_pass,
                feeds: &c.feeds,
                scratch: &c.scratch,
                ledger: &mut c.ledger,
                sink: &mut c.sink,
                metrics,
                held: &mut c.held,
                failures: &mut c.failures,
            }
            .run_once(&stopping);
            for message in &errors {
                eprintln!("dz-recorder-load: {message}");
            }
            failed += judged.failed;
            if first_failure.is_none() {
                first_failure = errors.first().cloned();
            }
            eprintln!(
                "dz-recorder-load: conformance: judged {} object(s), recorded {}, failed {}; \
                 {} unjudged of which {} held by the sink, oldest {}s behind",
                judged.judged,
                judged.recorded,
                judged.failed,
                judged.unjudged,
                judged.held,
                judged.oldest_unjudged_age_seconds
            );
        }

        if args.mode == Mode::Once || stopping() {
            break;
        }
        // Slept in short slices so a signal is answered promptly: a loader that
        // took a whole poll interval to notice a SIGTERM would be killed
        // mid-object by every supervisor with a stop timeout.
        let mut waited = std::time::Duration::ZERO;
        while waited < config.loader.poll_interval && !stopping() {
            let slice =
                std::time::Duration::from_millis(200).min(config.loader.poll_interval - waited);
            std::thread::sleep(slice);
            waited += slice;
        }
        if stopping() {
            break;
        }
    }

    // Everything held, due or not: a `--once` pass and a shutdown both end here,
    // so no run leaves rows in memory that the ledger will never account for.
    // The objects that land are recorded, because rows in the store with no
    // ledger entry are an object the next run derives again for nothing.
    match sink.flush(now_unix_nanos()) {
        Ok(landed) => {
            metrics.bytes_posted(landed.bytes_posted);
            // The same recording a pass does, and not a second copy of it: an
            // object recorded differently on the way out is an object the next
            // run disagrees with itself about.
            let recorded = loader::record_landed(&landed.objects, &mut pending, ledger, metrics);
            if recorded.recorded > 0 {
                eprintln!(
                    "dz-recorder-load: flushed {} held object(s) on the way out",
                    recorded.recorded
                );
            }
            for message in recorded.failures {
                eprintln!("dz-recorder-load: ledger: {message}");
                failed += 1;
                if first_failure.is_none() {
                    first_failure = Some(message);
                }
            }
        }
        Err(e) => {
            eprintln!("dz-recorder-load: flush: {e}");
            failed += 1;
            if first_failure.is_none() {
                first_failure = Some(e.to_string());
            }
        }
    }
    if let Some(c) = conformance {
        match c.sink.flush(now_unix_nanos()) {
            Ok(landed) => {
                metrics.bytes_posted(landed.bytes_posted);
                let mut errors = Vec::new();
                // Every entry that could not be written is in `errors`, and is
                // counted as a failure below, one per message.
                let (recorded, _) = record_judged(
                    &landed.objects,
                    &mut c.held,
                    &mut c.ledger,
                    metrics,
                    &mut errors,
                );
                if recorded > 0 {
                    eprintln!(
                        "dz-recorder-load: conformance: flushed {recorded} held judgement(s) on \
                         the way out"
                    );
                }
                for message in errors {
                    eprintln!("dz-recorder-load: conformance ledger: {message}");
                    failed += 1;
                    if first_failure.is_none() {
                        first_failure = Some(message);
                    }
                }
            }
            Err(e) => {
                eprintln!("dz-recorder-load: conformance flush: {e}");
                failed += 1;
                if first_failure.is_none() {
                    first_failure = Some(e.to_string());
                }
            }
        }
    }

    // A pass that could not load an object exits non-zero, so a timer's own
    // failure count is the same number an operator would get from the metrics.
    // The objects that did load stay loaded: this is a report, not a rollback.
    match (failed, first_failure) {
        (0, _) => Ok(()),
        (failed, Some(first)) => Err(Failure::Objects { failed, first }),
        (failed, None) => Err(Failure::Objects {
            failed,
            first: "no message was recorded".to_owned(),
        }),
    }
}
