//! `run`: the entry point a venue's `main` calls, and the only place in this
//! crate that opens anything.
//!
//! Everything above this module is composed from arguments and takes its time
//! from an injected clock, which is what makes the wiring testable with no
//! socket, no filesystem, no privilege and no sleep. This module is where the
//! real implementations are supplied — a state directory, an era file, two
//! multicast sockets, a metrics endpoint, a signal handler and a tokio runtime —
//! and it decides nothing that the composed publisher does not already decide.
//!
//! # Two futures, one task, and why the runtime is current-thread
//!
//! The publisher has to do two things at once: drive the transport, and tick.
//! [`Driver::run`](dz_ingress_core::Driver::run) borrows the adapter and the
//! event sink for as long as it runs, so the tick cannot hold either — and the
//! composed publisher is deliberately not `Send`, because
//! [`DatagramSink`](dz_publisher_egress::DatagramSink) has no `Send` bound and a
//! socket does not need to move between threads.
//!
//! So both run as futures in one task on a current-thread runtime, and they
//! reach the publisher through a [`RefCell`] and the adapter through a
//! [`Mutex`]. That is sound for one specific reason and it is a reason worth
//! stating: **neither borrow is ever held across an `await`.** Every
//! [`EventSink`] method and every tick body is synchronous, and the awaiting —
//! the receive, the send, the sleep — happens with nothing borrowed. A borrow
//! held across an await here would be a panic at runtime rather than a
//! compile error, which is the cost of this shape and the reason it is
//! confined to this module.

use std::cell::RefCell;
use std::ffi::{OsStr, OsString};
use std::net::SocketAddrV4;
use std::os::unix::ffi::OsStrExt as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::{
    Adapter, AdapterError, ConnectionId, DepthBound, Desync, DisconnectReason, Event, EventSink,
    InstrumentRef, ListingSink, ParseError, Payload, SnapshotSink, UpstreamSink,
    VenueTimestampKind,
};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_edge_mbp::MarketByPrice;
use dz_edge_tob::TopOfBook;
use dz_ingress_core::{Driver, IngressError, Input};
use dz_publisher_egress::{
    EraStore, FailureScope, KernelRoute, MulticastTransmitter, ReferenceStream, Tee,
};
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_refdata::{
    CycleSchedule, FileStore, Registry, RegistryConfig, ShardConfig, StateStore,
};

use crate::clock::{Clock, SystemClock};
use crate::config::{Config, Feed, FeedSpec, ShardName, Source, SourceRole};
use crate::error::StartupError;
use crate::guard::{Exit, Inconsistency};
use crate::observer::MetricsObserver;
use crate::pipeline::{FeedPipeline, PathDownSink, Port, Ports};
use crate::publisher::{Feeds, Publisher, ShardFeeds, SnapshotError};
use crate::{AdapterContext, AdapterRegistry};

/// How often the tick body runs.
///
/// A constant, not a key: every cadence the tick serves is read off the clock as
/// a debt rather than counted in ticks, so this value changes only how promptly
/// a due thing happens and never how much of it happens. Ten milliseconds is
/// well below the shortest cadence the design's own configuration states.
///
/// **One thing the tick serves is not a debt, and that is why this is public.**
/// The tick body takes at most one periodic snapshot, because a snapshot is a
/// group of datagrams and the unit of progress is an instrument. So this value
/// *is* the process's snapshot serving rate, every shard's rotation draws on
/// that one budget, and
/// [`rotation::schedule_share`](crate::rotation::schedule_share) needs it to say
/// whether the configured cycles can all be met. See `crate::rotation`'s note on
/// the ceiling.
pub const TICK: Duration = Duration::from_millis(10);

/// The most datagrams one definition tick may emit.
///
/// One, which is the smallest schedule that makes progress and the strongest
/// form of the anti-burst rule: the reference-data specification forbids
/// emitting the published set as a single burst, and one datagram per tick
/// cannot approximate one. A stall therefore degrades into a denser lap rather
/// than a spike, which is what
/// [`CycleSchedule`](dz_publisher_refdata::CycleSchedule) is built to do.
const MAX_DEFINITION_DATAGRAMS_PER_TICK: usize = 1;

/// What `--help` says, and what every refusal from the command-line reader
/// names.
///
/// Every accepted form, including both spellings of the two flags that publish
/// nothing: a reader that refuses an option by name is only half an answer if
/// the message does not also say what it would have taken.
const USAGE: &str = "usage: <publisher> [--config] <config.toml> | --version|-V | --help|-h";

/// The version [`run`] reports: this crate's own.
///
/// **The only compile-time version read in this module, and a test holds it at
/// one.** `CARGO_PKG_VERSION` expands to the version of the crate being
/// compiled, which here is the runtime and never the venue binary that links
/// it — so a venue that wants its own number reports it through
/// [`run_with_version`], and whatever is reported reaches stdout and
/// `dz_publisher_build_info` as one argument. Two reads of a version in this
/// file would be two answers to one question, and the one an operator compares
/// against a pin would be whichever they happened to ask.
const RUNTIME_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Run a publisher, reporting this crate's version as the build's.
///
/// The whole of a venue's `main`:
///
/// ```no_run
/// # use dz_publisher_runtime::{AdapterRegistry, Venue};
/// fn main() -> std::process::ExitCode {
///     dz_publisher_runtime::run(AdapterRegistry::new().with("a-venue", |_cx| {
///         unimplemented!("the venue's adapter and its transport")
///     }))
/// }
/// ```
///
/// Reads the configuration path from the command line, composes everything, and
/// returns only when a guard fires, a signal arrives, or the upstream turns out
/// to be unusable. A startup failure is printed and returns
/// [`ExitCode::FAILURE`]; every one of them names what would have been accepted.
///
/// `--version` answers with this crate's own version, which is the runtime's
/// number and not the linking binary's. A venue whose deployment pins its own
/// version passes it: see [`run_with_version`].
#[must_use]
pub fn run(registry: AdapterRegistry) -> ExitCode {
    run_with_version(RUNTIME_VERSION, registry)
}

/// Run a publisher that answers `--version` with `version`.
///
/// The whole of a venue's `main`, where the version a deployment pins is the
/// venue binary's:
///
/// ```no_run
/// # use dz_publisher_runtime::{AdapterRegistry, Venue};
/// fn main() -> std::process::ExitCode {
///     dz_publisher_runtime::run_with_version(
///         env!("CARGO_PKG_VERSION"),
///         AdapterRegistry::new().with("a-venue", |_cx| {
///             unimplemented!("the venue's adapter and its transport")
///         }),
///     )
/// }
/// ```
///
/// Everything [`run`] does, and `version` is the one string two consumers read
/// the build's identity from:
///
/// - `--version` and `-V` write it to **stdout**, alone, on one line, and exit
///   0. Exactly the pinned version and nothing else, because both consumers
///   compare rather than parse: a configuration-management role runs the binary
///   to decide whether it already has the build it wants, and a release
///   workflow checks that the asset it is about to publish is the tag it is
///   naming. That comparison is against `vMAJOR.MINOR.PATCH` with the `v`
///   removed — the string `RELEASING.md` calls the tag — so a decorated line
///   would oblige both of them to pick a field out of it, and a field position
///   is a format that drifts.
/// - `dz_publisher_build_info{version}` carries the same argument, so the
///   number a scrape reports and the number the binary answers with cannot
///   disagree. The commit and the toolchain on that gauge stay compile-time
///   environment reads, which is where a build stamps them.
#[must_use]
pub fn run_with_version(version: &str, registry: AdapterRegistry) -> ExitCode {
    // Once, for both answers: what stdout says and what the gauge carries are
    // the same string, so it is read and trimmed once rather than twice.
    let version = reported_version(version);
    let path = match invocation(std::env::args_os().skip(1)) {
        Ok(Invocation::Version) => {
            print!("{}", version_stdout(version));
            return ExitCode::SUCCESS;
        }
        // Asking what the accepted forms are is not a failure to name one, and
        // the answer goes where an answer goes.
        Ok(Invocation::Help) => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Ok(Invocation::Config(path)) => path,
        Err(error) => return report_refusal(&error),
    };
    match start(version, path, &registry) {
        Ok(exit) => {
            eprintln!("dz-publisher-runtime: exiting because of {exit}");
            match exit {
                // A signal is the operator asking, and an orderly answer to it
                // is a success. Everything else is this publisher reporting that
                // it could not go on, and a supervisor should see that.
                Exit::Signal => ExitCode::SUCCESS,
                Exit::IdleGuard | Exit::ConsistencyGuard(_) => ExitCode::FAILURE,
            }
        }
        Err(error) => report_refusal(&error),
    }
}

/// Print a refusal, with its causes, and fail.
fn report_refusal(error: &StartupError) -> ExitCode {
    eprintln!("dz-publisher-runtime: {error}");
    let mut next = std::error::Error::source(error);
    while let Some(cause) = next {
        eprintln!("  caused by: {cause}");
        next = cause.source();
    }
    ExitCode::FAILURE
}

/// What a build reports when it was handed no version.
///
/// A literal, and never an empty string — the rule the recorder's identity
/// module states about an unstamped commit, for the same reason: an empty line
/// on stdout reads as a flag that half works, while this one reads as an answer
/// and fails a comparison against any pin.
const UNKNOWN_VERSION: &str = "unknown";

/// The version this process reports, out of what the caller handed over.
///
/// Trimmed, because the string is written into stdout as given and surrounding
/// whitespace is a difference no pin carries. Empty is
/// [`UNKNOWN_VERSION`]: the argument is a caller's value, so a venue that
/// assembles it from an environment its build did not set passes nothing at
/// all, and nothing at all must not print as a blank line and reach the gauge
/// as a blank label.
///
/// **A value carrying a line break is [`UNKNOWN_VERSION`] for the same
/// reason.** [`run_with_version`] promises stdout exactly one line, and a
/// caller that assembled its argument out of a command's whole output hands
/// over something that would print as two — a first line a comparison passes
/// on and a second nobody pinned. One line that fails every comparison is an
/// answer; two lines are a contract broken for every consumer of the flag, and
/// a label a scrape splits on.
fn reported_version(version: &str) -> &str {
    let named = version.trim();
    if named.is_empty() || named.contains(['\n', '\r']) {
        UNKNOWN_VERSION
    } else {
        named
    }
}

/// Exactly what `--version` writes to stdout.
///
/// The version and a newline, and nothing else. A function rather than a
/// `println!` at the call site because the format *is* the contract here — a
/// consumer's whole use of this flag is comparing the output against a pinned
/// string — so it is a value a test reads back rather than a literal inside a
/// branch no test enters.
fn version_stdout(version: &str) -> String {
    format!("{version}\n")
}

/// What the command line asked for.
#[derive(Debug, PartialEq, Eq)]
enum Invocation {
    /// Report the version and exit. Publishes nothing, binds nothing, and reads
    /// no configuration file: it is asked of a binary that is not running.
    Version,
    /// Print [`USAGE`] and exit. Publishes nothing, for the same reason.
    Help,
    /// Load this configuration and publish.
    Config(PathBuf),
}

/// The option that names the configuration document.
///
/// A constant because the places that have to agree on it are no longer one:
/// the two spellings [`option_value`] reads, and the refusal that names the
/// option back to an operator.
const CONFIG_OPTION: &str = "--config";

/// The value `option` was given, in whichever spelling it was written.
///
/// `None` when this argument is not that option at all, which is what lets the
/// caller go on to the next thing an argument can be.
///
/// `--config /etc/dz/publisher.toml` takes the next argument; the `=`-joined
/// `--config=/etc/dz/publisher.toml` carries its own value, and carries it to
/// the end of the argument — nothing is split on a second `=`, so a path that
/// contains one arrives whole.
///
/// # Errors
///
/// [`StartupError::OptionNeedsValue`], naming the option, when the value is not
/// there: either the option ended the command line, or the `=` did. Both are an
/// operator who meant to name a document, and naming the option is the only
/// answer that says so — an empty path opened as a file would report the
/// failure one step away from the mistake.
fn option_value<I: Iterator<Item = OsString>>(
    arg: &OsString,
    option: &'static str,
    rest: &mut I,
) -> Result<Option<OsString>, StartupError> {
    let needs_value = || StartupError::OptionNeedsValue {
        option,
        usage: USAGE,
    };
    if arg.as_os_str() == OsStr::new(option) {
        return rest.next().ok_or_else(needs_value).map(Some);
    }
    // Bytes rather than a string, because a configuration path is a path and
    // not text: an argument this process cannot decode is still a file it can
    // open, and `to_string_lossy` would hand `Config::load` a name with
    // replacement characters in it.
    let bytes = arg.as_os_str().as_encoded_bytes();
    let joined = option.len() + 1;
    if bytes.len() < joined || !bytes.starts_with(option.as_bytes()) || bytes[option.len()] != b'='
    {
        return Ok(None);
    }
    let value = OsStr::from_bytes(&bytes[joined..]);
    if value.is_empty() {
        return Err(needs_value());
    }
    Ok(Some(value.to_os_string()))
}

/// Read the invocation out of the arguments after the program name.
///
/// **The arguments are read in order, and the first decisive one answers.** A
/// reader that took only the first would start a publisher for
/// `<publisher> --config publisher.toml --version` — binding transmitters and
/// putting datagrams on a group in answer to a question about a string — and
/// that ordering is the one a unit file writes, because the recorder beside
/// this takes it.
///
/// Decisive cuts both ways, and deliberately. `--version` and `--help` are
/// answered from the flag itself, so what follows one is not read:
/// `<publisher> --version --verison` prints the version, because a deployment
/// asking a binary what it is needs an answer and not a verdict on the rest of
/// the line — the same choice the recorder makes. A refusal reached first wins
/// for the same reason: `<publisher> --verison --version` reports the
/// misspelling, because by then the misspelling is what has been read.
///
/// **An option this reader does not know is refused by name.** Anything left to
/// fall through to the path branch becomes a filename, so a misspelled flag — or a
/// flag this reader has not been taught — fails as a configuration file that
/// could not be opened, named `--whatever-it-was`. That is a true statement
/// about a file nobody meant and it says nothing about the command line, which
/// is the one thing wrong with it.
///
/// **An option that takes a value is read in both spellings.**
/// `--config=/etc/dz/publisher.toml` names the same document as
/// `--config /etc/dz/publisher.toml`, and a unit file is as likely to carry
/// either: refusing the joined form would report a known option as one this
/// publisher does not know, which is the misdiagnosis-by-one-step the refusal
/// above exists to remove. Both spellings go through [`option_value`], so an
/// option added to this reader is taught both at once.
///
/// A leading `-` is what makes an argument an option here, so a real file is
/// still named bare: only a path that begins with a dash has to be written
/// `--config -weird-name` or `./-weird-name`. The value after `--config` is
/// taken as written, dash or no dash, because naming it after the option is
/// what says it is a path — and so is the value after `--config=`, up to the
/// end of the argument, so a path of its own containing an `=` survives.
fn invocation<I: IntoIterator<Item = OsString>>(args: I) -> Result<Invocation, StartupError> {
    let mut path: Option<PathBuf> = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        if arg == "--help" || arg == "-h" {
            return Ok(Invocation::Help);
        }
        if arg == "--version" || arg == "-V" {
            return Ok(Invocation::Version);
        }
        if let Some(named) = option_value(&arg, CONFIG_OPTION, &mut args)? {
            name_config_path(&mut path, named)?;
            continue;
        }
        if arg.as_os_str().as_encoded_bytes().starts_with(b"-") {
            return Err(StartupError::UnknownOption {
                option: arg.to_string_lossy().into_owned(),
                usage: USAGE,
            });
        }
        name_config_path(&mut path, arg)?;
    }
    path.map(Invocation::Config)
        .ok_or(StartupError::NoConfigPath { usage: USAGE })
}

/// Record the configuration file, refusing a second one.
///
/// A publisher reads one document, so two named on one command line is a
/// question about which — and dropping either of them answers it silently. A
/// unit file edited to point at a new document while the old argument stayed
/// behind is how both get named, and the reading that keeps running is the one
/// nobody meant to keep.
fn name_config_path(path: &mut Option<PathBuf>, named: OsString) -> Result<(), StartupError> {
    match path {
        Some(first) => Err(StartupError::TwoConfigPaths {
            first: first.display().to_string(),
            second: named.to_string_lossy().into_owned(),
            usage: USAGE,
        }),
        None => {
            *path = Some(PathBuf::from(named));
            Ok(())
        }
    }
}

/// Everything `run_with_version` does once the command line is understood, with
/// the failure typed.
fn start(version: &str, path: PathBuf, registry: &AdapterRegistry) -> Result<Exit, StartupError> {
    let config = Config::load(path)?;
    compose_and_run(version, registry, config)
}

fn compose_and_run(
    version: &str,
    registry: &AdapterRegistry,
    config: Config,
) -> Result<Exit, StartupError> {
    // Every enabled feed's identity is the same, which `Document::resolve`
    // has already checked: a `Source ID` is the publisher's registered
    // identity and the lowering takes it once.
    let identity = config
        .feeds
        .first()
        .ok_or(StartupError::NoEnabledFeed)?
        .clone();

    // The adapter first, because its declarations are what make two metric
    // label sets knowable at startup: the connection names, so that the
    // `connection_state == 0` alert can fire on a publisher whose upstream never
    // came up at all, and the upstream message types, so that no panel is blank
    // because a message has not arrived yet.
    // The feeds go with it, because whether this adapter can serve them is a
    // question only the adapter can answer: a depth feed obliges
    // `Adapter::snapshot`, and an adapter that holds no book has to be able to
    // refuse that at startup rather than publish deltas no subscriber can apply.
    let feed_specs = config.feed_specs();
    let cx = AdapterContext::new(
        &config.adapter,
        config.ingress_kind,
        &config.venue,
        &config.sources,
        &feed_specs,
    );
    let venue = registry.open(&cx)?;
    check_sources(&config, &venue)?;
    // **`[adapter.replay]` substitutes for the transport, not for the
    // adapter.** An offline run exercises this whole function — the config, the
    // registry, the venue's own adapter, the lowering, the sockets — with
    // recorded upstream bytes in place of a live venue. The adapter cannot tell
    // the difference, which is the property that makes the exercise worth
    // anything; the transport the venue built is never connected — it is held,
    // unused, for the length of the run — and a line says so rather than
    // leaving an operator to wonder why nothing connected.
    //
    // **One replaying input replaces every source**, named after the primary. A
    // fixture directory is one recording, so replaying it once per source would
    // publish every payload as many times as there are sources — and a race
    // between two copies of one recording is not a race. Replaying the primary
    // is the run the offline comparison is defined against.
    let mut inputs: Vec<Box<dyn Input>> = match &config.adapter.replay {
        replay if replay.enabled => {
            let path = replay
                .path
                .as_deref()
                .ok_or(StartupError::ReplayWithoutPath)?;
            let connection = primary_connection(&config, &venue);
            let replaying = crate::ReplayInput::open(connection, path)
                .map_err(|source| StartupError::Replay { source })?;
            eprintln!(
                "replaying {} payloads as `{connection}` from {}: {}",
                replaying.remaining(),
                path.display(),
                replaying.names().join(", ")
            );
            vec![Box::new(replaying)]
        }
        _ => venue.sources,
    };
    // Taken before the adapter is, because both are fields of the same value
    // and the adapter is about to be moved out of it.
    let venue_collectors = venue.collectors;
    let adapter = Arc::new(Mutex::new(venue.adapter));
    let message_types = {
        let held = adapter.lock().unwrap_or_else(|held| held.into_inner());
        held.message_types().to_vec()
    };
    // Every input's connection name, so that `ingress_connection_state` is
    // pre-created at 0 for each of them: a publisher whose second upstream never
    // came up is the case the alert exists for, and a series that appeared on
    // first success would not carry it.
    //
    // **The list this reads is the substituted one**, so a replay run
    // pre-creates one `connection` value and not one per declared `[[source]]`
    // — every family labelled by `connection` comes up as narrow as the run is.
    // That is the one consequence of the substitution above a venue is likely
    // to plan against without noticing, so `BRINGING-UP-A-FEED.md` states it
    // beside the offline proof.
    let connections: Vec<&'static str> = inputs
        .iter()
        .map(|input| input.connection().as_str())
        .collect();

    let metrics = publisher_registry(
        version,
        &PublisherMetricsConfig {
            venue: &config.venue,
            source_id: identity.source_id.get(),
            port_roles: &config.port_roles(),
            connections: &connections,
            channel_ids: &config.channel_ids(),
            ingress_message_types: &message_types,
        },
        venue_collectors,
    )?;

    let clock = SystemClock::new();

    // The era store, opened once: one file per feed, keyed on the feed's own
    // name, so a newly enabled feed advertises its first era rather than
    // inheriting one from a feed that has published for months.
    //
    // It lives in `[refdata] state_dir` because that is the only durable
    // directory the document names.
    let eras = EraStore::open(&config.refdata.state_dir)?;

    // One registry for every feed, and that is right rather than a
    // simplification - see `Publisher::new`. It is opened before the sockets so
    // that the single-writer guard refuses a second publisher on one state
    // directory before that publisher has bound anything.
    let schedule = CycleSchedule::new(
        identity.definition_cycle,
        MAX_DATAGRAM_SIZE as u16,
        MAX_DEFINITION_DATAGRAMS_PER_TICK,
    );
    // One entry per distinct shard, in the document's own order, so that the
    // index the registry addresses a published set by is the index the feed
    // pipelines carry. Two orders that agree by convention rather than by
    // construction is how a shard's definitions end up on another shard's port.
    //
    // The `Channel ID` is the first block carrying that shard, and it is the one
    // a manifest states when nothing overwrites it. A shard carrying two
    // specifications is two channel instances sharing one published set; the
    // datagram builder stamps the header at push, so the copy in the message
    // body cannot disagree with the port it left by.
    let shards: Vec<ShardConfig> = config
        .shards()
        .into_iter()
        .map(|shard| ShardConfig {
            channel_id: config
                .feeds
                .iter()
                .find(|feed| feed.shard == shard)
                .map_or(identity.channel_id, |feed| feed.channel_id),
            name: shard.as_str().to_owned(),
        })
        .collect();
    let refdata = Registry::open(
        RegistryConfig {
            source_id: identity.source_id,
            shards,
            selection: config.refdata.selection,
            schedule,
        },
        FileStore::new(&config.refdata.state_dir),
        clock.clone(),
    )?;

    // **The composition is a function now, and that is what makes it
    // testable.** Inline here, nothing but a real socket could reach it: the
    // whole suite passed with the shard order reversed, which publishes each
    // shard's instruments under another channel instance's sequence series and
    // is undetectable from a subscriber. See `compose_feeds` and `PortOpener`.
    let ports = KernelPorts::new(&config, &metrics);
    let feeds = compose_feeds(&config.shards(), &config.feeds, &eras, &metrics, &ports)?;

    let publisher = RefCell::new(Publisher::new(
        Arc::clone(&metrics),
        refdata,
        clock.clone(),
        identity.source_id,
        feeds,
        identity.idle_guard,
    ));
    // A depth feed with no cadence emits recovery snapshots and no others,
    // which is a feed a subscriber cannot join mid-session. It is a legitimate
    // configuration and it is not a default anybody should get by accident, so
    // it is stated at startup rather than left to be inferred from silence.
    match publisher.borrow().snapshot_cycle() {
        Some(cycle) => eprintln!("snapshot rotation: one pass over the published set every {cycle:?}"),
        None if config.feeds.iter().any(|feed| feed.snapshot_port.is_some()) => eprintln!(
            "dz-publisher-runtime: a feed carries a snapshot port and no `[[feed]] snapshot_cycle`: \
             only recovery snapshots will be sent, so a subscriber that joins mid-session cannot \
             bootstrap its book"
        ),
        None => {}
    }

    // The same argument one section over. A `reconnect_backoff_max` equal to
    // `reconnect_backoff_initial` is a fixed reconnect delay: the jitter's
    // window is a single point, so every connection this publisher holds
    // against one address retries at the same instants - and the delays are
    // inside their configured pair either way, so no series and no error can
    // show it. Legitimate, not a default anybody should get by accident, and
    // therefore stated at startup. The line is composed and asserted in
    // `dz-ingress-core`, where the two keys are spelled; see
    // `BackoffPolicy::lockstep_line`.
    if let Some(line) = config.ingress.backoff.lockstep_line() {
        eprintln!("dz-publisher-runtime: {line}");
    }

    let server = if config.metrics.enabled {
        Some(
            dz_publisher_metrics::serve(Arc::clone(&metrics), config.metrics.listen_addr).map_err(
                |source| StartupError::Metrics {
                    addr: config.metrics.listen_addr,
                    source,
                },
            )?,
        )
    } else {
        None
    };

    let observer = MetricsObserver::new(Arc::clone(&metrics));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .enable_io()
        .build()
        .map_err(|source| StartupError::Runtime { source })?;

    // **Admit before anything can arrive.** The tick loop polls listings on its
    // own cadence, and it starts alongside the driver — so on a live feed the
    // first payloads of every restart reach an adapter holding no handles and
    // are dropped as events for instruments nobody admitted. Continuous traffic
    // hides that; a finite replay does not, which is how it was found. One poll
    // here costs a startup that is already opening sockets, and it means the
    // first payload is the first payload rather than the first one after a
    // tick.
    {
        let mut held = adapter.lock().unwrap_or_else(|held| held.into_inner());
        let mut publisher = publisher.borrow_mut();
        // Which venue clock this adapter's timestamps carry, read once: it is a
        // property of the adapter and not of a message.
        publisher.declare_venue_timestamps(&**held);
        publisher.poll_listings(&mut **held);
    }

    let exit = runtime.block_on(async {
        // One driver per source, which is the shape `Driver` was written for:
        // the connection, the backoff and the rate limit are per upstream, and
        // a second source being rate limited must not pace the first. The
        // clock, the observer and the adapter are shared, and the adapter
        // through the same lock a single-source publisher already used —
        // uncontended, because every driver and the tick are futures in one
        // task on a current-thread runtime and none of them locks across an
        // await.
        let mut shared_adapters: Vec<SharedAdapter> = inputs
            .iter()
            .map(|_| SharedAdapter::new(Arc::clone(&adapter), message_types.clone()))
            .collect();
        let mut sinks: Vec<SharedSink<'_, _, _>> =
            inputs.iter().map(|_| SharedSink(&publisher)).collect();
        // Whether a fatal error from this connection ends the process.
        //
        // This connection's role decides it, and that is the whole of what a
        // role decides about a live run — `primary_connection` reads it too,
        // but only on the replay path above, and the credential rule reads it
        // at load, where nothing is running yet.
        // `Driver::run` returns only on `IngressError::Fatal`, which any
        // non-retryable connect, send or receive operation can report: a
        // per-connection configuration fault found at connect most of all, an
        // invalid endpoint, a missing credential path or an unsupported scheme,
        // and equally a message the transport cannot carry at all. A
        // `comparison` connection answers `false`, because everything it
        // carries arrives on the primary too — so a mistyped URL on a
        // connection that by design must not reach the wire cannot take the
        // healthy primary down, and cannot keep it down across restarts with a
        // fault that lives in the file. A `primary` and an `upstream-partition`
        // answer `true`: the instruments an upstream partition carries arrive
        // on no other connection, so carrying on without it would serve that
        // subset stale.
        //
        // A document that declares no upstream connections has one implicit
        // connection and no role to read, so every input ends the process and
        // the behaviour is exactly what a single-connection publisher has
        // always had. An input the document does not name cannot happen —
        // `check_sources` holds the two sets equal before this — and if it ever
        // did, ending the process is the answer that does not silently keep a
        // publisher running past a fault.
        let mut drivers: Vec<(&'static str, bool, Driver<'_>)> = inputs
            .iter_mut()
            .zip(shared_adapters.iter_mut())
            .map(|(input, shared)| {
                let connection = input.connection();
                (
                    connection.as_str(),
                    fatal_ends_the_process(&config.sources, connection),
                    Driver::new(&mut **input, shared, &clock, &observer, config.ingress),
                )
            })
            .collect();
        // Not `dz_ingress_core::BoxFuture`, which is `Send`: none of these are.
        // They reach the publisher through the `RefCell` this whole module is
        // built around, which is sound precisely because everything stays in
        // one task on a current-thread runtime.
        type Run<'a> =
            std::pin::Pin<Box<dyn std::future::Future<Output = (&'static str, IngressError)> + 'a>>;
        let mut runs: Vec<(bool, Run<'_>)> = drivers
            .iter_mut()
            .zip(sinks.iter_mut())
            .map(|((name, ends_the_process, driver), sink)| {
                let name = *name;
                (
                    *ends_the_process,
                    Box::pin(async move { (name, driver.run(sink).await) }) as Run<'_>,
                )
            })
            .collect();

        // The first driver **whose failure is fatal** to give up ends the
        // process, and it is named. There is no `select!` over a count decided
        // at runtime, and no task per connection either: the composed publisher
        // is deliberately not `Send`, so polling them in turn from one future
        // is what keeps every borrow in this task. Each returns `Pending`
        // having registered its own waker, so this parks rather than spins.
        //
        // A driver whose failure is not fatal — a `comparison` connection's —
        // is **dropped from the set and named**, and the publisher carries on.
        // `Driver::run` returns only on a fatal error, so such a driver is
        // permanently done and polling it again would panic; leaving it out is
        // also what leaves its `connection_state` at 0, which is the alert for
        // a connection that never came up. What the published set depends on is
        // the primary and every upstream partition, and those are what the wire
        // feels the failure of.
        let first_to_give_up = std::future::poll_fn(|cx| {
            poll_first_fatal_run_to_give_up(&mut runs, cx, |connection, error| {
                // Named here rather than counted into a new family: the series
                // that says this happened already exists and is already
                // alerted on, and what a log adds is the reason.
                eprintln!(
                    "`{connection}` gave up. It is neither the primary nor an upstream \
                     partition, so this publisher carries on without it. Nothing retries it: \
                     its connection_state stays at 0 until this process is restarted, which is \
                     what retries it — and several causes of a fatal error are only fatal for \
                     one attempt, a credential path that does not exist yet most of all. \
                     {error}"
                );
            })
        });

        tokio::select! {
            (connection, error) = first_to_give_up => Exit::ConsistencyGuard(
                Inconsistency::UpstreamUnusable {
                    detail: format!("`{connection}`: {error}"),
                },
            ),
            exit = tick_loop(&publisher, &adapter, &clock) => exit,
            () = signalled() => Exit::Signal,
        }
    });

    // The drivers are dropped, so nothing more arrives from upstream: the first
    // step of the teardown is already true when `shut_down` records it.
    let teardown = publisher.borrow_mut().shut_down(exit);
    report(&publisher.borrow(), &observer);
    // Rendered once more with the exit recorded, so a scrape that lands after
    // the process is gone is not the first one that would have carried it.
    drop(server);
    Ok(teardown.exit().clone())
}

/// Hold the venue's transports to the document's sources.
///
/// # Why this is checked rather than trusted
///
/// The document says which sources exist and the venue's own `main` builds them,
/// so the two can disagree — and every way they can disagree is silent. A venue
/// that skipped a source publishes from fewer upstreams than the file says, with
/// no series for the one that is missing, which reads exactly like an upstream
/// that is down. A venue that built a name nobody configured moves traffic under
/// a `connection` label the registry never declared, so it is counted under no
/// series at all.
///
/// So the names have to match as a set, and a mismatch names both sides. This is
/// the same check `[adapter] kind` gets, for the same reason: *what is in this
/// binary* is the question an operator cannot answer from the file.
///
/// # Errors
///
/// [`StartupError::NoVenueSource`], [`StartupError::SourcesUndeclared`] and
/// [`StartupError::SourcesDisagree`], which are the three ways the two sides can
/// fail to line up.
pub fn check_sources(config: &Config, venue: &crate::Venue) -> Result<(), StartupError> {
    if venue.sources.is_empty() {
        return Err(StartupError::NoVenueSource);
    }
    if config.sources.is_empty() {
        // No `[[source]]` block: one implicit source, named by the transport the
        // venue built, which is what every document said before the array
        // existed. Several transports without a document that declares them is
        // still a mismatch — nothing would say what the second one is.
        if venue.sources.len() > 1 {
            return Err(StartupError::SourcesUndeclared {
                built: venue.sources.len(),
            });
        }
        return Ok(());
    }

    let mut declared: Vec<&str> = config
        .sources
        .iter()
        .map(|source| source.connection.as_str())
        .collect();
    let mut built: Vec<&str> = venue
        .sources
        .iter()
        .map(|input| input.connection().as_str())
        .collect();
    // Compared as sets: the document's order is a reading order and the venue's
    // is a construction order, and neither is a promise to the other.
    declared.sort_unstable();
    built.sort_unstable();
    if declared != built {
        return Err(StartupError::SourcesDisagree {
            declared: declared.join(", "),
            built: built.join(", "),
        });
    }
    Ok(())
}

/// The connection a replay run publishes under.
///
/// The primary's, when the document declares one, because that is the source the
/// offline comparison is defined against; otherwise the one transport the venue
/// built, which is what a single-source publisher has always used.
///
/// `role == Primary` exactly, and not "whatever the published set depends on":
/// an `upstream-partition` is depended on and is not the primary, so a
/// partitioned upstream replays under the primary's connection like any other
/// publisher. This is the one place outside `fatal_ends_the_process` where a
/// role changes what the runtime does, and what it changes is the `connection`
/// label an offline run carries.
fn primary_connection(config: &Config, venue: &crate::Venue) -> ConnectionId {
    config
        .sources
        .iter()
        .find(|source| source.role == SourceRole::Primary)
        .map(|source| source.connection)
        .unwrap_or_else(|| venue.sources[0].connection())
}

/// What opens one feed's send paths.
///
/// # Why this is a trait, and why the route was not enough
///
/// `RouteLookup` already puts the routing table behind a trait, and its own doc
/// comment says why: "a test that needs a route to a multicast group is a test
/// that does not run in CI". That is true and it is not the seam that was
/// missing. `MulticastTransmitter::open` binds a socket and connects it, so a
/// composition holding a `RouteLookup` still needs a network to compose.
///
/// **Without this seam nothing but a real socket can reach the composition**,
/// and a permuted shard order is undetectable from anywhere else. The
/// definition path is keyed on a shard's *name* — `ShardFeeds` derives it from
/// one of its own send paths — so every reference-data port still carries
/// exactly its own shard's definitions. The event path is keyed on the
/// *index*, so a quote reaches another shard's pipeline, whose lowering does
/// not hold the instrument, and is dropped before any wire: every channel
/// loses its own market data and no channel gains any. The end-to-end harness
/// composes its own `Feeds`, so it cannot see the difference either.
pub trait PortOpener {
    /// The send paths for one `[[feed]]` block.
    ///
    /// # Errors
    ///
    /// [`StartupError`] for anything that stops this feed from being composed:
    /// a route that does not resolve, an address outside the declared prefix, a
    /// socket that cannot be opened, a fan-out path that is not a socket.
    fn open(&self, feed: &Feed) -> Result<Ports, StartupError>;
}

/// The real one: real sockets, over the routing table the send path itself asks.
pub struct KernelPorts<'a> {
    config: &'a Config,
    metrics: &'a Arc<PublisherMetrics>,
    route: KernelRoute,
}

impl<'a> KernelPorts<'a> {
    #[must_use]
    pub fn new(config: &'a Config, metrics: &'a Arc<PublisherMetrics>) -> Self {
        Self {
            config,
            metrics,
            route: KernelRoute,
        }
    }
}

impl PortOpener for KernelPorts<'_> {
    fn open(&self, feed: &Feed) -> Result<Ports, StartupError> {
        open_ports(feed, self.config, self.metrics, &self.route)
    }
}

/// One shard's send paths per shard, in the order the shard list states them.
///
/// **The order is the whole of it.** `Feeds` is indexed by shard, and the index
/// a routing decision uses is the one `Registry::shard_of` returns — an index
/// into the registry's shard list, built from the same `Config::shards()`.
/// Iterating that one list here is what keeps the two in step, and a test that
/// asserts this function's output order is the only thing that says so.
///
/// Shard-outer and block-inner, so a document that interleaves its blocks
/// cannot separate a shard's two specifications: they are built together and
/// held together in one `ShardFeeds`.
///
/// # Errors
///
/// [`StartupError::ShardWithNoFeed`] for a shard the feed list does not
/// mention, which no document and no resolved `Config` can produce —
/// `Config::shards()` is the distinct shards *of the enabled blocks* — and
/// which is therefore reachable only from here. That is the reason the variant
/// exists and the reason this function takes the two lists separately rather
/// than a `Config`: an invariant no document can violate is one a later
/// refactor can, and skipping the shard would shift every later shard's index
/// one off the registry's.
///
/// Everything else the era store or the port opener refuses.
pub fn compose_feeds(
    shards: &[ShardName],
    feeds: &[Feed],
    eras: &EraStore,
    metrics: &Arc<PublisherMetrics>,
    ports: &dyn PortOpener,
) -> Result<Feeds, StartupError> {
    let mut composed = Feeds::default();
    for shard in shards {
        let mut top_of_book = None;
        let mut market_by_price = None;
        for feed in feeds.iter().filter(|feed| feed.shard == *shard) {
            let opened = ports.open(feed)?;
            // The match is total over a set that is not `#[non_exhaustive]`, so
            // a feed specification added to `FeedSpec` breaks the build here -
            // which is the point. A value a configuration can name that nothing
            // composes is a value that resolves to nothing at startup.
            match feed.spec {
                FeedSpec::TopOfBook => {
                    top_of_book = Some(FeedPipeline::new(
                        feed,
                        Arc::clone(metrics),
                        eras.begin_era::<TopOfBook>(feed.shard.era_shard())?,
                        opened,
                    ));
                }
                FeedSpec::MarketByPrice => {
                    market_by_price = Some(FeedPipeline::new(
                        feed,
                        Arc::clone(metrics),
                        eras.begin_era::<MarketByPrice>(feed.shard.era_shard())?,
                        opened,
                    ));
                }
            }
        }
        let Some(shard_feeds) = ShardFeeds::new(top_of_book, market_by_price) else {
            // Refused rather than skipped, because skipping would shift every
            // later shard's index one off the registry's and publish a shard's
            // instruments under another channel instance's sequence series.
            return Err(StartupError::ShardWithNoFeed {
                shard: shard.as_str().to_owned(),
            });
        };
        composed.push(shard_feeds);
    }
    Ok(composed)
}

/// Open one feed's transmitters and wrap each in its own fan-out.
///
/// # Every port role is `FailureScope::Process`, and two of the three are the
/// decision the design left open
///
/// A dead **mktdata** socket means this publisher is not publishing, which is a
/// reason to end the process and let a supervisor restart it where the route
/// works. That one the design states.
///
/// A dead **refdata** socket leaves existing subscribers served and makes the
/// feed unjoinable: every `Instrument ID` on the wire resolves to a definition
/// that is no longer being retransmitted, and the reference-data cycle is what a
/// subscriber's whole view of identity is built on. Degrading silently into a
/// feed nobody new can join is worse than a restart.
///
/// A dead **snapshot** socket is the same argument for a depth feed and slightly
/// stronger: a subscriber that lost a datagram cannot rebuild its book without
/// one, so a depth feed with no snapshot port is a feed whose subscribers
/// diverge one gap at a time and never recover. That is exactly why
/// `snapshot_port` is required for a depth feed rather than optional.
fn open_ports(
    feed: &Feed,
    config: &Config,
    metrics: &Arc<PublisherMetrics>,
    route: &KernelRoute,
) -> Result<Ports, StartupError> {
    let open = |name: &'static str, port_role: PortRole, dst_port: u16| {
        let destination = SocketAddrV4::new(feed.group, dst_port);
        let transmitter = MulticastTransmitter::open(
            name,
            &config.egress,
            destination,
            port_role,
            FailureScope::Process,
            route,
        )?;
        let endpoint = transmitter.endpoint();
        let mut sink = Tee::new(port_role, Arc::clone(metrics));
        sink.add(Box::new(transmitter));
        // `[adapter.tee]` adds a second member to this fan-out, and to this
        // rather than as a second transmitter: it darkens nothing when it
        // fails and must never be able to end a send, which is exactly what
        // `Tee` guarantees a member and what `FailureScope::Channel` declares.
        //
        // **One socket per feed and port role**, at `path` suffixed with the
        // feed's own `spec` token and the role's. A Unix datagram carries
        // neither a destination port nor a group, and the diff this stream
        // exists for is keyed on both — so a recorder handed two roles on one
        // socket, or two feeds' copies of one role on one socket, could not
        // attribute a datagram without decoding it, and decoding is the one
        // thing a record path does not do.
        //
        // **The feed is in the name because this function runs once per feed.**
        // `[[feed]]` is an array and a publisher emitting both feeds is the
        // ordinary case; a name keyed on the role alone is correct only for the
        // publisher that happens to emit one. The shape mirrors the recorder's
        // own configuration, which keys its ports per feed. See
        // `TeeConfig::destination`.
        if config.adapter.tee.enabled {
            let destination = config
                .adapter
                .tee
                .destination(feed.spec, &feed.shard, port_role)?;
            eprintln!(
                "fanning out {} {} datagrams to {}",
                feed.spec.as_str(),
                port_role.as_str(),
                destination.display()
            );
            sink.add(Box::new(
                ReferenceStream::open(name, &destination).map_err(|source| StartupError::Tee {
                    path: destination.clone(),
                    source,
                })?,
            ));
        }
        Ok::<Port, StartupError>(Port { endpoint, sink })
    };

    Ok(Ports {
        mktdata: open("mktdata", PortRole::Mktdata, feed.mktdata_port)?,
        refdata: open("refdata", PortRole::Refdata, feed.refdata_port)?,
        snapshot: match feed.snapshot_port {
            Some(dst_port) => Some(open("snapshot", PortRole::Snapshot, dst_port)?),
            None => None,
        },
    })
}

/// The line an offer on an unknown shard name earns.
///
/// **Both halves, because either alone is unactionable.** The name the venue
/// asked for says what its adapter believes; the names this document configures
/// say what the process has. A misspelling is only visible as the pair, and an
/// operator handed one of them has to go and find the other before the line
/// means anything.
///
/// Separate from its call site so it can be asserted directly, which is the
/// only part of this path a test can reach: the call site is inside the tick
/// loop, and nothing in the suite runs that.
fn unknown_shard_line(offered: &str, configured: &[String]) -> String {
    let names = if configured.is_empty() {
        "none".to_owned()
    } else {
        configured
            .iter()
            .map(|name| format!("`{name}`"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "dz-publisher-runtime: the venue offered instruments on `{offered}`, which is not a shard \
         this publisher is configured with. They are declined and reach no channel. Configured: \
         {names}. Said once for this name however many instruments were offered under it."
    )
}

/// The numbers no series carries, on the way out.
///
/// Seven of them, each named where it is documented: lowering refusals by
/// reason, snapshots asked for and not sent, events this build had no feed to
/// carry, adapter failures the closed family set has nowhere for, fan-out
/// members that are no longer being fed, and the two shard refusals. A log line
/// is not a substitute for a series and is not offered as one; it is what a
/// closed metric set leaves.
///
/// The two shard refusals are the newest and the reason they are here is worth
/// stating. `Counts` maps them to no family deliberately — the normative set is
/// closed — and the gauge it points at instead,
/// `refdata_instruments_current{channel_id}` at 0, only shows a venue that
/// misnames *every* offer for a shard. One that misnames some of them leaves
/// the gauge non-zero and those instruments unpublished. So these two numbers
/// and the tick loop's own lines are the whole of the signal, and a number that
/// only exists in a log has to actually be printed.
fn report<S: StateStore, K: Clock + Clone>(
    publisher: &Publisher<S, K>,
    observer: &MetricsObserver,
) {
    let refusals = publisher.refusals();
    if refusals.total() > 0 {
        let detail: Vec<String> = refusals
            .by_reason()
            .iter()
            .filter(|(_, count)| *count > 0)
            .map(|(reason, count)| format!("{reason}={count}"))
            .collect();
        eprintln!(
            "dz-publisher-runtime: {} lowering refusals ({})",
            refusals.total(),
            detail.join(" ")
        );
    }
    // Both counts, whenever either moved. A depth feed whose books never
    // bootstrap is the case this exists for, and it is invisible everywhere
    // else: the datagram counters keep moving, the sequence series stays dense,
    // and the aggregate snapshot rate looks normal because the instruments that
    // *are* ready are still being served.
    let snapshots = publisher.snapshot_refusals();
    if snapshots.total() > 0 {
        eprintln!(
            "dz-publisher-runtime: {} snapshots were not sent ({} refused, {} on a book that was \
             not ready)",
            snapshots.total(),
            snapshots.refused,
            snapshots.not_ready
        );
    }
    for dropped in publisher.dropped_sinks() {
        eprintln!("dz-publisher-runtime: was no longer sending to {dropped}");
    }
    if publisher.unroutable() > 0 {
        eprintln!(
            "dz-publisher-runtime: {} events had no feed to carry them",
            publisher.unroutable()
        );
    }
    if observer.adapter_errors() > 0 {
        eprintln!(
            "dz-publisher-runtime: {} adapter failures",
            observer.adapter_errors()
        );
    }
    if publisher.snapshot_schedule_overruns() > 0 {
        eprintln!(
            "dz-publisher-runtime: on {} ticks the configured `[[feed]] snapshot_cycle` values \
             together asked for more snapshots than one process can send, so every channel \
             lapped more slowly than its own key states. One process serves one periodic \
             snapshot per {TICK:?}, and that budget is shared across every shard",
            publisher.snapshot_schedule_overruns()
        );
    }
    let counts = publisher.refdata().counts();
    if counts.declined_unknown_shard > 0 {
        eprintln!(
            "dz-publisher-runtime: {} listings were declined naming a shard this publisher has \
             no channel for; the names are in the lines written when they were first offered",
            counts.declined_unknown_shard
        );
    }
    if counts.declined_shard_restated > 0 {
        eprintln!(
            "dz-publisher-runtime: {} re-offers named a different shard for an instrument \
             already published, which stayed on the shard it was admitted to",
            counts.declined_shard_restated
        );
    }
}

/// Whether a repetition of the same failure is worth another line.
///
/// **The first one, then one per decade: 1, 10, 100, 1,000.** The tick body runs
/// every 10ms, so a permanent refusal printed on each of them is up to a hundred
/// lines a second — which does not inform an operator, it teaches them to turn
/// the log off, and it buries every other line in the process. A decade
/// schedule states the first occurrence promptly, keeps saying so while the
/// order of magnitude is still changing, and costs four lines an hour where the
/// unfiltered version costs three hundred thousand.
///
/// The count itself is not sampled — see
/// [`SnapshotRefusals`](crate::SnapshotRefusals) — so what a line drops is a
/// repetition and never the evidence.
const fn worth_a_line(count: u64) -> bool {
    match count {
        0 => false,
        1 => true,
        // `is_power_of_ten` does not exist; a divisor walk on a `u64` this
        // small is a handful of divisions on a path that only runs when
        // something has already gone wrong.
        mut n => {
            while n % 10 == 0 {
                n /= 10;
            }
            n == 1
        }
    }
}

/// Poll listings and tick until a guard fires.
async fn tick_loop<S: StateStore, K: Clock + Clone>(
    publisher: &RefCell<Publisher<S, K>>,
    adapter: &Arc<Mutex<Box<dyn Adapter>>>,
    clock: &K,
) -> Exit {
    // A fan-out member is named the first time it is seen to be gone and never
    // again: the drop is permanent by construction, so a line per tick would be
    // a line per tick forever. The exit report names the whole set again.
    let mut named_dropped: Vec<String> = Vec::new();
    // A member whose route is down is named when it goes down and again when
    // it comes back, and not in between: it refuses every datagram while it is
    // down, so anything per datagram or per tick is noise.
    let mut named_path_down: Vec<String> = Vec::new();
    loop {
        clock.sleep(TICK).await;
        // One synchronous critical section, and nothing awaited inside it. See
        // the module note: a borrow held across an await here is a panic rather
        // than a compile error.
        let exit = {
            let mut publisher = publisher.borrow_mut();
            {
                let mut held = adapter.lock().unwrap_or_else(|held| held.into_inner());
                publisher.poll_listings(&mut **held);
                // A shard name the venue offered that this document has no
                // channel for. Named here rather than left to the exit report,
                // because the instruments under it are being declined *now* and
                // the only series that could show it is a gauge at 0 — which
                // says a channel is empty and cannot say what was asked for.
                // The registry hands each distinct name back once and never
                // again, so this is a line per name and not a line per poll,
                // which matters because an adapter may re-offer its whole set
                // every second.
                let offered = publisher.take_unknown_shards();
                if !offered.is_empty() {
                    let configured: Vec<String> = (0..publisher.feeds().shard_count())
                        .filter_map(|index| publisher.feeds().shard_name(index))
                        .map(str::to_owned)
                        .collect();
                    for name in offered {
                        eprintln!("{}", unknown_shard_line(&name, &configured));
                    }
                }
                // The recovery snapshots an `InstrumentReset` obliged. Drained
                // here rather than inside the adapter's own callback because
                // capturing a book is a walk of it, and because a snapshot has
                // to be captured *after* the reset that announced it — a
                // subscriber discards any snapshot for the instrument with an
                // older anchor.
                //
                // A capture that refuses is not retried: the reset already
                // reached the wire, so the instrument is waiting, and the next
                // consistency check will announce it again with a fresh anchor.
                // Retrying here would hold a tick open on a book that is not
                // ready.
                //
                // Every refusal is counted before it is printed, and printed on
                // the decade schedule `worth_a_line` states — including
                // `NotReady`, which is filtered here as it is below because a
                // book that has not bootstrapped is the expected refusal and the
                // count is where it is recorded.
                for (instrument, anchor) in publisher.owed_snapshots() {
                    if let Err(error) = publisher.snapshot_anchored_at(&**held, instrument, anchor)
                    {
                        report_snapshot_refusal("a recovery", &error, &publisher);
                    }
                }
                // The periodic rotation, which is what a subscriber joining
                // mid-session bootstraps from. One instrument per tick, so this
                // is O(1) in the published set; see `crate::rotation`.
                //
                // A refusal is reported and the rotation has already stepped
                // past the instrument, so a book that has not bootstrapped
                // costs one slot of one lap rather than the rotation.
                if let Some(Err(error)) = publisher.periodic_snapshot(&**held) {
                    report_snapshot_refusal("a periodic", &error, &publisher);
                }
            }
            // A fan-out member that has been dropped is silent by design — the
            // send that lost it returned `Ok`, because the alternative is one
            // auxiliary consumer's socket deciding what happens to a
            // `Sequence Number`. Read between ticks and named once, which is
            // where that silence ends.
            for dropped in publisher.dropped_sinks() {
                let name = dropped.to_string();
                if !named_dropped.contains(&name) {
                    eprintln!("dz-publisher-runtime: no longer sending to {name}");
                    named_dropped.push(name);
                }
            }
            let path_down: Vec<String> = publisher
                .path_down_sinks()
                .iter()
                .map(ToString::to_string)
                .collect();
            for name in &path_down {
                if !named_path_down.contains(name) {
                    eprintln!(
                        "dz-publisher-runtime: the route is down for {name}; holding its socket \
                         for up to {:?} for the route to return",
                        dz_publisher_egress::MAX_PATH_DOWN
                    );
                }
            }
            // A member that left the set because the transmitter gave up on it
            // is dropped, not back; the line above already named it.
            let given_up: Vec<String> = publisher
                .dropped_sinks()
                .iter()
                .map(|d| {
                    PathDownSink {
                        spec: d.spec,
                        port_role: d.port_role,
                        name: d.name,
                    }
                    .to_string()
                })
                .collect();
            for name in &named_path_down {
                if !path_down.contains(name) && !given_up.contains(name) {
                    eprintln!("dz-publisher-runtime: the route is back for {name}");
                }
            }
            named_path_down = path_down;
            let exit = publisher.tick();
            // The tick that just ran counted whether the configured cycles can
            // be met. On the decade schedule, because a document that asks for
            // more than the process can send asks for it on every tick
            // thereafter and one line per tick is a hundred a second. The exit
            // report names the total.
            if worth_a_line(publisher.snapshot_schedule_overruns()) {
                eprintln!(
                    "dz-publisher-runtime: the configured snapshot cycles want more snapshots \
                     than one process can send ({} ticks so far), so every channel is lapping \
                     more slowly than its `[[feed]] snapshot_cycle` states",
                    publisher.snapshot_schedule_overruns()
                );
            }
            exit
        };
        if let Some(exit) = exit {
            return exit;
        }
    }
}

/// Print a snapshot refusal, on the schedule [`worth_a_line`] states.
///
/// The count comes from the publisher rather than from a local, so that the
/// number in the line is the same number the exit report prints and there is
/// one place a refusal is tallied.
fn report_snapshot_refusal<S: StateStore, K: Clock + Clone>(
    which: &str,
    error: &SnapshotError,
    publisher: &Publisher<S, K>,
) {
    let counts = publisher.snapshot_refusals();
    // `NotReady` is the expected refusal — the rotation has stepped past the
    // instrument and comes back on the next lap — so it is worded as a book
    // that is not ready rather than as a failure. It is no longer discarded:
    // *never ready* and *not ready yet* read identically in one line, and the
    // count is what separates them.
    if matches!(error, SnapshotError::Adapter(AdapterError::NotReady { .. })) {
        if worth_a_line(counts.not_ready) {
            eprintln!(
                "dz-publisher-runtime: {which} snapshot found a book that is not ready \
                 ({} so far): {error}",
                counts.not_ready
            );
        }
    } else if worth_a_line(counts.refused) {
        eprintln!(
            "dz-publisher-runtime: {which} snapshot was refused ({} so far): {error}",
            counts.refused
        );
    }
}

/// Wait for `SIGTERM` or `SIGINT`.
#[cfg(unix)]
async fn signalled() {
    use tokio::signal::unix::{signal, SignalKind};
    // A handler that cannot be installed is not a reason to refuse to publish:
    // the process is still killable, and the cost is an abrupt end rather than
    // an `EndOfSession`. Reported and then waited on forever, so the select
    // branch simply never fires.
    let install = |kind: SignalKind, name: &str| match signal(kind) {
        Ok(stream) => Some(stream),
        Err(error) => {
            eprintln!("dz-publisher-runtime: no {name} handler: {error}");
            None
        }
    };
    let mut term = install(SignalKind::terminate(), "SIGTERM");
    let mut interrupt = install(SignalKind::interrupt(), "SIGINT");
    match (term.as_mut(), interrupt.as_mut()) {
        (Some(term), Some(interrupt)) => {
            tokio::select! {
                _ = term.recv() => {},
                _ = interrupt.recv() => {},
            }
        }
        (Some(one), None) | (None, Some(one)) => {
            one.recv().await;
        }
        (None, None) => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn signalled() {
    // Nothing to install. The publisher runs until a guard fires.
    std::future::pending().await
}

/// The event sink the driver writes into, reaching the publisher through the
/// cell they share.
///
/// Nothing here awaits, which is what makes the borrow safe; see the module
/// note.
struct SharedSink<'a, S: StateStore, K: Clock + Clone>(&'a RefCell<Publisher<S, K>>);

/// **Every method, and that is the point of writing them all out.** A wrapper
/// that forwards some of a trait and defaults the rest compiles, runs, and
/// silently drops whatever it forgot — the driver's own wrapper did exactly that
/// with `desynchronised` until it was noticed, which meant an adapter could say
/// its book had diverged and nothing downstream would hear it. So each method
/// is here explicitly rather than inherited, and a method added to `EventSink`
/// should be added here in the same change.
impl<S: StateStore, K: Clock + Clone> EventSink for SharedSink<'_, S, K> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.0.borrow_mut().upstream_message(message_type);
    }

    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.0.borrow_mut().payload_scope(recv_ts_ns);
    }

    fn drained(&mut self) {
        self.0.borrow_mut().drained();
    }

    fn event(&mut self, event: Event<'_>) {
        self.0.borrow_mut().event(event);
    }

    fn desynchronised(&mut self, instrument: dz_adapter_core::InstrumentRef, reason: Desync) {
        self.0.borrow_mut().desynchronised(instrument, reason);
    }
}

/// The adapter, reachable from both the driver and the tick.
///
/// # Why this exists
///
/// [`Driver`] takes `&mut dyn Adapter` and holds it for as long as it runs,
/// which is forever. The runtime also has to call
/// [`Adapter::poll_listings`] on a cadence and
/// [`Adapter::snapshot`] on demand, and those are the runtime's precisely
/// because the cadence and the framing are what a subscriber's recovery depends
/// on. One `&mut` cannot serve both, so the adapter is shared and the driver is
/// handed a delegate.
///
/// `Mutex` and not `RefCell` because [`Adapter`] is `Send` and a delegate that
/// was not could not be one. It is uncontended: the driver and the tick are two
/// futures in one task, and neither locks across an await.
///
/// `message_types` is copied out at construction rather than delegated, and it
/// has to be: the method returns a borrow, and there is no way to lend one out
/// of a lock. Copying is correct rather than a workaround — the boundary
/// declares the set up front, at startup, so that every series exists before a
/// message arrives.
struct SharedAdapter {
    inner: Arc<Mutex<Box<dyn Adapter>>>,
    message_types: Vec<&'static str>,
}

impl SharedAdapter {
    fn new(inner: Arc<Mutex<Box<dyn Adapter>>>, message_types: Vec<&'static str>) -> Self {
        Self {
            inner,
            message_types,
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Box<dyn Adapter>> {
        self.inner.lock().unwrap_or_else(|held| held.into_inner())
    }
}

impl Adapter for SharedAdapter {
    fn message_types(&self) -> &[&'static str] {
        &self.message_types
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        self.held().poll_listings(out);
    }

    /// Forwarded rather than defaulted, and the default is why: it is `None`,
    /// so a wrapper that inherited it would answer "this venue publishes no
    /// clock of its own" for every venue — leaving the venue-to-receive
    /// latency family at zero across all four of its pre-created children,
    /// which is the shape of a stopped feed rather than of a missing
    /// declaration.
    fn source_timestamp_kind(&self) -> Option<VenueTimestampKind> {
        self.held().source_timestamp_kind()
    }

    fn on_connected(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.held().on_connected(conn, out)
    }

    /// Forwarded rather than defaulted, and the default is why: it is a no-op
    /// that answers `Ok(())`. A wrapper that inherited it would report every
    /// venue as having nothing outstanding, on every cadence, for ever — and
    /// the failure that reaches an operator is the one this method exists to
    /// close: an instrument admitted mid-session gets an `Instrument ID`, a
    /// definition on the reference-data port and a place in the manifest, and
    /// never a subscription, with a healthy feed and a manifest saying it is
    /// published. A refusal would at least be counted. This says nothing.
    fn poll_upstream(
        &mut self,
        conn: ConnectionId,
        out: &mut dyn UpstreamSink,
    ) -> Result<(), AdapterError> {
        self.held().poll_upstream(conn, out)
    }

    fn on_disconnected(&mut self, conn: ConnectionId, reason: DisconnectReason) {
        self.held().on_disconnected(conn, reason);
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        self.held().on_payload(payload, out)
    }

    /// Forwarded rather than defaulted, and the default is why: it refuses. A
    /// wrapper that inherited it would report every venue's book as
    /// unimplemented.
    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<DepthBound, AdapterError> {
        self.held().snapshot(instrument, out)
    }
}

/// Whether a fatal error on `connection` ends the process.
///
/// **That connection's role decides it, and that is the whole of what a role
/// decides about a live run** — see
/// [`SourceRole::fatal_error_ends_the_process`]. A role is read in two other
/// places, neither of them here: [`primary_connection`], on the replay path,
/// and the credential rule in [`crate::config`] at load, which asks
/// [`SourceRole::credential_may_be_shared_with`] whether a pair of blocks may
/// state one credential.
/// `Driver::run` returns only on
/// [`IngressError::Fatal`](dz_ingress_core::IngressError::Fatal), which any
/// non-retryable connect, send or receive operation can report: a
/// per-connection configuration fault found at connect most of all, an invalid
/// endpoint, a missing credential path or an unsupported scheme, and equally a
/// message the transport cannot carry at all. A `comparison` connection answers
/// `false`, so a mistyped URL on a connection that by design must not reach the
/// wire cannot take the healthy primary down — nor keep it down across
/// restarts, with a fault that lives in the file a supervisor hands back. A
/// `primary` and an `upstream-partition` answer `true`: what an upstream
/// partition carries arrives on no other connection, so a publisher that
/// carried on without it would serve that subset stale while every other signal
/// said it was well.
///
/// A document that declares no upstream connections has one implicit
/// connection and no role to read, so every input ends the process and the
/// behaviour is exactly what a single-connection publisher has always had. An
/// input the document does not name cannot happen — `check_sources` holds the
/// two sets equal before this — and if it ever did, ending the process is the
/// answer that does not silently keep a publisher running past a fault.
///
/// The cost of answering `false` is stated on
/// [`poll_first_fatal_run_to_give_up`]: that connection is then down until
/// somebody restarts the process, because nothing else retries a fatal error.
fn fatal_ends_the_process(sources: &[Source], connection: ConnectionId) -> bool {
    sources.is_empty()
        || sources
            .iter()
            .find(|source| source.connection == connection)
            .is_none_or(|source| source.role.fatal_error_ends_the_process())
}

/// Poll every run, and return only when a run **whose failure is fatal** has
/// given up.
///
/// A run whose failure is not fatal is reported through `report`, dropped from
/// the set, and the publisher carries on. `Driver::run` returns only on a fatal
/// error, so such a run is permanently done and polling it again would panic;
/// leaving it out of the set is also what leaves its `connection_state` at 0,
/// which is the alert for a connection that never came up.
///
/// **Nothing retries it, and a restart is what does.** Several of the causes of
/// a fatal error are only fatal for one attempt — a credential path that does
/// not exist yet is the plain one, under late secret injection — and before this
/// the process exited and both sources came back. The trade is deliberate: a
/// source that by design must not reach the wire must not be able to take the
/// wire down with it, and the cost is that a fault which used to clear on a
/// restart the process took itself now needs one somebody takes. The report says
/// so, because "carries on without it" on its own reads like a wait.
///
/// Every run still in the set is polled on every pass, including after one
/// whose failure is not fatal has ended in the same pass — so each has
/// registered its waker and this parks rather than spins. Returning as soon as
/// one of those ended would leave the runs after it in the vector unpolled and
/// their wakers unregistered, which is a publisher that stops noticing its own
/// upstreams.
///
/// The `bool` beside each run is
/// [`SourceRole::fatal_error_ends_the_process`] for the connection that run
/// drives, answered by [`fatal_ends_the_process`]. Nothing here reads a role:
/// what this needs is that one answer per run, and taking it as a `bool` is
/// what lets the mechanism be exercised without a transport.
fn poll_first_fatal_run_to_give_up<F, E>(
    runs: &mut Vec<(bool, F)>,
    cx: &mut std::task::Context<'_>,
    mut report: impl FnMut(&'static str, &E),
) -> std::task::Poll<(&'static str, E)>
where
    F: std::future::Future<Output = (&'static str, E)> + Unpin,
{
    let mut done: Vec<usize> = Vec::new();
    for (index, (ends_the_process, run)) in runs.iter_mut().enumerate() {
        let std::task::Poll::Ready((connection, error)) = std::pin::Pin::new(run).poll(cx) else {
            continue;
        };
        if *ends_the_process {
            return std::task::Poll::Ready((connection, error));
        }
        report(connection, &error);
        done.push(index);
    }
    // Back to front, so the earlier indices stay valid.
    for index in done.into_iter().rev() {
        // Dropped, and deliberately: the future has already returned `Ready`,
        // so there is nothing left in it to await.
        drop(runs.remove(index));
    }
    std::task::Poll::Pending
}

/// The registry this publisher exposes, with the build that is running stamped
/// on it.
///
/// **The stamp goes on the registry rather than on the publisher, because it is
/// a statement about the process and not about the publishing.** It is set here
/// so that it is already on the gauge before the metrics server can be
/// scraped — a build that answered `dz_publisher_build_info` only once the
/// sockets were up would be unidentifiable for exactly the window an operator
/// is watching.
///
/// `version` is the argument [`run_with_version`] was handed, which is the same
/// string `--version` writes to stdout. **A gauge that read this crate's own
/// `CARGO_PKG_VERSION` here instead would report the runtime while the flag
/// reported the venue binary that linked it**: two numbers for one question,
/// with no failure anywhere — the scrape and the binary simply disagreeing
/// about which build is deployed.
///
/// The commit and the toolchain stay compile-time environment reads, which is
/// where a build stamps them. Absent is `unknown`, which is honest: a build
/// that did not stamp its commit cannot be asked what it was.
///
/// # Errors
///
/// Whatever [`register_venue_collectors`] refuses.
fn publisher_registry(
    version: &str,
    config: &PublisherMetricsConfig<'_>,
    venue_collectors: Vec<Box<dyn dz_publisher_metrics::prometheus::core::Collector>>,
) -> Result<Arc<PublisherMetrics>, StartupError> {
    let metrics = Arc::new(PublisherMetrics::new(config));
    register_venue_collectors(&metrics, venue_collectors)?;
    metrics.process().set_build_info(
        version,
        option_env!("DZ_PUBLISHER_COMMIT").unwrap_or("unknown"),
        option_env!("DZ_PUBLISHER_TOOLCHAIN").unwrap_or("unknown"),
    );
    Ok(metrics)
}

/// Registers a venue's own collectors into the second registry.
///
/// **Called after the normative set exists, because it cannot be called
/// before.** See [`Venue::collectors`](crate::Venue::collectors) for why its
/// argument arrives here rather than at construction.
///
/// # Errors
///
/// [`StartupError::VenueMetric`], for any of the three things that registry
/// refuses: a series name under the reserved `dz_publisher_` prefix, a label
/// named `venue` or `source_id` — which it applies as constant labels, so a
/// collector carrying either fails the whole scrape rather than one series —
/// and whatever the underlying registration rejects, a duplicate descriptor
/// being the one to expect.
///
/// Every one is a startup failure rather than a dropped collector. A publisher
/// that ran anyway would report one thing under the name of another, or serve
/// a scrape that fails whole, for as long as nobody looked.
fn register_venue_collectors(
    metrics: &PublisherMetrics,
    collectors: Vec<Box<dyn dz_publisher_metrics::prometheus::core::Collector>>,
) -> Result<(), StartupError> {
    for collector in collectors {
        metrics
            .venue_registry()
            .register(collector)
            .map_err(|source| StartupError::VenueMetric { source })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dz_publisher_metrics::prometheus::core::Collector;
    use dz_publisher_metrics::prometheus::IntCounter;

    /// Parse a command line written the way a shell hands it over: the
    /// arguments after the program name, and nothing else.
    fn invocation_of(args: &[&str]) -> Result<Invocation, StartupError> {
        invocation(args.iter().map(OsString::from))
    }

    /// A real file is still named bare, and still named after `--config`.
    ///
    /// The branch every other case here falls *past*, so a refusal that reached
    /// too far would take this with it: the whole cost of refusing an unknown
    /// option is that this must keep working.
    #[test]
    fn a_configuration_file_is_named_bare_or_after_the_option() {
        assert_eq!(
            invocation_of(&["/etc/dz/publisher.toml"]).expect("a bare path is a path"),
            Invocation::Config(PathBuf::from("/etc/dz/publisher.toml"))
        );
        assert_eq!(
            invocation_of(&["--config", "/etc/dz/publisher.toml"]).expect("and so is this one"),
            Invocation::Config(PathBuf::from("/etc/dz/publisher.toml"))
        );
        // A relative path, and one whose first character is not a letter: a
        // dash is what makes an argument an option, and `.` is not a dash.
        assert_eq!(
            invocation_of(&["./publisher.toml"]).expect("a relative path is a path"),
            Invocation::Config(PathBuf::from("./publisher.toml"))
        );
    }

    /// `--config=<path>` names the document `--config <path>` names.
    ///
    /// **The spelling a unit file writes.** `ExecStart` lines carry
    /// `--config=/etc/dz/publisher.toml` as readily as the two-argument form,
    /// and a reader that matched only the exact option would fall past it to
    /// the refusal below and report a known option as one this publisher does
    /// not know — the misdiagnosis-by-one-step that refusal exists to remove,
    /// for the spelling an operator is most likely to have written.
    #[test]
    fn a_configuration_file_is_named_by_the_joined_spelling_too() {
        assert_eq!(
            invocation_of(&["--config=/etc/dz/publisher.toml"])
                .expect("the joined spelling names a document"),
            Invocation::Config(PathBuf::from("/etc/dz/publisher.toml"))
        );
        // Nothing is split on a second `=`: the value runs to the end of the
        // argument, because a path is a path and one of them may contain one.
        assert_eq!(
            invocation_of(&["--config=/etc/dz/a=b.toml"]).expect(
                "the value ends where the \
                 argument does"
            ),
            Invocation::Config(PathBuf::from("/etc/dz/a=b.toml"))
        );
        // The two spellings are one option, so a second document is still a
        // second document however either of them was written.
        match invocation_of(&["--config=a.toml", "--config", "b.toml"]) {
            Err(StartupError::TwoConfigPaths { first, second, .. }) => {
                assert_eq!(first, "a.toml");
                assert_eq!(second, "b.toml");
            }
            other => panic!("the joined spelling did not name the first document: {other:?}"),
        }
    }

    /// `--config=` is the option asking for a value, not a file named nothing.
    ///
    /// The empty path would otherwise be opened and refused as a document that
    /// could not be read, which reports the failure one step away from the
    /// mistake — the same defect as refusing the joined spelling by name.
    #[test]
    fn the_joined_spelling_with_nothing_after_it_names_the_option() {
        match invocation_of(&["--config="]) {
            Err(StartupError::OptionNeedsValue { option, usage }) => {
                assert_eq!(option, CONFIG_OPTION);
                assert_eq!(usage, USAGE);
            }
            other => panic!("an empty value did not name the option: {other:?}"),
        }
    }

    /// Both spellings, because deployment tooling writes whichever it writes.
    #[test]
    fn the_version_is_asked_for_by_either_spelling() {
        assert_eq!(
            invocation_of(&["--version"]).expect("--version is an option"),
            Invocation::Version
        );
        assert_eq!(
            invocation_of(&["-V"]).expect("-V is the same option"),
            Invocation::Version
        );
    }

    /// `-V` and `-v` are not the same argument, and neither is a path.
    ///
    /// The lower-case spelling is nothing here, so it has to be refused by name
    /// rather than opened as a file — which is the defect this refusal exists
    /// for, met by the nearest possible typo.
    #[test]
    fn an_option_this_reader_does_not_know_is_refused_by_name() {
        // `--confg=` is the joined spelling of an option that does not exist:
        // accepting `--config=<path>` must not turn every `--anything=<value>`
        // into a document, and the whole argument is what an operator has to
        // find in their unit file.
        for option in ["--verison", "--nope", "-v", "-", "--confg=publisher.toml"] {
            // First, and after a configuration file that is perfectly good: an
            // option nobody reads is not a command line anybody meant, whichever
            // end of it the typo is at.
            for args in [vec![option], vec!["publisher.toml", option]] {
                match invocation_of(&args) {
                    Err(StartupError::UnknownOption {
                        option: named,
                        usage,
                    }) => {
                        assert_eq!(named, option);
                        assert_eq!(usage, USAGE);
                    }
                    other => panic!("{args:?} was not refused by name: {other:?}"),
                }
            }
        }
    }

    /// The forms that publish nothing are in the text an operator is shown.
    #[test]
    fn the_usage_names_every_accepted_form() {
        for form in ["--config", "--version", "-V", "--help", "-h"] {
            assert!(USAGE.contains(form), "{form} is not in the usage: {USAGE}");
        }
    }

    /// A publisher asked for nothing at all asks for a document, and names the
    /// forms it would take one in.
    #[test]
    fn a_command_line_with_no_configuration_file_names_the_usage() {
        match invocation_of(&[]) {
            Err(StartupError::NoConfigPath { usage }) => assert_eq!(usage, USAGE),
            other => panic!("an empty command line did not ask for a document: {other:?}"),
        }
    }

    /// The two forms that publish nothing win wherever they are written.
    ///
    /// **This is the one that costs a live publisher when it is wrong.** A
    /// reader that took only the first argument answers
    /// `--config publisher.toml --version` by composing everything and putting
    /// datagrams on a group — and that ordering is not exotic: the recorder
    /// beside this one takes it, and a unit file that runs a binary to ask what
    /// it is writes the configuration first.
    #[test]
    fn the_flags_that_publish_nothing_win_wherever_they_appear() {
        for args in [
            vec!["--config", "publisher.toml", "--version"],
            vec!["publisher.toml", "--version"],
            vec!["publisher.toml", "-V"],
        ] {
            assert_eq!(
                invocation_of(&args).expect("a version is still asked for"),
                Invocation::Version,
                "{args:?}"
            );
        }
        for args in [
            vec!["--help"],
            vec!["-h"],
            vec!["publisher.toml", "--help"],
            vec!["--config", "publisher.toml", "-h"],
        ] {
            assert_eq!(
                invocation_of(&args).expect("help is still asked for"),
                Invocation::Help,
                "{args:?}"
            );
        }
    }

    /// The first decisive argument answers, whichever kind it is.
    ///
    /// Both orders are here because both are decisions and neither is an
    /// exception: a version query is answered from the flag rather than from
    /// the rest of the line, and a line whose misspelling comes first is
    /// refused because that is what was read by then.
    #[test]
    fn the_first_decisive_argument_is_the_one_answered() {
        assert_eq!(
            invocation_of(&["--version", "--verison"]).expect("the flag was reached first"),
            Invocation::Version
        );
        assert_eq!(
            invocation_of(&["--help", "--verison"]).expect("and so was this one"),
            Invocation::Help
        );
        match invocation_of(&["--verison", "--version"]) {
            Err(StartupError::UnknownOption { option, .. }) => assert_eq!(option, "--verison"),
            other => panic!("the misspelling was reached first: {other:?}"),
        }
    }

    /// Two documents is a question about which, and there is no rule for it.
    ///
    /// Dropping either would answer it silently, and the answer that keeps
    /// running is the one nobody meant to keep.
    #[test]
    fn two_configuration_files_are_refused_rather_than_one_being_dropped() {
        for args in [
            vec!["a.toml", "b.toml"],
            vec!["--config", "a.toml", "--config", "b.toml"],
            vec!["a.toml", "--config", "b.toml"],
            vec!["--config", "a.toml", "b.toml"],
        ] {
            match invocation_of(&args) {
                Err(StartupError::TwoConfigPaths {
                    first,
                    second,
                    usage,
                }) => {
                    assert_eq!(first, "a.toml", "{args:?}");
                    assert_eq!(second, "b.toml", "{args:?}");
                    assert_eq!(usage, USAGE, "{args:?}");
                }
                other => panic!("{args:?} did not refuse the second file: {other:?}"),
            }
        }
    }

    /// An option with no value is refused as the option, not as the file.
    ///
    /// The second case is the one a wrong refusal reads badly on: a perfectly
    /// good document is on the line, so *no configuration file* would be
    /// pointing at the half of it that is right.
    #[test]
    fn an_option_with_no_value_is_named_rather_than_the_file() {
        for args in [vec!["--config"], vec!["publisher.toml", "--config"]] {
            match invocation_of(&args) {
                Err(StartupError::OptionNeedsValue { option, usage }) => {
                    assert_eq!(option, "--config", "{args:?}");
                    assert_eq!(usage, USAGE, "{args:?}");
                }
                other => panic!("{args:?} did not name the option: {other:?}"),
            }
        }
    }

    /// A version nobody set prints as an answer rather than as a blank line.
    ///
    /// Both halves matter to a consumer that compares: the trim, because
    /// surrounding whitespace is a difference no pin carries and stdout is
    /// written as given; and `unknown`, because an empty line reads as a flag
    /// that half works and would compare equal to nothing an operator holds.
    #[test]
    fn a_version_nobody_set_is_reported_as_unknown() {
        assert_eq!(reported_version(""), UNKNOWN_VERSION);
        assert_eq!(reported_version("   "), UNKNOWN_VERSION);
        assert_eq!(reported_version("\n"), UNKNOWN_VERSION);
        assert_eq!(version_stdout(reported_version("")), "unknown\n");
        assert_eq!(reported_version(" 1.2.3\n"), "1.2.3");
        assert_eq!(reported_version("0.2.0"), "0.2.0");
    }

    /// A caller's value that would print as two lines is `unknown`.
    ///
    /// **The output contract is one line, and it is the public promise
    /// `run_with_version` makes.** A venue that assembles its argument out of a
    /// command's whole output hands over a first line a comparison passes on
    /// and a second nobody pinned, and the same string reaches
    /// `dz_publisher_build_info` as a label a scrape splits on. One line that
    /// fails every comparison is an answer; two lines are a contract broken for
    /// every consumer of the flag.
    #[test]
    fn a_version_that_would_print_as_two_lines_is_reported_as_unknown() {
        for handed in ["1.2.3\nextra", "1.2.3\r\nextra", "1.2.3\rextra", "a\nb\nc"] {
            assert_eq!(reported_version(handed), UNKNOWN_VERSION, "{handed:?}");
            let out = version_stdout(reported_version(handed));
            assert_eq!(out.lines().count(), 1, "{out:?}");
        }
        // The trim still does its own job: a trailing newline is surrounding
        // whitespace, not a second line, and the version survives it.
        assert_eq!(reported_version("1.2.3\n"), "1.2.3");
    }

    /// The format is the contract: exactly the version, on one line, alone.
    ///
    /// Both consumers compare this against a string they already hold — a
    /// configuration-management role against the version it pins, a release
    /// workflow against the tag it is cutting with the `v` removed — so a
    /// prefix, a suffix or a second line would oblige each of them to pick a
    /// field out of the output instead.
    #[test]
    fn the_version_is_written_alone_on_one_line() {
        assert_eq!(version_stdout("0.2.0"), "0.2.0\n");
        assert_eq!(version_stdout("1.2.3-rc.1"), "1.2.3-rc.1\n");
        let out = version_stdout("0.2.0");
        assert_eq!(out.lines().count(), 1, "{out:?}");
        assert!(out.ends_with('\n'), "{out:?}");
    }

    /// `--version` and `dz_publisher_build_info{version}` answer from one
    /// argument, and this is that argument arriving at both.
    ///
    /// **A second read of a version is how the two answers come apart.**
    /// `CARGO_PKG_VERSION` expands to the version of the crate being compiled,
    /// so a read at the gauge reports this runtime while the flag reports the
    /// venue binary that linked it — two numbers, one question, and no failure
    /// anywhere: the scrape and the binary simply disagree about which build is
    /// deployed. The composition is what threads the one argument to the gauge,
    /// and [`publisher_registry`] is the whole of it, so the version handed to
    /// it is rendered and read back here.
    ///
    /// A version no crate in this workspace carries is what makes the reading
    /// decisive: a gauge that had read its own `CARGO_PKG_VERSION` could not
    /// match it by accident. It is handed over the way a caller's value
    /// arrives — through [`reported_version`] — so what the gauge carries is
    /// exactly what stdout writes, asserted here beside it.
    #[test]
    fn the_build_gauge_carries_the_version_the_flag_answers_with() {
        let handed = reported_version("  9.9.9-handed-over \n");

        let metrics = publisher_registry(
            handed,
            &PublisherMetricsConfig {
                venue: "test-venue",
                source_id: 1,
                port_roles: &[],
                connections: &[],
                channel_ids: &[],
                ingress_message_types: &[],
            },
            Vec::new(),
        )
        .expect("a venue with no collectors of its own is no refusal");

        let rendered = metrics.render();
        let line = rendered
            .lines()
            .find(|line| line.starts_with("dz_publisher_build_info{"))
            .unwrap_or_else(|| {
                panic!("nothing stamped the build on the registry:\n{rendered}");
            });
        assert!(
            line.contains("version=\"9.9.9-handed-over\""),
            "the gauge carries a version `--version` never answers with: {line}"
        );
        assert!(
            !line.contains(RUNTIME_VERSION),
            "the gauge read this crate's own version instead of the one it was \
             handed: {line}"
        );
        assert!(line.ends_with(" 1"), "build_info is a gauge at one: {line}");

        // The other consumer of the same value, on the same line it hands to
        // stdout: one argument, two answers, and no room between them.
        assert_eq!(version_stdout(handed), "9.9.9-handed-over\n");
    }

    /// An adapter that records what the wrapper forwarded to it.
    ///
    /// Only the methods under test are given bodies; the rest are the trait's
    /// own defaults, which is the point — `SharedAdapter` is a hand-written
    /// delegate, so what it forgets to write out is silently answered by a
    /// default that belongs to no venue.
    #[derive(Default)]
    struct Recording {
        polled: Vec<ConnectionId>,
    }

    impl Adapter for Recording {
        fn message_types(&self) -> &[&'static str] {
            &["A-B"]
        }

        fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

        fn poll_upstream(
            &mut self,
            conn: ConnectionId,
            out: &mut dyn UpstreamSink,
        ) -> Result<(), AdapterError> {
            self.polled.push(conn);
            out.send_text("subscribe:A-B");
            Ok(())
        }

        fn on_payload(
            &mut self,
            _payload: &Payload<'_>,
            _out: &mut dyn EventSink,
        ) -> Result<(), ParseError> {
            Ok(())
        }
    }

    /// Collects what an adapter wrote, so a forwarded call can be told from a
    /// defaulted one by more than a counter.
    #[derive(Default)]
    struct Wrote(Vec<String>);

    impl UpstreamSink for Wrote {
        fn send_text(&mut self, text: &str) {
            self.0.push(text.to_owned());
        }

        fn send_binary(&mut self, bytes: &[u8]) {
            self.0.push(format!("{} bytes", bytes.len()));
        }
    }

    /// The delegate asks the venue's adapter what is outstanding.
    ///
    /// `Adapter::poll_upstream` is defaulted to a silent `Ok(())`, so a
    /// `SharedAdapter` that does not write it out compiles, runs, and answers
    /// *nothing outstanding* for every venue on every cadence — with the driver
    /// asking exactly as designed and no series anywhere going non-zero. The
    /// same shape `SharedSink`'s own doc records having been found once
    /// already, on `desynchronised`.
    ///
    /// Asserted through the sink as well as through the count, because an
    /// implementation that forwarded the call and dropped the `out` it was
    /// handed would satisfy a count alone.
    #[test]
    fn the_delegate_asks_the_venue_adapter_what_is_outstanding() {
        let inner: Arc<Mutex<Box<dyn Adapter>>> =
            Arc::new(Mutex::new(Box::new(Recording::default())));
        let mut shared = SharedAdapter::new(Arc::clone(&inner), vec!["A-B"]);
        let mut wrote = Wrote::default();

        shared
            .poll_upstream(ConnectionId::new("primary"), &mut wrote)
            .expect("the adapter has something outstanding and no reason to refuse");

        assert_eq!(
            wrote.0,
            vec!["subscribe:A-B".to_owned()],
            "what the venue's adapter queued has to reach the queue the driver flushes"
        );
    }

    /// A metrics set shaped like the smallest publisher there is.
    fn metrics() -> PublisherMetrics {
        PublisherMetrics::new(&PublisherMetricsConfig {
            venue: "a-venue",
            source_id: 1,
            port_roles: &[dz_edge_core::PortRole::Mktdata],
            connections: &["primary"],
            channel_ids: &[0],
            ingress_message_types: &["A-B"],
        })
    }

    fn collector(name: &str) -> Box<dyn Collector> {
        Box::new(IntCounter::new(name, "a venue's own count").expect("the metric is well formed"))
    }

    /// A venue's series reaches the exposition, under the venue registry.
    ///
    /// The whole ask: a venue counts something the normative set has no name
    /// for, and an operator scraping one endpoint sees it beside the series
    /// that set does describe.
    #[test]
    fn a_venue_collector_reaches_the_exposition() {
        let metrics = metrics();
        register_venue_collectors(&metrics, vec![collector("venue_books_crossed_total")])
            .expect("a name outside the reserved prefix is taken");

        let rendered = metrics.render();
        assert!(
            rendered.contains("venue_books_crossed_total"),
            "the venue's own series is not in the exposition: {rendered}"
        );
        // And it did not displace the normative set, which is the other half of
        // one endpoint carrying both.
        assert!(rendered.contains("dz_publisher_"), "{rendered}");
    }

    /// A venue cannot shadow the normative contract, and finds out at startup.
    ///
    /// The reserved prefix exists so that a series a subscriber's alert is
    /// written against means what that subscriber thinks it means. A collector
    /// that was dropped with a warning would leave a publisher reporting one
    /// thing under the name of another for as long as nobody read the log — so
    /// this refuses, and the message names what was refused.
    #[test]
    fn a_reserved_name_is_refused_at_startup_and_named() {
        let metrics = metrics();
        let error = register_venue_collectors(
            &metrics,
            vec![collector("dz_publisher_egress_datagrams_total")],
        )
        .expect_err("the reserved prefix is not a venue's to use");

        let message = error.to_string();
        assert!(
            message.contains("dz_publisher_egress_datagrams_total"),
            "the refusal has to name the series an operator must rename: {message}"
        );
    }

    /// A refused collector stops registration at that point; it does not roll
    /// back what registered before it.
    ///
    /// The collector ahead of the refusal is already registered when this
    /// returns `Err`, and it stays registered. That is harmless only because
    /// the caller treats the error as a startup failure and the process never
    /// runs with the gap — a fact about the caller, not about this function.
    /// This asserts what the function itself guarantees: the collector after
    /// the refusal is never attempted, and the one before it is not undone.
    #[test]
    fn a_refused_collector_stops_registration_without_rolling_it_back() {
        let metrics = metrics();
        let error = register_venue_collectors(
            &metrics,
            vec![
                collector("venue_first_total"),
                collector("dz_publisher_not_yours_total"),
                collector("venue_third_total"),
            ],
        );
        assert!(error.is_err());
        let rendered = metrics.render();
        assert!(
            rendered.contains("venue_first_total"),
            "the collector registered before the refusal must still be there: {rendered}"
        );
        assert!(
            !rendered.contains("venue_third_total"),
            "registration continued past the refusal: {rendered}"
        );
    }

    /// A reserved *label* is refused too, and the failure it prevents is worse.
    ///
    /// The registry applies `venue` and `source_id` as constant labels, so a
    /// collector carrying either renders a sample with a repeated label name —
    /// and the text parser rejects **the whole scrape**, not that one series. A
    /// venue's own counter would take the normative set down with it.
    #[test]
    fn a_reserved_label_is_refused_and_named() {
        use dz_publisher_metrics::prometheus::IntCounterVec;

        let metrics = metrics();
        let collector = IntCounterVec::new(
            dz_publisher_metrics::prometheus::Opts::new("venue_books_crossed_total", "a count"),
            &["venue"],
        )
        .expect("the metric is well formed");

        let message = register_venue_collectors(&metrics, vec![Box::new(collector)])
            .expect_err("a label the registry applies is not a venue's to apply")
            .to_string();
        // **The quoted tokens, not the bare words.** `MetricsError` renders as
        // `venue metric "..." carries the reserved label name "..."`, so
        // `contains("venue")` is satisfied by the boilerplate and by the metric
        // name alike — it would pass against a message that named no label at
        // all, or named `source_id`. What an operator needs from this refusal
        // is which collector to change and which label to drop, so both are
        // asserted as the formatter writes them.
        assert!(
            message.contains("\"venue\""),
            "the refusal has to name the label an operator must drop: {message}"
        );
        assert!(
            message.contains("\"venue_books_crossed_total\""),
            "and the collector it has to be dropped from: {message}"
        );
        // And the exposition still renders, which is the thing the refusal
        // protected.
        assert!(metrics.render().contains("dz_publisher_"));
    }

    /// A venue with nothing to add is not a venue that failed to add it.
    #[test]
    fn a_venue_with_no_collectors_registers_nothing_and_succeeds() {
        let metrics = metrics();
        register_venue_collectors(&metrics, Vec::new()).expect("empty is the ordinary case");
        assert!(metrics.render().contains("dz_publisher_"));
    }

    // -----------------------------------------------------------------------
    // A venue's own collectors, travelling up through the composition
    // -----------------------------------------------------------------------

    /// A document every section of which is valid, naming the built-in record
    /// adapter and whatever state directory the caller wants.
    ///
    /// Text rather than a `Config` assembled field by field, because what the
    /// composition is handed is what an operator wrote: a typed value built
    /// here would skip the resolution that decides which constructor runs at
    /// all, and the constructor is the thing under test.
    fn document(state_dir: &std::path::Path) -> String {
        format!(
            "venue = \"a-venue\"\n\
             \n\
             [egress]\n\
             ttl = 1\n\
             \n\
             [[feed]]\n\
             spec = \"top-of-book\"\n\
             enabled = true\n\
             channel_id = 3\n\
             source_id = 41\n\
             multicast_group = \"233.252.0.4\"\n\
             mktdata_port = 30001\n\
             refdata_port = 30002\n\
             heartbeat_interval = \"1s\"\n\
             definition_cycle = \"30s\"\n\
             manifest_cadence = \"1s\"\n\
             idle_guard = \"60s\"\n\
             \n\
             [refdata]\n\
             state_dir = \"{}\"\n\
             [refdata.selection]\n\
             bootstrap_top_n = 8\n\
             max_published = 16\n\
             warn_published_above = 8\n\
             \n\
             [metrics]\n\
             enabled = false\n\
             listen_addr = \"127.0.0.1:9100\"\n\
             \n\
             [ingress]\n\
             kind = \"uds\"\n\
             connect_timeout = \"5s\"\n\
             \n\
             [adapter]\n\
             kind = \"uds\"\n\
             \n\
             [adapter.upstream]\n\
             [[adapter.upstream.listing]]\n\
             symbol = \"A-B\"\n\
             asset_class = \"crypto_spot\"\n\
             price_exponent = -2\n\
             qty_exponent = -3\n\
             market_model = \"clob\"\n\
             tick_size = \"0.01\"\n\
             lot_size = \"0.001\"\n\
             settle_type = \"cash\"\n\
             price_bound = \"non_negative\"\n",
            state_dir.display()
        )
    }

    /// A `[refdata] state_dir` no directory can be created at: a path inside a
    /// regular file.
    ///
    /// **The composition has to stop somewhere observable, and this is the
    /// first such place.** [`EraStore::open`] is the step immediately after the
    /// collectors are registered, and every step after *it* opens a socket — so
    /// a state directory that cannot exist is what drives the whole of
    /// `compose_and_run` up to and including the registration and no further,
    /// with nothing bound and nothing to wait for.
    fn state_dir_that_cannot_be_created(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "dz-venue-collectors-{label}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("a temporary directory");
        let file = dir.join("regular-file");
        std::fs::write(&file, b"not a directory").expect("the file is writable");
        file.join("state")
    }

    /// A venue whose adapter is the built-in record adapter, and which counts
    /// things of its own besides.
    ///
    /// Registered under `uds`, which the composition resolves to this entry
    /// rather than to the built-in — a venue's own registration wins — so the
    /// entry delegates for the adapter and the transport it has no reason to
    /// invent, and adds what these tests are about through
    /// [`Venue::with_collectors`]. That is the only way in a venue has: the
    /// collectors leave the constructor inside a `Venue`, and the runtime is
    /// what registers them.
    fn a_venue_counting(series: &[&str]) -> AdapterRegistry {
        let series: Vec<String> = series.iter().map(|name| (*name).to_owned()).collect();
        AdapterRegistry::new().with("uds", move |cx| {
            let built_in = crate::builtin::open(cx).expect("the built-in answers `uds`")?;
            Ok(built_in.with_collectors(series.iter().map(|name| collector(name)).collect()))
        })
    }

    /// Compose that document with that registry, and return the refusal.
    fn compose(registry: &AdapterRegistry, state_dir: &std::path::Path) -> StartupError {
        let config = crate::config::Document::parse(&document(state_dir))
            .expect("the document is valid")
            .resolve()
            .expect("and it resolves");
        compose_and_run(RUNTIME_VERSION, registry, config).expect_err("this document cannot be run")
    }

    /// A venue's collectors reach that registry through the composition, and
    /// not only through the function that registers them.
    ///
    /// **The wire-up is what this path added, and nothing else here tests it.**
    /// Every other test above hands collectors to `register_venue_collectors`
    /// itself, so deleting its call site — or handing it `Vec::new()` — left
    /// all of them green: nothing drove [`Venue::collectors`] as far as the
    /// registry, and nothing called [`Venue::with_collectors`] at all.
    ///
    /// A reserved name is what makes the arrival observable from out here. The
    /// `PublisherMetrics` the composition builds is a local value no test can
    /// render, but that registry's refusal cannot be raised by a collector
    /// which did not reach it — so a venue handing up a `dz_publisher_` series
    /// is refused by name, and a composition that dropped the collectors
    /// instead gets as far as the state directory and fails for that.
    #[test]
    fn a_venues_reserved_series_is_refused_by_the_composition() {
        let error = compose(
            &a_venue_counting(&["dz_publisher_egress_datagrams_total"]),
            &state_dir_that_cannot_be_created("reserved"),
        );

        match error {
            StartupError::VenueMetric { source } => assert!(
                source
                    .to_string()
                    .contains("dz_publisher_egress_datagrams_total"),
                "the refusal has to name the series an operator must rename: {source}"
            ),
            other => panic!(
                "the venue's collectors never reached that registry: the composition refused \
                 for {other} instead"
            ),
        }
    }

    /// A series that registry accepts does not stop the composition, and it is
    /// registered before anything is opened.
    ///
    /// The other half of the wire-up. A venue with counters of its own has to
    /// start, so an accepted name must not be a refusal; and the step the
    /// composition reaches next is the state directory, which is what says the
    /// registration happened before a socket existed. A venue that learns its
    /// metric names are unusable only once the publisher is on the wire has
    /// learned it too late.
    #[test]
    fn an_accepted_series_lets_the_composition_reach_the_state_directory() {
        let error = compose(
            &a_venue_counting(&["venue_books_crossed_total"]),
            &state_dir_that_cannot_be_created("accepted"),
        );

        assert!(
            matches!(error, StartupError::Era { .. }),
            "an accepted collector is not a refusal, and the next step is the state \
             directory: {error}"
        );
    }

    #[test]
    fn a_repeated_refusal_is_printed_on_a_decade_schedule() {
        // The tick body runs every 10ms, so a permanent refusal printed on
        // every one of them is a hundred lines a second, counted nowhere and
        // burying everything else. The first is prompt, and after that only the
        // order of magnitude is news.
        let printed: Vec<u64> = (0..=1_000).filter(|n| worth_a_line(*n)).collect();
        assert_eq!(printed, [1, 10, 100, 1_000]);
    }

    #[test]
    fn nothing_is_printed_for_a_refusal_that_has_not_happened() {
        // The count is taken before the line, so zero means the caller asked
        // about the wrong bucket - and a line about a failure that did not
        // happen is worse than no line.
        assert!(!worth_a_line(0));
    }

    // -----------------------------------------------------------------------
    // The unknown shard name, and the line an operator gets
    // -----------------------------------------------------------------------

    #[test]
    fn the_unknown_shard_line_names_the_offer_and_the_configured_shards() {
        // Both halves or the line is unactionable. `pepr` against `perp` is
        // only a misspelling once the reader can see `perp`, and a line
        // carrying either name alone sends an operator to open the document
        // and work out the other half themselves.
        let line = unknown_shard_line("pepr", &["perp".to_owned(), "spot".to_owned()]);
        assert!(
            line.contains("`pepr`"),
            "the offered name is missing: {line}"
        );
        assert!(
            line.contains("`perp`") && line.contains("`spot`"),
            "the configured names are missing: {line}"
        );
        // And it says what happened to the instruments, because "not
        // configured" on its own does not say whether they were published.
        assert!(line.contains("declined"), "{line}");
    }

    #[test]
    fn a_publisher_with_no_named_shard_still_names_what_it_has() {
        // Every block defaulting to the default shard is the ordinary
        // single-channel document, and it is the one most likely to meet an
        // adapter that names shards. An empty list rendered as nothing at all
        // would read as a truncated line rather than as an answer.
        let line = unknown_shard_line("perp", &[]);
        assert!(line.contains("none"), "{line}");
    }

    // -----------------------------------------------------------------------
    // Which upstream source's fatal error ends the process
    // -----------------------------------------------------------------------

    fn declared(name: &'static str, role: SourceRole) -> Source {
        Source {
            connection: ConnectionId::new(name),
            kind: dz_ingress_core::Kind::Uds,
            role,
            upstream: toml::Table::new(),
            credentials: toml::Table::new(),
        }
    }

    /// A connection that by design must not reach the wire must not be able to
    /// take the wire down.
    ///
    /// `Driver::run` returns only on `IngressError::Fatal`, which any
    /// non-retryable connect, send or receive operation can report, a
    /// per-connection configuration fault found at connect most of all — so a
    /// mistyped URL on a comparison connection would otherwise end the process,
    /// and keep ending it across restarts, because the fault is in the file a
    /// supervisor hands back.
    #[test]
    fn a_comparison_fatal_error_does_not_end_the_process() {
        let sources = [
            declared("ws", SourceRole::Primary),
            declared("fix", SourceRole::Comparison),
        ];
        assert!(fatal_ends_the_process(&sources, ConnectionId::new("ws")));
        assert!(!fatal_ends_the_process(&sources, ConnectionId::new("fix")));
    }

    /// And a fatal error on an upstream partition does end the process, which
    /// is the whole of the difference between that role and a comparison.
    ///
    /// What the partition carries arrives on no other connection, so dropping
    /// its driver and carrying on serves that subset of instruments stale:
    /// their last published values stay on the wire, the surviving connections
    /// hold their own `connection_state` at 1, and the process reports itself
    /// healthy.
    #[test]
    fn an_upstream_partition_fatal_error_ends_the_process() {
        let sources = [
            declared("ws", SourceRole::Primary),
            declared("ws-2", SourceRole::UpstreamPartition),
            declared("fix", SourceRole::Comparison),
        ];
        assert!(fatal_ends_the_process(&sources, ConnectionId::new("ws-2")));
        // Beside it, unchanged, so the answer is the role's and not the
        // position's.
        assert!(fatal_ends_the_process(&sources, ConnectionId::new("ws")));
        assert!(!fatal_ends_the_process(&sources, ConnectionId::new("fix")));
    }

    /// A document with no `[[source]]` array keeps exactly the behaviour a
    /// single-source publisher has always had.
    #[test]
    fn with_no_sources_declared_every_fatal_error_still_ends_the_process() {
        assert!(fatal_ends_the_process(&[], ConnectionId::new("whatever")));
    }

    /// An input the document does not name cannot happen — `check_sources`
    /// holds the two sets equal first — and if it ever did, the answer must not
    /// be the one that keeps a publisher running past a fault.
    #[test]
    fn an_undeclared_connection_fatal_error_ends_the_process() {
        let sources = [declared("ws", SourceRole::Primary)];
        assert!(fatal_ends_the_process(
            &sources,
            ConnectionId::new("nobody")
        ));
    }

    /// The mechanism: a run whose failure is not fatal is reported, dropped,
    /// and the publisher carries on.
    #[test]
    fn a_non_fatal_run_that_gives_up_is_reported_and_dropped_from_the_set() {
        type Run =
            std::pin::Pin<Box<dyn std::future::Future<Output = (&'static str, &'static str)>>>;
        let ready = |name: &'static str| -> Run { Box::pin(std::future::ready((name, "fatal"))) };
        let pending = || -> Run { Box::pin(std::future::pending()) };

        // Two runs whose failure is not fatal have given up and the one whose
        // failure is fatal has not. Both are reported in the same pass, which
        // is what says the loop carries on past the first rather than
        // returning: a run left unpolled is a waker unregistered, and a
        // publisher that stops noticing its own upstreams.
        let mut runs: Vec<(bool, Run)> = vec![
            (false, ready("fix")),
            (true, pending()),
            (false, ready("poll")),
        ];
        let mut reported: Vec<&'static str> = Vec::new();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        let polled = poll_first_fatal_run_to_give_up(&mut runs, &mut cx, |connection, _| {
            reported.push(connection);
        });
        assert!(polled.is_pending(), "no fatal run has given up");
        assert_eq!(reported, vec!["fix", "poll"]);
        // Dropped from the set, because a future that returned `Ready` panics
        // if it is polled again — and because leaving it out is what leaves its
        // connection_state at 0.
        assert_eq!(runs.len(), 1);
        assert!(runs[0].0, "the one left is the run whose failure is fatal");

        // A second pass over the same set does not re-report, and does not
        // panic on a completed future.
        reported.clear();
        assert!(
            poll_first_fatal_run_to_give_up(&mut runs, &mut cx, |connection, _| {
                reported.push(connection);
            })
            .is_pending()
        );
        assert!(reported.is_empty());
    }

    /// And a fatal run's own failure is returned, named, on the pass it
    /// happens.
    #[test]
    fn a_fatal_run_that_gives_up_ends_the_poll_and_is_named() {
        type Run =
            std::pin::Pin<Box<dyn std::future::Future<Output = (&'static str, &'static str)>>>;
        let mut runs: Vec<(bool, Run)> = vec![
            (false, Box::pin(std::future::pending())),
            (true, Box::pin(std::future::ready(("ws", "endpoint")))),
        ];
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        let polled = poll_first_fatal_run_to_give_up(&mut runs, &mut cx, |_, _| {
            panic!("a run whose failure is fatal is not reported and carried on from");
        });
        match polled {
            std::task::Poll::Ready((connection, error)) => {
                assert_eq!(connection, "ws");
                assert_eq!(error, "endpoint");
            }
            std::task::Poll::Pending => panic!("the primary gave up"),
        }
    }
}
