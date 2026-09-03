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
use std::net::SocketAddrV4;
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
use dz_publisher_refdata::{CycleSchedule, FileStore, Registry, RegistryConfig, StateStore};

use crate::clock::{Clock, SystemClock};
use crate::config::{Config, Feed, FeedSpec, SourceRole};
use crate::error::StartupError;
use crate::guard::{Exit, Inconsistency};
use crate::observer::MetricsObserver;
use crate::pipeline::{FeedPipeline, Port, Ports};
use crate::publisher::{Feeds, Publisher, SnapshotError};
use crate::registry::{AdapterContext, AdapterRegistry};

/// How often the tick body runs.
///
/// A constant, not a key: every cadence the tick serves is read off the clock as
/// a debt rather than counted in ticks, so this value changes only how promptly
/// a due thing happens and never how much of it happens. Ten milliseconds is
/// well below the shortest cadence the design's own configuration states.
const TICK: Duration = Duration::from_millis(10);

/// The most datagrams one definition tick may emit.
///
/// One, which is the smallest schedule that makes progress and the strongest
/// form of the anti-burst rule: the reference-data specification forbids
/// emitting the published set as a single burst, and one datagram per tick
/// cannot approximate one. A stall therefore degrades into a denser lap rather
/// than a spike, which is what
/// [`CycleSchedule`](dz_publisher_refdata::CycleSchedule) is built to do.
const MAX_DEFINITION_DATAGRAMS_PER_TICK: usize = 1;

/// What `--help` says.
const USAGE: &str = "usage: <publisher> <config.toml>";

/// Run a publisher.
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
#[must_use]
pub fn run(registry: AdapterRegistry) -> ExitCode {
    match start(&registry) {
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
        Err(error) => {
            eprintln!("dz-publisher-runtime: {error}");
            let mut source = std::error::Error::source(&error);
            while let Some(cause) = source {
                eprintln!("  caused by: {cause}");
                source = cause.source();
            }
            ExitCode::FAILURE
        }
    }
}

/// Everything `run` does, with the failure typed.
fn start(registry: &AdapterRegistry) -> Result<Exit, StartupError> {
    let path = config_path()?;
    let config = Config::load(path)?;
    compose_and_run(registry, config)
}

fn config_path() -> Result<PathBuf, StartupError> {
    let mut args = std::env::args_os().skip(1);
    match args.next() {
        Some(first) if first == "--help" || first == "-h" => {
            Err(StartupError::NoConfigPath { usage: USAGE })
        }
        Some(first) if first == "--config" => args
            .next()
            .map(PathBuf::from)
            .ok_or(StartupError::NoConfigPath { usage: USAGE }),
        Some(first) => Ok(PathBuf::from(first)),
        None => Err(StartupError::NoConfigPath { usage: USAGE }),
    }
}

fn compose_and_run(registry: &AdapterRegistry, config: Config) -> Result<Exit, StartupError> {
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
    let cx = AdapterContext::new(
        &config.adapter,
        config.ingress_kind,
        &config.venue,
        &config.sources,
    );
    let venue = registry.open(&cx)?;
    check_sources(&config, &venue)?;
    // **`[adapter.replay]` substitutes for the transport, not for the
    // adapter.** An offline run exercises this whole function — the config, the
    // registry, the venue's own adapter, the lowering, the sockets — with
    // recorded upstream bytes in place of a live venue. The adapter cannot tell
    // the difference, which is the property that makes the exercise worth
    // anything; the transport the venue built is dropped unused, and a line
    // says so rather than leaving an operator to wonder why nothing connected.
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
    let adapter = Arc::new(Mutex::new(venue.adapter));
    let message_types = {
        let held = adapter.lock().unwrap_or_else(|held| held.into_inner());
        held.message_types().to_vec()
    };
    // Every source's name, so that `ingress_connection_state` is pre-created at
    // 0 for each of them: a publisher whose second upstream never came up is
    // the case the alert exists for, and a series that appeared on first
    // success would not carry it.
    let connections: Vec<&'static str> = inputs
        .iter()
        .map(|input| input.connection().as_str())
        .collect();

    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: &config.venue,
        source_id: identity.source_id.get(),
        port_roles: &config.port_roles(),
        connections: &connections,
        channel_ids: &config.channel_ids(),
        ingress_message_types: &message_types,
    }));

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
    let refdata = Registry::open(
        RegistryConfig {
            source_id: identity.source_id,
            channel_id: identity.channel_id,
            selection: config.refdata.selection,
            schedule,
        },
        FileStore::new(&config.refdata.state_dir),
        clock.clone(),
    )?;

    let route = KernelRoute;
    let mut feeds = Feeds::default();
    for feed in &config.feeds {
        // The match is total over a set that is not `#[non_exhaustive]`, so a
        // feed specification added to `FeedSpec` breaks the build here - which
        // is the point. A value a configuration can name that nothing composes
        // is a value that resolves to nothing at startup.
        match feed.spec {
            FeedSpec::TopOfBook => {
                let ports = open_ports(feed, &config, &metrics, &route)?;
                feeds.top_of_book = Some(FeedPipeline::new(
                    feed,
                    Arc::clone(&metrics),
                    eras.begin_era::<TopOfBook>()?,
                    ports,
                ));
            }
            FeedSpec::MarketByPrice => {
                let ports = open_ports(feed, &config, &metrics, &route)?;
                feeds.market_by_price = Some(FeedPipeline::new(
                    feed,
                    Arc::clone(&metrics),
                    eras.begin_era::<MarketByPrice>()?,
                    ports,
                ));
            }
        }
    }

    let publisher = RefCell::new(Publisher::new(
        Arc::clone(&metrics),
        refdata,
        clock.clone(),
        identity.source_id,
        feeds,
        identity.idle_guard,
    ));
    publisher.borrow().record_build_info(
        env!("CARGO_PKG_VERSION"),
        // Compile-time environment variables a build sets, not configuration
        // keys. Absent is `unknown`, which is honest: a build that did not stamp
        // its commit cannot be asked what it was.
        option_env!("DZ_PUBLISHER_COMMIT").unwrap_or("unknown"),
        option_env!("DZ_PUBLISHER_TOOLCHAIN").unwrap_or("unknown"),
    );

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
        let mut drivers: Vec<(&'static str, Driver<'_>)> = inputs
            .iter_mut()
            .zip(shared_adapters.iter_mut())
            .map(|(input, shared)| {
                let name = input.connection().as_str();
                (
                    name,
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
        let mut runs: Vec<Run<'_>> = drivers
            .iter_mut()
            .zip(sinks.iter_mut())
            .map(|((name, driver), sink)| {
                let name = *name;
                Box::pin(async move { (name, driver.run(sink).await) }) as Run<'_>
            })
            .collect();

        // The first driver to give up ends the process, and it is named. There
        // is no `select!` over a count decided at runtime, and no task per
        // source either: the composed publisher is deliberately not `Send`, so
        // polling them in turn from one future is what keeps every borrow in
        // this task. Each returns `Pending` having registered its own waker, so
        // this parks rather than spins.
        let first_to_give_up = std::future::poll_fn(|cx| {
            for run in &mut runs {
                if let std::task::Poll::Ready(ended) = run.as_mut().poll(cx) {
                    return std::task::Poll::Ready(ended);
                }
            }
            std::task::Poll::Pending
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
fn primary_connection(config: &Config, venue: &crate::Venue) -> ConnectionId {
    config
        .sources
        .iter()
        .find(|source| source.role == SourceRole::Primary)
        .map(|source| source.connection)
        .unwrap_or_else(|| venue.sources[0].connection())
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
        // **One socket per port role**, at `path` suffixed with the role's own
        // token. The diff this stream exists for is keyed on the destination
        // port among other things, and a Unix datagram carries no UDP header —
        // so three roles sharing one socket would hand a recorder datagrams it
        // could not attribute without decoding them, and decoding is the one
        // thing a record path does not do. The shape mirrors the recorder's own
        // configuration, which already names a port per role.
        if config.adapter.tee.enabled {
            let prefix = config
                .adapter
                .tee
                .path
                .as_deref()
                .ok_or(StartupError::TeeWithoutPath)?;
            let mut destination = prefix.as_os_str().to_owned();
            destination.push(".");
            destination.push(port_role.as_str());
            let destination = PathBuf::from(destination);
            eprintln!(
                "teeing {} datagrams to {}",
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

/// The numbers no series carries, on the way out.
///
/// Three of them, each named where it is documented: lowering refusals by
/// reason, events this build had no feed to carry, and adapter failures the
/// closed family set has nowhere for. A log line is not a substitute for a
/// series and is not offered as one; it is what a closed metric set leaves.
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
}

/// Poll listings and tick until a guard fires.
async fn tick_loop<S: StateStore, K: Clock + Clone>(
    publisher: &RefCell<Publisher<S, K>>,
    adapter: &Arc<Mutex<Box<dyn Adapter>>>,
    clock: &K,
) -> Exit {
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
                for (instrument, anchor) in publisher.owed_snapshots() {
                    if let Err(error) = publisher.snapshot_anchored_at(&**held, instrument, anchor)
                    {
                        eprintln!("dz-publisher-runtime: a recovery snapshot was refused: {error}");
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
                    match error {
                        // Expected, transient, and not a failure: the contract
                        // of `NotReady` is that the caller comes back.
                        SnapshotError::Adapter(AdapterError::NotReady { .. }) => {}
                        error => eprintln!(
                            "dz-publisher-runtime: a periodic snapshot was refused: {error}"
                        ),
                    }
                }
            }
            publisher.tick()
        };
        if let Some(exit) = exit {
            return exit;
        }
    }
}

/// Wait for `SIGTERM` or `SIGINT`.
#[cfg(unix)]
async fn signalled() {
    use tokio::signal::unix::{signal, SignalKind};
    // A handler that cannot be installed is not a reason to refuse to publish:
    // the process is still killable, and the cost is an abrupt end rather than
    // an `EndOfSession`. Reported and then waited on forever, so the select
    // arm simply never fires.
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
