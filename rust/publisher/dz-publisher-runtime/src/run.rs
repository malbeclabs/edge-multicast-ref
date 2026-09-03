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
use crate::config::{Config, Feed, FeedSpec, Source, SourceRole};
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
        // Whether a fatal error from this connection ends the process.
        //
        // Only a primary's does, and that is the whole of what `role` decides
        // at runtime. `Driver::run` returns only on `IngressError::Fatal`,
        // whose documented causes are the per-source configuration faults found
        // at connect: an invalid endpoint, a missing credential path, an
        // unsupported scheme. Without this a mistyped URL on a source that by
        // design must not reach the wire takes the healthy primary down — and
        // keeps it down across restarts, because the fault is in the file.
        //
        // A document with no `[[source]]` array has one implicit source and no
        // role to read, so every input is primary and the behaviour is exactly
        // what a single-source publisher has always had. An input the document
        // does not name cannot happen — `check_sources` holds the two sets
        // equal before this — and if it ever did, primary is the answer that
        // does not silently keep a publisher running past a fault.
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
            .map(|((name, primary, driver), sink)| {
                let name = *name;
                (
                    *primary,
                    Box::pin(async move { (name, driver.run(sink).await) }) as Run<'_>,
                )
            })
            .collect();

        // The first **primary** driver to give up ends the process, and it is
        // named. There is no `select!` over a count decided at runtime, and no
        // task per source either: the composed publisher is deliberately not
        // `Send`, so polling them in turn from one future is what keeps every
        // borrow in this task. Each returns `Pending` having registered its own
        // waker, so this parks rather than spins.
        //
        // A driver that is not the primary's is **dropped from the set and
        // named**, and the publisher carries on. `Driver::run` returns only on
        // a fatal error, so such a driver is permanently done and polling it
        // again would panic; leaving it out is also what leaves its
        // `connection_state` at 0, which is the alert for a connection that
        // never came up. The primary is what the wire depends on, and it is the
        // only thing whose failure the wire should feel.
        let first_to_give_up = std::future::poll_fn(|cx| {
            poll_first_primary_to_give_up(&mut runs, cx, |connection, error| {
                // Named here rather than counted into a new family: the series
                // that says this happened already exists and is already
                // alerted on, and what a log adds is the reason.
                eprintln!(
                    "`{connection}` gave up and is not the primary, so this publisher carries \
                     on without it. Nothing retries it: its connection_state stays at 0 until \
                     this process is restarted, which is what retries it — and several causes \
                     of a fatal error are only fatal for one attempt, a credential path that \
                     does not exist yet most of all. {error}"
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
            let destination = config.adapter.tee.destination(feed.spec, port_role)?;
            eprintln!(
                "teeing {} {} datagrams to {}",
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

/// The numbers no series carries, on the way out.
///
/// Five of them, each named where it is documented: lowering refusals by
/// reason, snapshots asked for and not sent, events this build had no feed to
/// carry, adapter failures the closed family set has nowhere for, and fan-out
/// members that are no longer being fed. A log line is not a substitute for a
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
            publisher.tick()
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

/// Whether a fatal error from `connection` ends the process.
///
/// **Only a primary's does, and that is the whole of what `role` decides at
/// runtime.** `Driver::run` returns only on
/// [`IngressError::Fatal`](dz_ingress_core::IngressError::Fatal), whose
/// documented causes are the per-source configuration faults found at connect:
/// an invalid endpoint, a missing credential path, an unsupported scheme.
/// Without this a mistyped URL on a source that by design must not reach the
/// wire takes the healthy primary down — and keeps it down across restarts,
/// because the fault is in the file and a supervisor restarting the process
/// reads the same file.
///
/// A document with no `[[source]]` array has one implicit source and no role to
/// read, so every input is primary and the behaviour is exactly what a
/// single-source publisher has always had. An input the document does not name
/// cannot happen — `check_sources` holds the two sets equal before this — and if
/// it ever did, primary is the answer that does not silently keep a publisher
/// running past a fault.
///
/// The cost of answering `false` is stated on
/// [`poll_first_primary_to_give_up`]: that source is then down until somebody
/// restarts the process, because nothing else retries a fatal error.
fn fatal_ends_the_process(sources: &[Source], connection: ConnectionId) -> bool {
    sources.is_empty()
        || sources
            .iter()
            .find(|source| source.connection == connection)
            .is_none_or(Source::is_primary)
}

/// Poll every run, and return only when a **primary** has given up.
///
/// A run that is not a primary's is reported through `report`, dropped from the
/// set, and the publisher carries on. `Driver::run` returns only on a fatal
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
/// Every run still in the set is polled on every pass, including after a
/// non-primary has ended in the same pass — so each has registered its waker
/// and this parks rather than spins. Returning as soon as one non-primary ended
/// would leave the runs after it in the vector unpolled and their wakers
/// unregistered, which is a publisher that stops noticing its own upstreams.
fn poll_first_primary_to_give_up<F, E>(
    runs: &mut Vec<(bool, F)>,
    cx: &mut std::task::Context<'_>,
    mut report: impl FnMut(&'static str, &E),
) -> std::task::Poll<(&'static str, E)>
where
    F: std::future::Future<Output = (&'static str, E)> + Unpin,
{
    let mut done: Vec<usize> = Vec::new();
    for (index, (primary, run)) in runs.iter_mut().enumerate() {
        let std::task::Poll::Ready((connection, error)) = std::pin::Pin::new(run).poll(cx) else {
            continue;
        };
        if *primary {
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

#[cfg(test)]
mod tests {
    use super::*;

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
    // Only a primary's fatal error ends the process
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

    /// A source that by design must not reach the wire must not be able to take
    /// the wire down.
    ///
    /// `Driver::run` returns only on `IngressError::Fatal`, and its documented
    /// causes are the per-source configuration faults found at connect. So
    /// before this a mistyped URL on a comparison source ended the process —
    /// and kept ending it across restarts, because the fault is in the file a
    /// supervisor hands back.
    #[test]
    fn only_a_primarys_fatal_error_ends_the_process() {
        let sources = [
            declared("ws", SourceRole::Primary),
            declared("fix", SourceRole::Comparison),
        ];
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
    fn an_undeclared_connection_is_treated_as_a_primary() {
        let sources = [declared("ws", SourceRole::Primary)];
        assert!(fatal_ends_the_process(
            &sources,
            ConnectionId::new("nobody")
        ));
    }

    /// The mechanism: a non-primary is reported, dropped, and the publisher
    /// carries on.
    #[test]
    fn a_non_primary_that_gives_up_is_reported_and_dropped_from_the_set() {
        type Run =
            std::pin::Pin<Box<dyn std::future::Future<Output = (&'static str, &'static str)>>>;
        let ready = |name: &'static str| -> Run { Box::pin(std::future::ready((name, "fatal"))) };
        let pending = || -> Run { Box::pin(std::future::pending()) };

        // Two comparison sources have given up and the primary has not. Both
        // are reported in the same pass, which is what says the loop carries on
        // past the first rather than returning: a run left unpolled is a waker
        // unregistered, and a publisher that stops noticing its own upstreams.
        let mut runs: Vec<(bool, Run)> = vec![
            (false, ready("fix")),
            (true, pending()),
            (false, ready("rest")),
        ];
        let mut reported: Vec<&'static str> = Vec::new();
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        let polled = poll_first_primary_to_give_up(&mut runs, &mut cx, |connection, _| {
            reported.push(connection);
        });
        assert!(polled.is_pending(), "the primary has not given up");
        assert_eq!(reported, vec!["fix", "rest"]);
        // Dropped from the set, because a future that returned `Ready` panics
        // if it is polled again — and because leaving it out is what leaves its
        // connection_state at 0.
        assert_eq!(runs.len(), 1);
        assert!(runs[0].0, "the one left is the primary's");

        // A second pass over the same set does not re-report, and does not
        // panic on a completed future.
        reported.clear();
        assert!(
            poll_first_primary_to_give_up(&mut runs, &mut cx, |connection, _| {
                reported.push(connection);
            })
            .is_pending()
        );
        assert!(reported.is_empty());
    }

    /// And the primary's own failure is returned, named, on the pass it happens.
    #[test]
    fn a_primary_that_gives_up_ends_the_poll_and_is_named() {
        type Run =
            std::pin::Pin<Box<dyn std::future::Future<Output = (&'static str, &'static str)>>>;
        let mut runs: Vec<(bool, Run)> = vec![
            (false, Box::pin(std::future::pending())),
            (true, Box::pin(std::future::ready(("ws", "endpoint")))),
        ];
        let waker = std::task::Waker::noop();
        let mut cx = std::task::Context::from_waker(waker);

        let polled = poll_first_primary_to_give_up(&mut runs, &mut cx, |_, _| {
            panic!("a primary is not reported and carried on from");
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
