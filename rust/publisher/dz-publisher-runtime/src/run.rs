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
    Adapter, AdapterError, ConnectionId, DisconnectReason, Event, EventSink, InstrumentRef,
    ListingSink, ParseError, Payload, SnapshotSink, UpstreamSink,
};
use dz_edge_core::{PortRole, MAX_DATAGRAM_SIZE};
use dz_edge_mbp::MarketByPrice;
use dz_edge_tob::TopOfBook;
use dz_ingress_core::Driver;
use dz_publisher_egress::{EraStore, FailureScope, KernelRoute, MulticastTransmitter, Tee};
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_refdata::{CycleSchedule, FileStore, Registry, RegistryConfig, StateStore};

use crate::clock::{Clock, SystemClock};
use crate::config::{Config, Feed, FeedSpec};
use crate::error::StartupError;
use crate::guard::{Exit, Inconsistency};
use crate::observer::MetricsObserver;
use crate::pipeline::{FeedPipeline, Port, Ports};
use crate::publisher::{Feeds, Publisher};
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
    let cx = AdapterContext::new(&config.adapter, config.ingress_kind, &config.venue);
    let venue = registry.open(&cx)?;
    let mut input = venue.input;
    let adapter = Arc::new(Mutex::new(venue.adapter));
    let (message_types, connection) = {
        let held = adapter.lock().unwrap_or_else(|held| held.into_inner());
        (held.message_types().to_vec(), input.connection())
    };

    let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: &config.venue,
        source_id: identity.source_id.get(),
        port_roles: &config.port_roles(),
        connections: &[connection.as_str()],
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

    let exit = runtime.block_on(async {
        let mut shared_adapter = SharedAdapter::new(Arc::clone(&adapter), message_types.clone());
        let mut driver = Driver::new(
            &mut *input,
            &mut shared_adapter,
            &clock,
            &observer,
            config.ingress,
        );
        let mut sink = SharedSink(&publisher);
        tokio::select! {
            error = driver.run(&mut sink) => Exit::ConsistencyGuard(
                Inconsistency::UpstreamUnusable { detail: error.to_string() },
            ),
            exit = tick_loop(&publisher, &adapter, &clock) => exit,
            () = signalled() => Exit::Signal,
        }
    });

    // The driver is dropped, so nothing more arrives from upstream: the first
    // step of the teardown is already true when `shut_down` records it.
    let teardown = publisher.borrow_mut().shut_down(exit);
    report(&publisher.borrow(), &observer);
    // Rendered once more with the exit recorded, so a scrape that lands after
    // the process is gone is not the first one that would have carried it.
    drop(server);
    Ok(teardown.exit().clone())
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
        // `[adapter.tee]` would add a second member to this fan-out, and to
        // this rather than to a second transmitter: it darkens nothing when it
        // fails and must never be able to end a send, which is exactly what
        // `Tee` guarantees a member. It is not added because the framing it
        // writes is the framing the offline comparison reads, and that framing
        // does not exist yet - see `crate::config::TeeConfig`.
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

impl<S: StateStore, K: Clock + Clone> EventSink for SharedSink<'_, S, K> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.0.borrow_mut().upstream_message(message_type);
    }

    fn event(&mut self, event: Event<'_>) {
        self.0.borrow_mut().event(event);
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

    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<(), AdapterError> {
        self.held().snapshot(instrument, out)
    }
}
