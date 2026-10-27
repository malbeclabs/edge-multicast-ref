//! The threaded input model: connections decoding on their own tasks, a bounded
//! queue, and the loop's dedupe. Driven through `threaded::spawn` with scripted
//! transports and a recording sink, on real worker threads, because the
//! property worth asserting is what survives a real race.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::threaded::Inline;
use dz_adapter_core::{
    Adapter, Aggressor, ClaimTable, ConnectionId, DecodedSink, Decoder, Event, EventSink,
    InstrumentRef, ListingSink, Order, ParseError, Payload, Scalar, SideUpdate, ThreadedAdapter,
    TradeFlags,
};
use dz_edge_core::PortRole;
use dz_ingress_core::{
    BackoffPolicy, BoxFuture, IngressError, Input, Policy, Received, UpstreamMessage,
};
use dz_publisher_metrics::{PublisherMetrics, PublisherMetricsConfig};
use dz_publisher_runtime::threaded::{self, Connections, Drain};
use dz_publisher_runtime::{MetricsObserver, SystemClock};
use dz_venue_composition::SharedThreadedAdapter;

/// Messages separated by spaces: `q:<instrument>:<update id>` a quote under
/// `Supersedes`, claimed before anything else is read, and `t:<instrument>:<trade
/// id>` a trade under `Identity`.
#[derive(Default)]
struct Fixture {
    inline: Option<Inline>,
}

struct FixtureDecoder {
    claims: ClaimTable,
    /// Two decoders' generations in one, so a test can order them:
    /// `g:<view>:<instrument>:<counter>` claims through one of them, and
    /// `r:<view>:<instrument>:<counter>` has it judge a restart first.
    views: [dz_adapter_core::CounterGenerations; 2],
}

fn quote(index: u32, update: u64) -> Event<'static> {
    Event::Quote {
        instrument: InstrumentRef::from_admission(index),
        source_ts_ns: 0,
        bid: SideUpdate::Present {
            px: Scalar::fixed(update as i64, 0),
            qty: Scalar::fixed(1, 0),
            source_count: None,
        },
        ask: SideUpdate::Gone,
        // The venue's counter is the claim's key, as it is for a venue that
        // claims on the counter it publishes.
        venue_update_id: Some(update),
    }
}

fn trade(index: u32, id: u64) -> Event<'static> {
    Event::Trade {
        instrument: InstrumentRef::from_admission(index),
        source_ts_ns: 0,
        px: Scalar::fixed(1, 0),
        qty: Scalar::fixed(1, 0),
        aggressor: Aggressor::Unknown,
        trade_id: Some(id),
        cumulative_volume: None,
        flags: TradeFlags::NONE,
    }
}

impl Decoder for FixtureDecoder {
    fn decode(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn DecodedSink,
    ) -> Result<(), ParseError> {
        let text =
            std::str::from_utf8(payload.bytes).map_err(|_| ParseError::malformed("utf-8"))?;
        for message in text.split(' ') {
            let mut fields = message.split(':');
            let kind = fields.next().ok_or(ParseError::truncated("kind"))?;
            if kind == "g" || kind == "r" {
                let mut number = || {
                    fields
                        .next()
                        .and_then(|field| field.parse::<u64>().ok())
                        .ok_or(ParseError::malformed("field"))
                };
                let view = number()? as usize;
                let index = number()? as u32;
                let counter = number()?;
                out.upstream_message("m");
                let instrument = InstrumentRef::from_admission(index);
                if kind == "r" {
                    self.views[view].restart(instrument);
                }
                match self.views[view].claim(&self.claims, instrument, counter) {
                    dz_adapter_core::Claim::Won(key) => {
                        out.push(quote(index, key), Order::Supersedes(key));
                    }
                    dz_adapter_core::Claim::LostToNewerGeneration => {
                        out.duplicate();
                        out.lost_to_newer_generation();
                    }
                    _ => out.duplicate(),
                }
                continue;
            }
            let mut number = || {
                fields
                    .next()
                    .and_then(|field| field.parse::<u64>().ok())
                    .ok_or(ParseError::malformed("field"))
            };
            let index = number()? as u32;
            let key = number()?;
            out.upstream_message("m");
            match kind {
                "q" => {
                    if !self.claims.claim(InstrumentRef::from_admission(index), key) {
                        out.duplicate();
                        continue;
                    }
                    out.push(quote(index, key), Order::Supersedes(key));
                }
                "t" => out.push(trade(index, key), Order::Identity(key)),
                _ => return Err(ParseError::schema("kind")),
            }
        }
        Ok(())
    }
}

impl ThreadedAdapter for Fixture {
    fn decoder(&self, _conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder> {
        Box::new(FixtureDecoder {
            claims,
            views: Default::default(),
        })
    }
}

impl Adapter for Fixture {
    fn message_types(&self) -> &[&'static str] {
        &["m"]
    }

    fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let mut inline = self.inline.take().unwrap_or_default();
        let result = inline.on_payload(self, payload, out);
        self.inline = Some(inline);
        result
    }
}

/// A transport that delivers its script and then waits for ever, saying when
/// the driver has come back for more than the script held — which is after the
/// last payload has been decoded.
struct Script {
    name: &'static str,
    payloads: VecDeque<Vec<u8>>,
    current: Vec<u8>,
    delivered: Arc<AtomicUsize>,
    exhausted: Arc<AtomicBool>,
}

#[derive(Clone, Default)]
struct Progress {
    delivered: Arc<AtomicUsize>,
    exhausted: Arc<AtomicBool>,
}

impl Script {
    fn new(name: &'static str, payloads: &[&str]) -> (Self, Progress) {
        let progress = Progress::default();
        (
            Self {
                name,
                payloads: payloads.iter().map(|p| p.as_bytes().to_vec()).collect(),
                current: Vec::new(),
                delivered: Arc::clone(&progress.delivered),
                exhausted: Arc::clone(&progress.exhausted),
            },
            progress,
        )
    }
}

impl Input for Script {
    fn connection(&self) -> ConnectionId {
        ConnectionId::new(self.name)
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn send<'a>(
        &'a mut self,
        _message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            match self.payloads.pop_front() {
                Some(payload) => {
                    self.current = payload;
                    self.delivered.fetch_add(1, Ordering::SeqCst);
                    // Stamped by the driver off the wall clock, as a
                    // transport with no kernel stamp is.
                    Ok(Received::Payload {
                        bytes: &self.current,
                        ts_ns: None,
                    })
                }
                None => {
                    self.exhausted.store(true, Ordering::SeqCst);
                    std::future::pending().await
                }
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// What the loop applied, in order.
#[derive(Default)]
struct Applied {
    events: Vec<Event<'static>>,
    upstream: usize,
    flushes: usize,
}

impl EventSink for Applied {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.upstream += 1;
    }

    fn event(&mut self, event: Event<'_>) {
        // The fixture's events borrow nothing, so they are rebuilt owned.
        let owned = match event {
            Event::Quote {
                instrument,
                bid:
                    SideUpdate::Present {
                        px: Scalar::Fixed { mantissa, .. },
                        ..
                    },
                ..
            } => quote(instrument.index(), mantissa as u64),
            Event::Trade {
                instrument,
                trade_id: Some(id),
                ..
            } => trade(instrument.index(), id),
            other => panic!("the fixture emits no {other:?}"),
        };
        self.events.push(owned);
    }

    fn drained(&mut self) {
        self.flushes += 1;
    }
}

fn metrics() -> Arc<PublisherMetrics> {
    Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
        venue: "test-venue",
        source_id: 41,
        port_roles: &[PortRole::Mktdata],
        connections: &["a", "b"],
        channels: &[],
        ingress_message_types: &["m"],
    }))
}

fn policy() -> Policy {
    Policy {
        connect_timeout: Duration::from_secs(1),
        backoff: BackoffPolicy::new(Duration::from_millis(10), Duration::from_millis(100))
            .expect("a valid policy"),
        rate_limit_per_second: 0,
        idle_timeout: None,
    }
}

struct Bench {
    workers: tokio::runtime::Runtime,
    publisher_loop: tokio::runtime::Runtime,
    adapter: SharedThreadedAdapter,
    metrics: Arc<PublisherMetrics>,
}

impl Bench {
    fn new() -> Self {
        Self {
            workers: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("workers"),
            publisher_loop: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("loop"),
            adapter: Arc::new(Mutex::new(Box::new(Fixture::default()))),
            metrics: metrics(),
        }
    }

    fn spawn(&self, scripts: Vec<Script>, depth: usize) -> (Connections, Drain) {
        let _entered = self.publisher_loop.enter();
        threaded::spawn(
            self.workers.handle(),
            scripts
                .into_iter()
                .map(|script| (Box::new(script) as Box<dyn Input>, true))
                .collect(),
            &self.adapter,
            Arc::new(SystemClock::new()),
            Arc::new(MetricsObserver::new(Arc::clone(&self.metrics))),
            policy(),
            Arc::clone(&self.metrics),
            depth,
        )
    }

    fn rendered(&self, family: &str) -> f64 {
        let rendered = self.metrics.render();
        rendered
            .lines()
            .find(|line| line.starts_with(family) && !line.starts_with('#'))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no {family} in:\n{rendered}"))
    }
}

/// Wait, on real threads, for something the connections do.
async fn until(what: &str, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

/// Run the scripts to their end and apply everything, through shutdown.
fn run_to_the_end(bench: &Bench, scripts: Vec<Script>, progress: Vec<Progress>) -> Applied {
    let (mut connections, mut drain) = bench.spawn(scripts, threaded::QUEUE_DEPTH);
    let mut out = Applied::default();
    bench.publisher_loop.block_on(async {
        until("every script delivered", || {
            progress
                .iter()
                .all(|progress| progress.exhausted.load(Ordering::SeqCst))
        })
        .await;
        let held = connections.stop().await;
        assert_eq!(
            drain
                .finish(held, &bench.adapter, &mut out, Duration::from_secs(5))
                .await,
            0
        );
    });
    out
}

fn key(event: &Event<'static>) -> (u32, char, u64) {
    match *event {
        Event::Quote {
            instrument,
            bid:
                SideUpdate::Present {
                    px: Scalar::Fixed { mantissa, .. },
                    ..
                },
            ..
        } => (instrument.index(), 'q', mantissa as u64),
        Event::Trade {
            instrument,
            trade_id: Some(id),
            ..
        } => (instrument.index(), 't', id),
        _ => unreachable!(),
    }
}

#[test]
fn two_connections_racing_emit_each_update_once_and_in_order_per_instrument() {
    let bench = Bench::new();
    // Duplicate and out-of-order `Supersedes` keys on both connections, and
    // every trade on both.
    let a = [
        "q:1:1 t:1:100",
        "q:1:2",
        "q:2:1 t:2:200",
        "q:1:4 t:1:101",
        "q:1:3",
        "q:2:3 t:2:201",
        "q:1:6",
        "q:2:2",
        "q:1:7 t:1:102",
        "q:2:9 t:2:202",
    ];
    let b = [
        "q:1:2",
        "q:1:1 t:1:100",
        "q:2:2 t:2:200",
        "q:1:3 t:1:101",
        "q:1:5",
        "q:1:4",
        "q:2:1 t:2:201",
        "q:1:7 t:1:102",
        "q:2:9 t:2:202",
        "q:2:8",
    ];
    let messages: usize = a.iter().chain(&b).map(|p| p.split(' ').count()).sum();
    let (script_a, progress_a) = Script::new("a", &a);
    let (script_b, progress_b) = Script::new("b", &b);
    let out = run_to_the_end(
        &bench,
        vec![script_a, script_b],
        vec![progress_a, progress_b],
    );

    let mut last: HashMap<u32, u64> = HashMap::new();
    let mut trades = HashSet::new();
    let mut applied = HashSet::new();
    for event in &out.events {
        let (instrument, kind, key) = key(event);
        if let Event::Quote {
            venue_update_id, ..
        } = *event
        {
            // Carried through the queue and the dedupe untouched, on the copy
            // that won.
            assert_eq!(venue_update_id, Some(key), "{event:?}");
        }
        assert!(
            applied.insert((instrument, kind, key)),
            "emitted twice: {event:?}"
        );
        match kind {
            'q' => {
                if let Some(previous) = last.insert(instrument, key) {
                    assert!(
                        key > previous,
                        "instrument {instrument} went from {previous} to {key}"
                    );
                }
            }
            _ => {
                trades.insert((instrument, key));
            }
        }
    }
    // The newest state of each instrument is always applied, and every trade
    // exactly once.
    assert_eq!(last.get(&1), Some(&7));
    assert_eq!(last.get(&2), Some(&9));
    assert_eq!(
        trades,
        [(1, 100), (1, 101), (1, 102), (2, 200), (2, 201), (2, 202)]
            .into_iter()
            .collect()
    );
    // Every message is either applied or counted as a duplicate, by one stage
    // or the other.
    let duplicates = bench.rendered("dz_publisher_ingress_duplicates_total");
    assert_eq!(duplicates as usize + out.events.len(), messages);
    // And counted at decode, per connection.
    assert!(out.upstream > 0, "the idle guard heard nothing upstream");
}

#[test]
fn a_full_queue_stops_the_connection_and_drops_nothing() {
    let bench = Bench::new();
    let payloads: Vec<String> = (1..=50).map(|key| format!("q:1:{key}")).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let (mut connections, mut drain) = bench.spawn(vec![script], 4);
    let mut out = Applied::default();
    bench.publisher_loop.block_on(async {
        until("the queue filled", || drain.queued() == 4).await;
        // Nothing drains it, so the connection must stop reading: four queued,
        // one held behind them, and the receive after that waits.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let read = progress.delivered.load(Ordering::SeqCst);
        assert!(
            read <= 6,
            "the connection read {read} payloads into a queue of 4"
        );
        assert!(!progress.exhausted.load(Ordering::SeqCst));

        // Drained, it carries on, and every update arrives, in order.
        while out.events.len() < 50 {
            let taken = drain.recv().await;
            assert!(taken > 0 && taken <= threaded::BATCH);
            drain.apply(&bench.adapter, &mut out);
        }
        connections.stop().await;
    });
    let keys: Vec<u64> = out.events.iter().map(|event| key(event).2).collect();
    assert_eq!(keys, (1..=50).collect::<Vec<u64>>());
    assert!(out.flushes > 0);
    assert!(bench.rendered("dz_publisher_ingress_batch_size_count") > 0.0);
}

#[test]
fn the_queue_wait_is_observed_once_per_dequeued_event() {
    let bench = Bench::new();
    // Twelve messages in nine payloads, every one of them applied: the wait is
    // per event taken off the queue, not per payload and not per batch.
    let payloads = [
        "q:1:1 t:1:1",
        "q:1:2",
        "q:1:3 t:1:2 t:1:3",
        "q:2:1",
        "q:2:2",
        "t:2:1",
        "q:1:4",
        "q:2:3",
        "t:2:2",
    ];
    let (script, progress) = Script::new("a", &payloads);
    let out = run_to_the_end(&bench, vec![script], vec![progress]);
    assert_eq!(out.events.len(), 12);
    assert_eq!(
        bench.rendered("dz_publisher_ingress_queue_wait_seconds_count"),
        12.0
    );
    // The receipt is a wall reading taken just before, so every wait sits in
    // a bucket and none is the overflow a mispaired clock would produce.
    assert!(bench.rendered("dz_publisher_ingress_queue_wait_seconds_sum") < 10.0);
}

#[test]
fn an_inline_publisher_leaves_the_queue_wait_empty() {
    // Nothing observes it but the threaded drain, so a fresh registry renders
    // the family at zero.
    let metrics = metrics();
    let rendered = metrics.render();
    assert!(rendered.contains("# TYPE dz_publisher_ingress_queue_wait_seconds histogram"));
    let count = rendered
        .lines()
        .find(|line| line.starts_with("dz_publisher_ingress_queue_wait_seconds_count"))
        .expect("pre-created");
    assert!(count.ends_with(" 0"), "{count}");
}

#[test]
fn shutdown_applies_what_was_queued_before_the_teardown() {
    let bench = Bench::new();
    let payloads: Vec<String> = (1..=20).map(|key| format!("t:3:{key}")).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let (mut connections, mut drain) = bench.spawn(vec![script], threaded::QUEUE_DEPTH);
    let mut out = Applied::default();
    bench.publisher_loop.block_on(async {
        until("all twenty queued", || {
            progress.exhausted.load(Ordering::SeqCst) && drain.queued() == 20
        })
        .await;
        assert!(out.events.is_empty(), "nothing has been applied yet");
        let held = connections.stop().await;
        assert!(held.is_empty(), "nothing waited behind a queue with room");
        assert_eq!(
            drain
                .finish(held, &bench.adapter, &mut out, threaded::DRAIN_DEADLINE)
                .await,
            0,
            "the queue did not drain to its end"
        );
    });
    assert_eq!(out.events.len(), 20);
}

#[test]
fn the_inline_helper_emits_what_the_threaded_path_emits() {
    // One connection, so the threaded path's order is the script's and the two
    // can be compared event for event: duplicates, a stale key and a repeated
    // trade included.
    let script = [
        "q:1:1 t:1:100",
        "q:1:1",
        "q:1:3 t:1:100",
        "q:1:2 t:1:101",
        "q:2:5",
        "q:1:4 q:2:5 t:2:7",
        "t:2:7 q:2:6",
    ];

    let bench = Bench::new();
    let (threaded_script, progress) = Script::new("a", &script);
    let threaded_out = run_to_the_end(&bench, vec![threaded_script], vec![progress]);

    let mut adapter = Fixture::default();
    let mut inline_out = Applied::default();
    for payload in script {
        adapter
            .on_payload(
                &Payload {
                    bytes: payload.as_bytes(),
                    recv_ts_ns: 1,
                    connection: ConnectionId::new("a"),
                },
                &mut inline_out,
            )
            .expect("parses");
    }
    assert_eq!(threaded_out.events, inline_out.events);
    assert_eq!(threaded_out.events.len(), 8);
}

#[test]
fn a_connection_whose_failure_is_fatal_is_named_when_it_gives_up() {
    struct Refused;
    impl Input for Refused {
        fn connection(&self) -> ConnectionId {
            ConnectionId::new("a")
        }
        fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
            Box::pin(std::future::ready(Err(IngressError::fatal(
                "no such endpoint",
            ))))
        }
        fn send<'a>(
            &'a mut self,
            _message: UpstreamMessage<'a>,
        ) -> BoxFuture<'a, Result<(), IngressError>> {
            Box::pin(std::future::ready(Ok(())))
        }
        fn recv<'a>(
            &'a mut self,
            _budget: Option<Duration>,
        ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
            Box::pin(std::future::pending())
        }
        fn shutdown(&mut self) -> BoxFuture<'_, ()> {
            Box::pin(std::future::ready(()))
        }
    }

    let bench = Bench::new();
    let _entered = bench.publisher_loop.enter();
    let (mut connections, _drain) = threaded::spawn(
        bench.workers.handle(),
        vec![(Box::new(Refused) as Box<dyn Input>, true)],
        &bench.adapter,
        Arc::new(SystemClock::new()),
        Arc::new(MetricsObserver::new(Arc::clone(&bench.metrics))),
        policy(),
        Arc::clone(&bench.metrics),
        4,
    );
    let (connection, error) = bench
        .publisher_loop
        .block_on(connections.first_fatal_to_give_up(|_, _| panic!("it ends the process")));
    assert_eq!(connection, "a");
    assert!(error.is_fatal());
    // And a stopped set with a finished run in it stops cleanly.
    bench.publisher_loop.block_on(connections.stop());
}

#[test]
fn the_default_worker_count_is_one_per_connection_and_never_zero() {
    assert_eq!(threaded::default_decode_threads(0).get(), 1);
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let one = threaded::default_decode_threads(1).get();
    assert_eq!(one, 1);
    assert!(threaded::default_decode_threads(64).get() <= cores.max(1));
}

#[test]
fn the_batch_size_histogram_tops_out_at_the_batch_bound() {
    // `BATCH` is the one constant setting how many events a batch takes; the
    // metrics crate cannot depend on this one, so the bucket it states is held
    // to it here.
    assert_eq!(
        dz_publisher_metrics::INGRESS_BATCH_SIZE_BUCKETS.last(),
        Some(&(threaded::BATCH as f64))
    );
}

#[test]
fn shutdown_with_a_full_queue_publishes_the_held_overflow_too() {
    // A queue of four and five trades: four queued, the fifth held on the
    // connection behind them, waiting for room. Shutdown must publish all
    // five, the held one last, rather than drop what never reached the queue.
    let bench = Bench::new();
    let payloads: Vec<String> = (1..=5).map(|id| format!("t:1:{id}")).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let (mut connections, mut drain) = bench.spawn(vec![script], 4);
    let mut out = Applied::default();
    bench.publisher_loop.block_on(async {
        until("four queued and the fifth read", || {
            drain.queued() == 4 && progress.delivered.load(Ordering::SeqCst) == 5
        })
        .await;
        let held = connections.stop().await;
        assert_eq!(held.len(), 1, "the fifth waited behind the full queue");
        assert_eq!(
            drain
                .finish(held, &bench.adapter, &mut out, threaded::DRAIN_DEADLINE)
                .await,
            0,
            "the drain reported everything published"
        );
    });
    let ids: Vec<u64> = out.events.iter().map(|event| key(event).2).collect();
    assert_eq!(
        ids,
        [1, 2, 3, 4, 5],
        "an event held on the connection was lost"
    );
}

#[test]
fn a_drain_cut_off_by_its_deadline_counts_what_it_left() {
    // The same full queue and held fifth, with no time to publish anything
    // beyond what is already in hand: what the deadline leaves is returned as
    // a count for the caller to name, never dropped silently.
    let bench = Bench::new();
    let payloads: Vec<String> = (1..=5).map(|id| format!("t:1:{id}")).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let (mut connections, mut drain) = bench.spawn(vec![script], 4);
    let mut out = Applied::default();
    let unpublished = bench.publisher_loop.block_on(async {
        until("four queued and the fifth read", || {
            drain.queued() == 4 && progress.delivered.load(Ordering::SeqCst) == 5
        })
        .await;
        let held = connections.stop().await;
        drain
            .finish(held, &bench.adapter, &mut out, Duration::ZERO)
            .await
    });
    assert_eq!(
        out.events.len() + unpublished,
        5,
        "every event is published or counted"
    );
    assert!(
        unpublished > 0,
        "a zero deadline leaves the held event unpublished, and says so"
    );
}

/// A clock whose steady reading jumps ten seconds every time it is read, so
/// every receive is followed by the driver's upstream poll.
struct Jumping {
    steady_ns: std::sync::atomic::AtomicU64,
}

impl dz_ingress_core::Clock for Jumping {
    fn wall_ns(&self) -> u64 {
        dz_ingress_core::Clock::wall_ns(&SystemClock::new())
    }

    fn steady_ns(&self) -> u64 {
        self.steady_ns.fetch_add(10_000_000_000, Ordering::SeqCst)
    }

    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// The fixture, writing upstream every time it is asked.
#[derive(Default)]
struct Polling(Fixture);

impl ThreadedAdapter for Polling {
    fn decoder(&self, conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder> {
        self.0.decoder(conn, claims)
    }
}

impl Adapter for Polling {
    fn message_types(&self) -> &[&'static str] {
        self.0.message_types()
    }

    fn poll_listings(&mut self, out: &mut dyn ListingSink) {
        self.0.poll_listings(out);
    }

    fn poll_upstream(
        &mut self,
        _conn: ConnectionId,
        out: &mut dyn dz_adapter_core::UpstreamSink,
    ) -> Result<(), dz_adapter_core::AdapterError> {
        out.send_text("outstanding");
        Ok(())
    }

    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        self.0.on_payload(payload, out)
    }
}

/// Two payloads, then nothing; the second upstream send is fatal.
struct FatalSecondSend {
    payloads: VecDeque<Vec<u8>>,
    current: Vec<u8>,
    sends: usize,
    shut: Arc<AtomicBool>,
}

impl Input for FatalSecondSend {
    fn connection(&self) -> ConnectionId {
        ConnectionId::new("a")
    }

    fn connect(&mut self, _timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn send<'a>(
        &'a mut self,
        _message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        self.sends += 1;
        let result = if self.sends < 2 {
            Ok(())
        } else {
            Err(IngressError::fatal("the transport cannot carry it"))
        };
        Box::pin(std::future::ready(result))
    }

    fn recv<'a>(
        &'a mut self,
        _budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        Box::pin(async move {
            match self.payloads.pop_front() {
                Some(payload) => {
                    self.current = payload;
                    Ok(Received::Payload {
                        bytes: &self.current,
                        ts_ns: None,
                    })
                }
                None => std::future::pending().await,
            }
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        self.shut.store(true, Ordering::SeqCst);
        Box::pin(std::future::ready(()))
    }
}

#[test]
fn a_connection_ending_mid_session_hands_its_held_events_to_the_loop_in_order() {
    // A queue of one. The first trade is queued; the second, decoded while the
    // queue is full, is held; the upstream poll after it fails fatally and the
    // connection ends while the publisher carries on. Its held trade must
    // still reach the loop, after the queued one.
    let bench = Bench::new();
    let shut = Arc::new(AtomicBool::new(false));
    let input = FatalSecondSend {
        payloads: ["t:1:1", "t:1:2"]
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect(),
        current: Vec::new(),
        sends: 0,
        shut: Arc::clone(&shut),
    };
    let adapter: SharedThreadedAdapter = Arc::new(Mutex::new(Box::new(Polling::default())));
    let (_connections, mut drain) = {
        let _entered = bench.publisher_loop.enter();
        threaded::spawn(
            bench.workers.handle(),
            vec![(Box::new(input) as Box<dyn Input>, false)],
            &adapter,
            Arc::new(Jumping {
                steady_ns: std::sync::atomic::AtomicU64::new(0),
            }),
            Arc::new(MetricsObserver::new(Arc::clone(&bench.metrics))),
            policy(),
            Arc::clone(&bench.metrics),
            1,
        )
    };
    let mut out = Applied::default();
    bench.publisher_loop.block_on(async {
        until("the connection ended", || shut.load(Ordering::SeqCst)).await;
        // Drain until every connection's hold on the queue is gone.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            match tokio::time::timeout_at(deadline, drain.recv()).await {
                Ok(0) => break,
                Ok(_) => drain.apply(&adapter, &mut out),
                Err(_) => panic!("the connection never let go of the queue"),
            }
        }
    });
    let ids: Vec<u64> = out.events.iter().map(|event| key(event).2).collect();
    assert_eq!(
        ids,
        [1, 2],
        "the held trade was lost when the connection ended"
    );
}

#[test]
fn a_drain_past_its_deadline_stops_taking_queued_batches_and_counts_them() {
    // Only queued events, several batches of them, and a deadline already
    // passed: the drain must not keep publishing ready batches past it, and
    // what it leaves is counted.
    let bench = Bench::new();
    let events = 3 * threaded::BATCH;
    let payloads: Vec<String> = (1..=events).map(|id| format!("t:1:{id}")).collect();
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let (mut connections, mut drain) = bench.spawn(vec![script], threaded::QUEUE_DEPTH);
    let mut out = Applied::default();
    let unpublished = bench.publisher_loop.block_on(async {
        until("every trade queued", || {
            progress.exhausted.load(Ordering::SeqCst) && drain.queued() == events
        })
        .await;
        let held = connections.stop().await;
        assert!(held.is_empty());
        drain
            .finish(held, &bench.adapter, &mut out, Duration::ZERO)
            .await
    });
    assert_eq!(
        unpublished, events,
        "a ready batch was published past the deadline"
    );
    assert!(out.events.is_empty());
}

#[test]
fn a_trade_copy_later_than_the_identity_window_is_dropped_and_counted_once() {
    // Trades 1 to 257 on one instrument, then trade 1 again: the window has
    // let it go, so it is late. It is not published a second time, and it is
    // counted once. One connection, so the order is the script's.
    let bench = Bench::new();
    let window = dz_adapter_core::threaded::IDENTITY_WINDOW as u64;
    let mut payloads: Vec<String> = (1..=window + 1).map(|id| format!("t:1:{id}")).collect();
    payloads.push("t:1:1".to_string());
    let payloads: Vec<&str> = payloads.iter().map(String::as_str).collect();
    let (script, progress) = Script::new("a", &payloads);
    let out = run_to_the_end(&bench, vec![script], vec![progress]);
    assert_eq!(
        out.events.len() as u64,
        window + 1,
        "a late copy was published again"
    );
    assert_eq!(
        bench.rendered("dz_publisher_ingress_late_identities_total"),
        1.0
    );
    assert_eq!(bench.rendered("dz_publisher_ingress_duplicates_total"), 0.0);
}

#[test]
fn a_claim_lost_to_a_newer_generation_is_counted() {
    // View 1 saw 500,000 and never sees the restart view 0 judges. Its rising
    // 500,001 loses to the newer generation and is counted; its falling
    // 10,002 adopts it and is published.
    let bench = Bench::new();
    let (script, progress) = Script::new(
        "a",
        &[
            "g:0:1:500000",
            "g:1:1:500000",
            "r:0:1:1",
            "g:1:1:500001",
            "g:1:1:10002",
        ],
    );
    let out = run_to_the_end(&bench, vec![script], vec![progress]);
    assert_eq!(out.events.len(), 3, "500,000, 1 and 10,002");
    assert_eq!(
        bench.rendered("dz_publisher_ingress_claims_lost_to_newer_generation_total"),
        1.0
    );
}
