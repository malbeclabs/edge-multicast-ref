//! The threaded input model: each connection decodes on its own task, and the
//! publisher loop applies what survives.
//!
//! The inline model drives every connection and the tick as futures in one
//! task — see [`crate::run`]'s module note — and that stays the default. A venue
//! composed with [`Venue::threaded`](crate::Venue::threaded) is driven like
//! this instead:
//!
//! ```text
//! conn task 1 ─ Driver ─ Decoder ─┐
//! conn task 2 ─ Driver ─ Decoder ─┼─ bounded queue ─ publisher loop ─ on_decoded ─ Publisher ─ egress
//! conn task N ─ Driver ─ Decoder ─┘                  (batches of 32, one flush per batch)
//! ```
//!
//! # Per connection, nothing new
//!
//! Each connection runs today's [`Driver`], unchanged, on a worker thread of a
//! second, multi-thread runtime. It is handed a delegate adapter whose
//! `on_payload` calls that connection's [`Decoder`], and whose connect,
//! disconnect and upstream-poll callbacks lock the shared adapter — rare, and
//! now lightly contended. So keepalive, backoff and the attempt spread,
//! `connection_state`, reconnect reasons and parse errors all stay per
//! connection with no new code.
//!
//! # The publisher stays where it is
//!
//! `!Send`, on the current-thread runtime. The loop takes up to [`BATCH`]
//! events at a time off a queue of [`QUEUE_DEPTH`], applies the loop stage of
//! the dedupe, calls [`ThreadedAdapter::on_decoded`] for each survivor, and
//! flushes once per batch. The tick runs between batches, so it is not starved
//! by a connection that stays ready.
//!
//! # Backpressure, never a drop
//!
//! A full queue makes the connection wait: the events its payload produced are
//! held, and the next receive does not start until they are queued. The
//! connection stops reading, the transport pushes back, and a venue that gives
//! up on a slow reader is a reconnect with a reason. Nothing is dropped on a
//! full queue.
//!
//! # Timestamps
//!
//! A payload's receive stamp is taken where it always is, on the connection, so
//! `venue_to_recv` excludes the queue and `recv_to_send` includes it.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use dz_adapter_core::threaded::{carried, Dedupe};
use dz_adapter_core::{
    Adapter, AdapterError, ConnectionId, Decoded, DecodedSink, Decoder, DepthBound, Desync,
    DisconnectReason, Event, EventSink, InstrumentRef, ListingSink, Order, ParseError, Payload,
    SnapshotSink, ThreadedAdapter, UpstreamSink, VenueTimestampKind,
};
use dz_ingress_core::{
    BoxFuture, Driver, IngressError, IngressObserver, Input, Policy, Received, StampSource,
    UpstreamMessage,
};
use dz_publisher_metrics::PublisherMetrics;
use dz_publisher_refdata::StateStore;
use dz_venue_composition::SharedThreadedAdapter;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::config::Config;
use crate::error::StartupError;
use crate::guard::{Exit, Inconsistency};
use crate::observer::MetricsObserver;
use crate::publisher::Publisher;
use crate::run::{
    fatal_ends_the_process, poll_first_fatal_run_to_give_up, signalled, SharedSink, Ticker, TICK,
};

/// How many decoded events may wait between the connections and the loop.
///
/// A constant and not a key, as in the publisher this design is drawn from: it
/// is the slack that absorbs a burst, and a full queue is backpressure rather
/// than loss, so the number decides how long a burst may be before the
/// connections slow down — not whether anything is dropped.
pub const QUEUE_DEPTH: usize = 65_536;

/// The most events the loop takes off the queue before it flushes.
///
/// One flush per batch is the point: the inline model flushes when its input
/// drains, which a connection that never stops being ready never does.
pub const BATCH: usize = 32;

/// How long shutdown waits for the queue to drain before the teardown runs.
pub const DRAIN_DEADLINE: Duration = Duration::from_secs(1);

/// What the loop tells the publisher's idle guard when a connection has
/// recognised upstream messages since it last asked. The publisher reads the
/// call and not the name; the per-type count is taken on the connection.
const UPSTREAM: &str = "threaded";

/// The worker count when `[ingress] decode_threads` is absent: one per
/// connection, and never more than this host has cores beside the loop's own.
#[must_use]
pub fn default_decode_threads(connections: usize) -> NonZeroUsize {
    let spare = std::thread::available_parallelism().map_or(1, |cores| cores.get() - 1);
    NonZeroUsize::new(connections.min(spare)).unwrap_or(NonZeroUsize::MIN)
}

fn lock<T: ?Sized>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// One connection's way onto the queue.
///
/// The events a payload produced are offered without waiting; whatever the
/// queue cannot take is held here, in order, and [`QueuedInput`] waits for it
/// to be taken before it reads again. Held in a lock only because the delegate
/// adapter and the input are two values the driver borrows separately: both
/// are on this connection's task, so it is never contended.
#[derive(Clone)]
struct Outbox {
    queue: mpsc::Sender<Decoded>,
    held: Arc<Mutex<VecDeque<Decoded>>>,
}

impl Outbox {
    fn push(&self, decoded: Decoded) {
        let mut held = lock(&self.held);
        if !held.is_empty() {
            // Behind what is already waiting, so one connection's events stay
            // in the order it pushed them.
            held.push_back(decoded);
            return;
        }
        match self.queue.try_send(decoded) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(decoded)) => held.push_back(decoded),
            // The loop is gone, which is shutdown: nothing will apply it.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
        }
    }

    /// Wait until everything held is on the queue.
    async fn flush(&self) {
        loop {
            let empty = lock(&self.held).is_empty();
            if empty {
                return;
            }
            // A slot first, then the event: a wait that is abandoned leaves
            // the event held rather than lost.
            let Ok(slot) = self.queue.reserve().await else {
                lock(&self.held).clear();
                return;
            };
            let next = lock(&self.held).pop_front();
            if let Some(decoded) = next {
                slot.send(decoded);
            }
        }
    }
}

/// The connection's transport, which will not read while events it produced
/// are still waiting for the queue.
///
/// This is where backpressure becomes a wait. [`Decoder::decode`] is
/// synchronous and cannot await a full queue, so the events it could not
/// queue are held and the *next* receive, or the next connect, waits first.
struct QueuedInput {
    inner: Box<dyn Input>,
    outbox: Outbox,
}

impl Input for QueuedInput {
    fn connection(&self) -> ConnectionId {
        self.inner.connection()
    }

    fn stamp_source(&self) -> StampSource {
        self.inner.stamp_source()
    }

    fn connect(&mut self, timeout: Duration) -> BoxFuture<'_, Result<(), IngressError>> {
        let Self { inner, outbox } = self;
        Box::pin(async move {
            outbox.flush().await;
            inner.connect(timeout).await
        })
    }

    fn send<'a>(
        &'a mut self,
        message: UpstreamMessage<'a>,
    ) -> BoxFuture<'a, Result<(), IngressError>> {
        self.inner.send(message)
    }

    fn recv<'a>(
        &'a mut self,
        budget: Option<Duration>,
    ) -> BoxFuture<'a, Result<Received<'a>, IngressError>> {
        let Self { inner, outbox } = self;
        Box::pin(async move {
            outbox.flush().await;
            inner.recv(budget).await
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        self.inner.shutdown()
    }
}

/// The adapter one connection's driver holds: its own decoder for payloads, the
/// shared adapter for everything else.
///
/// Every method is written out, for the reason the inline delegate's are: a
/// forwarder that inherits a default answers for the venue with the default.
struct ConnectionAdapter<O> {
    shared: SharedThreadedAdapter,
    decoder: Box<dyn Decoder>,
    message_types: Vec<&'static str>,
    outbox: Outbox,
    observer: Arc<O>,
}

impl<O> ConnectionAdapter<O> {
    fn held(&self) -> MutexGuard<'_, Box<dyn ThreadedAdapter>> {
        lock(&self.shared)
    }
}

impl<O: IngressObserver + Send + Sync> Adapter for ConnectionAdapter<O> {
    fn message_types(&self) -> &[&'static str] {
        &self.message_types
    }

    fn source_timestamp_kind(&self) -> Option<VenueTimestampKind> {
        self.held().source_timestamp_kind()
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

    /// The decode, on this connection's task, with no lock taken.
    fn on_payload(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn EventSink,
    ) -> Result<(), ParseError> {
        let mut sink = ConnectionSink {
            outbox: &self.outbox,
            out,
            observer: &*self.observer,
            connection: payload.connection,
            recv_ts_ns: payload.recv_ts_ns,
            refused: None,
        };
        self.decoder.decode(payload, &mut sink)?;
        sink.refused.map_or(Ok(()), Err)
    }

    fn snapshot(
        &self,
        instrument: InstrumentRef,
        out: &mut dyn SnapshotSink,
    ) -> Result<DepthBound, AdapterError> {
        self.held().snapshot(instrument, out)
    }
}

/// What a decoder writes into, for one payload.
struct ConnectionSink<'a, O> {
    outbox: &'a Outbox,
    /// The driver's own sink for this payload, which counts the message type
    /// under this connection's name.
    out: &'a mut dyn EventSink,
    observer: &'a O,
    connection: ConnectionId,
    recv_ts_ns: u64,
    refused: Option<ParseError>,
}

impl<O: IngressObserver> DecodedSink for ConnectionSink<'_, O> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.out.upstream_message(message_type);
    }

    fn push(&mut self, event: Event<'static>, order: Order) {
        // Refused here rather than on the loop, so the parse error is counted
        // against the payload that produced it.
        if let Err(error) = carried(&event) {
            self.refused.get_or_insert(error);
            return;
        }
        self.outbox.push(Decoded {
            event,
            order,
            connection: self.connection,
            recv_ts_ns: self.recv_ts_ns,
        });
    }

    /// The connection stage's duplicates, counted where they are found.
    fn duplicate(&mut self) {
        self.observer.duplicate();
    }
}

/// The sink a connection's driver runs against.
///
/// It sees only what the driver itself states: the decoder writes to the
/// queue, so no event reaches here. What does is the message boundary, which
/// is the upstream half of the publisher's idle guard, and that is carried to
/// the loop as a count.
struct QueueSink {
    upstream: Arc<AtomicU64>,
}

impl EventSink for QueueSink {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.upstream.fetch_add(1, Ordering::Relaxed);
    }

    /// Unreachable: the delegate adapter emits through the queue.
    fn event(&mut self, _event: Event<'_>) {}

    /// The stamp travels with each event, in [`Decoded::recv_ts_ns`].
    fn payload_scope(&mut self, _recv_ts_ns: Option<u64>) {}

    /// The loop flushes once per batch instead.
    fn drained(&mut self) {}

    /// Unreachable: a decoder has no way to say it.
    fn desynchronised(&mut self, _instrument: InstrumentRef, _reason: Desync) {}
}

/// One connection task's result, as a future the loop can poll beside the
/// others.
///
/// A panic on the connection is resumed on the loop, so it ends the process
/// as the same panic would inline. A task stopped by [`Connections::stop`]
/// never resolves.
struct Joined(Option<JoinHandle<(&'static str, IngressError)>>);

impl Future for Joined {
    type Output = (&'static str, IngressError);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(handle) = self.0.as_mut() else {
            return Poll::Pending;
        };
        let Poll::Ready(joined) = Pin::new(handle).poll(cx) else {
            return Poll::Pending;
        };
        self.0 = None;
        match joined {
            Ok(ended) => Poll::Ready(ended),
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Poll::Pending,
        }
    }
}

/// The connection tasks.
pub struct Connections {
    runs: Vec<(bool, Joined)>,
    aborts: Vec<tokio::task::AbortHandle>,
}

impl Connections {
    /// The first connection **whose failure is fatal** to give up, as the
    /// inline model reports it; a connection whose failure is not is passed to
    /// `report` and dropped from the set. See the inline model's own rule in
    /// [`crate::run`].
    pub fn first_fatal_to_give_up<'a>(
        &'a mut self,
        mut report: impl FnMut(&'static str, &IngressError) + 'a,
    ) -> impl Future<Output = (&'static str, IngressError)> + 'a {
        std::future::poll_fn(move |cx| {
            poll_first_fatal_run_to_give_up(&mut self.runs, cx, &mut report)
        })
    }

    /// Stop every connection and wait until each has.
    ///
    /// Through the path the inline model's drivers end by: the driver's
    /// future is dropped where it stands, and with it the connection's
    /// transport and its hold on the queue. So once this returns, nothing more
    /// can arrive, and draining the queue to empty is draining it to the end.
    pub async fn stop(&mut self) {
        for abort in &self.aborts {
            abort.abort();
        }
        for (_, run) in &mut self.runs {
            if let Some(handle) = run.0.take() {
                if let Err(error) = handle.await {
                    if error.is_panic() {
                        std::panic::resume_unwind(error.into_panic());
                    }
                }
            }
        }
    }
}

/// The loop's half: the queue, and the dedupe's loop stage.
pub struct Drain {
    queue: mpsc::Receiver<Decoded>,
    batch: Vec<Decoded>,
    dedupe: Dedupe,
    upstream: Arc<AtomicU64>,
    seen_upstream: u64,
    metrics: Arc<PublisherMetrics>,
}

impl Drain {
    /// Wait for the next batch, of at most [`BATCH`] events. `0` means every
    /// connection has ended and the queue is empty.
    ///
    /// Cancel-safe, so it can sit in a `select!` beside the tick.
    pub async fn recv(&mut self) -> usize {
        let taken = self.queue.recv_many(&mut self.batch, BATCH).await;
        if taken > 0 {
            self.metrics.ingress().batch(taken);
            self.metrics.ingress().set_queue_depth(self.queue.len());
        }
        taken
    }

    /// How many events are waiting on the queue.
    #[must_use]
    pub fn queued(&self) -> usize {
        self.queue.len()
    }

    /// Tell `out` about upstream messages recognised since it was last told.
    ///
    /// The idle guard's upstream half. Counted on the connections, read here,
    /// at every batch and every tick: a connection whose every copy is a
    /// duplicate puts nothing on the queue and is still an upstream that is
    /// talking.
    pub fn upstream(&mut self, out: &mut dyn EventSink) {
        let now = self.upstream.load(Ordering::Relaxed);
        if now != self.seen_upstream {
            self.seen_upstream = now;
            out.upstream_message(UPSTREAM);
        }
    }

    /// Apply the batch [`recv`](Self::recv) took, and flush once.
    ///
    /// The loop stage of the dedupe runs here, and is the authority: two
    /// claims can be queued in the other order, so a `Supersedes` key is
    /// applied only above the last one applied for its instrument, and an
    /// `Identity` key only once inside its window. Each survivor reaches
    /// [`ThreadedAdapter::on_decoded`] under its own payload's receive stamp,
    /// with the shared adapter locked once for the batch.
    pub fn apply(&mut self, adapter: &SharedThreadedAdapter, out: &mut dyn EventSink) {
        self.upstream(out);
        if self.batch.is_empty() {
            return;
        }
        let mut held = lock(adapter);
        let mut scope = None;
        for decoded in self.batch.drain(..) {
            // Checked again only to name the instrument: a refused event never
            // reached the queue.
            let Ok(instrument) = carried(&decoded.event) else {
                continue;
            };
            if !self.dedupe.admit(instrument, decoded.order) {
                self.metrics.ingress().duplicate();
                continue;
            }
            if scope != Some(decoded.recv_ts_ns) {
                scope = Some(decoded.recv_ts_ns);
                out.payload_scope(scope);
            }
            held.on_decoded(&decoded, &mut Applied(&mut *out));
        }
        if scope.is_some() {
            out.payload_scope(None);
        }
        out.drained();
    }

    /// Apply everything still queued, until every connection's hold on the
    /// queue is gone and it is empty, or `deadline` passes. `true` when it
    /// drained to the end.
    ///
    /// For shutdown, after [`Connections::stop`]: what was received before the
    /// signal is published before the teardown runs.
    pub async fn finish(
        &mut self,
        adapter: &SharedThreadedAdapter,
        out: &mut dyn EventSink,
        deadline: Duration,
    ) -> bool {
        let until = tokio::time::Instant::now() + deadline;
        loop {
            match tokio::time::timeout_at(until, self.recv()).await {
                Ok(0) => return true,
                Ok(_) => self.apply(adapter, out),
                Err(_) => return false,
            }
        }
    }
}

/// The sink `on_decoded` writes into: the publisher's, without the two calls
/// that are the loop's own.
struct Applied<'a>(&'a mut dyn EventSink);

impl EventSink for Applied<'_> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.0.upstream_message(message_type);
    }

    fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) {
        self.0.upstream_identity(sid, seq);
    }

    fn event(&mut self, event: Event<'_>) {
        self.0.event(event);
    }

    /// Not forwarded: the stamp is the payload's, stated by the loop.
    fn payload_scope(&mut self, _recv_ts_ns: Option<u64>) {}

    /// Not forwarded: the loop flushes once per batch.
    fn drained(&mut self) {}

    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        self.0.desynchronised(instrument, reason);
    }
}

/// Start one task per connection on `workers`, and hand back the two halves.
///
/// Each input is paired with whether a fatal error on it ends the process.
/// Every connection's decoder is built here, once, from the shared adapter,
/// with one [`ClaimTable`](dz_adapter_core::ClaimTable) shared among them,
/// and is kept across reconnects. `depth` is [`QUEUE_DEPTH`] in a running
/// publisher; a test states a smaller one.
#[allow(clippy::too_many_arguments)]
pub fn spawn<K, O>(
    workers: &tokio::runtime::Handle,
    inputs: Vec<(Box<dyn Input>, bool)>,
    adapter: &SharedThreadedAdapter,
    clock: Arc<K>,
    observer: Arc<O>,
    policy: Policy,
    metrics: Arc<PublisherMetrics>,
    depth: usize,
) -> (Connections, Drain)
where
    K: dz_ingress_core::Clock + 'static,
    O: IngressObserver + Send + Sync + 'static,
{
    let (queue, receiver) = mpsc::channel(depth.max(1));
    let upstream = Arc::new(AtomicU64::new(0));
    let claims = dz_adapter_core::ClaimTable::new();
    let message_types = lock(adapter).message_types().to_vec();
    let mut runs = Vec::with_capacity(inputs.len());
    let mut aborts = Vec::with_capacity(inputs.len());
    for (input, ends_the_process) in inputs {
        let connection = input.connection();
        let outbox = Outbox {
            queue: queue.clone(),
            held: Arc::new(Mutex::new(VecDeque::new())),
        };
        let decoder = lock(adapter).decoder(connection, claims.clone());
        let mut delegate = ConnectionAdapter {
            shared: Arc::clone(adapter),
            decoder,
            message_types: message_types.clone(),
            outbox: outbox.clone(),
            observer: Arc::clone(&observer),
        };
        let mut transport = QueuedInput {
            inner: input,
            outbox,
        };
        let mut sink = QueueSink {
            upstream: Arc::clone(&upstream),
        };
        let clock = Arc::clone(&clock);
        let observer = Arc::clone(&observer);
        let handle = workers.spawn(async move {
            let mut driver =
                Driver::new(&mut transport, &mut delegate, &*clock, &*observer, policy);
            (connection.as_str(), driver.run(&mut sink).await)
        });
        aborts.push(handle.abort_handle());
        runs.push((ends_the_process, Joined(Some(handle))));
    }
    (
        Connections { runs, aborts },
        Drain {
            queue: receiver,
            batch: Vec::with_capacity(BATCH),
            dedupe: Dedupe::new(),
            upstream,
            seen_upstream: 0,
            metrics,
        },
    )
}

/// The threaded model's whole run, from the first connect to the drained
/// queue, for [`crate::run`] to tear down after.
///
/// # Errors
///
/// [`StartupError::Runtime`] when the worker runtime will not start.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run<S: StateStore, K: Clock + Clone + 'static>(
    runtime: &tokio::runtime::Runtime,
    publisher: &RefCell<Publisher<S, K>>,
    adapter: &Arc<Mutex<Box<dyn Adapter>>>,
    shared: &SharedThreadedAdapter,
    inputs: Vec<Box<dyn Input>>,
    clock: &K,
    observer: &Arc<MetricsObserver>,
    config: &Config,
    metrics: &Arc<PublisherMetrics>,
) -> Result<Exit, StartupError> {
    let threads = config
        .decode_threads
        .unwrap_or_else(|| default_decode_threads(inputs.len()));
    let workers = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads.get())
        .thread_name("dz-decode")
        .enable_all()
        .build()
        .map_err(|source| StartupError::Runtime { source })?;
    eprintln!(
        "threaded input: {} connection(s) decode on {threads} worker thread(s)",
        inputs.len()
    );
    let inputs: Vec<(Box<dyn Input>, bool)> = inputs
        .into_iter()
        .map(|input| {
            let ends = fatal_ends_the_process(&config.sources, input.connection());
            (input, ends)
        })
        .collect();

    let exit = runtime.block_on(async {
        let (mut connections, mut drain) = spawn(
            workers.handle(),
            inputs,
            shared,
            Arc::new(clock.clone()),
            Arc::clone(observer),
            config.ingress,
            Arc::clone(metrics),
            QUEUE_DEPTH,
        );
        let exit = {
            let mut ticker = Ticker::new();
            let drain = &mut drain;
            let publishing = async move {
                let mut tick = clock.sleep(TICK);
                let mut open = true;
                loop {
                    tokio::select! {
                        // The tick first when it is due: it runs between
                        // batches, and a connection that stays ready cannot
                        // hold it off.
                        biased;
                        () = &mut tick => {
                            tick = clock.sleep(TICK);
                            drain.upstream(&mut SharedSink(publisher));
                            if let Some(exit) = ticker.tick(publisher, adapter) {
                                return exit;
                            }
                        }
                        taken = drain.recv(), if open => {
                            if taken == 0 {
                                // Every connection has ended, and only one
                                // whose failure is not fatal can end without
                                // ending the process: the tick carries on.
                                open = false;
                            } else {
                                drain.apply(shared, &mut SharedSink(publisher));
                            }
                        }
                    }
                }
            };
            let first_to_give_up = connections.first_fatal_to_give_up(|connection, error| {
                eprintln!(
                    "`{connection}` gave up. It is neither the primary nor an upstream \
                     partition, so this publisher carries on without it. Nothing retries it: \
                     its connection_state stays at 0 until this process is restarted, which is \
                     what retries it. {error}"
                );
            });
            tokio::select! {
                (connection, error) = first_to_give_up => Exit::ConsistencyGuard(
                    Inconsistency::UpstreamUnusable {
                        detail: format!("`{connection}`: {error}"),
                    },
                ),
                exit = publishing => exit,
                () = signalled() => Exit::Signal,
            }
        };
        // The connections stop first, so nothing more arrives; then what they
        // had queued is published; then the teardown, as the inline model's.
        connections.stop().await;
        if !drain
            .finish(shared, &mut SharedSink(publisher), DRAIN_DEADLINE)
            .await
        {
            eprintln!(
                "dz-publisher-runtime: {} decoded event(s) were still queued after {:?}, and \
                 are not published",
                drain.queued(),
                DRAIN_DEADLINE
            );
        }
        exit
    });
    workers.shutdown_timeout(DRAIN_DEADLINE);
    Ok(exit)
}
