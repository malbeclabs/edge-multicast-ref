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
//! conn task N ─ Driver ─ Decoder ─┘                  (batches of BATCH, one flush per batch)
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
//! `dz_publisher_ingress_queue_wait_seconds` is the part the queue accounts
//! for: from that stamp to the moment the loop takes the event off the queue.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use dz_adapter_core::threaded::{carried, Admission, Dedupe};
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

/// The most events the loop takes off the queue in one `recv_many`, and so the
/// bound on events per flush.
///
/// One `recv_many` is one batch: it waits only while the queue is empty, then
/// takes what is already there, up to this, and never waits for more. The
/// batch's survivors are packed and every partly filled datagram is sent at its
/// end, so nothing is held past it. The inline model flushes when its input
/// drains instead, which a connection that never stops being ready never does.
///
/// The only constant that sets the batch. `dz_publisher_ingress_batch_size`'s
/// top bucket is held equal to it by a test.
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

    /// Release the transport, then wait until everything this connection
    /// held is on the queue.
    ///
    /// The driver calls this on every ending — a reconnect, and the fatal one
    /// that ends the driver mid-session, for a connection the publisher
    /// carries on without. Held events would otherwise wait for the next
    /// receive that never comes, and be dropped with the task. The transport
    /// goes first, so the peer is not kept waiting on a queue it cannot see;
    /// the wait is the backpressure every receive already applies.
    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        let Self { inner, outbox } = self;
        Box::pin(async move {
            inner.shutdown().await;
            outbox.flush().await;
        })
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

    fn lost_to_newer_generation(&mut self) {
        self.observer.lost_to_newer_generation();
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
    /// Each connection's held events, shared with its task so that they
    /// outlive it: stopping a connection must not destroy what it decoded and
    /// could not yet queue.
    held: Vec<Arc<Mutex<VecDeque<Decoded>>>>,
}

/// What the connections had decoded and could not queue when they stopped,
/// one run per connection, each in the order it was pushed.
///
/// Handed from [`Connections::stop`] to [`Drain::finish`], which publishes it
/// after everything already queued: a connection's events reach its held run
/// only once the queue is full, so its queued events come first.
#[derive(Debug, Default)]
pub struct Held {
    per_connection: Vec<VecDeque<Decoded>>,
}

impl Held {
    /// How many events are held, across every connection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.per_connection.iter().map(VecDeque::len).sum()
    }

    /// Whether nothing is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
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

    /// Stop every connection, wait until each has, and hand back what each had
    /// decoded and not yet queued.
    ///
    /// Through the path the inline model's drivers end by: the driver's
    /// future is dropped where it stands, and with it the connection's
    /// transport and its hold on the queue. A connection stops only at an
    /// await, never inside a decode, so every payload it read is decoded and
    /// its events are on the queue or in [`Held`]. Once this returns nothing
    /// more can arrive: the queue closes when it is empty, and the held runs
    /// are all that is left.
    pub async fn stop(&mut self) -> Held {
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
        Held {
            per_connection: self
                .held
                .iter()
                .map(|held| std::mem::take(&mut *lock(held)))
                .collect(),
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
    /// Read once per batch, for the queue wait.
    clock: Arc<dyn dz_ingress_core::Clock>,
}

impl Drain {
    /// Wait for the next batch, of at most [`BATCH`] events. `0` means every
    /// connection has ended and the queue is empty.
    ///
    /// Cancel-safe, so it can sit in a `select!` beside the tick.
    pub async fn recv(&mut self) -> usize {
        let taken = self.queue.recv_many(&mut self.batch, BATCH).await;
        if taken > 0 {
            let ingress = self.metrics.ingress();
            ingress.batch(taken);
            ingress.set_queue_depth(self.queue.len());
            // Against the wall clock, which is the pairing `recv_to_send`
            // uses: `recv_ts_ns` is a wall or a kernel reading, and a
            // monotonic one differenced against it means nothing. Keeping the
            // pairing also keeps the two comparable — the queue wait is the
            // first part of the same interval, measured from the same receipt.
            // Saturating, because a wall clock stepped backwards is not a
            // negative wait.
            let now_ns = self.clock.wall_ns();
            for decoded in &self.batch[self.batch.len() - taken..] {
                let waited = now_ns.saturating_sub(decoded.recv_ts_ns);
                // A nanosecond count as seconds, exact far past any wait.
                #[allow(clippy::cast_precision_loss)]
                ingress.observe_queue_wait(waited as f64 / 1_000_000_000.0);
            }
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
            match self.dedupe.judge(instrument, decoded.order) {
                Admission::Applied => {}
                Admission::Late => {
                    self.metrics.ingress().late_identity();
                    continue;
                }
                _ => {
                    self.metrics.ingress().duplicate();
                    continue;
                }
            }
            if scope != Some(decoded.recv_ts_ns) {
                scope = Some(decoded.recv_ts_ns);
                out.payload_scope(scope);
            }
            let mut applied = Applied {
                out: &mut *out,
                rescoped: false,
            };
            held.on_decoded(&decoded, &mut applied);
            // The adapter scoped what it emitted to another payload, such as
            // an event it held from an earlier one: this one's stamp goes
            // back, so the next event is not measured from that payload.
            if applied.rescoped {
                out.payload_scope(scope);
            }
        }
        if scope.is_some() {
            out.payload_scope(None);
        }
        out.drained();
    }

    /// Publish everything the connections decoded: what is queued, until the
    /// queue is closed and empty, and then what [`Connections::stop`] found
    /// held, connection by connection, in the order each pushed it. Applied in
    /// batches of [`BATCH`], through the dedupe, exactly as a running loop
    /// applies them. Returns how many events `deadline` left unpublished: `0`
    /// is everything.
    ///
    /// For shutdown: what was received before the signal is published before
    /// the teardown runs, and what the deadline cuts off is counted rather
    /// than dropped without a word.
    pub async fn finish(
        &mut self,
        mut held: Held,
        adapter: &SharedThreadedAdapter,
        out: &mut dyn EventSink,
        deadline: Duration,
    ) -> usize {
        let until = tokio::time::Instant::now() + deadline;
        loop {
            // Checked before each batch, and not left to the timeout: a timeout
            // polls what it wraps first, so a queue that stays ready would
            // keep publishing past the deadline.
            if tokio::time::Instant::now() >= until {
                return self.queue.len() + held.len();
            }
            match tokio::time::timeout_at(until, self.recv()).await {
                Ok(0) => break,
                Ok(_) => self.apply(adapter, out),
                Err(_) => return self.queue.len() + held.len(),
            }
        }
        for run in &mut held.per_connection {
            while !run.is_empty() {
                if tokio::time::Instant::now() >= until {
                    return held.len();
                }
                let take = run.len().min(BATCH);
                self.batch.extend(run.drain(..take));
                self.metrics.ingress().batch(take);
                self.apply(adapter, out);
            }
        }
        0
    }
}

/// The sink `on_decoded` writes into: the publisher's, without the flush,
/// which is the loop's own.
struct Applied<'a> {
    out: &'a mut dyn EventSink,
    /// Whether the adapter changed the payload scope, so the loop puts the
    /// event's own back when `on_decoded` returns.
    rescoped: bool,
}

impl EventSink for Applied<'_> {
    fn upstream_message(&mut self, message_type: &'static str) {
        self.out.upstream_message(message_type);
    }

    fn upstream_identity(&mut self, sid: Option<u64>, seq: Option<u64>) {
        self.out.upstream_identity(sid, seq);
    }

    fn event(&mut self, event: Event<'_>) {
        self.out.event(event);
    }

    /// Forwarded, so an event the adapter held from an earlier payload is
    /// measured from that payload's receipt. The loop states each event's own
    /// stamp before `on_decoded` and puts it back after. Under the threaded
    /// model the publisher neither splits nor sends a datagram on a scope.
    fn payload_scope(&mut self, recv_ts_ns: Option<u64>) {
        self.rescoped = true;
        self.out.payload_scope(recv_ts_ns);
    }

    /// Not forwarded: the loop flushes once per batch.
    fn drained(&mut self) {}

    fn desynchronised(&mut self, instrument: InstrumentRef, reason: Desync) {
        self.out.desynchronised(instrument, reason);
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
    let wall: Arc<dyn dz_ingress_core::Clock> = clock.clone();
    let (queue, receiver) = mpsc::channel(depth.max(1));
    let upstream = Arc::new(AtomicU64::new(0));
    let claims = dz_adapter_core::ClaimTable::new();
    let message_types = lock(adapter).message_types().to_vec();
    let mut runs = Vec::with_capacity(inputs.len());
    let mut aborts = Vec::with_capacity(inputs.len());
    let mut held = Vec::with_capacity(inputs.len());
    for (input, ends_the_process) in inputs {
        let connection = input.connection();
        let outbox = Outbox {
            queue: queue.clone(),
            held: Arc::new(Mutex::new(VecDeque::new())),
        };
        held.push(Arc::clone(&outbox.held));
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
        Connections { runs, aborts, held },
        Drain {
            queue: receiver,
            batch: Vec::with_capacity(BATCH),
            dedupe: Dedupe::new(),
            upstream,
            seen_upstream: 0,
            metrics,
            clock: wall,
        },
    )
}

/// The publisher loop: the tick beside the queue, until a tick says to exit.
///
/// `tick` is one tick body and `apply` applies the batch just taken; both are
/// the caller's, so this is the scheduling and nothing else. The tick goes
/// first whenever it is due: it runs between batches, and a connection that
/// stays ready cannot hold it off.
async fn publish(
    drain: &mut Drain,
    clock: &dyn dz_ingress_core::Clock,
    mut tick: impl FnMut(&mut Drain) -> Option<Exit>,
    mut apply: impl FnMut(&mut Drain),
) -> Exit {
    let mut tick_due = clock.sleep(TICK);
    let mut open = true;
    loop {
        tokio::select! {
            biased;
            () = &mut tick_due => {
                if let Some(exit) = tick(drain) {
                    return exit;
                }
                // After the body, as the inline loop sleeps after it. Started
                // before, a body that takes a whole `TICK` — a tick serving
                // snapshots — would leave the next one already due, and the
                // biased select would serve ticks and never the queue.
                tick_due = clock.sleep(TICK);
            }
            taken = drain.recv(), if open => {
                if taken == 0 {
                    // Every connection has ended, and only one whose failure
                    // is not fatal can end without ending the process: the
                    // tick carries on.
                    open = false;
                } else {
                    apply(drain);
                }
            }
        }
    }
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
            let publishing = publish(
                drain,
                clock,
                |drain| {
                    drain.upstream(&mut SharedSink(publisher));
                    ticker.tick(publisher, adapter)
                },
                |drain| drain.apply(shared, &mut SharedSink(publisher)),
            );
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
        // The connections stop first, so nothing more arrives; then, on a
        // signal, what they had queued and held behind a full queue is
        // published; then the teardown, as the inline model's.
        let held = connections.stop().await;
        let outcome = wind_down(
            &exit,
            held,
            &mut drain,
            shared,
            &mut SharedSink(publisher),
            DRAIN_DEADLINE,
        )
        .await;
        if let Some(message) = wind_down_message(&exit, outcome) {
            eprintln!("{message}");
        }
        exit
    });
    workers.shutdown_timeout(DRAIN_DEADLINE);
    Ok(exit)
}

/// What became of the decoded events still queued or held at an exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WoundDown {
    /// A signal: published, all but `unpublished`, which the deadline cut off.
    Drained { unpublished: usize },
    /// A guard exit: none published, and `left` of them dropped.
    Dropped { left: usize },
}

/// Drain the queue at an exit, **only for a signal.**
///
/// A signal is an orderly stop, and what was received before it is published
/// before the teardown. A guard exit is not: the idle guard found nothing
/// reaching the wire, and the consistency guard found the publisher unable to
/// describe itself truthfully, an essential egress dark or its state
/// unrecordable among them. Draining then would publish into exactly that,
/// for up to [`DRAIN_DEADLINE`]. The inline model publishes nothing after any
/// exit; a guard exit here does the same, and counts what it leaves.
pub(crate) async fn wind_down(
    exit: &Exit,
    held: Held,
    drain: &mut Drain,
    shared: &SharedThreadedAdapter,
    out: &mut dyn EventSink,
    deadline: Duration,
) -> WoundDown {
    if matches!(exit, Exit::Signal) {
        WoundDown::Drained {
            unpublished: drain.finish(held, shared, out, deadline).await,
        }
    } else {
        WoundDown::Dropped {
            left: drain.queued() + held.len(),
        }
    }
}

/// The line to log about [`wind_down`]'s outcome, if there is anything to say.
pub(crate) fn wind_down_message(exit: &Exit, outcome: WoundDown) -> Option<String> {
    match outcome {
        WoundDown::Drained { unpublished: 0 } | WoundDown::Dropped { left: 0 } => None,
        WoundDown::Drained { unpublished } => Some(format!(
            "dz-publisher-runtime: shutdown drain deadline: {unpublished} decoded event(s) were \
             still queued or held on their connections after {DRAIN_DEADLINE:?}, and are not \
             published"
        )),
        WoundDown::Dropped { left } => Some(format!(
            "dz-publisher-runtime: exit by {exit}: {left} decoded event(s) still queued or held \
             on their connections are not published, because only a signal drains"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicU64;

    use dz_adapter_core::{InstrumentRef, Scalar, SideUpdate};
    use dz_edge_core::PortRole;
    use dz_publisher_metrics::PublisherMetricsConfig;

    /// A clock whose time moves only when something moves it: a tick body
    /// that takes a whole `TICK`, or a sleep polled before it is due, which
    /// stands in for the loop waiting.
    struct Stepped {
        now_ns: Arc<AtomicU64>,
    }

    impl dz_ingress_core::Clock for Stepped {
        fn wall_ns(&self) -> u64 {
            self.now_ns.load(Ordering::SeqCst)
        }

        fn steady_ns(&self) -> u64 {
            self.now_ns.load(Ordering::SeqCst)
        }

        fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
            let due = self.now_ns.load(Ordering::SeqCst)
                + u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
            let now_ns = Arc::clone(&self.now_ns);
            Box::pin(std::future::poll_fn(move |cx| {
                if now_ns.load(Ordering::SeqCst) >= due {
                    return Poll::Ready(());
                }
                // Waiting: a millisecond passes for every look.
                now_ns.fetch_add(1_000_000, Ordering::SeqCst);
                cx.waker().wake_by_ref();
                Poll::Pending
            }))
        }
    }

    fn drain_with(events: usize) -> (Drain, mpsc::Sender<Decoded>) {
        let metrics = Arc::new(PublisherMetrics::new(&PublisherMetricsConfig {
            venue: "test-venue",
            source_id: 41,
            port_roles: &[PortRole::Mktdata],
            connections: &[],
            channels: &[],
            ingress_message_types: &[],
        }));
        let (queue, receiver) = mpsc::channel(events.max(1));
        for n in 0..events {
            queue
                .try_send(Decoded {
                    event: Event::Quote {
                        instrument: InstrumentRef::from_admission(0),
                        source_ts_ns: 0,
                        bid: SideUpdate::Present {
                            px: Scalar::fixed(n as i64, 0),
                            qty: Scalar::fixed(1, 0),
                            source_count: None,
                        },
                        ask: SideUpdate::Gone,
                    },
                    order: Order::None,
                    connection: ConnectionId::new("a"),
                    recv_ts_ns: 0,
                })
                .expect("room for every event");
        }
        let drain = Drain {
            queue: receiver,
            batch: Vec::with_capacity(BATCH),
            dedupe: Dedupe::new(),
            upstream: Arc::new(AtomicU64::new(0)),
            seen_upstream: 0,
            metrics,
            clock: Arc::new(crate::clock::SystemClock::new()),
        };
        // The sender is handed back so the queue stays open, as it does while
        // a connection lives.
        (drain, queue)
    }

    /// Forwards every decoded event.
    struct Forward;
    impl Adapter for Forward {
        fn message_types(&self) -> &[&'static str] {
            &[]
        }
        fn poll_listings(&mut self, _out: &mut dyn ListingSink) {}
        fn on_payload(
            &mut self,
            _payload: &Payload<'_>,
            _out: &mut dyn EventSink,
        ) -> Result<(), ParseError> {
            Ok(())
        }
    }
    impl ThreadedAdapter for Forward {
        fn decoder(
            &self,
            _conn: ConnectionId,
            _claims: dz_adapter_core::ClaimTable,
        ) -> Box<dyn Decoder> {
            unreachable!("no connection runs here")
        }
    }

    /// Counts what reaches it.
    #[derive(Default)]
    struct Counted(usize);
    impl EventSink for Counted {
        fn upstream_message(&mut self, _message_type: &'static str) {}
        fn event(&mut self, _event: Event<'_>) {
            self.0 += 1;
        }
    }

    fn forward() -> SharedThreadedAdapter {
        Arc::new(Mutex::new(Box::new(Forward)))
    }

    #[test]
    fn a_guard_exit_publishes_nothing_more_and_says_what_it_left() {
        // The guard fired because the publisher can no longer publish
        // truthfully, or at all: draining would publish into that. What was
        // queued is counted and named with the reason instead.
        let (mut drain, _open) = drain_with(5);
        let mut out = Counted::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let exit = Exit::IdleGuard;
        let outcome = runtime.block_on(wind_down(
            &exit,
            Held::default(),
            &mut drain,
            &forward(),
            &mut out,
            DRAIN_DEADLINE,
        ));
        assert_eq!(outcome, WoundDown::Dropped { left: 5 });
        assert_eq!(out.0, 0, "a guard exit published");
        let said = wind_down_message(&exit, outcome).expect("something to say");
        assert!(said.contains("5 decoded event(s)"), "{said}");
        assert!(said.contains("idle guard"), "{said}");
    }

    #[test]
    fn a_signal_exit_still_drains() {
        let (mut drain, open) = drain_with(5);
        drop(open);
        let mut out = Counted::default();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a runtime");
        let outcome = runtime.block_on(wind_down(
            &Exit::Signal,
            Held::default(),
            &mut drain,
            &forward(),
            &mut out,
            DRAIN_DEADLINE,
        ));
        assert_eq!(outcome, WoundDown::Drained { unpublished: 0 });
        assert_eq!(out.0, 5);
        assert_eq!(wind_down_message(&Exit::Signal, outcome), None);
    }

    #[test]
    fn a_tick_body_as_long_as_the_tick_does_not_starve_the_queue() {
        // Every tick body takes a whole `TICK`. If the next tick's timer were
        // started before the body, it would be due the moment the body ended,
        // and the biased select would pick the tick again for ever: the queue
        // would stop draining once the first tick fired. The clock moves a
        // millisecond per wait, so about ten batches drain before that tick:
        // twenty are queued, so the rest have to come between ticks.
        let events = 20 * BATCH;
        let (mut drain, _open) = drain_with(events);
        let clock = Stepped {
            now_ns: Arc::new(AtomicU64::new(0)),
        };
        let tick_ns = u64::try_from(TICK.as_nanos()).expect("small");
        let mut ticks = 0;
        let mut applied = 0;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("a runtime");
        let exit = runtime.block_on(publish(
            &mut drain,
            &clock,
            |_| {
                ticks += 1;
                clock.now_ns.fetch_add(tick_ns, Ordering::SeqCst);
                (ticks == 8).then_some(Exit::Signal)
            },
            |drain| {
                applied += drain.batch.len();
                drain.batch.clear();
            },
        ));
        assert!(matches!(exit, Exit::Signal));
        assert_eq!(applied, events, "the queue was starved by the tick");
    }
}
