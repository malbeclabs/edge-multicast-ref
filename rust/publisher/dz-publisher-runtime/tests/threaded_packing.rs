//! A threaded batch leaves in as few datagrams as fit it, and at once.
//!
//! Driven end to end through the threaded drain into a publisher composed over
//! recorders: connections on real worker threads decode into the queue, the
//! loop takes one batch and applies it, and the assertions read what reached
//! "the wire" back through the codec's own walk.

mod harness;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dz_adapter_core::threaded::Inline;
use dz_adapter_core::{
    Adapter, Aggressor, ClaimTable, ConnectionId, DecodedSink, Decoder, Event, EventSink,
    InstrumentRef, ListingSink, Order, ParseError, Payload, Scalar, SideUpdate, ThreadedAdapter,
    TradeFlags, VenueTimestampKind,
};
use dz_edge_core::{AppMessage, DATAGRAM_HEADER_SIZE, MAX_DATAGRAM_SIZE};
use dz_edge_tob::{Quote, Trade};
use dz_ingress_core::{
    BackoffPolicy, BoxFuture, IngressError, Input, Policy, Received, UpstreamMessage,
};
use dz_publisher_runtime::threaded::{self, Connections, Drain};
use dz_publisher_runtime::{MetricsObserver, SystemClock};
use dz_venue_composition::SharedThreadedAdapter;
use harness::{depth_feed, feed, harness, harness_both, FakeAdapter, Harness};

/// The venue's own stamp on every event the fixture decodes, so that
/// `venue_to_recv` has something to measure.
const SOURCE_TS_NS: u64 = 1_700_000_000_000_000_000;

/// `q:<instrument>:<bid in cents>` a quote, and `t:<instrument>:<trade id>` a
/// trade at one cent per id, so every message on the wire names its place in
/// the script by its price or its id.
#[derive(Default)]
struct Packing {
    inline: Option<Inline>,
}

struct PackingDecoder;

impl Decoder for PackingDecoder {
    fn decode(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn DecodedSink,
    ) -> Result<(), ParseError> {
        let text =
            std::str::from_utf8(payload.bytes).map_err(|_| ParseError::malformed("utf-8"))?;
        let mut fields = text.split(':');
        let kind = fields.next().ok_or(ParseError::truncated("kind"))?;
        let mut number = || {
            fields
                .next()
                .and_then(|field| field.parse::<u64>().ok())
                .ok_or(ParseError::malformed("field"))
        };
        let instrument = InstrumentRef::from_admission(number()? as u32);
        let value = number()?;
        out.upstream_message("quote");
        match kind {
            "q" => out.push(
                Event::Quote {
                    instrument,
                    source_ts_ns: SOURCE_TS_NS,
                    bid: SideUpdate::Present {
                        px: Scalar::fixed(value as i64, -2),
                        qty: Scalar::fixed(1_000, -3),
                        source_count: None,
                    },
                    ask: SideUpdate::Gone,
                },
                Order::None,
            ),
            "t" => out.push(
                Event::Trade {
                    instrument,
                    source_ts_ns: SOURCE_TS_NS,
                    px: Scalar::fixed(value as i64, -2),
                    qty: Scalar::fixed(1_000, -3),
                    aggressor: Aggressor::Buy,
                    trade_id: Some(value),
                    cumulative_volume: None,
                    flags: TradeFlags::NONE,
                },
                Order::Identity(value),
            ),
            _ => return Err(ParseError::schema("kind")),
        }
        Ok(())
    }
}

impl ThreadedAdapter for Packing {
    fn decoder(&self, _conn: ConnectionId, _claims: ClaimTable) -> Box<dyn Decoder> {
        Box::new(PackingDecoder)
    }
}

impl Adapter for Packing {
    fn message_types(&self) -> &[&'static str] {
        &["quote"]
    }

    fn source_timestamp_kind(&self) -> Option<VenueTimestampKind> {
        Some(VenueTimestampKind::MatchingEngine)
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

/// A transport that hands over one payload per permit, so a test decides what
/// is on the queue when the loop takes a batch.
struct Gated {
    payloads: VecDeque<Vec<u8>>,
    current: Vec<u8>,
    gate: Arc<tokio::sync::Semaphore>,
    exhausted: Arc<AtomicBool>,
}

impl Input for Gated {
    fn connection(&self) -> ConnectionId {
        ConnectionId::new("primary")
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
            if self.payloads.is_empty() {
                self.exhausted.store(true, Ordering::SeqCst);
                return std::future::pending().await;
            }
            self.gate.acquire().await.expect("never closed").forget();
            self.current = self.payloads.pop_front().expect("checked above");
            Ok(Received::Payload {
                bytes: &self.current,
                ts_ns: None,
            })
        })
    }

    fn shutdown(&mut self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A threaded publisher: the harness's, set to send live messages per batch,
/// with three instruments admitted and the connection's payloads gated.
struct Threaded {
    h: Harness,
    workers: tokio::runtime::Runtime,
    publisher_loop: tokio::runtime::Runtime,
    adapter: SharedThreadedAdapter,
    connections: Connections,
    drain: Drain,
    gate: Arc<tokio::sync::Semaphore>,
}

impl Threaded {
    fn new(mut h: Harness, payloads: &[String]) -> Self {
        let mut listings = FakeAdapter::new(&["A-B", "C-D", "E-F"]);
        h.publisher.poll_listings(&mut listings);
        assert_eq!(
            listings
                .handles()
                .iter()
                .map(|h| h.index())
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        let adapter: SharedThreadedAdapter = Arc::new(Mutex::new(Box::new(Packing::default())));
        h.publisher
            .declare_venue_timestamps(&**adapter.lock().expect("uncontended"));
        h.publisher.send_live_per_batch();

        let workers = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("workers");
        let publisher_loop = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("loop");
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let input = Gated {
            payloads: payloads.iter().map(|p| p.as_bytes().to_vec()).collect(),
            current: Vec::new(),
            gate: Arc::clone(&gate),
            exhausted: Arc::new(AtomicBool::new(false)),
        };
        let (connections, drain) = {
            let _entered = publisher_loop.enter();
            threaded::spawn(
                workers.handle(),
                vec![(Box::new(input) as Box<dyn Input>, true)],
                &adapter,
                Arc::new(SystemClock::new()),
                Arc::new(MetricsObserver::new(Arc::clone(&h.metrics))),
                Policy {
                    connect_timeout: Duration::from_secs(1),
                    backoff: BackoffPolicy::new(
                        Duration::from_millis(10),
                        Duration::from_millis(100),
                    )
                    .expect("a valid policy"),
                    rate_limit_per_second: 0,
                    idle_timeout: None,
                },
                Arc::clone(&h.metrics),
                threaded::QUEUE_DEPTH,
            )
        };
        Self {
            h,
            workers,
            publisher_loop,
            adapter,
            connections,
            drain,
            gate,
        }
    }

    /// Release `count` payloads, wait until all of them are on the queue, and
    /// apply exactly one batch. Returns how many events the batch took.
    fn one_batch(&mut self, count: usize) -> usize {
        self.gate.add_permits(count);
        let Self {
            h,
            publisher_loop,
            adapter,
            drain,
            ..
        } = self;
        publisher_loop.block_on(async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while drain.queued() < count {
                assert!(tokio::time::Instant::now() < deadline, "never queued");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let taken = drain.recv().await;
            drain.apply(adapter, &mut h.publisher);
            taken
        })
    }

    fn stop(mut self) -> Harness {
        self.publisher_loop.block_on(self.connections.stop());
        drop(self.workers);
        self.h
    }
}

/// What the wire carried, in order, as `('q', bid cents)` or `('t', trade id)`.
fn on_the_wire(h: &Harness) -> Vec<(char, u64)> {
    h.tob
        .as_ref()
        .expect("a top-of-book feed")
        .mktdata
        .messages()
        .into_iter()
        .map(|(type_id, bytes)| match type_id {
            0x03 => (
                'q',
                Quote::decode(&bytes).expect("composed").bid_price as u64,
            ),
            0x04 => ('t', Trade::decode(&bytes).expect("composed").trade_id),
            other => panic!("unexpected message type {other:#04x}"),
        })
        .collect()
}

/// The datagrams a run of messages fills when each is packed into the open
/// datagram until the next would not fit — computed from the codec's sizes.
fn datagrams_for(kinds: &[char]) -> usize {
    let room = MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE;
    let mut datagrams = 0;
    let mut used = room;
    for kind in kinds {
        let size = match kind {
            'q' => <Quote as AppMessage>::SIZE,
            _ => <Trade as AppMessage>::SIZE,
        };
        if used + size > room {
            datagrams += 1;
            used = 0;
        }
        used += size;
    }
    datagrams
}

fn rendered_count(h: &Harness, prefix: &str) -> u64 {
    h.metrics
        .render()
        .lines()
        .filter(|line| line.starts_with(prefix))
        .filter_map(|line| line.rsplit(' ').next())
        .filter_map(|value| value.parse::<f64>().ok())
        .map(|value| value as u64)
        .sum()
}

/// A batch of `BATCH` messages over three instruments, quotes and trades mixed
/// irregularly so the sizes do not divide the datagram evenly.
fn mixed_script() -> (Vec<String>, Vec<(char, u64)>) {
    let mut payloads = Vec::new();
    let mut expected = Vec::new();
    for n in 1..=threaded::BATCH as u64 {
        let instrument = n % 3;
        if n % 3 == 0 || n % 7 == 0 {
            payloads.push(format!("t:{instrument}:{n}"));
            expected.push(('t', n));
        } else {
            payloads.push(format!("q:{instrument}:{n}"));
            expected.push(('q', n));
        }
    }
    (payloads, expected)
}

#[test]
fn a_full_batch_leaves_in_as_few_datagrams_as_fit_it_complete_and_in_order() {
    let (payloads, expected) = mixed_script();
    let mut threaded = Threaded::new(harness(feed()), &payloads);
    let taken = threaded.one_batch(payloads.len());
    assert_eq!(taken, threaded::BATCH, "one batch took the whole queue");
    let h = threaded.stop();

    let kinds: Vec<char> = expected.iter().map(|(kind, _)| *kind).collect();
    let wanted = datagrams_for(&kinds);
    let per_quote = (MAX_DATAGRAM_SIZE - DATAGRAM_HEADER_SIZE) / <Quote as AppMessage>::SIZE;
    assert!(
        wanted < threaded::BATCH && wanted >= threaded::BATCH.div_ceil(per_quote),
        "the script must fill more than one datagram and fewer than one per message"
    );
    assert_eq!(h.tob.as_ref().expect("tob").mktdata.len(), wanted);
    assert_eq!(on_the_wire(&h), expected);
}

#[test]
fn a_small_batch_is_on_the_wire_before_the_next_batch_is_taken() {
    let payloads: Vec<String> = (1..=5).map(|n| format!("q:{}:{n}", n % 3)).collect();
    let mut threaded = Threaded::new(harness(feed()), &payloads);

    // Three now: one datagram, sent at the batch's end — no timer, and no
    // second `recv_many` needed to push it out.
    assert_eq!(threaded.one_batch(3), 3);
    let sent_after_first = threaded.h.tob.as_ref().expect("tob").mktdata.len();
    assert_eq!(sent_after_first, 1);

    // Two more: a datagram of their own, never the first batch's.
    assert_eq!(threaded.one_batch(2), 2);
    let h = threaded.stop();
    let recorder = &h.tob.as_ref().expect("tob").mktdata;
    assert_eq!(recorder.len(), 2);
    let per_datagram: Vec<usize> = recorder
        .datagrams()
        .iter()
        .map(|datagram| {
            dz_edge_core::Datagram::decode(datagram, dz_edge_tob::MAGIC_TOB)
                .expect("composed")
                .messages()
                .count()
        })
        .collect();
    assert_eq!(per_datagram, [3, 2]);
    assert_eq!(
        on_the_wire(&h),
        (1..=5).map(|n| ('q', n)).collect::<Vec<_>>()
    );
}

#[test]
fn latency_is_observed_once_per_message_when_its_datagram_leaves() {
    let (payloads, expected) = mixed_script();
    let mut threaded = Threaded::new(harness(feed()), &payloads);
    threaded.one_batch(payloads.len());
    let h = threaded.stop();
    let messages = expected.len() as u64;
    assert_eq!(
        rendered_count(&h, "dz_publisher_recv_to_send_latency_seconds_count"),
        messages
    );
    assert_eq!(
        rendered_count(&h, "dz_publisher_venue_to_recv_latency_seconds_count"),
        messages
    );
}

#[test]
fn a_packed_message_is_measured_at_its_send_and_not_before() {
    // The publisher alone, as the loop drives it: an event packed is not yet
    // a latency, and the batch's end is what sends it and measures it. A zero
    // venue stamp is still no venue-to-receive sample.
    let mut h = harness(feed());
    let mut listings = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut listings);
    h.publisher.declare_venue_timestamps(&Packing::default());
    h.publisher.send_live_per_batch();
    let instrument = listings.handles()[0];
    let quote = |source_ts_ns| Event::Quote {
        instrument,
        source_ts_ns,
        bid: SideUpdate::Present {
            px: Scalar::fixed(100, -2),
            qty: Scalar::fixed(1_000, -3),
            source_count: None,
        },
        ask: SideUpdate::Gone,
    };
    h.publisher.payload_scope(Some(SOURCE_TS_NS + 1_000_000));
    h.publisher.event(quote(SOURCE_TS_NS));
    h.publisher.event(quote(0));
    h.publisher.payload_scope(None);
    assert_eq!(
        h.tob.as_ref().expect("tob").mktdata.len(),
        0,
        "held to the batch's end"
    );
    assert_eq!(
        rendered_count(&h, "dz_publisher_recv_to_send_latency_seconds_count"),
        0
    );

    h.publisher.drained();
    assert_eq!(h.tob.as_ref().expect("tob").mktdata.len(), 1);
    assert_eq!(
        rendered_count(&h, "dz_publisher_recv_to_send_latency_seconds_count"),
        2
    );
    assert_eq!(
        rendered_count(&h, "dz_publisher_venue_to_recv_latency_seconds_count"),
        1
    );
}

#[test]
fn a_trade_is_measured_once_whichever_feeds_its_shard_carries() {
    // On a shard with no top-of-book feed the trade leaves only on the depth
    // feed, so that is where its arrival is recorded. On one carrying both it
    // is measured once, on top-of-book, and not again on depth.
    for (h, label) in [
        (harness(depth_feed()), "depth only"),
        (harness_both(), "both feeds"),
    ] {
        let mut h = h;
        let mut listings = FakeAdapter::new(&["A-B"]);
        h.publisher.poll_listings(&mut listings);
        h.publisher.declare_venue_timestamps(&Packing::default());
        h.publisher.send_live_per_batch();
        let instrument = listings.handles()[0];
        h.publisher.payload_scope(Some(SOURCE_TS_NS + 1_000_000));
        h.publisher.event(Event::Trade {
            instrument,
            source_ts_ns: SOURCE_TS_NS,
            px: Scalar::fixed(100, -2),
            qty: Scalar::fixed(1_000, -3),
            aggressor: Aggressor::Buy,
            trade_id: Some(1),
            cumulative_volume: None,
            flags: TradeFlags::NONE,
        });
        h.publisher.payload_scope(None);
        h.publisher.drained();
        assert_eq!(
            h.mbp.as_ref().expect("mbp").mktdata.len(),
            1,
            "{label}: the trade left on depth"
        );
        assert_eq!(
            rendered_count(&h, "dz_publisher_recv_to_send_latency_seconds_count"),
            1,
            "{label}"
        );
        assert_eq!(
            rendered_count(&h, "dz_publisher_venue_to_recv_latency_seconds_count"),
            1,
            "{label}"
        );
    }
}

#[test]
fn a_hold_timer_does_not_split_a_threaded_batch() {
    // Past `MAX_LIVE_HOLD` between two trades of one batch, on a shard carrying
    // both feeds: under the threaded model the hold is not consulted, so each
    // feed still sends the two together, in one datagram, at the batch's end.
    let mut h = harness_both();
    let mut listings = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut listings);
    h.publisher.send_live_per_batch();
    let instrument = listings.handles()[0];
    let trade = |id| Event::Trade {
        instrument,
        source_ts_ns: 1,
        px: Scalar::fixed(100, -2),
        qty: Scalar::fixed(1_000, -3),
        aggressor: Aggressor::Buy,
        trade_id: Some(id),
        cumulative_volume: None,
        flags: TradeFlags::NONE,
    };
    h.publisher.payload_scope(Some(1));
    h.publisher.event(trade(1));
    h.clock.advance(dz_publisher_runtime::MAX_LIVE_HOLD * 10);
    h.publisher.payload_scope(Some(2));
    h.publisher.event(trade(2));
    h.publisher.payload_scope(None);
    let top = h.tob.clone().expect("tob").mktdata;
    let depth = h.mbp.clone().expect("mbp").mktdata;
    assert_eq!((top.len(), depth.len()), (0, 0), "nothing leaves mid-batch");
    h.publisher.drained();
    assert_eq!((top.len(), depth.len()), (1, 1));
    assert_eq!((top.messages().len(), depth.messages().len()), (2, 2));
}

#[test]
fn an_inline_venue_still_sends_one_top_of_book_datagram_per_quote() {
    let mut h = harness(feed());
    let mut listings = FakeAdapter::new(&["A-B", "C-D"]);
    h.publisher.poll_listings(&mut listings);
    for (n, instrument) in listings
        .handles()
        .to_vec()
        .into_iter()
        .cycle()
        .take(6)
        .enumerate()
    {
        h.publisher.payload_scope(Some(n as u64 + 1));
        h.publisher.event(harness::quote(instrument, n as u64 + 1));
        // Each one is on the wire before the next arrives, with no drain.
        assert_eq!(h.tob.as_ref().expect("tob").mktdata.len(), n + 1);
    }
    h.publisher.payload_scope(None);
    h.publisher.drained();
    assert_eq!(h.tob.as_ref().expect("tob").mktdata.len(), 6);
}

#[test]
fn a_packed_trade_is_byte_for_byte_the_depth_feeds_trade() {
    let mut h = harness_both();
    let mut listings = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut listings);
    h.publisher.send_live_per_batch();
    let instrument = listings.handles()[0];
    h.publisher.payload_scope(Some(1));
    h.publisher.event(Event::Trade {
        instrument,
        source_ts_ns: 5,
        px: Scalar::fixed(123, -2),
        qty: Scalar::fixed(4_000, -3),
        aggressor: Aggressor::Sell,
        trade_id: Some(77),
        cumulative_volume: None,
        flags: TradeFlags::NONE,
    });
    h.publisher.payload_scope(None);
    h.publisher.drained();
    let trades = |recorder: &harness::Recorder| -> Vec<Vec<u8>> {
        recorder
            .messages()
            .into_iter()
            .filter(|(type_id, _)| *type_id == 0x04)
            .map(|(_, bytes)| bytes)
            .collect()
    };
    let top = trades(&h.tob.as_ref().expect("tob").mktdata);
    let depth = trades(&h.mbp.as_ref().expect("mbp").mktdata);
    assert_eq!(top.len(), 1);
    assert_eq!(top, depth);
}

/// `dz_publisher_egress_messages_per_datagram` for mktdata, as `(count, sum)`.
fn per_datagram(h: &Harness) -> (u64, u64) {
    let rendered = h.metrics.render();
    let read = |suffix: &str| {
        rendered
            .lines()
            .filter(|line| {
                line.starts_with(&format!(
                    "dz_publisher_egress_messages_per_datagram_{suffix}"
                )) && line.contains(r#"port_role="mktdata""#)
            })
            .filter_map(|line| line.rsplit(' ').next())
            .filter_map(|value| value.parse::<f64>().ok())
            .map(|value| value as u64)
            .sum()
    };
    (read("count"), read("sum"))
}

#[test]
fn every_datagram_that_left_is_observed_once_with_its_message_count() {
    let (payloads, expected) = mixed_script();
    let mut threaded = Threaded::new(harness(feed()), &payloads);
    threaded.one_batch(payloads.len());
    let h = threaded.stop();
    let datagrams = h.tob.as_ref().expect("tob").mktdata.len() as u64;
    assert_eq!(per_datagram(&h), (datagrams, expected.len() as u64));
    // The refdata role is pre-created too, as a declared port role.
    assert!(h.metrics.render().lines().any(|line| line
        .starts_with("dz_publisher_egress_messages_per_datagram_count")
        && line.contains(r#"port_role="refdata""#)));
}

#[test]
fn an_inline_venue_observes_one_message_per_datagram_and_a_lost_one_not_at_all() {
    let mut h = harness(feed());
    let mut listings = FakeAdapter::new(&["A-B"]);
    h.publisher.poll_listings(&mut listings);
    let instrument = listings.handles()[0];
    for n in 1..=3 {
        h.publisher.payload_scope(Some(n));
        h.publisher.event(harness::quote(instrument, n));
    }
    assert_eq!(per_datagram(&h), (3, 3));

    // While the route is down the fan-out still answers `Ok`, and the datagram
    // went nowhere: it is not observed.
    h.tob.as_ref().expect("tob").mktdata_route_down.set(true);
    h.publisher.payload_scope(Some(4));
    h.publisher.event(harness::quote(instrument, 4));
    h.publisher.payload_scope(None);
    assert_eq!(per_datagram(&h), (3, 3));
}
