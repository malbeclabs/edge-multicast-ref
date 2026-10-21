//! The threaded model's boundary: the claim, the loop stage's dedupe, what
//! `push` refuses, and the inline helper that makes a threaded adapter an
//! inline one.

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::Arc;

use dz_adapter_core::threaded::{
    self, carried, Admission, ClaimTable, Decoded, DecodedSink, Decoder, Dedupe, Inline, Order,
    ThreadedAdapter, IDENTITY_WINDOW, MAX_TRACKED_INSTRUMENTS,
};
use dz_adapter_core::{
    Adapter, Aggressor, ClearScope, ConnectionId, Event, EventSink, InstrumentRef, ListingSink,
    ParseError, Payload, Presence, Scalar, Side, SideUpdate, TradeFlags,
};

fn instrument(index: u32) -> InstrumentRef {
    InstrumentRef::from_admission(index)
}

fn quote(index: u32, bid: i64) -> Event<'static> {
    Event::Quote {
        instrument: instrument(index),
        source_ts_ns: 0,
        bid: SideUpdate::Present {
            px: Scalar::fixed(bid, -2),
            qty: Scalar::fixed(1, 0),
            source_count: None,
        },
        ask: SideUpdate::Gone,
    }
}

fn trade(index: u32, px: i64, trade_id: u64) -> Event<'static> {
    Event::Trade {
        instrument: instrument(index),
        source_ts_ns: 0,
        px: Scalar::fixed(px, -2),
        qty: Scalar::fixed(1, 0),
        aggressor: Aggressor::Buy,
        trade_id: Some(trade_id),
        cumulative_volume: None,
        flags: TradeFlags::NONE,
    }
}

fn vwap(px: Scalar<'static>, volume: Scalar<'static>) -> Event<'static> {
    Event::Vwap {
        instrument: instrument(6),
        window_end_ns: 0,
        window_ms: NonZeroU32::new(1_000).expect("not zero"),
        compute_ts_ns: 0,
        px,
        volume,
        stale: false,
    }
}

#[test]
fn a_claim_wins_only_above_every_key_claimed_before() {
    let claims = ClaimTable::new();
    let one = instrument(1);
    assert!(claims.claim(one, 0), "zero is a key like any other");
    assert!(!claims.claim(one, 0), "the same key twice");
    assert!(claims.claim(one, 10));
    assert!(!claims.claim(one, 10));
    assert!(!claims.claim(one, 9), "an older key after a newer one");
    assert!(claims.claim(one, 11));
    // Per instrument: another instrument's keys are its own.
    assert!(claims.claim(instrument(2), 5));
}

#[test]
fn a_clone_is_the_same_table() {
    let claims = ClaimTable::new();
    let other = claims.clone();
    assert!(claims.claim(instrument(0), 7));
    assert!(!other.claim(instrument(0), 7));
}

#[test]
fn an_instrument_listed_after_startup_is_claimable_without_a_resize() {
    let claims = ClaimTable::new();
    // The first segment, a later one, and the last tracked handle.
    for index in [
        0,
        4_095,
        4_096,
        100_000,
        (MAX_TRACKED_INSTRUMENTS - 1) as u32,
    ] {
        assert!(claims.claim(instrument(index), 1), "{index}");
        assert!(!claims.claim(instrument(index), 1), "{index}");
    }
}

#[test]
fn a_handle_beyond_the_table_always_claims_and_the_loop_still_applies_it() {
    let claims = ClaimTable::new();
    let beyond = instrument(MAX_TRACKED_INSTRUMENTS as u32);
    assert!(claims.claim(beyond, 1));
    assert!(claims.claim(beyond, 1));
    let mut dedupe = Dedupe::new();
    assert!(dedupe.admit(beyond, Order::Supersedes(1)));
    assert!(dedupe.admit(beyond, Order::Supersedes(1)));
}

#[test]
fn concurrent_claims_never_win_one_key_twice() {
    // Eight connections racing over the same ten thousand updates. A key can
    // be lost by all of them — one claims 5 before another reaches 4 — but no
    // key may be won by two, and the last key is held.
    let claims = ClaimTable::new();
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let claims = claims.clone();
            std::thread::spawn(move || {
                (0..10_000u64)
                    .filter(|key| claims.claim(instrument(3), *key))
                    .collect::<Vec<u64>>()
            })
        })
        .collect();
    let mut won = HashSet::new();
    for thread in threads {
        for key in thread.join().expect("a claiming thread") {
            assert!(won.insert(key), "key {key} won twice");
        }
    }
    assert!(won.contains(&9_999));
    assert!(!claims.claim(instrument(3), 9_999));
}

#[test]
fn the_loop_stage_drops_a_supersedes_key_not_above_the_last_applied() {
    let mut dedupe = Dedupe::new();
    let one = instrument(1);
    // B's 11 is dequeued before A's 10, which both claimed.
    assert!(dedupe.admit(one, Order::Supersedes(11)));
    assert!(!dedupe.admit(one, Order::Supersedes(10)));
    assert!(!dedupe.admit(one, Order::Supersedes(11)));
    assert!(dedupe.admit(one, Order::Supersedes(12)));
    assert!(dedupe.admit(instrument(9), Order::Supersedes(0)));
}

#[test]
fn an_identity_key_is_applied_once_inside_its_window() {
    let mut dedupe = Dedupe::new();
    let one = instrument(1);
    // Unordered: a point event's identity has no order.
    for key in [5, 3, 9] {
        assert!(dedupe.admit(one, Order::Identity(key)));
    }
    for key in [3, 9, 5] {
        assert!(!dedupe.admit(one, Order::Identity(key)), "{key}");
    }
    // Another instrument's identities are its own.
    assert!(dedupe.admit(instrument(2), Order::Identity(5)));
}

#[test]
fn an_identity_key_evicted_from_the_window_and_seen_again_is_late() {
    // Once the window has moved past a key, a copy of it is late, as a
    // trade ID below a consumer's window is: dropped, not applied again.
    let mut dedupe = Dedupe::new();
    let one = instrument(1);
    assert!(dedupe.admit(one, Order::Identity(0)));
    for key in 1..IDENTITY_WINDOW as u64 {
        assert!(dedupe.admit(one, Order::Identity(key)));
    }
    assert_eq!(
        dedupe.judge(one, Order::Identity(0)),
        Admission::Duplicate,
        "still inside"
    );
    assert!(dedupe.admit(one, Order::Identity(IDENTITY_WINDOW as u64)));
    assert_eq!(
        dedupe.judge(one, Order::Identity(0)),
        Admission::Late,
        "pushed out, and not applied again"
    );
    // And a key above everything evicted is still new.
    assert_eq!(
        dedupe.judge(one, Order::Identity(IDENTITY_WINDOW as u64 + 1)),
        Admission::Applied
    );
}

#[test]
fn an_event_with_no_order_is_always_applied() {
    let mut dedupe = Dedupe::new();
    assert!(dedupe.admit(instrument(1), Order::None));
    assert!(dedupe.admit(instrument(1), Order::None));
}

#[test]
fn push_carries_quotes_trades_and_vwaps_with_fixed_scalars_only() {
    assert_eq!(carried(&quote(4, 100)), Ok(instrument(4)));
    assert_eq!(carried(&trade(5, 100, 1)), Ok(instrument(5)));
    assert_eq!(
        carried(&vwap(Scalar::fixed(100, -2), Scalar::fixed(1, 0))),
        Ok(instrument(6))
    );

    let level = Event::Level {
        instrument: instrument(1),
        source_ts_ns: 0,
        side: Side::Bid,
        px: Scalar::fixed(1, 0),
        qty: Scalar::fixed(1, 0),
        order_count: None,
        presence: Presence::Unknown,
    };
    let clear = Event::Clear {
        instrument: instrument(1),
        source_ts_ns: 0,
        scope: ClearScope::BothSides,
    };
    let text_quote = Event::Quote {
        instrument: instrument(1),
        source_ts_ns: 0,
        bid: SideUpdate::Gone,
        ask: SideUpdate::Present {
            px: Scalar::text("1.00"),
            qty: Scalar::fixed(1, 0),
            source_count: None,
        },
    };
    let text_volume = Event::Trade {
        instrument: instrument(1),
        source_ts_ns: 0,
        px: Scalar::fixed(1, 0),
        qty: Scalar::fixed(1, 0),
        aggressor: Aggressor::Unknown,
        trade_id: None,
        cumulative_volume: Some(Scalar::text("10")),
        flags: TradeFlags::NONE,
    };
    let text_vwap_px = vwap(Scalar::text("1.00"), Scalar::fixed(1, 0));
    let text_vwap_volume = vwap(Scalar::fixed(100, -2), Scalar::text("1"));
    for refused in [
        level,
        clear,
        text_quote,
        text_volume,
        text_vwap_px,
        text_vwap_volume,
    ] {
        assert!(
            matches!(carried(&refused), Err(ParseError::Schema { .. })),
            "{refused:?}"
        );
    }
}

/// Payloads of space-separated messages, each `<kind>:<instrument>:<key>:<px>`:
/// `q` a quote under `Supersedes`, `t` a trade under `Identity`, `n` a trade
/// with no order, `l` a level (refused), `x` a quote with a text price
/// (refused). A `q` is claimed before its price is read.
pub struct Fixture {
    inline: Option<Inline>,
    /// Quotes `on_decoded` saw, so an override is observable.
    decoded: usize,
    /// Suppress a quote whose bid equals the last one forwarded.
    suppress_unchanged: bool,
    last_bid: Option<Event<'static>>,
}

impl Fixture {
    fn new(suppress_unchanged: bool) -> Self {
        Self {
            inline: None,
            decoded: 0,
            suppress_unchanged,
            last_bid: None,
        }
    }
}

struct FixtureDecoder {
    claims: ClaimTable,
}

fn field(fields: &mut std::str::Split<'_, char>) -> Result<u64, ParseError> {
    fields
        .next()
        .and_then(|field| field.parse().ok())
        .ok_or(ParseError::malformed("field"))
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
            let index = field(&mut fields)? as u32;
            let key = field(&mut fields)?;
            out.upstream_message("message");
            match kind {
                "q" => {
                    if !self.claims.claim(instrument(index), key) {
                        out.duplicate();
                        continue;
                    }
                    let px = field(&mut fields)? as i64;
                    out.push(quote(index, px), Order::Supersedes(key));
                }
                "t" => {
                    let px = field(&mut fields)? as i64;
                    out.push(trade(index, px, key), Order::Identity(key));
                }
                "n" => {
                    let px = field(&mut fields)? as i64;
                    out.push(trade(index, px, key), Order::None);
                }
                "l" => out.push(
                    Event::Level {
                        instrument: instrument(index),
                        source_ts_ns: 0,
                        side: Side::Bid,
                        px: Scalar::fixed(1, 0),
                        qty: Scalar::fixed(1, 0),
                        order_count: None,
                        presence: Presence::Unknown,
                    },
                    Order::None,
                ),
                "x" => out.push(
                    Event::Quote {
                        instrument: instrument(index),
                        source_ts_ns: 0,
                        bid: SideUpdate::Present {
                            px: Scalar::text("1.00"),
                            qty: Scalar::fixed(1, 0),
                            source_count: None,
                        },
                        ask: SideUpdate::Gone,
                    },
                    Order::Supersedes(key),
                ),
                _ => return Err(ParseError::schema("kind")),
            }
        }
        Ok(())
    }
}

impl ThreadedAdapter for Fixture {
    fn decoder(&self, _conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder> {
        Box::new(FixtureDecoder { claims })
    }

    fn on_decoded(&mut self, decoded: &Decoded, out: &mut dyn EventSink) {
        self.decoded += 1;
        if self.suppress_unchanged && matches!(decoded.event, Event::Quote { .. }) {
            if self.last_bid == Some(decoded.event) {
                return;
            }
            self.last_bid = Some(decoded.event);
        }
        out.event(decoded.event);
    }
}

impl Adapter for Fixture {
    fn message_types(&self) -> &[&'static str] {
        &["message"]
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

#[derive(Default)]
struct Recorded {
    messages: usize,
    events: Vec<String>,
}

impl EventSink for Recorded {
    fn upstream_message(&mut self, _message_type: &'static str) {
        self.messages += 1;
    }

    fn event(&mut self, event: Event<'_>) {
        self.events.push(format!("{event:?}"));
    }
}

fn payload<'a>(bytes: &'a str, connection: &'static str) -> Payload<'a> {
    Payload {
        bytes: bytes.as_bytes(),
        recv_ts_ns: 1,
        connection: ConnectionId::new(connection),
    }
}

#[test]
fn two_connections_inline_emit_each_update_once_and_in_order() {
    let mut adapter = Fixture::new(false);
    let mut out = Recorded::default();
    let stream = [
        ("q:1:10:100 t:1:7:100", "a"),
        ("q:1:10:100 t:1:7:100", "b"),
        ("q:1:12:102", "b"),
        ("q:1:11:101", "a"),
        ("q:1:12:102 t:1:8:102", "a"),
        ("t:1:8:102 t:1:7:100", "b"),
    ];
    for (bytes, connection) in stream {
        adapter
            .on_payload(&payload(bytes, connection), &mut out)
            .expect("parses");
    }
    assert_eq!(
        out.events,
        vec![
            format!("{:?}", quote(1, 100)),
            format!("{:?}", trade(1, 100, 7)),
            format!("{:?}", quote(1, 102)),
            format!("{:?}", trade(1, 102, 8)),
        ]
    );
    // Every message is still counted, duplicates included: counting is at
    // decode.
    assert_eq!(out.messages, 10);
}

#[test]
fn a_refused_push_is_a_parse_error_and_the_rest_of_the_payload_still_applies() {
    let mut adapter = Fixture::new(false);
    let mut out = Recorded::default();
    let result = adapter.on_payload(&payload("q:1:1:100 l:1:0:0 x:1:2:0 q:2:1:5", "a"), &mut out);
    assert!(
        matches!(result, Err(ParseError::Schema { .. })),
        "{result:?}"
    );
    assert_eq!(
        out.events,
        vec![format!("{:?}", quote(1, 100)), format!("{:?}", quote(2, 5))]
    );
}

#[test]
fn on_decoded_sees_only_survivors_and_may_suppress_them() {
    let mut adapter = Fixture::new(true);
    let mut out = Recorded::default();
    for bytes in ["q:1:1:100", "q:1:1:100", "q:1:2:100", "q:1:3:101"] {
        adapter
            .on_payload(&payload(bytes, "a"), &mut out)
            .expect("parses");
    }
    // The duplicate never reaches `on_decoded`; the unchanged quote does, and
    // is suppressed there.
    assert_eq!(adapter.decoded, 3);
    assert_eq!(
        out.events,
        vec![
            format!("{:?}", quote(1, 100)),
            format!("{:?}", quote(1, 101))
        ]
    );
}

#[test]
fn the_free_helper_and_inline_agree() {
    let stream = [
        "q:3:1:100 n:3:0:5",
        "q:3:1:100 n:3:0:5",
        "t:3:4:9",
        "t:3:4:9",
    ];

    let mut held = Fixture::new(false);
    let mut by_inline = Recorded::default();
    for bytes in stream {
        held.on_payload(&payload(bytes, "a"), &mut by_inline)
            .expect("parses");
    }

    let mut free = Fixture::new(false);
    let mut decoder = free.decoder(ConnectionId::new("a"), ClaimTable::new());
    let mut dedupe = Dedupe::new();
    let mut by_helper = Recorded::default();
    for bytes in stream {
        threaded::on_payload(
            &mut free,
            &mut *decoder,
            &mut dedupe,
            &payload(bytes, "a"),
            &mut by_helper,
        )
        .expect("parses");
    }
    assert_eq!(by_inline.events, by_helper.events);
    // `Order::None` is forwarded every time.
    assert_eq!(by_helper.events.len(), 4);
}

#[test]
fn the_boundary_types_cross_threads_and_upcast() {
    fn send_sync<T: Send + Sync>() {}
    fn send<T: Send>() {}
    send_sync::<ClaimTable>();
    send::<Box<dyn Decoder>>();
    send::<Decoded>();
    send::<Inline>();
    send::<Box<dyn ThreadedAdapter>>();

    // A threaded adapter is held as one and driven as an inline one.
    let threaded: Arc<std::sync::Mutex<Box<dyn ThreadedAdapter>>> =
        Arc::new(std::sync::Mutex::new(Box::new(Fixture::new(false))));
    let mut held = threaded.lock().expect("uncontended");
    let inline: &mut dyn Adapter = &mut **held;
    let mut out = Recorded::default();
    inline
        .on_payload(&payload("q:1:1:1", "a"), &mut out)
        .expect("parses");
    assert_eq!(out.events.len(), 1);
}

/// A decoder for one counter that restarts: `<instrument>:<counter>`, judged
/// restarted when it falls below half the last counter this connection saw.
struct RestartingDecoder {
    claims: ClaimTable,
    generations: dz_adapter_core::CounterGenerations,
    last: std::collections::HashMap<u32, u64>,
}

impl Decoder for RestartingDecoder {
    fn decode(
        &mut self,
        payload: &Payload<'_>,
        out: &mut dyn DecodedSink,
    ) -> Result<(), ParseError> {
        let text =
            std::str::from_utf8(payload.bytes).map_err(|_| ParseError::malformed("utf-8"))?;
        let mut fields = text.split(':');
        let index = field(&mut fields)? as u32;
        let counter = field(&mut fields)?;
        out.upstream_message("message");
        if self.last.get(&index).is_some_and(|last| counter < last / 2) {
            self.generations.restart(instrument(index));
        }
        self.last.insert(index, counter);
        let key = self
            .generations
            .key(instrument(index), counter)
            .ok_or(ParseError::malformed("counter"))?;
        if !self.claims.claim(instrument(index), key) {
            out.duplicate();
            return Ok(());
        }
        out.push(quote(index, counter as i64), Order::Supersedes(key));
        Ok(())
    }
}

struct Restarting;

impl ThreadedAdapter for Restarting {
    fn decoder(&self, _conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder> {
        Box::new(RestartingDecoder {
            claims,
            generations: dz_adapter_core::CounterGenerations::new(),
            last: std::collections::HashMap::new(),
        })
    }
}

impl Adapter for Restarting {
    fn message_types(&self) -> &[&'static str] {
        &["message"]
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

#[test]
fn a_counter_restart_starts_a_generation_and_a_stale_copy_from_the_old_one_is_dropped() {
    let mut adapter = Restarting;
    let mut inline = Inline::new();
    let mut out = Recorded::default();
    // `a` sees the restart; `b` is a slower connection whose copy of 100 from
    // the old counter arrives after it.
    for (bytes, connection) in [
        ("1:99", "b"),
        ("1:100", "a"),
        ("1:101", "a"),
        ("1:1", "a"),
        ("1:2", "a"),
        ("1:100", "b"),
        ("1:101", "b"),
        ("1:1", "b"),
        ("1:3", "a"),
    ] {
        inline
            .on_payload(&mut adapter, &payload(bytes, connection), &mut out)
            .expect("parses");
    }
    assert_eq!(
        out.events,
        [99, 100, 101, 1, 2, 3]
            .iter()
            .map(|counter| format!("{:?}", quote(1, *counter)))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_generation_key_orders_above_the_old_counter_and_refuses_a_wide_one() {
    let mut generations = dz_adapter_core::CounterGenerations::new();
    let one = instrument(1);
    let old = generations.key(one, 1_000_000).expect("fits");
    generations.restart(one);
    assert_eq!(generations.generation(one), 1);
    assert_eq!(generations.generation(instrument(2)), 0, "per instrument");
    let new = generations.key(one, 1).expect("fits");
    assert!(new > old);
    assert_eq!(
        generations.key(one, 1 << dz_adapter_core::CounterGenerations::COUNTER_BITS),
        None
    );
    // The loop stage orders them the same way.
    let mut dedupe = Dedupe::new();
    assert!(dedupe.admit(one, Order::Supersedes(old)));
    assert!(dedupe.admit(one, Order::Supersedes(new)));
    assert!(!dedupe.admit(one, Order::Supersedes(old)));
}

#[test]
fn a_decoder_a_generation_behind_catches_up_on_its_next_falling_update() {
    // A and B share a claim table. A judges a restart; B, which saw 500,000,
    // never sees the drop to near zero. B's next update is below what it saw,
    // and the slot holds a newer generation: B adopts it and wins.
    use dz_adapter_core::threaded::Claim;
    let claims = ClaimTable::new();
    let one = instrument(1);
    let mut a = dz_adapter_core::CounterGenerations::new();
    let mut b = dz_adapter_core::CounterGenerations::new();
    assert!(matches!(a.claim(&claims, one, 500_000), Claim::Won(_)));
    assert_eq!(b.claim(&claims, one, 500_000), Claim::Lost);
    a.restart(one);
    assert!(matches!(a.claim(&claims, one, 1), Claim::Won(_)));
    assert!(matches!(b.claim(&claims, one, 10_002), Claim::Won(_)));
    assert_eq!(b.generation(one), 1);
    assert!(matches!(b.claim(&claims, one, 600_000), Claim::Won(_)));
}

#[test]
fn a_late_pre_restart_copy_does_not_adopt_a_newer_generation() {
    // B is live and has not yet seen the restart A judged. Its copy of the
    // old counter is rising, not falling: it keeps its generation and loses,
    // and says it lost to a newer one.
    use dz_adapter_core::threaded::Claim;
    let claims = ClaimTable::new();
    let one = instrument(1);
    let mut a = dz_adapter_core::CounterGenerations::new();
    let mut b = dz_adapter_core::CounterGenerations::new();
    assert!(matches!(a.claim(&claims, one, 500_000), Claim::Won(_)));
    assert_eq!(b.claim(&claims, one, 500_000), Claim::Lost);
    a.restart(one);
    assert!(matches!(a.claim(&claims, one, 1), Claim::Won(_)));
    assert_eq!(b.claim(&claims, one, 500_001), Claim::LostToNewerGeneration);
    assert_eq!(b.generation(one), 0, "a late copy promoted its decoder");
    // A's next update still wins: nothing was wedged above it.
    assert!(matches!(a.claim(&claims, one, 2), Claim::Won(_)));
}
