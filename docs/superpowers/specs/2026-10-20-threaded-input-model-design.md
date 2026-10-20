# A threaded input model for the publisher runtime

**Status:** design.
**Applies to:** `rust/publisher/dz-publisher-runtime/`, `rust/ingress/dz-ingress-core/`,
`rust/adapter/dz-adapter-core/`, `rust/venue/dz-venue-composition/`, `rust/recorder/dz-recorder-relower/`
**Builds on:** [2026-09-02-venue-adapter-interface-design.md](2026-09-02-venue-adapter-interface-design.md)
**Read at:** `origin/main` = `9a22d82`. Every file cited is as it was at `v0.6.0`
(`5adfd82`): the only change since that tag is in `dz-recorder-load`.

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, config key or metric prefix. `GLOSSARY.md` governs vocabulary.

---

## 1. The problem

The runtime has one input model. Every connection and the tick are futures in
one task on a current-thread runtime (`dz-publisher-runtime/src/run.rs:10-28`,
`:632`, `:727-731`). The driver awaits `recv` and calls `Adapter::on_payload`
inline (`dz-ingress-core/src/driver.rs:570-611`). `recv` borrows the transport's
buffer, so the next payload cannot be read until this one is mapped
(`dz-ingress-core/src/input.rs:187-190`). There is no queue.

That is the right model for a cheap decode, and it keeps two properties worth
keeping: `on_payload` is a pure function of its payload and the adapter's state
(`dz-adapter-core/src/adapter.rs:21-29`), which is what lets the recorder re-run
it over an archive, and the publisher stays `!Send` with no locks held across
an await.

It is the wrong model for **a venue whose payload is text that is expensive to
decode, delivered redundantly over several connections at high rate.** Every
copy is decoded on one core, and most copies are duplicates found only after
decoding. A publisher built outside this framework for that case measured,
over 24 hours on one host:

| | Mean | Peak, 10 s |
|---|---|---|
| Inbound payloads/s, every copy | 23,000 | 146,000 to 159,000 |
| Outbound datagrams/s | 7,800 | 73,000 to 75,000 |

Two payloads in three were duplicates. Its single loop did the JSON walk,
decimal parsing, dedupe, packing and a `sendto` per payload, and its
65,536-deep queue filled on every host. It fixed this by decoding and
pre-checking duplicates on the connection tasks, then draining the queue in
batches of 32 with one flush per batch. That design is the reference for this
one, described generically below.

A second symptom of the single task is already recorded here:
`MAX_LIVE_HOLD` exists because "while `recv` stays ready the tick never runs"
(`dz-publisher-runtime/src/publisher.rs:99-114`).

## 2. Goals and non-goals

- **Keep the inline model as the default.** `Venue::new` and every existing
  adapter compile and behave unchanged.
- **Add a threaded model, chosen at composition time.** Decode runs per
  connection, in parallel. Apply runs on the publisher loop, serially.
- **Nothing a specification decided moves into the decode stage.** The decode
  stage produces the same `Event`s an adapter produces today.
- **Not in scope:** parallel egress, parallel apply, or any change to
  sequencing, `Reset Count`, the refdata cycle, lowering or the wire.

## 3. Shape

```
conn task 1 ─ Driver ─ Decoder ─┐
conn task 2 ─ Driver ─ Decoder ─┼─ bounded queue ─ publisher loop ─ on_decoded ─ Publisher ─ egress
conn task N ─ Driver ─ Decoder ─┘                  (recv_many, one flush per batch)
```

### 3a. Trait sketch (dz-adapter-core)

```rust
/// An adapter whose decode can run off the publisher loop.
pub trait ThreadedAdapter: Adapter {
    /// One decoder per connection, built once at startup and kept across
    /// reconnects. It runs on that connection's task.
    fn decoder(&self, conn: ConnectionId, claims: ClaimTable) -> Box<dyn Decoder>;

    /// One decoded event that survived dedupe, on the publisher loop.
    /// Defaulted to forward. An adapter holding a book suppresses an
    /// unchanged quote here, as it does in `on_payload` today.
    fn on_decoded(&mut self, decoded: &Decoded, out: &mut dyn EventSink) {
        out.event(decoded.event);
    }
}

/// Runs on a connection task. Same rule as `on_payload`: no blocking, no
/// I/O, no allocation per payload, and a pure function of the payload, its
/// own state and read-mostly shared state.
pub trait Decoder: Send {
    fn decode(&mut self, payload: &Payload<'_>, out: &mut dyn DecodedSink)
        -> Result<(), ParseError>;
}

pub trait DecodedSink {
    fn upstream_message(&mut self, message_type: &'static str);
    fn push(&mut self, event: Event<'static>, order: Order);
    /// A copy dropped by a lost claim, before it was fully decoded.
    fn duplicate(&mut self);
}

/// How the runtime dedupes one event across connections.
pub enum Order {
    /// No cross-connection identity. Forwarded as delivered.
    None,
    /// State: a strictly increasing per-instrument key, newest wins.
    Supersedes(u64),
    /// A point event: each key once per instrument, inside a window.
    Identity(u64),
}

pub struct Decoded {
    pub event: Event<'static>,
    pub order: Order,
    pub connection: ConnectionId,
    pub recv_ts_ns: u64,
}
```

**`Event<'static>` is the owned form, and it needs nothing new.** `Event` is
`Copy` (`dz-adapter-core/src/event.rs:23`). With `Scalar::Fixed` it borrows
nothing. The decode stage parses decimal text into `Fixed { mantissa,
exponent }` at the precision the venue stated, so the expensive part moves off
the loop. Lowering already rescales `Fixed` to the instrument's exponent
(`dz-publisher-lowering/src/scale.rs:32-49`, `:234`), so the venue still does
not choose the wire exponent. `Scalar::Text` is refused at `push`, by type if
the signature can say it and at runtime otherwise.

**Only `Quote` and `Trade` in the first version.** `Level` and `Clear` are
deltas. Two connections' deltas reordered on a queue corrupt a book while
every per-connection check passes. A first-copy floor is not enough for them.
They are refused at `push` until a design argues otherwise.

**A threaded adapter is also an inline one.** A helper,
`threaded::on_payload(adapter, decoder, payload, out)`, runs decode then
`on_decoded` in place. `ThreadedAdapter: Adapter` implements `on_payload` with
it. So the recorder's relower, tests and a single-connection deployment drive
the same code serially, and replay stays a diff.

### 3b. Dedupe in two stages

- **Connection stage, `Supersedes` only.** `ClaimTable::claim(instrument, key)`
  is a per-instrument `AtomicU64` with a load as pre-check and `fetch_max` as
  the claim. A decoder reads the instrument and key first and claims before
  parsing prices. A lost claim calls `duplicate()` and stops. In the reference
  publisher this kept about two-thirds of payloads out of the queue.
- **Loop stage, the authority.** Claims can be queued out of order: A claims
  10, B claims 11, B's item is dequeued first. So the loop repeats the check
  against the last applied key and drops anything not above it. `Identity`
  events are checked only here, against a bounded window, because a point
  event is not superseded by a later one.

The runtime owns both stages. Both reference publishers wrote the same
first-copy-wins rule by hand, and a shared one cannot drift.

### 3c. Per-connection behavior stays in the Driver

Each connection runs today's `Driver` unchanged, on its own task, handed:

- a delegate adapter whose `on_payload` calls that connection's `Decoder`,
  and whose `on_connected`, `poll_upstream` and `on_disconnected` lock the
  shared adapter. Those are rare: on connect, and every `UPSTREAM_POLL`, 5 s
  (`dz-ingress-core/src/driver.rs:98`, `:625-660`). The lock was uncontended
  by construction (`run.rs:1547-1550`) and becomes lightly contended.
- an `EventSink` that pushes onto the queue.

So keepalive (in the `Input`), backoff and the attempt spread
(`driver.rs:386-411`), `connection_state` going to 1 after subscriptions are
sent, reconnect reasons and parse errors all stay per connection with no new
code. **To verify:** `Driver` holds `&dyn Clock` and `&dyn IngressObserver`
(`driver.rs:340-348`). Running it on a multi-thread runtime needs both to be
`Sync` and the future to be `Send`. That is a reading of the signatures, not a
build.

### 3d. Runtimes

The publisher stays `!Send` and stays where it is, on the current-thread
runtime (`run.rs:16-18`). Connection tasks run on a second, multi-thread tokio
runtime with a configured worker count. A bounded tokio `mpsc` bridges the two;
it works across runtimes. The loop `select!`s on `recv_many(batch, 32)` beside
the tick, offers every event, then calls `drained()` once, which is
`flush_live` (`publisher.rs:2073-2075`). The tick now runs at batch boundaries,
so the starvation `MAX_LIVE_HOLD` guards against goes away for this model.
`MAX_LIVE_HOLD` stays.

### 3e. Timestamps

`recv_ts_ns` is still stamped where it is today, by the transport or the driver
on the connection task (`driver.rs:580-587`). So `venue_to_recv` excludes queue
wait, and `recv_to_send` now includes it. Both names keep their meaning as
written: receipt is receipt. A dashboard comparing across the switch reads a
definition change, and the release notes say so.

### 3f. Backpressure

A full queue makes `push` await. The connection task stops reading, TCP pushes
back, and the venue may drop the connection. That is visible as a reconnect
with a reason. Nothing is dropped silently on a full queue. Default depth
65,536, a constant, as in the reference.

### 3g. Ordering

Per connection, FIFO: one task, one sender. Across connections, none. Per
instrument, `Supersedes` makes the applied series increasing. `Identity` and
`None` events from one connection keep that connection's order.

### 3h. Shutdown

Signal, then each connection task stops through the Driver's existing path
(`Input::shutdown`, `on_disconnected`). Senders drop. The loop drains what is
queued, applies it, and the publisher's ordered teardown runs as today,
`EndOfSession` last. A drain deadline bounds it.

## 4. Metrics

**No normative name or label changes.** `dz_publisher_ingress_duplicates_total`
counts both stages, so duplicates over published still tracks connections
minus one. `dz_publisher_ingress_messages_total` is counted at decode, so its
rate becomes arrival rather than drain. Proposed additions, normative and
venue-neutral:

- `dz_publisher_ingress_queue_depth{venue, source_id}`, gauge, sampled at drain.
- `dz_publisher_ingress_batch_size{venue, source_id}`, histogram.

## 5. Composition and migration

`Venue::threaded(adapter: Box<dyn ThreadedAdapter>, sources)` beside
`Venue::new` (`dz-venue-composition/src/registry.rs:264`). The runtime picks the
model from the variant. One optional document key sets the worker count.
Existing adapters do not change, and nothing they see changes.

## 6. Open questions

- Default worker count, and whether it is a key or derived from the host.
- Whether `ClaimTable` sizes from the instrument registry or grows on listing.
- Whether `Identity`'s window is per instrument or per adapter.
- Whether the relower needs a threaded entry point or the inline helper is enough.
