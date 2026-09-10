# An upstream write that is not a connect

**Status:** design. Answers a request for a way to send something upstream after the connection is already open.

## The failure, which is measured rather than argued

`UpstreamSink` reaches an adapter in exactly one method: `on_connected`, called on every successful connect. `poll_listings` is handed a `ListingSink` and nothing else.

So an instrument discovered mid-session can be admitted completely — minted an `Instrument ID`, defined on the reference-data port, counted in the manifest, given a `Manifest Seq` bump — and never subscribed, because the subscription was composed at logon from the set the adapter held then. Nothing reports it. The feed is healthy, the manifest says the instrument is published, and no message for it will ever arrive.

Over thirty days on one venue, 5 instruments were listed in two mid-session batches. A publisher reading over a websocket carried all five inside one 17-day process lifetime, because its transport re-subscribes on its own poll. A publisher of the same venue reading over a session transport carried none of them until its next restart — 10.35 and 7.32 days later. Same venue, same instruments, same boundary; the difference is whether the transport happened to have a reason of its own to write again.

It is also the whole of the repair path. A mid-session snapshot request cannot be expressed either, so a publisher that wants to ask the venue to resend a book it has lost has nowhere to put the request — and a recovery-path metric built on top of that would publish a flat zero certifying a repair that never runs.

## Why neither suggested shape works

Two shapes were offered:

```rust
fn poll_listings(&mut self, listings: &mut dyn ListingSink, upstream: &mut dyn UpstreamSink);
fn listings_changed(&mut self, out: &mut dyn UpstreamSink);   // defaulted
```

Both are refused, for one reason that is fatal and one that follows from it.

**Neither names the connection, and the write has to.** `on_connected`'s own doc comment already argues this at length, because it is where the same mistake was found before: "One adapter serves every source a publisher opens. A publisher with several `[[source]]` blocks drives one connection per source and hands every payload to *this* object… So state that belongs to a connection has to be stored per `conn` and not per adapter. An adapter that keeps one upstream sequence cursor, or one authentication token, or one 'have I subscribed yet' flag, is correct with one source and wrong the moment a second is configured — and the way it is wrong is silent."

An `UpstreamSink` with no `ConnectionId` beside it is exactly that flag, handed to the adapter by the boundary. Two sources means two sessions, two sequence spaces and two subscription states, and the bytes for one are not the bytes for the other.

**There is nowhere for the runtime to send it from.** `poll_listings` is called from the runtime's tick loop, which holds the adapter and no transport: the drivers own the `Input`s, one per source, each in its own future. A sink handed to the tick loop would have to be a queue whose consumer is *some* driver, and picking one is picking a connection — the decision the first objection says must be the adapter's, made explicitly.

So the shape is not a second argument to `poll_listings`. The write belongs where a write already happens: in the driver, on its own connection, through the queue and the paced flush that `on_connected` already uses.

## The shape

One defaulted method on the boundary:

```rust
fn poll_upstream(
    &mut self,
    conn: ConnectionId,
    out: &mut dyn UpstreamSink,
) -> Result<(), AdapterError> {
    let _ = (conn, out);
    Ok(())
}
```

Each driver calls it on its own connection, on a cadence, while the connection is up. What the adapter queues is flushed through the same rate-limited `flush` that carries `on_connected`'s messages, so a subscription burst cannot outrun the venue's limit any more than a logon can.

**Named for the mechanism, not for one of its causes.** `listings_changed` names the measured failure and nothing else; the repair path is a second cause and a venue's own reasons are a third. `poll_upstream` is symmetric with `poll_listings` — the runtime asks, on a cadence, and the adapter answers with whatever is outstanding — and it uses the vocabulary the boundary already has: `UpstreamSink`, `UpstreamMessage`, `upstream_message`.

**No reason is passed, deliberately.** The runtime could say *a poll admitted something* or *an instrument reset*, and it must not: the adapter is the only thing that knows what it has already written on this connection, so a reason would invite it to key its state on the reason rather than on `conn` — which is the mistake `on_connected` documents. It already holds the handles `list_on` returned; the diff is its own.

**The runtime needs no change at all.** `poll_listings` admits, the adapter records the handle, the driver asks, the adapter writes what it has not written on that connection. No generation counter, no shared flag, no new plumbing between the tick loop and the drivers.

### What the adapter owes, stated because the runtime cannot enforce it

Re-offering an instrument to `poll_listings` is free: the sink dedups and returns the handle already minted. **Writing upstream is not free, and nothing here can dedup it.** The runtime does not understand a venue's bytes, so an adapter that writes its whole subscription set every time it is asked sends that set to the venue on every cadence. The contract is therefore: write what is *outstanding on this connection*, and nothing when nothing is.

The failure mode of getting that wrong is a venue rate-limiting or disconnecting a publisher that looks, from here, as though it is working — so the doc comment says it in those words rather than leaving it to be discovered.

### Two asymmetries with `on_connected`, both argued

**A refusal does not tear the connection down.** When `on_connected` returns an error the driver ends the connection and retries, because a connection subscribed to nothing is worth nothing. Mid-session it is worth a great deal: the instruments subscribed at logon are still arriving. So a refusal here is counted at the observer — the same `dz_publisher_ingress_adapter_errors_total{reason}` family that already holds "an adapter that cannot compose its own subscription" — and the connection is left alone. The next cadence asks again.

**A send failure does end the connection**, exactly as any other send failure does. The flush returns an `IngressError`, the driver tears down and reconnects, and `on_connected` then writes the whole subscription set again — so the repair for a failed mid-session write is the reconnect path that already exists, and no state has to be reconciled.

### The cadence is a constant, not a key

It bounds how long a newly admitted instrument waits for its subscription. It is not something an operator tunes against their own network, and a key for it would be a second place to state how often listings matter — the runtime already states that in its listing poll. So it is a constant beside the driver, documented against that one: there is nothing to write that a poll has not admitted, so asking more often than listings are polled buys nothing.

It is checked where the idle guard's arithmetic is already computed, so it costs one comparison per receive and no extra clock read.

## What this does not do

- **No new metric family.** The normative set is closed, and the one family that fits — adapter errors — already exists and already has this exact meaning. A recovery-path metric is a separate question and the request is right that it was unanswerable before this; it is still not this change.
- **No change to `on_connected`, `poll_listings`, `EventSink` or `ListingSink`.** Their signatures, their parameters and their defaults are untouched.
- **No change to the runtime.** Not one line: the mechanism is a boundary method and a driver call.
- **No breaking change.** The method is defaulted, so every existing adapter compiles and behaves exactly as it did. An adapter that wants the mechanism implements one method.
- **It does not make a mid-session subscription reliable on a transport that cannot carry one.** A transport whose venue only accepts subscriptions at logon will have its adapter queue nothing and wait for a reconnect, which is what happens today — but now that is the adapter's stated decision rather than a boundary that had no way to ask.

## Decisions

| Decision | Why |
|---|---|
| A method on `Adapter`, called by the driver | The driver owns the connection and the paced flush; the tick loop owns neither |
| It carries a `ConnectionId` | One adapter serves every source, and a subscription without a connection is the silent per-adapter flag `on_connected` documents |
| Named `poll_upstream`, not `listings_changed` | The listing change is one cause; the repair path is another and a venue's own reasons are a third |
| No reason parameter | The adapter is the only thing that knows what it has written on this connection; a reason invites state keyed on the reason |
| A refusal is counted and the connection survives | Mid-session the connection is delivering the instruments subscribed at logon; dropping it costs those to save the one |
| A send failure ends the connection | It already does, and the reconnect writes the whole set again — the repair is the existing path |
| The cadence is a constant | It bounds a wait, not a network property, and the runtime already states how often listings matter |

## Non-goals

A recovery-path metric. A resend or gap-fill protocol. Any way for an adapter to name a `Channel ID`, a group, a port, a sequence number or an era — this method carries a connection and a sink, and nothing else.
