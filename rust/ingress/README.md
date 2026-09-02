# Ingress crates

The transport half of the venue boundary: the half that waits.

| Crate | |
|---|---|
| [`dz-ingress-core`](dz-ingress-core/) | The `Input` trait, the driver that runs an `Adapter` against it, reconnection and backoff |
| [`dz-ingress-websocket`](dz-ingress-websocket/) | A WebSocket `Input` |

## Why this is not in `adapter/`

Two things vary independently, and only one of them is asynchronous.

An `Adapter` maps bytes onto normalized events. It is synchronous, does no I/O, and is a pure function of its payload and its own state — see [`adapter/`](../adapter/) for what that buys. An `Input` owns a connection, a subscription, a reconnect, a backoff and a rate limit, and every one of those waits.

Putting both in one crate would hand every adapter a websocket client's dependency tree whether it used one or not. And the two upstreams that motivated this boundary have nothing in common — one connects out to a websocket and a FIX session, the other tails a local directory — so a single trait assuming a connection would fit one and not the other.

## The driver is where the two halves meet

It connects, lets the adapter write its subscriptions, sends them, hands every payload to `on_payload`, and reports every disconnect. `on_connected` runs on **every** successful connect, reconnects included, which is what makes a subscription that was silently lost come back.

That driver is also what makes the adapter's synchronous purity possible: the adapter writes into a queue and the driver drains it afterwards.

Design: [the venue adapter interface](../../docs/superpowers/specs/2026-09-02-venue-adapter-interface-design.md) for the split, [the publisher crates design](../../docs/superpowers/specs/2026-08-26-edge-publisher-crates-design.md) for `[ingress] kind`.
