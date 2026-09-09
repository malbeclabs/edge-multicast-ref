# A polled transport, and the token it should have

**Status:** design. Answers a request to build the declared-and-unbuilt polled transport, so that a venue's catalogue arrives as payloads rather than through a queue a venue also owns.

## The question it starts from

An adapter is I/O-free by contract: it is handed payloads and asked what they mean. A venue whose instrument catalogue lives behind a request/response endpoint therefore cannot fetch it — so every such venue writes its own poller task in its own binary, holds its own timer, its own backoff and its own failure counting, and hands results across a queue it also owns.

The family already declares the transport that removes all of that. With it, the catalogue arrives on a second `[[source]]` as payloads, `on_payload` decodes it like anything else, and the runtime keeps the cadence, the backoff, the failure classification and the connection-state series it already has for every other transport.

This is also the half that makes an upstream write worth having: a poll tells an adapter that an instrument exists, and [`poll_upstream`](2026-09-09-upstream-write-after-a-listing-change-design.md) is how it gets subscribed. Neither is a substitute for the other.

## Naming, before the word becomes a key, a field and a label

The variant exists as `Kind::Rest`, with the token `"rest"` and the doc comment "polled request/response. Not yet built." **The token should be `poll`, and the variant `Kind::Poll`.** Three reasons, in the order they matter.

**`rest` collides with a word this repository already uses for something else.** A price level's quantity is its *resting* quantity — `Event::Level` documents "one price level's aggregate resting quantity" and "orders resting at this price", and `LevelUpdate` states the resting quantity at a price on the wire. One word for a transport and for what a book holds is the `route` problem restated: the feed-routes design rejected `route` because `[egress]` already meant the IP route by it, and the same objection applies here with the added cost that this word appears in the wire vocabulary rather than only in configuration.

**It names an architectural style, and the family names mechanisms.** `websocket` names a protocol, `multicast` a delivery model, `filetail` what it does, `uds` a socket family. A venue's catalogue may be a plain `GET` with no REST semantics at all, or JSON-RPC over `POST`; both are polled request/response and neither is more or less REST. A token that describes a style invites an argument about whether a given endpoint qualifies, and there is nothing for that argument to decide.

**`poll` is already this repository's word for asking on a cadence**, and it is used consistently: `poll_listings`, `poll_upstream`, the listing poll, the definition pacer's own laps. `GLOSSARY.md` neither defines nor bans it, and it collides with nothing here.

**Renaming costs nothing now and is expensive later.** Nothing constructs the variant, so no document can name it usefully: `kind = "rest"` today resolves to a transport the binary does not link and is refused at startup with a message listing what it does. Once a venue ships a document naming it, the token is in configuration management and the rename is a coordinated change. So the rename lands *before* the transport, as its own task, and `Kind::ALL`, `TOKEN_LIST`, `as_token` and the `every_kind_has_a_token` test move with it.

## The shape under `Input`

The trait's contract maps onto polling without stretching, and each method's answer is a decision worth stating rather than an obvious one.

| `Input` | A polled transport's answer |
|---|---|
| `connect` | Resolve the endpoint and perform the first request. A connect that "succeeds" without proving the endpoint answers is a transport that reports a healthy connection to a host that is not there. |
| `recv(budget)` | When the next poll is due, request and return the body as `Received::Payload`. When the response says nothing changed, `Received::Liveness`. When the budget elapses before the poll is due, `Received::Idle`. |
| `send` | What an adapter writes changes *what is polled* — a symbol list, a cursor, a page token. The transport holds it as the next request's parameters. |
| `shutdown` | Release the client and the connection pool. |

**`Liveness` and not a payload for an unchanged response** is the load-bearing one. A catalogue endpoint answering `304`, or answering with a body whose digest has not moved, has proved the endpoint is alive and produced nothing for the adapter. `Received::Liveness`'s doc says exactly what that case is for and warns what treating it as a payload costs: the idle guard counts time since the last *payload*, so a transport that reported every unchanged poll as a payload would make the guard unable to fire on the one failure it exists for — an endpoint that answers forever and has stopped changing.

**A failed request ends the connection.** Not a silent internal retry. A polled transport's "connection" is a fiction, and the honest mapping is *the endpoint answered the last poll*: with it, `dz_publisher_ingress_connection_state` pre-created at 0 means what it means for every other transport, the reconnect counter counts failures by reason, and the driver's backoff paces the retries instead of a second backoff inside the transport. An operator's `== 0` alert then fires for a catalogue that has stopped answering, which is what pre-creating that gauge is for. A transport that retried internally would report a healthy connection while nothing arrived.

## The one decision that is not about polling

**The `Input` trait is async, and this workspace's only HTTP client is blocking.**

`ureq` is in the tree for the column-store writer, which is a separate process doing batched writes where blocking is right. Calling it from inside an async `Input` blocks the runtime the drivers share — and the ingress crates deliberately start no runtime and spawn nothing (`dz-ingress-websocket` takes `tokio` without `rt` and says why), so `spawn_blocking` is not available to them either.

So this transport needs an async HTTP client, and that is a first for the family. The decision, and its terms:

- **An async client rather than HTTP over `tokio` by hand.** Chunked transfer encoding, redirects, connection reuse, timeouts and TLS certificate verification are a security-relevant surface with no upside in owning. The websocket transport already made this call for its protocol and pinned a specific implementation with `default-features = false`, so that neither `native-tls` nor a second cryptographic backend can arrive by way of a default. The same discipline applies here.
- **Two HTTP clients in one workspace is the cost, and it is named rather than hidden.** The blocking one stays where it is: it belongs to a different process, a different failure model and a different tier. What must not happen is either one migrating toward the other because they look similar in a manifest.
- **TLS is a feature, not a default.** The endpoint's scheme is checked at configuration load, exactly as the column-store writer refuses `https` when it was not built with TLS — an operator learns at load rather than at the first request.

## Configuration: one key, and the word for it

The poll interval is the transport's own and belongs in configuration, because it is a property of the endpoint and of what an operator is willing to ask of it — a catalogue polled once a minute and a book polled every second are the same transport.

**`poll_interval`, and it is an interval rather than a cycle.** This repository distinguishes the two deliberately: `definition_cycle` and `snapshot_cycle` are *one full pass over a set*, divided by the set's size, because a per-instrument interval has the whole set falling due together. There is no set here: one tick is one request, so the honest word is the one that means the time between two of them.

`connect_timeout` and the backoff are the family's and are not restated. The request timeout is the receive budget the driver already hands over.

## What this refuses

- **No second poller.** The driver holds the cadence, the backoff, the failure counting and the connection state. A transport that held any of them would be the second implementation of the thing this family exists to have one of.
- **No decoding.** A response body is a payload. What it means is the adapter's, including whether an empty catalogue is an outage or a market with nothing listed.
- **No pagination logic in the transport.** A cursor is state the venue defines; the adapter writes the next request's parameters through `send`, which is the mechanism that already exists for telling a transport what to ask for.
- **No new metric family.** Every series a polled transport moves — connection state, reconnects by reason, bytes, messages, connect failures by reason — exists and is pre-created.
- **No blocking client in the ingress family.**

## Decisions

| Decision | Why |
|---|---|
| `Kind::Poll`, token `poll`, renamed before the transport is built | `rest` collides with a book's *resting* quantity, names a style rather than a mechanism, and is free to change only while nothing constructs it |
| An unchanged response is `Liveness` | The idle guard counts payloads, so an unchanged poll reported as a payload would stop the guard firing on an endpoint that answers forever and has stopped changing |
| A failed request ends the connection | It makes the connection gauge, the reconnect reasons and the backoff mean for this transport what they mean for every other |
| An async HTTP client, pinned, defaults off | The trait is async and the workspace's blocking client belongs to another process; hand-rolled HTTP over TLS is a surface with no upside in owning |
| `poll_interval`, not `poll_cycle` | A cycle is one pass over a set divided by its size; one tick here is one request |
| The adapter changes what is polled through `send` | It is the existing mechanism for telling a transport what to ask upstream, and a cursor is the venue's |

## Non-goals

A catalogue schema. Pagination or cursor semantics. A second HTTP client for the recorder tier. Anything about *which* instruments a catalogue's contents should admit — that is the selection policy's, and it already exists.
