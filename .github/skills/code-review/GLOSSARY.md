<!-- Vendored from malbeclabs/edge-feed-spec at glossary/v1.3.0. -->
<!-- Do not edit here. .github/workflows/glossary-sync.yml replaces this file. -->

# Glossary

Canonical vocabulary for DoubleZero Edge market data.

Use these words with these meanings in specs, docs, plans, comments, identifiers, CLI flags, config keys, metric names, and log fields. If a word you want is listed under [Banned](#banned-words), use the replacement.

This document specifies version **1.3.0**: the terms defined below, the banned words, and their documented exceptions. See [Versioning](#versioning) for what a level means and what has changed.

## Precedence

This file is the authority. A definition here overrides any local one, wherever it appears. A project may add terms it alone needs; it may not redefine a term listed here.

## Identity

| Term | Definition | Not |
|---|---|---|
| **Venue** | The external exchange or market operator (Hyperliquid, Kalshi, Phoenix). One venue may run several matching engines and therefore hold several Source IDs. | A matching engine, a publisher, a host, a product line |
| **Matching engine** | One matching domain at a venue: a set of instruments whose resting orders can match against one another, under one set of order-matching rules. Two domains are distinct engines where their rules differ, and equally where they match independently over disjoint instrument sets under identical rules. Hyperliquid native perps, each HIP-3 builder DEX, HIP-4 prediction markets, Kalshi events, and Kalshi perps are five engines across two venues. | A venue, a channel, a feed |
| **Source ID** | The `u16` wire field naming the matching engine whose activity a message describes, assigned by the Source ID Registry. Present on every price and event message, and on `InstrumentDefinition` as of Schema Version 3. | A venue, a channel, a publisher instance, a host, a transport, a timestamp |
| **Source ID Registry** | The assignment table in `sources/spec.md`. Sole authority. | Any copy of it |
| **Registry mirror** | Any copy of the Source ID Registry outside `sources/spec.md` — a hardcoded table, or a config artifact a service loads at runtime. Derived, never authoritative. | The registry |
| **Instrument** | One tradable entity, keyed by `Instrument ID` (`u32`), unique within a channel. | A symbol, a market, an asset |
| **Symbol** | The `char[64]` human-readable name in `InstrumentDefinition`. Display and filtering only. | A state key |
| **Operator** | The person or organization running a publisher. | The publisher process |

Use `venue` for the external exchange. `exchange` is a synonym; prefer `venue`.

**A Source ID identifies a matching engine, not a venue.** Split the ID wherever the microstructure or the logic governing how orders match differs, because that is the boundary a subscriber has to reason about: a venue name tells it nothing about how a book behaves. Split it equally where two instrument sets match independently of each other under the same rules, because identical rules running over disjoint books are still two books: each opens, halts and resets on its own schedule, its interface is versioned on its own schedule, and a subscriber holding a message from one learns nothing about the state of the other. One venue therefore holds as many Source IDs as it runs engines, and a new engine at a known venue is a new ID rather than a reuse of the venue's existing one. Registry IDs are stable and never renumbered, so this only ever adds: an ID already assigned keeps meaning whichever engine has been publishing under it. Venues split their own engines on their own schedule, so treat the set as open and track it against each venue's roadmap rather than assuming today's list is final.

**Independent matching is not the same as sharding.** A venue that spreads one instrument set across several processes for capacity, and can move an instrument between them, runs one engine published over several **channels**: the partition is a deployment detail and `Channel ID` already carries it. It is a second engine where the venue itself treats the boundary as durable, publishing the two as separate platforms with their own interface versions, trading calendars and session lifecycles, and no instrument crossing between them. Registry IDs are never renumbered, so read the venue's own treatment of the boundary rather than the current shape of its deployment.

**Venues are sometimes carried under a codename before they launch.** The Source ID Registry is public, so a venue that has not yet announced its DoubleZero feed may appear under a placeholder name there, in multicast group codes, and in operator-facing config. Retire the codename once the venue is public, registry first. Retirement is a sequenced change rather than a search-and-replace: until the DoubleZero ledger re-registers the groups, code paths that resolve ledger strings MUST keep accepting the old name on input, and a `code` that stops matching its live group fails silently rather than loudly.

## Transport

| Term | Definition | Not |
|---|---|---|
| **Multicast group** | The IP group datagrams are sent to. Say "group" only where multicast is unambiguous. | A channel, a feed |
| **Port role** | One of exactly `mktdata`, `refdata`, `snapshot`. Use these tokens verbatim. | A channel |
| **Channel** | A logical shard of the instrument set, named by `Channel ID` (`u8`) in the datagram header. Two redundant paths may carry the same channel. | A port role, a `chan`/`mpsc`, a venue's pub/sub topic, a sequence series |
| **Channel instance** | One path's view of one channel, keyed `(source IP address, Channel ID, destination port)`. The unit that owns a sequence series, a `Reset Count`, and a snapshot cycle. | A channel |
| **Datagram** | The contents of one UDP packet: a 24-byte datagram header plus N application messages. | A message |
| **Message** | One application record inside a datagram, with its own 4-byte header. | A datagram |

Prefer `datagram` over `frame` or `packet` for our own traffic. A frame is a layer-2 construct carrying Ethernet and IP headers; what we define and version is the UDP payload.

**Sequencing keys on the channel instance, never the channel.** `Sequence Number`, `Reset Count`, and the snapshot cycle belong to one path's view of a channel. Redundant paths carrying the same channel run as separate processes on separate hosts and cannot share a counter, so a subscriber binding more than one sees an independent series per instance and MUST key gap detection and recovery state on `(source IP address, Channel ID, destination port)`. Each host publishes on a distinct destination port by deployment convention, defined out of band. Arbitrating between instances of the same channel is a separate concern from sequencing within one.

## Protocol and roles

| Term | Definition | Not |
|---|---|---|
| **Feed** | Both the wire protocol defined by a feed spec (Top-of-Book, Midpoint, Market-by-Order, Market-by-Price, Order-Intent, Perp-Stats) and the live traffic carrying it on one channel. Say "feed spec" or "live feed" only where the difference matters. | A multicast group, an upstream vendor |
| **DoubleZero Edge family** | The set of feed specs sharing the 24-byte datagram header, the 4-byte message header, and one Type ID space: Top-of-Book, Midpoint, Market-by-Order, Market-by-Price, Order-Intent, Perp-Stats. Say "a feed in the family" for one member. | A venue's product lines, a set of live feeds, a versioned family |
| **Publisher** | The process that transmits datagrams using a multicast group. The multicast sender role. | A deploy unit, a struct, one redundant path |
| **Subscriber** | A process that joins a multicast group, strips headers, and decodes datagrams. | A trading client |
| **Era** | The span between two `Reset Count` values. | An epoch |
| **Snapshot** | A moment-in-time copy of the order book, published on the `snapshot` port. | A blockchain state dump |
| **Delta** | An incremental book-mutating message on `mktdata`. | |

Two pairs, one per layer, and both are correct. From the network's point of view a publisher is a **transmitter** (or **sender**) and a subscriber is a **receiver**: use those for the act of moving datagrams. For the roles themselves use **publisher** and **subscriber**.

Neither half of either pair is redefined here. `receiver` in particular keeps its plain meaning — anything that receives — so every subscriber is a receiver, and the specs' normative "Publishers SHOULD … receivers MUST …" is correct as written.

## Components

| Term | Definition | Not |
|---|---|---|
| **Decoder** | The pure function turning wire bytes into a struct. | A binary |
| **Parser** | A binary that subscribes, decodes, and republishes records downstream. | The decoder inside it |
| **Book-builder** | A binary consuming parser output that maintains book state and optionally persists it. | A bot |
| **Bot** | A real automated trading client. We ship none. | Any component we build |

## Banned words

| Banned | Use instead | Note |
|---|---|---|
| `arm`, `disarm` (every sense) | see the note below | Two external proper nouns aside — see the note below |
| `bot` (our components) | `book-builder` | |
| `lane` | `feed` or `path` | |
| `feed` (upstream vendor) | `upstream <vendor>` | |
| `stream` (our live traffic) | `feed` | A live feed is a feed; the extra word bought nothing. `snapshot stream` and `delta stream` are the exception: they name the two traffic shapes within one three-port feed, a distinction `feed` cannot carry |
| `frame` (our own traffic) | `datagram` | |
| `channel` (port role) | `port role` | |
| `channel` (venue pub/sub topic) | `venue topic` | |
| `source` (unqualified) | a qualified form | Never bare — see the note below. `source of truth` is the exception: a fixed English idiom naming authority, not an origin |
| `epoch` | `era` | `Unix epoch` is the exception: it is the externally owned name of the 1970-01-01T00:00:00Z origin, not a sense of the word. Use `era` for our own `Reset Count` span |
| `sibling feed`, `sibling spec`, `sibling protocol` | name the family | See the note below. Bare `sibling` is untouched |
| `tee` | `fan-out` | |
| `sweep` | Name the operation | Currently means three unrelated things. `Trade Flags` bit 1 keeps the name: it is the standard term for an order sweeping several levels, externally defined and carried on the wire |
| `normalization` | `decode` | Reserve `normalization` for cross-feed latency metrics |
| `venue` (Rust trait over product lines) | `product line` or `adapter` | |
| `roster`, `active set` | `published set` | |

**`arm` is banned outright**, in every sense and every place: specs, docs, plans, identifiers, CLI flags, config keys, metric names, log fields, and code comments alike. The only things that survive are names we do not own — the `ARM64` architecture and the `ARM` vendor — which are proper nouns rather than a sense of the word. Replace it by sense — a redundant publisher is a `path`, a `match` or `select!` branch is a `branch`, and the Order-Intent dead-man switch is **set** and **cleared** rather than armed and disarmed. That last one costs nothing on the wire: the switch is carried by `ScheduleCancel`'s `Trigger Time`, where a non-zero timestamp sets it and `0` clears it, so `arm` was only ever prose.

`path` is deliberately not defined in this glossary. It is used in its ordinary English sense — one of several redundant routes carrying the same data — and needs no house meaning.

**`sibling feed` names the family badly, and plain `feed` is the wrong replacement.** The set it reaches for is the **DoubleZero Edge family**, defined above and already named that way in `reference-data/spec.md`. Write "a feed in the family", or name the specs, and say "the other feed specs" where `sibling specs` appears.

Plain `feed` was the original replacement and it loses the constraint wherever the sentence is about family membership rather than about feeds generally. "A message Type ID that appears in more than one feed MUST carry the same semantic meaning in each" is false of feeds at large and true only within the family, which is the whole point of the rule at `market-by-order/spec.md` and `market-by-price/spec.md`.

Naming the family also repairs a strict-reading slip those two sentences carry. A feed's siblings exclude itself, so "appears in more than one sibling feed" wrongly exempts a Type ID carried by this feed and exactly one other. "More than one feed in the family" includes the speaker and states the rule that was always intended.

**`sibling` on its own is not banned** and keeps its ordinary English meaning. A sibling module, a sibling subcommand, and a Merkle proof sibling are all correct, the last being an externally defined term in the same class as `ARM64` and `Unix epoch`. Only the compound that names this family is replaced.

**`source` always takes a qualifier.** The word is banned bare — in specs, docs, plans, identifiers, CLI flags, config keys, metric names, and log fields. Write the qualified form and the sense is unambiguous:

| Qualified form | Means |
|---|---|
| `source_id` | Matching engine identity, as assigned by the Source ID Registry |
| `source_ts` / `source_timestamp_ns` | The venue's own clock |
| `source IP address` | The layer-3 sender of a datagram |
| `source publisher` | Which publisher a subscriber received given data from |
| `upstream source` | The external vendor or venue API a publisher reads |

These mean unrelated things and several share a prefix, which is why the qualifier is mandatory rather than stylistic. Where a bare use means none of the above, replace the word outright: an ingest transport choice is a `transport`, an input is an `input`, and a metrics sample origin needs its type renamed.

---

## Versioning

This glossary is versioned so that a spec, a repository, or a conformance pass can name the vocabulary it was written against. It has no wire format and no `Schema Version`; its version tracks the vocabulary itself. Releases are tagged `glossary/vMAJOR.MINOR.PATCH`, following the repository-wide scheme in the [Versioning Policy](./VERSIONING.md) with this document's own name as the prefix, because it is not in a directory of its own.

| Class | Example | Level |
|---|---|---|
| **Editorial** | Rationale, a note, an example, or wording, with no change to what the rules require | `PATCH` |
| **Additive** | Define a new term; ban a new word; record a new exception; refine guidance that no conforming pass has applied yet | `MINOR` |
| **Breaking** | Redefine a term, or change a replacement that conforming text already follows, so that text becomes wrong | `MAJOR` |

A `MAJOR` release is the expensive one, because prose, identifiers, and config keys across several repositories were written against the older ruling and each has to be revisited. Prefer recording an exception over redefining a term.

### Changes

**1.3.0** — widened **Matching engine**. The `1.2.0` test was one set of order-matching rules, under which two platforms at one venue running identical rules over disjoint instrument sets read as a single engine; they are now two. Added the guard separating that case from a capacity shard, which is a channel rather than an engine. Classified `MINOR` rather than `MAJOR` because the widening only ever splits further: every engine identified under `1.2.0` is still an engine, no Source ID Registry assignment changes, and no conforming text becomes wrong.

**1.2.0** — defined the **DoubleZero Edge family** and repointed the `sibling feed` ban at it, having established that plain `feed` loses the Type ID constraint the banned phrase carries. Widened that row to `sibling spec` and `sibling protocol`, which are the same concept in different spellings, and stated that bare `sibling` keeps its ordinary meaning. No pass had applied the `1.0.0` replacement, so nothing conforming to an earlier version becomes wrong.

**1.1.0** — recorded the first exceptions to bans that had been written as absolute: the `ARM64` architecture and the `ARM` vendor under `arm`; `snapshot stream` and `delta stream` under `stream`; `Trade Flags` bit 1 under `sweep`; `Unix epoch` under `epoch`; and `source of truth` under bare `source`. Each is an externally owned name or a fixed idiom rather than a sense of the word.

**1.0.0** — first release. The Identity, Transport, Protocol and roles, and Components terms, the banned-word table, and the `arm` and `source` notes.
