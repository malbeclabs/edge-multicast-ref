# Several channel instances of one feed specification, from one process

**Status:** draft, pending review
**Date:** 2026-09-09
**Applies to:** `rust/publisher/`, `rust/adapter/dz-adapter-core`
**Authority:** [`edge-feed-spec`](https://github.com/malbeclabs/edge-feed-spec), its [`GLOSSARY.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/GLOSSARY.md) and [`VERSIONING.md`](https://github.com/malbeclabs/edge-feed-spec/blob/main/VERSIONING.md); `reference-data/spec.md` §Publisher Behavior
**Builds on:** [2026-08-26-edge-publisher-crates-design.md](2026-08-26-edge-publisher-crates-design.md), [2026-09-02-venue-adapter-interface-design.md](2026-09-02-venue-adapter-interface-design.md)

---

## Naming

This repository is public. This document names no venue, venue repository,
venue crate, metric prefix or issue tracker, and gives no count of publisher
hosts. Where a number below came from a request, it is carried as a worked
example of a size, not as a description of a deployment.

`GLOSSARY.md` governs all vocabulary and overrides any local definition:
`datagram` never `frame`, `era` never `epoch`, `port role` with the tokens
`mktdata`/`refdata`/`snapshot`, and `channel` only for the `Channel ID` shard.

### The key is called `shard`, and three better-sounding words are wrong

The thing being named is *which channel an instrument belongs to*, stated by a
venue in a word a venue is allowed to use — because the venue must remain unable
to name a `Channel ID`. That constraint is not new; it is the venue adapter
interface's, quoted below. What is new is choosing the word.

| Candidate | Why not |
|---|---|
| `route` | The word is taken twice over. `[egress]` already means *the IP route to the group* by it — `pin` is documented as "an operator's override of route discovery" — so two unrelated senses would sit in one configuration document. Worse, `GLOSSARY.md` defines `path` as "one of several redundant routes carrying the same data", which makes "62 routes" read as sixty-two redundant paths carrying the same instruments. These carry **disjoint** instrument sets. The word says the opposite of what is meant. |
| `category` | `GLOSSARY.md` 1.4.0 defines `Category` as a feed-registry field and is explicit about what it is not: "an arbitration boundary, not a taxonomy … It is not a place to describe what a market is about, and a value that reads like a description invites exactly that use." Rows differing in category "carry disjoint instrument sets and never contest each other's tape or book" — but these shards are one instrument set spread for capacity, they arbitrate as one, and they share a `(venue, category)`. Using the registry's word for a deployment partition would put a second meaning on a field the venue-wide tape gate reads. |
| `lane` | Banned outright. `feed` or `path`. |
| `group` | Taken: the multicast group. |

`shard` is already this repository's own word for exactly this, in prose and in
doc comments, and it comes from the glossary's own definition of a channel: "a
logical shard of the instrument set, named by `Channel ID`". `FeedSection` says
it in one line today — *"The `Channel ID` shard. `channel` means this and
nothing else."* So a `[[feed]] shard` key names the shard, `channel_id` names
the `Channel ID` that identifies it on the wire, and the mapping between them is
the configuration's. A venue names a shard; it cannot name a `Channel ID`.

---

## Purpose

A publisher process today emits **at most one feed per specification.** It can
emit a top-of-book feed and a market-by-price feed, and that is the whole of its
range: `Config::resolve` refuses a second `[[feed]]` block of a specification it
has already seen, with `StartupError::DuplicateFeedSpec`.

Venues exist whose instrument set is sharded across many channels — the case
`GLOSSARY.md` describes as "a venue that spreads one instrument set across
several processes for capacity, and can move an instrument between them", which
"runs one engine published over several **channels**: the partition is a
deployment detail and `Channel ID` already carries it." Publishing such a venue
needs one process to operate many channel instances of one specification.

This design says how. The worked size throughout is the one that prompted it:
**62 channel instances from one process — 31 of each of two specifications** —
against the 2 a process operates today.

The change is small in the configuration document and large in what it makes
untrue. Four separate pieces of the runtime are correct today *only* because one
specification means one feed, and each of the four was written that way for a
reason. One is a gate rather than a check, one is the only one that fails
loudly, one is a latent state-corruption bug the moment the constraint lifts,
and one is a conformance requirement whose naive fix is a burst the
reference-data specification forbids. They are enumerated before the shape,
because the shape is mostly a consequence of them.

---

## Why one process, and not one process per channel

The obvious answer is to run the process that exists, once per channel, and
change nothing. It was the first thing considered and it is rejected — not on
elegance, and not on resource use.

| Cost | Class |
|---|---|
| A venue's request allowance for the catalogue poll is granted **per credential**, not per process. N processes divide one allowance among N uncoordinated pollers, each with its own timer, and the aggregate is a burst the venue rate-limits rather than a cadence the venue sized. | External, and not ours to change |
| The number of concurrent upstream sessions on one credential is a number the venue approves. N processes is N sessions, and the approval is a conversation with a counterparty, not a configuration change. | External, and slow |
| `Instrument ID` minting is persisted under `[refdata] state_dir`, which "takes exactly one writer; clearing it restarts the feed's identity history". N processes is N state directories and therefore N independent identity spaces. Every instrument is re-identified, once, and **`Instrument ID`s are never reused** — so it cannot be undone by putting the processes back together. | Irreversible |
| The catalogue poller is multiplied by N, or becomes a daemon the N processes read from. That daemon needs a local transport, and `dz_ingress_core::Kind` documents `uds` and `filetail` alike as "Not yet built." | Work that is not this work |

Only the third is technical, and it is the one that decides: a sharded venue
published from N processes has already spent an identity change it can never
spend back. The other three say the same thing more cheaply — the shape of the
process must follow the shape of the credential, and the credential is one.

The counter-argument worth stating is fault isolation: one process operating 62
channel instances darkens 62 when it dies. That is true, and it is already the
publisher's posture for 2 — the redundancy story in this family is a second
**path**, on a second host, publishing the same channels, which is unchanged by
this design and is the answer to that risk. A process split is a poor
substitute for a second path, and a second path makes the process split
unnecessary.

---

## The four fences

Each of these is a place where the code is correct today and would be wrong the
moment a second `[[feed]]` of one specification is admitted. Every one carries a
doc comment saying why it is the way it is, and the reasons are not stylistic.

| # | Where | What it does | What breaks when the constraint lifts |
|---|---|---|---|
| 1 | `config.rs`, `Config::resolve`'s `for section in enabled` loop | Refuses a second `[[feed]]` of a specification, `StartupError::DuplicateFeedSpec` | Nothing, on its own — and that is the danger. It is the gate holding fences 2–4 shut. |
| 2 | `publisher.rs`, `Feeds` | Two `Option` fields, one per specification | A second feed of a specification has nowhere to live |
| 3 | `era.rs`, `EraStore::begin_era<F>` | Resolves the era file from `F::NAME` | 31 channel instances draw from one counter, so which era each advertises is decided by document order and the whole set can be handed an era it has already published under |
| 4 | `publisher.rs`, `Publisher::tick` | Drains the definition tick **once** and packs it onto every feed's refdata port | Either a burst the pacer exists to prevent, or a `Manifest Seq` and a `Valid` that describe the process instead of the channel |

### Fence 1 is not a check, it is a gate

`DuplicateFeedSpec` reads like input validation. It is not. It is the single
mechanism that keeps fences 2, 3 and 4 from being wrong, and it is why they were
allowed to be written the way they were. Relaxing it is therefore not a small
first step that makes progress — it is the last step, and it is why this change
cannot be delivered in pieces. See *Why this cannot land incrementally*.

### Fence 2: two typed `Option`s, and the reason is the datagram path

`Feeds` is deliberately not a collection:

> Two typed fields rather than a collection, because `FeedPipeline` is generic
> over the wire feed — `Magic` belongs to the feed — so the two are different
> types and a `Vec` of them would need dynamic dispatch on the datagram path to
> buy nothing.

The reason survives the change and the shape does not. Several instances of one
specification are all the same Rust type, so a `Vec<FeedPipeline<MarketByPrice>>`
needs no dynamic dispatch at all: it is one indexed load on a monomorphized
type. What the field structure must keep is that the two specifications stay two
static types, which a per-specification vector does.

### Fence 3: the era store is a latent state-corruption bug, not an inefficiency

`GLOSSARY.md` defines the unit:

> **Channel instance** — One path's view of one channel, keyed `(source IP
> address, Channel ID, destination port)`. **The unit that owns a sequence
> series, a `Reset Count`, and a snapshot cycle.**

`begin_era<F>` is keyed on `F::NAME`, so `path_for` resolves `top-of-book.era`
and `market-by-price.era` and nothing else. Thirty-one market-by-price channel
instances in one process would draw their eras from that one file, and it is
worth being exact about what that does, because the obvious reading is wrong in
a way that makes the real failure easy to miss.

The store reads, increments and persists on **every call**, and the runtime
calls it once per `[[feed]]` block while composing the send paths. Thirty-one
blocks therefore get thirty-one *different* eras — 1 through 31 on the first
start, 32 through 62 on the second — so they do not share a `Reset Count`, and
nothing announces one channel's restart on another. This publisher has no
mid-session channel reset at all: `Sequencer` holds the era it was constructed
with for the life of the process, and `ChannelSequence::begin_era` is reachable
only from the codec's own tests. A reset here means a restart.

Three things follow, and they are worse than the sharing would have been because
each is invisible:

- **Which era a channel instance advertises is decided by its position in the
  document.** Reordering `[[feed]]` blocks — a text edit nobody would think of
  as a state change — swaps the eras two channels get, and hands one of them a
  value the other published under.
- **Every start advances the shared counter by N, not by 1**, so each channel's
  own era history is an arithmetic progression of stride N that wraps the `u8`
  after ⌈256/N⌉ starts — nine, at the worked size. The progressions stay
  disjoint only while N and the order hold. Add a shard, disable one, or move a
  block, and the strides shift and a channel is handed an era it has already
  published under. A subscriber detects a reset by inequality against what it
  last saw, so an era it already holds state under is a restart it is never told
  about: it keeps the stale book and applies fresh deltas onto it, which is the
  first thing the era module's own header says this mechanism exists to prevent.
- **`persisted_era<F>` can no longer answer the question it exists for.** It
  reads one file per specification, so no diagnostic and no check mode can say
  what era any individual channel instance is in.

Nothing detects any of it. Every datagram is well formed, densely numbered and
decodable. The failure is entirely in the meaning of one byte.

The store's own doc comment already contains the principle the fix needs, in the
constant that would otherwise be re-used:

> **Not zero.** … One is also what a newly enabled feed advertises, which is the
> reason the store is keyed per feed: a feed that has never published must not
> inherit an era from another feed that has published for months, or its first
> datagram claims a history it does not have.

That argument was made about feeds and is true of channel instances. Re-keying
the store extends it rather than revising it — and it makes one thing sharp that
the fix must design around: **a renamed era file reads as no era file**, which
resolves to `FIRST_ERA`. A publisher on era 7 whose file is renamed by an
upgrade restarts at era 1 and tells its subscribers nothing, which is exactly
the silent corruption the corrupt-file refusal exists to prevent, arrived at
through a rename. See *The era store* below.

The venue adapter interface design already recorded the discrepancy without
naming it, in one row of its ownership table: *"`Sequence Number`, `Reset
Count`, `Channel ID` … per channel instance, persisted per feed across
restarts."* The correct unit and the implemented unit are in the same sentence.

### Fence 4: the single drain, and the burst it prevents

> The definition tick is drained **once** and packed onto every feed's refdata
> port. Draining per feed would ask the pacer for the lap's debt twice and emit
> twice as much of the set per tick, which is the burst the pacer exists to
> prevent arriving through the caller.

The pacer hands out a lap's debt against the clock. Asking it once per feed does
not slice the lap N ways; it asks for the whole debt N times. With two feeds
that is a doubling. With 31 it is a burst of the whole published set, per tick,
against a rule that reads: "Publishers MUST NOT emit the entire published set as
a single burst."

So the naive per-feed drain is out. But so is the current single drain, for a
different reason and a stronger one.

---

## The specification settles the reference-data question

This is the part of the design that is not a design choice. `reference-data/spec.md`
makes reference data **per channel**, in normative text, four times over:

| Line | Text |
|---|---|
| 51 | `Valid` — "`1` when the channel has an established instrument set; `0` when the publisher is uninitialized or **the channel** is inactive." |
| 53 | `Manifest Seq` — "Increments every time the published instrument set changes **on this channel**" |
| 76 | "**A publisher operating a channel** adopting this supplement MUST:" — the heading over all seven publisher obligations |
| 84 | Obligation 4 — "Set the `Valid` flag to reflect **channel state**." |
| 99 | "A subscriber adopting this mechanism maintains the following state **per channel**" |
| 158, 174 | The sizing — "For **a channel** with **N** active instruments…", "operationally invisible at B-scale (~1,000 instruments **per channel**)" |

Packing one process-wide published set onto every channel's refdata port
therefore does not produce a publisher that is roughly right. It produces four
specific untruths:

- **`Manifest Seq` describes the process.** It would increment when any shard's
  set changed. A subscriber on a quiet shard sees its manifest sequence advance
  for an admission on a shard it cannot see, re-checks its set, finds it
  unchanged, and does so again on the next unrelated admission anywhere in the
  process.
- **`Valid` describes the process.** The field is defined against the channel's
  own state; a process-wide flag cannot say that one channel is uninitialized
  while the rest are established, which is precisely the state a process
  admitting 62 channels passes through at every start.
- **`Instrument Count` is 31× what any subscriber will ever see a message
  for.** The count is what a subscriber compares its collected definitions
  against.
- **Obligation 6 is inverted.** "Restart the definition cycle on `Manifest Seq`
  change" is a per-channel obligation. With one pacer, a single admission
  anywhere in the process restarts the definition cycle for all 62 channels at
  once — which is the burst of obligation 2, triggered by the correct handling
  of obligation 6.

And the bandwidth section is written per channel. At the worked size, the
reference-data rate on each channel would be 31× the specification's own
example, and the process's aggregate 961×, for a mechanism whose defining
property is that it is "operationally invisible".

**So the definition drain is partitioned per shard.** Not as a preference, and
not for bandwidth: because `Manifest Seq` and `Valid` have normative definitions
that the alternative contradicts.

Partitioning also restores fence 4's property rather than breaking it. Each
shard gets its own pacer, so each pacer is asked exactly once per tick for
exactly its own lap's debt; and within a shard the drained buffer is packed onto
**every feed of that shard** — which is fence 4's rule, unchanged, at its correct
scope. The rule was never "once per process". It was "once per pacer".

That the two feeds of one shard share one published set and one `Manifest Seq`
is already settled and already tested; the venue adapter plan recorded why when
two feeds first ran in one process: "one composed `ManifestSummary` is truthful
on both refdata ports" because "the datagram builder stamps the `Channel ID` at
push". Everything in that argument holds within a shard, and nothing in it holds
across shards.

---

## Why this cannot land incrementally

The natural decomposition is to relax the configuration check first and fix the
rest behind it. That decomposition produces a publisher that starts, runs, emits
well-formed datagrams, and is wrong in two normatively defined fields and one
byte of the datagram header.

- Relaxing fence 1 without fence 4 gives every channel the process's
  `Manifest Seq`, `Valid` and `Instrument Count`.
- Relaxing fence 1 without fence 3 gives every channel an era decided by its
  position in the document, drawn from one counter that advances by 31 a start,
  and a channel that is eventually handed an era it has already published under
  — which is a restart no subscriber is told about.
- Relaxing fence 1 without fence 2 does not compile, which is the only one of
  the three that protects itself.

None of those degrade gracefully and none of them are visible from the
publisher's own metrics or logs. So fence 1 is the **last** change, not the
first: `shard` is parsed, checked, named in errors and threaded through every
structure while `DuplicateFeedSpec` still refuses a second block — and the check
is relaxed in the same change that partitions the drain and re-keys the era
store. The plan is ordered on that constraint and says so at each task.

---

## The shape

Five changes. The venue-facing one is deliberately the smallest.

### Configuration: one optional key

```toml
[[feed]]
spec            = "market-by-price"
shard           = "alpha"          # optional; absent is the default shard
channel_id      = 11
source_id       = 1
multicast_group = "233.252.0.11"
mktdata_port    = 41100
refdata_port    = 41101
snapshot_port   = 41102
```

A `[[feed]]` block is still exactly one channel instance: its own `Channel ID`,
its own group, its own ports, its own sequence series, its own era. Nothing
about a block changes. What changes is that several blocks may share a `spec`,
distinguished by `shard`, and that blocks sharing a `shard` across
specifications carry one published set.

Absent means the default shard, which is what every document written today
states and what every document written today means. There is no migration.

Five checks at load, each for a failure that is otherwise silent:

| Check | Error | The failure it prevents |
|---|---|---|
| `(spec, shard)` unique across enabled blocks | `DuplicateFeedShard` | Two blocks describing one channel instance; which one is in force would depend on document order |
| `channel_id` unique across enabled blocks | `DuplicateChannelId` | Two channel instances writing one metric series — see *What it costs* |
| Every shard offers a block for every enabled specification | `ShardSpecsDisagree` | An instrument admitted to a shard with no top-of-book block has quotes that reach no wire and are counted only as unroutable |
| A shard name is one lowercase path component, at most 64 bytes of `[a-z0-9-]` | `UnsafeShardName` | The name becomes a path component in the era file and in the reference-copy fan-out socket. `EraStore::path_for` already checks this for feed names and says why: "a name that is not one path component is a directory traversal from a constant nobody thought of as one" |
| A shard name is not the reserved default token | `ReservedShardName` | Two spellings of one shard, with two era files |

The third check is the one worth arguing. It looks like an arbitrary symmetry
requirement, and it is the rule that makes `list_on` total: an instrument
belongs to one shard, and the runtime must be able to resolve that shard to one
channel instance *per specification it emits*. A document offering 31
market-by-price shards and one top-of-book shard leaves 30 of them with no
answer, and the runtime's only remaining option is to drop quotes it was given.
Refusing the document is the version of that an operator can fix.

### Admission: `list_on`, and the direction of the default

`dz-adapter-core`'s `ListingSink` gains one method:

```rust
pub const DEFAULT_SHARD: &str = "default";

pub trait ListingSink {
    /// Offer one instrument for publication on a named shard.
    fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef>;

    /// Offer one instrument on the default shard.
    fn list(&mut self, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef> {
        self.list_on(DEFAULT_SHARD, spec)
    }

    fn delist(&mut self, instrument: InstrumentRef);
}
```

An adapter states a shard by name and still cannot state a `Channel ID`, a
group, a port or a sequence number. That is not a new principle borrowed for the
occasion; it is the venue adapter interface's central rule — *"a venue …
decides nothing that a specification already decided"* — and its own list of
what must not be expressible names `Channel ID` explicitly: *"No `Instrument
ID`, no `Source ID`, no `Channel ID` … Not 'a venue should not' — there is no
parameter to pass one through."* A shard name is a venue word for a partition
the venue computes; the `Channel ID` remains the configuration's, and the
mapping between them is the operator's. Consistency with that design is an
argument for this shape and not merely an absence of conflict with it.

**`list_on` is required and `list` is defaulted, and the other direction is a
trap.** Defaulting `list_on` to `list` would mean that an implementor who did
not update silently admits every instrument to the default shard — 31 shards
collapsed onto one channel, with no error, no counter and no log. Requiring
`list_on` makes that omission a compile error. The cost is real and is paid
knowingly: adding a required method to a public trait breaks implementors, so
`dz-adapter-core` takes a major version. The additive-by-construction rule that
version bump appears to violate is a rule about **callers** — venues call
`ListingSink`, they do not implement it, and every implementor of it is in this
workspace. A venue's cost is a tag bump with no code change; the alternative's
cost is a silent misroute in production.

Two rules on the value, and both are refusals the registry already has a shape
for:

- **An unknown shard name is refused, not defaulted.** A fallback to the default
  shard would put an instrument on a channel nobody chose, in a way that reads
  as a working publisher. It joins `Refusal` alongside `Capped` and
  `ScaleRestated`, is counted in `Counts`, and is named by `last_refusal` —
  and it invents **no metric family**, because `dz-publisher-refdata`
  "constructs no metric … a series is not this crate's to invent". The
  alertable signal already exists and is already pre-created:
  `dz_publisher_refdata_instruments_current{channel_id}` sits at 0 for a shard
  nothing was ever admitted to, from startup, with no datagram required.
- **The shard is pinned at admission.** A re-offer naming a different shard is
  `Refusal::ShardRestated`, exactly as a re-offer restating an instrument's
  scale is `Refusal::ScaleRestated` — the registry already refuses a re-offer
  that changes something identity-bearing, and which channel an `Instrument ID`
  is live on is identity-bearing. Honouring it would be a *move* between
  channels, and no message in the family says "this instrument moved": a
  subscriber on the old channel would see it stop updating, which is
  indistinguishable from a market that went quiet. See *Non-goals*.

`AdapterContext` gains `shards()` alongside `feeds()`, so an adapter that knows
its classification set at construction can compare it against the configured set
and refuse at startup rather than declining instrument by instrument at the
first poll. `feeds()` returns the **distinct** specification set: it is answering
"which feeds does this publisher emit", and 31 repetitions of one answer is a
number an adapter might reasonably size something by.

### Reference data: one registry, N published sets

The registry stays one, and this is the part that must not be over-partitioned.
`Instrument ID` identity "can only be one thing", the state store "takes exactly
one writer", and `GLOSSARY.md` makes an `Instrument ID` "unique within a
channel" — so a single process-wide minting table is strictly stronger than the
specification requires and remains conformant. One table, one writer, one
`state_dir`, unchanged.

What partitions is the **published set**, per shard:

| Per shard | Process-wide |
|---|---|
| the published set and its `Instrument Count` | the `Instrument ID` minting table and its persistence |
| `Manifest Seq` | the selection policy's caps |
| `Valid` | `definition_cycle` and `idle_guard` (one stated answer, applied to every pacer) |
| one `DefinitionPacer` | the instrument table the lowerings borrow per call |
| one composed `ManifestSummary` per tick | |

`Publisher::tick` drains once **per shard** and packs each shard's buffer onto
that shard's feeds' refdata ports. The pacer count goes from one to the number
of shards, each asked once per tick, so the lap-debt property fence 4 protects
is preserved exactly.

`RegistryConfig` carries a `channel_id` today, taken from the first enabled
`[[feed]]` block, so that a manifest composed without a datagram builder still
has a truthful field. Under shards that value becomes the shard's, which is the
only value that could be truthful — and the redundant copy in the message body
continues to be overwritten by the builder from the datagram that carries it, so
the two cannot disagree per port.

One thing deliberately does not learn about shards: **`dz-publisher-lowering`.**
The shard is recorded on the registry's own published entry, not on
`InstrumentTable`, so the lowering — which the recorder links without the
runtime, its egress socket or its signal handling — keeps the exact shape it has
and Mode C re-lowering is unaffected. Resolving an instrument's shard is a
lookup on the registry beside the table borrow the hot path already takes.

The selection policy stays process-wide. `max_published` is a cap on what this
publisher publishes, and a per-shard cap is a different policy that nobody has
asked for; the caps are stated once and the counts are reported per channel,
which is the combination that lets an operator see a shard approaching a cap
that is not per shard.

### The send paths: two vectors, and one index

```rust
pub struct Feeds {
    pub top_of_book: Vec<FeedPipeline<TopOfBook>>,
    pub market_by_price: Vec<FeedPipeline<MarketByPrice>>,
}
```

Two fields, still typed, still monomorphized, no dynamic dispatch on the
datagram path — fence 2's reason, kept.

Routing is by index, never by name. The instrument table carries a shard index
per admitted instrument, minted at admission from the configured shard set, so
the hot path resolves a pipeline with one array read beside the instrument
lookup it already performs. No string comparison reaches `EventSink`, and
`EventSink` itself is unchanged: the runtime routes from the instrument's
admitted shard, so an adapter emits the same events it emits today and cannot
influence routing per event.

Four call sites take the index, and two of them are correctness rather than
efficiency:

- `Event::Quote`, `Event::Level`, `Event::Clear` — the shard's pipeline for the
  specification that carries the message.
- `Event::Trade` — the shard's top-of-book **and** market-by-price pipelines,
  from one lowered value. The wire's cross-specification requirement that `0x04`
  be byte-identical is held by there being one lowered trade and no second call
  site, which is unchanged; there are now two sends of one value per shard
  instead of two per process.
- `desynchronised` — the `Anchor Seq` is read off the pipeline as "where the
  feed is now". Read off the wrong shard's pipeline it is a sequence number from
  a different channel instance's series, which the subscriber will compare
  against its own. This is a wrong-answer bug, not a slow one.
- `snapshot` / `capture_and_send` — the same, for the anchor the framing
  carries.

`SnapshotRotation` becomes one rotation per shard that carries a snapshot port
role. `GLOSSARY.md` puts the snapshot cycle in the same sentence as the sequence
series and the `Reset Count` — all three are owned by the channel instance — and
`[[feed]] snapshot_cycle` is already a per-block key documented as "one full
pass of the snapshot rotation". A single process-wide rotation over 31 shards
would give each shard's instruments 1/31 of the configured rate, so a subscriber
joining mid-session on any one channel waits 31 cycles for its book. The key
would still be honoured on paper and would mean something else entirely.

The rotation derives its per-instrument tick as the cycle divided by the
published set size, freshly on every tick. Under shards that divisor must be the
**shard's** published count, not the process's, or every shard is paced as
though it held all 62 channels' instruments — which is the same 1/31 error
arriving through the arithmetic instead of through the cursor. So the registry
holds each shard's published membership, which is also what makes the count the
manifest reports and the count the rotation divides by one number rather than
two that can drift.

### The era store: keyed on the channel instance, and the default keeps its file

`begin_era` takes the shard alongside the feed, and the path becomes:

| Shard | Era file |
|---|---|
| the default shard | `<spec>.era` — **unchanged** |
| a named shard | `<spec>.<shard>.era` |

The default's file name is preserved deliberately, and it is the one part of
this design that exists purely to survive an upgrade. A renamed file reads as
*no file*, and no file resolves to `FIRST_ERA` — so a publisher that has been
running for months on era 7 would restart on era 1 and announce nothing, because
a subscriber detects a reset by inequality against what it last saw and 1 may
well be a value it holds. The store refuses to start on a corrupt file precisely
to avoid re-advertising an era subscribers already hold state under; reaching
the same outcome through a file rename would be that failure delivered by the
upgrade meant to be safe.

One file per channel instance is what buys back all three of the properties
fence 3 loses: each instance's era advances by exactly one per start, its
progression is its own history rather than a stride through a shared counter,
document order stops meaning anything, and `persisted_era` can answer for one
channel instance again.

A newly named shard has no file and therefore starts at `FIRST_ERA`, which is
correct and is what the constant's own documentation says it is for: a channel
instance that has never published must not inherit an era from one that has.

What does *not* change is that one block's three port roles share one era.
`FeedPipeline::new` hands the same `ResetCount` to all three because "a restart
is one event for the whole feed, and every series it carries restarts together".
That is a statement about one block, and one block is one shard's view of one
specification, so it survives untouched.

### The reference-copy fan-out, and the same argument a third time

`TeeConfig::destination` builds `<path>.<spec>.<port role>` and its doc comment
states exactly why both halves are there:

> a Unix datagram carries neither a destination port nor a group, and the diff
> this stream exists for is keyed on both. A recorder handed two roles on one
> socket, or two feeds' copies of one role on one socket, cannot attribute a
> datagram without decoding it — and decoding is the one thing a record path
> does not do.

Two *shards'* copies of one role on one socket is the same failure with a third
noun. The destination becomes `<path>.<spec>.<shard>.<port role>` for a named
shard and keeps `<path>.<spec>.<port role>` for the default one, for the same
upgrade reason the era file does — a recorder configured against the existing
socket path keeps working.

### Teardown

Unchanged in order, multiplied in extent. The order the runtime asserts today —
ingress stopped, admissions closed, the final manifest with `Valid = 0` on
refdata, `EndOfSession` on mktdata, both roles flushed — is a **per channel
instance** order, because every message in it is per channel instance. At the
worked size that is 62 final manifests and 62 `EndOfSession` messages, and the
ordering constraint that the manifest precedes `EndOfSession` holds within each
instance, not across them.

---

## What it costs

### Metric cardinality: 31× the pre-created channel surface

`PublisherMetrics::new` pre-creates a series for every declared `Channel ID`,
and it does so on purpose: a gauge that only appears once something has been
written to it is a gauge that cannot alert on the case where nothing ever is.
Four families are keyed on `channel_id`, and `port_roles` is at most three
across a publisher:

| Family | Keys | Today (2 channels) | At 62 channels |
|---|---|---|---|
| `dz_publisher_egress_sequence_current` | `port_role` × `channel_id` | 6 | 186 |
| `dz_publisher_egress_heartbeat_last_sent_timestamp_seconds` | `channel_id`, `mktdata` only | 2 | 62 |
| `dz_publisher_refdata_manifest_seq` | `channel_id` | 2 | 62 |
| `dz_publisher_refdata_manifest_valid` | `channel_id` | 2 | 62 |
| **Total pre-created, channel-keyed** | | **12** | **372** |

Exactly 31-fold, which is the point: the growth is linear in channel instances
and it is entirely at startup. One family moves in addition —
`dz_publisher_refdata_instruments_current` gains a `channel_id` label, going
from 1 series to 62 — because it is the gauge that mirrors the wire's
`Instrument Count`, and `Instrument Count` is per channel. A process total that
disagrees with all 62 wire values is a gauge every operator will read as the
channel's.

Three sibling families stay process-wide and the contrast is deliberate:
`definitions_emitted_total`, `new_listings_total` and `delistings_total` are
rates that aggregate honestly, nobody asks them a per-channel question that
`sequence_current{port_role="refdata"}` does not already answer, and labelling
them would buy 186 series for no query.

Everything else is unaffected: the port-role and message-type families do not
carry `channel_id` and do not grow, and the label values themselves cost
nothing, because the metrics crate already interns a decimal string for all 256
`Channel ID`s at first use rather than formatting one per call.

**This should be sized, not discovered.** A publisher's exposition grows by
roughly 420 pre-created series before its first datagram, and the `channel_id`
label on `instruments_current` changes what a bare selector returns for anyone
already graphing it.

### The refdata ceiling multiplies; the rate does not

`MAX_DEFINITION_DATAGRAMS_PER_TICK` is 1 — a ceiling on what one pacer may owe
in one 10 ms tick. It is per pacer, and there is now one pacer per shard, so the
**process** ceiling at the worked size becomes 31 refdata datagrams in a tick.
The **per-channel** ceiling is unchanged at one, and the per-channel one is what
the specification bounds: obligation 2 forbids emitting the published set as a
burst on a channel, and 31 datagrams spread across 31 channels is one datagram
each.

The steady-state rate does not multiply per channel either, because the pacer
emits against a lap debt rather than against a tick: at the specification's own
worked settings a shard of a thousand instruments owes about four datagrams a
second, which is what it owed when it was the only shard. What multiplies is the
process aggregate, linearly, which is the honest cost of publishing 31 channels
from one process rather than 31.

### `Channel ID` is a `u8`, so 256 is the ceiling

The uniqueness check makes the ceiling explicit rather than implicit. 62 fits
comfortably; a document asking for more channel instances than there are
`Channel ID`s is refused at load, naming the collision.

### The configuration document gets long

62 blocks of ten keys is a document an operator scrolls. The alternative — a
range or a count key that generates blocks, with groups and ports derived — is
rejected. `deny_unknown_fields` and one explicit block per channel instance is
what makes a wrong `Channel ID` a conversation an operator can have with a file
they can read, and a derived group assignment is exactly the class of implicit
value the audit that shaped this document found the most defects in. The length
is the cost of the property.

### The hot path

One array read per event to resolve the shard index, on a path that already
performs an instrument lookup. `EventSink` is unchanged, the lowering is
unchanged, and no allocation or comparison is added.

### The idle guard still measures the publisher

One guard, publisher-wide, as today — the silence it measures is "upstream
delivering and nothing reaching any wire". At 62 channel instances it will not
notice one silent shard among 61 busy ones. That is a deliberate limit rather
than an oversight: a per-shard guard whose exit ends the process would darken 61
healthy channels for one quiet one, and the series that shows a single quiet
channel is `dz_publisher_egress_sequence_current{channel_id}`, which is
pre-created from startup and therefore alertable without a single datagram
having been sent.

---

## What is explicitly not changing

| | |
|---|---|
| `EventSink` and the event grain | An adapter emits what it emits today. Routing is the runtime's, from the instrument's admitted shard |
| The wire | No feed spec change, no new message type, no new field, no `Schema Version` bump |
| `Source ID` | One per process. "The lowering takes it once and every message a process sends carries it" — 62 channel instances of one matching engine share one |
| The `Instrument ID` table | One, process-wide, one writer, one `state_dir` |
| `definition_cycle`, `idle_guard` | Still one stated answer per publisher, still refused when two blocks state different values. One answer now paces N pacers |
| `[refdata.selection]` | Process-wide caps |
| `[[source]]`, `[ingress]`, `[adapter]` | Untouched. One adapter, one primary upstream source, N shards |
| The datagram cap and the framing | `dz-edge-core`'s, as always |
| The `Channel ID` | The configuration's. A venue names a shard and cannot name a `Channel ID` |
| Redundancy | A second **path** on a second host, unchanged and unaffected |

---

## Decisions

**The shard is the unit of reference data, and the channel instance is the unit
of sequencing.** Both come from documents that already say so — the
reference-data specification for the first, `GLOSSARY.md` for the second — and
the two differ: a shard's two feeds share a published set and a `Manifest Seq`
while owning separate sequence series, `Reset Count`s and snapshot cycles. That
distinction is the whole design, and getting it backwards in either direction is
one of the two failures this document exists to prevent.

**A venue names a shard; the configuration names the `Channel ID`.** This is the
venue adapter interface's rule applied unchanged. The alternative — a
`Channel ID` on `list` — was rejected there in a sentence worth re-reading: "an
interface that lets a venue supply an `Instrument ID`, a scaled integer, a flags
byte or an `Action` is an interface that will be supplied a wrong one."

**`list_on` is required, not defaulted.** A defaulted `list_on` fails by
misrouting silently. A required one fails at compile time and costs a major
version of a crate no venue implements.

**Duplicate `Channel ID`s are refused rather than disambiguated by a new metric
label.** Two channel instances of one process may legally share a `Channel ID`
and differ by destination port, and the metric families keyed on
`(port_role, channel_id)` cannot tell them apart. The two available fixes are to
forbid the collision or to add a label to four normative families. Forbidding
costs an operator nothing — `Channel ID` is theirs to assign and 256 > 62 — and
adding a label changes series identity for every dashboard that already exists.

**The default shard keeps every existing name.** The era file, the reference-copy
fan-out socket, and the meaning of a document with no `shard` key. An upgrade
that renames state is an upgrade that resets it, and this state is the one whose
reset is invisible.

**One process, because the credential is one.** The technical costs of N
processes are all payable. The identity cost is not: `Instrument ID`s minted in
N state directories cannot be merged back, and they are never reused.

---

## Non-goals

**No move between shards.** The shard is pinned at admission. A live move would
need a delisting on the old channel and an admission on the new one, keeping the
`Instrument ID` — which is legal, since ids are never reused and are unique
within a channel — plus a snapshot on the new channel to bootstrap it, which the
periodic rotation already provides. What it would not have is any way to tell a
subscriber on the old channel that the instrument moved rather than stopped.
That is a wire question, and it belongs to the specification before it belongs
here.

**No per-shard selection policy, and no per-shard idle guard.** Both are
publisher-wide today for stated reasons and neither reason weakens. See above
for what watches a single quiet channel instead.

**No configuration generation.** The document lists its channel instances.

**No feed spec changes**, and no new metric family. One existing family gains a
label; nothing is renamed and nothing is proposed.

**No change to the redundancy model.** A second path is a second host.
