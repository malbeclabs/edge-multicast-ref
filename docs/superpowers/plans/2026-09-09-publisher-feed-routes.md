# Several channel instances of one feed specification — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** One publisher process operating many channel instances of one feed
specification, with reference data, sequencing, eras and snapshot cycles all
keyed on the unit the specification and the glossary say owns them — and with a
venue able to say which shard an instrument belongs to without being able to say
which `Channel ID` that is.

**Spec:** `docs/superpowers/specs/2026-09-09-publisher-feed-routes-design.md`

**Tech stack:** Rust 2021, workspace MSRV. No new dependency anywhere, and none
at all in `dz-adapter-core` — see *Global constraints*. No async below
`dz-ingress-*`; nothing in this plan touches a transport.

---

## Scope

One plan, twelve tasks, all in this repository. No venue repository changes at
any point: an adapter that never names a shard compiles unchanged against every
task, and the last task is the one that lets a document ask for a second block
of one specification.

The worked size throughout is the design's: **62 channel instances from one
process, 31 of each of two specifications**, against the 2 a process operates
today.

---

## The ordering constraint, which is the whole shape of this plan

`Config::resolve` refusing a second `[[feed]]` of one specification —
`StartupError::DuplicateFeedSpec` — is not input validation. It is the gate that
makes three other pieces of the runtime correct, and the design says why at
length. Relaxing it early does not produce a publisher that is approximately
right:

| Relaxed without | The publisher | What a subscriber sees |
|---|---|---|
| the per-shard drain | starts, runs, emits well-formed datagrams | `Manifest Seq` that increments for an admission on a channel it is not bound to, `Valid` that describes the process, `Instrument Count` 31× what it will ever get a message for |
| the re-keyed era store | starts, runs, emits well-formed datagrams | an era decided by the block's position in the document, drawn from one counter that advances by 31 a start — and eventually an era this channel has published under before, which is a restart nothing tells it about |
| the vectored `Feeds` | does not compile | — |

Only the third protects itself. So **task 9 is the gate, and tasks 1–8 all leave
`DuplicateFeedSpec` in force.** Each of them is a merge that changes no observed
behaviour, because with one shard configured every partition is a partition of
one. That is what makes them independently gateable: the suite that passes today
must still pass, unchanged, at the end of every task before 9.

A task that finds itself needing to relax the check early has hit the boundary
this plan is ordered around, and the answer is to stop and re-order rather than
to relax it.

---

## Global constraints

- **Vocabulary:** `GLOSSARY.md` governs every identifier, comment, test name,
  config key and commit message. `channel` only for the `Channel ID` shard;
  `datagram` never `frame`; `era` never `epoch`; `feed` or `path` never `lane`;
  `fan-out` for the reference copy; `source` never bare; and the word the
  glossary bans outright in every sense stays out of prose, identifiers, test
  names and comments alike. The new key is `shard` and the design's Naming
  section says why it
  is not `route`, `category` or `lane` — a task that renames it has to re-argue
  that section.
- **No venue names.** This repository is public. No commit message, comment,
  test name, fixture or configuration example names a venue, a venue repository,
  a venue crate or an issue tracker, or gives a count of publisher hosts.
- **`dz-adapter-core` takes no new dependency.** `thiserror`, and nothing else.
  Its manifest test enforces this; a task that needs a second stops and asks.
- **Nothing spec-decided becomes expressible.** A shard name is a venue word. No
  `Channel ID`, group, port, sequence number, `Reset Count` or era reaches any
  parameter an adapter touches, at any task.
- **Lints:** `#![forbid(unsafe_code)]` and the workspace clippy set.
  `cargo clippy --all-targets -- -D warnings` and `cargo fmt --all --check` pass
  at every task boundary, on CI's toolchain rather than on a local stable that
  may be older.
- **Tests run in both profiles.** CI runs `cargo test --all` and
  `cargo test --all --release`; `debug_assert!` differs.
- **No network, no privilege, no venue, in any test in this plan.** Every one is
  reachable with the fakes the suite already has: recording sinks behind
  `DatagramSink`, injected clocks, `MemoryStore`, and a temporary directory for
  the two tasks that touch the filesystem. Nothing here needs a socket, and
  `scripts/check-public-repo-rules.sh` runs before the toolchain does.
- **Every test must be shown to kill its mutant.** Revert the change, watch the
  new test fail, restore it. A test that passes against the unfixed tree is a
  test that asserts nothing, and this plan adds several whose subject is an
  absence — a series that is *not* shared, a file that is *not* renamed.
- **Commit before touching the tree with anything that discards work.** Two of
  these tasks rewrite files under a state directory in tests.

---

## The pieces where the obvious implementation is the wrong one

Stated up front, because each was found by reading the code rather than by
reasoning about it, and each is a task below that would otherwise be written
wrong.

| Piece | Why the obvious version is wrong |
|---|---|
| `list_on` defaulted to `list` | an implementor who did not update admits every instrument to the default shard — 31 shards collapsed onto one channel, no error, no counter, no log. The default goes the other way: `list_on` is required, `list` is defaulted, and the crate takes a major version |
| the era file for the default shard | a renamed file reads as *no file*, which resolves to `FIRST_ERA`. A publisher on era 7 would restart on era 1 and announce nothing — the exact corruption the corrupt-file refusal exists to prevent, delivered by the upgrade meant to be safe. The default shard keeps `<spec>.era` |
| partitioning the definition drain | per **feed** asks each pacer for the whole lap's debt once per feed, which is fence 4's burst. Per **shard** is the unit: one pacer, asked once, packed onto every feed of that shard |
| partitioning the registry | one registry per shard is N `Instrument ID` spaces and N writers on one state directory — and the single-writer guard would refuse the second, so it fails loudly at startup rather than subtly. One registry, N published sets |
| `SnapshotRotation::tick` | it divides the cycle by the published set size, read fresh every tick. Divided by the *process's* count, every shard is paced 31× slow while the key still reads as honoured |
| the `Anchor Seq` | `desynchronised` and `snapshot` read it off `feeds.market_by_price`. Off the wrong shard's pipeline it is a number from another channel instance's sequence series, which the subscriber compares against its own. A wrong answer, not a slow one |
| where the shard is recorded | on `InstrumentTable` it drags `dz-publisher-lowering` — and therefore the recorder's Mode C re-lowering, which links the lowering without the runtime — into a change that buys them nothing. It goes on the registry's own published entry |
| `Config::channel_ids()` | it sorts and **dedups**, so two blocks sharing a `Channel ID` silently pre-create one series set and both write to it. Nothing checks uniqueness today; 62 blocks make the collision likely rather than theoretical |
| `Config::feed_specs()` | one entry per block. At the worked size an adapter is handed `market-by-price` thirty-one times through `AdapterContext::feeds`, and it is answering "which feeds does this publisher emit" |
| `RegistryConfig.channel_id` | taken from `config.feeds.first()`. The builder overwrites the redundant copy per push, so it is invisible on the wire — but the field exists for the caller that encodes without a builder, which is exactly the caller it would lie to |
| the three port roles of one block | they share one era on purpose — "a restart is one event for the whole feed". That is a statement about one block, one block is one channel instance, and splitting it would be re-keying the wrong thing |
| relaxing `DuplicateFeedSpec` first | see *The ordering constraint*. It is task 9, not task 1 |

---

## Tasks

### 1. `shard` in the document: parsed, checked, and still singular

- [x] `FeedSection::shard: Option<String>`, `#[serde(default)]`, under the
      existing `deny_unknown_fields`.
- [x] A checked `ShardName` on the resolved `Feed`, produced by a constructor
      that owns the invariant — the pattern `SourceId` and `SelectionPolicy`
      already follow.
- [x] `DEFAULT_SHARD`, one token, in `dz-adapter-core` so that the boundary and
      the configuration cannot spell it differently. An absent key resolves to
      it.
- [x] `StartupError::UnsafeShardName` — one lowercase path component, at most 64
      bytes of `[a-z0-9-]`, checked at load. The same rule `EraStore::path_for`
      already applies to a feed name, checked in the one place the value enters
      the process, because it becomes a path component in two places later.
- [x] `StartupError::ReservedShardName` for a block spelling the default token
      explicitly: two spellings of one shard would be two era files.
- [x] `StartupError::DuplicateChannelId`, naming both blocks. New, and it closes
      a hole that exists today.
- [x] `Config::shards()` — the distinct shard names, in document order.
- [x] `Config::feed_specs()` returns the **distinct** specification set;
      `AdapterContext::shards()` added beside `feeds()`.
- [x] `DuplicateFeedSpec` unchanged and still in force.

**Test** (`dz-publisher-runtime/tests/config_document.rs`, no filesystem — every
case goes through `Document::parse` on a string): a name with an underscore, a
slash, an upper-case letter and a 65-byte name are each refused, and the message
names what would have been accepted; the default token spelled explicitly is
refused; two enabled blocks sharing a `channel_id` are refused naming both; a
document with no `shard` key resolves both blocks to the default shard; and the
control — `two_feed_blocks_naming_one_specification_are_refused` passes
unchanged, asserted by name so that a later task cannot delete it by accident.

> The `DuplicateChannelId` test is the one to check the mutant on: remove the
> check and it must fail. `channel_ids()` dedups, so without the check the
> document loads, one series set is created, and nothing anywhere says so.

---

### 2. `ListingSink::list_on`, and the shard set the registry checks it against

- [x] `fn list_on(&mut self, shard: &str, spec: &InstrumentSpec<'_>) -> Option<InstrumentRef>`
      on `ListingSink`, **required**; `list` becomes the defaulted method,
      forwarding with `DEFAULT_SHARD`.
- [x] `dz-adapter-core` takes a major version. Recorded in its changelog as
      breaking for implementors and additive for callers, with the one-line
      reason: every implementor of `ListingSink` is in this workspace, and a
      venue calls it.
- [x] `Registry` holds the configured shard set, handed to it through
      `RegistryConfig`.
- [x] `Refusal::UnknownShard` and `Refusal::ShardRestated`, and their `Counts`
      entries. No new metric family — `dz-publisher-refdata` constructs none,
      and the alertable signal is task 10's
      `dz_publisher_refdata_instruments_current{channel_id}` sitting at 0.
- [x] The unknown name is logged once per distinct value, not once per poll: an
      adapter may re-offer its whole set every second.

**Test** (`dz-publisher-refdata/tests/identity.rs` and a new
`tests/shards.rs`, `MemoryStore` and an injected clock): `list` and
`list_on(DEFAULT_SHARD, ..)` admit the same instrument to the same handle; an
unknown shard name refuses with `UnknownShard` and mints no `Instrument ID` —
asserted by restarting the registry over the same store and finding `next_id`
unmoved, because a refusal that still consumed an id is the failure worth
naming; a re-offer naming a different shard is `ShardRestated` and leaves the
instrument where it was. Plus a compile-level control: a fake `ListingSink` in
the test module that implements only `list_on` compiles, and one that implements
only `list` does not — the second as a `trybuild`-style doc assertion or, if
that is too much machinery for one property, as a comment on the trait pointing
at the design's paragraph.

---

### 3. The registry partitions its published set

- [x] Per shard: the published membership, `Instrument Count`, `Manifest Seq`,
      `Valid`, and one `DefinitionPacer`.
- [x] Process-wide and untouched: the `Instrument ID` minting table, `next_id`,
      the persisted record, the state-directory claim, `SelectionPolicy`, the
      `InstrumentTable` the lowerings borrow.
- [x] The shard is recorded on the registry's published entry. `InstrumentTable`
      and `dz-publisher-lowering` are not touched.
- [x] `definition_tick` takes a shard and drains that shard's pacer;
      `manifest()` takes a shard and composes that shard's summary with that
      shard's `channel_id`.
- [x] `RegistryConfig` carries the per-shard `Channel ID` rather than the first
      block's.
- [x] `seeding_complete` stays process-wide: one poll establishes the set.

**Test** (`dz-publisher-refdata/tests/cycle.rs`, extended): with two shards
configured, an admission on one bumps that shard's `Manifest Seq` and **not** the
other's, and the other's `Instrument Count` is unmoved — the assertion the
design's four specification citations reduce to; one lap of one shard's pacer
covers that shard's instruments exactly once and none of the other's; and the
existing `no_tick_can_be_made_to_emit_the_whole_published_set` is re-run per
shard so the anti-burst property is asserted at its new scope rather than
retired. With one shard configured, every existing test in the file passes
byte-for-byte unchanged, which is the task's real gate.

---

### 4. The era store keyed on the channel instance

- [x] `begin_era` and `persisted_era` take the shard alongside the feed.
- [x] The path is `<spec>.era` for the default shard and `<spec>.<shard>.era`
      for a named one. The shard component is already known safe from task 1;
      `path_for`'s own check stays as the second line of defence, because it is
      the function that builds the path.
- [x] `run.rs`'s two call sites pass the block's shard.

**Test** (`dz-publisher-egress/tests/era_persistence.rs`, a temporary
directory): a store holding `top-of-book.era` at 7 — written as the file, not
through the API, so the test describes an existing installation rather than one
this code just made — returns 8 for the default shard and **1** for a named one;
two named shards of one specification advance independently, and advancing one
leaves the other's file byte-identical; `persisted_era` answers for one shard
rather than for the specification; and the property fence 3 actually loses,
asserted directly: across two starts, each shard's era advances by exactly one,
and the values are the same whichever order the shards are begun in — which is
the order-dependence test, and it needs the two starts because a single start
cannot tell a stride of one from a stride of two. The existing `OtherFeed` test,
which already asserts that two feeds do not share an era, is kept and joined by
its shard twin.

> The mutant to kill is the rename. Change the default shard's path to
> `<spec>.default.era` and the first assertion must fail with 1 where 8 was
> expected. If it does not, the test is reading a store it created itself.

---

### 5. The reference-copy fan-out socket keyed on the shard

- [x] `TeeConfig::destination` takes the shard:
      `<path>.<spec>.<shard>.<port role>` for a named shard,
      `<path>.<spec>.<port role>` unchanged for the default one.
- [x] The doc comment gains the third noun. Its existing argument — a Unix
      datagram carries neither a destination port nor a group, so a recorder
      handed two things on one socket cannot attribute a datagram without
      decoding it — is the same argument for shards and should be extended, not
      rewritten.

**Test** (`dz-publisher-runtime/tests/config_document.rs`, pure path
arithmetic, no socket): the default shard's destinations are exactly the five
strings the current doc comment lists; a named shard's are distinct from them
and from each other across two shards and three port roles; and the suffix is
appended to the last component rather than becoming a child directory, which is
the property the existing `OsString` construction exists for.

---

### 6. `Feeds` as two vectors, and routing by index

- [x] `Feeds { top_of_book: Vec<FeedPipeline<TopOfBook>>, market_by_price: Vec<FeedPipeline<MarketByPrice>> }`.
      Two typed fields, no dynamic dispatch on the datagram path — fence 2's
      reason, kept.
- [x] A shard index resolved once per event beside the instrument lookup. No
      string comparison on the hot path.
- [x] `Event::Quote`, `Level`, `Clear` route to the shard's pipeline for the
      specification that carries them. `Event::Trade` goes to both of the
      shard's pipelines **from one lowered value** — one lowering, no second
      call site, unchanged.
- [x] `channel_ids`, `dark_transmitter` and `dropped_sinks` iterate.
- [x] `EventSink` unchanged. Not "unchanged in spirit" — the trait, its
      methods and its parameters are untouched, and a task that changes one has
      moved routing to the adapter.

**Test** (`dz-publisher-runtime/tests/depth_end_to_end.rs`, recording sinks):
with one shard, `one_trade_reaches_both_feeds_as_the_same_bytes` and
`a_publisher_emitting_both_feeds_routes_each_event_to_the_feed_that_carries_it`
pass unchanged — this task is a refactor and the existing suite is its gate. One
new test asserts the negative that will matter at task 9: a quote for an
instrument admitted to shard A appears on shard A's top-of-book mktdata sink and
on **no other sink**, asserted as an emptiness across every sink in the harness
rather than as a presence on one.

---

### 7. The snapshot rotation and the anchors

- [x] One `SnapshotRotation` per shard carrying a snapshot port role.
- [x] The per-instrument tick divides the cycle by **that shard's** published
      count, from the membership task 3 holds.
- [x] `desynchronised` reads the `Anchor Seq` from the instrument's own shard's
      market-by-price pipeline.
- [x] `snapshot` and `capture_and_send` likewise, and `SnapshotError::NoDepthFeed`
      keeps its meaning per shard: an instrument on a shard whose blocks carry no
      snapshot port role.

**Test** (`rotation.rs`'s unit tests and `depth_end_to_end.rs`): the existing
`the_tick_is_the_cycle_divided_by_the_published_set` gains a sibling asserting
it is divided by the shard's count, with two shards of unequal size, so the
31×-slow bug is the thing the test fails on; and a reset on an instrument in
shard B produces an `InstrumentReset` whose anchor equals **shard B's**
mktdata sequence, with shard A's sequence deliberately set to a different value
first — a test that cannot pass by accident.

---

### 8. Teardown and the metric bridge, per channel instance

- [x] `shut_down` sends one final manifest with `Valid = 0` and one
      `EndOfSession` **per channel instance**, in the existing order within each:
      admissions closed, manifest, `EndOfSession`, flush. The manifest precedes
      `EndOfSession` because a subscriber that stops at the terminal statement
      would otherwise never see it, and that is a per-instance ordering.
- [x] `forward_counts` writes `manifest_seq` and `manifest_valid` from the
      shard that owns each `Channel ID`, not from one registry-wide value.

**Test** (`depth_end_to_end.rs`): with two shards,
`shutting_down_a_depth_publisher_ends_every_feeds_mktdata_channel` extends to
every channel instance, and each final manifest carries **its own** shard's
`Manifest Seq` — asserted with the two shards holding different published sets,
because equal sets would let a process-wide value pass.

---

### 9. The gate: a second `[[feed]]` of one specification

- [x] `DuplicateFeedSpec` becomes `DuplicateFeedShard { spec, shard }`, keyed on
      the pair.
- [x] `StartupError::ShardSpecsDisagree`, naming the shard and the specification
      it has no block for. This is the check that makes `list_on` total: an
      instrument admitted to a shard with no top-of-book block would have quotes
      that reach no wire and are counted only as unroutable.
- [x] `run.rs` composes one pipeline per block instead of assigning into an
      `Option`.
- [x] The design's *Why this cannot land incrementally* section is the review
      note on this task: it is the change that makes tasks 3, 4 and 7 load-bearing
      rather than latent.

**Test** (`config_document.rs` and a new
`dz-publisher-runtime/tests/shards_end_to_end.rs`): two blocks of one
specification with distinct shards resolve; two with the same shard are refused;
a shard with a market-by-price block and no top-of-book block is refused naming
both; and end to end, with two shards and both specifications — four channel
instances — an admission on one shard bumps one `Manifest Seq` and leaves the
other three unmoved, each channel instance starts in its own era, and each
refdata port carries only its own shard's definitions.

> That last assertion is the plan's centre. Written as *shard A's refdata port
> carries A's definitions*, it passes against a publisher that packs everything
> everywhere. It has to be written as *and none of B's*.

---

### 10. Metrics: sizing, and the one label that moves

- [x] `dz_publisher_refdata_instruments_current` gains a `channel_id` label and
      is pre-created per declared `Channel ID`. It is the gauge that mirrors the
      wire's `Instrument Count`, and that is per channel.
- [x] `definitions_emitted_total`, `new_listings_total` and `delistings_total`
      stay process-wide, and the reason goes in their doc comments so the next
      reader does not undo it.
- [x] No family is renamed and none is added, so `NORMATIVE_NAMES` is unchanged
      — which is itself the assertion that this task added nothing to a set
      somebody else owns.

**Test** (`dz-publisher-metrics/tests/precreated_at_startup.rs`): 62 declared
`Channel ID`s pre-create exactly 372 channel-keyed series across the four
families plus 62 for `instruments_current`, counted from the gathered
exposition rather than from arithmetic in the test; and 2 declared ids give 12
plus 2, so the ratio is asserted rather than asserted-about. The existing
`declared_channel_ids_render_at_zero_from_startup` extends to the new family.

---

### 11. The documents that have to stay true

- [x] `BRINGING-UP-A-FEED.md` gains `shard` in its configuration block, marked
      optional, with one line on when an operator needs it. That guide is the
      one document in this repository that must stay true rather than be a
      record of a date, and a key it does not mention is a key an operator will
      not know exists.
- [x] The publisher crate READMEs describe the shard as the unit of reference
      data and the channel instance as the unit of sequencing, in the design's
      own words, because those two being different is what everything above
      turns on.
- [x] `docs/README.md` carries the row for this pair. Landed with the spec and
      the plan, not here.

**Test:** `scripts/check-public-repo-rules.sh`, which runs before the toolchain
in CI, plus a read of the new prose against the glossary's banned-word table.
The words most likely to slip in here are the three the Naming section rejected.

---

### 12. It has to run

- [x] `examples/replay.sh` extended to a document with **four shards of one
      specification** — four channel instances — over the built-in
      normalized-event encoding, read by **this repository's Go subscriber**.
      Two shards and both specifications is what this bullet asked for and it
      is refused at startup: the built-in record adapter holds no book, so it
      answers no snapshot and `AdapterRegistry` refuses it beside a
      `market-by-price` block rather than publish deltas nothing can
      resynchronise. Four blocks of one specification is the arrangement the
      duplicate-specification gate refused, which is the half of the change
      this run exists to exercise; the depth half has no venue in this
      repository to run it. See *It ran*.
- [x] One block names no shard, so the run covers the upgrade as well as the
      feature: the default shard's era file has to keep the name it has always
      had while the other three take one of their own.
- [x] The subscriber's own output is the assertion: four channel instances,
      each with its own sequence series starting at 0, its own `Reset Count`,
      its own manifest, and only its own instruments' definitions.
- [x] Run twice, because an era is only observable across a restart: every
      channel instance's era advances by exactly one, independently.

**Test:** the script itself, run by hand and recorded here in the plan's own
"It ran" section, as the venue adapter plan does. It is deliberately not a CI
gate: it needs a multicast group and two processes, and a test that can only run
by hand must not be able to fail the build. Everything it exercises is already
covered by task 9's end-to-end test against recording sinks — which is exactly
the split that made the last plan's real run worth doing, because what it found
was two things no fake could have.

---

## It ran

Everything above is tested against fakes — sockets behind traits, injected
clocks, recording sinks — which is what makes the suite run unprivileged with no
network, and which means all of it was a hypothesis until four channels left
four sockets.

`rust/publisher/dz-publisher-runtime/examples/replay.sh` is that run. It writes
a document with four `[[feed]]` blocks of one specification, records a directory
of normalized-event payloads for eight instruments, runs `run()` over them —
the real config, the real registry, the real built-in adapter, the real
lowering, real multicast sockets, the real teardown — and reads the other end
with four instances of **this repository's Go subscriber**, one per channel
instance. Then it does the whole thing again, because an era is only observable
across a restart.

```text
  channel  0 (the default shard)  38 messages, era 2, seq from 0, definitions REPLAY-D1,REPLAY-D2
  channel  1 alpha                38 messages, era 2, seq from 0, definitions REPLAY-A1,REPLAY-A2
  channel  2 beta                 38 messages, era 2, seq from 0, definitions REPLAY-B1,REPLAY-B2
  channel  3 gamma                38 messages, era 2, seq from 0, definitions REPLAY-G1,REPLAY-G2

  four eras, one per channel instance: [2, 2, 2, 2]
  each advanced by exactly one across the restart

  tee.top-of-book.mktdata       20 datagrams, longest 84 bytes, first magic 0x5a44
  tee.top-of-book.alpha.mktdata 20 datagrams, longest 84 bytes, first magic 0x5a44
```

Each subscriber read its own `Channel ID` and no other, its own sequence series
from 0, a `Reset Count` equal to its own era file, and **exactly its own
shard's two definitions** — stated as an equality, because *carries mine*
passes against a publisher that packs everything onto everything. Four era
files sit under the state directory, the default shard's still
`top-of-book.era`, and each advanced by one rather than by four, which is what
a shared counter would have done.

Two instruments per shard rather than one, for the end-to-end test's reason: a
packing publisher owes one definition per lap and would still put exactly one
symbol on each port, so with one each the failure reads as *carries none of its
own* and the exclusion is never reached.

**What could not be run, and why the plan asked for it anyway.** Task 12 asked
for two shards and *both* specifications. The built-in record adapter holds no
book, so it answers no snapshot, and `AdapterRegistry` refuses it beside a
`market-by-price` block — a depth feed publishing deltas with no snapshot leaves
a mid-session joiner wrong at every price it never saw an update for,
indefinitely. That refusal is right and predates this plan; what it means is
that a real depth run needs a venue with a book, and this repository has none.
Four shards of one specification exercises everything the gate at task 9
lifted; the depth path's shard routing is covered where it can be observed, by
`depth_end_to_end.rs` and `shards_end_to_end.rs` against recording sinks.

**Two things only the real run could find.**

`GROUPS` is bash's own array of the invoking user's group ids, and assignments to
it are silently ignored. The generated document therefore carried
`multicast_group = "1000"`, and startup refused it by name — the failure this
publisher is built to produce instead of starting on something that cannot be a
group. A script that had defaulted the key, or a publisher that had parsed it
loosely, would have joined nothing and looked healthy.

And the reference-copy fan-out was still keyed on the feed and the port role
alone. Four channel instances fanned out to five sockets, two shards' copies of
one role arriving on one — which a recorder cannot attribute without decoding,
the one thing a record path does not do. That is task 5, and the tasks-1-to-8
commit had missed it: `tee.top-of-book.alpha.mktdata` did not exist until it
landed, and the run's second reference stream is the assertion that it does.

## Acceptance

The plan is done when:

1. a document with 62 `[[feed]]` blocks — 31 shards × 2 specifications — starts,
   and one with two blocks naming one `(spec, shard)` pair, or a duplicate
   `Channel ID`, or a shard missing a specification's block, is refused at load
   with a message naming both offenders;
2. an adapter admits an instrument with `list_on("<a shard>", spec)` without
   naming a `Channel ID`, a group, a port, a sequence number or an era anywhere
   in its own code, and an adapter that never names a shard compiles and runs
   unchanged;
3. an admission on one shard changes one shard's `Manifest Seq`, `Valid` and
   `Instrument Count`, and no other shard's;
4. a restart advances every channel instance's era by exactly one, independently,
   with one era file each under the state directory, and reordering the
   `[[feed]]` blocks changes none of them — with an existing default-shard era
   file carried across the upgrade rather than restarted;
5. each channel instance's refdata port carries its own shard's definitions and
   none of any other's;
6. the pre-created exposition is 372 channel-keyed series plus 62, counted, not
   estimated;

and when reverting any one of tasks 3, 4, 7 or 9's changes makes at least one
named test fail — because a plan whose tests all pass against the tree it was
written for has documented the tree rather than changed it.
