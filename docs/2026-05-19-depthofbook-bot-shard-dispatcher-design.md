# depthofbook-bot: shard dispatcher by instrument_id — design

Issue: https://github.com/malbeclabs/edge-multicast-ref/issues/12

## Problem

`go/depthofbook-bot/main.go`'s dispatcher is single-goroutine bound. Under
sustained load on a feed with one channel and many instruments it hits ~1.2
cores and becomes the throughput choke point: the parser's per-client
unix-socket queue fills, `socket_client_drops_total{reason="queue_full"}`
accumulates, which drives `per_instrument_gaps_total` up and triggers
snapshot-end reassembly failures.

The per-channel mutex (`ChannelState.Mu` in `channel.go`) serializes all record
application for the channel. With a single channel there is no channel-level
parallelism, but with hundreds of instruments there is abundant
instrument-level parallelism. The host has ~5 idle cores.

Out of scope (unchanged): wire-format / JSON-decode changes
(`bot.go`'s `json.Unmarshal` stays); multi-channel parallelization.

## Success criteria

Eliminate parser `queue_full` drops and `per_instrument_gaps_total` growth at
the **current demo profile (1 channel, ~330 active instruments)**. Near-linear
scaling across workers is explicitly *not* required — only enough parallelism
to clear the choke using the idle cores. Per-instrument correctness must be
identical to the single-dispatcher behavior.

## Configuration

- `--shards N` flag. Default-on (the sharded path is the only path; there is no
  second code path to maintain).
- Default `N` derived at runtime from `GOMAXPROCS` (e.g. a capped
  `min(GOMAXPROCS-2, cap)`), exact formula chosen during implementation.
- `N=1` is a valid degenerate case: a single coordinator + single shard that
  must behave identically to today's dispatcher.

## Approach (chosen: Approach A — coordinator + share-nothing sharded workers)

A new **Coordinator** goroutine replaces the inline dispatcher closure in
`main.go`. Work is sharded by `instrument_id` across `N` **Shard** goroutines,
each owning a disjoint subset of instruments. State is split between
channel-scoped (coordinator) and instrument-scoped (shard); the hot path is
share-nothing.

Rejected alternatives:

- **Lock-striped shared `ChannelState`**: smaller diff but the global
  `DeltaBuffer` and shared `Refdata` stay contended, `snapshot_order` still
  needs cross-instrument coordination, and lock overhead is re-added. Lower
  ceiling, messier correctness.
- **Pipeline split (extract → apply → write), no instrument sharding**: least
  invasive but book-apply stays single-threaded; likely only 2–3x and may not
  clear drops since the choke *is* `c.Apply`. Lower confidence it meets the bar.

### Architecture

```
 socket ─► Bot.read (json.Unmarshal)            [unchanged, own goroutine]
              │ Dispatcher.Dispatch(rec)         [synchronous — see safety note]
              ▼
        ┌─────────────────────────────┐
        │        Coordinator          │  single goroutine, no locks
        │  owns: ResetCount, Manifest │
        │        SeqLast[port]        │
        │        snapshotRoute        │  (channelID, snapshotID) → shard idx
        │  classify + route + barrier │
        └──┬──────────┬──────────┬────┘
       route by instrumentID % N (type-first; snapshot_order via snapshotRoute)
           ▼          ▼          ▼
        Shard0     Shard1  …  ShardN-1     each: own goroutine + bounded FIFO inbox
          │ owns (exclusive): Instruments subset, Refdata subset,
          │   per-instrument DeltaBuffers, per-instrument snapCtx,
          │   SnapshotWriter goroutine + per-shard sync.Mutex
          ▼
     eventsWriter.Write / SnapshotWriter.MarkDirty
     (ClickHouse Enqueue is already concurrency-safe)
```

### Ownership split

**Coordinator (single goroutine, no locks):**

- `ResetCount`, `Manifest`, `SeqLast[port]` (channel-scoped).
- `snapshotRoute map[snapKey]int` where `snapKey = {channelID, snapshotID}` →
  shard index. Registered when the coordinator *sees* `snapshot_begin`, cleared
  on `snapshot_end`.
- Writes channel-health rows (`heartbeat`, `manifest_summary`,
  `end_of_session`) directly — these touch no book. `manifest_summary` also
  updates the coordinator-owned `Manifest`.

**Each shard (own goroutine + bounded inbox):**

- A disjoint subset of `Instruments` (by `instrumentID % N`), that subset's
  `Refdata`, per-instrument `DeltaBuffer`s, per-instrument snapshot context.
- Its own `SnapshotWriter` goroutine and a per-shard `sync.Mutex` that guards
  *only* book mutation so that writer can read levels. Uncontended across
  shards.

**`ChannelState` is decomposed.** Today it mixes channel-scoped fields
(`ResetCount`, `Manifest`, `SeqLast`) and instrument-scoped fields
(`Instruments`, `Refdata`, `DeltaBuffer`) under one `Mu`. Instrument-scoped
fields and the `apply*` methods move to a new per-shard `Shard` type (the
`applyInner` switch is reused largely intact, rebound to `Shard`).
Channel-scoped fields move to the coordinator. The channel-global `DeltaBuffer`
becomes per-instrument buffers inside the shard — it was already logically
per-instrument (`replayBuffer`/`filterBuffer` filter by `InstrumentID`), which
also removes the global `sort.Slice` on every buffered delta.

Invariant: a given `instrument_id` is handled by exactly one shard for its
whole life, so per-instrument FIFO and per-instrument sequence-gap detection
are unchanged. Cross-instrument global ordering is intentionally relaxed —
ClickHouse rows are timestamped and queried per-instrument/symbol, so this is
safe.

### Record classification & routing

The classifier branches on **record type first**, and only hashes
`instrumentID % N` for instrument-scoped types. A record with no
`instrument_id` is never hashed (no accidental `instrument_id=0 → shard[0]`).

| Record type | has instrument_id? | Routing |
|---|---|---|
| order_add / order_cancel / order_execute, instrument_definition, instrument_reset, snapshot_begin, snapshot_end, trade | yes | `shard[instrumentID % N]` |
| `snapshot_order` | no (snapshot_id only) | `snapshotRoute[(channelID, snapshotID)]` → shard; registered on `snapshot_begin`, cleared on `snapshot_end` |
| heartbeat, manifest_summary | no | coordinator writes directly, **no fence**; manifest_summary updates coordinator `Manifest` |
| end_of_session, batch_boundary | no | **fence**: coordinator drains all shards, then writes |
| reset_count change | — | **barrier** (see below) |

Parser facts grounding this table (`go/depthofbook-parser/depthofbook.go`):
`trade` carries `InstrumentID` (line ~105) so it shards normally;
`batch_boundary` does **not** (line ~201) so it is channel-scoped;
`snapshot_order` has no `InstrumentID` (line ~239), only `snapshot_id`;
`snapshot_begin`/`snapshot_end` carry `InstrumentID`.

**Why `snapshot_order` routing is race-free.** `snapshot_begin` and its
following `snapshot_order`s are emitted in order by the publisher and routed to
the *same* shard inbox (strict FIFO). The coordinator registers the route the
instant it sees `snapshot_begin` (synchronously, before the shard processes
it); the coordinator-side map is used only for routing and is never read by
shard logic. So the shard always processes `snapshot_begin` before its
`snapshot_order`s. A `snapshot_order` with no registered route (begin missed,
or arrived after end) is dropped and a counter incremented — parity with
today's `applySnapshotOrder` returning nil when no instrument is in
`StatusBuildingSnapshot`.

**Composite key.** `snapshotRoute` is keyed on `(channelID, snapshotID)`, not
bare `snapshotID`, to prevent silent cross-channel misrouting if snapshot IDs
collide across channels.

### Channel-reset barrier

Today `ChannelState.Apply` (channel.go:69–82) detects a `reset_count` change,
emits `channel_reset`, calls `c.reset()` (wipes Instruments / Refdata /
Manifest / DeltaBuffer / SeqLast), then applies the triggering record as the
first new-era frame. Sharded version, as an **in-band FIFO barrier**:

1. Coordinator (owns `ResetCount`) sees a record `R` whose `reset_count`
   differs. It **holds `R`** — does not route it.
2. Coordinator creates `acks := make(chan shardID, N)` (buffered = N so ack
   sends never block) and spawns **one goroutine per shard** that does the
   blocking `shard.inbox <- resetMarker{ack: acks}` through the normal FIFO
   channel. (Goroutine-per-shard, not sequential send-then-collect, to avoid
   the deadlock where the coordinator blocks sending a marker into one full
   inbox while other shards sit idle.)
3. Each shard drains all its old-era records first (FIFO), then hits the
   marker, wipes its own Instruments / Refdata / per-instrument DeltaBuffers /
   snapCtx **and clears its `SnapshotWriter` dirty map**, then sends an ack.
4. Coordinator blocks until it has acks from all `N` shards. This guarantees
   every old-era record was fully applied and its ClickHouse rows enqueued
   before any new-era record exists anywhere.
5. Coordinator increments `ChannelResetsTotal`, clears its own
   `snapshotRoute`, `SeqLast`, `Manifest`, adopts the new `ResetCount`.
6. Coordinator routes the **held `R`** through the full classifier as the
   first new-era frame. Because the classifier is type-first, an `R` that is
   channel-scoped (`heartbeat` / `manifest_summary` are legitimate first
   new-era frames — they carry `reset_count` but no `instrument_id`) takes the
   coordinator direct-write path, not a shard hash.

Explicitly **not** done: broadcasting reset out-of-band before inboxes drain.
The marker rides the FIFO queue precisely so old-era records cannot be applied
to wiped state.

**Safety property (Codex-confirmed).** The bot read loop calls
`Dispatcher.Dispatch` synchronously (`bot.go:79–98`) and does not read the next
JSONL record until `Dispatch(R)` returns. Holding `R` and running the barrier
inside that call provably closes the "old-era record routed after wipe"
window: no later old-era `snapshot_order` (or any record) can be routed after
the coordinator observes `R`.

`SnapshotWriter.Reset()` is added to support step 3 — `SnapshotWriter.dirty`
(snapshot_writer.go:18–20,44–61) is separate from instrument state; a surviving
old-era dirty entry would otherwise cause a stale flush in the new era.

### Error handling

- **Inbox full** → coordinator blocks on send. Intended backpressure,
  equivalent to today's single mutex. The parser's own queue +
  `socket_client_drops_total` remains the real drop point, unchanged. During a
  barrier the read loop blocks transiently (bounded) — acceptable, no deadlock
  given the goroutine-per-shard barrier structure.
- **`snapshot_order` with no route** → dropped + counter incremented.
  Intentional parity behavior.
- **Per-shard SnapshotWriter** guarded by a per-shard mutex (chosen over a
  mutex-free copy-handoff alternative — smaller change, success bar is modest,
  mutex is uncontended across shards). *Noted alternative:* the shard computes
  an immutable level copy and hands it to the writer, eliminating the mutex; a
  larger rewrite of `snapshot_writer.go`, deferred unless profiling shows the
  per-shard mutex matters.
- **Shard panic**: no new recovery added — parity with today's no-recovery
  dispatcher. Out of scope.

## Testing strategy

- **Unit (per shard):** reuse existing `channel_test.go` cases against the new
  `Shard` type — delta application, per-instrument gap detection, snapshot
  begin/order/end, instrument_reset, buffered-delta replay. Pure per-instrument
  logic; behavior must not change.
- **Routing/classification table test:** every record type → expected
  destination (shard hash / coordinator-direct / fence / snapshot-route /
  barrier), including the no-`instrument_id` types and the `instrument_id=0`
  guard.
- **Order-preservation golden test:** feed an interleaved multi-instrument
  stream; assert per-instrument output order is identical to the
  single-dispatcher baseline. Cross-instrument order intentionally not
  asserted. This is the primary guard that sharding did not change
  per-instrument semantics.
- **Reset-barrier test:** stream old-era records across multiple shards, inject
  a `reset_count` bump mid-stream with records still queued; assert (a) all
  old-era rows written before the reset, (b) no new-era record applied to
  pre-wipe state, (c) SnapshotWriter dirty map cleared, (d) held triggering
  record applied as the first new-era frame, including the case where the
  triggering frame is `manifest_summary` (channel-scoped).
- **Snapshot routing test:** `snapshot_begin`/`order`/`end` for instruments on
  different shards interleaved; assert each snapshot reassembles on its owning
  shard and `snapshot_order` with no registered route is dropped + counted.
- **Race detector:** concurrency test with `N` shards under `-race` driving
  randomized multi-instrument load; assert no races.

### End-to-end verification — in-process automated harness (acceptance gate)

There is no existing e2e/integration suite in this repo (no `e2e/` directory,
no `//go:build e2e` tags); the only end-to-end vehicle today is the manual
`demo/docker-compose.yml` stack. Because the success bar is defined in metrics
(`socket_client_drops_total{reason="queue_full"}`,
`per_instrument_gaps_total`) that unit tests cannot produce, the acceptance
gate is a new **in-process automated harness** (Go test, runnable in CI):

- A synthetic record generator produces a representative stream (~330
  instruments, 1 channel, including snapshots, resets, and bursts) feeding the
  bot in-process with `N` shards.
- Assert: zero `queue_full` drops at the target load, no
  `per_instrument_gaps_total` growth, and per-instrument output **parity**
  against a single-dispatcher golden run over the same stream.
- Run the same harness with `--shards=1` to confirm the degenerate path
  matches single-dispatcher behavior.

The `demo/docker-compose.yml` stack remains an **optional, non-gating** sanity
check (baseline-vs-sharded bot CPU spread + Grafana dashboard renders
correctly), not the acceptance mechanism.

## Out of scope / non-goals

- Wire-format swap; JSON decode stays in the read loop.
- Multi-channel parallelization (not useful at current cardinality).
- The parser-side `outQueueLen` 4096→16384 workaround stays as-is; this design
  fixes steady-state throughput so the larger buffer only absorbs burst
  variance.
