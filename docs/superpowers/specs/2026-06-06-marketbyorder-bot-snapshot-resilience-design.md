# marketbyorder-bot: snapshot resilience (gap-recovery, not periodic rebaseline) — design

Related: #12 (shard dispatcher), #13 (parser back-pressure). This spec is
independent of both and does not depend on either landing.

## Problem

On the Tokyo MBO feed (`aws-tyo-hl-mainnet2` → ubuntu1 demo stack), the
`Sequence gaps (per-instrument)` panel shows ~20% of per-instrument deltas
missing in steady state, `dz_mbo_bot_snapshot_order_dropped_total` climbs
continuously, and the bot logs hundreds of `snapshot end failed: ... order
count short` lines per minute.

The cause is **not** the publisher and **not** the network at the level the
numbers imply. It is the bot's snapshot reassembly being all-or-nothing and
treating snapshots as a periodic re-baseline instead of a gap-recovery tool.

### Evidence

Measured on the live feed (clean dedicated 64 MiB-buffer subscriber on ubuntu1,
NIC and socket drop counters both zero):

| Stream | loss | notes |
|--------|------|-------|
| MBO mktdata (deltas) | **0.0015%** | essentially lossless; one frame in 67k |
| MBO snapshot | ~0.5–0.9% + 0.37% reorder | bursty; the raw seq-gap number is inflated by reordering |
| snapshot completeness | **3.66% arrive short** | matches the bot's own ~4.6% failure rate |
| steady iperf (publisher→sub, 60s, 100 Mbit/s) | 0.025% | the path floor under non-bursty load |

The harm is amplification, not loss magnitude. A full-book snapshot for a large
instrument is ~143 frames (MTU 1232, 27 `SnapshotOrder` per frame). At the
iperf-measured 0.025% per-frame loss, the probability that a 143-frame snapshot
loses at least one frame is `1 − 0.99975^143 ≈ 3.5%`. Snapshot microbursts push
the in-burst rate well above the steady floor, so the observed ~3.7% short rate
is consistent. The point: **even a near-perfect link fails large all-or-nothing
snapshots a few percent of the time.**

Because mktdata is ~lossless, instruments almost never truly desync from delta
loss. Nearly all the observed churn is self-inflicted: the bot processes
periodic re-snapshots it does not need, and a short one **demotes a live,
correct book**.

### Current behavior (the two demote paths)

In `go/marketbyorder-bot`:

1. `instrument.go EndSnapshot()` — on `OpenSnapshot == nil`, snapshot_id
   mismatch, anchor mismatch, **or `ReceivedOrders != TotalOrders`** sets
   `Status = StatusAwaitingSnapshot` and discards the book. So one lost
   `SnapshotOrder` frame evicts a book that mktdata was keeping correct.

2. `shard.go applySnapshotBegin()` skips the begin for an already-`Ready`,
   caught-up instrument (`Status == StatusReady && anchor <= LastAppliedMktdataSeq`),
   leaving `OpenSnapshot == nil`. The matching `applySnapshotEnd()` then still
   calls `EndSnapshot()`, hits the `OpenSnapshot == nil` branch, and demotes the
   instrument anyway.

Either path turns transient snapshot-stream loss into a dropped book and a
~one-round-robin-cycle (~10–13 s) gap for that instrument, which is what the
per-instrument panel reports.

`go/marketbyorder-bot/shard.go EventsWriter` writes **applied deltas only**, so
deltas dropped while an instrument is not `Ready` surface as per-instrument-seq
gaps in ClickHouse even though they arrived correctly on mktdata.

## Goals / success criteria

- A `Ready`, in-sync instrument is **never** demoted by snapshot-stream loss or
  reordering.
- Under ≥1% induced snapshot-stream loss, steady-state ready-fraction ≈ 100% and
  `dz_mbo_bot_per_instrument_gaps_total` stops growing for instruments that
  never lost an mktdata delta.
- Snapshots are consumed only for cold start and confirmed gap recovery.
- A short / mismatched / unneeded snapshot is a **no-op** on serving state.
- Per-instrument book correctness is identical to a lossless run.
- **No wire-format change.** FPGA and other hardware consumers are unaffected;
  the publisher stays dumb, one-way, fire-and-forget.

## Non-goals

- No FEC, no retransmission, no subscriber→publisher request channel. The feed
  stays one-way multicast and trivially decodable in hardware. (FEC was
  explicitly rejected: it forces every consumer, including FPGAs, to implement a
  decoder.)
- No change to publisher snapshot cadence, snapshot size, or fabric loss. Those
  are complementary levers tracked separately.
- Not the dispatcher sharding (#12) or parser back-pressure decoupling (#13).
  Both are real and complementary; this spec stands alone.

## Approach (chosen): decouple "have a book" from "building a snapshot"

Treat the live book as authoritative once in-sync. Snapshots build into a
**shadow** that never touches the live book until it commits atomically. A
failed shadow is discarded with no effect on serving state.

### State model

Replace the conflated status with two orthogonal axes:

- **Serving status** (per instrument): `NO_BOOK` → `READY` → `GAP`.
  - `NO_BOOK`: no usable book yet (cold start).
  - `READY`: usable book, in sync by `per_instrument_seq`.
  - `GAP`: usable but stale book; a real mktdata delta gap was detected and not
    yet repaired. Keep serving the old book, flagged stale.
- **Shadow** (orthogonal): an in-progress `PendingSnapshot` build. Its existence
  does not change serving status. `StatusBuildingSnapshot` is removed.

### Transition rules

```
READY + contiguous per_instrument_seq      → apply delta, advance seq. IGNORE snapshots.
READY + piSeq > expected (real gap)        → GAP. Keep serving old book. Buffer forward deltas.
(NO_BOOK | GAP) + snapshot_begin           → start a SHADOW. Do NOT touch live book or status.
snapshot_order (shadow open, id matches)   → add to shadow (count-based → reorder tolerant).
snapshot_end, shadow COMPLETE              → COMMIT: live book = shadow; seq = last_instrument_seq;
                                             replay buffered deltas with piSeq > last_instrument_seq;
                                             status = READY.
snapshot_end, shadow SHORT / mismatch      → discard shadow only. Status & live book UNCHANGED.
snapshot_end, no shadow                     → no-op. (fixes the demote-on-skip bug)
```

The decisive properties:

1. A short or unmatched `snapshot_end` never demotes a book.
2. A `Ready` in-sync instrument ignores the entire snapshot triad. Since mktdata
   is ~lossless, this is almost every instrument almost always, so snapshot-stream
   loss becomes nearly irrelevant to the gap metric.
3. The live book is mutated only by in-order deltas and by an atomic commit of a
   **complete** shadow.

## Detailed changes (file by file)

### `instrument.go`

- Add `Shadow *PendingSnapshot` alongside the live `Bids`/`Asks`. Remove
  `StatusBuildingSnapshot`; statuses become `NO_BOOK` / `READY` / `GAP`.
- `BeginSnapshot()`: allocate `Shadow`, never clear live `Bids`/`Asks`, never
  change `Status`.
- `AddSnapshotOrder()`: append to `Shadow` (unchanged logic, now on the shadow).
- `EndSnapshot()` → split into validation + `Commit()`:
  - On `Shadow == nil` / id mismatch / anchor mismatch / `Received != Total`:
    discard `Shadow`, **return an error WITHOUT touching `Status` or the live
    book.**
  - On success: `Commit()` swaps `Shadow` → live, sets `LastAppliedInstrumentSeq
    = last_instrument_seq`, `LastAppliedMktdataSeq = anchor`, `Status = READY`.

### `shard.go`

- `applySnapshotBegin()`: only start a shadow when `Status != READY` or the
  instrument is in `GAP`. A `READY`, in-sync instrument returns without building.
- `applySnapshotOrder()`: route into the shadow by snapshot_id (count-based, so
  reordered order frames within the build window are still counted).
- `applySnapshotEnd()`:
  - no shadow → `return nil` (no-op, no demote).
  - shadow complete → `Commit()` then `replayBuffer()`.
  - shadow short/mismatch → discard shadow, leave status/book unchanged, emit a
    metric (below). No `applied_snapshot` event.
- `applyDeltaToReady()`: on `piSeq > expected`, transition to `GAP` and buffer,
  but keep the live book. Add a bounded reorder window (below) before declaring
  `GAP`.

### Reorder tolerance

Before declaring a `GAP` on `piSeq > expected`, hold the out-of-order delta in
the per-instrument buffer for a small window (N messages or a few ms). If the
missing seq arrives, fill and continue; only escalate to `GAP` if it is truly
absent. The snapshot path showed ~0.37% reorder and the eager-gap-on-first-
out-of-order logic currently counts reordering as loss. This also fixes the
measurement artifact that inflated the original loss estimate.

### Metrics

- Keep `dz_mbo_bot_per_instrument_gaps_total` but only increment on a *confirmed*
  gap (after the reorder window), so it measures genuine mktdata loss.
- Add `dz_mbo_bot_snapshot_discarded_total{reason="short|mismatch|no_shadow"}`
  for visibility into ignored/failed snapshots (replaces the misleading
  `snapshot_order_dropped` semantics).
- Add `dz_mbo_bot_book_demotions_total` as a regression guard. In steady state
  with healthy mktdata this must stay flat. A nonzero rate means a real mktdata
  gap or a logic regression.

## Consumer contract (spec implication)

This reference bot is the model hardware teams copy, so the rule must be written
into the feed spec, not just the Go code:

> A consumer maintains its book from `OrderAdd`/`OrderCancel`/`OrderExecute`
> deltas. `per_instrument_seq` is dense and contiguous per instrument; a consumer
> that observes contiguous deltas is in sync and **must ignore** the snapshot
> stream. Snapshots are consumed only to bootstrap (no book yet) or to repair a
> detected `per_instrument_seq` gap. A snapshot must never downgrade an in-sync
> book, and an incomplete snapshot must be discarded, never partially applied.

Without this, an FPGA team re-implements the demote bug in silicon.

## Correctness notes / edge cases

- **Forward reconciliation keys on `per_instrument_seq`, not mktdata frame seq.**
  On commit at `last_instrument_seq = L`, replay buffered deltas with
  `piSeq > L`. Mixing the two seq spaces would double-apply or skip. Dedicated
  unit test required.
- **Empty book** (`total_orders == 0`): begin+end with no orders commits an empty
  book → `READY`. Unchanged.
- **`reset_count` era change**: still handled by the coordinator barrier
  (`coordinator.go runResetBarrier`), which wipes all shard state. A reset is a
  genuine publisher-side discontinuity and correctly forces re-bootstrap. This
  spec does not change reset handling.
- **GAP serving**: a `GAP` instrument keeps serving its last good book, flagged
  stale; events written during `GAP` are labeled so downstream (ClickHouse) can
  distinguish confirmed-stale from in-sync.
- **In-sync detection vs anchor**: "ignore snapshot when `READY` and in-sync" is
  driven by `per_instrument_seq` contiguity, not by the `anchor <=
  LastAppliedMktdataSeq` heuristic that currently leaves the broken end path.

## Alternatives considered

- **FEC / parity frames on the snapshot stream.** Rejected: forces a decoder on
  every consumer including FPGAs; adds wire complexity; contradicts the
  simple-one-way-feed goal.
- **Partially apply a short snapshot.** Rejected: a snapshot is a full state
  dump; an incomplete one yields a wrong book. Discarding and keeping the
  delta-maintained live book is strictly safer.
- **Publisher priority re-snapshot on subscriber request.** Rejected: requires a
  back-channel, breaking one-way multicast. (`MboSnapshotRequest::Priority` in
  the publisher is internal recovery, not subscriber-reachable.)
- **Land #13 / raise `SO_RCVBUF` only.** Insufficient: removes the back-pressure
  kernel-drop amplifier but not the fabric loss or the all-or-nothing
  sensitivity. Complementary, not a substitute.
- **Reduce snapshot size or cadence on the publisher.** Complementary (shrinks
  per-snapshot failure probability) but does not make the subscriber robust, and
  needs a wire/publisher change. Pursue separately.
- **Periodic non-destructive snapshot validation while `READY`.** Compare an
  incoming snapshot to the live book and log divergence without demoting.
  Rejected for now: per-instrument-seq contiguity already guarantees correctness
  for a correct apply path, so this only guards against apply-logic bugs, at
  real CPU and code cost. A `READY` in-sync instrument fully ignores the snapshot
  stream (confirmed design decision).

## Testing / verification

Unit (`go/marketbyorder-bot`):
1. `READY` book receives a short snapshot → stays `READY`, live book intact, no
   demote, `book_demotions_total == 0`.
2. `READY` book receives a complete redundant snapshot → ignored, book
   unchanged.
3. `GAP` instrument receives a complete snapshot → commit + forward replay of
   `piSeq > L` deltas → correct book, `READY`.
4. Reordered delta within the window → no spurious `GAP`.

Integration:
5. Replay a captured pcap (recorder warehouse, Tokyo MBO) with synthetic 1%
   snapshot-frame loss injected; assert steady-state ready-fraction ≈ 100% and
   `book_demotions_total == 0`.
6. Golden parity: against a lossless replay, final books per instrument are
   byte-identical to the pre-change bot.

## Rollout

Single code path (the resilient reassembly is the only path; no flag to
maintain), matching the shard-dispatcher precedent. Ship behind the existing
test suite plus the new unit/integration tests above.
