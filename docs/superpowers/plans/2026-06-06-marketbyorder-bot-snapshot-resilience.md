# marketbyorder-bot snapshot resilience — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the MBO subscriber treat snapshots as gap-recovery, not periodic rebaseline, so transient snapshot-stream loss/reordering can never demote a live, correct book.

**Architecture:** Decouple "have a book" (serving status: `AwaitingSnapshot`/`Ready`/`Gap`) from "building a snapshot" (an orthogonal shadow, `Instrument.OpenSnapshot`). Snapshots build into the shadow and commit atomically only when complete; a short/mismatched/unneeded snapshot end is a no-op. A `Ready` instrument, kept correct by contiguous `per_instrument_seq` deltas, ignores the snapshot stream entirely. No wire-format change.

**Tech Stack:** Go 1.26, `github.com/prometheus/client_golang`. Package `main` under `go/marketbyorder-bot`. Tests with the std `testing` package (see existing `*_test.go`).

**Spec:** `docs/superpowers/specs/2026-06-06-marketbyorder-bot-snapshot-resilience-design.md`

**Working dir for all commands:** `go/marketbyorder-bot`

---

## File map

- `instrument.go` — state model + snapshot lifecycle (`BeginSnapshot`/`EndSnapshot`/`Commit`). Core changes in Tasks 1, 4.
- `shard.go` — record application (`applySnapshotBegin`/`applySnapshotOrder`/`applySnapshotEnd`/`applyDeltaToReady`). Core changes in Tasks 2, 4.
- `metrics.go` — add `SnapshotDiscardedTotal`, `BookDemotionsTotal`. Task 3.
- `snapshot_writer.go` — serve last-good levels for `Gap`, flagged stale. Task 5.
- Tests: `instrument_test.go`, `shard_test.go`, `parity_test.go`.

Baseline before starting: `go test ./...` is green (~9s).

---

## Task 1: Instrument state model — non-demoting EndSnapshot + shadow build

**Files:**
- Modify: `instrument.go`
- Test: `instrument_test.go`

Removes `StatusBuildingSnapshot`. `BeginSnapshot` builds the shadow without changing `Status` or the live book. `EndSnapshot` commits atomically on success and, on any failure, discards only the shadow — it never touches `Status`, `Bids`, or `Asks`.

- [ ] **Step 1: Write failing tests** in `instrument_test.go`:

```go
func TestEndSnapshotShortDoesNotDemoteReady(t *testing.T) {
	i := NewInstrument(7, "BTC", 0, 0)
	i.Status = StatusReady
	i.Bids[1] = &RestingOrder{OrderID: 1, Side: 0, Price: 100, Quantity: 5}
	i.LastAppliedInstrumentSeq = 42
	// A re-snapshot begins and comes up one order short.
	i.BeginSnapshot(9, 1000, 2 /*total*/, 50)
	if i.Status != StatusReady {
		t.Fatalf("BeginSnapshot must not change Status; got %v", i.Status)
	}
	i.AddSnapshotOrder(9, 11, 0, 0, time.Time{}, 100, 5) // only 1 of 2
	_, _, err := i.EndSnapshot(9, 1000)
	if err == nil {
		t.Fatal("expected short-snapshot error")
	}
	if i.Status != StatusReady {
		t.Fatalf("short snapshot must NOT demote a Ready book; got %v", i.Status)
	}
	if _, ok := i.Bids[1]; !ok {
		t.Fatal("live book must be intact after a failed snapshot")
	}
	if i.OpenSnapshot != nil {
		t.Fatal("shadow must be discarded on failure")
	}
}

func TestEndSnapshotNoOpenSnapshotIsNoDemote(t *testing.T) {
	i := NewInstrument(7, "BTC", 0, 0)
	i.Status = StatusReady
	i.Bids[1] = &RestingOrder{OrderID: 1}
	_, _, err := i.EndSnapshot(9, 1000) // no shadow open
	if err == nil {
		t.Fatal("expected errNoOpenSnapshot")
	}
	if i.Status != StatusReady || len(i.Bids) != 1 {
		t.Fatal("end with no open snapshot must be a no-op on a Ready book")
	}
}

func TestEndSnapshotCompleteCommits(t *testing.T) {
	i := NewInstrument(7, "BTC", 0, 0)
	i.Status = StatusAwaitingSnapshot
	i.BeginSnapshot(9, 1000, 1, 50)
	i.AddSnapshotOrder(9, 11, 0, 0, time.Time{}, 100, 5)
	anchor, last, err := i.EndSnapshot(9, 1000)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if i.Status != StatusReady || anchor != 1000 || last != 50 {
		t.Fatalf("commit failed: status=%v anchor=%d last=%d", i.Status, anchor, last)
	}
	if _, ok := i.Bids[11]; !ok {
		t.Fatal("committed book must contain the snapshot order")
	}
	if i.LastAppliedInstrumentSeq != 50 || i.LastAppliedMktdataSeq != 1000 {
		t.Fatal("commit must set seqs from the snapshot")
	}
}
```

- [ ] **Step 2: Run, verify fail**

Run: `go test ./... -run TestEndSnapshot -v`
Expected: FAIL (current `BeginSnapshot` sets `StatusBuildingSnapshot`; current `EndSnapshot` demotes; no `errNoOpenSnapshot`).

- [ ] **Step 3: Implement** in `instrument.go`:

Remove the `StatusBuildingSnapshot` constant from the `const` block so statuses are `StatusAwaitingSnapshot` (iota 0), `StatusReady`, `StatusGap`. Update `String()` accordingly (drop the building case).

Add the sentinel error near `errSnapshotMismatch`:

```go
var errNoOpenSnapshot = errors.New("snapshot end with no open snapshot")
```

Replace `BeginSnapshot` so it does NOT set `Status` (leave the line that sets the status out entirely):

```go
func (i *Instrument) BeginSnapshot(snapID uint32, anchorSeq uint64, totalOrders, lastInstrSeq uint32) {
	i.OpenSnapshot = &PendingSnapshot{
		SnapshotID:        snapID,
		AnchorSeq:         anchorSeq,
		TotalOrders:       totalOrders,
		LastInstrumentSeq: lastInstrSeq,
		Bids:              map[uint64]*RestingOrder{},
		Asks:              map[uint64]*RestingOrder{},
	}
}
```

Replace `EndSnapshot` so failures never mutate serving state:

```go
func (i *Instrument) EndSnapshot(snapID uint32, anchorSeq uint64) (uint64, uint32, error) {
	if i.OpenSnapshot == nil {
		return 0, 0, errNoOpenSnapshot
	}
	if i.OpenSnapshot.SnapshotID != snapID || i.OpenSnapshot.AnchorSeq != anchorSeq {
		i.OpenSnapshot = nil // discard shadow only; live book & Status untouched
		return 0, 0, fmt.Errorf("%w: snapshot_id=%d anchor=%d", errSnapshotMismatch, snapID, anchorSeq)
	}
	if i.OpenSnapshot.ReceivedOrders != i.OpenSnapshot.TotalOrders {
		got, want := i.OpenSnapshot.ReceivedOrders, i.OpenSnapshot.TotalOrders
		i.OpenSnapshot = nil // discard shadow only; live book & Status untouched
		return 0, 0, fmt.Errorf("%w: got %d expected %d", errSnapshotShort, got, want)
	}
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	anchor := i.OpenSnapshot.AnchorSeq
	lastInstr := i.OpenSnapshot.LastInstrumentSeq
	i.OpenSnapshot = nil
	i.Status = StatusReady
	i.LastAppliedMktdataSeq = anchor
	i.LastAppliedInstrumentSeq = lastInstr
	return anchor, lastInstr, nil
}
```

- [ ] **Step 4: Build will break in shard.go** — that's expected; `shard.go` still references `StatusBuildingSnapshot`. Fix only the compile references needed to run these unit tests by leaving `shard.go` for Task 2. To keep the package compiling for this task's test run, temporarily replace `StatusBuildingSnapshot` occurrences in `shard.go` with `StatusReady` is NOT acceptable. Instead, do Task 1 and Task 2 edits together before running the suite. (Run `go vet ./...` after Task 2.)

> Note for the worker: Tasks 1 and 2 touch the same compile unit. Make both sets of edits, then run tests once at the end of Task 2. Commit after Task 2.

- [ ] **Step 5: (deferred to Task 2 commit)**

---

## Task 2: shard.go snapshot paths — ignore-when-ready, shadow routing, no-op end

**Files:**
- Modify: `shard.go`
- Test: `shard_test.go`

- [ ] **Step 1: Write failing tests** in `shard_test.go` (use the existing test helpers in that file for building a shard and feeding records; mirror their `Record`/`Fields` construction):

```go
func TestReadyInstrumentIgnoresSnapshot(t *testing.T) {
	s := newTestShard(t) // existing helper
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusReady
	s.instruments[k].Bids[1] = &RestingOrder{OrderID: 1}
	s.instruments[k].LastAppliedInstrumentSeq = 10

	s.apply(snapshotBeginRec(0, 7, 99 /*snap*/, 5 /*total*/, 2000 /*anchor*/, 20))
	if s.instruments[k].OpenSnapshot != nil {
		t.Fatal("Ready instrument must not start a shadow build")
	}
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("snapshot end on a Ready (no-shadow) instrument must be a no-op")
	}
}

func TestShortSnapshotKeepsReadyBook(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap // needs a snapshot
	s.apply(snapshotBeginRec(0, 7, 99, 2 /*total*/, 2000, 20))
	if s.instruments[k].OpenSnapshot == nil {
		t.Fatal("Gap instrument must start a shadow build")
	}
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5)) // only 1 of 2
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].OpenSnapshot != nil {
		t.Fatal("short snapshot shadow must be discarded")
	}
	if s.instruments[k].Status != StatusGap {
		t.Fatal("short snapshot must leave a Gap instrument in Gap, not demote further")
	}
}

func TestCompleteSnapshotRepairsGap(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap
	s.apply(snapshotBeginRec(0, 7, 99, 1, 2000, 20))
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5))
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("complete snapshot must repair a Gap to Ready")
	}
	if _, ok := s.instruments[k].Bids[11]; !ok {
		t.Fatal("repaired book must contain snapshot order")
	}
}
```

> If `shard_test.go` lacks `newTestShard`/`snapshotBeginRec`/etc. helpers, add small local helpers at the top of the test file. `snapshotBeginRec(ch, instID, snapID, total, anchor, lastInstr)` builds `Record{Type:"snapshot_begin", ChannelID:ch, InstrumentID:instID, Fields: map[string]any{"snapshot_id":float64(snapID),"total_orders":float64(total),"anchor_seq":float64(anchor),"last_instrument_seq":float64(lastInstr)}}`. (Numeric fields are `float64` — the parser→bot link is JSON; see `toUint32`/`toUint64` in `shard.go`.) `snapshotOrderRec(ch, snapID, orderID, side, price, qty)` sets `Type:"snapshot_order"` with `snapshot_id`, `order_id`, `side` (`"bid"`/`"ask"` string), `order_flags`, `enter_ts`, `price_raw`, `qty_raw`. `snapshotEndRec(ch, instID, snapID, anchor)` sets `Type:"snapshot_end"`, `InstrumentID`, `snapshot_id`, `anchor_seq`.

- [ ] **Step 2: Run, verify fail**

Run: `go test ./... -run 'TestReadyInstrumentIgnores|TestShortSnapshotKeeps|TestCompleteSnapshotRepairs' -v`
Expected: FAIL / compile error (current code uses `StatusBuildingSnapshot`).

- [ ] **Step 3: Implement** in `shard.go`. Replace the three snapshot functions:

```go
func (s *Shard) applySnapshotBegin(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		inst = NewInstrument(k.id, "", 0, 0)
		s.instruments[k] = inst
	}
	// A Ready instrument is maintained by contiguous deltas; snapshots are
	// gap-recovery only, so ignore them while Ready.
	if inst.Status == StatusReady {
		return nil
	}
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	snapID := toUint32(rec.Fields["snapshot_id"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

func (s *Shard) applySnapshotOrder(rec Record) []ChannelEvent {
	snapID := toUint32(rec.Fields["snapshot_id"])
	for _, inst := range s.instruments {
		if inst.OpenSnapshot == nil || inst.OpenSnapshot.SnapshotID != snapID {
			continue
		}
		orderID := toUint64(rec.Fields["order_id"])
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.AddSnapshotOrder(snapID, orderID, side, flags, enter, price, qty)
		return nil
	}
	return nil
}

func (s *Shard) applySnapshotEnd(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	if inst.OpenSnapshot == nil {
		return nil // no shadow in progress; ignore (never demote)
	}
	snapID := toUint32(rec.Fields["snapshot_id"])
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		if s.metrics != nil {
			s.metrics.SnapshotDiscardedTotal.WithLabelValues(discardReason(err)).Inc()
		}
		log.Printf("shard %d instrument %d: snapshot discarded: %v", s.idx, k.id, err)
		return nil // discard shadow only; live book & status unchanged
	}
	s.replayBuffer(k, inst)
	return []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}
```

Add the reason classifier at the bottom of `shard.go`:

```go
func discardReason(err error) string {
	switch {
	case errors.Is(err, errSnapshotShort):
		return "short"
	case errors.Is(err, errSnapshotMismatch):
		return "mismatch"
	default:
		return "other"
	}
}
```

Add `"errors"` to the `shard.go` import block. `SnapshotDiscardedTotal` is added in Task 3 — to keep this task compiling and tested independently, guard the metric call with the `s.metrics != nil` check already shown AND land Task 3's metric field first if your worker prefers; otherwise temporarily comment the `SnapshotDiscardedTotal` line and restore it in Task 3. (Recommended: do Task 3's `metrics.go` field addition as Step 3a here so the package compiles.)

> **Step 3a (compile dependency):** add the `SnapshotDiscardedTotal *prometheus.CounterVec` field + registration now (full details in Task 3) so `shard.go` compiles.

- [ ] **Step 4: Run, verify pass**

Run: `go test ./... -v 2>&1 | tail -20`
Expected: PASS (all existing + new tests). If `parity_test.go` fails, inspect — a parity break here is a real regression in snapshot handling, stop and investigate.

- [ ] **Step 5: Commit**

```bash
cd go/marketbyorder-bot
git add instrument.go instrument_test.go shard.go shard_test.go metrics.go
git commit -m "marketbyorder-bot: snapshots are gap-recovery; never demote a live book"
```

---

## Task 3: Metrics — discard + demotion visibility

**Files:**
- Modify: `metrics.go`, `shard.go`
- Test: `shard_test.go` (assert counter behavior)

- [ ] **Step 1: Write failing test** in `shard_test.go`:

```go
func TestShortSnapshotIncrementsDiscardedNotDemotion(t *testing.T) {
	s := newTestShard(t) // built with a real Metrics via NewMetrics("test","test")
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap
	s.apply(snapshotBeginRec(0, 7, 99, 2, 2000, 20))
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5))
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if got := testCounterVec(t, s.metrics.SnapshotDiscardedTotal, "short"); got != 1 {
		t.Fatalf("snapshot_discarded_total{short} = %v, want 1", got)
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 0 {
		t.Fatalf("book_demotions_total = %v, want 0", got)
	}
}
```

> Reuse the existing `testCounter` helper from `coordinator_test.go`. Add a `testCounterVec(t, cv, labels...)` helper if absent (use `cv.WithLabelValues(labels...)` + `dto.Metric` read, mirroring `testCounter`).

- [ ] **Step 2: Run, verify fail**

Run: `go test ./... -run TestShortSnapshotIncrementsDiscarded -v`
Expected: FAIL (fields don't exist).

- [ ] **Step 3: Implement** in `metrics.go`. Add fields to the `Metrics` struct (near `SnapshotOrderDroppedTotal`):

```go
	SnapshotDiscardedTotal *prometheus.CounterVec
	BookDemotionsTotal     prometheus.Counter
```

In `NewMetrics`, construct + register them:

```go
	m.SnapshotDiscardedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_discarded_total"}, []string{"reason"})
	m.BookDemotionsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "book_demotions_total"})
```

Add both to the `reg.MustRegister(...)` list.

`BookDemotionsTotal` is a regression guard: it must be incremented at the *only* legitimate demotion site — when `applyDeltaToReady` transitions `Ready → Gap` on a confirmed gap (Task 4). For this task, just register it (stays 0). Wire `SnapshotDiscardedTotal` is already called from `applySnapshotEnd` (Task 2).

- [ ] **Step 4: Run, verify pass**

Run: `go test ./... -v 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add metrics.go shard.go shard_test.go
git commit -m "marketbyorder-bot: add snapshot_discarded_total and book_demotions_total"
```

---

## Task 4: Reorder tolerance on the delta path

**Files:**
- Modify: `instrument.go` (add `Pending` field), `shard.go` (`applyDeltaToReady`)
- Test: `shard_test.go`

A delta with `piSeq > expected` is held briefly; if the missing seq arrives within a small window it is a reorder (apply, no gap). Only escalate to `Gap` once the window is exceeded. Mktdata reordering is rare (~1 in 67k) but this removes the false-gap-on-reorder failure mode and the measurement artifact it causes.

- [ ] **Step 1: Write failing tests** in `shard_test.go`:

```go
func TestReorderedDeltaDoesNotGap(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	in := NewInstrument(7, "BTC", 0, 0)
	in.Status = StatusReady
	in.LastAppliedInstrumentSeq = 10
	s.instruments[k] = in
	// seq 12 arrives before 11 (reorder)
	s.apply(orderAddRec(0, 7, 12 /*piSeq*/, 120, 100, 1))
	if in.Status == StatusGap {
		t.Fatal("a single reordered delta must not declare a gap")
	}
	s.apply(orderAddRec(0, 7, 11 /*piSeq*/, 110, 100, 1))
	if in.Status != StatusReady || in.LastAppliedInstrumentSeq != 12 {
		t.Fatalf("reorder must drain to seq 12 Ready; got status=%v seq=%d", in.Status, in.LastAppliedInstrumentSeq)
	}
}

func TestRealGapEscalatesPastWindow(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	in := NewInstrument(7, "BTC", 0, 0)
	in.Status = StatusReady
	in.LastAppliedInstrumentSeq = 10
	s.instruments[k] = in
	// A burst of far-ahead deltas with a permanent hole at 11.
	for piSeq := uint32(12); piSeq <= 12+uint32(reorderWindow)+1; piSeq++ {
		s.apply(orderAddRec(0, 7, piSeq, uint64(piSeq), 100, 1))
	}
	if in.Status != StatusGap {
		t.Fatal("a hole beyond the reorder window must declare a gap")
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 1 {
		t.Fatalf("book_demotions_total = %v, want 1", got)
	}
}
```

> `orderAddRec(ch, instID, piSeq, mktSeq, price, qty)` builds `Record{Type:"order_add", ChannelID:ch, InstrumentID:instID, SequenceNumber:mktSeq, Fields:{"per_instrument_seq":float64(piSeq),"side":"bid","order_flags":float64(0),"order_id":float64(piSeq),"enter_ts":"","price_raw":float64(price),"qty_raw":float64(qty)}}`.

- [ ] **Step 2: Run, verify fail**

Run: `go test ./... -run 'TestReorderedDelta|TestRealGapEscalates' -v`
Expected: FAIL (`reorderWindow` undefined; current code gaps immediately).

- [ ] **Step 3: Implement.**

In `instrument.go`, add to `Instrument`: `Pending map[uint32]Record` (out-of-order deltas keyed by `per_instrument_seq`).

In `shard.go`, add `const reorderWindow = 16` near the other const, and refactor the apply switch in `applyDeltaToReady` into a helper `applyOne` that performs the order_add/cancel/execute mutation and returns the `applied_delta` event:

```go
func (s *Shard) applyOne(inst *Instrument, rec Record) ChannelEvent {
	switch rec.Type {
	case "order_add":
		inst.ApplyOrderAdd(toUint64(rec.Fields["order_id"]), sideFromString(toString(rec.Fields["side"])), toUint8(rec.Fields["order_flags"]), toTime(rec.Fields["enter_ts"]), toInt64(rec.Fields["price_raw"]), toUint64(rec.Fields["qty_raw"]))
	case "order_cancel":
		inst.ApplyOrderCancel(toUint64(rec.Fields["order_id"]))
	case "order_execute":
		inst.ApplyOrderExecute(toUint64(rec.Fields["order_id"]), toUint8(rec.Fields["exec_flags"]), toUint64(rec.Fields["exec_qty_raw"]))
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = toUint32(rec.Fields["per_instrument_seq"])
	return ChannelEvent{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
}
```

Replace `applyDeltaToReady`:

```go
func (s *Shard) applyDeltaToReady(k instKey, inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1
	if piSeq < expected {
		return nil // old / duplicate
	}
	if piSeq > expected {
		if inst.Pending == nil {
			inst.Pending = map[uint32]Record{}
		}
		inst.Pending[piSeq] = rec
		if uint32(len(inst.Pending)) <= reorderWindow && piSeq-expected <= reorderWindow {
			return nil // within reorder window; wait for the hole to fill
		}
		// Window exceeded: genuine gap.
		inst.Status = StatusGap
		inst.Pending = nil
		s.bufferDelta(k, rec)
		if s.metrics != nil {
			s.metrics.BookDemotionsTotal.Inc()
		}
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}
	// piSeq == expected: apply, then drain contiguous reordered deltas.
	evs := []ChannelEvent{s.applyOne(inst, rec)}
	for inst.Pending != nil {
		next := inst.LastAppliedInstrumentSeq + 1
		pr, ok := inst.Pending[next]
		if !ok {
			break
		}
		delete(inst.Pending, next)
		evs = append(evs, s.applyOne(inst, pr))
		if len(inst.Pending) == 0 {
			inst.Pending = nil
		}
	}
	return evs
}
```

Clear `Pending` in `Instrument.Reset()` (set `i.Pending = nil`).

- [ ] **Step 4: Run, verify pass**

Run: `go test ./... -v 2>&1 | tail -20`
Expected: PASS. Check `parity_test.go` still passes (lossless in-order replay must be byte-identical — the drain path must reproduce in-order application exactly).

- [ ] **Step 5: Commit**

```bash
git add instrument.go shard.go shard_test.go
git commit -m "marketbyorder-bot: tolerate delta reordering before declaring a gap"
```

---

## Task 5: Serve last-good book during Gap, flagged stale

**Files:**
- Modify: `snapshot_writer.go`
- Test: `snapshot_writer_test.go`

Per the confirmed design decision, a `Gap` instrument keeps serving its last-good book, marked stale, until repaired. Today `snapshot_writer.go:151` only serves `StatusReady`.

- [ ] **Step 1: Read** `snapshot_writer.go` around the `inst.Status != StatusReady` guard and `level_snapshots` row construction to find the stale-flag insertion point. If `level_snapshots` has no stale column, add a boolean `stale` field to the in-memory row struct and the ClickHouse DDL/insert (mirror an existing boolean column; keep the column name `stale UInt8`).

- [ ] **Step 2: Write failing test** in `snapshot_writer_test.go`: build an instrument in `StatusGap` with a non-empty book, run the writer's level-extraction, assert it emits the levels with `stale = true`; an instrument in `StatusReady` emits `stale = false`; an instrument in `StatusAwaitingSnapshot` (no book) emits nothing.

- [ ] **Step 3: Implement:** change the guard from `inst.Status != StatusReady` to `inst.Status == StatusAwaitingSnapshot` (skip only no-book instruments) and set `stale = (inst.Status == StatusGap)` on the emitted rows.

- [ ] **Step 4: Run, verify pass**

Run: `go test ./... -v 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add snapshot_writer.go snapshot_writer_test.go clickhouse.go
git commit -m "marketbyorder-bot: serve last-good book during gap, flagged stale"
```

---

## Task 6: Loss-simulation integration test + full verification

**Files:**
- Test: `shard_test.go` (new integration-style test)

- [ ] **Step 1: Write the test** — drive a single shard through a realistic sequence for one instrument: cold-start snapshot (complete) → Ready; a run of in-order deltas; then a scripted re-snapshot every K deltas where one `snapshot_order` is *dropped* (omitted) on, say, every 3rd re-snapshot. Assert: the instrument stays `Ready` throughout (snapshots are ignored while Ready), `BookDemotionsTotal == 0`, and the final book equals the book produced by the same delta sequence with no snapshots at all.

```go
func TestSteadyStateIgnoresLossySnapshots(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	// cold start
	s.apply(snapshotBeginRec(0, 7, 1, 1, 1000, 0))
	s.apply(snapshotOrderRec(0, 1, 100, 0, 50, 10))
	s.apply(snapshotEndRec(0, 7, 1, 1000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("cold-start snapshot should make it Ready")
	}
	// interleave deltas with lossy re-snapshots
	for i := uint32(1); i <= 30; i++ {
		s.apply(orderAddRec(0, 7, i, uint64(1000+i), int64(50+i), 1))
		if i%5 == 0 {
			snap := 10 + i
			s.apply(snapshotBeginRec(0, 7, snap, 3, uint64(2000+i), i))
			s.apply(snapshotOrderRec(0, snap, 200, 0, 60, 1))
			if i%3 != 0 { // drop an order on some snapshots
				s.apply(snapshotOrderRec(0, snap, 201, 0, 61, 1))
				s.apply(snapshotOrderRec(0, snap, 202, 0, 62, 1))
			}
			s.apply(snapshotEndRec(0, 7, snap, uint64(2000+i)))
		}
	}
	if s.instruments[k].Status != StatusReady {
		t.Fatalf("Ready instrument must ignore lossy re-snapshots; got %v", s.instruments[k].Status)
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 0 {
		t.Fatalf("book_demotions_total = %v, want 0", got)
	}
}
```

- [ ] **Step 2: Run, verify pass**

Run: `go test ./... -v 2>&1 | tail -20`
Expected: PASS.

- [ ] **Step 3: Full suite + vet**

Run: `go test ./... && go vet ./...`
Expected: all green, no vet warnings introduced.

- [ ] **Step 4: Commit**

```bash
git add shard_test.go
git commit -m "marketbyorder-bot: integration test for lossy-snapshot steady state"
```

---

## Self-review notes (author)

- **Spec coverage:** non-demoting end (T1/T2), ignore-when-ready (T2), shadow build + atomic commit + forward replay (T1/T2, `replayBuffer` reused), reorder tolerance (T4), GAP flagged-stale serving (T5), metrics incl. `book_demotions_total` regression guard (T3), no wire change (no parser/protocol files touched), loss-simulation test (T6). All covered.
- **Status-rename ripple:** removing `StatusBuildingSnapshot` requires fixing every reference (`shard.go:161` old order check, `String()`); `grep -rn StatusBuildingSnapshot .` before finishing T2 must return nothing.
- **Consumer contract:** the spec also calls for documenting the rule in the feed spec for FPGA teams. That is a docs change in a separate repo/spec, tracked as a follow-up, not a code task here.
- **Forward reconciliation:** `replayBuffer` already keys on `MktdataSeq > LastAppliedMktdataSeq`; after a commit sets `LastAppliedMktdataSeq = anchor`, buffered deltas past the anchor replay correctly. T6 exercises this.
