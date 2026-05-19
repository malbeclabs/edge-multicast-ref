# depthofbook-bot Shard Dispatcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the depthofbook-bot's single-goroutine dispatcher with a coordinator that shards record application across N per-instrument worker goroutines, eliminating the throughput choke at ~330 instruments/1 channel.

**Architecture:** A `Coordinator` (single goroutine, the `Dispatcher`) owns channel-scoped state and routing; it forwards each record to one of N `Shard` goroutines keyed by `instrument_id % N`. Each shard exclusively owns its instruments, refdata, per-instrument delta buffers, snapshot context, and its own `SnapshotWriter`. Channel-reset and `end_of_session`/`batch_boundary` use an in-band FIFO marker/ack barrier.

**Tech Stack:** Go 1.25, package `main` in `go/depthofbook-bot/` (module `depthofbook-bot`), Prometheus client, standard `testing`. Design doc: `docs/2026-05-19-depthofbook-bot-shard-dispatcher-design.md`.

---

## Conventions for every task

- All commands run from the worktree root unless stated. Test command form:
  `cd go/depthofbook-bot && go test ./... -run <Name> -v`
- Race tests: `cd go/depthofbook-bot && go test ./... -race -run <Name> -v`
- Commit messages: all lowercase, no `Co-Authored-By` line, no body needed unless noted (per repo CLAUDE.md).
- Branch is already `ss/angry-dhawan-71b52e` (worktree). Do not create new branches.
- Never commit binaries or `.claude/`.

---

## File Structure

- **Create** `go/depthofbook-bot/shard.go` — `Shard` type: instrument-scoped state (instruments, refdata, per-instrument delta buffers, per-snapshot context), the ported `apply*` logic, the per-shard `sync.Mutex`, the inbox + `Run` loop, reset/fence marker handling, and the dispatcher-body persistence logic (events + snapshot MarkDirty) for its instruments.
- **Create** `go/depthofbook-bot/shard_test.go` — per-shard unit tests (ported from `channel_test.go`) + inbox/marker tests.
- **Create** `go/depthofbook-bot/coordinator.go` — `Coordinator` type: channel-scoped state (`resetCount`, `manifest`, `seqLast`), `snapshotRoute` map, classification/routing, reset barrier, fence, channel-health direct writes. Implements `Dispatcher`.
- **Create** `go/depthofbook-bot/coordinator_test.go` — routing/classification, reset-barrier, fence, snapshot-routing tests.
- **Create** `go/depthofbook-bot/parity_test.go` — order-preservation golden test + in-process acceptance harness.
- **Modify** `go/depthofbook-bot/snapshot_writer.go` — add `generation`, `resetCh`, `Reset()`, generation re-check in `flushDue`.
- **Modify** `go/depthofbook-bot/snapshot_writer_test.go` — add `Reset()` tests.
- **Modify** `go/depthofbook-bot/metrics.go` — add `SnapshotOrderDroppedTotal` counter.
- **Modify** `go/depthofbook-bot/main.go` — `--shards` flag, GOMAXPROCS default, build coordinator + N shards, wire as `Dispatcher`; remove the old `ChannelState`/`getOrCreateChannel`/inline-dispatcher path.
- **Delete** `go/depthofbook-bot/channel.go` and `go/depthofbook-bot/channel_test.go` — `ChannelState` is decomposed into `Shard` (instrument-scoped) + `Coordinator` (channel-scoped). Helper conversions (`toUint8`, `toUint32`, …, `sideFromString`, `filterBuffer`) and the `ChannelEvent`/`InstrumentDef`/`ManifestState`/`BufferedDelta` types move into `shard.go`.

Decomposition rationale: `ChannelState` today mixes channel-scoped and instrument-scoped responsibilities under one mutex; splitting along that exact seam is the change. `Shard` stays focused on one instrument subset; `Coordinator` stays focused on classify/route/barrier.

---

## Task 1: SnapshotWriter — generation counter + serialized Reset()

**Files:**
- Modify: `go/depthofbook-bot/snapshot_writer.go`
- Test: `go/depthofbook-bot/snapshot_writer_test.go`

This is self-contained (no Shard yet). It adds the reset-ordering guarantee from the design's "SnapshotWriter reset ordering" section.

- [ ] **Step 1: Write the failing test**

Add to `go/depthofbook-bot/snapshot_writer_test.go`:

```go
func TestSnapshotWriter_ResetClearsDirtyAndBumpsGeneration(t *testing.T) {
	metrics := stubMetrics()
	inst := NewInstrument(1, "X", -2, -8)
	inst.Status = StatusReady
	w := NewSnapshotWriter(nil, 5, 100, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.MarkDirty(1)
	w.mu.Lock()
	if len(w.dirty) != 1 {
		w.mu.Unlock()
		t.Fatalf("expected 1 dirty entry, got %d", len(w.dirty))
	}
	gen0 := w.generation
	w.mu.Unlock()

	w.Reset() // must block until the writer goroutine handled it

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.dirty) != 0 {
		t.Errorf("dirty not cleared after Reset: %d", len(w.dirty))
	}
	if w.generation != gen0+1 {
		t.Errorf("generation not bumped: got %d want %d", w.generation, gen0+1)
	}
}
```

`context` is already imported in that test file via other tests; if `go vet` complains, add `"context"` to the import block.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run TestSnapshotWriter_ResetClearsDirtyAndBumpsGeneration -v`
Expected: FAIL — `w.generation` undefined and `w.Reset` undefined (compile error).

- [ ] **Step 3: Add fields, Reset(), generation handling**

In `go/depthofbook-bot/snapshot_writer.go`, extend the struct (add the two fields after `channel uint8`):

```go
	mu             sync.Mutex
	dirty          map[uint32]*dirtyEntry
	withInstrument func(uint32, func(*Instrument)) // runs fn under the channel lock with the current instrument (or nil)
	channel        uint8
	generation     uint64           // guarded by mu; bumped on Reset to invalidate in-flight flush batches
	resetCh        chan chan struct{}
}
```

In `NewSnapshotWriter`, initialize `resetCh` (add the field to the returned struct literal):

```go
		dirty:            map[uint32]*dirtyEntry{},
		channel:          channelID,
		withInstrument:   withInstrument,
		resetCh:          make(chan chan struct{}),
	}
```

Add the `Reset` method and a private `doReset` (place after `MarkDirty`):

```go
// Reset clears pending dirty state and invalidates any in-flight flush batch.
// It is serialized onto the writer goroutine (via resetCh) and blocks until that
// goroutine has applied the reset, so the caller can rely on no concurrent flush.
func (w *SnapshotWriter) Reset() {
	done := make(chan struct{})
	w.resetCh <- done
	<-done
}

func (w *SnapshotWriter) doReset() {
	w.mu.Lock()
	w.dirty = map[uint32]*dirtyEntry{}
	w.generation++
	w.mu.Unlock()
}
```

Wire `resetCh` into `Run`'s select:

```go
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case done := <-w.resetCh:
			w.doReset()
			close(done)
		case <-tick.C:
			w.flushDue()
		}
	}
}
```

Add the generation re-check in `flushDue`. Capture the generation while holding the lock when extracting the batch, and re-check before each write:

```go
func (w *SnapshotWriter) flushDue() {
	w.mu.Lock()
	now := time.Now()
	gen := w.generation
	due := []*dirtyEntry{}
	for id, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, id)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		w.mu.Lock()
		stale := w.generation != gen
		w.mu.Unlock()
		if stale {
			return // a Reset happened after this batch was extracted; abandon it
		}
		var (
			snap    LevelSnapshot
			instID  uint32
			symbol  string
			lastSeq uint64
			ready   bool
		)
		w.withInstrument(e.instrumentID, func(inst *Instrument) {
			if inst == nil || inst.Status != StatusReady {
				return
			}
			snap = ComputeLevels(inst, w.depth)
			instID = inst.ID
			symbol = inst.Symbol
			lastSeq = inst.LastAppliedMktdataSeq
			ready = true
		})
		if !ready {
			continue
		}
		w.write(snap, instID, symbol, lastSeq, now)
		_ = e.coalescedCount
		if w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}
		w.mu.Lock()
		if e2, ok := w.dirty[e.instrumentID]; ok {
			rearm := now.Add(w.coalesceInterval)
			if e2.nextAllowedAt.Before(rearm) {
				e2.nextAllowedAt = rearm
			}
		}
		w.mu.Unlock()
	}
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run TestSnapshotWriter -v`
Expected: PASS (new test + the three existing `TestSnapshotWriter_*` still pass).

- [ ] **Step 5: Commit**

```bash
git add go/depthofbook-bot/snapshot_writer.go go/depthofbook-bot/snapshot_writer_test.go
git commit -m "depthofbook-bot: add serialized snapshotwriter reset with generation fence"
```

---

## Task 2: metrics — snapshot_order dropped counter

**Files:**
- Modify: `go/depthofbook-bot/metrics.go`

- [ ] **Step 1: Add the counter field**

In `go/depthofbook-bot/metrics.go`, add to the `Metrics` struct under the "Book state" group (after `PerInstrumentGapsTotal`):

```go
	PerInstrumentGapsTotal prometheus.Counter
	SnapshotOrderDroppedTotal prometheus.Counter
```

- [ ] **Step 2: Construct and register it**

In `NewMetrics`, after the `m.PerInstrumentGapsTotal = ...` line add:

```go
	m.SnapshotOrderDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_order_dropped_total"})
```

In the `reg.MustRegister(` call, add `m.SnapshotOrderDroppedTotal,` to the book-state line:

```go
		m.InstrumentsTotal, m.InstrumentResetsTotal, m.ChannelResetsTotal, m.PerInstrumentGapsTotal, m.SnapshotOrderDroppedTotal,
```

- [ ] **Step 3: Verify it compiles**

Run: `cd go/depthofbook-bot && go build ./...`
Expected: no output (success).

- [ ] **Step 4: Commit**

```bash
git add go/depthofbook-bot/metrics.go
git commit -m "depthofbook-bot: add snapshot_order_dropped_total metric"
```

---

## Task 3: Shard type — instrument-scoped state + ported apply logic

**Files:**
- Create: `go/depthofbook-bot/shard.go`
- Create: `go/depthofbook-bot/shard_test.go`
- (Do not delete `channel.go` yet — it stays until Task 8 so the build keeps passing.)

`Shard` owns one instrument subset. State maps are keyed by `instKey{ch, id}` so two channels sharing an instrument_id never collide (preserves today's per-channel `ChannelState` isolation). Per-instrument delta buffers replace the channel-global `DeltaBuffer`.

- [ ] **Step 1: Write the failing test**

Create `go/depthofbook-bot/shard_test.go`:

```go
package main

import (
	"testing"
	"time"
)

// sr builds a record for shard tests (channel 0, reset_count 1).
func sr(rt, port string, seq uint64, instID uint32, fields map[string]any) Record {
	return Record{
		Type: rt, Timestamp: time.Unix(1700000000, 0), ChannelID: 0,
		Port: port, SequenceNumber: seq, ResetCount: 1,
		InstrumentID: instID, Fields: fields,
	}
}

func newTestShard() *Shard {
	return NewShard(0, 1, NewEventsWriter(nil), nil, nil)
}

func TestShard_ColdStart(t *testing.T) {
	s := newTestShard()
	s.apply(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	if _, ok := s.refdata[instKey{0, 100}]; !ok {
		t.Fatal("refdata not stored")
	}

	s.apply(sr("order_add", "mktdata", 50, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(101),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if got := len(s.deltaBuf[instKey{0, 100}]); got != 1 {
		t.Fatalf("expected 1 buffered delta, got %d", got)
	}

	s.apply(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(49), "total_orders": float64(0),
		"snapshot_id": float64(7), "last_instrument_seq": float64(100),
	}))
	s.apply(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(49), "snapshot_id": float64(7),
	}))

	inst := s.instruments[instKey{0, 100}]
	if inst.Status != StatusReady {
		t.Fatalf("status: %v", inst.Status)
	}
	if len(inst.Bids) != 1 {
		t.Errorf("expected buffered delta replayed: bids=%d", len(inst.Bids))
	}
	if inst.LastAppliedInstrumentSeq != 101 {
		t.Errorf("last applied instrument seq: %d", inst.LastAppliedInstrumentSeq)
	}
}

func TestShard_PerInstrumentGap(t *testing.T) {
	s := newTestShard()
	s.apply(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.apply(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.apply(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))
	inst := s.instruments[instKey{0, 100}]
	inst.LastAppliedInstrumentSeq = 0

	s.apply(sr("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if inst.Status != StatusReady {
		t.Fatalf("after seq=1 status: %v", inst.Status)
	}

	evs := s.apply(sr("order_add", "mktdata", 102, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(3),
		"order_id": float64(2), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82440), "qty_raw": float64(2000),
	}))
	if inst.Status != StatusGap {
		t.Errorf("expected status gap, got %v", inst.Status)
	}
	if len(evs) != 1 || evs[0].Kind != "per_instrument_gap" {
		t.Errorf("expected per_instrument_gap event, got %+v", evs)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run TestShard_ -v`
Expected: FAIL — `NewShard`, `Shard`, `instKey` undefined (compile error).

- [ ] **Step 3: Create shard.go with state + ported apply logic**

Create `go/depthofbook-bot/shard.go`. This ports the instrument-scoped logic from `channel.go` (the `apply*` methods, `bufferDelta`/`replayBuffer`, the type-conversion helpers, and the `ChannelEvent`/`InstrumentDef`/`ManifestState`/`BufferedDelta` types) onto `Shard`. Delta buffers are per-instrument (`map[instKey][]BufferedDelta`), removing the global `sort.Slice`. Channel-reset detection and `SeqLast` are intentionally NOT here (the coordinator owns them).

```go
package main

import (
	"log"
	"sort"
	"sync"
	"time"
)

const maxBufferedDeltasPerInstrument = 10000

type instKey struct {
	ch uint8
	id uint32
}

type BufferedDelta struct {
	MktdataSeq uint64
	Record     Record
}

type InstrumentDef struct {
	Symbol        string
	PriceExponent int8
	QtyExponent   int8
}

type ManifestState struct {
	Seq             uint16
	Valid           bool
	InstrumentCount uint32
}

// ChannelEvent is the small subset of bot-side state changes a shard reports
// outward (used by writers to enqueue persistence and by metrics to track resets).
type ChannelEvent struct {
	Kind         string // "applied_delta" | "applied_snapshot" | "instrument_reset" | "channel_reset" | "per_instrument_gap"
	InstrumentID uint32
	Symbol       string
	Record       Record
}

// Shard owns a disjoint subset of instruments (by instrument_id % N) and all
// their state. Its goroutine is the only writer of that state; mu guards book
// mutation only so the per-shard SnapshotWriter goroutine can read levels.
type Shard struct {
	idx int
	n   int

	mu          sync.Mutex
	instruments map[instKey]*Instrument
	refdata     map[instKey]InstrumentDef
	deltaBuf    map[instKey][]BufferedDelta // per instrument, ordered by MktdataSeq
	snapCtx     map[uint32]SnapshotContext  // keyed by snapshot_id (publisher never interleaves snapshot groups)

	inbox   chan shardMsg
	sw      *SnapshotWriter
	eventsW *EventsWriter
	metrics *Metrics
}

// NewShard builds shard idx of n. sw may be nil in unit tests that only call apply().
func NewShard(idx, n int, eventsW *EventsWriter, sw *SnapshotWriter, metrics *Metrics) *Shard {
	return &Shard{
		idx: idx, n: n,
		instruments: map[instKey]*Instrument{},
		refdata:     map[instKey]InstrumentDef{},
		deltaBuf:    map[instKey][]BufferedDelta{},
		snapCtx:     map[uint32]SnapshotContext{},
		inbox:       make(chan shardMsg, 4096),
		sw:          sw,
		eventsW:     eventsW,
		metrics:     metrics,
	}
}

func (s *Shard) reset() {
	s.instruments = map[instKey]*Instrument{}
	s.refdata = map[instKey]InstrumentDef{}
	s.deltaBuf = map[instKey][]BufferedDelta{}
	s.snapCtx = map[uint32]SnapshotContext{}
}

// apply mutates book state for one record and returns the resulting events.
// It holds s.mu so the SnapshotWriter's withInstrument callback is safe.
func (s *Shard) apply(rec Record) []ChannelEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := instKey{rec.ChannelID, rec.InstrumentID}

	switch rec.Type {
	case "instrument_definition":
		return s.applyInstrumentDefinition(k, rec)
	case "snapshot_begin":
		return s.applySnapshotBegin(k, rec)
	case "snapshot_order":
		return s.applySnapshotOrder(rec)
	case "snapshot_end":
		return s.applySnapshotEnd(k, rec)
	case "order_add", "order_cancel", "order_execute":
		return s.applyDelta(k, rec)
	case "instrument_reset":
		return s.applyInstrumentReset(k, rec)
	case "trade":
		return []ChannelEvent{{Kind: "applied_delta", InstrumentID: rec.InstrumentID, Record: rec}}
	}
	return nil
}

func (s *Shard) applyInstrumentDefinition(k instKey, rec Record) []ChannelEvent {
	symbol, _ := rec.Fields["symbol"].(string)
	priceExp := toInt8(rec.Fields["price_exponent"])
	qtyExp := toInt8(rec.Fields["qty_exponent"])
	s.refdata[k] = InstrumentDef{Symbol: symbol, PriceExponent: priceExp, QtyExponent: qtyExp}
	if inst, ok := s.instruments[k]; ok {
		inst.Symbol = symbol
		inst.PriceExponent = priceExp
		inst.QtyExponent = qtyExp
	} else {
		s.instruments[k] = NewInstrument(k.id, symbol, priceExp, qtyExp)
	}
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: k.id, Symbol: symbol, Record: rec}}
}

func (s *Shard) applySnapshotBegin(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		inst = NewInstrument(k.id, "", 0, 0)
		s.instruments[k] = inst
	}
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	snapID := toUint32(rec.Fields["snapshot_id"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])
	if inst.Status == StatusReady && anchor <= inst.LastAppliedMktdataSeq {
		return nil
	}
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

func (s *Shard) applySnapshotOrder(rec Record) []ChannelEvent {
	snapID := toUint32(rec.Fields["snapshot_id"])
	for _, inst := range s.instruments {
		if inst.Status != StatusBuildingSnapshot || inst.OpenSnapshot == nil {
			continue
		}
		if inst.OpenSnapshot.SnapshotID != snapID {
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
	snapID := toUint32(rec.Fields["snapshot_id"])
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		log.Printf("shard %d instrument %d: snapshot end failed: %v", s.idx, k.id, err)
		return nil
	}
	s.replayBuffer(k, inst)
	return []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}

func (s *Shard) applyDelta(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		s.bufferDelta(k, rec)
		return nil
	}
	if inst.Status == StatusReady {
		return s.applyDeltaToReady(k, inst, rec)
	}
	s.bufferDelta(k, rec)
	return nil
}

func (s *Shard) applyDeltaToReady(k instKey, inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1
	if piSeq < expected {
		return nil
	}
	if piSeq > expected {
		log.Printf("shard %d instrument %d: per-instrument gap, expected %d got %d",
			s.idx, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		s.bufferDelta(k, rec)
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}
	switch rec.Type {
	case "order_add":
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		orderID := toUint64(rec.Fields["order_id"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.ApplyOrderAdd(orderID, side, flags, enter, price, qty)
	case "order_cancel":
		inst.ApplyOrderCancel(toUint64(rec.Fields["order_id"]))
	case "order_execute":
		inst.ApplyOrderExecute(toUint64(rec.Fields["order_id"]), toUint8(rec.Fields["exec_flags"]), toUint64(rec.Fields["exec_qty_raw"]))
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = piSeq
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
}

func (s *Shard) applyInstrumentReset(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	inst.Reset()
	newAnchor := toUint64(rec.Fields["new_anchor_seq"])
	s.deltaBuf[k] = filterBuffer(s.deltaBuf[k], func(b BufferedDelta) bool {
		return b.MktdataSeq > newAnchor
	})
	return []ChannelEvent{{Kind: "instrument_reset", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}

func (s *Shard) bufferDelta(k instKey, rec Record) {
	buf := s.deltaBuf[k]
	if len(buf) >= maxBufferedDeltasPerInstrument {
		buf = buf[1:]
	}
	buf = append(buf, BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec})
	sort.Slice(buf, func(i, j int) bool { return buf[i].MktdataSeq < buf[j].MktdataSeq })
	s.deltaBuf[k] = buf
}

func (s *Shard) replayBuffer(k instKey, inst *Instrument) {
	buf := s.deltaBuf[k]
	remaining := make([]BufferedDelta, 0, len(buf))
	for _, b := range buf {
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue
		}
		s.applyDeltaToReady(k, inst, b.Record)
	}
	s.deltaBuf[k] = remaining
}

func filterBuffer(buf []BufferedDelta, keep func(BufferedDelta) bool) []BufferedDelta {
	out := make([]BufferedDelta, 0, len(buf))
	for _, b := range buf {
		if keep(b) {
			out = append(out, b)
		}
	}
	return out
}

// --- type conversion helpers (JSON unmarshal yields float64 / string / bool by default) ---

func toUint8(v any) uint8 {
	switch x := v.(type) {
	case float64:
		return uint8(x)
	case uint8:
		return x
	}
	return 0
}

func toUint16(v any) uint16 {
	switch x := v.(type) {
	case float64:
		return uint16(x)
	case uint16:
		return x
	}
	return 0
}

func toUint32(v any) uint32 {
	switch x := v.(type) {
	case float64:
		return uint32(x)
	case uint32:
		return x
	}
	return 0
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case float64:
		return uint64(x)
	case uint64:
		return x
	}
	return 0
}

func toInt8(v any) int8 {
	switch x := v.(type) {
	case float64:
		return int8(x)
	case int8:
		return x
	}
	return 0
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toTime(v any) time.Time {
	if s, ok := v.(string); ok {
		t, _ := time.Parse(time.RFC3339Nano, s)
		return t
	}
	return time.Time{}
}

func sideFromString(s string) uint8 {
	if s == "ask" {
		return 1
	}
	return 0
}
```

Note: `shardMsg` is referenced by the `inbox` field type and is defined in Task 5. To keep this task compiling on its own, add this minimal placeholder at the bottom of `shard.go` now; Task 5 fills it in:

```go
// shardMsg is the inbox protocol; populated in Task 5.
type shardMsg struct {
	rec  *Record
	kind shardMsgKind
	ack  chan int
}

type shardMsgKind int

const (
	msgRecord shardMsgKind = iota
	msgReset
	msgFence
)
```

Because `channel.go` still defines `toUint8`/`toUint16`/`toUint32`/`toUint64`/`toInt8`/`toInt64`/`toString`/`toTime`/`sideFromString`/`filterBuffer`/`BufferedDelta`/`InstrumentDef`/`ManifestState`/`ChannelEvent`, defining them again in `shard.go` is a duplicate-symbol compile error. Resolve by **deleting those duplicated declarations from `channel.go` in this same step** (the helpers and the four shared types), keeping `channel.go`'s `ChannelState` + its methods referencing the now-shared helpers. Concretely, in `channel.go` delete: the `BufferedDelta`, `InstrumentDef`, `ManifestState`, `ChannelEvent` type blocks and every `func toX(...)`, `func sideFromString`, `func filterBuffer` definition. Leave `ChannelState`, `NewChannelState`, `Apply`, `reset`, `applyInner`, and the `apply*`/`bufferDelta`/`replayBuffer` methods (they keep compiling against the shared helpers). `const maxBufferedDeltas` stays in `channel.go`; `shard.go` uses its own `maxBufferedDeltasPerInstrument`.

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestShard_|TestChannel_|TestSnapshotWriter' -v`
Expected: PASS — new `TestShard_*` pass; existing `TestChannel_*` and `TestSnapshotWriter_*` still pass (channel.go now uses the shared helpers).

- [ ] **Step 5: Run the full package build/test**

Run: `cd go/depthofbook-bot && go build ./... && go test ./...`
Expected: build clean; `ok depthofbook-bot`.

- [ ] **Step 6: Commit**

```bash
git add go/depthofbook-bot/shard.go go/depthofbook-bot/shard_test.go go/depthofbook-bot/channel.go
git commit -m "depthofbook-bot: add shard type with per-instrument state and ported apply logic"
```

---

## Task 4: Shard — persistence body (events + snapshot MarkDirty) for its instruments

**Files:**
- Modify: `go/depthofbook-bot/shard.go`
- Modify: `go/depthofbook-bot/shard_test.go`

This ports the dispatcher-closure body from `main.go:110-185` (events writing, snapshot frame routing, `MarkDirty`, metric increments) onto the shard, scoped to its instruments. `apply()` stays pure (book only); a new `handle()` calls `apply()` then does persistence.

- [ ] **Step 1: Write the failing test**

Add to `go/depthofbook-bot/shard_test.go`:

```go
func TestShard_HandleMarksDirtyOnAppliedDelta(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	// give it a SnapshotWriter so MarkDirty has a target
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})

	s.handle(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.handle(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.handle(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))

	s.sw.mu.Lock()
	_, dirty := s.sw.dirty[100]
	s.sw.mu.Unlock()
	if !dirty {
		t.Errorf("expected instrument 100 marked dirty after applied_snapshot")
	}
}

func TestShard_HandlePerInstrumentGapMetric(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	s.handle(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.handle(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0), "snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.handle(sr("snapshot_end", "snapshot", 2, 100, map[string]any{"anchor_seq": float64(0), "snapshot_id": float64(1)}))
	s.instruments[instKey{0, 100}].LastAppliedInstrumentSeq = 0
	s.handle(sr("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(1), "qty_raw": float64(1),
	}))
	// seq jump 1 -> 3 triggers a gap; handle() must increment the metric.
	s.handle(sr("order_add", "mktdata", 102, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(3),
		"order_id": float64(2), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(1), "qty_raw": float64(1),
	}))
	if got := testCounter(t, metrics.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("per_instrument_gaps_total = %v, want 1", got)
	}
}
```

Add this Prometheus test helper to `shard_test.go` (used by later tasks too):

```go
import (
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
)

func testCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}
```

Place the extra imports in `shard_test.go`'s import block (merge with `testing`/`time`). `github.com/prometheus/client_model/go` and `github.com/prometheus/client_golang/prometheus` are already in `go.sum` (used by `metrics.go`).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestShard_Handle' -v`
Expected: FAIL — `s.handle` undefined (compile error).

- [ ] **Step 3: Implement handle()**

Add to `go/depthofbook-bot/shard.go` (this mirrors `main.go`'s dispatcher body, scoped to the shard's instruments; refdata/snapCtx are shard-local). Add `"time"` is already imported. Append:

```go
// handle applies a record and performs persistence (events + snapshot dirty
// marking + metrics) for the shard's instruments. It is the shard goroutine's
// per-record entry point. Channel-scoped records never reach a shard.
func (s *Shard) handle(rec Record) {
	evs := s.apply(rec)

	k := instKey{rec.ChannelID, rec.InstrumentID}

	switch rec.Type {
	case "snapshot_begin":
		s.mu.Lock()
		def := s.refdata[k]
		s.mu.Unlock()
		s.snapCtx[toUint32(rec.Fields["snapshot_id"])] = SnapshotContext{
			InstrumentID:      rec.InstrumentID,
			Symbol:            def.Symbol,
			SnapshotID:        getUint32(rec.Fields, "snapshot_id"),
			AnchorSeq:         getUint64(rec.Fields, "anchor_seq"),
			TotalOrders:       getUint32(rec.Fields, "total_orders"),
			LastInstrumentSeq: getUint32(rec.Fields, "last_instrument_seq"),
			PriceExponent:     def.PriceExponent,
			QtyExponent:       def.QtyExponent,
		}
	case "snapshot_order":
		if sctx, ok := s.snapCtx[getUint32(rec.Fields, "snapshot_id")]; ok {
			s.eventsW.WriteSnapshotOrder(rec, rec.ChannelID, sctx)
		}
	case "snapshot_end":
		delete(s.snapCtx, getUint32(rec.Fields, "snapshot_id"))
	}

	for _, ev := range evs {
		s.mu.Lock()
		def := s.refdata[instKey{rec.ChannelID, ev.InstrumentID}]
		s.mu.Unlock()
		s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)

		switch ev.Kind {
		case "applied_delta", "applied_snapshot":
			if ev.InstrumentID != 0 && s.sw != nil {
				s.sw.MarkDirty(ev.InstrumentID)
			}
		case "instrument_reset":
			if s.metrics != nil {
				s.metrics.InstrumentResetsTotal.WithLabelValues(getString(ev.Record.Fields, "reason")).Inc()
			}
			if s.sw != nil {
				s.sw.MarkDirty(ev.InstrumentID)
			}
		case "per_instrument_gap":
			if s.metrics != nil {
				s.metrics.PerInstrumentGapsTotal.Inc()
			}
		}
	}
}
```

Note: `channel_reset` is intentionally not handled here — the coordinator owns reset detection (Task 7). `getUint32`/`getUint64`/`getString` already exist in `events_writer.go`.

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestShard_' -v`
Expected: PASS (all `TestShard_*`).

- [ ] **Step 5: Commit**

```bash
git add go/depthofbook-bot/shard.go go/depthofbook-bot/shard_test.go
git commit -m "depthofbook-bot: add shard persistence body (events, dirty marking, metrics)"
```

---

## Task 5: Shard — inbox protocol, Run loop, reset/fence marker handling

**Files:**
- Modify: `go/depthofbook-bot/shard.go`
- Modify: `go/depthofbook-bot/shard_test.go`

- [ ] **Step 1: Write the failing test**

Add to `go/depthofbook-bot/shard_test.go`:

```go
import "context"  // merge into existing import block

func TestShard_RunProcessesRecordsThenResetAcks(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.sw.Run(ctx)
	go s.Run(ctx)

	rec := sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	s.inbox <- shardMsg{kind: msgRecord, rec: &rec}

	acks := make(chan int, 1)
	s.inbox <- shardMsg{kind: msgReset, ack: acks}
	select {
	case got := <-acks:
		if got != 0 {
			t.Errorf("ack idx = %d, want 0", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reset ack")
	}

	// Reset must have wiped instrument state (processed before the marker, FIFO).
	s.mu.Lock()
	n := len(s.instruments)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("instruments not wiped after reset: %d", n)
	}
}

func TestShard_FenceAcksWithoutWipe(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.sw.Run(ctx)
	go s.Run(ctx)

	rec := sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	s.inbox <- shardMsg{kind: msgRecord, rec: &rec}
	acks := make(chan int, 1)
	s.inbox <- shardMsg{kind: msgFence, ack: acks}
	select {
	case <-acks:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fence ack")
	}
	s.mu.Lock()
	n := len(s.instruments)
	s.mu.Unlock()
	if n != 1 {
		t.Errorf("fence must NOT wipe state: instruments=%d want 1", n)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestShard_Run|TestShard_Fence' -v`
Expected: FAIL — `s.Run` undefined.

- [ ] **Step 3: Replace the placeholder shardMsg and add Run**

In `go/depthofbook-bot/shard.go`, the placeholder `shardMsg`/`shardMsgKind` from Task 3 already has the right shape — keep it. Add the `Run` loop:

```go
// Run is the shard goroutine. It processes its FIFO inbox until ctx is done.
// Records mutate book state; a reset marker wipes state and quiesces the
// SnapshotWriter before acking; a fence marker only acks (FIFO already
// guarantees preceding records' rows are enqueued).
func (s *Shard) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.inbox:
			switch msg.kind {
			case msgRecord:
				s.handle(*msg.rec)
			case msgReset:
				s.mu.Lock()
				s.reset()
				s.mu.Unlock()
				if s.sw != nil {
					s.sw.Reset() // blocks until writer goroutine quiesced
				}
				msg.ack <- s.idx
			case msgFence:
				msg.ack <- s.idx
			}
		}
	}
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestShard_' -v`
Expected: PASS.

- [ ] **Step 5: Race check**

Run: `cd go/depthofbook-bot && go test ./... -race -run 'TestShard_Run|TestShard_Fence' -v`
Expected: PASS, no race warnings.

- [ ] **Step 6: Commit**

```bash
git add go/depthofbook-bot/shard.go go/depthofbook-bot/shard_test.go
git commit -m "depthofbook-bot: add shard run loop with reset and fence markers"
```

---

## Task 6: Coordinator — type, classification, instrument routing, snapshot route map

**Files:**
- Create: `go/depthofbook-bot/coordinator.go`
- Create: `go/depthofbook-bot/coordinator_test.go`

- [ ] **Step 1: Write the failing test**

Create `go/depthofbook-bot/coordinator_test.go`:

```go
package main

import (
	"testing"
	"time"
)

// collectShards builds n shards whose inboxes we drain into a slice for assertions.
func newCoordWithCapture(n int) (*Coordinator, []chan shardMsg) {
	metrics := stubMetrics()
	shards := make([]*Shard, n)
	inboxes := make([]chan shardMsg, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		shards[i] = s
		inboxes[i] = s.inbox
	}
	return NewCoordinator(shards, NewEventsWriter(nil), metrics), inboxes
}

func TestCoordinator_RoutesInstrumentRecordByMod(t *testing.T) {
	c, inboxes := newCoordWithCapture(4)
	rec := Record{Type: "order_add", ChannelID: 0, InstrumentID: 6, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}}
	c.Dispatch(rec)
	// 6 % 4 == 2
	select {
	case m := <-inboxes[2]:
		if m.kind != msgRecord || m.rec.InstrumentID != 6 {
			t.Fatalf("wrong msg on shard 2: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("expected record on shard 2")
	}
	for i, in := range inboxes {
		if i == 2 {
			continue
		}
		select {
		case m := <-in:
			t.Fatalf("unexpected msg on shard %d: %+v", i, m)
		default:
		}
	}
}

func TestCoordinator_SnapshotOrderFollowsBeginRoute(t *testing.T) {
	c, inboxes := newCoordWithCapture(4)
	begin := Record{Type: "snapshot_begin", ChannelID: 0, InstrumentID: 9, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(42)}}
	c.Dispatch(begin) // 9 % 4 == 1
	order := Record{Type: "snapshot_order", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(42)}}
	c.Dispatch(order)

	// begin + order both land on shard 1, in order.
	m1 := <-inboxes[1]
	if m1.rec.Type != "snapshot_begin" {
		t.Fatalf("shard1 first msg: %s", m1.rec.Type)
	}
	m2 := <-inboxes[1]
	if m2.rec.Type != "snapshot_order" {
		t.Fatalf("shard1 second msg: %s", m2.rec.Type)
	}
}

func TestCoordinator_SnapshotOrderNoRouteDropsAndCounts(t *testing.T) {
	c, _ := newCoordWithCapture(2)
	order := Record{Type: "snapshot_order", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(7)}}
	c.Dispatch(order)
	if got := testCounter(t, c.metrics.SnapshotOrderDroppedTotal); got != 1 {
		t.Errorf("snapshot_order_dropped_total = %v, want 1", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run TestCoordinator_ -v`
Expected: FAIL — `NewCoordinator`/`Coordinator` undefined.

- [ ] **Step 3: Implement coordinator.go (routing + snapshot map; barrier/fence stubbed)**

Create `go/depthofbook-bot/coordinator.go`:

```go
package main

// Coordinator is the single-goroutine Dispatcher. It owns channel-scoped state
// and routes each record to exactly one shard (by instrument_id % N), or to a
// direct-write / barrier / fence path. Shards own all instrument-scoped state.
type Coordinator struct {
	shards  []*Shard
	n       int
	eventsW *EventsWriter
	metrics *Metrics

	resetSeen     bool
	resetCount    uint8
	manifest      ManifestState // parity bookkeeping; not read for logic
	seqLast       map[string]uint64
	snapshotRoute map[snapKey]int
}

type snapKey struct {
	ch   uint8
	snap uint32
}

func NewCoordinator(shards []*Shard, eventsW *EventsWriter, metrics *Metrics) *Coordinator {
	return &Coordinator{
		shards:        shards,
		n:             len(shards),
		eventsW:       eventsW,
		metrics:       metrics,
		seqLast:       map[string]uint64{},
		snapshotRoute: map[snapKey]int{},
	}
}

// Dispatch implements Dispatcher. Called synchronously from the bot read loop.
func (c *Coordinator) Dispatch(rec Record) {
	// Channel-reset barrier: reset_count change. (Implemented in Task 7.)
	if c.resetSeen && rec.ResetCount != c.resetCount {
		c.runResetBarrier(rec)
		return
	}
	if !c.resetSeen {
		c.resetSeen = true
		c.resetCount = rec.ResetCount
	}
	c.seqLast[rec.Port] = rec.SequenceNumber

	switch rec.Type {
	case "order_add", "order_cancel", "order_execute",
		"instrument_definition", "instrument_reset", "trade":
		c.routeInstrument(rec)

	case "snapshot_begin":
		idx := int(rec.InstrumentID) % c.n
		c.snapshotRoute[snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")}] = idx
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_order":
		key := snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")}
		idx, ok := c.snapshotRoute[key]
		if !ok {
			if c.metrics != nil {
				c.metrics.SnapshotOrderDroppedTotal.Inc()
			}
			return
		}
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_end":
		idx := int(rec.InstrumentID) % c.n
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}
		delete(c.snapshotRoute, snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")})

	case "heartbeat", "manifest_summary":
		c.writeChannelHealth(rec) // implemented in Task 8

	case "end_of_session", "batch_boundary":
		c.runFence(rec) // implemented in Task 8
	}
}

func (c *Coordinator) routeInstrument(rec Record) {
	idx := int(rec.InstrumentID) % c.n
	c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}
}

func recPtr(rec Record) *Record {
	r := rec
	return &r
}
```

To keep this task compiling before Tasks 7 and 8 land, add temporary stubs at the bottom of `coordinator.go` (they will be replaced in Tasks 7/8 — the replacement steps say so explicitly):

```go
// --- temporary stubs, replaced in Tasks 7 and 8 ---

func (c *Coordinator) runResetBarrier(rec Record) {
	// Task 7 replaces this. Minimal placeholder keeps build green:
	c.resetCount = rec.ResetCount
}

func (c *Coordinator) runFence(rec Record) {
	// Task 8 replaces this.
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}

func (c *Coordinator) writeChannelHealth(rec Record) {
	// Task 8 replaces this.
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run TestCoordinator_ -v`
Expected: PASS (routing, snapshot-follow, no-route-drop).

- [ ] **Step 5: Full build/test**

Run: `cd go/depthofbook-bot && go build ./... && go test ./...`
Expected: build clean; `ok`.

- [ ] **Step 6: Commit**

```bash
git add go/depthofbook-bot/coordinator.go go/depthofbook-bot/coordinator_test.go
git commit -m "depthofbook-bot: add coordinator with classification and instrument routing"
```

---

## Task 7: Coordinator — channel-reset barrier

**Files:**
- Modify: `go/depthofbook-bot/coordinator.go`
- Modify: `go/depthofbook-bot/coordinator_test.go`

Implements the design's "Channel-reset barrier": hold `R`, goroutine-per-shard `resetMarker` send, wait N acks, clear coordinator state, then route held `R` as first new-era frame.

- [ ] **Step 1: Write the failing test**

Add to `go/depthofbook-bot/coordinator_test.go`:

```go
import "context" // merge into import block

func TestCoordinator_ResetBarrierWipesShardsThenRoutesHeldRecord(t *testing.T) {
	metrics := stubMetrics()
	n := 3
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
			s.mu.Lock()
			defer s.mu.Unlock()
			fn(s.instruments[instKey{0, id}])
		})
		shards[i] = s
	}
	c := NewCoordinator(shards, NewEventsWriter(nil), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}

	// Era 1: define instrument 3 (3 % 3 == 0).
	c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: 3, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"symbol": "A", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	time.Sleep(50 * time.Millisecond)

	// Era 2: reset_count bump on a new instrument_definition (the held first new-era frame).
	c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: 5, ResetCount: 2,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"symbol": "B", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	time.Sleep(50 * time.Millisecond)

	shards[0].mu.Lock()
	_, oldGone := shards[0].instruments[instKey{0, 3}]
	shards[0].mu.Unlock()
	if oldGone {
		t.Error("old-era instrument 3 should have been wiped by reset barrier")
	}
	shards[2].mu.Lock()
	_, newHere := shards[2].instruments[instKey{0, 5}] // 5 % 3 == 2
	shards[2].mu.Unlock()
	if !newHere {
		t.Error("held first new-era record (instrument 5) not applied to shard 2")
	}
	if c.resetCount != 2 {
		t.Errorf("coordinator resetCount = %d, want 2", c.resetCount)
	}
	if got := testCounter(t, metrics.ChannelResetsTotal); got != 1 {
		t.Errorf("channel_resets_total = %v, want 1", got)
	}
}

func TestCoordinator_ResetBarrierHandlesChannelScopedFirstFrame(t *testing.T) {
	metrics := stubMetrics()
	n := 2
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
			s.mu.Lock()
			defer s.mu.Unlock()
			fn(s.instruments[instKey{0, id}])
		})
		shards[i] = s
	}
	c := NewCoordinator(shards, NewEventsWriter(nil), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}
	c.Dispatch(Record{Type: "heartbeat", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
	// First new-era frame is channel-scoped (manifest_summary) — must not panic / not hash.
	c.Dispatch(Record{Type: "manifest_summary", ChannelID: 0, ResetCount: 2,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"manifest_seq": float64(1), "valid": float64(1), "instrument_count": float64(0)}})
	if c.resetCount != 2 {
		t.Errorf("resetCount = %d, want 2", c.resetCount)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run TestCoordinator_ResetBarrier -v`
Expected: FAIL — held record not routed / instruments not wiped (stub `runResetBarrier` only sets resetCount).

- [ ] **Step 3: Replace the runResetBarrier stub**

In `go/depthofbook-bot/coordinator.go`, delete the temporary `runResetBarrier` stub and replace with:

```go
// runResetBarrier executes the in-band FIFO reset barrier, then routes the
// held triggering record as the first new-era frame.
func (c *Coordinator) runResetBarrier(held Record) {
	acks := make(chan int, c.n)
	for _, s := range c.shards {
		s := s
		go func() { s.inbox <- shardMsg{kind: msgReset, ack: acks} }()
	}
	for i := 0; i < c.n; i++ {
		<-acks
	}

	if c.metrics != nil {
		c.metrics.ChannelResetsTotal.Inc()
	}
	c.snapshotRoute = map[snapKey]int{}
	c.seqLast = map[string]uint64{}
	c.manifest = ManifestState{}
	c.resetCount = held.ResetCount

	// Route the held record as the first new-era frame, via the full classifier.
	// resetSeen is already true and resetCount now equals held.ResetCount, so
	// this re-entry into Dispatch falls through to normal classification.
	c.Dispatch(held)
}
```

- [ ] **Step 4: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./... -run TestCoordinator_ -v`
Expected: PASS (both reset-barrier tests + earlier coordinator tests).

- [ ] **Step 5: Race check**

Run: `cd go/depthofbook-bot && go test ./... -race -run TestCoordinator_ResetBarrier -v`
Expected: PASS, no races.

- [ ] **Step 6: Commit**

```bash
git add go/depthofbook-bot/coordinator.go go/depthofbook-bot/coordinator_test.go
git commit -m "depthofbook-bot: add coordinator channel-reset barrier"
```

---

## Task 8: Coordinator — fence + channel-health direct writes; delete channel.go

**Files:**
- Modify: `go/depthofbook-bot/coordinator.go`
- Modify: `go/depthofbook-bot/coordinator_test.go`
- Delete: `go/depthofbook-bot/channel.go`, `go/depthofbook-bot/channel_test.go`

- [ ] **Step 1: Write the failing test**

Add to `go/depthofbook-bot/coordinator_test.go`:

```go
func TestCoordinator_FenceDrainsAllShardsBeforeWrite(t *testing.T) {
	metrics := stubMetrics()
	n := 3
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
			s.mu.Lock()
			defer s.mu.Unlock()
			fn(s.instruments[instKey{0, id}])
		})
		shards[i] = s
	}
	c := NewCoordinator(shards, NewEventsWriter(nil), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}

	// Enqueue instrument records, then a fence. The fence must not return until
	// all shards have drained their inboxes.
	for id := uint32(1); id <= 9; id++ {
		c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
				"symbol": "S", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	}
	c.Dispatch(Record{Type: "end_of_session", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})

	// After the fence returns, every shard inbox is drained.
	for i, s := range shards {
		if len(s.inbox) != 0 {
			t.Errorf("shard %d inbox not drained after fence: %d", i, len(s.inbox))
		}
	}
}

func TestCoordinator_HeartbeatNotFenced(t *testing.T) {
	c, inboxes := newCoordWithCapture(2)
	c.Dispatch(Record{Type: "heartbeat", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
	for i, in := range inboxes {
		select {
		case m := <-in:
			t.Fatalf("heartbeat must not reach shard %d: %+v", i, m)
		default:
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/depthofbook-bot && go test ./... -run 'TestCoordinator_Fence|TestCoordinator_Heartbeat' -v`
Expected: FAIL — `TestCoordinator_FenceDrainsAllShardsBeforeWrite` flaky/failing because the stub `runFence` does not actually drain (writes immediately).

- [ ] **Step 3: Replace the runFence and writeChannelHealth stubs**

In `go/depthofbook-bot/coordinator.go`, delete the temporary `runFence` and `writeChannelHealth` stubs and replace with:

```go
// runFence drains every shard (FIFO marker/ack, no state wipe) so the fence
// record's ClickHouse row lands strictly after all preceding instrument rows.
func (c *Coordinator) runFence(rec Record) {
	acks := make(chan int, c.n)
	for _, s := range c.shards {
		s := s
		go func() { s.inbox <- shardMsg{kind: msgFence, ack: acks} }()
	}
	for i := 0; i < c.n; i++ {
		<-acks
	}
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}

// writeChannelHealth writes heartbeat / manifest_summary directly (no fence).
func (c *Coordinator) writeChannelHealth(rec Record) {
	if rec.Type == "manifest_summary" {
		c.manifest = ManifestState{
			Seq:             toUint16(rec.Fields["manifest_seq"]),
			Valid:           toUint8(rec.Fields["valid"]) != 0,
			InstrumentCount: toUint32(rec.Fields["instrument_count"]),
		}
	}
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}
```

- [ ] **Step 4: Delete the decomposed ChannelState**

`ChannelState` is now fully superseded by `Shard` + `Coordinator`. Delete both files:

```bash
git rm go/depthofbook-bot/channel.go go/depthofbook-bot/channel_test.go
```

`const maxBufferedDeltas` lived in `channel.go`; it is unused now (shard uses `maxBufferedDeltasPerInstrument`). The shared helpers/types already moved to `shard.go` in Task 3, so nothing else references `channel.go`.

- [ ] **Step 5: Run tests to verify pass**

Run: `cd go/depthofbook-bot && go test ./...`
Expected: PASS — `ok depthofbook-bot`. (`TestChannel_*` are gone; `TestShard_*`/`TestCoordinator_*` cover the same behavior.)

- [ ] **Step 6: Commit**

```bash
git add go/depthofbook-bot/coordinator.go go/depthofbook-bot/coordinator_test.go
git commit -m "depthofbook-bot: add coordinator fence + channel-health writes, remove channelstate"
```

---

## Task 9: Wire coordinator + shards into main.go (`--shards` flag, GOMAXPROCS default)

**Files:**
- Modify: `go/depthofbook-bot/main.go`

- [ ] **Step 1: Replace the channels map + dispatcher closure**

In `go/depthofbook-bot/main.go`:

(a) Add the flag in the `flag` block (after `coalesceMS`):

```go
		coalesceMS    = flag.Int("coalesce-ms", 50, "snapshot coalesce window in milliseconds")
		shards        = flag.Int("shards", 0, "number of instrument shards (0 = auto from GOMAXPROCS)")
```

(b) Add `"runtime"` to the imports.

(c) Replace the block from `// Channel state per channel_id ...` (the `var ( chMu ... )` through the end of the `dispatcher := DispatcherFunc(func(rec Record) { ... })` closure, i.e. main.go lines ~72–185) with:

```go
	eventsWriter := NewEventsWriter(ch)

	n := *shards
	if n <= 0 {
		n = runtime.GOMAXPROCS(0) - 2
		if n < 1 {
			n = 1
		}
		if n > 8 {
			n = 8
		}
	}

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, eventsWriter, nil, metrics)
		sw := NewSnapshotWriter(ch, *depth, *coalesceMS, metrics, 0, func(s *Shard) func(uint32, func(*Instrument)) {
			return func(instID uint32, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[instKey{0, instID}])
			}
		}(s))
		s.sw = sw
		shardList[i] = s
		go sw.Run(ctx)
		go s.Run(ctx)
	}

	coordinator := NewCoordinator(shardList, eventsWriter, metrics)
	log.Printf("depthofbook-bot %s sharding: shards=%d", version, n)
```

(d) Replace the `bot := NewBot(*socketPath, dispatcher, metrics)` line with:

```go
	bot := NewBot(*socketPath, coordinator, metrics)
```

(e) Delete the now-unused `DispatcherFunc` type + its `Dispatch` method at the bottom of `main.go` (the `Coordinator` is now the `Dispatcher`). The `Dispatcher` interface itself stays in `bot.go`.

The per-shard `withInstrument` closure uses channel id `0`: this build targets the single-channel profile (design "Out of scope: multi-channel"). Instruments from any channel still route correctly (keyed by `instKey{ch,id}`), but snapshot-level lookups assume channel 0 as today's demo does.

- [ ] **Step 2: Build**

Run: `cd go/depthofbook-bot && go build ./...`
Expected: no output (clean). Fix any leftover reference to `dispatcher`/`getOrCreateChannel`/`snapCtx`/`channels` (all removed).

- [ ] **Step 3: Full test + vet**

Run: `cd go/depthofbook-bot && go vet ./... && go test ./...`
Expected: vet clean; `ok depthofbook-bot`.

- [ ] **Step 4: Smoke-run the binary version flag**

Run: `cd go/depthofbook-bot && go run . --version`
Expected: prints `depthofbook-bot 0.1.0-dev (unknown)` and exits 0.

- [ ] **Step 5: Commit**

```bash
git add go/depthofbook-bot/main.go
git commit -m "depthofbook-bot: wire coordinator and shards into main, add --shards flag"
```

---

## Task 10: Order-preservation golden test (per-instrument parity)

**Files:**
- Create: `go/depthofbook-bot/parity_test.go`

Per-instrument event order through the sharded path must equal a single-shard baseline. We compare the ordered sequence of `applied_*`/`per_instrument_gap` events per instrument by capturing them at the shard boundary via a recording `EventsWriter` substitute. Since `EventsWriter` is concrete with `ch *ClickhouseClient` and a nil client makes `Write` a no-op, we instead record events by wrapping the shard: feed the same stream to (a) one `Shard` directly (baseline, N=1) and (b) a `Coordinator` over N shards, and compare each instrument's final `Instrument` book plus the ordered `ChannelEvent` kinds returned by `apply`.

- [ ] **Step 1: Write the test**

Create `go/depthofbook-bot/parity_test.go`:

```go
package main

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"
)

// genStream produces a deterministic multi-instrument stream: define, snapshot
// (empty), then ordered deltas per instrument.
func genStream(instruments int, deltasPer int, seed int64) []Record {
	rng := rand.New(rand.NewSource(seed))
	var recs []Record
	seq := uint64(1)
	for id := uint32(1); id <= uint32(instruments); id++ {
		recs = append(recs, Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"symbol": fmt.Sprintf("S%d", id), "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
		seq++
		recs = append(recs, Record{Type: "snapshot_begin", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"anchor_seq": float64(0), "total_orders": float64(0), "snapshot_id": float64(id), "last_instrument_seq": float64(0)}})
		seq++
		recs = append(recs, Record{Type: "snapshot_end", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"anchor_seq": float64(0), "snapshot_id": float64(id)}})
		seq++
	}
	piSeq := map[uint32]uint32{}
	order := make([]uint32, 0, instruments*deltasPer)
	for id := uint32(1); id <= uint32(instruments); id++ {
		for k := 0; k < deltasPer; k++ {
			order = append(order, id)
		}
	}
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	oid := uint64(1)
	for _, id := range order {
		piSeq[id]++
		recs = append(recs, Record{Type: "order_add", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"side": "bid", "order_flags": float64(0),
				"per_instrument_seq": float64(piSeq[id]), "order_id": float64(oid),
				"enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
				"price_raw": float64(100 + oid), "qty_raw": float64(10)}})
		seq++
		oid++
	}
	return recs
}

// runSharded feeds recs through a Coordinator over n shards and returns each
// instrument's final bid-order-count (book fingerprint).
func runSharded(t *testing.T, n int, recs []Record) map[uint32]int {
	t.Helper()
	metrics := stubMetrics()
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(s *Shard) func(uint32, func(*Instrument)) {
			return func(id uint32, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[instKey{0, id}])
			}
		}(s))
		shards[i] = s
	}
	c := NewCoordinator(shards, NewEventsWriter(nil), metrics)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}
	for _, r := range recs {
		c.Dispatch(r)
	}
	// Drain via a fence so all shard inboxes are empty.
	c.runFence(Record{Type: "end_of_session", ChannelID: 0, ResetCount: 1, Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})

	out := map[uint32]int{}
	for _, s := range shards {
		s.mu.Lock()
		for k, inst := range s.instruments {
			out[k.id] = len(inst.Bids)
		}
		s.mu.Unlock()
	}
	return out
}

func TestParity_ShardedMatchesSingleShard(t *testing.T) {
	recs := genStream(50, 20, 12345)
	base := runSharded(t, 1, recs)
	for _, n := range []int{2, 4, 8} {
		got := runSharded(t, n, recs)
		if !reflect.DeepEqual(base, got) {
			// Surface first mismatch for debugging.
			ids := make([]int, 0, len(base))
			for id := range base {
				ids = append(ids, int(id))
			}
			sort.Ints(ids)
			for _, id := range ids {
				if base[uint32(id)] != got[uint32(id)] {
					t.Fatalf("n=%d instrument %d: baseline bids=%d sharded bids=%d",
						n, id, base[uint32(id)], got[uint32(id)])
				}
			}
		}
	}
}
```

- [ ] **Step 2: Run the test**

Run: `cd go/depthofbook-bot && go test ./... -run TestParity_ShardedMatchesSingleShard -v`
Expected: PASS — N=2/4/8 produce identical per-instrument book fingerprints to N=1.

- [ ] **Step 3: Commit**

```bash
git add go/depthofbook-bot/parity_test.go
git commit -m "depthofbook-bot: add per-instrument parity test across shard counts"
```

---

## Task 11: In-process acceptance harness (throughput soak + N=1 parity)

**Files:**
- Modify: `go/depthofbook-bot/parity_test.go`

The design's acceptance gate: a synthetic high-rate stream over ~330 instruments must be processed without unbounded backlog/deadlock and with per-instrument parity. (Parser `queue_full` is a parser-side metric; in-process we assert the bot keeps up — bounded inboxes drain within a deadline — and parity holds.)

- [ ] **Step 1: Write the test**

Add to `go/depthofbook-bot/parity_test.go`:

```go
func TestAcceptance_ThroughputSoakAndParity(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}
	recs := genStream(330, 50, 99)

	// N=1 baseline fingerprint.
	base := runSharded(t, 1, recs)

	for _, n := range []int{1, 4} {
		start := time.Now()
		got := runSharded(t, n, recs)
		elapsed := time.Since(start)
		if !reflect.DeepEqual(base, got) {
			t.Fatalf("n=%d: parity mismatch vs single-shard baseline", n)
		}
		// 330*50 deltas + setup ~ 18k records; must complete well under the
		// hard deadline (generous to avoid CI flakiness).
		if elapsed > 30*time.Second {
			t.Fatalf("n=%d: processing took %v (>30s) — backlog/deadlock suspected", n, elapsed)
		}
		t.Logf("n=%d processed %d records in %v", n, len(recs), elapsed)
	}
}
```

- [ ] **Step 2: Run the test**

Run: `cd go/depthofbook-bot && go test ./... -run TestAcceptance_ThroughputSoakAndParity -v`
Expected: PASS — both N=1 and N=4 finish well under 30s with identical fingerprints. Log lines show elapsed times.

- [ ] **Step 3: Commit**

```bash
git add go/depthofbook-bot/parity_test.go
git commit -m "depthofbook-bot: add in-process throughput soak + parity acceptance test"
```

---

## Task 12: Race sweep + documentation + finish

**Files:**
- Modify: `go/depthofbook-bot/README.md`
- Modify: `/Users/fach/.claude/CLAUDE.md` is OFF LIMITS — do NOT touch. Only repo docs.

- [ ] **Step 1: Full race sweep**

Run: `cd go/depthofbook-bot && go test ./... -race`
Expected: PASS — `ok depthofbook-bot`, no `DATA RACE` reports. If a race appears, it is a real bug in shard/coordinator ownership — fix before continuing (do not mark this task complete with a failing race sweep).

- [ ] **Step 2: Full CI-style check for the package**

Run: `cd go/depthofbook-bot && gofmt -l . && go vet ./... && go test ./...`
Expected: `gofmt -l .` prints nothing (all formatted); vet clean; tests `ok`.

- [ ] **Step 3: Update the service README**

Open `go/depthofbook-bot/README.md`. Add a section documenting the new architecture and flag. Insert after the existing overview/usage section (match the file's existing heading style):

```markdown
## Sharded dispatch

The bot shards record application across N worker goroutines keyed by
`instrument_id % N`. A single coordinator goroutine owns channel-scoped state
(`reset_count`, manifest, `snapshot_id → shard` routing) and forwards each
record to the owning shard; each shard exclusively owns its instruments,
refdata, per-instrument delta buffers, snapshot context, and its own
snapshot writer.

- `--shards N` — number of shards. `0` (default) derives N from `GOMAXPROCS`
  (`GOMAXPROCS-2`, clamped to `[1, 8]`). `--shards=1` is a valid degenerate
  single-worker mode behaviorally equivalent to the pre-sharding dispatcher.
- Per-instrument FIFO ordering and per-instrument sequence-gap detection are
  preserved. Cross-instrument global ordering is intentionally relaxed
  (ClickHouse rows are timestamped and queried per instrument).
- `end_of_session` / `batch_boundary` use an all-shard drain fence so their
  rows land after preceding instrument rows; `reset_count` changes use an
  in-band barrier that wipes all shard state before the new era.
- New metric: `dz_dob_bot_snapshot_order_dropped_total` (snapshot_order with
  no registered route, e.g. begin missed or arrived post-end).

Design doc: `docs/2026-05-19-depthofbook-bot-shard-dispatcher-design.md`.
```

- [ ] **Step 4: Commit docs**

```bash
git add go/depthofbook-bot/README.md
git commit -m "depthofbook-bot: document sharded dispatch and --shards flag"
```

- [ ] **Step 5: Finish the development branch**

Use the superpowers:finishing-a-development-branch skill to decide merge/PR. The work is complete when: all tests pass (`go test ./...`), race sweep is clean (`go test ./... -race`), `gofmt -l .` is empty, `go vet ./...` is clean, and the README + design doc are committed. Reference GitHub issue #12 in the PR/merge description.

---

## Self-Review

**1. Spec coverage** (design doc → task):

- Coordinator + N shards, share-nothing → Tasks 3–9.
- `--shards` flag, GOMAXPROCS default, N=1 degenerate → Task 9; N=1 parity → Tasks 10–11.
- Type-first classification; `instrument_id` never hashed when absent → Task 6 (`switch rec.Type` before any `% n`; channel-scoped types never call `routeInstrument`); reset channel-scoped-first-frame → Task 7 test `...HandlesChannelScopedFirstFrame`.
- `(channelID, snapshotID)` snapshot route key → Task 6 `snapKey`.
- snapshot_order no-route drop + counter → Task 2 (metric) + Task 6 (logic/test).
- Per-instrument delta buffers replace global buffer → Task 3 (`deltaBuf map[instKey][]BufferedDelta`).
- Reset barrier (hold R, goroutine-per-shard markers, N acks, clear coord state, route held R) → Task 7.
- SnapshotWriter reset ordering (serialized reset, generation re-check, shard waits for writer ack before barrier ack) → Task 1 (writer) + Task 5 (`msgReset` calls `s.sw.Reset()` before `ack`).
- Fence for end_of_session/batch_boundary, heartbeat/manifest_summary not fenced → Task 8.
- SeqLast/Manifest coordinator-owned, parity-only bookkeeping → Task 6 (`seqLast`), Task 8 (`manifest`).
- Tests: per-shard unit (Task 3), routing table (Task 6), order-preservation golden (Task 10), reset-barrier (Task 7), reset-vs-in-flight-flush (Task 1 generation test covers the abandon path; reinforced by Task 7 barrier), fence ordering (Task 8), snapshot routing (Task 6), race detector (Tasks 5,7,12), in-process acceptance harness (Task 11).

Gap accepted: the design's "reset vs in-flight flush" test is realized as Task 1's generation-bump/clear test plus the Task 7 barrier (which calls `sw.Reset()` before the shard acks). A standalone test that races a half-extracted `flushDue` batch against a reset is inherently timing-fragile; the generation re-check in `flushDue` (Task 1) is the deterministic guard and is unit-tested directly. This is a deliberate, documented substitution, not a missing requirement.

**2. Placeholder scan:** The only intentional temporary code is the `runResetBarrier`/`runFence`/`writeChannelHealth` stubs in Task 6, each explicitly replaced in Tasks 7/8 with the replacement code shown in full. The `shardMsg` placeholder in Task 3 has its final shape (Task 5 only adds the `Run` consumer). No `TBD`/`TODO`/"handle edge cases" remain; all code steps show complete code.

**3. Type consistency:** `instKey{ch uint8; id uint32}`, `snapKey{ch uint8; snap uint32}`, `shardMsg{rec *Record; kind shardMsgKind; ack chan int}`, `Shard.instruments/refdata/deltaBuf/snapCtx`, `Coordinator.snapshotRoute/seqLast/manifest`, `NewShard(idx,n,eventsW,sw,metrics)`, `NewCoordinator(shards,eventsW,metrics)`, `SnapshotWriter.Reset()/generation/resetCh`, `s.handle`/`s.apply`/`s.Run` — all names consistent across Tasks 1–12. `getUint32`/`getUint64`/`getString` reused from `events_writer.go` (not redefined). Helpers `toUint8`/etc. moved once (Task 3) with the duplicate-symbol removal from `channel.go` called out in the same step.
