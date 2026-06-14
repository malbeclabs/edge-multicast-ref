package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// testGaugeVec reads a single labelled gauge value from a GaugeVec.
func testGaugeVec(t *testing.T, gv *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := gv.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatalf("gauge vec write: %v", err)
	}
	return m.GetGauge().GetValue()
}

type captureWriter struct {
	mu   sync.Mutex
	rows []map[string]any
}

func (w *captureWriter) Enqueue(table string, row map[string]any) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rows = append(w.rows, row)
	return true
}

func (w *captureWriter) captured() []map[string]any {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]map[string]any, len(w.rows))
	copy(out, w.rows)
	return out
}

func TestSnapshotWriter_CoalescesRapidChanges(t *testing.T) {
	// Build an instrument with one bid and one ask so ComputeLevels has output.
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 1, 0, time.Now(), 101, 3)

	cap := &captureWriter{}
	metrics := NewMetrics("test", "test")
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.ch = cap

	const rapid = 10
	for i := 0; i < rapid; i++ {
		w.MarkDirty(100)
	}

	w.flushDue()

	rows := cap.captured()
	// One flush for a 2-level book (1 bid + 1 ask) regardless of how many MarkDirty calls came in.
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (1 bid + 1 ask) after coalescing %d marks, got %d", rapid, len(rows))
	}
}

func TestSnapshotWriter_DirtyEntryCoalesces(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	metrics := NewMetrics("test", "test")
	w := NewSnapshotWriter(nil, 5, 100, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })

	// Mark dirty 5 times in rapid succession; only the first should create an entry.
	for i := 0; i < 5; i++ {
		w.MarkDirty(100)
	}

	w.mu.Lock()
	count := len(w.dirty)
	w.mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 dirty entry, got %d", count)
	}
}

func TestSnapshotWriter_RunFlushesAndClears(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)

	metrics := NewMetrics("test", "test")
	w := NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.tickInterval = 10 * time.Millisecond

	w.MarkDirty(100)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	w.mu.Lock()
	count := len(w.dirty)
	w.mu.Unlock()
	if count != 0 {
		t.Errorf("expected dirty cleared after flush, got %d", count)
	}
}

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

	w.Reset(ctx) // must block until the writer goroutine handled it

	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.dirty) != 0 {
		t.Errorf("dirty not cleared after Reset: %d", len(w.dirty))
	}
	if w.generation != gen0+1 {
		t.Errorf("generation not bumped: got %d want %d", w.generation, gen0+1)
	}
}

// Exercises the flushDue generation re-check directly: a batch is extracted,
// then a Reset (generation bump) lands mid-batch; every remaining write must
// be abandoned. This is the deterministic guard the design relies on for the
// "reset vs in-flight flush" hazard.
func TestSnapshotWriter_FlushAbortsRemainderOnGenerationBump(t *testing.T) {
	metrics := stubMetrics()
	insts := map[uint32]*Instrument{}
	for id := uint32(1); id <= 3; id++ {
		in := NewInstrument(id, "X", -2, -8)
		in.Status = StatusReady
		insts[id] = in
	}
	var w *SnapshotWriter
	bumped := false
	w = NewSnapshotWriter(nil, 5, 100, metrics, 0, func(id uint32, fn func(*Instrument)) {
		// Simulate a Reset landing after this flush batch was extracted:
		// bump generation while resolving the first instrument of the batch.
		if !bumped {
			bumped = true
			w.mu.Lock()
			w.generation++
			w.mu.Unlock()
		}
		fn(insts[id])
	})
	w.MarkDirty(1)
	w.MarkDirty(2)
	w.MarkDirty(3)
	w.flushDue() // synchronous; not via the Run goroutine

	var mm dto.Metric
	if err := metrics.SnapshotWritesTotal.Write(&mm); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	// The re-check runs at the top of each due entry. The first entry's
	// re-check passes (bump happens during its withInstrument), so it writes;
	// every subsequent entry sees the bumped generation and aborts. Exactly 1.
	if got := mm.GetCounter().GetValue(); got != 1 {
		t.Fatalf("expected exactly 1 write before generation re-check aborted the batch, got %v", got)
	}
}

// TestSnapshotWriter_GapInstrumentEmitsStaleRows verifies that an instrument
// in StatusGap with a non-empty book emits level rows with stale=1.
func TestSnapshotWriter_GapInstrumentEmitsStaleRows(t *testing.T) {
	inst := NewInstrument(42, "ETH-USDT", 0, 0)
	inst.Status = StatusGap
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5) // one bid

	metrics := stubMetrics()
	cap := &captureWriter{}
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.ch = cap

	w.MarkDirty(42)
	w.flushDue()

	rows := cap.captured()
	if len(rows) == 0 {
		t.Fatal("expected rows for StatusGap instrument with non-empty book, got none")
	}
	for _, row := range rows {
		staleVal, ok := row["stale"]
		if !ok {
			t.Fatalf("row missing stale field: %v", row)
		}
		if staleVal != uint8(1) {
			t.Errorf("expected stale=1 for Gap instrument, got %v", staleVal)
		}
	}
}

// TestSnapshotWriter_ReadyInstrumentEmitsNonStaleRows verifies that an instrument
// in StatusReady emits level rows with stale=0.
func TestSnapshotWriter_ReadyInstrumentEmitsNonStaleRows(t *testing.T) {
	inst := NewInstrument(43, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5) // one bid

	metrics := stubMetrics()
	cap := &captureWriter{}
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.ch = cap

	w.MarkDirty(43)
	w.flushDue()

	rows := cap.captured()
	if len(rows) == 0 {
		t.Fatal("expected rows for StatusReady instrument, got none")
	}
	for _, row := range rows {
		staleVal, ok := row["stale"]
		if !ok {
			t.Fatalf("row missing stale field: %v", row)
		}
		if staleVal != uint8(0) {
			t.Errorf("expected stale=0 for Ready instrument, got %v", staleVal)
		}
	}
}

// TestSnapshotWriter_AwaitingSnapshotEmitsNothing verifies that an instrument
// in StatusAwaitingSnapshot emits no rows even when the book is non-empty,
// proving the STATUS guard (not an empty book) is what suppresses output.
func TestSnapshotWriter_AwaitingSnapshotEmitsNothing(t *testing.T) {
	inst := NewInstrument(44, "SOL-USDT", 0, 0)
	// StatusAwaitingSnapshot is the default.
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5) // non-empty book

	metrics := stubMetrics()
	cap := &captureWriter{}
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.ch = cap

	w.MarkDirty(44)
	w.flushDue()

	if rows := cap.captured(); len(rows) != 0 {
		t.Fatalf("expected no rows for StatusAwaitingSnapshot instrument, got %d", len(rows))
	}
}

// TestSnapshotWriter_BookGaugesPopulatedOnFlush verifies that BookOrders,
// BookTopPrice, BookTopQty, and BookSpreadBps are set after a flush of a
// StatusReady instrument.
func TestSnapshotWriter_BookGaugesPopulatedOnFlush(t *testing.T) {
	// PriceExponent=-2 → scale 0.01; QtyExponent=-8 → scale 1e-8.
	// Two bids: price_raw 10200 (→ 102.00), qty_raw 300 (→ 3e-6)
	//           price_raw 10100 (→ 101.00), qty_raw 200 (→ 2e-6)
	// Two asks: price_raw 10300 (→ 103.00), qty_raw 150 (→ 1.5e-6)
	//           price_raw 10400 (→ 104.00), qty_raw 100 (→ 1e-6)
	// Best bid = 102.00, best ask = 103.00
	// Mid = (102 + 103) / 2 = 102.5
	// Spread bps = (103 - 102) / 102.5 * 10000 ≈ 97.56...
	inst := NewInstrument(55, "ETH-USDT", -2, -8)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 10200, 300) // best bid
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 10100, 200) // second bid
	inst.ApplyOrderAdd(3, 1, 0, time.Now(), 10300, 150) // best ask
	inst.ApplyOrderAdd(4, 1, 0, time.Now(), 10400, 100) // second ask

	metrics := stubMetrics()
	cap := &captureWriter{}
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })
	w.ch = cap

	w.MarkDirty(55)
	w.flushDue()

	// BookOrders: 2 bids, 2 asks.
	if got := testGaugeVec(t, metrics.BookOrders, "ETH-USDT", "bid"); got != 2 {
		t.Errorf("BookOrders bid = %v, want 2", got)
	}
	if got := testGaugeVec(t, metrics.BookOrders, "ETH-USDT", "ask"); got != 2 {
		t.Errorf("BookOrders ask = %v, want 2", got)
	}

	// BookTopPrice: best bid = 102.00, best ask = 103.00.
	const priceScale = 0.01
	if got := testGaugeVec(t, metrics.BookTopPrice, "ETH-USDT", "bid"); got != 10200*priceScale {
		t.Errorf("BookTopPrice bid = %v, want %v", got, 10200*priceScale)
	}
	if got := testGaugeVec(t, metrics.BookTopPrice, "ETH-USDT", "ask"); got != 10300*priceScale {
		t.Errorf("BookTopPrice ask = %v, want %v", got, 10300*priceScale)
	}

	// BookTopQty: best-bid qty_raw=300 * 1e-8 = 3e-6, best-ask qty_raw=150 * 1e-8 = 1.5e-6.
	const qtyScale = 1e-8
	const wantBidQty = 300 * qtyScale
	const wantAskQty = 150 * qtyScale
	if got := testGaugeVec(t, metrics.BookTopQty, "ETH-USDT", "bid"); got != wantBidQty {
		t.Errorf("BookTopQty bid = %v, want %v", got, wantBidQty)
	}
	if got := testGaugeVec(t, metrics.BookTopQty, "ETH-USDT", "ask"); got != wantAskQty {
		t.Errorf("BookTopQty ask = %v, want %v", got, wantAskQty)
	}

	// BookSpreadBps: (103 - 102) / 102.5 * 10000.
	bestBid := 10200 * priceScale
	bestAsk := 10300 * priceScale
	wantSpread := (bestAsk - bestBid) / ((bestBid + bestAsk) / 2) * 10000
	if got := testGaugeVec(t, metrics.BookSpreadBps, "ETH-USDT"); got != wantSpread {
		t.Errorf("BookSpreadBps = %v, want %v", got, wantSpread)
	}
}

// TestSnapshotWriter_BookGaugesSkipAwaitingSnapshot verifies that no book gauges
// are set for a StatusAwaitingSnapshot instrument.
func TestSnapshotWriter_BookGaugesSkipAwaitingSnapshot(t *testing.T) {
	inst := NewInstrument(56, "SOL-USDT", 0, 0)
	// StatusAwaitingSnapshot by default — do NOT set Status.

	metrics := stubMetrics()
	w := NewSnapshotWriter(nil, 5, 0, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })

	w.MarkDirty(56)
	w.flushDue()

	// None of the book gauges should have been touched for this symbol.
	// The GaugeVec returns 0 for unseen label combinations; we just confirm
	// no panic and the value is 0 (no series created).
	if got := testGaugeVec(t, metrics.BookOrders, "SOL-USDT", "bid"); got != 0 {
		t.Errorf("BookOrders bid unexpectedly set for AwaitingSnapshot: %v", got)
	}
}

// Closes the shutdown-during-reset hazard: if SnapshotWriter.Run has already
// returned via ctx.Done, a subsequent Reset must not hang — its own ctx
// must let it escape.
//
// We do NOT have a separate test for Run's ctx.Done drain branch (defense-in-
// depth in snapshot_writer.go): it covers only the cross-ctx-hierarchy case
// (Reset's ctx broader than Run's), which production never produces, so any
// test exercising it would also exercise the redundant-with-this-test ctx
// escape and the drain branch only nondeterministically due to select
// randomness. The drain branch is documented in the implementation as belt-
// and-suspenders for that case.
func TestSnapshotWriter_ResetReturnsAfterRunExits(t *testing.T) {
	metrics := stubMetrics()
	inst := NewInstrument(1, "X", -2, -8)
	inst.Status = StatusReady
	w := NewSnapshotWriter(nil, 5, 100, metrics, 0, func(id uint32, fn func(*Instrument)) { fn(inst) })

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { w.Run(ctx); close(runDone) }()

	cancel()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("Run did not return on ctx.Done")
	}

	// Reset is called AFTER Run has exited. Pre-fix (unbuffered resetCh +
	// non-ctx-aware Reset), this would hang forever; with the fix Reset's own
	// ctx lets it escape.
	rctx, rcancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer rcancel()
	resetDone := make(chan struct{})
	go func() {
		w.Reset(rctx)
		close(resetDone)
	}()
	select {
	case <-resetDone:
	case <-time.After(time.Second):
		t.Fatal("Reset hung after Run exited; ctx cancel should have unblocked it")
	}
}
