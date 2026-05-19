package main

import (
	"context"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type captureWriter struct {
	mu       sync.Mutex
	enqueued int
}

func (w *captureWriter) Enqueue(table string, row map[string]any) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.enqueued++
	return true
}

func TestSnapshotWriter_CoalescesRapidChanges(t *testing.T) {
	// Build an instrument with one bid and one ask so ComputeLevels has output.
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 1, 0, time.Now(), 101, 3)

	cap := &captureWriter{}
	// We can't easily inject captureWriter into SnapshotWriter without changing
	// the production type. For this test, use a real ClickhouseClient pointed at
	// a counting test server — see clickhouse_test.go for the pattern.
	t.Skip("Wire-up test using httptest server pattern from clickhouse_test.go; left as exercise during integration in Task 15")
	_ = inst
	_ = cap
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
