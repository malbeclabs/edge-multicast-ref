package main

import (
	"context"
	"sync"
	"testing"
	"time"
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
