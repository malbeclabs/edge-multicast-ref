package main

import (
	"testing"
	"time"
)

// A typed nil pointer stored in an interface is NOT == nil. Handing *ch*
// straight to the writers therefore made every `w.ch == nil` fast path dead —
// including under the default --clickhouse-url="", where the bot would build
// and discard a row map per record (and per level, for snapshots) instead of
// returning immediately.
func TestEnqueuerFor_NilClientYieldsNilInterface(t *testing.T) {
	if enq := enqueuerFor(nil); enq != nil {
		t.Error("a nil client must yield a nil enqueuer, or every writer's nil check is dead code")
	}

	// The trap itself, spelled out, so the guard above cannot be "simplified" away.
	var ch *ClickhouseClient
	var direct enqueuer = ch
	if direct == nil {
		t.Fatal("fixture: a typed nil in an interface must not compare equal to nil")
	}

	// A real client is passed through unchanged.
	real, err := NewClickhouseClient("http://127.0.0.1:1", "db", []BatcherConfig{
		{Table: "events", BatchSize: 1, BatchInterval: time.Second, BufferSize: 1},
	}, NewMetrics("test", "test"))
	if err != nil {
		t.Fatal(err)
	}
	if enqueuerFor(real) == nil {
		t.Error("a real client must reach the writers")
	}
}

// The writers must be genuine no-ops once the interface really is nil: both
// call sites in main.go (EventsWriter and SnapshotWriter) go through
// enqueuerFor, so proving it here for EventsWriter also proves the
// SnapshotWriter call site, which is constructed identically.
func TestEnqueuerFor_NilClientWritersDoNothing(t *testing.T) {
	w := NewEventsWriter(enqueuerFor(nil))
	if w.ch != nil {
		t.Fatal("NewEventsWriter(enqueuerFor(nil)) must store a genuinely nil enqueuer")
	}

	// If the guard were dead (the pre-fix bug), this would build a full row map
	// and hand it to a nil *ClickhouseClient's Enqueue. With the guard alive it
	// returns immediately at the top of Write, so nothing is ever built or
	// enqueued.
	w.Write(ChannelEvent{
		InstrumentID: 1,
		Record: Record{
			Type:         "order_add",
			InstrumentID: 1,
			Fields: map[string]any{
				"source_id": float64(1),
				"order_id":  float64(1),
			},
		},
	}, 0, "SYM", 0, 0)

	sw := NewSnapshotWriter(enqueuerFor(nil), 5, 0, NewMetrics("test", "test"), 0, func(uint32, func(*Instrument)) {})
	if sw.ch != nil {
		t.Fatal("NewSnapshotWriter(enqueuerFor(nil), ...) must store a genuinely nil enqueuer")
	}
}
