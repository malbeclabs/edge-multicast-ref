package main

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// A typed nil pointer stored in an interface is NOT == nil. Handing the client
// value straight to the writers therefore made every `ch == nil` check dead —
// including under the default --clickhouse-url="", where the bot built and
// discarded a row map per record and per level, and counted snapshot writes that
// never happened.
func TestEnqueuerFor_NilClientYieldsNilInterface(t *testing.T) {
	if enq := enqueuerFor(nil); enq != nil {
		t.Error("a nil client must yield a nil enqueuer, or every writer's nil check is dead code")
	}

	// The trap itself, spelled out, so the guard above cannot be "simplified" away.
	var ch *clickhouse.Client
	var direct enqueuer = ch
	if direct == nil {
		t.Fatal("fixture: a typed nil in an interface must not compare equal to nil")
	}

	// A real client is passed through unchanged.
	real, err := clickhouse.New("http://127.0.0.1:1", "db", []clickhouse.BatcherConfig{
		{Table: "events", BatchSize: 1, BatchInterval: time.Second, BufferSize: 1},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if enqueuerFor(real) == nil {
		t.Error("a real client must reach the writers")
	}
}

// The writers must be genuine no-ops once the interface really is nil.
func TestEnqueuerFor_NilClientWritersDoNothing(t *testing.T) {
	NewEventsWriter(enqueuerFor(nil)).Write(
		ChannelEvent{Kind: KindAppliedDelta, InstrumentID: 11, Record: levelUpdateRec(11, 900, 6, "bid", 1000, 5)},
		0, "SYM", 0, 0)
	NewEventsWriter(enqueuerFor(nil)).WriteWireLevel(
		snapLevelRec(11, 4, "bid", 1000, 5), 0, SnapshotGroup{SnapshotID: 4}, "SYM", 0, 0)
	// Reaching here without a panic and without a client is the assertion; the
	// paired counter assertion lives in TestSnapshotWriter_NilClientCountsNoWrites.
}

// captureServer counts the JSONEachRow lines a stub ClickHouse receives.
type captureServer struct {
	mu sync.Mutex
	n  int
}

func (c *captureServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sc := bufio.NewScanner(r.Body)
	c.mu.Lock()
	defer c.mu.Unlock()
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			c.n++
		}
	}
}

func (c *captureServer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// Shutdown must join the producers BEFORE stopping the batchers.
//
// A batcher drains only what is buffered at the instant its context is
// cancelled, then returns — typically in microseconds. Running it on the SAME
// context as the shard and snapshot-writer goroutines therefore stopped the
// consumer while the producers were still running: every row enqueued after that
// point sat in the channel forever, never written and never counted dropped,
// while joining the already-finished batchers made shutdown look clean.
//
// The producer here models a shard still working through its inbox after
// cancellation.
func TestDrainOnShutdown_WritesRowsProducedAfterCancel(t *testing.T) {
	cap := &captureServer{}
	srv := httptest.NewServer(cap)
	defer srv.Close()

	ch, err := clickhouse.New(srv.URL, "testdb", []clickhouse.BatcherConfig{
		{Table: "events", BatchSize: 1000, BatchInterval: time.Hour, BufferSize: 100},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	chCtx, chCancel := context.WithCancel(context.Background())
	chDone := make(chan struct{})
	go func() {
		ch.Run(chCtx)
		close(chDone)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // still draining its inbox
		if !ch.Enqueue("events", map[string]any{"tail": 1}) {
			t.Error("the tail row should have been accepted")
		}
	}()

	cancel()
	drainOnShutdown(&producers, chCancel, chDone, 5*time.Second)

	if got := cap.count(); got != 1 {
		t.Errorf("a row produced after cancellation must still be written: got %d rows want 1", got)
	}
}

// A wedged producer must not hang shutdown: both stages are bounded.
func TestDrainOnShutdown_IsBounded(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)

	var producers sync.WaitGroup
	producers.Add(1)
	go func() {
		defer producers.Done()
		<-stuck
	}()

	chDone := make(chan struct{})
	close(chDone)

	done := make(chan struct{})
	go func() {
		drainOnShutdown(&producers, func() {}, chDone, 50*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("drainOnShutdown must be bounded, not wait forever on a wedged producer")
	}
}
