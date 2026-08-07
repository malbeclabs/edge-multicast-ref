package clickhouse

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// capture is a stub ClickHouse server recording every insert body it receives.
type capture struct {
	mu     sync.Mutex
	rows   []map[string]any
	status int
}

func (c *capture) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.status >= 400 {
			w.WriteHeader(c.status)
			_, _ = w.Write([]byte("boom"))
			return
		}
		sc := bufio.NewScanner(r.Body)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err == nil {
				c.rows = append(c.rows, m)
			}
		}
	}
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.rows)
}

// recordingObserver captures observer callbacks for assertions.
type recordingObserver struct {
	mu      sync.Mutex
	written map[string]int
	dropped map[string]int
	errs    map[string]int
}

func newRecordingObserver() *recordingObserver {
	return &recordingObserver{
		written: map[string]int{}, dropped: map[string]int{}, errs: map[string]int{},
	}
}

func (o *recordingObserver) RowsWritten(table string, n int) {
	o.mu.Lock()
	o.written[table] += n
	o.mu.Unlock()
}

func (o *recordingObserver) RowsDropped(table, reason string, n int) {
	o.mu.Lock()
	o.dropped[table+"/"+reason] += n
	o.mu.Unlock()
}

func (o *recordingObserver) WriteError(table, reason string) {
	o.mu.Lock()
	o.errs[table+"/"+reason]++
	o.mu.Unlock()
}

func (o *recordingObserver) BatchDuration(table string, d time.Duration) {}
func (o *recordingObserver) BufferedRows(table string, n int)            {}

func (o *recordingObserver) get(m map[string]int, k string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return m[k]
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", limit)
}

// A full batch flushes as soon as BatchSize is reached, without waiting for the
// interval.
func TestClient_FlushesOnBatchSize(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(cap.handler())
	defer srv.Close()

	obs := newRecordingObserver()
	c, err := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 3, BatchInterval: time.Hour, BufferSize: 10},
	}, obs)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 3; i++ {
		if !c.Enqueue("events", map[string]any{"n": i}) {
			t.Fatalf("enqueue %d rejected", i)
		}
	}

	waitFor(t, 2*time.Second, func() bool { return cap.count() == 3 })
	waitFor(t, 2*time.Second, func() bool { return obs.get(obs.written, "events") == 3 })
}

// A partial batch flushes on the interval tick.
func TestClient_FlushesOnInterval(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(cap.handler())
	defer srv.Close()

	c, err := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 1000, BatchInterval: 20 * time.Millisecond, BufferSize: 10},
	}, nil) // nil Observer must be valid
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.Enqueue("events", map[string]any{"n": 1})
	waitFor(t, 2*time.Second, func() bool { return cap.count() == 1 })
}

// A full buffer drops the row and counts it rather than blocking. Blocking would
// back-pressure into the shard goroutine and from there into the socket reader.
func TestClient_DropsWhenBufferFull(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(cap.handler())
	defer srv.Close()

	obs := newRecordingObserver()
	// Never Run(), so nothing drains the channel.
	c, err := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 100, BatchInterval: time.Hour, BufferSize: 2},
	}, obs)
	if err != nil {
		t.Fatal(err)
	}

	if !c.Enqueue("events", map[string]any{"n": 1}) {
		t.Fatal("first enqueue should succeed")
	}
	if !c.Enqueue("events", map[string]any{"n": 2}) {
		t.Fatal("second enqueue should succeed")
	}
	if c.Enqueue("events", map[string]any{"n": 3}) {
		t.Error("third enqueue must be rejected: buffer is full")
	}
	if got := obs.get(obs.dropped, "events/buffer_full"); got != 1 {
		t.Errorf("buffer_full drops: got %d want 1", got)
	}
}

// An unknown table is rejected rather than silently accepted and lost.
func TestClient_RejectsUnknownTable(t *testing.T) {
	srv := httptest.NewServer((&capture{}).handler())
	defer srv.Close()
	c, _ := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 1, BatchInterval: time.Hour, BufferSize: 1},
	}, nil)
	if c.Enqueue("nope", map[string]any{}) {
		t.Error("unknown table must be rejected")
	}
}

// A server error drops the batch, counts the rows, and classifies the failure.
func TestClient_CountsWriteErrors(t *testing.T) {
	cap := &capture{status: 500}
	srv := httptest.NewServer(cap.handler())
	defer srv.Close()

	obs := newRecordingObserver()
	c, _ := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 2, BatchInterval: time.Hour, BufferSize: 10},
	}, obs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	c.Enqueue("events", map[string]any{"n": 1})
	c.Enqueue("events", map[string]any{"n": 2})

	waitFor(t, 2*time.Second, func() bool { return obs.get(obs.errs, "events/http_5xx") == 1 })
	if got := obs.get(obs.dropped, "events/write_failed"); got != 2 {
		t.Errorf("write_failed drops: got %d want 2", got)
	}
}

// Cancelling ctx drains and flushes what is buffered instead of discarding it.
func TestClient_FlushesOnShutdown(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(cap.handler())
	defer srv.Close()

	c, _ := New(srv.URL, "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 1000, BatchInterval: time.Hour, BufferSize: 10},
	}, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()

	c.Enqueue("events", map[string]any{"n": 1})
	c.Enqueue("events", map[string]any{"n": 2})
	time.Sleep(50 * time.Millisecond) // let the batcher pick them up
	cancel()
	<-done

	if cap.count() != 2 {
		t.Errorf("shutdown must flush buffered rows: got %d want 2", cap.count())
	}
}

// An empty URL disables persistence: New returns a nil Client whose methods are
// safe no-ops, so the bot runs exactly as it does without ClickHouse.
func TestClient_EmptyURLDisables(t *testing.T) {
	c, err := New("", "testdb", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatal("empty URL must return a nil Client")
	}
	if c.Enqueue("events", map[string]any{}) {
		t.Error("nil Client Enqueue must return false, not panic")
	}
	c.Run(context.Background()) // must not panic
}

// Batcher config comes from user-supplied flags, and New validated only the URL.
// A non-positive BatchInterval panicked time.NewTicker inside a batcher
// goroutine — unrecovered, so --clickhouse-batch-interval=0 killed the process
// after startup had already reported success. A non-positive BufferSize is
// quieter but no better: an unbuffered channel makes Enqueue's deliberately
// non-blocking send drop very nearly every row.
func TestNew_RejectsNonPositiveBatcherConfig(t *testing.T) {
	valid := BatcherConfig{Table: "events", BatchSize: 10, BatchInterval: time.Second, BufferSize: 10}

	mutate := func(fn func(*BatcherConfig)) BatcherConfig {
		c := valid
		fn(&c)
		return c
	}

	cases := []struct {
		name    string
		cfg     BatcherConfig
		wantErr bool
	}{
		{"valid", valid, false},
		{"zero batch size", mutate(func(c *BatcherConfig) { c.BatchSize = 0 }), true},
		{"negative batch size", mutate(func(c *BatcherConfig) { c.BatchSize = -1 }), true},
		{"zero batch interval", mutate(func(c *BatcherConfig) { c.BatchInterval = 0 }), true},
		{"negative batch interval", mutate(func(c *BatcherConfig) { c.BatchInterval = -time.Second }), true},
		{"zero buffer size", mutate(func(c *BatcherConfig) { c.BufferSize = 0 }), true},
		{"negative buffer size", mutate(func(c *BatcherConfig) { c.BufferSize = -5 }), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New("http://localhost:8123", "testdb", []BatcherConfig{tc.cfg}, nil)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %+v", tc.cfg)
				}
				if c != nil {
					t.Error("a rejected config must not yield a usable client")
				}
				if !strings.Contains(err.Error(), "events") {
					t.Errorf("the error should name the table: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("valid config rejected: %v", err)
			}
			// The valid case must actually be runnable: this is the call that
			// panicked on a non-positive interval.
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { c.Run(ctx); close(done) }()
			cancel()
			<-done
		})
	}
}

// One bad table among several must reject the whole client, not silently
// configure the rest.
func TestNew_RejectsWhenAnyTableIsInvalid(t *testing.T) {
	_, err := New("http://localhost:8123", "testdb", []BatcherConfig{
		{Table: "events", BatchSize: 10, BatchInterval: time.Second, BufferSize: 10},
		{Table: "level_snapshots", BatchSize: 10, BatchInterval: 0, BufferSize: 10},
	}, nil)
	if err == nil {
		t.Fatal("a single invalid table must reject the client")
	}
	if !strings.Contains(err.Error(), "level_snapshots") {
		t.Errorf("the error should name the offending table: %v", err)
	}
}

func TestChTime_FormatsForJSONEachRow(t *testing.T) {
	ts := time.Date(2026, 8, 7, 12, 34, 56, 123456789, time.UTC)
	if got, want := ChTime(ts), "2026-08-07 12:34:56.123456789"; got != want {
		t.Errorf("ChTime: got %q want %q", got, want)
	}
	if got := ChTime(time.Time{}); got != "" {
		t.Errorf("zero time must format empty, got %q", got)
	}
}
