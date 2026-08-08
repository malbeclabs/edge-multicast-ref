# Market-by-Price bot persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the market-by-price bot's book state and event stream to ClickHouse, delivering PR 4 of the parent spec's five-PR sequence.

**Architecture:** A new shared batching client at `go/internal/clickhouse` reports through an `Observer` interface so the module stays free of Prometheus. In the bot, a stateless `EventsWriter` maps each `ChannelEvent` to rows, and a per-shard `SnapshotWriter` coalesces book changes into `level_snapshots`. Both feed the shared client, which batches over HTTP `JSONEachRow`.

**Tech Stack:** Go 1.25.0, `prometheus/client_golang`, ClickHouse over HTTP, `net/http/httptest` for client tests.

**Design spec:** [`docs/superpowers/specs/2026-08-07-marketbyprice-bot-persistence-design.md`](../specs/2026-08-07-marketbyprice-bot-persistence-design.md)
**Parent spec:** [`docs/superpowers/specs/2026-08-02-marketbyprice-design.md`](../specs/2026-08-02-marketbyprice-design.md), Component 3

## State of play — read this first if you are resuming

Nothing in this plan is implemented yet. The book engine shipped in PR 3 (#34,
squashed to `b2a5b48` on `main`) and `Shard.handle` currently ends in `_ = evs`
with a comment pointing here.

Work happens on branch `feat/marketbyprice-bot-persistence`, already created off
`main`, with the design spec committed to it as `6bb1c09`.

## Global Constraints

- Module path `github.com/malbeclabs/edge-multicast-ref/go/marketbyprice-bot`, Go directive `go 1.25.0`. The new shared package lives in module `github.com/malbeclabs/edge-multicast-ref/go/internal`, same Go directive.
- Package `main` in the bot, no subpackages. The shared package is `package clickhouse`.
- Prometheus namespace exactly `dz_mbp_bot`. The shared package registers nothing and knows no namespace.
- Prices and quantities stay **raw** (`int64` / `uint64`) in book state and are scaled by `PriceExponent` / `QtyExponent` only at the persistence and read-out boundary.
- The parser OMITS `order_count` and `level_index` when the wire carried the `0xFFFF` sentinel. An absent key means "not supplied" and must become SQL `NULL`, never `0` — zero is a real count.
- `--symbol` gates persistence and read-out ONLY. The book engine always processes every instrument, or sequencing, gap detection and the delta buffer are wrong.
- Every task ends gofmt-clean: `gofmt -l ./marketbyprice-bot/ ./internal/` from `go/` must print nothing.
- Commit messages: `component: short description`, lowercase, imperative, no trailing period, no `Co-Authored-By` line, no "Generated with" footer.
- Write "DoubleZero" in prose, never "DZ". Binary names and env vars keep their `dz-` / `DZ_` prefixes.
- A write failure is counted and dropped, never fatal, and never resets a channel. Persistence observes the feed; it is not part of it.

## Verification commands

From `go/`:

```bash
gofmt -l ./marketbyprice-bot/ ./internal/       # must print nothing
go vet ./marketbyprice-bot/... ./internal/...
```

From `go/marketbyprice-bot/`:

```bash
go test ./...                                    # NOT ./... from go/ — see below
go test -race -count=1 ./...
go build -o /tmp/dz-marketbyprice-bot .
GOWORK=off go build -o /tmp/dz-mbp-d .
GOWORK=off GOOS=linux GOARCH=amd64 go build -o /tmp/dz-mbp-l .
```

From `go/internal/`: `go test ./...`

`./...` does NOT work from `go/` in this workspace — always name the module
directory. A bare `go build ./marketbyprice-bot/` also fails because the output
name collides with the directory; use `-o`. Neither is a defect.

## File map

- `go/internal/clickhouse/client.go` — `Client`, `BatcherConfig`, `Observer`, `ChTime`. Task 1.
- `go/internal/clickhouse/client_test.go` — Task 1.
- `demo/clickhouse/init/03_schema_mbp.sql` — five tables. Task 2.
- `go/marketbyprice-bot/clickhouse.go` — table configs and the `Observer` implementation over `*Metrics`. Task 3.
- `go/marketbyprice-bot/metrics.go` — restored metrics. Tasks 3 and 6.
- `go/marketbyprice-bot/go.mod`, `go.sum`, `Dockerfile` — Task 3.
- `go/marketbyprice-bot/events_writer.go` + test — Tasks 4 and 5.
- `go/marketbyprice-bot/instrument.go` — `LastBegin` field. Task 5.
- `go/marketbyprice-bot/dispatch.go` — record the last begin, wire events. Tasks 5 and 7.
- `go/marketbyprice-bot/snapshot_writer.go` + test — Task 6.
- `go/marketbyprice-bot/shard.go`, `main.go` — wiring. Task 7.
- `go/marketbyprice-bot/levels.go`, `main.go`, `README.md` — symbol gating. Task 8.

---

## Task 1: Shared ClickHouse client

**Files:**
- Create: `go/internal/clickhouse/client.go`
- Create: `go/internal/clickhouse/client_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `clickhouse.Client` with `New(rawURL, database string, configs []BatcherConfig, obs Observer) (*Client, error)`, `(*Client).Run(ctx context.Context)`, `(*Client).Enqueue(table string, row map[string]any) bool`; `clickhouse.BatcherConfig{Table string; BatchSize int; BatchInterval time.Duration; BufferSize int}`; `clickhouse.Observer` interface; `clickhouse.ChTime(t time.Time) string`.

- [ ] **Step 1: Settle the dependency question before writing code**

The design spec flags this as a risk to resolve first. `go/internal` requires
Bubble Tea and Lip Gloss for the receivers' TUI. Confirm those do not reach the
bot's build.

```bash
cd go/internal && mkdir -p clickhouse
printf 'package clickhouse\n\nfunc Ping() string { return "ok" }\n' > clickhouse/client.go
cd ../marketbyprice-bot
go mod edit -require=github.com/malbeclabs/edge-multicast-ref/go/internal@v0.0.0
go mod edit -replace=github.com/malbeclabs/edge-multicast-ref/go/internal=../internal
printf 'package main\n\nimport "github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"\n\nvar _ = clickhouse.Ping\n' > zz_dep_probe.go
go mod tidy && go build -o /tmp/probe . && echo BUILD_OK
grep -c 'bubbletea\|lipgloss' go.sum || echo "0 TUI entries"
rm zz_dep_probe.go
```

Expected: `BUILD_OK`, and `0 TUI entries` (module graph pruning keeps them out).
If TUI entries DO appear, stop and report — the design spec's stated fallback is
to give the ClickHouse client its own module rather than accept unrelated
dependencies in a market data binary. Do not silently proceed.

- [ ] **Step 2: Write the failing tests**

Create `go/internal/clickhouse/client_test.go`:

```go
package clickhouse

import (
	"context"
	"bufio"
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

func TestChTime_FormatsForJSONEachRow(t *testing.T) {
	ts := time.Date(2026, 8, 7, 12, 34, 56, 123456789, time.UTC)
	if got, want := ChTime(ts), "2026-08-07 12:34:56.123456789"; got != want {
		t.Errorf("ChTime: got %q want %q", got, want)
	}
	if got := ChTime(time.Time{}); got != "" {
		t.Errorf("zero time must format empty, got %q", got)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd go/internal && go test ./clickhouse/...`
Expected: FAIL to build — `undefined: New`, `undefined: BatcherConfig`, `undefined: ChTime`.

- [ ] **Step 4: Write the implementation**

Replace `go/internal/clickhouse/client.go` (the probe stub from Step 1) with:

```go
// Package clickhouse batches rows into a ClickHouse server over HTTP using
// JSONEachRow inserts, one goroutine and buffer per table.
//
// It reports through an Observer rather than owning metrics, so the module stays
// free of a Prometheus dependency and each consumer keeps its own metric names
// and namespace.
package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Observer receives batching outcomes. Every method must be safe for concurrent
// use. A nil Observer is valid and means "do not report".
type Observer interface {
	RowsWritten(table string, n int)
	RowsDropped(table, reason string, n int)
	WriteError(table, reason string)
	BatchDuration(table string, d time.Duration)
	BufferedRows(table string, n int)
}

// BatcherConfig controls one table's batcher.
type BatcherConfig struct {
	Table         string
	BatchSize     int           // flush once this many rows have accumulated
	BatchInterval time.Duration // maximum time between flushes
	BufferSize    int           // channel capacity; rows are dropped when full
}

// Client owns one Batcher per configured table.
type Client struct {
	url      string
	database string
	hc       *http.Client
	obs      Observer
	batchers map[string]*batcher
}

// New returns a configured Client, or a nil Client when rawURL is empty, which
// disables persistence. Every method is nil-safe, so callers do not branch.
func New(rawURL, database string, configs []BatcherConfig, obs Observer) (*Client, error) {
	if rawURL == "" {
		return nil, nil
	}
	if _, err := url.Parse(rawURL); err != nil {
		return nil, fmt.Errorf("clickhouse url: %w", err)
	}
	c := &Client{
		url:      strings.TrimRight(rawURL, "/"),
		database: database,
		hc:       &http.Client{Timeout: 30 * time.Second},
		obs:      obs,
		batchers: map[string]*batcher{},
	}
	for _, cfg := range configs {
		c.batchers[cfg.Table] = &batcher{
			client: c,
			cfg:    cfg,
			ch:     make(chan map[string]any, cfg.BufferSize),
		}
	}
	return c, nil
}

// Run starts every batcher and returns once ctx is cancelled and all have
// drained and flushed.
func (c *Client) Run(ctx context.Context) {
	if c == nil {
		return
	}
	var wg sync.WaitGroup
	for _, b := range c.batchers {
		wg.Add(1)
		go func(b *batcher) {
			defer wg.Done()
			b.run(ctx)
		}(b)
	}
	wg.Wait()
}

// Enqueue queues one row. It returns false when the row was dropped because the
// table is unknown or its buffer is full.
//
// It never blocks. Blocking here would back-pressure through the caller into the
// socket read loop, so a slow or dead ClickHouse would stop the bot reading its
// feed — persistence must never do that.
func (c *Client) Enqueue(table string, row map[string]any) bool {
	if c == nil {
		return false
	}
	b, ok := c.batchers[table]
	if !ok {
		return false
	}
	select {
	case b.ch <- row:
		c.report(func(o Observer) { o.BufferedRows(table, len(b.ch)) })
		return true
	default:
		c.report(func(o Observer) { o.RowsDropped(table, "buffer_full", 1) })
		return false
	}
}

func (c *Client) report(fn func(Observer)) {
	if c != nil && c.obs != nil {
		fn(c.obs)
	}
}

type batcher struct {
	client *Client
	cfg    BatcherConfig
	ch     chan map[string]any
}

func (b *batcher) run(ctx context.Context) {
	buf := make([]map[string]any, 0, b.cfg.BatchSize)
	tick := time.NewTicker(b.cfg.BatchInterval)
	defer tick.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		start := time.Now()
		if err := b.send(context.WithoutCancel(ctx), buf); err != nil {
			reason := classifyHTTPErr(err)
			n := len(buf)
			b.client.report(func(o Observer) {
				o.WriteError(b.cfg.Table, reason)
				o.RowsDropped(b.cfg.Table, "write_failed", n)
			})
			log.Printf("clickhouse %s: %v (dropped %d rows)", b.cfg.Table, err, n)
		} else {
			n := len(buf)
			b.client.report(func(o Observer) { o.RowsWritten(b.cfg.Table, n) })
		}
		d := time.Since(start)
		b.client.report(func(o Observer) { o.BatchDuration(b.cfg.Table, d) })
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain what is queued and flush it. Discarding buffered rows on
			// shutdown would silently lose the tail of every run.
			for {
				select {
				case row := <-b.ch:
					buf = append(buf, row)
				default:
					flush()
					return
				}
			}
		case row := <-b.ch:
			buf = append(buf, row)
			b.client.report(func(o Observer) { o.BufferedRows(b.cfg.Table, len(b.ch)) })
			if len(buf) >= b.cfg.BatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func (b *batcher) send(ctx context.Context, rows []map[string]any) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}

	q := url.Values{}
	q.Set("database", b.client.database)
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", b.cfg.Table))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.client.url+"/?"+q.Encode(), &body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.hc.Do(req)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(msg))
	}
	return nil
}

// ChTime formats a time into ClickHouse's DateTime64(9) textual form.
//
// The default date_time_input_format=basic rejects RFC3339 with a Z suffix in
// JSONEachRow, so emit the native form ClickHouse itself echoes from now64().
func ChTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000000")
}

func classifyHTTPErr(err error) string {
	s := err.Error()
	switch {
	case strings.HasPrefix(s, "transport"):
		return "transport"
	case strings.HasPrefix(s, "new request"):
		return "new_request"
	case strings.HasPrefix(s, "encode"):
		return "encode"
	case strings.HasPrefix(s, "http 4"):
		return "http_4xx"
	case strings.HasPrefix(s, "http 5"):
		return "http_5xx"
	default:
		return "other"
	}
}
```

Note `context.WithoutCancel(ctx)` in `flush`. The shutdown path flushes *after*
ctx is already cancelled; passing the cancelled ctx to `send` would fail every
final request and lose exactly the rows the drain exists to save.

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd go/internal && go test -race ./clickhouse/...`
Expected: PASS, all 8 tests.

- [ ] **Step 6: Commit**

```bash
cd go/internal && go mod tidy
cd ../.. && gofmt -l go/internal/
git add go/internal/
git commit -m "internal/clickhouse: add shared batching client"
```

---

## Task 2: ClickHouse schema

**Files:**
- Create: `demo/clickhouse/init/03_schema_mbp.sql`

**Interfaces:**
- Consumes: nothing.
- Produces: database `marketbyprice` with tables `instruments`, `events`, `level_snapshots`, `wire_levels`, `channel_health`. Later tasks enqueue rows whose keys must match these column names exactly.

- [ ] **Step 1: Write the schema**

Create `demo/clickhouse/init/03_schema_mbp.sql`:

```sql
CREATE DATABASE IF NOT EXISTS marketbyprice;

-- Slowly-changing instrument dimension. ReplacingMergeTree keeps the latest row
-- per (channel_id, instrument_id). No TTL: refdata must outlive the event window.
CREATE TABLE IF NOT EXISTS marketbyprice.instruments (
    recv_ts          DateTime64(9),
    channel_id       UInt8,
    instrument_id    UInt32,
    symbol           LowCardinality(String),
    leg1             LowCardinality(String),
    leg2             LowCardinality(String),
    asset_class      LowCardinality(String),
    market_model     LowCardinality(String),
    price_exponent   Int8,
    qty_exponent     Int8,
    tick_size        Float64,
    lot_size         Float64,
    contract_value   UInt64,
    expiry_ts        DateTime64(9),
    settle_type      LowCardinality(String),
    price_bound      LowCardinality(String),
    manifest_seq     UInt16
)
ENGINE = ReplacingMergeTree(recv_ts)
ORDER BY (channel_id, instrument_id);

-- Per-message log: level deltas, clears, trades, liquidations, structural events.
CREATE TABLE IF NOT EXISTS marketbyprice.events (
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    source_ts              Nullable(DateTime64(9)),
    recv_ts_kind           LowCardinality(String) DEFAULT '',
    send_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms      Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL, (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
    channel_id             UInt8,
    mktdata_seq            UInt64,
    reset_count            UInt8,
    kind                   LowCardinality(String),
    instrument_id          UInt32,
    symbol                 LowCardinality(String),
    source_id              UInt16 DEFAULT 0,
    per_instrument_seq     UInt32 DEFAULT 0,

    -- level_update. order_count and level_index are Nullable because the wire
    -- sentinel 0xFFFF means "not supplied" and the parser omits the key; 0 is a
    -- real count and a real rank.
    side                   LowCardinality(String) DEFAULT '',
    price                  Nullable(Float64),
    qty                    Nullable(Float64),
    order_count            Nullable(UInt32),
    level_index            Nullable(UInt16),
    action                 LowCardinality(String) DEFAULT '',
    update_reason          LowCardinality(String) DEFAULT '',
    level_flags            UInt8 DEFAULT 0,

    -- book_clear
    clear_side             LowCardinality(String) DEFAULT '',
    clear_scope            LowCardinality(String) DEFAULT '',
    from_price             Nullable(Float64),
    clear_reason           LowCardinality(String) DEFAULT '',

    -- trade
    trade_id               Nullable(UInt64),
    aggressor_side         LowCardinality(String) DEFAULT '',
    cumulative_volume      Nullable(Float64),
    trade_flags            UInt8 DEFAULT 0,

    -- liquidation
    liquidation_flags      UInt8 DEFAULT 0,
    method                 LowCardinality(String) DEFAULT '',
    mark_price             Nullable(Float64),
    liquidated_user        String DEFAULT '',

    -- batch_boundary
    batch_id               Nullable(UInt32),
    batch_ts               Nullable(DateTime64(9)),

    -- instrument_reset
    reset_reason           LowCardinality(String) DEFAULT '',
    new_anchor_seq         Nullable(UInt64)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, kind)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Coalesced top-N depth, one row per level for direct table and heatmap rendering.
CREATE TABLE IF NOT EXISTS marketbyprice.level_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    last_applied_seq    UInt64,
    side                LowCardinality(String),
    level_idx           UInt16,
    price               Float64,
    qty                 Float64,
    order_count         Nullable(UInt32),
    cumulative_qty      Float64,
    stale               UInt8 DEFAULT 0,
    -- crossed: the book was crossed at the last consistency point. Observability
    -- only; a crossed book is still served.
    crossed             UInt8 DEFAULT 0,
    -- depth_bound: NULL unknown, 0 the publisher claims a complete book, N
    -- bounded at N levels per side. cumulative_qty is exhaustive ONLY when 0.
    depth_bound         Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, side, level_idx)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Raw SnapshotLevel capture for full replay. Group identity is denormalized onto
-- every row from the instrument's last SnapshotBegin, accepted or declined.
CREATE TABLE IF NOT EXISTS marketbyprice.wire_levels (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    snapshot_id         UInt32,
    anchor_seq          UInt64,
    total_levels        UInt32,
    last_instrument_seq UInt32,
    depth_bound         Nullable(UInt32),
    side                LowCardinality(String),
    price               Float64,
    qty                 Float64,
    order_count         Nullable(UInt32),
    level_flags         UInt8 DEFAULT 0
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, snapshot_id, side, price)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Channel health: heartbeats, manifest summaries, end-of-session signals.
CREATE TABLE IF NOT EXISTS marketbyprice.channel_health (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    source_ts           Nullable(DateTime64(9)),
    recv_ts_kind        LowCardinality(String) DEFAULT '',
    send_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms   Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL, (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
    channel_id          UInt8,
    kind                LowCardinality(String),
    manifest_seq        Nullable(UInt16),
    manifest_valid      Nullable(UInt8),
    instrument_count    Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, recv_ts)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

- [ ] **Step 2: Verify the SQL parses**

If Docker is available:

```bash
docker run --rm -i clickhouse/clickhouse-server:latest \
  clickhouse-local --multiquery < demo/clickhouse/init/03_schema_mbp.sql && echo SCHEMA_OK
```

Expected: `SCHEMA_OK`. If Docker is unavailable, skip and note it — Task 7's
manual verification covers the schema end-to-end against a live server.

- [ ] **Step 3: Commit**

```bash
git add demo/clickhouse/init/03_schema_mbp.sql
git commit -m "marketbyprice-bot: add clickhouse schema"
```

---

## Task 3: Bot adapter, metrics, and build wiring

**Files:**
- Create: `go/marketbyprice-bot/clickhouse.go`
- Modify: `go/marketbyprice-bot/metrics.go`
- Modify: `go/marketbyprice-bot/go.mod`, `go/marketbyprice-bot/go.sum`
- Modify: `go/marketbyprice-bot/Dockerfile`
- Test: `go/marketbyprice-bot/clickhouse_test.go`

**Interfaces:**
- Consumes: `clickhouse.New`, `clickhouse.BatcherConfig`, `clickhouse.Observer`, `clickhouse.ChTime` from Task 1.
- Produces: `metricsObserver` implementing `clickhouse.Observer`; `newClickhouseClient(url, db string, batchSize int, batchInterval time.Duration, bufferSize int, m *Metrics) (*clickhouse.Client, error)`; `Metrics` fields `ClickhouseRowsWritten`, `ClickhouseRowsDropped`, `ClickhouseWriteErrors`, `ClickhouseBatchDuration`, `ClickhouseBufferedRows`.

- [ ] **Step 1: Write the failing test**

Create `go/marketbyprice-bot/clickhouse_test.go`:

```go
package main

import (
	"testing"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// The adapter must satisfy the shared package's interface at compile time, not
// through a runtime assertion that could silently stop matching.
var _ clickhouse.Observer = (*metricsObserver)(nil)

func TestMetricsObserver_RecordsIntoPrometheus(t *testing.T) {
	m := NewMetrics("test", "test")
	obs := &metricsObserver{m: m}

	obs.RowsWritten("events", 5)
	obs.RowsDropped("events", "buffer_full", 2)
	obs.WriteError("events", "http_5xx")
	obs.BufferedRows("events", 7)
	obs.BatchDuration("events", 3*time.Millisecond)

	if got := counterValue(m.ClickhouseRowsWritten.WithLabelValues("events")); got != 5 {
		t.Errorf("rows written: got %v want 5", got)
	}
	if got := counterValue(m.ClickhouseRowsDropped.WithLabelValues("events", "buffer_full")); got != 2 {
		t.Errorf("rows dropped: got %v want 2", got)
	}
	if got := counterValue(m.ClickhouseWriteErrors.WithLabelValues("events", "http_5xx")); got != 1 {
		t.Errorf("write errors: got %v want 1", got)
	}
	if got := gaugeRead(m.ClickhouseBufferedRows.WithLabelValues("events")); got != 7 {
		t.Errorf("buffered rows: got %v want 7", got)
	}
}

// An empty URL disables persistence and must not be an error.
func TestNewClickhouseClient_EmptyURLDisabled(t *testing.T) {
	c, err := newClickhouseClient("", "marketbyprice", 100, time.Second, 1000, NewMetrics("t", "t"))
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Error("empty URL must yield a nil client")
	}
}

// Every table the writers target must have a batcher, or its rows are silently
// rejected by Enqueue.
func TestNewClickhouseClient_ConfiguresEveryTable(t *testing.T) {
	c, err := newClickhouseClient("http://localhost:8123", "marketbyprice", 100, time.Second, 1000, NewMetrics("t", "t"))
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"events", "level_snapshots", "wire_levels", "instruments", "channel_health"} {
		if !c.Enqueue(table, map[string]any{"probe": 1}) {
			t.Errorf("table %q has no batcher configured", table)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/marketbyprice-bot && go test -run 'TestMetricsObserver|TestNewClickhouseClient' ./...`
Expected: FAIL to build — `undefined: metricsObserver`, `undefined: newClickhouseClient`, `m.ClickhouseRowsWritten undefined`.

- [ ] **Step 3: Add the metrics**

In `go/marketbyprice-bot/metrics.go`, add these fields to the `Metrics` struct
after `ChannelResetsTotal`:

```go
	// ClickHouse persistence. Populated through metricsObserver, which adapts
	// the shared internal/clickhouse client's Observer interface onto these.
	ClickhouseRowsWritten   *prometheus.CounterVec   // label: table
	ClickhouseRowsDropped   *prometheus.CounterVec   // labels: table, reason
	ClickhouseWriteErrors   *prometheus.CounterVec   // labels: table, reason
	ClickhouseBatchDuration *prometheus.HistogramVec // label: table
	ClickhouseBufferedRows  *prometheus.GaugeVec     // label: table
```

In `NewMetrics`, after the `m.ChannelResetsTotal = ...` line:

```go
	m.ClickhouseRowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_written_total"}, []string{"table"})
	m.ClickhouseRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_dropped_total"}, []string{"table", "reason"})
	m.ClickhouseWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_write_errors_total"}, []string{"table", "reason"})
	m.ClickhouseBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "clickhouse_batch_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"table"})
	m.ClickhouseBufferedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "clickhouse_buffered_rows"}, []string{"table"})
```

And add them to the `reg.MustRegister(...)` call:

```go
		m.ClickhouseRowsWritten, m.ClickhouseRowsDropped, m.ClickhouseWriteErrors,
		m.ClickhouseBatchDuration, m.ClickhouseBufferedRows,
```

- [ ] **Step 4: Write the adapter**

Create `go/marketbyprice-bot/clickhouse.go`:

```go
package main

import (
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// metricsObserver adapts the shared client's Observer onto this bot's metrics.
//
// The shared package deliberately owns no Prometheus dependency, so each
// consumer keeps its own metric names and namespace. This is the seam.
type metricsObserver struct{ m *Metrics }

func (o *metricsObserver) RowsWritten(table string, n int) {
	o.m.ClickhouseRowsWritten.WithLabelValues(table).Add(float64(n))
}

func (o *metricsObserver) RowsDropped(table, reason string, n int) {
	o.m.ClickhouseRowsDropped.WithLabelValues(table, reason).Add(float64(n))
}

func (o *metricsObserver) WriteError(table, reason string) {
	o.m.ClickhouseWriteErrors.WithLabelValues(table, reason).Inc()
}

func (o *metricsObserver) BatchDuration(table string, d time.Duration) {
	o.m.ClickhouseBatchDuration.WithLabelValues(table).Observe(d.Seconds())
}

func (o *metricsObserver) BufferedRows(table string, n int) {
	o.m.ClickhouseBufferedRows.WithLabelValues(table).Set(float64(n))
}

// newClickhouseClient configures one batcher per table the writers target.
//
// A table missing from this list is silently rejected by Enqueue, so every table
// in 03_schema_mbp.sql that the bot writes must appear here. instruments and
// channel_health get small, slow batchers: they are low-rate and worth landing
// promptly rather than sitting in a buffer waiting for a large batch to fill.
func newClickhouseClient(url, db string, batchSize int, batchInterval time.Duration, bufferSize int, m *Metrics) (*clickhouse.Client, error) {
	if url == "" {
		return nil, nil
	}
	return clickhouse.New(url, db, []clickhouse.BatcherConfig{
		{Table: "events", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "level_snapshots", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "wire_levels", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "instruments", BatchSize: 100, BatchInterval: time.Second, BufferSize: 1000},
		{Table: "channel_health", BatchSize: 100, BatchInterval: time.Second, BufferSize: 1000},
	}, &metricsObserver{m: m})
}
```

- [ ] **Step 5: Wire the module dependency and the container build**

```bash
cd go/marketbyprice-bot
go mod edit -require=github.com/malbeclabs/edge-multicast-ref/go/internal@v0.0.0
go mod edit -replace=github.com/malbeclabs/edge-multicast-ref/go/internal=../internal
go mod tidy
```

In `go/marketbyprice-bot/Dockerfile`, the build stage copies every workspace
member's `go.mod` but only this bot's source, with a comment saying other
modules' source is not needed. That is no longer true. Change:

```dockerfile
# Now copy only the bot's source.
COPY go/marketbyprice-bot/ ./
```

to:

```dockerfile
# The bot imports internal/clickhouse, so that module's source is needed too —
# unlike the other workspace members, whose go.mod alone satisfies the download.
COPY go/internal/ /src/go/internal/
COPY go/marketbyprice-bot/ ./
```

- [ ] **Step 6: Run tests and the standalone builds**

```bash
cd go/marketbyprice-bot
go test -run 'TestMetricsObserver|TestNewClickhouseClient' ./...   # PASS
go test -count=1 ./...                                             # PASS
GOWORK=off go build -o /tmp/dz-mbp-d .                             # must succeed
GOWORK=off GOOS=linux GOARCH=amd64 go build -o /tmp/dz-mbp-l .     # must succeed
```

The `GOWORK=off` builds are the ones that catch a missing `replace` directive.

- [ ] **Step 7: Verify the container build**

```bash
cd /Users/amcconnell/src/git/work/edge-multicast-ref
docker build -f go/marketbyprice-bot/Dockerfile -t dz-mbp-bot-probe . && echo DOCKER_OK
```

Expected: `DOCKER_OK`. This is the step that fails if the `COPY go/internal/`
line was missed. If Docker is unavailable, note it and flag it in the PR.

- [ ] **Step 8: Commit**

```bash
cd /Users/amcconnell/src/git/work/edge-multicast-ref
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: wire the shared clickhouse client and its metrics"
```

---

## Task 4: EventsWriter — events, instruments, channel_health

**Files:**
- Create: `go/marketbyprice-bot/events_writer.go`
- Test: `go/marketbyprice-bot/events_writer_test.go`

**Interfaces:**
- Consumes: `clickhouse.Client`, `clickhouse.ChTime` (Task 1); `ChannelEvent` and the `Kind*` constants from `shard.go`; the `toX` coercion helpers in `shard.go`.
- Produces: `EventsWriter` with `NewEventsWriter(ch enqueuer) *EventsWriter` and `(*EventsWriter).Write(ev ChannelEvent, channelID uint8, symbol string, priceExp, qtyExp int8)`; the `enqueuer` interface; helpers `getString`, `getUint8`, `getUint16`, `getUint32`, `getUint64`, `getInt8`, `getInt64`, `getTime`, `getOptUint16`, `scalePrice`, `scaleQty`.

- [ ] **Step 1: Write the failing test**

Create `go/marketbyprice-bot/events_writer_test.go`:

```go
package main

import (
	"testing"
)

// stubEnqueuer records rows by table so tests can assert the mapping.
type stubEnqueuer struct {
	rows map[string][]map[string]any
}

func newStubEnqueuer() *stubEnqueuer {
	return &stubEnqueuer{rows: map[string][]map[string]any{}}
}

func (s *stubEnqueuer) Enqueue(table string, row map[string]any) bool {
	s.rows[table] = append(s.rows[table], row)
	return true
}

func (s *stubEnqueuer) only(t *testing.T, table string) map[string]any {
	t.Helper()
	got := s.rows[table]
	if len(got) != 1 {
		t.Fatalf("expected exactly one %s row, got %d", table, len(got))
	}
	return got[0]
}

func TestEventsWriter_InstrumentDefinition(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       instDefRec(11, "BTC-USDT", 5),
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["symbol"] != "BTC-USDT" {
		t.Errorf("symbol: %v", row["symbol"])
	}
	if row["price_exponent"] != int8(-2) || row["qty_exponent"] != int8(-8) {
		t.Errorf("exponents: %v %v", row["price_exponent"], row["qty_exponent"])
	}
	if row["manifest_seq"] != uint16(5) {
		t.Errorf("manifest_seq: %v", row["manifest_seq"])
	}
}

// asset_class, market_model, settle_type and price_bound arrive as raw uint8.
// The parser stringifies side, action and the reason fields inline but NOT these,
// and the schema declares them LowCardinality(String), so reading them as strings
// would write empty values for every instrument.
func TestEventsWriter_InstrumentEnumsAreStringified(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := instDefRec(11, "SYM", 1)
	rec.Fields["asset_class"] = float64(1)  // crypto_spot
	rec.Fields["market_model"] = float64(1) // clob
	rec.Fields["settle_type"] = float64(1)  // cash
	rec.Fields["price_bound"] = float64(2)  // non_negative

	w.Write(ChannelEvent{Kind: KindInstrumentDefinition, InstrumentID: 11, Record: rec}, 0, "SYM", -2, -8)

	row := st.only(t, "instruments")
	for col, want := range map[string]string{
		"asset_class":  "crypto_spot",
		"market_model": "clob",
		"settle_type":  "cash",
		"price_bound":  "non_negative",
	} {
		if got := row[col]; got != want {
			t.Errorf("%s: got %v want %q", col, got, want)
		}
	}
}

func TestEventsWriter_LevelUpdateScalesAndKeepsSentinelsNull(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := levelUpdateRec(11, 900, 6, "bid", 123456, 500)
	// levelUpdateRec sets order_count; drop it to model the wire sentinel, which
	// the parser signals by OMITTING the key.
	delete(rec.Fields, "order_count")

	w.Write(ChannelEvent{Kind: KindAppliedDelta, InstrumentID: 11, Record: rec}, 0, "SYM", -2, -8)

	row := st.only(t, "events")
	if row["kind"] != "level_update" {
		t.Errorf("kind: %v", row["kind"])
	}
	if got := row["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled by 10^-2: got %v want ~1234.56", got)
	}
	if row["order_count"] != nil {
		t.Errorf("an omitted order_count must be nil, not %v — zero is a real count", row["order_count"])
	}
	if row["level_index"] != nil {
		t.Errorf("an omitted level_index must be nil, got %v", row["level_index"])
	}
	if row["per_instrument_seq"] != uint32(6) {
		t.Errorf("per_instrument_seq: %v", row["per_instrument_seq"])
	}
}

func TestEventsWriter_LevelUpdateKeepsRealZeroOrderCount(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := levelUpdateRec(11, 900, 6, "bid", 1000, 5)
	rec.Fields["order_count"] = float64(0) // a real count of zero, not the sentinel

	w.Write(ChannelEvent{Kind: KindAppliedDelta, InstrumentID: 11, Record: rec}, 0, "SYM", 0, 0)

	row := st.only(t, "events")
	if row["order_count"] == nil {
		t.Fatal("a present order_count of 0 must persist as 0, not NULL")
	}
	if got := row["order_count"].(uint32); got != 0 {
		t.Errorf("order_count: got %v want 0", got)
	}
}

func TestEventsWriter_BookClear(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindAppliedDelta,
		InstrumentID: 11,
		Record:       bookClearRec(11, 900, 6, "bid", "from_price", 5000),
	}, 0, "SYM", -2, 0)

	row := st.only(t, "events")
	if row["kind"] != "book_clear" {
		t.Errorf("kind: %v", row["kind"])
	}
	if row["clear_side"] != "bid" || row["clear_scope"] != "from_price" {
		t.Errorf("clear cols: %v %v", row["clear_side"], row["clear_scope"])
	}
	if got := row["from_price"].(float64); got < 49.99 || got > 50.01 {
		t.Errorf("from_price must be scaled: got %v want ~50", got)
	}
}

func TestEventsWriter_ChannelHealth(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{Record: Record{
		Type: "manifest_summary", Port: "refdata", ChannelID: 0,
		Fields: map[string]any{
			"manifest_seq": float64(7), "valid": float64(1), "instrument_count": float64(42),
		},
	}}, 0, "", 0, 0)

	row := st.only(t, "channel_health")
	if row["kind"] != "manifest_summary" {
		t.Errorf("kind: %v", row["kind"])
	}
	if row["manifest_seq"] != uint16(7) || row["instrument_count"] != uint32(42) {
		t.Errorf("manifest cols: %v %v", row["manifest_seq"], row["instrument_count"])
	}
}

// A nil client must be a safe no-op so the bot runs with persistence disabled.
func TestEventsWriter_NilClientIsNoOp(t *testing.T) {
	w := NewEventsWriter(nil)
	w.Write(ChannelEvent{Kind: KindAppliedDelta, Record: levelUpdateRec(11, 900, 6, "bid", 1000, 5)}, 0, "SYM", 0, 0)
	// No panic is the assertion.
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/marketbyprice-bot && go test -run TestEventsWriter ./...`
Expected: FAIL to build — `undefined: NewEventsWriter`.

- [ ] **Step 3: Write the implementation**

Create `go/marketbyprice-bot/events_writer.go`:

```go
package main

import (
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// enqueuer is satisfied by *clickhouse.Client and by test stubs.
type enqueuer interface {
	Enqueue(table string, row map[string]any) bool
}

// EventsWriter maps one ChannelEvent to ClickHouse rows. It is stateless: the
// batching client is already the asynchronous boundary and already drops to a
// counter when full, so no second queue sits in front of it.
type EventsWriter struct {
	ch enqueuer
}

func NewEventsWriter(ch enqueuer) *EventsWriter {
	return &EventsWriter{ch: ch}
}

// Write routes a record to its table. Prices and quantities are scaled by the
// instrument's exponents here, at the persistence boundary — book state stays
// raw.
func (w *EventsWriter) Write(ev ChannelEvent, channelID uint8, symbol string, priceExp, qtyExp int8) {
	if w == nil || w.ch == nil {
		return
	}
	rec := ev.Record
	now := time.Now().UTC()

	switch rec.Type {
	case "instrument_definition":
		w.ch.Enqueue("instruments", map[string]any{
			"recv_ts":        clickhouse.ChTime(now),
			"channel_id":     channelID,
			"instrument_id":  rec.InstrumentID,
			"symbol":         getString(rec.Fields, "symbol"),
			"leg1":           getString(rec.Fields, "leg1"),
			"leg2":           getString(rec.Fields, "leg2"),
			// These four arrive as raw uint8 enums: unlike side, action and the
			// reason fields, the parser does NOT stringify them. The schema
			// declares them LowCardinality(String), so getString would silently
			// write empty strings for every instrument.
			"asset_class":    assetClassString(getUint8(rec.Fields, "asset_class")),
			"market_model":   marketModelString(getUint8(rec.Fields, "market_model")),
			"price_exponent": getInt8(rec.Fields, "price_exponent"),
			"qty_exponent":   getInt8(rec.Fields, "qty_exponent"),
			"tick_size":      scalePrice(getInt64(rec.Fields, "tick_size_raw"), getInt8(rec.Fields, "price_exponent")),
			"lot_size":       scaleQty(getUint64(rec.Fields, "lot_size_raw"), getInt8(rec.Fields, "qty_exponent")),
			"contract_value": getUint64(rec.Fields, "contract_value"),
			"expiry_ts":      clickhouse.ChTime(getTime(rec.Fields, "expiry")),
			"settle_type":    settleTypeString(getUint8(rec.Fields, "settle_type")),
			"price_bound":    priceBoundString(getUint8(rec.Fields, "price_bound")),
			"manifest_seq":   getUint16(rec.Fields, "manifest_seq"),
		})

	case "heartbeat", "manifest_summary", "end_of_session":
		row := map[string]any{
			"recv_ts":           clickhouse.ChTime(rec.recvTime(now)),
			"publisher_send_ts": clickhouse.ChTime(rec.sendTime()),
			"recv_ts_kind":      rec.RecvTSKind,
			"channel_id":        channelID,
			"kind":              rec.Type,
		}
		if src, ok := rec.sourceTime(); ok {
			row["source_ts"] = clickhouse.ChTime(src)
		}
		if rec.Type == "manifest_summary" {
			row["manifest_seq"] = getUint16(rec.Fields, "manifest_seq")
			row["manifest_valid"] = getUint8(rec.Fields, "valid")
			row["instrument_count"] = getUint32(rec.Fields, "instrument_count")
		}
		w.ch.Enqueue("channel_health", row)

	case "level_update", "book_clear", "trade", "liquidation", "batch_boundary", "instrument_reset":
		row := buildEventRow(rec, channelID, symbol, now)
		switch rec.Type {
		case "level_update":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["side"] = getString(rec.Fields, "side")
			row["price"] = scalePrice(getInt64(rec.Fields, "price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "qty_raw"), qtyExp)
			row["action"] = getString(rec.Fields, "action")
			row["update_reason"] = getString(rec.Fields, "update_reason")
			row["level_flags"] = getUint8(rec.Fields, "level_flags")
			// Absent means the 0xFFFF sentinel, which is SQL NULL. Zero is a real
			// count and a real rank, so it must not be conflated with absent.
			row["order_count"] = getOptUint32(rec.Fields, "order_count")
			row["level_index"] = getOptUint16(rec.Fields, "level_index")
		case "book_clear":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["clear_side"] = getString(rec.Fields, "clear_side")
			row["clear_scope"] = getString(rec.Fields, "scope")
			row["from_price"] = scalePrice(getInt64(rec.Fields, "from_price_raw"), priceExp)
			row["clear_reason"] = getString(rec.Fields, "clear_reason")
		case "trade":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["price"] = scalePrice(getInt64(rec.Fields, "trade_price_raw"), priceExp)
			row["qty"] = scaleQty(getUint64(rec.Fields, "trade_qty_raw"), qtyExp)
			row["cumulative_volume"] = scaleQty(getUint64(rec.Fields, "cumulative_volume_raw"), qtyExp)
			row["trade_flags"] = getUint8(rec.Fields, "trade_flags")
		case "liquidation":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["liquidation_flags"] = getUint8(rec.Fields, "liquidation_flags")
			row["method"] = getString(rec.Fields, "method")
			row["mark_price"] = scalePrice(getInt64(rec.Fields, "mark_price_raw"), priceExp)
			row["liquidated_user"] = getString(rec.Fields, "liquidated_user")
		case "batch_boundary":
			row["batch_id"] = getUint32(rec.Fields, "batch_id")
			row["batch_ts"] = clickhouse.ChTime(getTime(rec.Fields, "batch_ts"))
		case "instrument_reset":
			row["reset_reason"] = getString(rec.Fields, "reason")
			row["new_anchor_seq"] = getUint64(rec.Fields, "new_anchor_seq")
		}
		w.ch.Enqueue("events", row)
	}
}

// buildEventRow fills the identity and timestamp columns shared by every kind.
func buildEventRow(rec Record, channelID uint8, symbol string, now time.Time) map[string]any {
	row := map[string]any{
		"recv_ts":           clickhouse.ChTime(rec.recvTime(now)),
		"publisher_send_ts": clickhouse.ChTime(rec.sendTime()),
		"recv_ts_kind":      rec.RecvTSKind,
		"channel_id":        channelID,
		"mktdata_seq":       rec.SequenceNumber,
		"reset_count":       rec.ResetCount,
		"kind":              rec.Type,
		"instrument_id":     rec.InstrumentID,
		"symbol":            symbol,
	}
	if src, ok := rec.sourceTime(); ok {
		row["source_ts"] = clickhouse.ChTime(src)
	}
	return row
}

// Field accessors. Records carry map[string]any after JSON decode.
func getString(m map[string]any, k string) string  { return toString(m[k]) }
func getUint8(m map[string]any, k string) uint8    { return toUint8(m[k]) }
func getUint16(m map[string]any, k string) uint16  { return toUint16(m[k]) }
func getUint32(m map[string]any, k string) uint32  { return toUint32(m[k]) }
func getUint64(m map[string]any, k string) uint64  { return toUint64(m[k]) }
func getInt8(m map[string]any, k string) int8      { return toInt8(m[k]) }
func getInt64(m map[string]any, k string) int64    { return toInt64(m[k]) }
func getTime(m map[string]any, k string) time.Time { return toTime(m[k]) }

// getOptUint32 and getOptUint16 return nil for an absent key, which encodes as
// SQL NULL. The parser omits these keys when the wire carried 0xFFFF, so absent
// means "the venue did not supply it" — distinct from a supplied zero.
func getOptUint32(m map[string]any, k string) any {
	if _, present := m[k]; !present {
		return nil
	}
	return toUint32(m[k])
}

func getOptUint16(m map[string]any, k string) any {
	if _, present := m[k]; !present {
		return nil
	}
	return toUint16(m[k])
}

// scalePrice and scaleQty apply the per-instrument exponent at the persistence
// boundary. An exponent of 0 means raw integers as floats.
func scalePrice(raw int64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}

func scaleQty(raw uint64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}

func pow10f(e int) float64 {
	v := 1.0
	if e >= 0 {
		for i := 0; i < e; i++ {
			v *= 10
		}
		return v
	}
	for i := 0; i < -e; i++ {
		v /= 10
	}
	return v
}

// Enum stringers for instrument_definition. The parser stringifies side, action
// and the reason fields inline, but leaves these four as raw uint8, so the
// mapping lives here. Values match the sibling market-by-order bot.
func assetClassString(v uint8) string {
	switch v {
	case 1:
		return "crypto_spot"
	case 2:
		return "prediction_binary"
	case 3:
		return "prediction_scalar"
	case 4:
		return "prediction_categorical"
	default:
		return "unknown"
	}
}

func marketModelString(v uint8) string {
	switch v {
	case 1:
		return "clob"
	case 2:
		return "amm"
	default:
		return "unknown"
	}
}

func settleTypeString(v uint8) string {
	switch v {
	case 1:
		return "cash"
	case 2:
		return "physical"
	default:
		return "n_a"
	}
}

func priceBoundString(v uint8) string {
	switch v {
	case 1:
		return "bounded_01"
	case 2:
		return "non_negative"
	default:
		return "unbounded"
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd go/marketbyprice-bot && go test -run TestEventsWriter ./... -v`
Expected: PASS, all 6 tests.

- [ ] **Step 5: Commit**

```bash
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/events_writer.go go/marketbyprice-bot/events_writer_test.go
git commit -m "marketbyprice-bot: add events writer"
```

---

## Task 5: wire_levels capture from the last SnapshotBegin

**Files:**
- Modify: `go/marketbyprice-bot/instrument.go` (add `LastBegin`)
- Modify: `go/marketbyprice-bot/dispatch.go` (record it in `applySnapshotBegin`)
- Modify: `go/marketbyprice-bot/events_writer.go` (add `WriteWireLevel`)
- Test: `go/marketbyprice-bot/events_writer_test.go`, `go/marketbyprice-bot/dispatch_test.go`

**Interfaces:**
- Consumes: `EventsWriter` (Task 4), `Instrument` (existing).
- Produces: `SnapshotGroup` struct; `Instrument.LastBegin *SnapshotGroup`; `(*EventsWriter).WriteWireLevel(rec Record, channelID uint8, g SnapshotGroup, symbol string, priceExp, qtyExp int8)`.

**Why this task exists:** `wire_levels` denormalizes group identity onto every
row, but those five fields come from `SnapshotBegin`, not from the
`SnapshotLevel` records. Reading them from the open shadow would capture almost
nothing: in steady state a ready, current instrument DECLINES its periodic
snapshot, so no shadow exists, yet the publisher still sends every level. The
replay table would populate only during recovery and sit near-empty on a healthy
feed — the exact inverse of what it is for.

- [ ] **Step 1: Write the failing tests**

Append to `go/marketbyprice-bot/dispatch_test.go`:

```go
// The last SnapshotBegin's identity must be recorded even when the snapshot is
// DECLINED, because wire_levels denormalizes it onto every captured level and
// declining is the steady-state case.
func TestApply_LastBeginRecordedEvenWhenDeclined(t *testing.T) {
	s := NewShard(0, 1, NewMetrics("test", "test"))
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	// K == tracker, so the begin is declined and no shadow opens.
	s.apply(snapBeginRec(11, 4, 3, 100, 25, 9999))

	if inst.OpenSnapshot != nil {
		t.Fatal("setup: a current ready instrument must not open a shadow")
	}
	if inst.LastBegin == nil {
		t.Fatal("LastBegin must be recorded even for a declined snapshot")
	}
	if inst.LastBegin.SnapshotID != 4 || inst.LastBegin.AnchorSeq != 9999 {
		t.Errorf("LastBegin identity: %+v", inst.LastBegin)
	}
	if inst.LastBegin.TotalLevels != 3 || inst.LastBegin.LastInstrumentSeq != 100 {
		t.Errorf("LastBegin counts: %+v", inst.LastBegin)
	}
	if inst.LastBegin.DepthBound != 25 {
		t.Errorf("LastBegin depth bound: %+v", inst.LastBegin)
	}
}

// It must also be recorded on the accepted path, so recovery captures too.
func TestApply_LastBeginRecordedWhenAccepted(t *testing.T) {
	s := NewShard(0, 1, NewMetrics("test", "test"))
	s.apply(instDefRec(11, "SYM", 1))
	s.apply(snapBeginRec(11, 7, 2, 50, 0, 5000))

	inst := s.instruments[instKey{0, 11}]
	if inst.LastBegin == nil || inst.LastBegin.SnapshotID != 7 {
		t.Fatalf("LastBegin: %+v", inst.LastBegin)
	}
}
```

Append to `go/marketbyprice-bot/events_writer_test.go`:

```go
func TestEventsWriter_WireLevelDenormalizesGroupIdentity(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	g := SnapshotGroup{
		SnapshotID: 4, AnchorSeq: 9999, TotalLevels: 3,
		LastInstrumentSeq: 100, DepthBound: 25,
	}
	rec := snapLevelRec(11, 4, "bid", 123456, 500)

	w.WriteWireLevel(rec, 0, g, "SYM", -2, -8)

	row := st.only(t, "wire_levels")
	if row["snapshot_id"] != uint32(4) || row["anchor_seq"] != uint64(9999) {
		t.Errorf("group identity: %v %v", row["snapshot_id"], row["anchor_seq"])
	}
	if row["total_levels"] != uint32(3) || row["last_instrument_seq"] != uint32(100) {
		t.Errorf("group counts: %v %v", row["total_levels"], row["last_instrument_seq"])
	}
	if row["depth_bound"] != uint32(25) {
		t.Errorf("depth_bound: %v", row["depth_bound"])
	}
	if got := row["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled: got %v", got)
	}
	if row["side"] != "bid" {
		t.Errorf("side: %v", row["side"])
	}
}

func TestEventsWriter_WireLevelOmittedOrderCountIsNull(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := snapLevelRec(11, 4, "bid", 1000, 5)
	delete(rec.Fields, "order_count") // the wire sentinel

	w.WriteWireLevel(rec, 0, SnapshotGroup{SnapshotID: 4}, "SYM", 0, 0)

	if row := st.only(t, "wire_levels"); row["order_count"] != nil {
		t.Errorf("an omitted order_count must be nil, got %v", row["order_count"])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go/marketbyprice-bot && go test -run 'TestApply_LastBegin|TestEventsWriter_WireLevel' ./...`
Expected: FAIL to build — `undefined: SnapshotGroup`, `inst.LastBegin undefined`, `undefined: WriteWireLevel`.

- [ ] **Step 3: Add SnapshotGroup and LastBegin**

In `go/marketbyprice-bot/instrument.go`, add above the `Instrument` struct:

```go
// SnapshotGroup is the identity a SnapshotBegin establishes for the group of
// SnapshotLevel records that follow it.
//
// It is recorded whether or not the snapshot is accepted. SnapshotLevel records
// carry only snapshot_id, so the remaining four fields exist nowhere else, and a
// ready, current instrument DECLINES its periodic snapshot without opening a
// shadow while the publisher still sends every level of that group. Sourcing
// these from OpenSnapshot would leave the replay capture empty exactly when the
// feed is healthy.
type SnapshotGroup struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	LastInstrumentSeq uint32
	DepthBound        uint32
}
```

Add this field to the `Instrument` struct, after `OpenSnapshot`:

```go
	// LastBegin is the identity of the most recent SnapshotBegin, accepted or
	// declined. Used only to denormalize group identity onto wire_levels rows.
	LastBegin *SnapshotGroup
```

`Instrument.Reset` must NOT clear `LastBegin`: a reset discards book state, but
levels already in flight from the pre-reset group still need their identity to be
captured correctly.

- [ ] **Step 4: Record it in applySnapshotBegin**

In `go/marketbyprice-bot/dispatch.go`, in `applySnapshotBegin`, immediately after
the `lastInstr := ...` line and BEFORE the `SnapshotAcceptable` call:

```go
	// Record the group identity before any accept/decline decision. Declining is
	// the steady-state case and its levels still arrive and still need capturing.
	inst.LastBegin = &SnapshotGroup{
		SnapshotID:        toUint32(rec.Fields["snapshot_id"]),
		AnchorSeq:         anchor,
		TotalLevels:       toUint32(rec.Fields["total_levels"]),
		LastInstrumentSeq: lastInstr,
		DepthBound:        toUint32(rec.Fields["depth_bound"]),
	}
```

- [ ] **Step 5: Add WriteWireLevel**

Append to `go/marketbyprice-bot/events_writer.go`:

```go
// WriteWireLevel captures one raw SnapshotLevel for replay, denormalizing the
// group identity from the instrument's last SnapshotBegin.
func (w *EventsWriter) WriteWireLevel(rec Record, channelID uint8, g SnapshotGroup, symbol string, priceExp, qtyExp int8) {
	if w == nil || w.ch == nil {
		return
	}
	w.ch.Enqueue("wire_levels", map[string]any{
		"recv_ts":             clickhouse.ChTime(rec.recvTime(time.Now().UTC())),
		"publisher_send_ts":   clickhouse.ChTime(rec.sendTime()),
		"channel_id":          channelID,
		"instrument_id":       rec.InstrumentID,
		"symbol":              symbol,
		"snapshot_id":         g.SnapshotID,
		"anchor_seq":          g.AnchorSeq,
		"total_levels":        g.TotalLevels,
		"last_instrument_seq": g.LastInstrumentSeq,
		"depth_bound":         g.DepthBound,
		"side":                getString(rec.Fields, "side"),
		"price":               scalePrice(getInt64(rec.Fields, "price_raw"), priceExp),
		"qty":                 scaleQty(getUint64(rec.Fields, "qty_raw"), qtyExp),
		"order_count":         getOptUint32(rec.Fields, "order_count"),
		"level_flags":         getUint8(rec.Fields, "level_flags"),
	})
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cd go/marketbyprice-bot && go test -count=1 ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: capture wire levels with last-begin group identity"
```

---

## Task 6: SnapshotWriter

**Files:**
- Create: `go/marketbyprice-bot/snapshot_writer.go`
- Modify: `go/marketbyprice-bot/metrics.go`
- Test: `go/marketbyprice-bot/snapshot_writer_test.go`

**Interfaces:**
- Consumes: `enqueuer` (Task 4), `clickhouse.ChTime` (Task 1), `ComputeLevels`/`LevelSnapshot` (existing `levels.go`), `instKey` (existing `shard.go`).
- Produces: `SnapshotWriter` with `NewSnapshotWriter(ch enqueuer, depth, coalesceMS int, m *Metrics, withInstrument func(instKey, func(*Instrument))) *SnapshotWriter`, `(*SnapshotWriter).MarkDirty(k instKey)`, `(*SnapshotWriter).Run(ctx)`, `(*SnapshotWriter).Reset(ctx)`; `Metrics` fields `SnapshotWritesTotal`, `SnapshotCoalescesTotal`, `SnapshotLagMs`, `BookLevels`, `BookTopPrice`, `BookTopQty`, `BookSpreadBps`.
- **Note for later tasks:** Task 8 appends a sixth parameter, `persists func(symbol string) bool`, to `NewSnapshotWriter`. Every call site changes then. Do not add it now — Task 6's tests are written against the five-parameter form.

- [ ] **Step 1: Add the metrics**

In `metrics.go`, add to the `Metrics` struct:

```go
	// Snapshot writer
	SnapshotWritesTotal    prometheus.Counter
	SnapshotCoalescesTotal prometheus.Counter
	SnapshotLagMs          prometheus.Histogram

	// Book state, refreshed on every snapshot flush
	BookLevels    *prometheus.GaugeVec // labels: symbol, side
	BookTopPrice  *prometheus.GaugeVec // labels: symbol, side
	BookTopQty    *prometheus.GaugeVec // labels: symbol, side
	BookSpreadBps *prometheus.GaugeVec // label: symbol
```

In `NewMetrics`:

```go
	m.SnapshotWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_writes_total"})
	m.SnapshotCoalescesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_coalesces_total"})
	m.SnapshotLagMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "snapshot_lag_ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})
	m.BookLevels = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_levels"}, []string{"symbol", "side"})
	m.BookTopPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_price"}, []string{"symbol", "side"})
	m.BookTopQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_qty"}, []string{"symbol", "side"})
	m.BookSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_spread_bps"}, []string{"symbol"})
```

And register all seven in `reg.MustRegister(...)`.

- [ ] **Step 2: Write the failing tests**

Create `go/marketbyprice-bot/snapshot_writer_test.go`:

```go
package main

import (
	"context"
	"testing"
	"time"
)

// newTestSnapshotWriter wires a writer over a fixed instrument map.
func newTestSnapshotWriter(t *testing.T, st enqueuer, m *Metrics, insts map[instKey]*Instrument) *SnapshotWriter {
	t.Helper()
	return NewSnapshotWriter(st, 5, 0 /*coalesce off for determinism*/, m, func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	})
}

func readyInstrument(id uint32, symbol string) *Instrument {
	inst := NewInstrument(id, symbol, 0, 0)
	inst.Status = StatusReady
	inst.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1) // bid
	inst.ApplyLevelUpdate(1, 1100, 60, 1, 0, 1) // ask
	return inst
}

// Two channels sharing an instrument ID must not collide. A dirty map keyed by
// bare uint32 — as the sibling market-by-order bot uses — would fold these two
// books into one entry and persist whichever flushed last.
func TestSnapshotWriter_KeysByChannelAndInstrument(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{
		{0, 11}: readyInstrument(11, "SYM-CH0"),
		{1, 11}: readyInstrument(11, "SYM-CH1"),
	}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	w.MarkDirty(instKey{0, 11})
	w.MarkDirty(instKey{1, 11})
	w.flushDue()

	seen := map[string]bool{}
	for _, row := range st.rows["level_snapshots"] {
		seen[row["symbol"].(string)] = true
	}
	if !seen["SYM-CH0"] || !seen["SYM-CH1"] {
		t.Errorf("both channels' books must be written, saw %v", seen)
	}
}

// Marking the same instrument repeatedly before a flush coalesces into one write.
func TestSnapshotWriter_CoalescesRepeatedMarks(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	insts := map[instKey]*Instrument{{0, 11}: readyInstrument(11, "SYM")}
	w := newTestSnapshotWriter(t, st, m, insts)

	for i := 0; i < 5; i++ {
		w.MarkDirty(instKey{0, 11})
	}
	w.flushDue()

	// depth 5, one bid and one ask level present => 2 rows for one flush.
	if got := len(st.rows["level_snapshots"]); got != 2 {
		t.Errorf("one coalesced flush should write 2 rows, got %d", got)
	}
	if got := counterValue(m.SnapshotCoalescesTotal); got != 4 {
		t.Errorf("coalesces: got %v want 4", got)
	}
	if got := counterValue(m.SnapshotWritesTotal); got != 1 {
		t.Errorf("writes: got %v want 1", got)
	}
}

// crossed and depth_bound must reach the row: they are the two columns this feed
// adds over the sibling's schema.
func TestSnapshotWriter_WritesCrossedAndDepthBound(t *testing.T) {
	st := newStubEnqueuer()
	inst := NewInstrument(11, "SYM", 0, 0)
	inst.Status = StatusReady
	inst.ApplyLevelUpdate(0, 1200, 50, 1, 0, 1) // bid above ask: crossed
	inst.ApplyLevelUpdate(1, 1000, 60, 1, 0, 1)
	bound := uint32(25)
	inst.DepthBound = &bound

	insts := map[instKey]*Instrument{{0, 11}: inst}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	rows := st.rows["level_snapshots"]
	if len(rows) == 0 {
		t.Fatal("expected rows")
	}
	if rows[0]["crossed"] != uint8(1) {
		t.Errorf("crossed: got %v want 1", rows[0]["crossed"])
	}
	if rows[0]["depth_bound"] != uint32(25) {
		t.Errorf("depth_bound: got %v want 25", rows[0]["depth_bound"])
	}
}

// An unknown depth bound is NULL, never 0 — 0 is the publisher's positive claim
// of a complete book, which is a different statement.
func TestSnapshotWriter_UnknownDepthBoundIsNull(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: readyInstrument(11, "SYM")} // DepthBound nil
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	if rows := st.rows["level_snapshots"]; rows[0]["depth_bound"] != nil {
		t.Errorf("unknown depth bound must be nil, got %v", rows[0]["depth_bound"])
	}
}

// An instrument that has never been snapshotted has no servable book.
func TestSnapshotWriter_SkipsAwaitingSnapshot(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: NewInstrument(11, "SYM", 0, 0)} // awaiting
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	if got := len(st.rows["level_snapshots"]); got != 0 {
		t.Errorf("awaiting-snapshot instrument must not be written, got %d rows", got)
	}
}

// A gapped instrument is still written, flagged stale, so a consumer can see the
// book exists but is not current.
func TestSnapshotWriter_MarksGappedBookStale(t *testing.T) {
	st := newStubEnqueuer()
	inst := readyInstrument(11, "SYM")
	inst.Status = StatusGap
	insts := map[instKey]*Instrument{{0, 11}: inst}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	rows := st.rows["level_snapshots"]
	if len(rows) == 0 || rows[0]["stale"] != uint8(1) {
		t.Errorf("gapped book must be flagged stale: %+v", rows)
	}
}

// Reset clears pending work and bumps the generation so an in-flight batch is
// abandoned rather than written against post-reset state.
func TestSnapshotWriter_ResetClearsDirty(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: readyInstrument(11, "SYM")}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.MarkDirty(instKey{0, 11})
	w.Reset(ctx)

	w.mu.Lock()
	n := len(w.dirty)
	w.mu.Unlock()
	if n != 0 {
		t.Errorf("Reset must clear the dirty map, %d entries remain", n)
	}
}

// Reset must not wedge when Run has already exited on shutdown.
func TestSnapshotWriter_ResetDoesNotWedgeAfterShutdown(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Run never starts

	done := make(chan struct{})
	go func() { w.Reset(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Reset wedged after shutdown")
	}
}

func TestSnapshotWriter_NilClientIsNoOp(t *testing.T) {
	insts := map[instKey]*Instrument{{0, 11}: readyInstrument(11, "SYM")}
	w := NewSnapshotWriter(nil, 5, 0, NewMetrics("t", "t"), func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	})
	w.MarkDirty(instKey{0, 11})
	w.flushDue() // must not panic
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd go/marketbyprice-bot && go test -run TestSnapshotWriter ./...`
Expected: FAIL to build — `undefined: NewSnapshotWriter`.

- [ ] **Step 4: Write the implementation**

Create `go/marketbyprice-bot/snapshot_writer.go`:

```go
package main

import (
	"context"
	"sync"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// SnapshotWriter coalesces book changes and writes a top-N read-out to
// level_snapshots at most once per coalesce interval per instrument.
//
// One writer per shard. The flush loop reaches instruments through
// withInstrument, which takes that shard's own mutex, so a flush never contends
// with another shard's goroutine.
type SnapshotWriter struct {
	ch               enqueuer
	depth            int
	coalesceInterval time.Duration
	tickInterval     time.Duration
	metrics          *Metrics

	// withInstrument runs fn under the owning shard's lock with the current
	// instrument, or nil when the shard does not have it.
	withInstrument func(instKey, func(*Instrument))

	mu sync.Mutex
	// dirty is keyed by instKey, NOT by a bare instrument ID. A shard owns
	// instruments across every channel for its id-modulo, so an id-only key would
	// fold two channels' books into one entry and silently persist whichever
	// flushed last.
	dirty map[instKey]*dirtyEntry
	// generation is bumped by Reset to invalidate a batch already extracted from
	// dirty but not yet written.
	generation uint64
	resetCh    chan chan struct{}
}

type dirtyEntry struct {
	key            instKey
	dirtiedAt      time.Time
	nextAllowedAt  time.Time
	coalescedCount int
}

func NewSnapshotWriter(ch enqueuer, depth, coalesceMS int, m *Metrics, withInstrument func(instKey, func(*Instrument))) *SnapshotWriter {
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          m,
		withInstrument:   withInstrument,
		dirty:            map[instKey]*dirtyEntry{},
		resetCh:          make(chan chan struct{}, 1),
	}
}

// MarkDirty signals that an instrument's book changed. Only a real book mutation
// may call this: dirtying on a non-mutating event would rewrite an unchanged
// book on every batch boundary.
func (w *SnapshotWriter) MarkDirty(k instKey) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if e, ok := w.dirty[k]; ok {
		e.coalescedCount++
		if w.metrics != nil {
			w.metrics.SnapshotCoalescesTotal.Inc()
		}
		return
	}
	w.dirty[k] = &dirtyEntry{key: k, dirtiedAt: now, nextAllowedAt: now}
}

// Reset clears pending work and invalidates any in-flight batch. It is
// serialized onto the writer goroutine and blocks until applied, so the caller
// can rely on no concurrent flush.
//
// It is ctx-aware so a shutdown already in flight — Run having returned via
// ctx.Done — cannot wedge the caller.
func (w *SnapshotWriter) Reset(ctx context.Context) {
	done := make(chan struct{})
	select {
	case w.resetCh <- done:
	case <-ctx.Done():
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (w *SnapshotWriter) doReset() {
	w.mu.Lock()
	w.dirty = map[instKey]*dirtyEntry{}
	w.generation++
	w.mu.Unlock()
}

// Run is the tick loop. Returns when ctx is cancelled.
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Release a Reset caller whose ctx is broader than Run's, so it cannot
			// wait on a goroutine that has already returned. resetCh is buffered at
			// 1, so a non-blocking peek suffices.
			select {
			case done := <-w.resetCh:
				close(done)
			default:
			}
			return
		case done := <-w.resetCh:
			w.doReset()
			close(done)
		case <-tick.C:
			w.flushDue()
		}
	}
}

func (w *SnapshotWriter) flushDue() {
	w.mu.Lock()
	now := time.Now()
	gen := w.generation
	var due []*dirtyEntry
	for k, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, k)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		w.mu.Lock()
		stale := w.generation != gen
		w.mu.Unlock()
		if stale {
			return // a Reset landed after this batch was extracted; abandon it
		}

		var (
			snap      LevelSnapshot
			lastSeq   uint64
			servable  bool
			bookStale bool
		)
		w.withInstrument(e.key, func(inst *Instrument) {
			if inst == nil || inst.Status == StatusAwaitingSnapshot {
				return
			}
			snap = ComputeLevels(inst, w.depth)
			lastSeq = inst.LastAppliedMktdataSeq
			servable = true
			bookStale = inst.Status == StatusGap
		})
		if !servable {
			continue
		}

		w.updateBookGauges(snap)
		w.write(e.key, snap, lastSeq, bookStale, now)

		if w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}

		// Re-arm the coalesce window if the instrument was dirtied again while we
		// were writing it.
		w.mu.Lock()
		if again, ok := w.dirty[e.key]; ok {
			rearm := now.Add(w.coalesceInterval)
			if again.nextAllowedAt.Before(rearm) {
				again.nextAllowedAt = rearm
			}
		}
		w.mu.Unlock()
	}
}

// updateBookGauges refreshes the book-state gauges from a freshly computed
// read-out, so they stay current without a separate sweep.
func (w *SnapshotWriter) updateBookGauges(snap LevelSnapshot) {
	if w.metrics == nil {
		return
	}
	m, sym := w.metrics, snap.Symbol

	m.BookLevels.WithLabelValues(sym, "bid").Set(float64(len(snap.Bids)))
	m.BookLevels.WithLabelValues(sym, "ask").Set(float64(len(snap.Asks)))

	if len(snap.Bids) > 0 {
		m.BookTopPrice.WithLabelValues(sym, "bid").Set(snap.Bids[0].Price)
		m.BookTopQty.WithLabelValues(sym, "bid").Set(snap.Bids[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(sym, "bid")
		m.BookTopQty.DeleteLabelValues(sym, "bid")
	}
	if len(snap.Asks) > 0 {
		m.BookTopPrice.WithLabelValues(sym, "ask").Set(snap.Asks[0].Price)
		m.BookTopQty.WithLabelValues(sym, "ask").Set(snap.Asks[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(sym, "ask")
		m.BookTopQty.DeleteLabelValues(sym, "ask")
	}

	if len(snap.Bids) > 0 && len(snap.Asks) > 0 {
		bestBid, bestAsk := snap.Bids[0].Price, snap.Asks[0].Price
		if mid := (bestBid + bestAsk) / 2; mid != 0 {
			m.BookSpreadBps.WithLabelValues(sym).Set((bestAsk - bestBid) / mid * 10000)
		}
	} else {
		m.BookSpreadBps.DeleteLabelValues(sym)
	}
}

func (w *SnapshotWriter) write(k instKey, snap LevelSnapshot, lastSeq uint64, bookStale bool, now time.Time) {
	if w.ch == nil {
		return
	}
	staleFlag, crossedFlag := uint8(0), uint8(0)
	if bookStale {
		staleFlag = 1
	}
	if snap.Crossed {
		crossedFlag = 1
	}
	// A nil DepthBound is unknown and must encode as SQL NULL. Zero is the
	// publisher's positive claim of a complete book — a different statement, and
	// the only value under which cumulative_qty is exhaustive.
	var depthBound any
	if snap.DepthBound != nil {
		depthBound = *snap.DepthBound
	}
	nowStr := clickhouse.ChTime(now)

	emit := func(side string, levels []Level) {
		for i, lvl := range levels {
			w.ch.Enqueue("level_snapshots", map[string]any{
				"recv_ts":           nowStr,
				"publisher_send_ts": nowStr,
				"channel_id":        k.ch,
				"instrument_id":     k.id,
				"symbol":            snap.Symbol,
				"last_applied_seq":  lastSeq,
				"side":              side,
				"level_idx":         uint16(i),
				"price":             lvl.Price,
				"qty":               lvl.Qty,
				"order_count":       lvl.OrderCount,
				"cumulative_qty":    lvl.CumulativeQty,
				"stale":             staleFlag,
				"crossed":           crossedFlag,
				"depth_bound":       depthBound,
			})
		}
	}
	emit("bid", snap.Bids)
	emit("ask", snap.Asks)
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd go/marketbyprice-bot && go test -race -run TestSnapshotWriter ./... -v`
Expected: PASS, all 9 tests.

- [ ] **Step 6: Commit**

```bash
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add coalescing snapshot writer"
```

---

## Task 7: Wire the writers into the shard, reset path, and main

**Files:**
- Modify: `go/marketbyprice-bot/shard.go` (fields, `handle`)
- Modify: `go/marketbyprice-bot/dispatch.go` (`applySnapshotLevel`, `Run` reset case)
- Modify: `go/marketbyprice-bot/main.go` (flags and construction)
- Test: `go/marketbyprice-bot/shard_test.go`

**Interfaces:**
- Consumes: `EventsWriter` (Tasks 4, 5), `SnapshotWriter` (Task 6), `newClickhouseClient` (Task 3).
- Produces: `Shard.eventsW *EventsWriter`, `Shard.sw *SnapshotWriter`; `NewShard(idx, n int, eventsW *EventsWriter, m *Metrics) *Shard` (signature change — `sw` is assigned after construction because the writer's `withInstrument` closure needs the shard).

- [ ] **Step 1: Write the failing tests**

Append to `go/marketbyprice-bot/shard_test.go`. Its import block currently holds
`bytes`, `encoding/json` and `testing`; add `context` for the disconnect test.

```go
// Only a real book mutation may dirty an instrument for snapshotting. A
// non-mutating kind marking the book dirty would rewrite an unchanged book on
// every batch boundary and every instrument definition.
func TestHandle_OnlyMutatingEventsMarkDirty(t *testing.T) {
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[k])
	})

	// instrument_definition and batch_boundary are non-mutating.
	s.handle(instDefRec(11, "SYM", 1))
	s.handle(Record{Type: "batch_boundary", Port: "mktdata", Fields: map[string]any{}})

	s.sw.mu.Lock()
	n := len(s.sw.dirty)
	s.sw.mu.Unlock()
	if n != 0 {
		t.Fatalf("non-mutating events must not dirty a book, got %d entries", n)
	}

	// An applied delta must.
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 1000, 50))

	s.sw.mu.Lock()
	n = len(s.sw.dirty)
	_, present := s.sw.dirty[instKey{0, 11}]
	s.sw.mu.Unlock()
	if n != 1 || !present {
		t.Errorf("an applied delta must dirty its instrument: n=%d present=%v", n, present)
	}
}

// Events reach the writer with the instrument's symbol and exponents attached,
// which is what lets the writer scale raw prices at the persistence boundary.
func TestHandle_WritesEventsWithRefdata(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) })

	s.handle(instDefRec(11, "BTC-USDT", 1)) // price_exponent -2, qty_exponent -8
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 123456, 500))

	rows := st.rows["events"]
	if len(rows) != 1 {
		t.Fatalf("expected one events row, got %d", len(rows))
	}
	if rows[0]["symbol"] != "BTC-USDT" {
		t.Errorf("symbol must come from refdata: %v", rows[0]["symbol"])
	}
	if got := rows[0]["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled by the instrument exponent: got %v", got)
	}
}

// A socket reconnect must NOT reset the snapshot writer. OnDisconnect clears
// in-flight shadows because a half-built shadow spans the break, but live books
// stay valid and keep being served, so pending dirty entries still point at real
// state. Resetting here would discard queued writes for books that never changed.
// This asserts a deliberate absence, which is exactly the kind of decision that
// regresses silently when someone later "tidies up" the disconnect path.
func TestOnDisconnect_DoesNotResetSnapshotWriter(t *testing.T) {
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		fn(s.instruments[k])
	})
	c := NewCoordinator(context.Background(), []*Shard{s}, m)

	s.sw.MarkDirty(instKey{0, 11})
	c.OnDisconnect()

	s.sw.mu.Lock()
	n := len(s.sw.dirty)
	s.sw.mu.Unlock()
	if n != 1 {
		t.Errorf("a disconnect must leave queued snapshot work intact, got %d entries", n)
	}
}

// Snapshot levels are captured for replay even when the snapshot was declined.
func TestHandle_CapturesWireLevelsForDeclinedSnapshot(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) })

	s.handle(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	s.handle(snapBeginRec(11, 4, 2, 100, 0, 9999)) // declined: K == tracker
	if inst.OpenSnapshot != nil {
		t.Fatal("setup: the snapshot should have been declined")
	}
	s.handle(snapLevelRec(11, 4, "bid", 1000, 5))

	if got := len(st.rows["wire_levels"]); got != 1 {
		t.Errorf("a declined snapshot's levels must still be captured, got %d rows", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go/marketbyprice-bot && go test -run TestHandle_ ./...`
Expected: FAIL to build — `NewShard` takes 3 arguments, `s.sw undefined`.

- [ ] **Step 3: Add the writer fields to Shard**

In `shard.go`, add to the `Shard` struct after `metrics *Metrics`:

```go
	eventsW *EventsWriter
	sw      *SnapshotWriter
```

Change the constructor signature and body:

```go
func NewShard(idx, n int, eventsW *EventsWriter, metrics *Metrics) *Shard {
	s := &Shard{
		idx: idx, n: n,
		instruments: map[instKey]*Instrument{},
		refdata:     map[instKey]InstrumentDef{},
		deltaBuf:    map[instKey][]BufferedDelta{},
		maxBuffered: maxBufferedDeltasPerShard,
		touched:     map[instKey]struct{}{},
		crossed:     map[instKey]struct{}{},
		inbox:       make(chan shardMsg, 4096),
		metrics:     metrics,
		eventsW:     eventsW,
	}
	if metrics != nil {
		lbl := strconv.Itoa(idx)
		s.crossedGauge = metrics.CrossedInstruments.WithLabelValues(lbl)
		s.bufferedGauge = metrics.DeltaBufferedRecords.WithLabelValues(lbl)
	}
	return s
}
```

`sw` is assigned after construction: its `withInstrument` closure needs the shard
that is being built.

Every existing `NewShard(i, n, m)` call in tests becomes
`NewShard(i, n, NewEventsWriter(nil), m)`. Update them mechanically:

```bash
cd go/marketbyprice-bot
perl -pi -e 's/NewShard\((\w+), (\w+), (nil|m|NewMetrics\([^)]*\))\)/NewShard($1, $2, NewEventsWriter(nil), $3)/g' *_test.go
```

- [ ] **Step 4: Replace `_ = evs` in handle**

In `dispatch.go`, replace `handle`:

```go
// handle is the shard goroutine's per-record entry point.
func (s *Shard) handle(rec Record) {
	evs := s.apply(rec)
	if len(evs) == 0 {
		return
	}
	k := instKey{rec.ChannelID, rec.InstrumentID}
	def := s.refdataFor(k)
	for _, ev := range evs {
		s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)
		// ONLY a real book mutation dirties an instrument. Dirtying on a
		// non-mutating kind would rewrite an unchanged book on every batch
		// boundary — which is why PR 3's review gave those paths their own kinds.
		if ev.Kind == KindAppliedDelta || ev.Kind == KindAppliedSnapshot {
			s.sw.MarkDirty(instKey{rec.ChannelID, ev.InstrumentID})
		}
	}
}

// refdataFor returns the instrument's definition, or a zero value when the
// definition has not arrived yet. Taken under the shard lock.
func (s *Shard) refdataFor(k instKey) InstrumentDef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refdata[k]
}
```

Add a nil guard to `MarkDirty` so a shard without a writer is safe:

```go
func (w *SnapshotWriter) MarkDirty(k instKey) {
	if w == nil {
		return
	}
	...
```

- [ ] **Step 5: Capture wire levels in applySnapshotLevel**

In `dispatch.go`, in `applySnapshotLevel`, after the `res := inst.AddSnapshotLevel(...)`
call and before the mismatch counter, add:

```go
	// Capture for replay regardless of whether the level joined a shadow: a
	// declined snapshot is the steady-state case and its levels still describe
	// the publisher's book.
	if inst.LastBegin != nil {
		def := s.refdata[k]
		s.eventsW.WriteWireLevel(rec, k.ch, *inst.LastBegin, def.Symbol, def.PriceExponent, def.QtyExponent)
	}
```

`applySnapshotLevel` already runs under `s.mu` via `apply`, so read `s.refdata`
directly here — do NOT call `refdataFor`, which would deadlock on the same mutex.

- [ ] **Step 6: Reset the snapshot writer on a channel reset**

In `dispatch.go`, in `Run`'s `msgReset` case, after `s.reset()` and the mutex
unlock, before the ack:

```go
			case msgReset:
				s.mu.Lock()
				s.reset()
				s.mu.Unlock()
				// Drop queued snapshot work for books that no longer exist, and
				// bump the generation so a batch already extracted is abandoned
				// rather than written against post-reset state.
				s.sw.Reset(ctx)
				select {
				case msg.ack <- s.idx:
				case <-ctx.Done():
					return
				}
```

Add a nil guard to `Reset` matching `MarkDirty`:

```go
func (w *SnapshotWriter) Reset(ctx context.Context) {
	if w == nil {
		return
	}
	...
```

- [ ] **Step 7: Wire main.go**

First delete the inert block at `main.go:47-52`. Its comment says these flags
"only [have] a consumer once the persistence layer lands in the follow-on plan" —
this is that plan, and `--depth` becomes live here via `NewSnapshotWriter`.
Remove all six lines including the comment:

```go
	// --symbol and --depth configure the level read-out (ComputeLevels), which
	// only has a consumer once the persistence layer lands in the follow-on plan.
	// They are accepted now so deployment configs do not need to change then;
	// until then they have no effect. See README.
	_ = symbolFilter
	_ = depth
```

`_ = symbolFilter` stays for now and is removed in Task 8; `_ = depth` goes here.
Keeping a `_ =` for a flag that IS used will not compile.

Then add flags beside the existing ones:

```go
		clickhouseURL = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (empty = persistence disabled)")
		clickhouseDB  = flag.String("clickhouse-database", "marketbyprice", "ClickHouse database")
		batchSize     = flag.Int("clickhouse-batch-size", 500, "rows per insert batch")
		batchInterval = flag.Duration("clickhouse-batch-interval", time.Second, "maximum time between insert batches")
		bufferSize    = flag.Int("clickhouse-buffer-size", 20000, "per-table row buffer; rows are dropped when full")
		coalesceMS    = flag.Int("coalesce-ms", 50, "minimum interval between level_snapshots writes per instrument")
```

Replace the shard construction loop with:

```go
	ch, err := newClickhouseClient(*clickhouseURL, *clickhouseDB, *batchSize, *batchInterval, *bufferSize, metrics)
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	eventsWriter := NewEventsWriter(ch)

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, eventsWriter, metrics)
		// The writer's withInstrument closure needs the shard, so sw is assigned
		// after construction.
		s.sw = NewSnapshotWriter(ch, *depth, *coalesceMS, metrics, func(s *Shard) func(instKey, func(*Instrument)) {
			return func(k instKey, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[k])
			}
		}(s))
		shardList[i] = s
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}

	if ch != nil {
		go ch.Run(ctx)
	}
```

`NewEventsWriter(ch)` with a nil `ch` is correct: `*clickhouse.Client` is nil-safe
and `EventsWriter.Write` guards on it, so persistence-disabled needs no branch.

Note `ch` is `*clickhouse.Client`, and passing it where an `enqueuer` is expected
works because the nil-safe methods have pointer receivers. Import
`"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"` and `"time"`.

- [ ] **Step 8: Run the full suite**

```bash
cd go/marketbyprice-bot
go vet ./...
go test -race -count=1 ./...
GOWORK=off go build -o /tmp/dz-mbp-d .
```

Expected: all PASS.

- [ ] **Step 9: End-to-end verification against a live ClickHouse**

```bash
docker run -d --name ch-probe -p 8123:8123 clickhouse/clickhouse-server:latest
sleep 10
docker exec -i ch-probe clickhouse-client --multiquery < demo/clickhouse/init/03_schema_mbp.sql
```

Then run the bot against a parser socket with
`--clickhouse-url=http://localhost:8123`, and confirm rows land:

```bash
docker exec ch-probe clickhouse-client --query \
  "SELECT table, count() FROM (
     SELECT 'events' AS table FROM marketbyprice.events
     UNION ALL SELECT 'level_snapshots' FROM marketbyprice.level_snapshots
     UNION ALL SELECT 'wire_levels' FROM marketbyprice.wire_levels
     UNION ALL SELECT 'instruments' FROM marketbyprice.instruments
     UNION ALL SELECT 'channel_health' FROM marketbyprice.channel_health
   ) GROUP BY table"
docker rm -f ch-probe
```

Expected: non-zero counts for every table the feed exercises. Record the numbers
for the PR's Testing Verification section. If no live feed is available, say so
explicitly in the PR rather than implying this ran.

- [ ] **Step 10: Commit**

```bash
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: wire persistence into the shard and entry point"
```

---

## Task 8: `--symbol` gating and README

**Files:**
- Modify: `go/marketbyprice-bot/main.go`
- Modify: `go/marketbyprice-bot/shard.go` (symbol set on the shard)
- Modify: `go/marketbyprice-bot/dispatch.go` (gate the writes)
- Modify: `go/marketbyprice-bot/README.md`
- Test: `go/marketbyprice-bot/shard_test.go`

**Interfaces:**
- Consumes: everything from Tasks 4-7.
- Produces: `Shard.symbols map[string]struct{}` (nil means no filter); `(*Shard).persists(symbol string) bool`; `parseSymbolFilter(csv string) map[string]struct{}`.
- **Changes an existing signature:** `NewSnapshotWriter` gains a sixth parameter, `persists func(symbol string) bool`, after `withInstrument`. Update every call site — `main.go` passes `s.persists`; the `newTestSnapshotWriter` helper in `snapshot_writer_test.go` and the direct calls in `shard_test.go` (added by Task 7) all pass `nil`. Find them with `grep -rn 'NewSnapshotWriter(' go/marketbyprice-bot/`. A nil value means no filtering, so Tasks 6 and 7 tests keep passing unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `go/marketbyprice-bot/shard_test.go`:

```go
func TestParseSymbolFilter(t *testing.T) {
	if got := parseSymbolFilter(""); got != nil {
		t.Errorf("an empty filter means no filter, got %v", got)
	}
	got := parseSymbolFilter(" BTC-USDT , ETH-USDT ")
	if len(got) != 2 {
		t.Fatalf("expected 2 symbols, got %v", got)
	}
	if _, ok := got["BTC-USDT"]; !ok {
		t.Error("BTC-USDT missing; entries must be trimmed")
	}
}

// A filtered symbol must be absent from every table, while the book engine still
// applies its deltas — sequencing and gap detection are only correct if every
// record is processed.
func TestSymbolFilter_GatesPersistenceNotTheEngine(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.symbols = parseSymbolFilter("WANTED")
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) })

	// An instrument that is NOT in the filter.
	s.handle(instDefRec(11, "IGNORED", 1))
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 1000, 50))

	if got := len(st.rows["events"]); got != 0 {
		t.Errorf("a filtered symbol must not be persisted, got %d event rows", got)
	}
	if got := len(st.rows["instruments"]); got != 0 {
		t.Errorf("a filtered symbol's definition must not be persisted, got %d rows", got)
	}

	// The book engine must still have applied it.
	inst := s.instruments[instKey{0, 11}]
	if inst.LastAppliedInstrumentSeq != 1 {
		t.Errorf("the engine must still apply filtered instruments: seq %d", inst.LastAppliedInstrumentSeq)
	}
	if inst.Bids[1000] == nil {
		t.Error("the book must still be maintained for a filtered instrument")
	}

	// A wanted symbol still persists.
	s.handle(instDefRec(12, "WANTED", 1))
	s.instruments[instKey{0, 12}].Status = StatusReady
	s.handle(levelUpdateRec(12, 901, 1, "bid", 1000, 50))
	if got := len(st.rows["events"]); got != 1 {
		t.Errorf("an unfiltered symbol must persist, got %d event rows", got)
	}
}

// Channel-scoped records carry no symbol and must never be filtered out.
func TestSymbolFilter_KeepsChannelScopedRecords(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.symbols = parseSymbolFilter("WANTED")
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) })

	s.handle(Record{Type: "heartbeat", Port: "mktdata", Fields: map[string]any{}})

	if got := len(st.rows["channel_health"]); got != 1 {
		t.Errorf("channel health must not be symbol-filtered, got %d rows", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go/marketbyprice-bot && go test -run TestSymbolFilter ./...`
Expected: FAIL to build — `undefined: parseSymbolFilter`, `s.symbols undefined`.

- [ ] **Step 3: Implement the filter**

In `shard.go`, add to the `Shard` struct:

```go
	// symbols gates PERSISTENCE and read-out only. Nil means no filter. The book
	// engine always processes every instrument: sequencing, gap detection and the
	// delta buffer are only correct if every record is applied.
	symbols map[string]struct{}
```

Add, in `shard.go`:

```go
// parseSymbolFilter turns a comma-separated list into a lookup set. An empty
// string means no filter, represented as a nil map.
func parseSymbolFilter(csv string) map[string]struct{} {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out[s] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// persists reports whether rows for this symbol should be written. An empty
// symbol belongs to a channel-scoped record, which is never filtered.
func (s *Shard) persists(symbol string) bool {
	if s.symbols == nil || symbol == "" {
		return true
	}
	_, ok := s.symbols[symbol]
	return ok
}
```

Add `"strings"` to `shard.go`'s imports.

- [ ] **Step 4: Gate the write sites**

In `dispatch.go`'s `handle`, wrap the writer calls:

```go
	def := s.refdataFor(k)
	persist := s.persists(def.Symbol)
	for _, ev := range evs {
		if persist {
			s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)
		}
		if ev.Kind == KindAppliedDelta || ev.Kind == KindAppliedSnapshot {
			s.sw.MarkDirty(instKey{rec.ChannelID, ev.InstrumentID})
		}
	}
```

`MarkDirty` stays outside the gate; the snapshot writer applies the filter itself
at flush time, so a filtered instrument's dirty entry is dropped there rather
than leaving stale gauge series behind.

In `applySnapshotLevel`, gate the capture:

```go
	if inst.LastBegin != nil {
		def := s.refdata[k]
		if s.persists(def.Symbol) {
			s.eventsW.WriteWireLevel(rec, k.ch, *inst.LastBegin, def.Symbol, def.PriceExponent, def.QtyExponent)
		}
	}
```

In `snapshot_writer.go`, add a `persists` hook so the writer can apply the same
filter. Add the field and parameter:

```go
	// persists reports whether an instrument's rows should be written. Supplied
	// by the owning shard so the symbol filter applies identically to snapshots.
	persists func(symbol string) bool
```

Set it in `NewSnapshotWriter` via a new parameter
`persists func(symbol string) bool`, defaulting to always-true when nil:

```go
	if persists == nil {
		persists = func(string) bool { return true }
	}
```

And in `flushDue`, after `if !servable { continue }`:

```go
		if !w.persists(snap.Symbol) {
			continue
		}
```

Update `NewSnapshotWriter`'s call sites: in `main.go` pass `s.persists`, and in
tests pass `nil`.

- [ ] **Step 5: Wire main.go**

Replace `_ = symbolFilter` with, inside the shard loop:

```go
		s.symbols = parseSymbolFilter(*symbolFilter)
```

placed immediately after `s := NewShard(...)` and before `s.sw = ...`, so
`s.persists` closes over a populated set by the time the writer captures it.

Read-out needs no separate gate. `ComputeLevels` has exactly one consumer — the
snapshot writer's `flushDue` — and Step 4 already filters there. The original
`main.go` comment described `--symbol` and `--depth` as configuring "the level
read-out (ComputeLevels)", and that read-out is the snapshot writer. There is no
printing path to gate.

- [ ] **Step 6: Run the full suite**

```bash
cd go/marketbyprice-bot
go vet ./...
go test -race -count=1 ./...
```

Expected: PASS.

- [ ] **Step 7: Update the README**

In `go/marketbyprice-bot/README.md`:

- Replace the "persistence is not yet implemented" statement with a Persistence
  section: the five tables, that an empty `--clickhouse-url` disables writing,
  and that a write failure is counted and dropped rather than affecting the feed.
- Document the new flags: `--clickhouse-url`, `--clickhouse-database`,
  `--clickhouse-batch-size`, `--clickhouse-batch-interval`,
  `--clickhouse-buffer-size`, `--coalesce-ms`.
- Document that `--symbol` gates persistence and read-out but never the book
  engine, and why.
- Add the new metrics to the metric table, one line each:
  `clickhouse_rows_written_total{table}`,
  `clickhouse_rows_dropped_total{table,reason}`,
  `clickhouse_write_errors_total{table,reason}`,
  `clickhouse_batch_duration_seconds{table}`, `clickhouse_buffered_rows{table}`,
  `snapshot_writes_total`, `snapshot_coalesces_total`, `snapshot_lag_ms`,
  `book_levels{symbol,side}`, `book_top_price{symbol,side}`,
  `book_top_qty{symbol,side}`, `book_spread_bps{symbol}`.
- State plainly that `cumulative_qty` is exhaustive only when `depth_bound` is 0.

- [ ] **Step 8: Commit**

```bash
gofmt -l go/marketbyprice-bot/
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: gate persistence and read-out by symbol"
```

---

## Done criteria

- All five tables receive rows from a live or replayed feed, verified by query.
- `go test -race` clean across `go/internal/` and `go/marketbyprice-bot/`.
- `go vet` and `gofmt` clean for both modules.
- `GOWORK=off go build` succeeds for darwin and linux.
- `docker build -f go/marketbyprice-bot/Dockerfile .` succeeds, proving the
  `COPY go/internal/` addition.
- An empty `--clickhouse-url` runs the bot exactly as before, with no writes and
  no errors.
- `--symbol` excludes filtered symbols from every table while their books stay
  correctly sequenced.
- No metric is registered without a writer populating it.

## PR description notes

Flag the size: roughly 780 non-test lines against the repository's ~500
guideline. The parent spec anticipated this and rejected splitting, because the
seam would land writers before the schema they write into. Say so explicitly
rather than leaving a reviewer to wonder.

Link both the parent spec and the persistence design spec.

## Follow-on work (not this plan)

- Migrate `marketbyorder-bot` and `topofbook-bot` onto `go/internal/clickhouse`,
  reconciling their divergent client designs. File this as an issue when PR 4
  lands.
- PR 5, the demo stack, remains blocked on the live feed's multicast group,
  market-by-price port sets, and channel ID.
