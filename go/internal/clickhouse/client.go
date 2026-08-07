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
