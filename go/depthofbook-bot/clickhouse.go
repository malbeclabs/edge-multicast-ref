package main

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

// ClickhouseClient writes rows to a ClickHouse server using HTTP JSONEachRow inserts.
// One Batcher per table, each with its own goroutine and buffer.
type ClickhouseClient struct {
	url      string
	database string
	hc       *http.Client
	metrics  *Metrics
	batchers map[string]*Batcher
}

// BatcherConfig controls one table's batcher.
type BatcherConfig struct {
	Table         string
	BatchSize     int
	BatchInterval time.Duration
	BufferSize    int
}

// NewClickhouseClient returns a configured client. table configs are created
// up-front; call Enqueue(table, row) to push rows.
func NewClickhouseClient(rawURL, database string, configs []BatcherConfig, metrics *Metrics) (*ClickhouseClient, error) {
	if rawURL == "" {
		return nil, nil // disabled
	}
	if _, err := url.Parse(rawURL); err != nil {
		return nil, fmt.Errorf("clickhouse url: %w", err)
	}
	c := &ClickhouseClient{
		url:      strings.TrimRight(rawURL, "/"),
		database: database,
		hc:       &http.Client{Timeout: 30 * time.Second},
		metrics:  metrics,
		batchers: map[string]*Batcher{},
	}
	for _, cfg := range configs {
		c.batchers[cfg.Table] = newBatcher(c, cfg)
	}
	return c, nil
}

// Run starts all batcher goroutines. Returns when ctx is cancelled and all batchers have flushed.
func (c *ClickhouseClient) Run(ctx context.Context) {
	if c == nil {
		return
	}
	var wg sync.WaitGroup
	for _, b := range c.batchers {
		wg.Add(1)
		go func(b *Batcher) {
			defer wg.Done()
			b.run(ctx)
		}(b)
	}
	wg.Wait()
}

// Enqueue queues a row for the named table. Returns false if dropped (buffer full or unknown table).
func (c *ClickhouseClient) Enqueue(table string, row map[string]any) bool {
	if c == nil {
		return false
	}
	b, ok := c.batchers[table]
	if !ok {
		return false
	}
	select {
	case b.ch <- row:
		c.metrics.ClickhouseBufferedRows.WithLabelValues(table).Set(float64(len(b.ch)))
		return true
	default:
		c.metrics.ClickhouseRowsDropped.WithLabelValues(table, "buffer_full").Inc()
		return false
	}
}

// Batcher is a per-table accumulator and flusher.
type Batcher struct {
	client *ClickhouseClient
	cfg    BatcherConfig
	ch     chan map[string]any
}

func newBatcher(c *ClickhouseClient, cfg BatcherConfig) *Batcher {
	return &Batcher{
		client: c,
		cfg:    cfg,
		ch:     make(chan map[string]any, cfg.BufferSize),
	}
}

func (b *Batcher) run(ctx context.Context) {
	buf := make([]map[string]any, 0, b.cfg.BatchSize)
	tick := time.NewTicker(b.cfg.BatchInterval)
	defer tick.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		start := time.Now()
		if err := b.send(ctx, buf); err != nil {
			b.client.metrics.ClickhouseWriteErrors.WithLabelValues(b.cfg.Table, classifyHTTPErr(err)).Inc()
			b.client.metrics.ClickhouseRowsDropped.WithLabelValues(b.cfg.Table, "write_failed").Add(float64(len(buf)))
			log.Printf("clickhouse %s: %v (dropped %d rows)", b.cfg.Table, err, len(buf))
		} else {
			b.client.metrics.ClickhouseRowsWritten.WithLabelValues(b.cfg.Table).Add(float64(len(buf)))
		}
		b.client.metrics.ClickhouseBatchDuration.WithLabelValues(b.cfg.Table).Observe(time.Since(start).Seconds())
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining items and flush before returning.
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
			b.client.metrics.ClickhouseBufferedRows.WithLabelValues(b.cfg.Table).Set(float64(len(b.ch)))
			if len(buf) >= b.cfg.BatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func (b *Batcher) send(ctx context.Context, rows []map[string]any) error {
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// chTime formats a time into ClickHouse's DateTime64(9) textual format.
// Default date_time_input_format=basic rejects RFC3339 with a Z suffix in
// JSONEachRow, so emit the native form ClickHouse echoes from now64() instead.
func chTime(t time.Time) string {
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
	case strings.HasPrefix(s, "http 4"):
		return "http_4xx"
	case strings.HasPrefix(s, "http 5"):
		return "http_5xx"
	default:
		return "other"
	}
}
