package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"
)

// ClickHouseConfig controls the optional ClickHouse tick-level writer.
// Zero-value URL disables the writer entirely.
type ClickHouseConfig struct {
	URL           string        // e.g. http://clickhouse:8123
	Database      string        // e.g. topofbook
	BatchSize     int           // flush when per-table accumulator hits this
	BatchInterval time.Duration // max time between flushes
	BufferSize    int           // per-table channel capacity; new rows dropped when full
	HTTPTimeout   time.Duration // per-request timeout
}

// DefaultClickHouseConfig gives sensible demo-scale defaults.
func DefaultClickHouseConfig() ClickHouseConfig {
	return ClickHouseConfig{
		Database:      "topofbook",
		BatchSize:     1000,
		BatchInterval: 200 * time.Millisecond,
		BufferSize:    100_000,
		HTTPTimeout:   5 * time.Second,
	}
}

// chWriter owns per-table batchers and exposes enqueue methods that the
// bot calls on every record. All enqueues are non-blocking: a full buffer
// drops the oldest-sent (actually just the new row, per Go channel
// semantics) and increments a drop counter.
type chWriter struct {
	cfg      ClickHouseConfig
	metrics  *metrics
	batchers map[string]*chBatcher
}

// newChWriter constructs but does not start the writer. Call Run on the
// returned object to begin the per-table flush loops.
func newChWriter(cfg ClickHouseConfig, m *metrics) (*chWriter, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("clickhouse URL required")
	}
	client := &http.Client{Timeout: cfg.HTTPTimeout}
	w := &chWriter{cfg: cfg, metrics: m, batchers: map[string]*chBatcher{}}
	for _, table := range []string{"quotes", "trades", "instruments"} {
		insertURL, err := buildInsertURL(cfg.URL, cfg.Database, table)
		if err != nil {
			return nil, err
		}
		w.batchers[table] = &chBatcher{
			name:     table,
			url:      insertURL,
			client:   client,
			in:       make(chan []byte, cfg.BufferSize),
			flushSz:  cfg.BatchSize,
			flushDur: cfg.BatchInterval,
			metrics:  m,
		}
	}
	return w, nil
}

// Run launches the flush loop for each table. Returns when ctx is cancelled
// and all in-flight batches have been sent.
func (w *chWriter) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range w.batchers {
		wg.Add(1)
		b := b
		go func() {
			defer wg.Done()
			b.run(ctx)
		}()
	}
	wg.Wait()
}

// EnqueueQuote serializes a quote record into the quotes batcher. Non-blocking.
func (w *chWriter) EnqueueQuote(rec *Record, recvTime time.Time) {
	row := map[string]any{
		"recv_ts":           chTime(recvTime),
		"publisher_send_ts": chTime(rec.Timestamp),
		"channel_id":        rec.ChannelID,
		"seq":               rec.SequenceNumber,
		"instrument_id":     rec.InstrumentID,
		"symbol":            rec.Symbol,
		"bid_price":         floatOrZero(rec, "bid_price"),
		"bid_qty":           floatOrZero(rec, "bid_qty"),
		"ask_price":         floatOrZero(rec, "ask_price"),
		"ask_qty":           floatOrZero(rec, "ask_qty"),
		"source_id":         uintOrZero(rec, "source_id"),
	}
	w.submit("quotes", row)
}

// EnqueueTrade serializes a trade record into the trades batcher.
func (w *chWriter) EnqueueTrade(rec *Record, recvTime time.Time) {
	side, _ := rec.aggressorSide()
	tid, _ := rec.tradeID()
	row := map[string]any{
		"recv_ts":           chTime(recvTime),
		"publisher_send_ts": chTime(rec.Timestamp),
		"channel_id":        rec.ChannelID,
		"seq":               rec.SequenceNumber,
		"instrument_id":     rec.InstrumentID,
		"symbol":            rec.Symbol,
		"price":             floatOrZero(rec, "trade_price"),
		"qty":               floatOrZero(rec, "trade_qty"),
		"cumulative_volume": floatOrZero(rec, "cumulative_volume"),
		"aggressor_side":    side,
		"trade_id":          tid,
		"source_id":         uintOrZero(rec, "source_id"),
	}
	w.submit("trades", row)
}

// EnqueueInstrument serializes an InstrumentDefinition into the instruments batcher.
func (w *chWriter) EnqueueInstrument(rec *Record, recvTime time.Time) {
	row := map[string]any{
		"recv_ts":        chTime(recvTime),
		"instrument_id":  rec.InstrumentID,
		"symbol":         rec.Symbol,
		"price_exponent": intOrZero(rec, "price_exponent"),
		"qty_exponent":   intOrZero(rec, "qty_exponent"),
	}
	w.submit("instruments", row)
}

// submit marshals and non-blockingly sends to the named batcher's channel.
// Marshal error drops the row with "marshal" reason; full buffer drops
// with "buffer_full" reason.
func (w *chWriter) submit(table string, row map[string]any) {
	b := w.batchers[table]
	if b == nil {
		return
	}
	data, err := json.Marshal(row)
	if err != nil {
		w.metrics.chRowsDropped.WithLabelValues(table, "marshal").Inc()
		return
	}
	select {
	case b.in <- data:
		w.metrics.chBufferedRows.WithLabelValues(table).Set(float64(len(b.in)))
	default:
		w.metrics.chRowsDropped.WithLabelValues(table, "buffer_full").Inc()
	}
}

// chBatcher accumulates JSON rows and flushes to ClickHouse via HTTP POST
// with FORMAT JSONEachRow. One per table.
type chBatcher struct {
	name     string
	url      string
	client   *http.Client
	in       chan []byte
	flushSz  int
	flushDur time.Duration
	metrics  *metrics
}

func (b *chBatcher) run(ctx context.Context) {
	var buf bytes.Buffer
	count := 0
	ticker := time.NewTicker(b.flushDur)
	defer ticker.Stop()

	flush := func() {
		if count == 0 {
			return
		}
		b.send(buf.Bytes(), count)
		buf.Reset()
		count = 0
		b.metrics.chBufferedRows.WithLabelValues(b.name).Set(float64(len(b.in)))
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining buffered rows, then exit.
			for drain := true; drain; {
				select {
				case line := <-b.in:
					buf.Write(line)
					buf.WriteByte('\n')
					count++
				default:
					drain = false
				}
			}
			flush()
			return
		case <-ticker.C:
			flush()
		case line := <-b.in:
			buf.Write(line)
			buf.WriteByte('\n')
			count++
			if count >= b.flushSz {
				flush()
			}
		}
	}
}

// send POSTs the batch body to ClickHouse and records outcome metrics.
func (b *chBatcher) send(body []byte, rows int) {
	start := time.Now()
	req, err := http.NewRequest(http.MethodPost, b.url, bytes.NewReader(body))
	if err != nil {
		b.metrics.chWriteErrors.WithLabelValues(b.name, "new_request").Inc()
		b.metrics.chRowsDropped.WithLabelValues(b.name, "new_request").Add(float64(rows))
		return
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := b.client.Do(req)
	elapsed := time.Since(start).Seconds()
	b.metrics.chBatchDuration.WithLabelValues(b.name).Observe(elapsed)

	if err != nil {
		b.metrics.chWriteErrors.WithLabelValues(b.name, "transport").Inc()
		b.metrics.chRowsDropped.WithLabelValues(b.name, "transport").Add(float64(rows))
		slog.Warn("clickhouse write failed", "table", b.name, "error", err, "rows", rows)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		reason := "http_" + strconv.Itoa(resp.StatusCode)
		b.metrics.chWriteErrors.WithLabelValues(b.name, reason).Inc()
		b.metrics.chRowsDropped.WithLabelValues(b.name, reason).Add(float64(rows))
		// Drain a short prefix of the response body for the log; ignore errors.
		errBuf := make([]byte, 512)
		n, _ := resp.Body.Read(errBuf)
		slog.Warn("clickhouse http error", "table", b.name, "status", resp.StatusCode,
			"rows", rows, "body_prefix", string(errBuf[:n]))
		return
	}

	b.metrics.chRowsWritten.WithLabelValues(b.name).Add(float64(rows))
}

// --- helpers ---

// buildInsertURL constructs a ClickHouse HTTP INSERT endpoint with the
// table baked into the query parameter.
func buildInsertURL(base, database, table string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("parsing clickhouse URL: %w", err)
	}
	q := u.Query()
	q.Set("database", database)
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", table))
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// chTime formats a time into ClickHouse's DateTime64(9) textual format.
// Uses a space separator (ClickHouse-native) rather than the T form so
// queries copy-paste-match what `now64()` returns in the UI.
func chTime(t time.Time) string {
	if t.IsZero() {
		// Let ClickHouse default to DEFAULT if we ever have a zero ts.
		return ""
	}
	return t.UTC().Format("2006-01-02 15:04:05.000000000")
}

func floatOrZero(rec *Record, key string) float64 {
	v, _ := floatField(rec, key)
	return v
}

func uintOrZero(rec *Record, key string) uint64 {
	f, ok := floatField(rec, key)
	if !ok {
		return 0
	}
	return uint64(f)
}

func intOrZero(rec *Record, key string) int64 {
	f, ok := floatField(rec, key)
	if !ok {
		return 0
	}
	return int64(f)
}
