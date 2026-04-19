package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"
)

// filter is a set of symbols to subscribe to. Empty filter = pass-through.
// Symbols are compared case-sensitively; the parser emits symbols as the
// publisher defined them, so treat them as opaque identifiers.
type filter struct {
	symbols map[string]struct{}
}

func newFilter(csv string) filter {
	f := filter{symbols: map[string]struct{}{}}
	for _, s := range strings.Split(csv, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			f.symbols[s] = struct{}{}
		}
	}
	return f
}

func (f filter) allow(symbol string) bool {
	if len(f.symbols) == 0 {
		return true
	}
	_, ok := f.symbols[symbol]
	return ok
}

func (f filter) list() []string {
	out := make([]string, 0, len(f.symbols))
	for s := range f.symbols {
		out = append(out, s)
	}
	return out
}

// Bot connects to the topofbook-parser's Unix socket, decodes JSON Lines
// records, filters by symbol, and emits Prometheus metrics.
type Bot struct {
	sockPath string
	filter   filter
	metrics  *metrics

	// reconnect policy
	initialBackoff time.Duration
	maxBackoff     time.Duration
}

func NewBot(sockPath string, filter filter, m *metrics) *Bot {
	return &Bot{
		sockPath:       sockPath,
		filter:         filter,
		metrics:        m,
		initialBackoff: 250 * time.Millisecond,
		maxBackoff:     5 * time.Second,
	}
}

// Run loops forever: dial the socket, stream records until EOF/error, back
// off and reconnect. Returns when ctx is cancelled.
func (b *Bot) Run(ctx context.Context) error {
	backoff := b.initialBackoff

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		slog.Info("connecting to parser socket", "path", b.sockPath)
		conn, err := dialUnixWithCtx(ctx, b.sockPath)
		if err != nil {
			slog.Warn("socket connect failed", "error", err, "retry_in", backoff)
			b.metrics.reconnects.WithLabelValues("dial_failed").Inc()
			if sleepCtx(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff, b.maxBackoff)
			continue
		}

		b.metrics.connected.Set(1)
		slog.Info("socket connected")
		backoff = b.initialBackoff // reset after a successful connect

		err = b.readLoop(ctx, conn)
		b.metrics.connected.Set(0)
		conn.Close()

		if ctx.Err() != nil {
			return nil
		}

		reason := "eof"
		if err != nil && !errors.Is(err, io.EOF) {
			reason = "read_error"
			slog.Warn("socket read error", "error", err)
		}
		b.metrics.reconnects.WithLabelValues(reason).Inc()

		if sleepCtx(ctx, backoff) {
			return nil
		}
		backoff = nextBackoff(backoff, b.maxBackoff)
	}
}

// readLoop decodes JSON lines from conn until EOF or error.
func (b *Bot) readLoop(ctx context.Context, conn net.Conn) error {
	// Buffered reader — JSON lines can be up to a few KB for wide trade records.
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for sc.Scan() {
		if ctx.Err() != nil {
			return nil
		}
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}

		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			b.metrics.decodeError.Inc()
			slog.Debug("decode error", "error", err, "line_len", len(line))
			continue
		}

		b.handle(&rec)
	}
	return sc.Err()
}

// handle applies the filter and updates metrics for a single record.
func (b *Bot) handle(rec *Record) {
	// Drop records for unsubscribed symbols.
	// Per-type records without a symbol (heartbeat, channel_reset, manifest_summary)
	// are always counted but never update per-symbol gauges.
	if rec.Symbol != "" && !b.filter.allow(rec.Symbol) {
		b.metrics.dropped.WithLabelValues("filter").Inc()
		return
	}

	b.metrics.records.WithLabelValues(rec.Type).Inc()

	if !rec.Timestamp.IsZero() {
		lat := time.Since(rec.Timestamp).Seconds()
		if lat >= 0 {
			b.metrics.latency.WithLabelValues(rec.Type).Observe(lat)
		}
	}

	// Per-symbol gauges — only for records that carry a symbol.
	if rec.Symbol == "" {
		return
	}

	switch rec.Type {
	case "quote":
		if bp, ok := rec.bidPrice(); ok {
			b.metrics.bidPrice.WithLabelValues(rec.Symbol).Set(bp)
		}
		if ap, ok := rec.askPrice(); ok {
			b.metrics.askPrice.WithLabelValues(rec.Symbol).Set(ap)
		}
		if bq, ok := rec.bidQty(); ok {
			b.metrics.bidQty.WithLabelValues(rec.Symbol).Set(bq)
		}
		if aq, ok := rec.askQty(); ok {
			b.metrics.askQty.WithLabelValues(rec.Symbol).Set(aq)
		}
		bp, hasBid := rec.bidPrice()
		ap, hasAsk := rec.askPrice()
		if hasBid && hasAsk && bp > 0 && ap > 0 {
			spread := ap - bp
			b.metrics.spread.WithLabelValues(rec.Symbol).Set(spread)
			mid := (ap + bp) / 2
			if mid > 0 {
				b.metrics.spreadBps.WithLabelValues(rec.Symbol).Set(spread / mid * 10000)
			}
		}
	case "trade":
		if p, ok := rec.tradePrice(); ok {
			b.metrics.lastTradePrice.WithLabelValues(rec.Symbol).Set(p)
		}
		if q, ok := rec.tradeQty(); ok {
			b.metrics.lastTradeQty.WithLabelValues(rec.Symbol).Set(q)
		}
	}

	b.metrics.lastUpdateTS.WithLabelValues(rec.Symbol).Set(float64(rec.Timestamp.UnixNano()) / 1e9)
}

// dialUnixWithCtx is net.Dialer-based so Dial respects ctx cancellation.
func dialUnixWithCtx(ctx context.Context, path string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "unix", path)
}

// sleepCtx sleeps for d or returns true if ctx was cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

// nextBackoff doubles current up to max.
func nextBackoff(cur, max time.Duration) time.Duration {
	n := cur * 2
	if n > max {
		return max
	}
	return n
}
