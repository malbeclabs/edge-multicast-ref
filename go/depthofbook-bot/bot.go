package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics is the subset used by Bot. Task 12 will replace this stub with the full implementation.
type Metrics struct {
	SocketConnected    prometheus.Gauge
	SocketReconnects   *prometheus.CounterVec
	RecordsTotal       *prometheus.CounterVec
	DecodeErrors       prometheus.Counter
	SocketToBotLatency *prometheus.HistogramVec
}

// Dispatcher is called for every successfully decoded Record.
// Implementations MUST be fast (don't block the read loop).
type Dispatcher interface {
	Dispatch(rec Record)
}

// Bot reads JSONL Records from a parser Unix socket and dispatches them.
// Reconnects with exponential backoff on disconnect.
type Bot struct {
	socketPath string
	dispatcher Dispatcher
	metrics    *Metrics
}

func NewBot(socketPath string, dispatcher Dispatcher, metrics *Metrics) *Bot {
	return &Bot{socketPath: socketPath, dispatcher: dispatcher, metrics: metrics}
}

// Run reads from the socket until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := net.Dial("unix", b.socketPath)
		if err != nil {
			b.metrics.SocketReconnects.WithLabelValues("dial_failed").Inc()
			log.Printf("dial %s: %v (retry in %v)", b.socketPath, err, backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		b.metrics.SocketConnected.Set(1)
		log.Printf("connected to %s", b.socketPath)
		backoff = 250 * time.Millisecond // reset on success

		reason := b.read(ctx, conn)
		_ = conn.Close()
		b.metrics.SocketConnected.Set(0)
		b.metrics.SocketReconnects.WithLabelValues(reason).Inc()

		if ctx.Err() != nil {
			return
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func (b *Bot) read(ctx context.Context, conn net.Conn) string {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // up to 1 MiB lines

	for scanner.Scan() {
		if ctx.Err() != nil {
			return "shutdown"
		}
		line := scanner.Bytes()

		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			b.metrics.DecodeErrors.Inc()
			continue
		}

		b.metrics.RecordsTotal.WithLabelValues(rec.Type).Inc()
		b.metrics.SocketToBotLatency.WithLabelValues(rec.Type).Observe(
			time.Since(rec.Timestamp).Seconds())
		b.dispatcher.Dispatch(rec)
	}

	err := scanner.Err()
	if err == nil || errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, net.ErrClosed) {
		return "shutdown"
	}
	return "read_error"
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	return next
}

// encodeRecord produces a JSON-encoded Record line for tests.
func encodeRecord(r Record) string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%s\n", b)
}
