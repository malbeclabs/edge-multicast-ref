package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"time"
)

// Dispatcher is called for every successfully decoded Record.
// Implementations MUST be fast (don't block the read loop).
type Dispatcher interface {
	Dispatch(rec Record)
}

// DisconnectAware is an optional Dispatcher capability. A dispatcher holding
// state that spans records — an open snapshot group, say — cannot tell that the
// stream was interrupted, because the reader simply resumes after reconnecting.
// Implementing this lets it drop what the break invalidated.
type DisconnectAware interface {
	OnDisconnect()
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

		var d net.Dialer
		conn, err := d.DialContext(ctx, "unix", b.socketPath)
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
		// Tell the dispatcher the stream broke, before any reconnect can feed it
		// records that its pre-drop state would misroute.
		if d, ok := b.dispatcher.(DisconnectAware); ok {
			d.OnDisconnect()
		}
		if ctx.Err() == nil {
			b.metrics.SocketReconnects.WithLabelValues(reason).Inc()
		}

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
		if len(line) == 0 {
			continue
		}

		// UseNumber, not a plain Unmarshal. Fields is map[string]any, so every
		// number would otherwise land as float64 and silently lose precision above
		// 2^53. price_raw is an int64 on the wire paired with an int8 exponent, so a
		// venue quoting with a small exponent reaches that range and the book would
		// be wrong with no error and no counter. json.Number keeps the literal text
		// for the coercion helpers to parse exactly.
		var rec Record
		dec := json.NewDecoder(bytes.NewReader(line))
		dec.UseNumber()
		if err := dec.Decode(&rec); err != nil {
			b.metrics.DecodeErrors.Inc()
			continue
		}

		b.metrics.RecordsTotal.WithLabelValues(rec.Type).Inc()
		if rec.SendTSNS != 0 {
			if lat := time.Since(rec.sendTime()).Seconds(); lat >= 0 {
				b.metrics.SocketToBotLatency.WithLabelValues(rec.Type).Observe(lat)
			}
		}
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

// sleepCtx sleeps for d or until ctx is cancelled.
// Returns true if the full duration elapsed; false if ctx was cancelled.
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
