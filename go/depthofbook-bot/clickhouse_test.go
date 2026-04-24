package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickhouseBatcher_FlushesOnSize(t *testing.T) {
	var rowsReceived atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// Each JSON line ends with \n. Count lines.
		lines := int64(0)
		for _, b := range body {
			if b == '\n' {
				lines++
			}
		}
		rowsReceived.Add(lines)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 5, BatchInterval: 1 * time.Hour, BufferSize: 100}
	c, err := NewClickhouseClient(srv.URL, "depthofbook", []BatcherConfig{cfg}, metrics)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 5; i++ {
		c.Enqueue("events", map[string]any{"row": i})
	}
	time.Sleep(200 * time.Millisecond)

	if got := rowsReceived.Load(); got != 5 {
		t.Errorf("expected 5 rows received, got %d", got)
	}
}

func TestClickhouseBatcher_FlushesOnInterval(t *testing.T) {
	var rowsReceived atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lines := int64(0)
		for _, b := range body {
			if b == '\n' {
				lines++
			}
		}
		rowsReceived.Add(lines)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 1000, BatchInterval: 100 * time.Millisecond, BufferSize: 100}
	c, _ := NewClickhouseClient(srv.URL, "depthofbook", []BatcherConfig{cfg}, metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 3; i++ {
		c.Enqueue("events", map[string]any{"row": i})
	}
	time.Sleep(300 * time.Millisecond)

	if got := rowsReceived.Load(); got != 3 {
		t.Errorf("expected 3 rows after interval flush, got %d", got)
	}
}

func TestClickhouseBatcher_DropsOnBufferFull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Hour) // never respond
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 5, BatchInterval: 1 * time.Hour, BufferSize: 3}
	c, _ := NewClickhouseClient(srv.URL, "depthofbook", []BatcherConfig{cfg}, metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	dropped := 0
	for i := 0; i < 10; i++ {
		if !c.Enqueue("events", map[string]any{"row": i}) {
			dropped++
		}
	}
	if dropped == 0 {
		t.Error("expected some rows to be dropped on buffer full")
	}
}
