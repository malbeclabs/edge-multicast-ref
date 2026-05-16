package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestBuildInsertURL(t *testing.T) {
	got, err := buildInsertURL("http://ch:8123", "topofbook", "quotes")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	// Verify it's a well-formed URL with the expected query parts.
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Host != "ch:8123" {
		t.Errorf("host = %q", u.Host)
	}
	q := u.Query()
	if q.Get("database") != "topofbook" {
		t.Errorf("database = %q", q.Get("database"))
	}
	if q.Get("query") != "INSERT INTO quotes FORMAT JSONEachRow" {
		t.Errorf("query = %q", q.Get("query"))
	}
}

func TestChTime_Format(t *testing.T) {
	ts := time.Date(2026, 4, 19, 18, 0, 0, 123456789, time.UTC)
	got := chTime(ts)
	want := "2026-04-19 18:00:00.123456789"
	if got != want {
		t.Errorf("chTime = %q, want %q", got, want)
	}
	if chTime(time.Time{}) != "" {
		t.Errorf("chTime(zero) should return empty string")
	}
}

func TestChWriter_BatchAndPost(t *testing.T) {
	// Record received POSTs per table.
	type received struct {
		table string
		body  string
	}
	var (
		mu        sync.Mutex
		gotPosts  []received
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("query")
		table := ""
		switch {
		case strings.Contains(q, "INSERT INTO quotes"):
			table = "quotes"
		case strings.Contains(q, "INSERT INTO trades"):
			table = "trades"
		case strings.Contains(q, "INSERT INTO instruments"):
			table = "instruments"
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotPosts = append(gotPosts, received{table: table, body: string(body)})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	m := newMetrics()
	cfg := DefaultClickHouseConfig()
	cfg.URL = srv.URL
	cfg.BatchSize = 2
	cfg.BatchInterval = 50 * time.Millisecond

	w, err := newChWriter(cfg, m)
	if err != nil {
		t.Fatalf("newChWriter: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	now := time.Date(2026, 4, 19, 18, 0, 0, 0, time.UTC)
	sendTS := now.Add(-100 * time.Millisecond)

	// Enqueue 2 quotes — should flush on size.
	for i := 0; i < 2; i++ {
		rec := &Record{
			Type: "quote", Timestamp: sendTS, ChannelID: 0, SequenceNumber: uint64(i),
			InstrumentID: 1, Symbol: "BTC",
			Fields: map[string]any{
				"bid_price": 67000.0, "bid_qty": 1.0,
				"ask_price": 67001.0, "ask_qty": 0.5,
				"source_id": 1.0,
			},
		}
		w.EnqueueQuote(rec, now)
	}

	// Enqueue 1 trade — should flush on interval.
	rec := &Record{
		Type: "trade", Timestamp: sendTS, ChannelID: 0, SequenceNumber: 42,
		InstrumentID: 1, Symbol: "BTC",
		Fields: map[string]any{
			"trade_price":       67000.5,
			"trade_qty":         0.25,
			"aggressor_side":    "buy",
			"trade_id":          float64(999),
			"cumulative_volume": 12.34,
			"source_id":         1.0,
		},
	}
	w.EnqueueTrade(rec, now)

	// Wait long enough for both size-flush and interval-flush to land.
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		count := len(gotPosts)
		mu.Unlock()
		if count >= 2 {
			break
		}
		select {
		case <-deadline:
			mu.Lock()
			t.Fatalf("only %d POSTs received; wanted 2", len(gotPosts))
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()

	// Verify we got a quotes POST with 2 rows and a trades POST with 1 row.
	var quotesRows, tradesRows int
	for _, p := range gotPosts {
		rows := countNewlines(p.body)
		switch p.table {
		case "quotes":
			quotesRows += rows
		case "trades":
			tradesRows += rows
		}
	}
	if quotesRows != 2 {
		t.Errorf("quotes rows = %d, want 2", quotesRows)
	}
	if tradesRows != 1 {
		t.Errorf("trades rows = %d, want 1", tradesRows)
	}

	// Spot-check one quote row's shape.
	for _, p := range gotPosts {
		if p.table != "quotes" {
			continue
		}
		// body is JSONL; parse first line.
		line := strings.SplitN(p.body, "\n", 2)[0]
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("quote row decode: %v", err)
		}
		if row["symbol"] != "BTC" {
			t.Errorf("row symbol = %v", row["symbol"])
		}
		if row["bid_price"] != 67000.0 {
			t.Errorf("row bid_price = %v", row["bid_price"])
		}
		if row["recv_ts"] == "" {
			t.Errorf("row recv_ts missing")
		}
		break
	}
}

func TestChWriter_BufferFullDropsRows(t *testing.T) {
	// Never-responding server: writer's inflight POST blocks, buffer fills up.
	// Teardown order matters: we must unblock the handler BEFORE srv.Close()
	// runs, or srv.Close() deadlocks waiting for the in-flight request.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	t.Cleanup(func() {
		close(block) // unblock handler first
		srv.Close()  // now srv.Close() can drain
	})

	m := newMetrics()
	cfg := DefaultClickHouseConfig()
	cfg.URL = srv.URL
	cfg.BatchSize = 1
	cfg.BatchInterval = 24 * time.Hour // disable interval flush
	cfg.BufferSize = 2                 // tiny buffer to trigger drop
	cfg.HTTPTimeout = 500 * time.Millisecond

	w, err := newChWriter(cfg, m)
	if err != nil {
		t.Fatalf("newChWriter: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	rec := &Record{
		Type: "quote", Timestamp: time.Now(), Symbol: "BTC",
		Fields: map[string]any{"bid_price": 1.0},
	}

	// Submit far more rows than the buffer can hold.
	for i := 0; i < 50; i++ {
		w.EnqueueQuote(rec, time.Now())
	}

	// Give the batcher a tick to consume into its POST, then drops should register.
	time.Sleep(50 * time.Millisecond)

	dropped := readCounter(t, m.chRowsDropped, "table", "quotes", "reason", "buffer_full")
	if dropped < 1 {
		t.Errorf("expected at least 1 drop; got %v", dropped)
	}
}

func countNewlines(s string) int {
	return strings.Count(s, "\n")
}
