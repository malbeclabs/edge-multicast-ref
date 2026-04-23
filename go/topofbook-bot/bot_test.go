package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestFilter_Empty_AllowsAll(t *testing.T) {
	f := newFilter("")
	if !f.allow("BTC") || !f.allow("ETH") {
		t.Fatal("empty filter must allow all symbols")
	}
}

func TestFilter_CSV_MatchesExact(t *testing.T) {
	f := newFilter("BTC,ETH, SOL")
	for _, sym := range []string{"BTC", "ETH", "SOL"} {
		if !f.allow(sym) {
			t.Errorf("filter should allow %q", sym)
		}
	}
	if f.allow("XRP") {
		t.Error("filter should reject XRP")
	}
	// Case-sensitive: btc must not match BTC.
	if f.allow("btc") {
		t.Error("filter should be case-sensitive")
	}
}

func TestFilter_IgnoresBlankEntries(t *testing.T) {
	f := newFilter(",,BTC,,,")
	if len(f.list()) != 1 {
		t.Fatalf("want 1 symbol, got %d: %v", len(f.list()), f.list())
	}
	if !f.allow("BTC") {
		t.Error("BTC should be allowed")
	}
}

func TestBot_HandleQuote_UpdatesTobGauges(t *testing.T) {
	m := newMetrics()
	b := NewBot("", newFilter("BTC"), m)

	rec := &Record{
		Type:      "quote",
		Timestamp: time.Now().Add(-50 * time.Millisecond),
		Symbol:    "BTC",
		Fields: map[string]any{
			"bid_price": 67000.50,
			"ask_price": 67001.00,
			"bid_qty":   1.25,
			"ask_qty":   0.75,
		},
	}
	b.handle(rec)

	want := map[*prometheus.GaugeVec]float64{
		m.bidPrice: 67000.50,
		m.askPrice: 67001.00,
		m.bidQty:   1.25,
		m.askQty:   0.75,
	}
	for gauge, want := range want {
		got := readGauge(t, gauge, "symbol", "BTC")
		if got != want {
			t.Errorf("gauge mismatch: got %v, want %v", got, want)
		}
	}

	spread := readGauge(t, m.spread, "symbol", "BTC")
	if spread != 0.50 {
		t.Errorf("spread = %v, want 0.50", spread)
	}
}

func TestBot_HandleQuote_DropsUnsubscribedSymbol(t *testing.T) {
	m := newMetrics()
	b := NewBot("", newFilter("BTC"), m)

	rec := &Record{
		Type:      "quote",
		Timestamp: time.Now(),
		Symbol:    "XRP",
		Fields:    map[string]any{"bid_price": 0.50, "ask_price": 0.51},
	}
	b.handle(rec)

	dropped := readCounter(t, m.dropped, "reason", "filter")
	if dropped != 1 {
		t.Errorf("dropped counter = %v, want 1", dropped)
	}
	// No gauge should exist for XRP (it wasn't registered).
	if readGauge(t, m.bidPrice, "symbol", "XRP") != 0 {
		t.Error("unsubscribed symbol should not have gauge populated")
	}
}

func TestBot_HandleTrade_UpdatesTradeGauges(t *testing.T) {
	m := newMetrics()
	b := NewBot("", newFilter("BTC"), m)

	rec := &Record{
		Type:      "trade",
		Timestamp: time.Now(),
		Symbol:    "BTC",
		Fields: map[string]any{
			"trade_price":       67000.25,
			"trade_qty":         0.5,
			"aggressor_side":    "buy",
			"trade_id":          float64(12345),
			"cumulative_volume": 1234.5,
		},
	}
	b.handle(rec)

	if got := readGauge(t, m.lastTradePrice, "symbol", "BTC"); got != 67000.25 {
		t.Errorf("last_trade_price = %v, want 67000.25", got)
	}
	if got := readGauge(t, m.lastTradeQty, "symbol", "BTC"); got != 0.5 {
		t.Errorf("last_trade_qty = %v, want 0.5", got)
	}
	if got := readCounter(t, m.records, "type", "trade"); got != 1 {
		t.Errorf("records{trade} = %v, want 1", got)
	}

	// Sanity on accessors
	if side, ok := rec.aggressorSide(); !ok || side != "buy" {
		t.Errorf("aggressorSide() = %q/%v, want \"buy\"/true", side, ok)
	}
	if id, ok := rec.tradeID(); !ok || id != 12345 {
		t.Errorf("tradeID() = %v/%v, want 12345/true", id, ok)
	}
}

func TestBot_HandleHeartbeat_NoSymbolCountsButNoGauge(t *testing.T) {
	m := newMetrics()
	b := NewBot("", newFilter(""), m)

	rec := &Record{
		Type:      "heartbeat",
		Timestamp: time.Now(),
	}
	b.handle(rec)

	if got := readCounter(t, m.records, "type", "heartbeat"); got != 1 {
		t.Errorf("records{heartbeat} = %v, want 1", got)
	}
}

func TestBot_ReadLoop_DecodesJSONLines(t *testing.T) {
	// Stand up a socketpair: one side is the "parser", other is the bot.
	tmp := t.TempDir()
	sock := filepath.Join(tmp, "t.sock")

	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer lis.Close()

	m := newMetrics()
	b := NewBot(sock, newFilter("BTC"), m)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		conn, _ := lis.Accept()
		if conn == nil {
			close(done)
			return
		}
		defer conn.Close()

		records := []Record{
			{
				Type:      "quote",
				Timestamp: time.Now(),
				Symbol:    "BTC",
				Fields: map[string]any{
					"bid_price": 100.0, "ask_price": 101.0,
				},
			},
			{
				Type:      "quote",
				Timestamp: time.Now(),
				Symbol:    "XRP",
				Fields: map[string]any{
					"bid_price": 1.0, "ask_price": 1.1,
				},
			},
		}
		w := bufio.NewWriter(conn)
		for _, r := range records {
			_ = json.NewEncoder(w).Encode(&r)
		}
		_ = w.Flush()
		close(done)
	}()

	// Run the bot until we see our expected counters, then cancel.
	botDone := make(chan struct{})
	go func() {
		_ = b.Run(ctx)
		close(botDone)
	}()

	deadline := time.After(2 * time.Second)
	for {
		if readCounter(t, m.records, "type", "quote") >= 1 &&
			readCounter(t, m.dropped, "reason", "filter") >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: records=%v dropped=%v",
				readCounter(t, m.records, "type", "quote"),
				readCounter(t, m.dropped, "reason", "filter"))
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	<-botDone

	// Sanity: gauge for BTC got set, XRP did not.
	if readGauge(t, m.bidPrice, "symbol", "BTC") != 100.0 {
		t.Errorf("BTC bid = %v, want 100", readGauge(t, m.bidPrice, "symbol", "BTC"))
	}
	if readGauge(t, m.bidPrice, "symbol", "XRP") != 0 {
		t.Errorf("XRP bid should not have been populated")
	}

	_ = os.Remove(sock)
}

// --- test helpers ---

func readGauge(t *testing.T, g *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	if len(labels)%2 != 0 {
		t.Fatalf("labels must be k/v pairs")
	}
	lvs := make([]string, 0, len(labels)/2)
	for i := 1; i < len(labels); i += 2 {
		lvs = append(lvs, labels[i])
	}
	m := &dto.Metric{}
	metric, err := g.GetMetricWithLabelValues(lvs...)
	if err != nil {
		t.Fatalf("metric lookup failed: %v", err)
	}
	if err := metric.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Gauge.GetValue()
}

func readCounter(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	if len(labels)%2 != 0 {
		t.Fatalf("labels must be k/v pairs")
	}
	lvs := make([]string, 0, len(labels)/2)
	for i := 1; i < len(labels); i += 2 {
		lvs = append(lvs, labels[i])
	}
	metric, err := c.GetMetricWithLabelValues(lvs...)
	if err != nil {
		t.Fatalf("metric lookup failed: %v", err)
	}
	m := &dto.Metric{}
	if err := metric.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Counter.GetValue()
}
