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
