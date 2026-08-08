package main

import (
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// metricsObserver adapts the shared client's Observer onto this bot's metrics.
//
// The shared package deliberately owns no Prometheus dependency, so each
// consumer keeps its own metric names and namespace. This is the seam.
type metricsObserver struct{ m *Metrics }

func (o *metricsObserver) RowsWritten(table string, n int) {
	o.m.ClickhouseRowsWritten.WithLabelValues(table).Add(float64(n))
}

func (o *metricsObserver) RowsDropped(table, reason string, n int) {
	o.m.ClickhouseRowsDropped.WithLabelValues(table, reason).Add(float64(n))
}

func (o *metricsObserver) WriteError(table, reason string) {
	o.m.ClickhouseWriteErrors.WithLabelValues(table, reason).Inc()
}

func (o *metricsObserver) BatchDuration(table string, d time.Duration) {
	o.m.ClickhouseBatchDuration.WithLabelValues(table).Observe(d.Seconds())
}

func (o *metricsObserver) BufferedRows(table string, n int) {
	o.m.ClickhouseBufferedRows.WithLabelValues(table).Set(float64(n))
}

// newClickhouseClient configures one batcher per table the writers target.
//
// A table missing from this list is silently rejected by Enqueue, so every table
// in 03_schema_mbp.sql that the bot writes must appear here. instruments and
// channel_health get small, slow batchers: they are low-rate and worth landing
// promptly rather than sitting in a buffer waiting for a large batch to fill.
func newClickhouseClient(url, db string, batchSize int, batchInterval time.Duration, bufferSize int, m *Metrics) (*clickhouse.Client, error) {
	if url == "" {
		return nil, nil
	}
	return clickhouse.New(url, db, []clickhouse.BatcherConfig{
		{Table: "events", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "level_snapshots", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "wire_levels", BatchSize: batchSize, BatchInterval: batchInterval, BufferSize: bufferSize},
		{Table: "instruments", BatchSize: 100, BatchInterval: time.Second, BufferSize: 1000},
		{Table: "channel_health", BatchSize: 100, BatchInterval: time.Second, BufferSize: 1000},
	}, &metricsObserver{m: m})
}
