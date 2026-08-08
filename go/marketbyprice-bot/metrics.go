package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

const metricsNamespace = "dz_mbp_bot"

// Metrics is the bot's full Prometheus metric set. Replaces the stub in bot.go.
type Metrics struct {
	registry *prometheus.Registry

	// Process
	BuildInfo     *prometheus.GaugeVec
	UptimeSeconds prometheus.GaugeFunc

	// Decode + intake
	SocketConnected    prometheus.Gauge
	SocketReconnects   *prometheus.CounterVec
	RecordsTotal       *prometheus.CounterVec
	DecodeErrors       prometheus.Counter
	SocketToBotLatency *prometheus.HistogramVec

	// Feed-specific defect and health counters
	CrossedBookEventsTotal prometheus.Counter
	// CrossedInstruments and DeltaBufferedRecords are labelled by shard because
	// each shard owns a disjoint slice of instruments and can only ever report its
	// own count. As bare process-wide gauges every shard would Set the same series
	// to its local number, so the exported value was one arbitrary shard's rather
	// than the total. Labelled per shard, sum() across the series is the truth.
	CrossedInstruments        *prometheus.GaugeVec // label: shard
	BookDivergenceTotal       *prometheus.CounterVec
	DeltaBufferOverflowTotal  prometheus.Counter
	DeltaBufferedRecords      *prometheus.GaugeVec   // label: shard
	SnapshotDiscardedTotal    *prometheus.CounterVec // label: reason
	SnapshotLevelDroppedTotal prometheus.Counter
	DeltasDiscardedTotal      *prometheus.CounterVec // label: reason
	PerInstrumentGapsTotal    prometheus.Counter
	InstrumentResetsTotal     *prometheus.CounterVec // label: reason
	ChannelResetsTotal        prometheus.Counter

	// ClickHouse persistence. Populated through metricsObserver, which adapts
	// the shared internal/clickhouse client's Observer interface onto these.
	ClickhouseRowsWritten   *prometheus.CounterVec   // label: table
	ClickhouseRowsDropped   *prometheus.CounterVec   // labels: table, reason
	ClickhouseWriteErrors   *prometheus.CounterVec   // labels: table, reason
	ClickhouseBatchDuration *prometheus.HistogramVec // label: table
	ClickhouseBufferedRows  *prometheus.GaugeVec     // label: table

	// Snapshot writer
	SnapshotWritesTotal    prometheus.Counter
	SnapshotCoalescesTotal prometheus.Counter
	SnapshotLagMs          prometheus.Histogram

	// Book state, refreshed on every snapshot flush
	BookLevels    *prometheus.GaugeVec // labels: symbol, side
	BookTopPrice  *prometheus.GaugeVec // labels: symbol, side
	BookTopQty    *prometheus.GaugeVec // labels: symbol, side
	BookSpreadBps *prometheus.GaugeVec // label: symbol

	startTime time.Time
}

func NewMetrics(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{registry: reg, startTime: time.Now()}

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "build_info"}, []string{"version", "commit"})
	m.UptimeSeconds = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "uptime_seconds"},
		func() float64 { return time.Since(m.startTime).Seconds() })

	m.SocketConnected = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "socket_connected"})
	m.SocketReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "socket_reconnects_total"}, []string{"reason"})
	m.RecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "records_total"}, []string{"type"})
	m.DecodeErrors = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "decode_errors_total"})
	m.SocketToBotLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "socket_to_bot_latency_seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"type"})

	m.CrossedBookEventsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "crossed_book_events_total"})
	m.CrossedInstruments = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "crossed_instruments"}, []string{"shard"})
	m.BookDivergenceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "book_divergence_total"}, []string{"kind"})
	m.DeltaBufferOverflowTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "delta_buffer_overflow_total"})
	m.DeltaBufferedRecords = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "delta_buffered_records"}, []string{"shard"})
	m.SnapshotDiscardedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_discarded_total"}, []string{"reason"})
	m.SnapshotLevelDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_level_dropped_total"})
	m.DeltasDiscardedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "deltas_discarded_total"}, []string{"reason"})
	m.PerInstrumentGapsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "per_instrument_gaps_total"})
	m.InstrumentResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "instrument_resets_total"}, []string{"reason"})
	m.ChannelResetsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "channel_resets_total"})

	m.ClickhouseRowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_written_total"}, []string{"table"})
	m.ClickhouseRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_dropped_total"}, []string{"table", "reason"})
	m.ClickhouseWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_write_errors_total"}, []string{"table", "reason"})
	m.ClickhouseBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "clickhouse_batch_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"table"})
	m.ClickhouseBufferedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "clickhouse_buffered_rows"}, []string{"table"})

	m.SnapshotWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_writes_total"})
	m.SnapshotCoalescesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_coalesces_total"})
	m.SnapshotLagMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "snapshot_lag_ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})
	m.BookLevels = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_levels"}, []string{"symbol", "side"})
	m.BookTopPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_price"}, []string{"symbol", "side"})
	m.BookTopQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_qty"}, []string{"symbol", "side"})
	m.BookSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_spread_bps"}, []string{"symbol"})

	reg.MustRegister(
		m.BuildInfo, m.UptimeSeconds,
		m.SocketConnected, m.SocketReconnects, m.RecordsTotal, m.DecodeErrors, m.SocketToBotLatency,
		m.CrossedBookEventsTotal, m.CrossedInstruments, m.BookDivergenceTotal,
		m.DeltaBufferOverflowTotal, m.DeltaBufferedRecords,
		m.SnapshotDiscardedTotal, m.SnapshotLevelDroppedTotal, m.DeltasDiscardedTotal,
		m.PerInstrumentGapsTotal, m.InstrumentResetsTotal, m.ChannelResetsTotal,
		m.ClickhouseRowsWritten, m.ClickhouseRowsDropped, m.ClickhouseWriteErrors,
		m.ClickhouseBatchDuration, m.ClickhouseBufferedRows,
		m.SnapshotWritesTotal, m.SnapshotCoalescesTotal, m.SnapshotLagMs,
		m.BookLevels, m.BookTopPrice, m.BookTopQty, m.BookSpreadBps,
	)
	m.BuildInfo.WithLabelValues(version, commit).Set(1)

	return m
}

func (m *Metrics) ServeHTTP(ctx context.Context, addr string, logErr func(error)) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logErr(fmt.Errorf("metrics server: %w", err))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}

// counterValue reads a counter's current value, for tests.
func counterValue(c prometheus.Counter) float64 {
	var m dto.Metric
	if err := c.(prometheus.Metric).Write(&m); err != nil {
		return -1
	}
	return m.GetCounter().GetValue()
}

// gaugeRead reads a gauge's current value, for tests.
func gaugeRead(g prometheus.Gauge) float64 {
	var m dto.Metric
	if err := g.(prometheus.Metric).Write(&m); err != nil {
		return -1
	}
	return m.GetGauge().GetValue()
}
