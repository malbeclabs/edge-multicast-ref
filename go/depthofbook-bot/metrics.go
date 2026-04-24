package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "dz_dob_bot"

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

	// Book state
	InstrumentsTotal       *prometheus.GaugeVec
	InstrumentResetsTotal  *prometheus.CounterVec
	ChannelResetsTotal     prometheus.Counter
	PerInstrumentGapsTotal prometheus.Counter
	BookOrders             *prometheus.GaugeVec
	BookTopPrice           *prometheus.GaugeVec
	BookTopQty             *prometheus.GaugeVec
	BookSpreadBps          *prometheus.GaugeVec

	// Snapshot writer
	SnapshotWritesTotal    prometheus.Counter
	SnapshotCoalescesTotal prometheus.Counter
	SnapshotLagMs          prometheus.Histogram

	// ClickHouse
	ClickhouseRowsWritten   *prometheus.CounterVec
	ClickhouseRowsDropped   *prometheus.CounterVec
	ClickhouseWriteErrors   *prometheus.CounterVec
	ClickhouseBatchDuration *prometheus.HistogramVec
	ClickhouseBufferedRows  *prometheus.GaugeVec

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

	m.InstrumentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "instruments_total"}, []string{"status"})
	m.InstrumentResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "instrument_resets_total"}, []string{"reason"})
	m.ChannelResetsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "channel_resets_total"})
	m.PerInstrumentGapsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "per_instrument_gaps_total"})
	m.BookOrders = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_orders"}, []string{"symbol", "side"})
	m.BookTopPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_price"}, []string{"symbol", "side"})
	m.BookTopQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_qty"}, []string{"symbol", "side"})
	m.BookSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_spread_bps"}, []string{"symbol"})

	m.SnapshotWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_writes_total"})
	m.SnapshotCoalescesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_coalesces_total"})
	m.SnapshotLagMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "snapshot_lag_ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	m.ClickhouseRowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_written_total"}, []string{"table"})
	m.ClickhouseRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_dropped_total"}, []string{"table", "reason"})
	m.ClickhouseWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_write_errors_total"}, []string{"table", "reason"})
	m.ClickhouseBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "clickhouse_batch_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"table"})
	m.ClickhouseBufferedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "clickhouse_buffered_rows"}, []string{"table"})

	reg.MustRegister(
		m.BuildInfo, m.UptimeSeconds,
		m.SocketConnected, m.SocketReconnects, m.RecordsTotal, m.DecodeErrors, m.SocketToBotLatency,
		m.InstrumentsTotal, m.InstrumentResetsTotal, m.ChannelResetsTotal, m.PerInstrumentGapsTotal,
		m.BookOrders, m.BookTopPrice, m.BookTopQty, m.BookSpreadBps,
		m.SnapshotWritesTotal, m.SnapshotCoalescesTotal, m.SnapshotLagMs,
		m.ClickhouseRowsWritten, m.ClickhouseRowsDropped, m.ClickhouseWriteErrors,
		m.ClickhouseBatchDuration, m.ClickhouseBufferedRows,
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
