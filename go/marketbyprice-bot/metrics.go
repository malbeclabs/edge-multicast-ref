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

	// Book state
	BookLevels    *prometheus.GaugeVec // labels: symbol, side
	BookTopPrice  *prometheus.GaugeVec // labels: symbol, side
	BookTopQty    *prometheus.GaugeVec // labels: symbol, side
	BookSpreadBps *prometheus.GaugeVec // labels: symbol

	// Feed-specific defect and health counters
	CrossedBookEventsTotal    prometheus.Counter
	CrossedInstruments        prometheus.Gauge
	BookDivergenceTotal       *prometheus.CounterVec // label: kind
	DeltaBufferOverflowTotal  prometheus.Counter
	DeltaBufferedRecords      prometheus.Gauge
	SnapshotDiscardedTotal    *prometheus.CounterVec // label: reason
	SnapshotLevelDroppedTotal prometheus.Counter
	DepthBoundedInstruments   prometheus.Gauge
	PerInstrumentGapsTotal    prometheus.Counter
	InstrumentResetsTotal     *prometheus.CounterVec // label: reason
	ChannelResetsTotal        prometheus.Counter
	InstrumentsTotal          *prometheus.GaugeVec // label: status

	// Snapshot writer
	SnapshotWritesTotal    prometheus.Counter
	SnapshotCoalescesTotal prometheus.Counter
	SnapshotLagMs          prometheus.Histogram

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

	m.BookLevels = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_levels"}, []string{"symbol", "side"})
	m.BookTopPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_price"}, []string{"symbol", "side"})
	m.BookTopQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_qty"}, []string{"symbol", "side"})
	m.BookSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_spread_bps"}, []string{"symbol"})

	m.CrossedBookEventsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "crossed_book_events_total"})
	m.CrossedInstruments = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "crossed_instruments"})
	m.BookDivergenceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "book_divergence_total"}, []string{"kind"})
	m.DeltaBufferOverflowTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "delta_buffer_overflow_total"})
	m.DeltaBufferedRecords = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "delta_buffered_records"})
	m.SnapshotDiscardedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_discarded_total"}, []string{"reason"})
	m.SnapshotLevelDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_level_dropped_total"})
	m.DepthBoundedInstruments = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "depth_bounded_instruments"})
	m.PerInstrumentGapsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "per_instrument_gaps_total"})
	m.InstrumentResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "instrument_resets_total"}, []string{"reason"})
	m.ChannelResetsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "channel_resets_total"})
	m.InstrumentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "instruments_total"}, []string{"status"})

	m.SnapshotWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_writes_total"})
	m.SnapshotCoalescesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_coalesces_total"})
	m.SnapshotLagMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "snapshot_lag_ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	reg.MustRegister(
		m.BuildInfo, m.UptimeSeconds,
		m.SocketConnected, m.SocketReconnects, m.RecordsTotal, m.DecodeErrors, m.SocketToBotLatency,
		m.BookLevels, m.BookTopPrice, m.BookTopQty, m.BookSpreadBps,
		m.CrossedBookEventsTotal, m.CrossedInstruments, m.BookDivergenceTotal,
		m.DeltaBufferOverflowTotal, m.DeltaBufferedRecords,
		m.SnapshotDiscardedTotal, m.SnapshotLevelDroppedTotal, m.DepthBoundedInstruments,
		m.PerInstrumentGapsTotal, m.InstrumentResetsTotal, m.ChannelResetsTotal, m.InstrumentsTotal,
		m.SnapshotWritesTotal, m.SnapshotCoalescesTotal, m.SnapshotLagMs,
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
