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

const metricsNamespace = "dz_mbo_parser"

type Metrics struct {
	registry *prometheus.Registry

	IngressPackets    *prometheus.CounterVec
	IngressBytes      *prometheus.CounterVec
	ParseErrors       *prometheus.CounterVec
	RecordsTotal      *prometheus.CounterVec
	SourceLatency     *prometheus.HistogramVec
	SendLatency       *prometheus.HistogramVec
	SocketClients     prometheus.Gauge
	SocketClientDrops *prometheus.CounterVec
	SocketRecordsSent prometheus.Counter
	SinkWriteErrors   prometheus.Counter
	BuildInfo         *prometheus.GaugeVec
	UptimeSeconds     prometheus.GaugeFunc

	// Frame header sequence gap tracking (real UDP datagram loss).
	FrameSeqGaps  *prometheus.CounterVec
	FramesMissing *prometheus.CounterVec

	// FramesTotal counts successfully parsed frames by port and wire schema
	// version. The version label is what makes a publisher's v1-to-v3 cutover
	// observable: v3 climbs, v1 goes flat, and v1 reaching zero is when the
	// legacy decode path can be retired.
	FramesTotal *prometheus.CounterVec // labels: port, schema_version

	startTime time.Time
}

func NewMetrics(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry:  reg,
		startTime: time.Now(),
	}

	m.IngressPackets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "ingress_packets_total",
		Help: "UDP datagrams received per port",
	}, []string{"port"})

	m.IngressBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "ingress_bytes_total",
		Help: "UDP bytes received per port",
	}, []string{"port"})

	m.ParseErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "parse_errors_total",
		Help: "Frame decode failures by reason",
	}, []string{"port", "reason"})

	m.RecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "records_total",
		Help: "Records emitted per record type",
	}, []string{"type"})

	m.SourceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "source_latency_seconds",
		Help:    "Latency from block/venue source timestamp to kernel receive, by port (crosses validator and local clocks).",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"port"})

	m.SendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "send_latency_seconds",
		Help:    "Latency from publisher egress send timestamp to kernel receive, by port.",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"port"})

	m.SocketClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "socket_clients",
		Help: "Currently connected Unix socket clients",
	})

	m.SocketClientDrops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "socket_client_drops_total",
		Help: "Slow clients dropped by reason",
	}, []string{"reason"})

	m.SocketRecordsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "socket_records_sent_total",
		Help: "Records written to >=1 client",
	})

	m.SinkWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "sink_write_errors_total",
		Help: "Sink write failures",
	})

	m.FrameSeqGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "datagram_seq_gaps_total",
		Help: "Number of UDP datagram header sequence discontinuities (real datagram loss events), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.FramesMissing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "datagrams_missing_total",
		Help: "Total UDP datagrams missing (sum of gap magnitudes in header seq), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.FramesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "datagrams_total",
		Help: "Successfully parsed datagrams, by port and wire schema version.",
	}, []string{"port", "schema_version"})

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "build_info",
		Help: "Build info; value always 1",
	}, []string{"version", "commit"})

	m.UptimeSeconds = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "uptime_seconds",
		Help: "Seconds since process start",
	}, func() float64 { return time.Since(m.startTime).Seconds() })

	reg.MustRegister(
		m.IngressPackets, m.IngressBytes, m.ParseErrors, m.RecordsTotal, m.SourceLatency, m.SendLatency,
		m.SocketClients, m.SocketClientDrops, m.SocketRecordsSent, m.SinkWriteErrors,
		m.FrameSeqGaps, m.FramesMissing, m.FramesTotal,
		m.BuildInfo, m.UptimeSeconds,
	)
	m.BuildInfo.WithLabelValues(version, commit).Set(1)

	return m
}

// ServeHTTP starts a /metrics HTTP server on addr. Returns immediately.
// Server errors are logged via the provided logger callback.
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
