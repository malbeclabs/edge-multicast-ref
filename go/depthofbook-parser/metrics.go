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

const metricsNamespace = "dz_dob_parser"

type Metrics struct {
	registry *prometheus.Registry

	IngressPackets    *prometheus.CounterVec
	IngressBytes      *prometheus.CounterVec
	ParseErrors       *prometheus.CounterVec
	RecordsTotal      *prometheus.CounterVec
	WireLatency       *prometheus.HistogramVec
	SocketClients     prometheus.Gauge
	SocketClientDrops *prometheus.CounterVec
	SocketRecordsSent prometheus.Counter
	SinkWriteErrors   prometheus.Counter
	BuildInfo         *prometheus.GaugeVec
	UptimeSeconds     prometheus.GaugeFunc

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

	m.WireLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "wire_latency_seconds",
		Help:    "Latency from publisher send_ts to parse, by port (includes clock skew)",
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

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "build_info",
		Help: "Build info; value always 1",
	}, []string{"version", "commit"})

	m.UptimeSeconds = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "uptime_seconds",
		Help: "Seconds since process start",
	}, func() float64 { return time.Since(m.startTime).Seconds() })

	reg.MustRegister(
		m.IngressPackets, m.IngressBytes, m.ParseErrors, m.RecordsTotal, m.WireLatency,
		m.SocketClients, m.SocketClientDrops, m.SocketRecordsSent, m.SinkWriteErrors,
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
