package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/sink"
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

	// Datagram header sequence gap tracking (real UDP datagram loss).
	DatagramSeqGaps  *prometheus.CounterVec
	DatagramsMissing *prometheus.CounterVec

	// DatagramsTotal counts successfully parsed datagrams by port and wire schema
	// version. The version label is what makes a publisher's v1-to-v3 cutover
	// observable: v3 climbs, v1 goes flat, and v1 reaching zero is when the
	// legacy decode path can be retired.
	DatagramsTotal *prometheus.CounterVec // labels: port, schema_version

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
		Help: "Datagram decode failures by reason",
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

	m.DatagramSeqGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "datagram_seq_gaps_total",
		Help: "Number of UDP datagram header sequence discontinuities (real datagram loss events), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.DatagramsMissing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "datagrams_missing_total",
		Help: "Total UDP datagrams missing (sum of gap magnitudes in header seq), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.DatagramsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
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
		m.DatagramSeqGaps, m.DatagramsMissing, m.DatagramsTotal,
		m.BuildInfo, m.UptimeSeconds,
	)
	m.BuildInfo.WithLabelValues(version, commit).Set(1)

	return m
}

// Metrics carries the socket sink's counters for the shared transport in
// go/internal/sink, which reports through this interface rather than holding a
// metrics backend of its own.
var _ sink.SocketMetrics = (*Metrics)(nil)

// socketMetrics hands these counters to the shared socket sink, and hands it
// an untyped nil when there are none. That is the difference between the sink
// skipping the counting outright and it calling methods on a nil pointer
// wrapped in a non-nil interface, which only the nil-receiver guards below
// would then save.
func (m *Metrics) socketMetrics() sink.SocketMetrics {
	if m == nil {
		return nil
	}
	return m
}

// SetSocketClients reports the number of currently connected socket clients.
//
// A nil receiver counts nothing: SinkConfig.Metrics is optional and reaches the
// sink as it stands, so all three of these tolerate one.
func (m *Metrics) SetSocketClients(n int) {
	if m == nil {
		return
	}
	m.SocketClients.Set(float64(n))
}

// AddSocketClientDrops counts n drops for reason, and what one drop is depends
// on the reason: sink.DropReasonQueueFull counts one batch dropped for a client
// whose outbound queue was full, which stays connected, while
// sink.DropReasonWriteError counts one client disconnected after a failed
// write.
func (m *Metrics) AddSocketClientDrops(reason string, n int) {
	if m == nil {
		return
	}
	m.SocketClientDrops.WithLabelValues(reason).Add(float64(n))
}

// AddSocketRecordsSent counts n records queued to at least one socket client.
// Queued, not delivered: a batch a client's queue accepted and its writer then
// failed to write counts here, and again as a write-error drop.
func (m *Metrics) AddSocketRecordsSent(n int) {
	if m == nil {
		return
	}
	m.SocketRecordsSent.Add(float64(n))
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
