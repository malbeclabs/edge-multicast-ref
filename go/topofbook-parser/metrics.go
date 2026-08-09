package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Latency histogram buckets: 100us .. 60s, log-spaced.
// Covers in-venue multicast (sub-ms) to geographic hops (hundreds of ms)
// to pathological buffering cases (seconds).
var latencyBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5,
	1, 2.5, 5,
	10, 30, 60,
}

// metrics holds all Prometheus metrics for the subscriber.
// Created once in main and passed by pointer to components that emit.
type metrics struct {
	registry *prometheus.Registry

	// Ingress (UDP → parser)
	ingressPackets    *prometheus.CounterVec
	ingressBytes      *prometheus.CounterVec
	parseErrors       *prometheus.CounterVec
	frameHeaderErrors *prometheus.CounterVec

	// Decoded output
	records         *prometheus.CounterVec
	sinkWriteErrors prometheus.Counter

	// Buffering / refdata state
	buffered           prometheus.Gauge
	bufferDrops        prometheus.Counter
	instrumentsTracked prometheus.Gauge

	// Latency metrics (kernel recv time as reference).
	sourceLatency *prometheus.HistogramVec
	sendLatency   *prometheus.HistogramVec

	// Frame header sequence gap tracking (real UDP datagram loss).
	frameSeqGaps  *prometheus.CounterVec
	framesMissing *prometheus.CounterVec

	// framesTotal counts successfully parsed frames by port and wire schema
	// version. The version label is what makes a publisher's v1-to-v3 cutover
	// observable: v3 climbs, v1 goes flat, and v1 reaching zero is when the
	// legacy decode path can be retired.
	framesTotal *prometheus.CounterVec // labels: port, schema_version

	// Socket sink
	socketClients     prometheus.Gauge
	socketClientDrops *prometheus.CounterVec
	socketRecordsSent prometheus.Counter

	// Process / health
	buildInfo *prometheus.GaugeVec
	startTime time.Time
	uptime    prometheus.GaugeFunc
}

func newMetrics() *metrics {
	reg := prometheus.NewRegistry()
	m := &metrics{
		registry:  reg,
		startTime: time.Now(),
	}

	m.ingressPackets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_ingress_packets_total",
		Help: "UDP datagrams received from the multicast group, by channel.",
	}, []string{"channel"})

	m.ingressBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_ingress_bytes_total",
		Help: "UDP bytes received from the multicast group, by channel.",
	}, []string{"channel"})

	m.parseErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_parse_errors_total",
		Help: "Frames that failed to decode, by channel and reason.",
	}, []string{"channel", "reason"})

	m.frameHeaderErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frame_header_errors_total",
		Help: "Frames rejected by header validation, by reason.",
	}, []string{"reason"})

	m.records = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_records_total",
		Help: "Decoded records emitted to the sink, by record type.",
	}, []string{"type"})

	m.sinkWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dz_subscriber_sink_write_errors_total",
		Help: "Sink write failures.",
	})

	m.buffered = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dz_subscriber_buffered_messages",
		Help: "Messages currently buffered awaiting instrument definitions.",
	})

	m.bufferDrops = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dz_subscriber_buffer_drops_total",
		Help: "Messages dropped because the cold-start buffer was full.",
	})

	m.instrumentsTracked = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dz_subscriber_instruments_tracked",
		Help: "Distinct instruments the parser has learned definitions for.",
	})

	m.sourceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_subscriber_source_latency_seconds",
		Help:    "Latency from block/venue source timestamp to kernel receive, by record type (crosses validator and local clocks).",
		Buckets: latencyBuckets,
	}, []string{"type"})

	m.sendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_subscriber_send_latency_seconds",
		Help:    "Latency from publisher egress send timestamp to kernel receive, by record type.",
		Buckets: latencyBuckets,
	}, []string{"type"})

	m.frameSeqGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frame_seq_gaps_total",
		Help: "Number of UDP frame header sequence discontinuities (real datagram loss events), by port.",
	}, []string{"port"})

	m.framesMissing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frames_missing_total",
		Help: "Total UDP frames missing (sum of gap magnitudes in header seq), by port.",
	}, []string{"port"})

	m.framesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frames_total",
		Help: "Successfully parsed frames, by port and wire schema version.",
	}, []string{"port", "schema_version"})

	m.socketClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dz_subscriber_socket_clients",
		Help: "Currently connected Unix socket clients.",
	})

	m.socketClientDrops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_socket_client_drops_total",
		Help: "Unix socket clients dropped, by reason.",
	}, []string{"reason"})

	m.socketRecordsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dz_subscriber_socket_records_sent_total",
		Help: "Records successfully written to at least one connected Unix socket client.",
	})

	m.buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_subscriber_build_info",
		Help: "Build metadata. Always 1.",
	}, []string{"version", "commit"})

	m.uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "dz_subscriber_uptime_seconds",
		Help: "Seconds since the process started.",
	}, func() float64 {
		return time.Since(m.startTime).Seconds()
	})

	reg.MustRegister(
		m.ingressPackets, m.ingressBytes, m.parseErrors, m.frameHeaderErrors,
		m.records, m.sinkWriteErrors,
		m.buffered, m.bufferDrops, m.instrumentsTracked,
		m.sourceLatency, m.sendLatency,
		m.frameSeqGaps, m.framesMissing, m.framesTotal,
		m.socketClients, m.socketClientDrops, m.socketRecordsSent,
		m.buildInfo, m.uptime,
	)

	return m
}

// serve starts an HTTP server on addr exposing /metrics. Returns when the
// server stops (listener closed, context cancelled, or unrecoverable error).
// Startup errors (bind failures) are returned synchronously.
func (m *metrics) serve(ctx context.Context, addr string) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{Registry: m.registry}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("metrics server listening", "addr", addr)
		err := srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		return nil
	case err := <-errCh:
		return err
	}
}
