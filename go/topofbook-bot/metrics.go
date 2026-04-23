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

var latencyBuckets = []float64{
	0.0001, 0.00025, 0.0005,
	0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05,
	0.1, 0.25, 0.5,
	1, 2.5, 5,
	10, 30, 60,
}

type metrics struct {
	registry *prometheus.Registry

	// Intake counters
	records     *prometheus.CounterVec
	dropped     *prometheus.CounterVec
	latency     *prometheus.HistogramVec
	reconnects  *prometheus.CounterVec
	connected   prometheus.Gauge
	decodeError prometheus.Counter

	// Per-symbol TOB state (cardinality bounded by the symbol filter)
	bidPrice        *prometheus.GaugeVec
	askPrice        *prometheus.GaugeVec
	bidQty          *prometheus.GaugeVec
	askQty          *prometheus.GaugeVec
	spread          *prometheus.GaugeVec
	spreadBps       *prometheus.GaugeVec
	lastTradePrice  *prometheus.GaugeVec
	lastTradeQty    *prometheus.GaugeVec
	lastUpdateTS    *prometheus.GaugeVec

	buildInfo *prometheus.GaugeVec
	uptime    prometheus.GaugeFunc

	// ClickHouse writer
	chRowsWritten   *prometheus.CounterVec
	chRowsDropped   *prometheus.CounterVec
	chWriteErrors   *prometheus.CounterVec
	chBatchDuration *prometheus.HistogramVec
	chBufferedRows  *prometheus.GaugeVec

	startTime time.Time
}

func newMetrics() *metrics {
	reg := prometheus.NewRegistry()
	m := &metrics{
		registry:  reg,
		startTime: time.Now(),
	}

	m.records = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_records_total",
		Help: "Records processed after filtering, by record type.",
	}, []string{"type"})

	m.dropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_records_dropped_total",
		Help: "Records dropped by the symbol filter, by reason.",
	}, []string{"reason"})

	m.latency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_bot_socket_to_bot_latency_seconds",
		Help:    "Time from publisher send_ts to bot receive. Includes publisher/subscriber/bot clock skew and any parser/socket buffering.",
		Buckets: latencyBuckets,
	}, []string{"type"})

	m.reconnects = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_socket_reconnects_total",
		Help: "Unix socket reconnect attempts, by trigger.",
	}, []string{"reason"})

	m.connected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "dz_bot_socket_connected",
		Help: "1 if the bot is currently connected to the parser socket, 0 otherwise.",
	})

	m.decodeError = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "dz_bot_decode_errors_total",
		Help: "JSON decode failures on the socket stream.",
	})

	m.bidPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_bid_price",
		Help: "Latest best bid price per subscribed symbol.",
	}, []string{"symbol"})

	m.askPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_ask_price",
		Help: "Latest best ask price per subscribed symbol.",
	}, []string{"symbol"})

	m.bidQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_bid_qty",
		Help: "Latest best bid quantity per subscribed symbol.",
	}, []string{"symbol"})

	m.askQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_ask_qty",
		Help: "Latest best ask quantity per subscribed symbol.",
	}, []string{"symbol"})

	m.spread = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_spread",
		Help: "ask_price - bid_price per subscribed symbol.",
	}, []string{"symbol"})

	m.spreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_spread_bps",
		Help: "Spread in basis points: (ask-bid)/mid*10000.",
	}, []string{"symbol"})

	m.lastTradePrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_last_trade_price",
		Help: "Price of the most recent observed trade per subscribed symbol.",
	}, []string{"symbol"})

	m.lastTradeQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_last_trade_qty",
		Help: "Quantity of the most recent observed trade per subscribed symbol.",
	}, []string{"symbol"})

	m.lastUpdateTS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_last_update_timestamp_seconds",
		Help: "Publisher send_ts (Unix seconds) of the most recent record per subscribed symbol. Alerting on staleness is simpler as a timestamp than a duration.",
	}, []string{"symbol"})

	m.buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_build_info",
		Help: "Build metadata. Always 1.",
	}, []string{"version", "commit"})

	m.uptime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "dz_bot_uptime_seconds",
		Help: "Seconds since the process started.",
	}, func() float64 { return time.Since(m.startTime).Seconds() })

	m.chRowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_clickhouse_rows_written_total",
		Help: "Rows successfully inserted into ClickHouse, by table.",
	}, []string{"table"})

	m.chRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_clickhouse_rows_dropped_total",
		Help: "Rows dropped on the ClickHouse write path, by table and reason.",
	}, []string{"table", "reason"})

	m.chWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_bot_clickhouse_write_errors_total",
		Help: "ClickHouse write failures, by table and reason.",
	}, []string{"table", "reason"})

	m.chBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_bot_clickhouse_batch_duration_seconds",
		Help:    "Time to POST a batch to ClickHouse, by table.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"table"})

	m.chBufferedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "dz_bot_clickhouse_buffered_rows",
		Help: "Rows currently queued for the ClickHouse batcher, by table.",
	}, []string{"table"})

	reg.MustRegister(
		m.records, m.dropped, m.latency,
		m.reconnects, m.connected, m.decodeError,
		m.bidPrice, m.askPrice, m.bidQty, m.askQty,
		m.spread, m.spreadBps,
		m.lastTradePrice, m.lastTradeQty,
		m.lastUpdateTS,
		m.chRowsWritten, m.chRowsDropped, m.chWriteErrors,
		m.chBatchDuration, m.chBufferedRows,
		m.buildInfo, m.uptime,
	)

	return m
}

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
