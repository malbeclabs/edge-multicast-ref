package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var (
	sockPath    = flag.String("socket", "", "path to the topofbook-parser Unix socket (required)")
	symbolsCSV  = flag.String("symbol", "", "comma-separated symbols to subscribe to (empty = all)")
	metricsAddr = flag.String("metrics-addr", "127.0.0.1:9091", "Prometheus /metrics HTTP addr")

	clickhouseURL      = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (e.g. http://clickhouse:8123); empty disables tick persistence")
	clickhouseDB       = flag.String("clickhouse-database", "topofbook", "ClickHouse database name")
	clickhouseBatchSz  = flag.Int("clickhouse-batch-size", 1000, "rows per batch before flush")
	clickhouseBatchInt = flag.Duration("clickhouse-batch-interval", 200*time.Millisecond, "max time between flushes")
	clickhouseBuffer   = flag.Int("clickhouse-buffer", 100_000, "per-table row buffer capacity before drop")

	verbose     = flag.Bool("v", false, "enable debug logging")
	versionFlag = flag.Bool("version", false, "print version and exit")

	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	flag.Parse()

	if *versionFlag {
		fmt.Printf("dz-example-bot %s (%s) built %s\n", version, commit, date)
		os.Exit(0)
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if *sockPath == "" {
		return fmt.Errorf("--socket is required")
	}

	m := newMetrics()
	m.buildInfo.WithLabelValues(version, commit).Set(1)

	f := newFilter(*symbolsCSV)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("starting",
		"socket", *sockPath,
		"symbols", f.list(),
		"metrics_addr", *metricsAddr,
	)

	if *metricsAddr != "" {
		go func() {
			if err := m.serve(ctx, *metricsAddr); err != nil {
				slog.Error("metrics server error", "error", err)
				cancel()
			}
		}()
	}

	bot := NewBot(*sockPath, f, m)

	// Optional ClickHouse tick-level writer. Runs its own flush goroutines.
	var chWG sync.WaitGroup
	if *clickhouseURL != "" {
		cfg := DefaultClickHouseConfig()
		cfg.URL = *clickhouseURL
		cfg.Database = *clickhouseDB
		cfg.BatchSize = *clickhouseBatchSz
		cfg.BatchInterval = *clickhouseBatchInt
		cfg.BufferSize = *clickhouseBuffer

		w, err := newChWriter(cfg, m)
		if err != nil {
			return fmt.Errorf("clickhouse writer init: %w", err)
		}
		chWG.Add(1)
		go func() {
			defer chWG.Done()
			w.Run(ctx)
		}()
		bot.AttachClickHouse(w)
		slog.Info("clickhouse writer enabled",
			"url", cfg.URL, "database", cfg.Database,
			"batch_size", cfg.BatchSize, "batch_interval", cfg.BatchInterval)
	}

	if err := bot.Run(ctx); err != nil {
		return fmt.Errorf("bot exited: %w", err)
	}

	// Allow ClickHouse batcher goroutines to drain and flush final batches.
	chWG.Wait()

	slog.Info("shutdown complete")
	return nil
}
