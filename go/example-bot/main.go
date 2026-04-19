package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

var (
	sockPath    = flag.String("socket", "", "path to the topofbook-parser Unix socket (required)")
	symbolsCSV  = flag.String("symbol", "", "comma-separated symbols to subscribe to (empty = all)")
	metricsAddr = flag.String("metrics-addr", "127.0.0.1:9091", "Prometheus /metrics HTTP addr")
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
	if err := bot.Run(ctx); err != nil {
		return fmt.Errorf("bot exited: %w", err)
	}

	slog.Info("shutdown complete")
	return nil
}
