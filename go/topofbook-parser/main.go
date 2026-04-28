package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var (
	groupIP        = flag.String("group", "", "multicast group IP address (required)")
	marketdataPort = flag.Int("marketdata-port", 0, "UDP port for marketdata messages (required)")
	refdataPort    = flag.Int("refdata-port", 0, "UDP port for refdata messages (required)")
	parserName     = flag.String("parser", "topofbook", "parser to use")
	format         = flag.String("format", "json", "output format: json or csv")
	output         = flag.String("output", "", "output path: file path or unix:///path/to/sock (required)")
	iface          = flag.String("interface", "", "network interface to join multicast on (optional; default: system-selected)")
	metricsAddr    = flag.String("metrics-addr", "", "if set, serve Prometheus /metrics on this addr (e.g. 127.0.0.1:9090)")
	verbose        = flag.Bool("v", false, "enable debug logging")
	versionFlag    = flag.Bool("version", false, "print version and exit")

	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	flag.Parse()

	if *versionFlag {
		fmt.Printf("dz-topofbook-parser %s (%s) built %s\n", version, commit, date)
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
	if *groupIP == "" {
		return fmt.Errorf("--group is required")
	}
	ip := net.ParseIP(*groupIP)
	if ip == nil {
		return fmt.Errorf("invalid multicast group IP: %q", *groupIP)
	}
	if !ip.IsMulticast() {
		return fmt.Errorf("IP %s is not a multicast address", ip)
	}

	if *marketdataPort <= 0 || *marketdataPort > 65535 {
		return fmt.Errorf("--marketdata-port is required and must be 1-65535 (got %d)", *marketdataPort)
	}
	if *refdataPort <= 0 || *refdataPort > 65535 {
		return fmt.Errorf("--refdata-port is required and must be 1-65535 (got %d)", *refdataPort)
	}
	if *marketdataPort == *refdataPort {
		return fmt.Errorf("--marketdata-port and --refdata-port must differ (got %d)", *marketdataPort)
	}
	if *output == "" {
		return fmt.Errorf("--output is required")
	}

	m := newMetrics()
	m.buildInfo.WithLabelValues(version, commit).Set(1)

	parser, ok := NewParser(*parserName)
	if !ok {
		return fmt.Errorf("unknown parser %q; available: %v", *parserName, RegisteredParsers())
	}

	sink, err := NewSink(SinkConfig{Format: *format, Path: *output, Metrics: m})
	if err != nil {
		return fmt.Errorf("creating output sink: %w", err)
	}
	defer sink.Close()

	runner := NewRunner(RunnerConfig{
		GroupIP:        ip,
		MarketdataPort: *marketdataPort,
		RefdataPort:    *refdataPort,
		Interface:      *iface,
		Parser:         parser,
		Sink:           sink,
		Metrics:        m,
	})

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if *metricsAddr != "" {
		go func() {
			if err := m.serve(ctx, *metricsAddr); err != nil {
				slog.Error("metrics server error", "error", err)
			}
		}()
	}

	slog.Info("starting",
		"group", ip,
		"marketdata_port", *marketdataPort,
		"refdata_port", *refdataPort,
		"parser", *parserName,
		"format", *format,
		"output", *output,
		"interface", *iface)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("runner exited: %w", err)
	}

	slog.Info("shutdown complete")
	return nil
}
