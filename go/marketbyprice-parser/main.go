package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const version = "0.1.0-dev"

var commit = "unknown"

func main() {
	var (
		group        = flag.String("group", "", "multicast group IP (required)")
		refdataPort  = flag.Int("refdata-port", 0, "refdata UDP port (required)")
		mktdataPort  = flag.Int("mktdata-port", 0, "mktdata UDP port (required)")
		snapshotPort = flag.Int("snapshot-port", 0, "snapshot UDP port (required)")
		iface        = flag.String("interface", "", "network interface for multicast join (e.g., doublezero1)")
		output       = flag.String("output", "", "output target: unix:///path/to/sock or file:///path/to/log (required)")
		format       = flag.String("format", "json", "output format: json")
		parserName   = flag.String("parser", "marketbyprice", "parser name from registry")
		metricsAddr  = flag.String("metrics-addr", "", "Prometheus /metrics HTTP listen address (empty = disabled)")
		verbose      = flag.Bool("v", false, "debug logging")
		showVersion  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyprice-parser %s (%s)\n", version, commit)
		os.Exit(0)
	}

	if *group == "" || *refdataPort == 0 || *mktdataPort == 0 || *snapshotPort == 0 || *output == "" {
		fmt.Fprintln(os.Stderr, "error: --group, --refdata-port, --mktdata-port, --snapshot-port, and --output are required")
		flag.Usage()
		os.Exit(2)
	}
	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	parser, err := newParser(*parserName)
	if err != nil {
		log.Fatalf("parser: %v", err)
	}
	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.ServeHTTP(ctx, *metricsAddr, func(e error) { log.Println(e) })

	sink, err := NewSink(SinkConfig{Path: *output, Format: *format, Metrics: metrics})
	if err != nil {
		log.Fatalf("sink: %v", err)
	}
	defer sink.Close()

	runner, err := NewRunner(parser, sink, metrics, *group, *iface, *refdataPort, *mktdataPort, *snapshotPort)
	if err != nil {
		log.Fatalf("runner: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %v, shutting down", s)
		cancel()
	}()

	log.Printf("marketbyprice-parser %s started: group=%s refdata=:%d mktdata=:%d snapshot=:%d output=%s",
		version, *group, *refdataPort, *mktdataPort, *snapshotPort, *output)

	if err := runner.Run(ctx); err != nil {
		log.Fatalf("runner: %v", err)
	}
	log.Println("shutdown complete")
}
