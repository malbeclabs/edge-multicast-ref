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

// config is the command line as run needs it.
type config struct {
	group        string
	iface        string
	refdataPort  int
	mktdataPort  int
	snapshotPort int
	output       string
	format       string
	parserName   string
	metricsAddr  string
}

// portRunner serves the feed's ports until the caller cancels the context or
// one of the ports fails fatally.
type portRunner interface {
	Run(ctx context.Context) error
}

// runnerFactory builds the portRunner run serves the feed with. run takes one
// as a parameter so a test can stand in a runner that fails the way a fatal
// read does, and check what the sink has written out by the time run returns.
type runnerFactory func(parser Parser, sink OutputSink, metrics *Metrics, cfg config) (portRunner, error)

// newPortRunner is the runnerFactory the process itself runs on.
func newPortRunner(parser Parser, sink OutputSink, metrics *Metrics, cfg config) (portRunner, error) {
	return NewRunner(parser, sink, metrics, cfg.group, cfg.iface, cfg.refdataPort, cfg.mktdataPort, cfg.snapshotPort)
}

func main() {
	var (
		group        = flag.String("group", "", "multicast group IP (required)")
		refdataPort  = flag.Int("refdata-port", 0, "refdata UDP port (required)")
		mktdataPort  = flag.Int("mktdata-port", 0, "mktdata UDP port (required)")
		snapshotPort = flag.Int("snapshot-port", 0, "snapshot UDP port (required)")
		iface        = flag.String("interface", "", "network interface for multicast join (e.g., doublezero1)")
		output       = flag.String("output", "", "output target: unix:///path/to/sock or file:///path/to/log (required)")
		format       = flag.String("format", "json", "output format: json")
		parserName   = flag.String("parser", "marketbyorder", "parser name from registry")
		metricsAddr  = flag.String("metrics-addr", "", "Prometheus /metrics HTTP listen address (empty = disabled)")
		verbose      = flag.Bool("v", false, "debug logging")
		showVersion  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyorder-parser %s (%s)\n", version, commit)
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

	cfg := config{
		group:        *group,
		iface:        *iface,
		refdataPort:  *refdataPort,
		mktdataPort:  *mktdataPort,
		snapshotPort: *snapshotPort,
		output:       *output,
		format:       *format,
		parserName:   *parserName,
		metricsAddr:  *metricsAddr,
	}

	// Every failure comes back as an error so that the defers inside run
	// unwind before the process reports it. Exiting from inside run — which
	// log.Fatalf does — would skip the sink's Close, and a socket sink holds
	// the batches already queued for each connected client until Close drains
	// them.
	if err := run(cfg, newPortRunner); err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
	log.Println("shutdown complete")
}

// run is the body of the process: it builds the parser, the sink and the
// runner, serves the feed, and returns whatever failed.
//
// A fatal read on one port is a path run is expected to take, since it winds
// down the other ports rather than leaving them running, so it has to return
// like any other failure and let the deferred Close release the sink.
func run(cfg config, newRunner runnerFactory) error {
	parser, err := newParser(cfg.parserName)
	if err != nil {
		return fmt.Errorf("parser: %w", err)
	}
	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.ServeHTTP(ctx, cfg.metricsAddr, func(e error) { log.Println(e) })

	sink, err := NewSink(SinkConfig{Path: cfg.output, Format: cfg.format, Metrics: metrics})
	if err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	defer sink.Close()

	runner, err := newRunner(parser, sink, metrics, cfg)
	if err != nil {
		return fmt.Errorf("runner: %w", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)
	go func() {
		select {
		case s := <-sigs:
			log.Printf("received %v, shutting down", s)
			cancel()
		case <-ctx.Done():
		}
	}()

	log.Printf("marketbyorder-parser %s started: group=%s refdata=:%d mktdata=:%d snapshot=:%d output=%s",
		version, cfg.group, cfg.refdataPort, cfg.mktdataPort, cfg.snapshotPort, cfg.output)

	if err := runner.Run(ctx); err != nil {
		return fmt.Errorf("runner: %w", err)
	}
	return nil
}
