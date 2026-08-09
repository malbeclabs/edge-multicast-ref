package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

const version = "0.1.0-dev"

var commit = "unknown"

func main() {
	var (
		socketPath    = flag.String("socket", "", "path to parser Unix socket (required)")
		symbolFilter  = flag.String("symbol", "", "comma-separated symbol filter (empty = all)")
		depth         = flag.Int("depth", 20, "snapshot depth (levels per side)")
		coalesceMS    = flag.Int("coalesce-ms", 50, "snapshot coalesce window in milliseconds")
		shards        = flag.Int("shards", 0, "number of instrument shards (0 = auto from GOMAXPROCS)")
		metricsAddr   = flag.String("metrics-addr", "127.0.0.1:9092", "Prometheus /metrics HTTP listen address")
		clickhouseURL = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (empty disables persistence)")
		clickhouseDB  = flag.String("clickhouse-database", "marketbyorder", "ClickHouse database")
		batchSize     = flag.Int("clickhouse-batch-size", 1000, "rows per batch flush")
		batchInterval = flag.Duration("clickhouse-batch-interval", 200*time.Millisecond, "max time between batch flushes")
		bufferSize    = flag.Int("clickhouse-buffer", 100000, "per-table channel capacity")
		verbose       = flag.Bool("v", false, "debug logging")
		showVersion   = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyorder-bot %s (%s)\n", version, commit)
		os.Exit(0)
	}
	if *socketPath == "" {
		fmt.Fprintln(os.Stderr, "error: --socket is required")
		flag.Usage()
		os.Exit(2)
	}
	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}
	_ = symbolFilter // reserved for future filtering

	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics.ServeHTTP(ctx, *metricsAddr, func(e error) { log.Println(e) })

	// ClickHouse client (nil if URL empty)
	var ch *ClickhouseClient
	if *clickhouseURL != "" {
		var err error
		ch, err = NewClickhouseClient(*clickhouseURL, *clickhouseDB, []BatcherConfig{
			{Table: "events", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "level_snapshots", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "wire_snapshots", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "instruments", BatchSize: 100, BatchInterval: 1 * time.Second, BufferSize: 1000},
			{Table: "channel_health", BatchSize: 100, BatchInterval: 1 * time.Second, BufferSize: 1000},
		}, metrics)
		if err != nil {
			log.Fatalf("clickhouse: %v", err)
		}
	}

	enq := enqueuerFor(ch)
	eventsWriter := NewEventsWriter(enq)

	n := *shards
	if n <= 0 {
		n = runtime.GOMAXPROCS(0) - 2
		if n < 1 {
			n = 1
		}
		if n > 8 {
			n = 8
		}
	}

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, eventsWriter, nil, metrics)
		sw := NewSnapshotWriter(enq, *depth, *coalesceMS, metrics, 0, func(s *Shard) func(uint32, func(*Instrument)) {
			return func(instID uint32, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[instKey{0, instID}])
			}
		}(s))
		s.sw = sw
		shardList[i] = s
		go sw.Run(ctx)
		go s.Run(ctx)
	}

	coordinator := NewCoordinator(ctx, shardList, eventsWriter, metrics)
	log.Printf("marketbyorder-bot %s sharding: shards=%d", version, n)

	// Spawn ClickHouse runner.
	if ch != nil {
		go ch.Run(ctx)
	}

	// Set up signal handler.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %v, shutting down", s)
		cancel()
	}()

	bot := NewBot(*socketPath, coordinator, metrics)
	log.Printf("marketbyorder-bot %s started: socket=%s clickhouse=%v depth=%d coalesce=%dms",
		version, *socketPath, *clickhouseURL != "", *depth, *coalesceMS)
	bot.Run(ctx)
	log.Println("shutdown complete")
}

// enqueuerFor adapts a possibly-nil *ClickhouseClient onto the interface the
// writers take.
//
// The guard is load-bearing, not defensive style. A typed nil pointer stored
// in an interface is NOT == nil, so assigning a nil *ClickhouseClient straight
// into an enqueuer field makes the writers' `ch == nil` fast path false
// forever — including under the default --clickhouse-url="", where the bot
// would then build and immediately discard a row map for every record, and
// SnapshotWritesTotal would count writes that never happened.
func enqueuerFor(ch *ClickhouseClient) enqueuer {
	if ch == nil {
		return nil
	}
	return ch
}
