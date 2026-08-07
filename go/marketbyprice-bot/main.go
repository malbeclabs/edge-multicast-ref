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

// version and commit are both vars, not consts, so the linker can stamp them:
// `-X main.version=...` is silently ignored on a const, and a const default
// would make the Dockerfile's VERSION build arg a no-op.
var (
	version = "0.1.0-dev"
	commit  = "unknown"
)

func main() {
	var (
		socketPath   = flag.String("socket", "", "path to parser Unix socket (required)")
		symbolFilter = flag.String("symbol", "", "comma-separated symbol filter (empty = all)")
		depth        = flag.Int("depth", 20, "read-out depth (levels per side)")
		shards       = flag.Int("shards", 0, "number of instrument shards (0 = auto from GOMAXPROCS)")
		metricsAddr  = flag.String("metrics-addr", "127.0.0.1:9094", "Prometheus /metrics HTTP listen address")
		verbose      = flag.Bool("v", false, "debug logging")
		showVersion  = flag.Bool("version", false, "print version and exit")

		clickhouseURL = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (empty = persistence disabled)")
		clickhouseDB  = flag.String("clickhouse-database", "marketbyprice", "ClickHouse database")
		batchSize     = flag.Int("clickhouse-batch-size", 500, "rows per insert batch")
		batchInterval = flag.Duration("clickhouse-batch-interval", time.Second, "maximum time between insert batches")
		bufferSize    = flag.Int("clickhouse-buffer-size", 20000, "per-table row buffer; rows are dropped when full")
		coalesceMS    = flag.Int("coalesce-ms", 50, "minimum interval between level_snapshots writes per instrument")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyprice-bot %s (%s)\n", version, commit)
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

	// --symbol configures the level read-out (ComputeLevels), which only has a
	// consumer once Task 8 lands. Accepted now so deployment configs do not need
	// to change then; until then it has no effect. See README.
	_ = symbolFilter

	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics.ServeHTTP(ctx, *metricsAddr, func(e error) { log.Println(e) })

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

	ch, err := newClickhouseClient(*clickhouseURL, *clickhouseDB, *batchSize, *batchInterval, *bufferSize, metrics)
	if err != nil {
		log.Fatalf("clickhouse: %v", err)
	}
	eventsWriter := NewEventsWriter(ch)

	// chDone closes once the client's batchers have drained and flushed after
	// ctx is cancelled. Joined below before main returns, with a bounded
	// timeout, so shutdown does not silently discard the buffered tail — see
	// the "discarding buffered rows on shutdown" comment in
	// internal/clickhouse/client.go — but a wedged ClickHouse cannot hang
	// shutdown forever either.
	chDone := make(chan struct{})
	if ch != nil {
		go func() {
			ch.Run(ctx)
			close(chDone)
		}()
	} else {
		close(chDone)
	}

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, eventsWriter, metrics)
		// The writer's withInstrument closure needs the shard, so sw is assigned
		// after construction.
		s.sw = NewSnapshotWriter(ch, *depth, *coalesceMS, metrics, func(s *Shard) func(instKey, func(*Instrument)) {
			return func(k instKey, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[k])
			}
		}(s))
		shardList[i] = s
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}

	coordinator := NewCoordinator(ctx, shardList, eventsWriter, metrics)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %v, shutting down", s)
		cancel()
	}()

	bot := NewBot(*socketPath, coordinator, metrics)
	log.Printf("marketbyprice-bot %s started: socket=%s shards=%d metrics=%s",
		version, *socketPath, n, *metricsAddr)
	bot.Run(ctx)

	// bot.Run only returns once ctx is done, so the client's batchers are
	// already draining; wait for them to finish rather than exiting out from
	// under them, bounded so a wedged ClickHouse cannot hang shutdown forever.
	select {
	case <-chDone:
	case <-time.After(5 * time.Second):
		log.Println("timed out waiting for clickhouse to drain buffered rows")
	}
	log.Println("shutdown complete")
}
