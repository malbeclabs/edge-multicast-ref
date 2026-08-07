package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// shutdownTimeout bounds each stage of the shutdown drain, so a wedged
// ClickHouse cannot hang the process.
const shutdownTimeout = 5 * time.Second

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
	enq := enqueuerFor(ch)
	eventsWriter := NewEventsWriter(enq)

	// The ClickHouse client runs on its OWN context, deliberately NOT derived
	// from ctx. Its batchers must outlive every goroutine that can enqueue a row.
	// On cancellation a batcher drains what is buffered at that instant and
	// returns — typically in microseconds — so sharing ctx left every row the
	// shards and snapshot writers produced after that point stranded in a
	// channel: never written, never counted dropped, while the join below made
	// shutdown look clean.
	//
	// chDone closes once every batcher has drained and flushed. See the
	// "discarding buffered rows on shutdown" comment in
	// internal/clickhouse/client.go.
	chCtx, chCancel := context.WithCancel(context.Background())
	defer chCancel()
	chDone := make(chan struct{})
	if ch != nil {
		go func() {
			ch.Run(chCtx)
			close(chDone)
		}()
	} else {
		close(chDone)
	}

	// producers counts every goroutine that can enqueue a row. They were
	// previously never joined at all, so shutdown could not tell whether they had
	// stopped producing.
	var producers sync.WaitGroup

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, eventsWriter, metrics)
		// s.symbols must be set before the SnapshotWriter is constructed: the
		// writer captures s.persists as a closure, so a populated filter must
		// already be in place by the time that closure is created.
		s.symbols = parseSymbolFilter(*symbolFilter)
		// The writer's withInstrument closure needs the shard, so sw is assigned
		// after construction.
		s.sw = NewSnapshotWriter(enq, *depth, *coalesceMS, metrics, func(s *Shard) func(instKey, func(*Instrument)) {
			return func(k instKey, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[k])
			}
		}(s), s.persists)
		shardList[i] = s
		producers.Add(2)
		go func() { defer producers.Done(); s.sw.Run(ctx) }()
		go func() { defer producers.Done(); s.Run(ctx) }()
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

	drainOnShutdown(&producers, chCancel, chDone, shutdownTimeout)
	log.Println("shutdown complete")
}

// enqueuerFor adapts a possibly-nil *clickhouse.Client onto the interface the
// writers take.
//
// The guard is load-bearing, not defensive style. A typed nil pointer stored in
// an interface is NOT == nil, so assigning a nil *clickhouse.Client straight
// into an enqueuer field makes the writers' `ch == nil` fast path false forever
// — including under the default --clickhouse-url="", where the bot would then
// build and immediately discard a row map for every record and every level, and
// snapshot_writes_total would count writes that never happened.
func enqueuerFor(ch *clickhouse.Client) enqueuer {
	if ch == nil {
		return nil
	}
	return ch
}

// drainOnShutdown stops the pipeline in producer-then-consumer order.
//
// bot.Run has already returned, so the Coordinator — which writes channel_health
// and batch_boundary rows on the read loop's own goroutine — has stopped. Join
// the remaining producers, the shard and snapshot-writer goroutines, so nothing
// can enqueue another row, and only THEN cancel the client's context so its
// batchers drain a queue that is guaranteed complete. Cancelling the batchers
// first, as sharing one context did, drains an arbitrary prefix and abandons the
// rest.
//
// Both stages are bounded: an unreachable ClickHouse must degrade to data loss,
// never to a process that will not exit.
func drainOnShutdown(producers *sync.WaitGroup, chCancel context.CancelFunc, chDone <-chan struct{}, timeout time.Duration) {
	if !waitTimeout(producers, timeout) {
		log.Println("timed out waiting for shard goroutines to stop")
	}
	chCancel()
	select {
	case <-chDone:
	case <-time.After(timeout):
		log.Println("timed out waiting for clickhouse to drain buffered rows")
	}
}

// waitTimeout waits for wg, reporting false when timeout elapsed first.
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}
