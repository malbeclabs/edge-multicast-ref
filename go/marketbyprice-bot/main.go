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
)

const version = "0.1.0-dev"

var commit = "unknown"

func main() {
	var (
		socketPath   = flag.String("socket", "", "path to parser Unix socket (required)")
		symbolFilter = flag.String("symbol", "", "comma-separated symbol filter (empty = all)")
		depth        = flag.Int("depth", 20, "read-out depth (levels per side)")
		shards       = flag.Int("shards", 0, "number of instrument shards (0 = auto from GOMAXPROCS)")
		metricsAddr  = flag.String("metrics-addr", "127.0.0.1:9094", "Prometheus /metrics HTTP listen address")
		verbose      = flag.Bool("v", false, "debug logging")
		showVersion  = flag.Bool("version", false, "print version and exit")
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

	// --symbol and --depth configure the level read-out (ComputeLevels), which
	// only has a consumer once the persistence layer lands in the follow-on plan.
	// They are accepted now so deployment configs do not need to change then;
	// until then they have no effect. See README.
	_ = symbolFilter
	_ = depth

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

	shardList := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, metrics)
		shardList[i] = s
		go s.Run(ctx)
	}

	coordinator := NewCoordinator(ctx, shardList, metrics)

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
	log.Println("shutdown complete")
}
