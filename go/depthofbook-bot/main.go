package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
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
		metricsAddr   = flag.String("metrics-addr", "127.0.0.1:9092", "Prometheus /metrics HTTP listen address")
		clickhouseURL = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (empty disables persistence)")
		clickhouseDB  = flag.String("clickhouse-database", "depthofbook", "ClickHouse database")
		batchSize     = flag.Int("clickhouse-batch-size", 1000, "rows per batch flush")
		batchInterval = flag.Duration("clickhouse-batch-interval", 200*time.Millisecond, "max time between batch flushes")
		bufferSize    = flag.Int("clickhouse-buffer", 100000, "per-table channel capacity")
		verbose       = flag.Bool("v", false, "debug logging")
		showVersion   = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("depthofbook-bot %s (%s)\n", version, commit)
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
	filter := NewSymbolFilter(*symbolFilter)
	if filter.Enabled() {
		log.Printf("symbol filter enabled: %s", filter.String())
	}

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

	// Channel state per channel_id (created lazily on first record).
	var (
		chMu        sync.Mutex
		channels    = map[uint8]*ChannelState{}
		snapWriters = map[uint8]*SnapshotWriter{}
	)
	getOrCreateChannel := func(id uint8) (*ChannelState, *SnapshotWriter) {
		chMu.Lock()
		defer chMu.Unlock()
		if c, ok := channels[id]; ok {
			return c, snapWriters[id]
		}
		c := NewChannelState(id)
		channels[id] = c
		sw := NewSnapshotWriter(ch, *depth, *coalesceMS, metrics, id, func(instID uint32, fn func(*Instrument)) {
			chMu.Lock()
			cs, ok := channels[id]
			chMu.Unlock()
			if !ok {
				fn(nil)
				return
			}
			cs.Mu.Lock()
			defer cs.Mu.Unlock()
			fn(cs.Instruments[instID])
		})
		snapWriters[id] = sw
		go sw.Run(ctx)
		return c, sw
	}

	eventsWriter := NewEventsWriter(ch)

	// Per-channel snapshot-in-flight context, keyed by channel_id.
	// Populated on snapshot_begin, consulted on snapshot_order, cleared on snapshot_end.
	snapCtxMu := sync.Mutex{}
	snapCtx := map[uint8]SnapshotContext{}

	dispatcher := DispatcherFunc(func(rec Record) {
		if !filter.Allow(rec) {
			return
		}
		c, sw := getOrCreateChannel(rec.ChannelID)
		c.Mu.Lock()
		defer c.Mu.Unlock()
		evs := c.Apply(rec)

		// Snapshot frame routing is unconditional per-record.
		// snapshot_begin and snapshot_order produce no ChannelEvents, so they
		// must be handled before the events loop.
		switch rec.Type {
		case "snapshot_begin":
			// Resolve exponents from refdata if available.
			var priceExp, qtyExp int8
			symbol := ""
			if def, ok := c.Refdata[rec.InstrumentID]; ok {
				symbol = def.Symbol
				priceExp = def.PriceExponent
				qtyExp = def.QtyExponent
			}
			snapCtxMu.Lock()
			snapCtx[c.ChannelID] = SnapshotContext{
				InstrumentID:      rec.InstrumentID,
				Symbol:            symbol,
				SnapshotID:        getUint32(rec.Fields, "snapshot_id"),
				AnchorSeq:         getUint64(rec.Fields, "anchor_seq"),
				TotalOrders:       getUint32(rec.Fields, "total_orders"),
				LastInstrumentSeq: getUint32(rec.Fields, "last_instrument_seq"),
				PriceExponent:     priceExp,
				QtyExponent:       qtyExp,
			}
			snapCtxMu.Unlock()
		case "snapshot_order":
			snapCtxMu.Lock()
			sctx, ok := snapCtx[c.ChannelID]
			snapCtxMu.Unlock()
			if ok {
				eventsWriter.WriteSnapshotOrder(rec, c.ChannelID, sctx)
			}
		}

		for _, ev := range evs {
			// Resolve symbol + exponents from refdata.
			symbol := ""
			var priceExp, qtyExp int8
			if def, ok := c.Refdata[ev.InstrumentID]; ok {
				symbol = def.Symbol
				priceExp = def.PriceExponent
				qtyExp = def.QtyExponent
			}

			switch rec.Type {
			case "snapshot_end":
				snapCtxMu.Lock()
				delete(snapCtx, c.ChannelID)
				snapCtxMu.Unlock()
				eventsWriter.Write(ev, c.ChannelID, symbol, priceExp, qtyExp)
			default:
				eventsWriter.Write(ev, c.ChannelID, symbol, priceExp, qtyExp)
			}

			// Mark dirty for snapshot writer if book changed.
			switch ev.Kind {
			case "applied_delta", "applied_snapshot":
				if ev.InstrumentID != 0 {
					sw.MarkDirty(ev.InstrumentID)
				}
			case "instrument_reset":
				metrics.InstrumentResetsTotal.WithLabelValues(getString(ev.Record.Fields, "reason")).Inc()
				sw.MarkDirty(ev.InstrumentID)
			case "channel_reset":
				metrics.ChannelResetsTotal.Inc()
			case "per_instrument_gap":
				metrics.PerInstrumentGapsTotal.Inc()
			}
		}
	})

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

	bot := NewBot(*socketPath, dispatcher, metrics)
	log.Printf("depthofbook-bot %s started: socket=%s clickhouse=%v depth=%d coalesce=%dms",
		version, *socketPath, *clickhouseURL != "", *depth, *coalesceMS)
	bot.Run(ctx)
	log.Println("shutdown complete")
}

// DispatcherFunc adapts a func(Record) to the Dispatcher interface.
type DispatcherFunc func(Record)

func (f DispatcherFunc) Dispatch(rec Record) { f(rec) }
