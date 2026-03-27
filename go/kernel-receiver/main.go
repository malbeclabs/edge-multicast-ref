package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/stats"
)

func main() {
	cli := ParseFlags()
	cfg, err := LoadConfig(cli.ConfigPath, cli)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "edge-multicast-receiver (Go) v0.1.0\n")
	fmt.Fprintf(os.Stderr, "Interface: %s, Multicast: %s, Shred port: %d, Heartbeat port: %d\n",
		cfg.Network.Interface, cfg.Network.MulticastGroup,
		cfg.Network.ShredPort, cfg.Network.HeartbeatPort)
	fmt.Fprintf(os.Stderr, "Display mode: %s\n", cfg.Display.Mode)

	s := stats.NewStats(cfg.Stats.MaxSlots)
	var mu sync.RWMutex

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := RunRecvLoop(ctx, cfg, s, &mu); err != nil {
			fmt.Fprintf(os.Stderr, "Receiver error: %v\n", err)
		}
	}()

	RunDisplay(ctx, cfg, s, &mu)

	cancel()
	wg.Wait()
	fmt.Fprintln(os.Stderr, "Shutdown complete.")
}
