package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	cli := ParseFlags()
	cfg, err := LoadConfig(cli.ConfigPath, cli)
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}

	frameCount := cfg.FrameCount()
	fmt.Fprintf(os.Stderr, "edge-multicast-xdp-receiver (Go) v0.1.0\n")
	fmt.Fprintf(os.Stderr, "Interface: %s, Multicast: %s, Shred port: %d, Heartbeat port: %d\n",
		cfg.Network.PhysicalInterface, cfg.Network.MulticastGroup,
		cfg.Network.ShredPort, cfg.Network.HeartbeatPort)
	fmt.Fprintf(os.Stderr, "XDP mode: %s, RX queue: %d, UMEM: %dMB (%d frames x %d bytes)\n",
		cfg.Xdp.XdpMode, cfg.Xdp.RxQueue,
		cfg.Xdp.UmemSize/(1024*1024), frameCount, cfg.Xdp.FrameSize)
	fmt.Fprintf(os.Stderr, "Display mode: %s\n", cfg.Display.Mode)

	s := NewXdpReceiverStats(cfg.Stats.MaxSlots)
	var mu sync.RWMutex

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Load and attach XDP program.
	handle, mode, err := AttachXDP(cfg)
	if err != nil {
		log.Fatalf("XDP attach: %v", err)
	}
	defer handle.Close()
	s.XdpAttachMode = mode

	// 2. Create AF_XDP socket.
	sock, err := NewAfXdpSocket(cfg)
	if err != nil {
		log.Fatalf("AF_XDP socket: %v", err)
	}
	defer sock.Close()

	// 3. Register socket in XSKMAP.
	if err := RegisterXskSocket(&handle.Objs, cfg.Xdp.RxQueue, sock.FD()); err != nil {
		log.Fatalf("Register XSK socket: %v", err)
	}
	fmt.Fprintf(os.Stderr, "AF_XDP socket registered in XSKMAP (queue %d, fd %d)\n",
		cfg.Xdp.RxQueue, sock.FD())

	// 4. Spawn receiver goroutine.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := RunRecvLoop(ctx, cfg, s, &mu, handle, sock); err != nil {
			fmt.Fprintf(os.Stderr, "Receiver error: %v\n", err)
		}
	}()

	// 5. Run display (blocks until context is cancelled).
	if err := RunDisplay(ctx, cfg, s, &mu); err != nil {
		fmt.Fprintf(os.Stderr, "Display error: %v\n", err)
	}

	cancel()
	wg.Wait()
	fmt.Fprintln(os.Stderr, "Shutdown complete.")
}
