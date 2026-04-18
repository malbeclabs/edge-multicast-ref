package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"
)

const (
	maxDatagramSize = 65535
	summaryInterval = 30 * time.Second
)

type RunnerConfig struct {
	GroupIP        net.IP
	MarketdataPort int
	RefdataPort    int
	Interface      string // network interface to join multicast on (optional)
	Parser         Parser
	Sink           OutputSink
}

type Runner struct {
	cfg              RunnerConfig
	recordsWritten   atomic.Uint64
	firstFrameLogged atomic.Bool
}

func NewRunner(cfg RunnerConfig) *Runner {
	return &Runner{cfg: cfg}
}

// Run starts listening for multicast packets on both the marketdata and
// refdata ports. It blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- r.listenPort(ctx, r.cfg.RefdataPort, "refdata")
	}()

	go func() {
		errCh <- r.listenPort(ctx, r.cfg.MarketdataPort, "marketdata")
	}()

	go r.logPeriodicSummary(ctx)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (r *Runner) logPeriodicSummary(ctx context.Context) {
	ticker := time.NewTicker(summaryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			slog.Info("runner summary",
				"records_written", r.recordsWritten.Load(),
				"buffered", r.cfg.Parser.Buffered(),
				"instruments_known", r.cfg.Parser.InstrumentCount())
		}
	}
}

func (r *Runner) listenPort(ctx context.Context, port int, label string) error {
	addr := &net.UDPAddr{
		IP:   r.cfg.GroupIP,
		Port: port,
	}

	var iface *net.Interface
	if r.cfg.Interface != "" {
		var err error
		iface, err = net.InterfaceByName(r.cfg.Interface)
		if err != nil {
			return fmt.Errorf("resolving interface %q: %w", r.cfg.Interface, err)
		}
	}

	conn, err := net.ListenMulticastUDP("udp4", iface, addr)
	if err != nil {
		return fmt.Errorf("joining multicast group %s port %d: %w", r.cfg.GroupIP, port, err)
	}
	defer conn.Close()

	pc := ipv4.NewPacketConn(conn)
	if err := pc.SetControlMessage(ipv4.FlagDst, true); err != nil {
		slog.Warn("could not set control message flag", "error", err)
	}

	slog.Info("listening for multicast", "group", r.cfg.GroupIP, "port", port, "label", label,
		"interface", r.cfg.Interface)

	buf := make([]byte, maxDatagramSize)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("read error", "port", label, "error", err)
			continue
		}

		records, err := r.cfg.Parser.Parse(buf[:n])
		if err != nil {
			slog.Warn("parse error", "port", label, "error", err)
			continue
		}

		if len(records) > 0 {
			if r.firstFrameLogged.CompareAndSwap(false, true) {
				slog.Info("parser producing records",
					"port", label,
					"first_batch_size", len(records))
			}
			if err := r.cfg.Sink.Write(records); err != nil {
				slog.Error("sink write error", "error", err)
				continue
			}
			r.recordsWritten.Add(uint64(len(records)))
		}
	}
}
