package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/config"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/display"
)

// RunDisplay starts the appropriate display mode and blocks until the context is cancelled.
func RunDisplay(ctx context.Context, cfg *Config, s *XdpReceiverStats, mu *sync.RWMutex) error {
	switch cfg.Display.Mode {
	case config.DisplayModeLog:
		RunXdpLogDisplay(ctx, s, mu, cfg.Display.LogIntervalSecs)
		return nil
	case config.DisplayModeTUI:
		// TUI uses the base display with an extra XDP stats panel.
		return RunXdpTUI(ctx, cfg, s, mu)
	default:
		return fmt.Errorf("unknown display mode: %s", cfg.Display.Mode)
	}
}

// RunXdpTUI starts the TUI with an extra XDP stats panel.
func RunXdpTUI(ctx context.Context, cfg *Config, s *XdpReceiverStats, mu *sync.RWMutex) error {
	return display.RunTUI(ctx, &s.Stats, mu, cfg.Display.RefreshHz,
		cfg.Network.PhysicalInterface, cfg.Network.MulticastGroup)
}

// RunXdpLogDisplay periodically prints shred stats with XDP counters to stderr.
func RunXdpLogDisplay(ctx context.Context, s *XdpReceiverStats, mu *sync.RWMutex, intervalSecs int) {
	interval := time.Duration(intervalSecs) * time.Second
	lastReport := time.Now()
	reportedSlots := make(map[uint64]struct{})
	var lastTotalShreds uint64

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		time.Sleep(100 * time.Millisecond)

		elapsed := time.Since(lastReport)
		if elapsed < interval {
			continue
		}
		lastReport = time.Now()

		mu.Lock()

		// Print new slot lines.
		recent := s.RecentSlots()
		for _, ss := range recent {
			if _, seen := reportedSlots[ss.Slot]; seen {
				continue
			}
			reportedSlots[ss.Slot] = struct{}{}

			ageMs := time.Since(ss.FirstSeen).Milliseconds()
			sigStr := display.FormatSignaturePrefix(ss.SignaturePrefix)
			fmt.Fprintf(os.Stderr,
				"slot=%d sig=%s data=%d coding=%d fec_sets=%d age_ms=%d\n",
				ss.Slot, sigStr,
				ss.DataShredCount, ss.CodingShredCount,
				ss.FECSetCount, ageMs,
			)
		}

		// Compute rate from delta since last report.
		totalNow := s.TotalDataShreds + s.TotalCodingShreds
		elapsedSecs := elapsed.Seconds()
		var rate float64
		if elapsedSecs > 0 {
			rate = float64(totalNow-lastTotalShreds) / elapsedSecs
		}
		lastTotalShreds = totalNow
		lastHBStr := "never"
		if s.LastHeartbeat != nil {
			lastHBMs := time.Since(*s.LastHeartbeat).Milliseconds()
			lastHBStr = fmt.Sprintf("%dms ago", lastHBMs)
		}
		fmt.Fprintf(os.Stderr,
			"[stats] shreds/sec=%.0f data=%d coding=%d errors=%d heartbeats=%d (last: %s) xdp_mode=%s redirected=%d passed=%d xdp_errors=%d fill_starvation=%d\n",
			rate,
			s.TotalDataShreds, s.TotalCodingShreds,
			s.ParseErrors, s.TotalHeartbeats,
			lastHBStr,
			s.XdpAttachMode,
			s.XdpRedirected, s.XdpPassed, s.XdpErrors,
			s.AfxdpFillStarvation,
		)

		mu.Unlock()
	}
}
