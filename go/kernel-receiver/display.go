package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/config"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/display"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/stats"
)

// RunDisplay starts the appropriate display mode and blocks until the context is cancelled.
func RunDisplay(ctx context.Context, cfg *Config, s *stats.Stats, mu *sync.RWMutex) error {
	switch cfg.Display.Mode {
	case config.DisplayModeLog:
		display.RunLogDisplay(ctx, s, mu, cfg.Display.LogIntervalSecs)
		return nil
	case config.DisplayModeTUI:
		return display.RunTUI(ctx, s, mu, cfg.Display.RefreshHz, cfg.Network.Interface, cfg.Network.MulticastGroup)
	default:
		return fmt.Errorf("unknown display mode: %s", cfg.Display.Mode)
	}
}
