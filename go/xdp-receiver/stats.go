package main

import "github.com/malbeclabs/edge-multicast-ref/go/internal/stats"

// XdpReceiverStats extends the base Stats with XDP-specific counters.
type XdpReceiverStats struct {
	stats.Stats

	// XDP program counters (read from BPF per-CPU map).
	XdpAttachMode       string
	XdpRedirected       uint64
	XdpPassed           uint64
	XdpErrors           uint64
	AfxdpRxFillLevel    int
	AfxdpFillStarvation uint64
}

// NewXdpReceiverStats creates a new XDP stats tracker.
func NewXdpReceiverStats(maxSlots int) *XdpReceiverStats {
	return &XdpReceiverStats{
		Stats: *stats.NewStats(maxSlots),
	}
}

// UpdateXdpCounters updates the XDP program counters from BPF map reads.
func (s *XdpReceiverStats) UpdateXdpCounters(redirected, passed, errors uint64) {
	s.XdpRedirected = redirected
	s.XdpPassed = passed
	s.XdpErrors = errors
}
