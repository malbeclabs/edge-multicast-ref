package display

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/stats"
)

// RunLogDisplay periodically prints shred stats to stderr until the context
// is cancelled. It sleeps in 100ms increments to allow prompt shutdown, and
// prints a report every intervalSecs seconds. Only new (previously unreported)
// slots are printed each cycle.
func RunLogDisplay(ctx context.Context, s *stats.Stats, mu *sync.RWMutex, intervalSecs int) {
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
			sigStr := FormatSignaturePrefix(ss.SignaturePrefix)
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
			"[stats] shreds/sec=%.0f data=%d coding=%d errors=%d heartbeats=%d (last: %s)\n",
			rate,
			s.TotalDataShreds, s.TotalCodingShreds,
			s.ParseErrors, s.TotalHeartbeats,
			lastHBStr,
		)

		mu.Unlock()
	}
}
