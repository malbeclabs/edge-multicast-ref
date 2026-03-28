package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ifaceName := flag.String("i", "", "physical NIC to attach to (required)")
	multicastIP := flag.String("g", "", "multicast group IP to decap (default: all GRE)")
	xdpMode := flag.String("m", "auto", "XDP attach mode: native, skb, auto")
	statsInterval := flag.Duration("s", time.Second, "stats reporting interval (0 to disable)")
	flag.Parse()

	if *ifaceName == "" {
		fmt.Fprintln(os.Stderr, "error: -i <interface> is required")
		flag.Usage()
		os.Exit(1)
	}

	groupDesc := "all GRE"
	if *multicastIP != "" {
		groupDesc = *multicastIP
	}
	fmt.Fprintf(os.Stderr, "gre-decap: interface=%s group=%s mode=%s\n", *ifaceName, groupDesc, *xdpMode)

	handle, actualMode, err := AttachDecap(*ifaceName, *xdpMode, *multicastIP)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	defer handle.Close()

	fmt.Fprintf(os.Stderr, "XDP program attached to %s (mode: %s), decapping %s\n", *ifaceName, actualMode, groupDesc)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *statsInterval <= 0 {
		// No stats, just wait for signal.
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(*statsInterval)
	defer ticker.Stop()

	var prevDecapped, prevPassed uint64
	var prevTime time.Time

	for {
		select {
		case <-ctx.Done():
			// Print final stats.
			decapped, passed, errors, err := handle.ReadStats()
			if err == nil {
				fmt.Fprintf(os.Stderr, "\nFinal: decapped=%d  passed=%d  errors=%d\n", decapped, passed, errors)
			}
			return
		case now := <-ticker.C:
			decapped, passed, errors, err := handle.ReadStats()
			if err != nil {
				fmt.Fprintf(os.Stderr, "stats error: %v\n", err)
				continue
			}

			var pps string
			if !prevTime.IsZero() {
				dt := now.Sub(prevTime).Seconds()
				if dt > 0 {
					rate := float64(decapped-prevDecapped) / dt
					pps = fmt.Sprintf("  (%.0f decap/s, %.0f pass/s)", rate, float64(passed-prevPassed)/dt)
				}
			}
			fmt.Printf("decapped=%-10d  passed=%-10d  errors=%-6d%s\n", decapped, passed, errors, pps)
			prevDecapped = decapped
			prevPassed = passed
			prevTime = now
		}
	}
}
