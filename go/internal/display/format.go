// Package display provides output formatting for edge-multicast receivers.
package display

import (
	"fmt"
	"time"
)

// FormatSignaturePrefix formats an 8-byte signature prefix as "ab12..cd34"
// (first 2 bytes hex + ".." + last 2 bytes hex).
func FormatSignaturePrefix(sig [8]byte) string {
	return fmt.Sprintf("%02x%02x..%02x%02x", sig[0], sig[1], sig[6], sig[7])
}

// FormatDurationShort formats a duration in a compact human-readable form
// such as "5s", "2m30s", or "1h15m".
func FormatDurationShort(d time.Duration) string {
	if d < 0 {
		d = -d
	}

	totalSeconds := int(d.Seconds())
	if totalSeconds < 60 {
		return fmt.Sprintf("%ds", totalSeconds)
	}

	hours := totalSeconds / 3600
	minutes := (totalSeconds % 3600) / 60
	seconds := totalSeconds % 60

	if hours > 0 {
		if minutes > 0 {
			return fmt.Sprintf("%dh%dm", hours, minutes)
		}
		return fmt.Sprintf("%dh", hours)
	}
	if seconds > 0 {
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	}
	return fmt.Sprintf("%dm", minutes)
}
