//go:build !linux

package main

import (
	"net"
	"time"
)

const (
	recvTimestampKindKernelSoftware = "kernel_udp_software"
	recvTimestampKindAppFallback    = "app_udp_fallback"
)

// enableTimestamping is a no-op on non-Linux platforms.
func enableTimestamping(conn *net.UDPConn) error { return nil }

// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, time.Time, string, error) {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, time.Time{}, "", err
	}
	return n, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
