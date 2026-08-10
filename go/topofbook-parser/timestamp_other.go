//go:build !linux

package main

import (
	"net"
	"net/netip"
	"time"
)

const (
	recvTimestampKindKernelSoftware = "kernel_udp_software"
	recvTimestampKindAppFallback    = "app_udp_fallback"
)

func enableTimestamping(_ *net.UDPConn) error {
	return nil
}

// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	return n, srcAddr(addr), time.Now().UTC(), recvTimestampKindAppFallback, nil
}
