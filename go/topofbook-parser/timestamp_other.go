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

func enableTimestamping(_ *net.UDPConn) error {
	return nil
}

func readDatagram(conn *net.UDPConn, buf []byte) (int, time.Time, string, error) {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, time.Time{}, "", err
	}
	return n, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
