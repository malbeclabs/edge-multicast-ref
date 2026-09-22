//go:build !linux

package udp

import (
	"net"
	"net/netip"
	"time"
)

// Receive-timestamp kinds reported by ReadDatagram. Only the fallback is
// reachable on a non-Linux platform, and both are declared so a parser's
// labels carry the same vocabulary everywhere.
const (
	RecvTimestampKindKernelSoftware = "kernel_udp_software"
	RecvTimestampKindAppFallback    = "app_udp_fallback"
)

// EnableTimestamping is a no-op on non-Linux platforms.
func EnableTimestamping(_ *net.UDPConn) error { return nil }

// A Reader reads datagrams off a socket. There are no control messages to read
// on a non-Linux platform, so it carries no buffer, but a receive goroutine
// still makes its own so the two platforms are used the same way.
type Reader struct{}

// NewReader returns a Reader for one receive goroutine.
func NewReader() *Reader { return &Reader{} }

// ReadDatagram falls back to application time on non-Linux platforms.
func (r *Reader) ReadDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	return n, srcAddr(addr), time.Now().UTC(), RecvTimestampKindAppFallback, nil
}
