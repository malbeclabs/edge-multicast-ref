// Package udp reads datagrams off a UDP socket with the most accurate receive
// timestamp the platform offers.
//
// On Linux that is the kernel's SO_TIMESTAMPNS value, taken from the control
// message that arrives with the datagram; elsewhere it is application time
// read just after the datagram. ReadDatagram names which one it returned, so a
// parser can label its latency metrics with the clock they were measured
// against.
package udp

import (
	"net"
	"net/netip"
)

// srcAddr normalises a datagram's sender address so one publisher always
// produces one map key. Shared by both build-tagged ReadDatagram variants.
func srcAddr(addr *net.UDPAddr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	return addr.AddrPort().Addr().Unmap()
}
