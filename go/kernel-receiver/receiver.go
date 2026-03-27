package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"golang.org/x/net/ipv4"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/shred"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/stats"
)

// resolveInterfaceIP returns the first IPv4 address for the named interface.
// Falls back to 0.0.0.0 if the interface or address cannot be determined.
func resolveInterfaceIP(ifaceName string) net.IP {
	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return net.IPv4zero
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return net.IPv4zero
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil && ip.To4() != nil {
			return ip
		}
	}
	return net.IPv4zero
}

// createMulticastConn creates a UDP connection joined to the given multicast group.
func createMulticastConn(port uint16, multicastGroup net.IP, iface *net.Interface, recvBufSize int) (*net.UDPConn, error) {
	// Listen on all interfaces for the given port.
	conn, err := net.ListenPacket("udp4", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen udp4 :%d: %w", port, err)
	}

	// Join multicast group via ipv4 PacketConn.
	// On point-to-point interfaces (like GRE tunnels) multicast join may fail
	// because the interface lacks the MULTICAST flag. This is harmless — packets
	// arrive directly on the tunnel without IGMP membership.
	p := ipv4.NewPacketConn(conn)
	if err := p.JoinGroup(iface, &net.UDPAddr{IP: multicastGroup}); err != nil {
		log.Printf("warning: multicast join %s on %s failed (expected on GRE tunnels): %v",
			multicastGroup, iface.Name, err)
	}

	// Extract the underlying *net.UDPConn.
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("unexpected conn type from ListenPacket")
	}

	// Set receive buffer size.
	if err := udpConn.SetReadBuffer(recvBufSize); err != nil {
		log.Printf("warning: failed to set receive buffer to %d: %v", recvBufSize, err)
	}

	return udpConn, nil
}

// RunRecvLoop runs the shred and heartbeat receive loops until the context is cancelled.
func RunRecvLoop(ctx context.Context, cfg *Config, s *stats.Stats, mu *sync.RWMutex) error {
	multicastGroup := net.ParseIP(cfg.Network.MulticastGroup)
	if multicastGroup == nil {
		return fmt.Errorf("invalid multicast group IP: %s", cfg.Network.MulticastGroup)
	}

	iface, err := net.InterfaceByName(cfg.Network.Interface)
	if err != nil {
		return fmt.Errorf("interface %s: %w", cfg.Network.Interface, err)
	}

	shredConn, err := createMulticastConn(cfg.Network.ShredPort, multicastGroup, iface, cfg.Network.RecvBufferSize)
	if err != nil {
		return fmt.Errorf("shred socket: %w", err)
	}

	heartbeatConn, err := createMulticastConn(cfg.Network.HeartbeatPort, multicastGroup, iface, cfg.Network.RecvBufferSize)
	if err != nil {
		shredConn.Close()
		return fmt.Errorf("heartbeat socket: %w", err)
	}

	var wg sync.WaitGroup

	// Shred receive goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			shredConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, _, err := shredConn.ReadFromUDP(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					select {
					case <-ctx.Done():
						return
					default:
						continue
					}
				}
				select {
				case <-ctx.Done():
					return
				default:
					log.Printf("shred read error: %v", err)
					continue
				}
			}
			parsed, err := shred.ParseShred(buf[:n])
			if err != nil {
				mu.Lock()
				s.RecordParseError()
				mu.Unlock()
				continue
			}
			mu.Lock()
			s.RecordShred(parsed.Slot, parsed.IsData, parsed.Index, parsed.FECSetIndex, parsed.Signature)
			mu.Unlock()
		}
	}()

	// Heartbeat receive goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 2048)
		for {
			heartbeatConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, _, err := heartbeatConn.ReadFromUDP(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					select {
					case <-ctx.Done():
						return
					default:
						continue
					}
				}
				select {
				case <-ctx.Done():
					return
				default:
					log.Printf("heartbeat read error: %v", err)
					continue
				}
			}
			mu.Lock()
			s.RecordHeartbeat()
			mu.Unlock()
		}
	}()

	// Wait for cancellation, then clean up.
	<-ctx.Done()
	shredConn.Close()
	heartbeatConn.Close()
	wg.Wait()
	return nil
}
