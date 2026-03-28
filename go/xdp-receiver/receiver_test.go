package main

import "testing"

// buildGREUDPPacket creates a minimal GRE-encapsulated UDP packet for testing.
func buildGREUDPPacket(dstPort uint16) []byte {
	var pkt []byte

	// Ethernet header (14 bytes)
	pkt = append(pkt, make([]byte, 12)...) // dst + src MAC
	pkt = append(pkt, 0x08, 0x00)          // EtherType: IPv4

	// Outer IPv4 (20 bytes) - protocol 47 (GRE)
	pkt = append(pkt, 0x45)            // version=4, IHL=5
	pkt = append(pkt, make([]byte, 8)...) // ToS, TotalLen, ID, Flags, TTL
	pkt = append(pkt, 47)              // Protocol: GRE
	pkt = append(pkt, 0x00, 0x00)      // Header checksum
	pkt = append(pkt, 10, 0, 0, 1)     // Src IP
	pkt = append(pkt, 10, 0, 0, 2)     // Dst IP

	// GRE header (4 bytes, no optional fields)
	pkt = append(pkt, 0x00, 0x00) // Flags: none
	pkt = append(pkt, 0x08, 0x00) // Protocol: IPv4

	// Inner IPv4 (20 bytes) - protocol 17 (UDP)
	pkt = append(pkt, 0x45)            // version=4, IHL=5
	pkt = append(pkt, make([]byte, 8)...) // ToS, TotalLen, etc
	pkt = append(pkt, 17)              // Protocol: UDP
	pkt = append(pkt, 0x00, 0x00)      // Header checksum
	pkt = append(pkt, 148, 51, 0, 1)   // Src IP
	pkt = append(pkt, 233, 84, 178, 1) // Dst IP (multicast)

	// UDP header (8 bytes)
	pkt = append(pkt, 0x00, 0x00) // Src port
	pkt = append(pkt, byte(dstPort>>8), byte(dstPort&0xFF)) // Dst port
	pkt = append(pkt, 0x00, 0x00) // Length
	pkt = append(pkt, 0x00, 0x00) // Checksum

	// Payload (dummy data)
	for i := 0; i < 100; i++ {
		pkt = append(pkt, 0xAA)
	}

	return pkt
}

func TestFindUDPPayloadShredPort(t *testing.T) {
	pkt := buildGREUDPPacket(7733)
	offset, port, ok := findUDPPayload(pkt)
	if !ok {
		t.Fatal("expected to find UDP payload")
	}
	if port != 7733 {
		t.Fatalf("expected port 7733, got %d", port)
	}
	// Eth(14) + outerIP(20) + GRE(4) + innerIP(20) + UDP(8) = 66
	if offset != 66 {
		t.Fatalf("expected offset 66, got %d", offset)
	}
}

func TestFindUDPPayloadHeartbeatPort(t *testing.T) {
	pkt := buildGREUDPPacket(5765)
	_, port, ok := findUDPPayload(pkt)
	if !ok {
		t.Fatal("expected to find UDP payload")
	}
	if port != 5765 {
		t.Fatalf("expected port 5765, got %d", port)
	}
}

func TestFindUDPPayloadTruncated(t *testing.T) {
	pkt := make([]byte, 30) // Too short
	_, _, ok := findUDPPayload(pkt)
	if ok {
		t.Fatal("expected failure on truncated packet")
	}
}

func TestFindUDPPayloadGREWithKey(t *testing.T) {
	var pkt []byte

	// Ethernet (14)
	pkt = append(pkt, make([]byte, 12)...)
	pkt = append(pkt, 0x08, 0x00)

	// Outer IPv4 (20) - GRE
	pkt = append(pkt, 0x45)
	pkt = append(pkt, make([]byte, 8)...)
	pkt = append(pkt, 47)
	pkt = append(pkt, 0x00, 0x00)
	pkt = append(pkt, 10, 0, 0, 1)
	pkt = append(pkt, 10, 0, 0, 2)

	// GRE with Key flag set (8 bytes total)
	pkt = append(pkt, 0x20, 0x00)             // Flags: Key bit (0x2000)
	pkt = append(pkt, 0x08, 0x00)             // Protocol: IPv4
	pkt = append(pkt, 0x00, 0x00, 0x00, 0x01) // Key value

	// Inner IPv4 (20) - UDP
	pkt = append(pkt, 0x45)
	pkt = append(pkt, make([]byte, 8)...)
	pkt = append(pkt, 17)
	pkt = append(pkt, 0x00, 0x00)
	pkt = append(pkt, 148, 51, 0, 1)
	pkt = append(pkt, 233, 84, 178, 1)

	// UDP (8)
	pkt = append(pkt, 0x00, 0x00)
	pkt = append(pkt, 0x1E, 0x35) // 7733 in big-endian
	pkt = append(pkt, 0x00, 0x00, 0x00, 0x00)

	// Payload
	for i := 0; i < 50; i++ {
		pkt = append(pkt, 0xBB)
	}

	offset, port, ok := findUDPPayload(pkt)
	if !ok {
		t.Fatal("expected to find UDP payload")
	}
	if port != 7733 {
		t.Fatalf("expected port 7733, got %d", port)
	}
	// Eth(14) + outerIP(20) + GRE(8) + innerIP(20) + UDP(8) = 70
	if offset != 70 {
		t.Fatalf("expected offset 70, got %d", offset)
	}
}

func TestNearestPow2(t *testing.T) {
	tests := []struct {
		input, expected int
	}{
		{2048, 2048},
		{1024, 1024},
		{2000, 1024},
		{1, 1},
		{3, 2},
		{4, 4},
	}
	for _, tt := range tests {
		got := nearestPow2(tt.input)
		if got != tt.expected {
			t.Errorf("nearestPow2(%d) = %d, want %d", tt.input, got, tt.expected)
		}
	}
}
