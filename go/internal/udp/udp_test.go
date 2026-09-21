package udp

import (
	"net"
	"net/netip"
	"testing"
	"time"
)

func TestSrcAddr(t *testing.T) {
	tests := []struct {
		name string
		addr *net.UDPAddr
		want netip.Addr
	}{
		{
			name: "nil address yields the zero Addr",
			addr: nil,
			want: netip.Addr{},
		},
		{
			name: "IPv4 address",
			addr: &net.UDPAddr{IP: net.IPv4(10, 0, 0, 7), Port: 4000},
			want: netip.MustParseAddr("10.0.0.7"),
		},
		{
			name: "IPv4-in-IPv6 is unmapped, so one publisher is one key",
			addr: &net.UDPAddr{IP: net.ParseIP("::ffff:10.0.0.7"), Port: 4000},
			want: netip.MustParseAddr("10.0.0.7"),
		},
		{
			name: "IPv6 address",
			addr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 4000},
			want: netip.MustParseAddr("2001:db8::1"),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := srcAddr(tc.addr)
			if got != tc.want {
				t.Errorf("srcAddr() = %v, want %v", got, tc.want)
			}
			if got.Is4In6() {
				t.Errorf("srcAddr() returned a v4-in-v6 address: %v", got)
			}
		})
	}
}

// TestReadDatagram_Loopback reads a real datagram over the loopback interface,
// which is the only way to cover the platform read path end to end.
func TestReadDatagram_Loopback(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer conn.Close()

	if err := EnableTimestamping(conn); err != nil {
		t.Fatalf("EnableTimestamping: %v", err)
	}

	sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	defer sender.Close()

	payload := []byte("one datagram")
	before := time.Now().UTC().Add(-time.Second)
	if _, err := sender.Write(payload); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}
	buf := make([]byte, 2048)
	n, src, recvTime, kind, err := ReadDatagram(conn, buf)
	if err != nil {
		t.Fatalf("ReadDatagram: %v", err)
	}

	if n != len(payload) {
		t.Errorf("read %d bytes, want %d", n, len(payload))
	}
	if string(buf[:n]) != string(payload) {
		t.Errorf("read %q, want %q", buf[:n], payload)
	}
	if want := netip.MustParseAddr("127.0.0.1"); src != want {
		t.Errorf("source address %v, want %v", src, want)
	}
	if kind != RecvTimestampKindKernelSoftware && kind != RecvTimestampKindAppFallback {
		t.Errorf("receive-timestamp kind %q is neither of the two defined kinds", kind)
	}
	if recvTime.Before(before) || recvTime.After(time.Now().UTC().Add(time.Second)) {
		t.Errorf("receive timestamp %v is outside the window the read happened in", recvTime)
	}
	if recvTime.Location() != time.UTC {
		t.Errorf("receive timestamp is in %v, want UTC", recvTime.Location())
	}
}

// TestReadDatagram_ReportsReadError pins that a failed read reports no address,
// no timestamp and no kind, so a caller cannot mistake them for real values.
func TestReadDatagram_ReportsReadError(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer conn.Close()

	// A deadline already in the past makes the next read fail immediately.
	if err := conn.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}

	buf := make([]byte, 2048)
	n, src, recvTime, kind, err := ReadDatagram(conn, buf)
	if err == nil {
		t.Fatal("expected a read error, got nil")
	}
	if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
		t.Errorf("expected a timeout error, got %v", err)
	}
	if n != 0 || src.IsValid() || !recvTime.IsZero() || kind != "" {
		t.Errorf("error return carried values: n=%d src=%v recvTime=%v kind=%q", n, src, recvTime, kind)
	}
}
