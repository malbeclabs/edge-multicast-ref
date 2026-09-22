//go:build linux

package udp

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestExtractKernelTimestamp_ParsesScmTimestampns(t *testing.T) {
	want := time.Unix(1717689600, 123456789).UTC()
	data := make([]byte, 16)
	binary.NativeEndian.PutUint64(data[0:8], uint64(want.Unix()))
	binary.NativeEndian.PutUint64(data[8:16], uint64(want.Nanosecond()))

	oob := buildCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPNS, data)

	got, ok := extractKernelTimestamp(oob)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestExtractKernelTimestamp_EmptyReturnsFalse(t *testing.T) {
	if _, ok := extractKernelTimestamp(nil); ok {
		t.Fatal("expected ok=false for empty oob")
	}
}

// TestExtractKernelTimestamp_IgnoresOtherControlMessages covers the two skip
// rules: a control message that is not SCM_TIMESTAMPNS, and one that is but
// carries fewer than the 16 bytes the two u64s need.
func TestExtractKernelTimestamp_IgnoresOtherControlMessages(t *testing.T) {
	data := make([]byte, 16)
	binary.NativeEndian.PutUint64(data[0:8], 1717689600)

	tests := []struct {
		name  string
		level int
		typ   int
		data  []byte
	}{
		{"another level", unix.SOL_IP, unix.SCM_TIMESTAMPNS, data},
		{"another type", unix.SOL_SOCKET, unix.SCM_RIGHTS, data},
		{"truncated payload", unix.SOL_SOCKET, unix.SCM_TIMESTAMPNS, data[:8]},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if ts, ok := extractKernelTimestamp(buildCmsg(tc.level, tc.typ, tc.data)); ok {
				t.Fatalf("expected ok=false, got %v", ts)
			}
		})
	}
}

// enablePktinfo turns on the second control message a parser asks for.
// topofbook-parser asks for it as ipv4.FlagDst, which x/net resolves on Linux
// to exactly this setsockopt; setting it directly keeps the udp package's
// tests free of a dependency on x/net.
func enablePktinfo(t *testing.T, conn *net.UDPConn) {
	t.Helper()
	rawConn, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var setsockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		setsockoptErr = unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_PKTINFO, 1)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if setsockoptErr != nil {
		t.Fatalf("setting IP_PKTINFO: %v", setsockoptErr)
	}
}

// loopbackPair returns a listening socket with every control message the
// parsers ask for enabled, and a sender connected to it.
func loopbackPair(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	enablePktinfo(t, conn)
	if err := EnableTimestamping(conn); err != nil {
		t.Fatalf("EnableTimestamping: %v", err)
	}

	sender, err := net.DialUDP("udp4", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatalf("dialling: %v", err)
	}
	t.Cleanup(func() { sender.Close() })

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}
	return conn, sender
}

// TestControlBuffer_HoldsEveryRequestedControlMessage asks the kernel itself
// whether a Reader's control buffer was big enough. It enables both control
// messages the parsers enable on a real socket, sends a datagram, and reads it
// into the very buffer a Reader carries.
//
// When that buffer is too small the kernel writes what fits, discards the rest
// and raises MSG_CTRUNC — and ReadMsgUDP still returns a nil error, which is
// why ReadDatagram cannot see the loss and the receive timestamp can degrade
// to the userspace fallback for a whole parser with nothing reported. Which
// control message loses depends on the order the kernel emits them in, so the
// assertion is that none was discarded, not that one of them survived.
func TestControlBuffer_HoldsEveryRequestedControlMessage(t *testing.T) {
	conn, sender := loopbackPair(t)

	if _, err := sender.Write([]byte("kernel please stamp this")); err != nil {
		t.Fatalf("sending: %v", err)
	}

	// The buffer under test is the one a Reader carries, so the sizing is taken
	// off the production path rather than restated here.
	oob := NewReader().oob
	buf := make([]byte, 2048)
	_, oobn, flags, _, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		t.Fatalf("ReadMsgUDP: %v", err)
	}
	if flags&unix.MSG_CTRUNC != 0 {
		t.Errorf("the kernel raised MSG_CTRUNC: the %d-byte control buffer could not hold every "+
			"control message the socket asked for, so one was discarded with no error", len(oob))
	}

	cmsgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		t.Fatalf("parsing the control messages: %v", err)
	}
	var timestampns, pktinfo bool
	for _, cmsg := range cmsgs {
		switch {
		case cmsg.Header.Level == unix.SOL_SOCKET && cmsg.Header.Type == unix.SCM_TIMESTAMPNS:
			if len(cmsg.Data) < scmTimestampnsLen {
				t.Errorf("SCM_TIMESTAMPNS carries %d bytes, want at least %d: its payload was truncated",
					len(cmsg.Data), scmTimestampnsLen)
			}
			timestampns = true
		case cmsg.Header.Level == unix.SOL_IP && cmsg.Header.Type == unix.IP_PKTINFO:
			if len(cmsg.Data) < unix.SizeofInet4Pktinfo {
				t.Errorf("IP_PKTINFO carries %d bytes, want at least %d: its payload was truncated",
					len(cmsg.Data), unix.SizeofInet4Pktinfo)
			}
			pktinfo = true
		}
	}
	if !timestampns {
		t.Error("SCM_TIMESTAMPNS did not arrive, so ReadDatagram would report the userspace fallback")
	}
	if !pktinfo {
		t.Error("IP_PKTINFO did not arrive although the socket asked for it")
	}
}

// TestReadDatagram_ReportsKernelTimestamp pins the timestamp quality the
// parsers label their metrics with, on a socket configured the way
// topofbook-parser configures its own: the kernel's own value, never the
// userspace fallback.
//
// On a kernel that emits SCM_TIMESTAMPNS ahead of the IP control messages this
// still passes with a one-message control buffer, because the message
// discarded there is IP_PKTINFO. It guards the reverse order, where the
// timestamp is the one lost; the sizing itself is held by
// TestControlBuffer_HoldsEveryRequestedControlMessage.
func TestReadDatagram_ReportsKernelTimestamp(t *testing.T) {
	conn, sender := loopbackPair(t)

	// One Reader across several datagrams, the way a receive goroutine uses it,
	// so a control buffer left in a bad state by the previous read shows up.
	reader := NewReader()
	buf := make([]byte, 2048)
	for i := range 3 {
		if _, err := sender.Write([]byte("kernel please stamp this")); err != nil {
			t.Fatalf("sending datagram %d: %v", i, err)
		}
		_, _, recvTime, kind, err := reader.ReadDatagram(conn, buf)
		if err != nil {
			t.Fatalf("ReadDatagram %d: %v", i, err)
		}
		if kind != RecvTimestampKindKernelSoftware {
			t.Errorf("datagram %d: receive-timestamp kind %q, want %q",
				i, kind, RecvTimestampKindKernelSoftware)
		}
		if recvTime.IsZero() {
			t.Errorf("datagram %d: receive timestamp is zero", i)
		}
	}
}

// TestEnableTimestamping_SetsTheSocketOption reads the option back off the
// socket, so a call that silently stopped setting it is visible.
func TestEnableTimestamping_SetsTheSocketOption(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	defer conn.Close()

	if err := EnableTimestamping(conn); err != nil {
		t.Fatalf("EnableTimestamping: %v", err)
	}

	rawConn, err := conn.SyscallConn()
	if err != nil {
		t.Fatalf("SyscallConn: %v", err)
	}
	var (
		value     int
		getoptErr error
	)
	if err := rawConn.Control(func(fd uintptr) {
		value, getoptErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPNS)
	}); err != nil {
		t.Fatalf("Control: %v", err)
	}
	if getoptErr != nil {
		t.Fatalf("GetsockoptInt: %v", getoptErr)
	}
	if value == 0 {
		t.Error("SO_TIMESTAMPNS is not set on the socket")
	}
}

// buildCmsg constructs a single socket control message for testing.
func buildCmsg(level, typ int, data []byte) []byte {
	buf := make([]byte, unix.CmsgSpace(len(data)))
	h := (*unix.Cmsghdr)(unsafe.Pointer(&buf[0]))
	h.Level = int32(level)
	h.Type = int32(typ)
	h.SetLen(unix.CmsgLen(len(data)))
	copy(buf[unix.CmsgLen(0):], data)
	return buf
}
