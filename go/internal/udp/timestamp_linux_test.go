//go:build linux

package udp

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"net"
	"strings"
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

// loopbackPair returns a listening socket configured the way a parser
// configures its own, and a sender connected to it.
func loopbackPair(t *testing.T) (*net.UDPConn, *net.UDPConn) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listening: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

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

// captureWarnings sends the default logger's output to a buffer for the length
// of one test, so what ReadDatagram reports about a truncated control buffer
// can be asserted on.
func captureWarnings(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &out
}

// TestReadDatagram_ReportsKernelTimestamp pins the timestamp quality the
// parsers label their metrics with: the kernel's own value, never the
// userspace fallback.
//
// It is also what holds controlBufferSize. The socket asks for
// SCM_TIMESTAMPNS, so a buffer too small for it makes the kernel discard the
// message and raise MSG_CTRUNC, which is not a read error — the kind silently
// becomes the fallback, and this test is what says so.
func TestReadDatagram_ReportsKernelTimestamp(t *testing.T) {
	warnings := captureWarnings(t)
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
	if got := warnings.String(); got != "" {
		t.Errorf("ReadDatagram warned about the %d-byte control buffer it was given: %s",
			len(reader.oob), got)
	}
}

// TestReadDatagram_WarnsWhenTheKernelTruncatesControlMessages holds the other
// half: that a control buffer too small for what the socket asks for is
// reported rather than swallowed.
//
// The kernel signals it with MSG_CTRUNC and a nil error, so nothing downstream
// can tell a lost timestamp from a socket that never had timestamping on. The
// Reader is given a buffer one control message header wide, which cannot hold
// a struct timespec, to put a real MSG_CTRUNC on the read rather than a
// simulated one. The warning is once per Reader: the cause does not change
// between datagrams, and the receive path runs at line rate.
func TestReadDatagram_WarnsWhenTheKernelTruncatesControlMessages(t *testing.T) {
	warnings := captureWarnings(t)
	conn, sender := loopbackPair(t)

	reader := &Reader{oob: make([]byte, unix.CmsgSpace(0))}
	buf := make([]byte, 2048)
	for i := range 3 {
		if _, err := sender.Write([]byte("kernel please stamp this")); err != nil {
			t.Fatalf("sending datagram %d: %v", i, err)
		}
		_, _, _, kind, err := reader.ReadDatagram(conn, buf)
		if err != nil {
			t.Fatalf("ReadDatagram %d: %v", i, err)
		}
		if kind != RecvTimestampKindAppFallback {
			t.Errorf("datagram %d: receive-timestamp kind %q, want %q: the timestamp cannot have "+
				"survived a control buffer this small", i, kind, RecvTimestampKindAppFallback)
		}
	}

	if got := strings.Count(warnings.String(), "level=WARN"); got != 1 {
		t.Errorf("ReadDatagram logged %d warnings across 3 truncated reads, want 1:\n%s",
			got, warnings.String())
	}
	if !strings.Contains(warnings.String(), "control_buffer_bytes=") {
		t.Errorf("the warning does not name the buffer that was too small:\n%s", warnings.String())
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
