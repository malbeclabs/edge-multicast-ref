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
	binary.LittleEndian.PutUint64(data[0:8], uint64(want.Unix()))
	binary.LittleEndian.PutUint64(data[8:16], uint64(want.Nanosecond()))

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
	binary.LittleEndian.PutUint64(data[0:8], 1717689600)

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

// TestReadDatagram_ReportsKernelTimestamp asserts the Linux path returns the
// kernel's own receive timestamp: EnableTimestamping has succeeded, so the
// control message must arrive and the fallback must not be reported.
func TestReadDatagram_ReportsKernelTimestamp(t *testing.T) {
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

	if _, err := sender.Write([]byte("kernel please stamp this")); err != nil {
		t.Fatalf("sending: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("setting the read deadline: %v", err)
	}
	buf := make([]byte, 2048)
	_, _, recvTime, kind, err := ReadDatagram(conn, buf)
	if err != nil {
		t.Fatalf("ReadDatagram: %v", err)
	}
	if kind != RecvTimestampKindKernelSoftware {
		t.Errorf("receive-timestamp kind %q, want %q", kind, RecvTimestampKindKernelSoftware)
	}
	if recvTime.IsZero() {
		t.Error("receive timestamp is zero")
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
