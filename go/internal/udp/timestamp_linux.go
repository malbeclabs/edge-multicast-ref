//go:build linux

package udp

import (
	"encoding/binary"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"golang.org/x/sys/unix"
)

// Receive-timestamp kinds reported by ReadDatagram.
const (
	RecvTimestampKindKernelSoftware = "kernel_udp_software"
	RecvTimestampKindAppFallback    = "app_udp_fallback"
)

// scmTimestampnsLen is the payload SCM_TIMESTAMPNS carries: a struct timespec
// of two 64-bit words. A host whose timespec is smaller cannot satisfy
// extractKernelTimestamp, which falls back rather than guess.
const scmTimestampnsLen = 16

// controlBufferSize is the room a Reader gives the kernel for control messages.
// SO_TIMESTAMPNS is the only one this package asks for, so a Reader holds one
// SCM_TIMESTAMPNS and nothing else.
//
// The size matters because a control message that does not fit is not a read
// error: the kernel writes what fits, discards the rest, raises MSG_CTRUNC, and
// ReadMsgUDP still returns nil. A socket given a further control message
// elsewhere — IP_PKTINFO, SO_RXQ_OVFL — can therefore lose the timestamp and
// degrade every latency the process reports to the userspace fallback.
// ReadDatagram repeats the kernel's MSG_CTRUNC rather than let that happen
// quietly, so this buffer and what a socket asks for cannot drift apart
// unnoticed.
var controlBufferSize = unix.CmsgSpace(scmTimestampnsLen)

// EnableTimestamping asks the kernel to attach an SO_TIMESTAMPNS control
// message to every datagram read from conn.
func EnableTimestamping(conn *net.UDPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var setsockoptErr error
	err = rawConn.Control(func(fd uintptr) {
		setsockoptErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1)
	})
	if err != nil {
		return err
	}
	return setsockoptErr
}

// A Reader reads datagrams off a socket, holding the control-message buffer
// across reads so the receive path does not allocate one per datagram.
//
// That buffer is overwritten by every read and the Reader's other field is
// written without a lock, so a Reader must not be used from two goroutines at
// once: a receive goroutine makes its own, next to its own datagram buffer.
type Reader struct {
	oob []byte
	// controlTruncationLogged holds the MSG_CTRUNC warning to one line per
	// Reader. The cause is how the socket is configured, so it holds for every
	// datagram that follows and would otherwise be logged at line rate.
	controlTruncationLogged bool
}

// NewReader returns a Reader for one receive goroutine.
func NewReader() *Reader {
	return &Reader{oob: make([]byte, controlBufferSize)}
}

// ReadDatagram reads one datagram and returns the sender address plus the
// kernel receive timestamp when available, otherwise an application-time
// fallback.
//
// A datagram whose control messages the kernel had to truncate is still
// returned — the payload is intact — but the first one warns, because the
// timestamp it should have carried may be the message that was discarded.
func (r *Reader) ReadDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, oobn, flags, addr, err := conn.ReadMsgUDP(buf, r.oob)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	if flags&unix.MSG_CTRUNC != 0 && !r.controlTruncationLogged {
		r.controlTruncationLogged = true
		slog.Warn("the kernel discarded a control message: this socket asks for more of them "+
			"than the control buffer holds, so the receive timestamp can fall back to application time",
			"control_buffer_bytes", len(r.oob))
	}
	src := srcAddr(addr)
	if recvTime, ok := extractKernelTimestamp(r.oob[:oobn]); ok {
		return n, src, recvTime.UTC(), RecvTimestampKindKernelSoftware, nil
	}
	return n, src, time.Now().UTC(), RecvTimestampKindAppFallback, nil
}

func extractKernelTimestamp(oob []byte) (time.Time, bool) {
	if len(oob) == 0 {
		return time.Time{}, false
	}
	cmsgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return time.Time{}, false
	}
	for _, cmsg := range cmsgs {
		if cmsg.Header.Level != unix.SOL_SOCKET || cmsg.Header.Type != unix.SCM_TIMESTAMPNS {
			continue
		}
		if len(cmsg.Data) < scmTimestampnsLen {
			continue
		}
		// The kernel writes the timespec in host byte order.
		sec := int64(binary.NativeEndian.Uint64(cmsg.Data[0:8]))
		nsec := int64(binary.NativeEndian.Uint64(cmsg.Data[8:16]))
		return time.Unix(sec, nsec).UTC(), true
	}
	return time.Time{}, false
}
