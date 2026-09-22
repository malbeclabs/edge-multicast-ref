//go:build linux

package udp

import (
	"encoding/binary"
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
// Every control message any caller asks for needs its own slot here. The kernel
// fills the buffer in its own order and, once out of room, drops the rest and
// raises MSG_CTRUNC — which is not a read error, so a control message that does
// not fit is lost with no signal whatsoever. The two asked for today are:
//
//   - SO_TIMESTAMPNS, set by EnableTimestamping, which yields SCM_TIMESTAMPNS.
//   - IP_PKTINFO, set by topofbook-parser through ipv4.FlagDst, which yields a
//     struct in_pktinfo.
//
// A caller that enables a further control message must add its space here too.
var controlBufferSize = unix.CmsgSpace(scmTimestampnsLen) + unix.CmsgSpace(unix.SizeofInet4Pktinfo)

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
// That buffer is the Reader's only state and it is overwritten by every read,
// so a Reader must not be used from two goroutines at once: a receive goroutine
// makes its own, next to its own datagram buffer.
type Reader struct {
	oob []byte
}

// NewReader returns a Reader for one receive goroutine.
func NewReader() *Reader {
	return &Reader{oob: make([]byte, controlBufferSize)}
}

// ReadDatagram reads one datagram and returns the sender address plus the
// kernel receive timestamp when available, otherwise an application-time
// fallback.
func (r *Reader) ReadDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, oobn, _, addr, err := conn.ReadMsgUDP(buf, r.oob)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
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
