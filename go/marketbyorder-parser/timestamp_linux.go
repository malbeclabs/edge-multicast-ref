//go:build linux

package main

import (
	"encoding/binary"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

const (
	recvTimestampKindKernelSoftware = "kernel_udp_software"
	recvTimestampKindAppFallback    = "app_udp_fallback"
)

func enableTimestamping(conn *net.UDPConn) error {
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

// readDatagram reads one datagram and returns the kernel receive timestamp when
// available, otherwise an application-time fallback.
func readDatagram(conn *net.UDPConn, buf []byte) (int, time.Time, string, error) {
	oob := make([]byte, unix.CmsgSpace(16))
	n, oobn, _, _, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, time.Time{}, "", err
	}
	if recvTime, ok := extractKernelTimestamp(oob[:oobn]); ok {
		return n, recvTime.UTC(), recvTimestampKindKernelSoftware, nil
	}
	return n, time.Now().UTC(), recvTimestampKindAppFallback, nil
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
		if len(cmsg.Data) < 16 {
			continue
		}
		sec := int64(binary.LittleEndian.Uint64(cmsg.Data[0:8]))
		nsec := int64(binary.LittleEndian.Uint64(cmsg.Data[8:16]))
		return time.Unix(sec, nsec).UTC(), true
	}
	return time.Time{}, false
}
