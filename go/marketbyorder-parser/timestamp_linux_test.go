//go:build linux

package main

import (
	"encoding/binary"
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
