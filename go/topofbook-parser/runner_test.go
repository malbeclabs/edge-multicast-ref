package main

import (
	"encoding/binary"
	"testing"

	"github.com/malbeclabs/edge-multicast-ref/go/topofbook-parser/tob"
)

// Frame/message layout constants, mirrored from tob/topofbook_wire.go (which
// keeps them unexported) so these tests can build raw datagrams without
// depending on the tob package's own test helpers.
const (
	tobFrameHeaderBytes = 24
	tobMsgHeaderBytes   = 4

	tobMsgHeartbeat            = 0x01
	tobMsgInstrumentDefinition = 0x02
)

// buildTobFrame assembles a frame header plus a single application message.
func buildTobFrame(schemaVersion uint8, msgType uint8, body []byte) []byte {
	msgLen := tobMsgHeaderBytes + len(body)
	frameLen := tobFrameHeaderBytes + msgLen

	buf := make([]byte, frameLen)
	buf[0] = 0x5A
	buf[1] = 0x44
	buf[2] = schemaVersion
	buf[3] = 1                                            // channel id
	binary.LittleEndian.PutUint64(buf[4:12], 100)         // sequence
	binary.LittleEndian.PutUint64(buf[12:20], 1700000000) // send timestamp
	buf[20] = 1                                           // msg count
	buf[21] = 0                                           // reset count
	binary.LittleEndian.PutUint16(buf[22:24], uint16(frameLen))

	off := tobFrameHeaderBytes
	buf[off] = msgType
	buf[off+1] = uint8(msgLen)
	binary.LittleEndian.PutUint16(buf[off+2:off+4], 0) // flags
	copy(buf[off+4:], body)

	return buf
}

// buildInstDefBody76 returns an arbitrary-but-well-formed 76-byte
// InstrumentDefinition body (the v1 layout).
func buildInstDefBody76() []byte {
	b := make([]byte, 76)
	binary.LittleEndian.PutUint32(b[0:4], 42)
	copy(b[4:20], "BTC-USDT")
	return b
}

// buildHeartbeatBody returns a well-formed 12-byte Heartbeat body.
func buildHeartbeatBody() []byte {
	b := make([]byte, 12)
	b[0] = 1 // channel id
	binary.LittleEndian.PutUint64(b[4:12], 1700000000)
	return b
}

// TestClassifyParseErr_PinsReasons pins classifyParseErr's substring-based
// buckets against the actual errors Parse produces, for the faults this fix
// wave touches directly:
//
//   - a length/version mismatch on InstrumentDefinition (I3) must classify as
//     "truncated", the same bucket the identical fault lands in on
//     marketbyorder and marketbyprice;
//   - an unsupported Schema Version, whether caught inside InstrumentDefinition
//     decoding (I1's new default case) or by validateHeader's accepted-version
//     check, must classify as "schema_version". Version 2 is the fixture for
//     "unsupported" here: it was specified upstream and superseded before any
//     publisher emitted it, so it is rejected exactly like version 255 would be;
//   - bad magic still classifies as "bad_magic".
//
// This guards the ordering dependency I1 removed: before I1, a version-2
// frame carrying InstrumentDefinition decoded as v1 silently and was only
// later rejected by validateHeader, so the "schema_version" and "truncated"
// paths could not previously be told apart at the InstrumentDefinition layer.
func TestClassifyParseErr_PinsReasons(t *testing.T) {
	p := tob.NewTopOfBookParser()

	tests := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "bad magic",
			data: func() []byte {
				buf := make([]byte, tobFrameHeaderBytes)
				return buf // magic bytes left zero, i.e. wrong
			}(),
			want: "bad_magic",
		},
		{
			name: "instrument_definition length disagrees with declared version 1",
			data: buildTobFrame(1, tobMsgInstrumentDefinition, make([]byte, 70)), // want 76
			want: "truncated",
		},
		{
			name: "instrument_definition length disagrees with declared version 3",
			data: buildTobFrame(3, tobMsgInstrumentDefinition, buildInstDefBody76()), // want 126
			want: "truncated",
		},
		{
			name: "unsupported schema version at instrument_definition decode",
			data: buildTobFrame(2, tobMsgInstrumentDefinition, buildInstDefBody76()),
			want: "schema_version",
		},
		{
			name: "unsupported schema version caught by validateHeader",
			data: buildTobFrame(2, tobMsgHeartbeat, buildHeartbeatBody()),
			want: "schema_version",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.Parse(tc.data, tob.PacketMeta{})
			if err == nil {
				t.Fatalf("expected a parse error, got nil")
			}
			if got := classifyParseErr(err); got != tc.want {
				t.Errorf("classifyParseErr(%q) = %q, want %q", err.Error(), got, tc.want)
			}
		})
	}
}
