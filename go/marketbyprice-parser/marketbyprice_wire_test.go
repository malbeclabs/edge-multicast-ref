package main

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// buildFrameHeader constructs a 24-byte frame header for tests.
func buildFrameHeader(magic uint16, schema, channel uint8, seq uint64, ts time.Time, msgCount, resetCount uint8, frameLen uint16) []byte {
	buf := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint16(buf[0:2], magic)
	buf[2] = schema
	buf[3] = channel
	binary.LittleEndian.PutUint64(buf[4:12], seq)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts.UnixNano()))
	buf[20] = msgCount
	buf[21] = resetCount
	binary.LittleEndian.PutUint16(buf[22:24], frameLen)
	return buf
}

func TestMagicIsMarketByPrice(t *testing.T) {
	// 0x4442 is this feed's magic. It must differ from the sibling feeds so a
	// misrouted frame is rejected rather than cross-decoded.
	if mbpMagic != 0x4442 {
		t.Fatalf("magic: got %#x want 0x4442", mbpMagic)
	}
	for name, other := range map[string]uint16{"topofbook": 0x445A, "marketbyorder": 0x4444, "midpoint": 0x4D44} {
		if mbpMagic == other {
			t.Fatalf("magic collides with %s feed", name)
		}
	}
}

func TestParseFrameHeader_Valid(t *testing.T) {
	ts := time.Unix(1700000000, 123456789)
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersion, 7, 42, ts, 3, 1, frameHeaderSize)
	h, err := ParseFrameHeader(buf)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Magic != mbpMagic {
		t.Errorf("magic: got %x want %x", h.Magic, mbpMagic)
	}
	if h.ChannelID != 7 {
		t.Errorf("channel: got %d", h.ChannelID)
	}
	if h.Sequence != 42 {
		t.Errorf("seq: got %d", h.Sequence)
	}
	if !h.SendTimestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", h.SendTimestamp, ts)
	}
	if h.MessageCount != 3 || h.ResetCount != 1 || h.FrameLength != frameHeaderSize {
		t.Errorf("fields: %+v", h)
	}
}

func TestParseFrameHeader_BadMagic(t *testing.T) {
	// A market-by-order frame must not decode here.
	buf := buildFrameHeader(0x4444, mbpSchemaVersion, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errBadMagic) {
		t.Fatalf("expected errBadMagic, got %v", err)
	}
}

func TestParseFrameHeader_WrongVersion(t *testing.T) {
	buf := buildFrameHeader(mbpMagic, 99, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errSchemaVersion) {
		t.Fatalf("expected errSchemaVersion, got %v", err)
	}
}

func TestParseFrameHeader_LengthMismatch(t *testing.T) {
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 0, time.Now(), 0, 0, 999)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errFrameLength) {
		t.Fatalf("expected errFrameLength, got %v", err)
	}
}

func TestParseFrameHeader_TooShort(t *testing.T) {
	if _, err := ParseFrameHeader(make([]byte, 10)); !errors.Is(err, errFrameTooShort) {
		t.Fatalf("expected errFrameTooShort, got %v", err)
	}
}

func TestParseMessageHeader(t *testing.T) {
	buf := []byte{0x40, 48, 0x01, 0x00}
	mh, err := ParseMessageHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if mh.Type != 0x40 || mh.Length != 48 {
		t.Errorf("header: %+v", mh)
	}
	if mh.Flags&flagSnapshot == 0 {
		t.Error("snapshot flag should be set")
	}
}

func TestParseMessageHeader_TooShort(t *testing.T) {
	if _, err := ParseMessageHeader([]byte{0x40, 48}); !errors.Is(err, errMessageTooShort) {
		t.Fatalf("expected errMessageTooShort, got %v", err)
	}
}

func TestFixedString(t *testing.T) {
	if got := fixedString([]byte{'B', 'T', 'C', 0, 0}); got != "BTC" {
		t.Errorf("got %q", got)
	}
	// No null terminator: the whole field is the value.
	if got := fixedString([]byte{'A', 'B'}); got != "AB" {
		t.Errorf("got %q", got)
	}
}
