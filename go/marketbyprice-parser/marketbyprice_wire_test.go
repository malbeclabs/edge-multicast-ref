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

func TestParseHeartbeat(t *testing.T) {
	ts := time.Unix(1700000001, 0)
	buf := make([]byte, 12)
	buf[0] = 5 // Channel ID
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))
	body, err := ParseHeartbeat(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.ChannelID != 5 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseHeartbeat_WrongLength(t *testing.T) {
	if _, err := ParseHeartbeat(make([]byte, 11)); !errors.Is(err, errTruncated) {
		t.Fatalf("expected errTruncated, got %v", err)
	}
}

func TestParseEndOfSession(t *testing.T) {
	ts := time.Unix(1700000002, 0)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ts.UnixNano()))
	body, err := ParseEndOfSession(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !body.Timestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", body.Timestamp, ts)
	}
}

func TestParseManifestSummary(t *testing.T) {
	ts := time.Unix(1700000003, 0)
	buf := make([]byte, 20)
	buf[0] = 7 // Channel ID
	buf[1] = 1 // Valid
	binary.LittleEndian.PutUint16(buf[4:6], 100)
	binary.LittleEndian.PutUint32(buf[8:12], 25)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts.UnixNano()))
	body, err := ParseManifestSummary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.ChannelID != 7 || body.Valid != 1 || body.ManifestSeq != 100 || body.InstrumentCount != 25 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentDefinition(t *testing.T) {
	expiry := time.Unix(1800000000, 0)
	buf := make([]byte, 76)
	binary.LittleEndian.PutUint32(buf[0:4], 4242)
	copy(buf[4:20], "BTC-USDT")
	copy(buf[20:28], "BTC")
	copy(buf[28:36], "USDT")
	buf[36] = 1                                              // Asset Class: crypto spot
	buf[37] = 254                                             // Price Exponent: -2 as int8
	buf[38] = 248                                             // Qty Exponent: -8 as int8
	buf[39] = 1                                              // Market Model: CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1)))
	binary.LittleEndian.PutUint64(buf[48:56], 100)
	binary.LittleEndian.PutUint64(buf[56:64], 0)
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1 // Settle Type: cash
	buf[73] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(buf[74:76], 9)

	body, err := ParseInstrumentDefinition(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 4242 || body.Symbol != "BTC-USDT" || body.Leg1 != "BTC" || body.Leg2 != "USDT" {
		t.Errorf("identity: %+v", body)
	}
	if body.AssetClass != 1 || body.PriceExponent != -2 || body.QtyExponent != -8 || body.MarketModel != 1 {
		t.Errorf("scaling: %+v", body)
	}
	if body.TickSizeRaw != 1 || body.LotSizeRaw != 100 || body.ContractValue != 0 {
		t.Errorf("sizes: %+v", body)
	}
	if !body.Expiry.Equal(expiry) || body.SettleType != 1 || body.PriceBound != 2 || body.ManifestSeq != 9 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseTrade(t *testing.T) {
	ts := time.Unix(1700000004, 500)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 7)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // Aggressor Side: buy
	buf[7] = 0x02 // Trade Flags: sweep
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint64(buf[16:24], ^uint64(1499)) // -1500 as int64
	binary.LittleEndian.PutUint64(buf[24:32], 250)
	binary.LittleEndian.PutUint64(buf[32:40], 99887766)
	binary.LittleEndian.PutUint64(buf[40:48], 1000000)

	body, err := ParseTrade(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 7 || body.SourceID != 1 || body.AggressorSide != 1 || body.TradeFlags != 0x02 {
		t.Errorf("head: %+v", body)
	}
	if !body.SourceTimestamp.Equal(ts) || body.TradePriceRaw != -1500 || body.TradeQtyRaw != 250 {
		t.Errorf("exec: %+v", body)
	}
	if body.TradeID != 99887766 || body.CumulativeVolumeRaw != 1000000 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseBatchBoundary(t *testing.T) {
	ts := time.Unix(1700000005, 0)
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], 123456)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))
	body, err := ParseBatchBoundary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.BatchID != 123456 || !body.BatchTime.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentReset(t *testing.T) {
	ts := time.Unix(1700000006, 0)
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], 55)
	buf[4] = 3 // Reason: upstream gap
	binary.LittleEndian.PutUint64(buf[8:16], 9000)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(ts.UnixNano()))
	body, err := ParseInstrumentReset(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 55 || body.Reason != 3 || body.NewAnchorSeq != 9000 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotEnd(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], 77)
	binary.LittleEndian.PutUint64(buf[4:12], 12345)
	binary.LittleEndian.PutUint32(buf[12:16], 9)
	body, err := ParseSnapshotEnd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 77 || body.AnchorSeq != 12345 || body.SnapshotID != 9 {
		t.Errorf("body: %+v", body)
	}
}

// Every inherited body rejects a length that is off by one in either direction.
func TestInheritedBodies_ExactLengthOnly(t *testing.T) {
	cases := []struct {
		name string
		size int
		fn   func([]byte) error
	}{
		{"heartbeat", 12, func(b []byte) error { _, err := ParseHeartbeat(b); return err }},
		{"instrument_definition", 76, func(b []byte) error { _, err := ParseInstrumentDefinition(b); return err }},
		{"trade", 48, func(b []byte) error { _, err := ParseTrade(b); return err }},
		{"end_of_session", 8, func(b []byte) error { _, err := ParseEndOfSession(b); return err }},
		{"manifest_summary", 20, func(b []byte) error { _, err := ParseManifestSummary(b); return err }},
		{"batch_boundary", 12, func(b []byte) error { _, err := ParseBatchBoundary(b); return err }},
		{"instrument_reset", 24, func(b []byte) error { _, err := ParseInstrumentReset(b); return err }},
		{"snapshot_end", 16, func(b []byte) error { _, err := ParseSnapshotEnd(b); return err }},
	}
	for _, c := range cases {
		if err := c.fn(make([]byte, c.size)); err != nil {
			t.Errorf("%s: exact size %d rejected: %v", c.name, c.size, err)
		}
		if err := c.fn(make([]byte, c.size-1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: size %d accepted or wrong error: %v", c.name, c.size-1, err)
		}
		if err := c.fn(make([]byte, c.size+1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: trailing byte accepted (must be exact-length): %v", c.name, err)
		}
	}
}
