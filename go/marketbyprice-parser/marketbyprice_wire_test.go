package main

import (
	"encoding/binary"
	"errors"
	"strings"
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
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 7, 42, ts, 3, 1, frameHeaderSize)
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
	buf := buildFrameHeader(0x4444, mbpSchemaVersionV1, 0, 0, time.Now(), 0, 0, frameHeaderSize)
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
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 0, time.Now(), 0, 0, 999)
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
	buf[36] = 1 // Asset Class: crypto spot
	// Exponents are negative. Assign through typed variables: `byte(int8(-2))`
	// is a compile-time overflow error, because the operand is a constant.
	priceExp, qtyExp := int8(-2), int8(-8)
	buf[37] = byte(priceExp)
	buf[38] = byte(qtyExp)
	buf[39] = 1 // Market Model: CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1)))
	binary.LittleEndian.PutUint64(buf[48:56], 100)
	binary.LittleEndian.PutUint64(buf[56:64], 0)
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1 // Settle Type: cash
	buf[73] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(buf[74:76], 9)

	body, err := ParseInstrumentDefinition(buf, mbpSchemaVersionV1)
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
	buf[6] = 1    // Aggressor Side: buy
	buf[7] = 0x02 // Trade Flags: sweep
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts.UnixNano()))
	tradePrice := int64(-1500) // typed variable; see the note on exponents above
	binary.LittleEndian.PutUint64(buf[16:24], uint64(tradePrice))
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
		{"instrument_definition", 76, func(b []byte) error { _, err := ParseInstrumentDefinition(b, mbpSchemaVersionV1); return err }},
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

func TestParseLiquidation(t *testing.T) {
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint32(buf[0:4], 12)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 0x03 // Flags: short liquidated (bit 0) + ADL (bit 1)
	buf[7] = 1    // Method: backstop
	binary.LittleEndian.PutUint64(buf[8:16], 5150)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(int64(432100)))
	for i := 0; i < 20; i++ {
		buf[24+i] = byte(0xA0 + i)
	}
	body, err := ParseLiquidation(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 12 || body.SourceID != 1 || body.Flags != 0x03 || body.Method != 1 {
		t.Errorf("head: %+v", body)
	}
	if body.TradeID != 5150 || body.MarkPriceRaw != 432100 {
		t.Errorf("pairing: %+v", body)
	}
	if body.LiquidatedUser[0] != 0xA0 || body.LiquidatedUser[19] != 0xB3 {
		t.Errorf("user: %x", body.LiquidatedUser)
	}
}

func TestParseSnapshotBegin(t *testing.T) {
	ts := time.Unix(1700000007, 0)
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint32(buf[0:4], 77)
	binary.LittleEndian.PutUint64(buf[4:12], 12345)
	binary.LittleEndian.PutUint32(buf[12:16], 400)  // Total Levels
	binary.LittleEndian.PutUint32(buf[16:20], 9)    // Snapshot ID
	binary.LittleEndian.PutUint32(buf[20:24], 8888) // Last Instrument Seq
	binary.LittleEndian.PutUint64(buf[24:32], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint32(buf[32:36], 0) // Depth Bound: complete book

	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 77 || body.AnchorSeq != 12345 || body.TotalLevels != 400 {
		t.Errorf("head: %+v", body)
	}
	if body.SnapshotID != 9 || body.LastInstrumentSeq != 8888 || !body.Timestamp.Equal(ts) {
		t.Errorf("ids: %+v", body)
	}
	if body.DepthBound != 0 {
		t.Errorf("depth bound: got %d want 0", body.DepthBound)
	}
}

// The 36-byte body is the market-by-order 32-byte layout plus Depth Bound.
// A 32-byte body is a market-by-order message and must be rejected here: the
// prefix-superset rule lets an MBO decoder read an MBP frame, not the reverse.
func TestParseSnapshotBegin_RejectsShortSiblingLayout(t *testing.T) {
	if _, err := ParseSnapshotBegin(make([]byte, 32)); !errors.Is(err, errTruncated) {
		t.Fatalf("expected errTruncated for 32-byte body, got %v", err)
	}
}

func TestParseSnapshotBegin_BoundedDepth(t *testing.T) {
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint32(buf[32:36], 50)
	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.DepthBound != 50 {
		t.Errorf("depth bound: got %d want 50", body.DepthBound)
	}
}

func TestParseLevelUpdate(t *testing.T) {
	ts := time.Unix(1700000008, 42)
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint32(buf[0:4], 101)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // Side: ask
	buf[7] = 2 // Action: change
	binary.LittleEndian.PutUint32(buf[8:12], 777)
	// Negative prices are legal. Encode through a typed variable: a constant
	// conversion such as uint64(int64(-2500)) is a compile-time overflow error.
	priceRaw := int64(-2500)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(priceRaw))
	binary.LittleEndian.PutUint64(buf[20:28], 12345)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint16(buf[36:38], 4) // Order Count
	binary.LittleEndian.PutUint16(buf[38:40], 2) // Level Index
	buf[40] = 1                                  // Update Reason: trade
	buf[41] = 0x02                               // Level Flags: AMM-synthetic

	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 101 || body.SourceID != 1 || body.Side != 1 || body.Action != 2 {
		t.Errorf("head: %+v", body)
	}
	if body.PerInstrumentSeq != 777 || body.PriceRaw != -2500 || body.QtyRaw != 12345 {
		t.Errorf("level: %+v", body)
	}
	if !body.Timestamp.Equal(ts) || body.OrderCount != 4 || body.LevelIndex != 2 {
		t.Errorf("meta: %+v", body)
	}
	if body.UpdateReason != 1 || body.LevelFlags != 0x02 {
		t.Errorf("tail: %+v", body)
	}
}

// Quantity 0 is the delete signal and must decode cleanly, not be treated as absent.
func TestParseLevelUpdate_ZeroQtyIsValid(t *testing.T) {
	buf := make([]byte, 44)
	buf[7] = 3 // Action: delete
	binary.LittleEndian.PutUint64(buf[12:20], uint64(int64(500)))
	binary.LittleEndian.PutUint64(buf[20:28], 0)
	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.QtyRaw != 0 || body.Action != 3 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseLevelUpdate_Sentinels(t *testing.T) {
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint16(buf[36:38], u16Unavailable)
	binary.LittleEndian.PutUint16(buf[38:40], u16Unavailable)
	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	// The wire value is preserved here; omission from JSON happens in decodeMessage.
	if body.OrderCount != u16Unavailable || body.LevelIndex != u16Unavailable {
		t.Errorf("sentinels not preserved: %+v", body)
	}
}

func TestParseBookClear(t *testing.T) {
	ts := time.Unix(1700000009, 0)
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[0:4], 202)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 2 // Clear Side: both
	buf[7] = 0 // Scope: entire side
	binary.LittleEndian.PutUint32(buf[8:12], 900)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(int64(0)))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(ts.UnixNano()))
	buf[28] = 1 // Clear Reason: halt

	body, err := ParseBookClear(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 202 || body.SourceID != 1 || body.ClearSide != 2 || body.Scope != 0 {
		t.Errorf("head: %+v", body)
	}
	if body.PerInstrumentSeq != 900 || !body.Timestamp.Equal(ts) || body.ClearReason != 1 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseBookClear_ScopedFromPrice(t *testing.T) {
	buf := make([]byte, 32)
	buf[6] = 0 // Clear Side: bid
	buf[7] = 1 // Scope: from price outward
	// Typed variable, not a constant conversion — see TestParseLevelUpdate.
	fromPrice := int64(-777)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(fromPrice))
	body, err := ParseBookClear(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.Scope != 1 || body.FromPriceRaw != -777 {
		t.Errorf("body: %+v", body)
	}
}

// Scope=1 with Clear Side=2 is malformed: one price cannot bound both sides.
// The spec requires the subscriber to discard and count it.
func TestParseBookClear_ScopeBothSidesMalformed(t *testing.T) {
	buf := make([]byte, 32)
	buf[6] = 2 // Clear Side: both
	buf[7] = 1 // Scope: from price outward
	if _, err := ParseBookClear(buf); !errors.Is(err, errMalformedBody) {
		t.Fatalf("expected errMalformedBody, got %v", err)
	}
}

func TestParseSnapshotLevel(t *testing.T) {
	buf := make([]byte, 28)
	binary.LittleEndian.PutUint32(buf[0:4], 9)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(int64(998877)))
	binary.LittleEndian.PutUint64(buf[12:20], 654321)
	binary.LittleEndian.PutUint16(buf[20:22], 12)
	buf[22] = 0    // Side: bid
	buf[23] = 0x01 // Level Flags: implied

	body, err := ParseSnapshotLevel(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.SnapshotID != 9 || body.PriceRaw != 998877 || body.QtyRaw != 654321 {
		t.Errorf("level: %+v", body)
	}
	if body.OrderCount != 12 || body.Side != 0 || body.LevelFlags != 0x01 {
		t.Errorf("meta: %+v", body)
	}
}

func TestNewBodies_ExactLengthOnly(t *testing.T) {
	cases := []struct {
		name string
		size int
		fn   func([]byte) error
	}{
		{"liquidation", 44, func(b []byte) error { _, err := ParseLiquidation(b); return err }},
		{"snapshot_begin", 36, func(b []byte) error { _, err := ParseSnapshotBegin(b); return err }},
		{"level_update", 44, func(b []byte) error { _, err := ParseLevelUpdate(b); return err }},
		{"book_clear", 32, func(b []byte) error { _, err := ParseBookClear(b); return err }},
		{"snapshot_level", 28, func(b []byte) error { _, err := ParseSnapshotLevel(b); return err }},
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

// buildInstDefV1 builds a 76-byte v1 InstrumentDefinition body.
func buildInstDefV1(symbol string) []byte {
	b := make([]byte, 76)
	binary.LittleEndian.PutUint32(b[0:4], 4242)
	copy(b[4:20], symbol)
	copy(b[20:28], "BTC")
	copy(b[28:36], "USDT")
	b[36] = 1 // asset class
	// Typed variables, not constant conversions: byte(int8(-2)) is a
	// compile-time overflow error because the operand is a constant.
	priceExp, qtyExp := int8(-2), int8(-8)
	b[37] = byte(priceExp)                              // price exponent
	b[38] = byte(qtyExp)                                // qty exponent
	b[39] = 1                                           // market model
	binary.LittleEndian.PutUint64(b[40:48], 50)         // tick size
	binary.LittleEndian.PutUint64(b[48:56], 100)        // lot size
	binary.LittleEndian.PutUint64(b[56:64], 1000)       // contract value
	binary.LittleEndian.PutUint64(b[64:72], 1700000000) // expiry
	b[72] = 1                                           // settle type
	b[73] = 2                                           // price bound
	binary.LittleEndian.PutUint16(b[74:76], 7)          // manifest seq
	return b
}

// buildInstDefV3 builds a 126-byte v3 body. Source ID is inserted at 4:6 and
// Symbol widens to 64 bytes, so every field after Instrument ID sits 50 bytes
// later than in v1.
func buildInstDefV3(symbol string) []byte {
	b := make([]byte, 126)
	binary.LittleEndian.PutUint32(b[0:4], 4242)
	binary.LittleEndian.PutUint16(b[4:6], 77) // source id
	copy(b[6:70], symbol)
	copy(b[70:78], "BTC")
	copy(b[78:86], "USDT")
	b[86] = 1
	priceExp, qtyExp := int8(-2), int8(-8)
	b[87] = byte(priceExp)
	b[88] = byte(qtyExp)
	b[89] = 1
	binary.LittleEndian.PutUint64(b[90:98], 50)
	binary.LittleEndian.PutUint64(b[98:106], 100)
	binary.LittleEndian.PutUint64(b[106:114], 1000)
	binary.LittleEndian.PutUint64(b[114:122], 1700000000)
	b[122] = 1
	b[123] = 2
	binary.LittleEndian.PutUint16(b[124:126], 7)
	return b
}

func assertInstDefFields(t *testing.T, got InstrumentDefinitionBody, wantSymbol string) {
	t.Helper()
	if got.InstrumentID != 4242 {
		t.Errorf("instrument id: got %d want 4242", got.InstrumentID)
	}
	if got.Symbol != wantSymbol {
		t.Errorf("symbol: got %q want %q", got.Symbol, wantSymbol)
	}
	if got.Leg1 != "BTC" || got.Leg2 != "USDT" {
		t.Errorf("legs: got %q %q want BTC USDT", got.Leg1, got.Leg2)
	}
	if got.AssetClass != 1 || got.MarketModel != 1 {
		t.Errorf("asset class / market model: got %d %d want 1 1", got.AssetClass, got.MarketModel)
	}
	if got.PriceExponent != -2 || got.QtyExponent != -8 {
		t.Errorf("exponents: got %d %d want -2 -8", got.PriceExponent, got.QtyExponent)
	}
	if got.TickSizeRaw != 50 || got.LotSizeRaw != 100 {
		t.Errorf("tick/lot: got %d %d want 50 100", got.TickSizeRaw, got.LotSizeRaw)
	}
	if got.ContractValue != 1000 {
		t.Errorf("contract value: got %d want 1000", got.ContractValue)
	}
	if got.SettleType != 1 || got.PriceBound != 2 {
		t.Errorf("settle/bound: got %d %d want 1 2", got.SettleType, got.PriceBound)
	}
	if got.ManifestSeq != 7 {
		t.Errorf("manifest seq: got %d want 7", got.ManifestSeq)
	}
}

func TestParseInstrumentDefinition_V1(t *testing.T) {
	got, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 1)
	if err != nil {
		t.Fatal(err)
	}
	assertInstDefFields(t, got, "BTC-USDT")
}

// The v3 symbol MUST exceed 16 bytes, or this test proves nothing a v1 test
// does not. This is the whole point of the widening: a Kalshi ticker like
// KXNFLGAME-26SEP13NYJTEN-NYJ is 27 bytes and was previously truncated.
func TestParseInstrumentDefinition_V3LongSymbol(t *testing.T) {
	const long = "KXNFLGAME-26SEP13NYJTEN-NYJ"
	if len(long) <= 16 {
		t.Fatal("fixture symbol must exceed 16 bytes to be meaningful")
	}
	got, err := ParseInstrumentDefinition(buildInstDefV3(long), 3)
	if err != nil {
		t.Fatal(err)
	}
	assertInstDefFields(t, got, long)
	if got.SourceID != 77 {
		t.Errorf("source id: got %d want 77", got.SourceID)
	}
}

// v1 has no Source ID on the wire. It must decode as 0 (registry Unknown)
// rather than picking up the first two bytes of Symbol.
func TestParseInstrumentDefinition_V1SourceIDIsZero(t *testing.T) {
	got, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceID != 0 {
		t.Errorf("v1 source id: got %d want 0", got.SourceID)
	}
}

// A symbol filling all 64 bytes has no null terminator; it must not be truncated
// or run past the field.
func TestParseInstrumentDefinition_V3SymbolFillsField(t *testing.T) {
	full := strings.Repeat("A", 64)
	got, err := ParseInstrumentDefinition(buildInstDefV3(full), 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Symbol != full {
		t.Errorf("symbol length: got %d want 64", len(got.Symbol))
	}
	if got.Leg1 != "BTC" {
		t.Errorf("a full-width symbol must not bleed into Leg1: got %q", got.Leg1)
	}
}

// The declared version and the body length must agree. A v3 frame carrying a v1
// body would otherwise read Source ID and Symbol across 66 bytes of adjacent
// fields and produce plausible garbage instead of an error.
func TestParseInstrumentDefinition_LengthMustMatchVersion(t *testing.T) {
	if _, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 3); err == nil {
		t.Error("version 3 with a 76-byte body must be rejected")
	}
	if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), 1); err == nil {
		t.Error("version 1 with a 126-byte body must be rejected")
	}
}

// Version 2 was specified and superseded before any publisher emitted it. It is
// not a layout this decoder implements, and it must be rejected as firmly as a
// version that never existed at all — a version ceiling would let it through.
func TestParseInstrumentDefinition_UnsupportedVersion(t *testing.T) {
	for _, v := range []uint8{0, 2, 4, 255} {
		if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), v); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// The frame header accepts both implemented versions and nothing else.
func TestParseFrameHeader_AcceptsV1AndV3(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	for _, v := range []uint8{1, 3} {
		buf := buildFrameHeader(mbpMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err != nil {
			t.Errorf("schema version %d must be accepted: %v", v, err)
		}
	}
	for _, v := range []uint8{0, 2, 4, 255} {
		buf := buildFrameHeader(mbpMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// A publisher cutting over from v1 to v3 mid-stream must be followed without a
// restart. This is why the version is read per frame rather than latched from
// the first frame.
func TestParseFrame_FollowsVersionSwitchMidStream(t *testing.T) {
	p := &marketByPriceParser{}
	ts := time.Unix(1700000000, 0)

	build := func(version uint8, body []byte) []byte {
		msg := buildMsg(msgTypeInstrumentDefinition, 0, body)
		total := frameHeaderSize + len(msg)
		frame := buildFrameHeader(mbpMagic, version, 0, 1, ts, 1, 0, uint16(total))
		return append(frame, msg...)
	}

	v1Frame := build(1, buildInstDefV1("SHORT"))
	v3Frame := build(3, buildInstDefV3("KXNFLGAME-26SEP13NYJTEN-NYJ"))

	for i, tc := range []struct {
		frame []byte
		want  string
	}{
		{v1Frame, "SHORT"},
		{v3Frame, "KXNFLGAME-26SEP13NYJTEN-NYJ"},
		{v1Frame, "SHORT"}, // and back again
	} {
		recs, _, err := p.ParseFrame("refdata", tc.frame)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if len(recs) != 1 {
			t.Fatalf("frame %d: expected 1 record, got %d", i, len(recs))
		}
		if got := recs[0].Fields["symbol"]; got != tc.want {
			t.Errorf("frame %d symbol: got %v want %q", i, got, tc.want)
		}
	}
}
