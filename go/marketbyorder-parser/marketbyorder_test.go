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

func TestParseFrameHeader_Valid(t *testing.T) {
	ts := time.Unix(1700000000, 123456789)
	buf := buildFrameHeader(mboMagic, mboSchemaVersionV1, 7, 42, ts, 3, 1, frameHeaderSize)
	h, err := ParseFrameHeader(buf)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Magic != mboMagic {
		t.Errorf("magic: got %x want %x", h.Magic, mboMagic)
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
	buf := buildFrameHeader(0xDEAD, mboSchemaVersionV1, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errBadMagic) {
		t.Fatalf("expected errBadMagic, got %v", err)
	}
}

func TestParseFrameHeader_WrongVersion(t *testing.T) {
	buf := buildFrameHeader(mboMagic, 99, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errSchemaVersion) {
		t.Fatalf("expected errSchemaVersion, got %v", err)
	}
}

func TestParseFrameHeader_LengthMismatch(t *testing.T) {
	buf := buildFrameHeader(mboMagic, mboSchemaVersionV1, 0, 0, time.Now(), 0, 0, 999)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errFrameLength) {
		t.Fatalf("expected errFrameLength, got %v", err)
	}
}

func TestParseFrameHeader_TooShort(t *testing.T) {
	buf := make([]byte, 10)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errFrameTooShort) {
		t.Fatalf("expected errFrameTooShort, got %v", err)
	}
}

func TestParseHeartbeat(t *testing.T) {
	ts := time.Unix(1700000001, 0)
	buf := make([]byte, 12)
	buf[0] = 5
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
	if _, err := ParseHeartbeat(make([]byte, 11)); err == nil {
		t.Fatal("expected error")
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
	buf[0] = 7                                   // ChannelID
	buf[1] = 1                                   // Valid
	binary.LittleEndian.PutUint16(buf[4:6], 100) // ManifestSeq
	binary.LittleEndian.PutUint32(buf[8:12], 25) // InstrumentCount
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
	binary.LittleEndian.PutUint32(buf[0:4], 12345) // InstrumentID
	copy(buf[4:20], "BTC-USDT")                    // Symbol (null-padded)
	copy(buf[20:28], "BTC")                        // Leg1
	copy(buf[28:36], "USDT")                       // Leg2
	buf[36] = 1                                    // AssetClass = Crypto Spot
	priceExp, qtyExp := int8(-2), int8(-8)
	buf[37] = uint8(priceExp)                                   // PriceExponent
	buf[38] = uint8(qtyExp)                                     // QtyExponent
	buf[39] = 1                                                 // MarketModel = CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1))) // TickSize
	binary.LittleEndian.PutUint64(buf[48:56], 100)              // LotSize
	binary.LittleEndian.PutUint64(buf[56:64], 0)                // ContractValue
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1                                  // SettleType
	buf[73] = 0                                  // PriceBound
	binary.LittleEndian.PutUint16(buf[74:76], 7) // ManifestSeq

	body, err := ParseInstrumentDefinition(buf, mboSchemaVersionV1)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 12345 || body.Symbol != "BTC-USDT" || body.Leg1 != "BTC" || body.Leg2 != "USDT" {
		t.Errorf("strings: %+v", body)
	}
	if body.AssetClass != 1 || body.PriceExponent != -2 || body.QtyExponent != -8 || body.MarketModel != 1 {
		t.Errorf("enums/exponents: %+v", body)
	}
	if body.TickSizeRaw != 1 || body.LotSizeRaw != 100 || body.ContractValue != 0 || !body.Expiry.Equal(expiry) {
		t.Errorf("numerics: %+v", body)
	}
	if body.SettleType != 1 || body.PriceBound != 0 || body.ManifestSeq != 7 {
		t.Errorf("trailing: %+v", body)
	}
}

func TestParseTrade(t *testing.T) {
	ts := time.Unix(1700000004, 0)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 99)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // AggressorSide = Buy
	buf[7] = 0
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(int64(6743250)))
	binary.LittleEndian.PutUint64(buf[24:32], 50)
	binary.LittleEndian.PutUint64(buf[32:40], 1234567890)
	binary.LittleEndian.PutUint64(buf[40:48], 99999)

	body, err := ParseTrade(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 99 || body.SourceID != 1 || body.AggressorSide != 1 {
		t.Errorf("hdr: %+v", body)
	}
	if body.TradePriceRaw != 6743250 || body.TradeQtyRaw != 50 || body.TradeID != 1234567890 || body.CumulativeVolumeRaw != 99999 {
		t.Errorf("body: %+v", body)
	}
	if !body.SourceTimestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", body.SourceTimestamp, ts)
	}
}

func TestParseOrderAdd(t *testing.T) {
	enter := time.Unix(1700000010, 0)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 0 // bid
	buf[7] = 1 // post-only flag
	binary.LittleEndian.PutUint32(buf[8:12], 42)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(buf[36:44], 3031)

	body, err := ParseOrderAdd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.SourceID != 1 || body.Side != 0 || body.OrderFlags != 1 {
		t.Errorf("hdr: %+v", body)
	}
	if body.PerInstrumentSeq != 42 || body.OrderID != 999 || !body.EnterTimestamp.Equal(enter) {
		t.Errorf("ids/ts: %+v", body)
	}
	if body.PriceRaw != 82446 || body.QtyRaw != 3031 {
		t.Errorf("price/qty: %+v", body)
	}
}

func TestParseOrderCancel(t *testing.T) {
	ts := time.Unix(1700000011, 0)
	buf := make([]byte, 28)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // UserCancel
	binary.LittleEndian.PutUint32(buf[8:12], 43)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(ts.UnixNano()))

	body, err := ParseOrderCancel(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.SourceID != 1 || body.Reason != 1 ||
		body.PerInstrumentSeq != 43 || body.OrderID != 999 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseOrderExecute(t *testing.T) {
	ts := time.Unix(1700000012, 0)
	buf := make([]byte, 52)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // Buy aggressor
	buf[7] = 1 // full-fill flag
	binary.LittleEndian.PutUint32(buf[8:12], 44)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], 1234567890)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(int64(82500)))
	binary.LittleEndian.PutUint64(buf[44:52], 100)

	body, err := ParseOrderExecute(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.SourceID != 1 || body.AggressorSide != 1 || body.ExecFlags != 1 ||
		body.PerInstrumentSeq != 44 || body.OrderID != 999 || body.TradeID != 1234567890 ||
		!body.Timestamp.Equal(ts) || body.ExecPriceRaw != 82500 || body.ExecQtyRaw != 100 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseBatchBoundary(t *testing.T) {
	ts := time.Unix(1700000013, 0)
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], 7000)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))

	body, err := ParseBatchBoundary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.BatchID != 7000 || !body.BatchTime.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentReset(t *testing.T) {
	ts := time.Unix(1700000014, 0)
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	buf[4] = 1 // PublisherInconsistency
	binary.LittleEndian.PutUint64(buf[8:16], 5000)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(ts.UnixNano()))

	body, err := ParseInstrumentReset(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.Reason != 1 || body.NewAnchorSeq != 5000 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotBegin(t *testing.T) {
	ts := time.Unix(1700000015, 0)
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint64(buf[4:12], 5000)
	binary.LittleEndian.PutUint32(buf[12:16], 25)  // TotalOrders
	binary.LittleEndian.PutUint32(buf[16:20], 7)   // SnapshotID
	binary.LittleEndian.PutUint32(buf[20:24], 100) // LastInstrumentSeq
	binary.LittleEndian.PutUint64(buf[24:32], uint64(ts.UnixNano()))

	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.AnchorSeq != 5000 || body.TotalOrders != 25 ||
		body.SnapshotID != 7 || body.LastInstrumentSeq != 100 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotOrder(t *testing.T) {
	enter := time.Unix(1700000016, 0)
	buf := make([]byte, 40)
	binary.LittleEndian.PutUint32(buf[0:4], 7)
	binary.LittleEndian.PutUint64(buf[4:12], 999)
	buf[12] = 0
	buf[13] = 1
	binary.LittleEndian.PutUint64(buf[16:24], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(buf[32:40], 3031)

	body, err := ParseSnapshotOrder(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.SnapshotID != 7 || body.OrderID != 999 || body.Side != 0 || body.OrderFlags != 1 ||
		!body.EnterTimestamp.Equal(enter) || body.PriceRaw != 82446 || body.QtyRaw != 3031 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotEnd(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint64(buf[4:12], 5000)
	binary.LittleEndian.PutUint32(buf[12:16], 7)

	body, err := ParseSnapshotEnd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.AnchorSeq != 5000 || body.SnapshotID != 7 {
		t.Errorf("body: %+v", body)
	}
}

// buildSingleMessageFrame wraps an application message body in a complete frame.
func buildSingleMessageFrame(t *testing.T, msgType uint8, msgLength uint8, msgBody []byte) []byte {
	t.Helper()
	frameLen := frameHeaderSize + messageHeaderSize + len(msgBody)
	buf := buildFrameHeader(mboMagic, mboSchemaVersionV1, 1, 100, time.Unix(1700000020, 0), 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgType
	mh[1] = msgLength
	binary.LittleEndian.PutUint16(mh[2:4], 0)
	buf = append(buf, mh...)
	buf = append(buf, msgBody...)
	return buf
}

func TestMarketByOrderParser_OrderAdd(t *testing.T) {
	enter := time.Unix(1700000010, 0)
	body := make([]byte, 48)
	binary.LittleEndian.PutUint32(body[0:4], 100)
	binary.LittleEndian.PutUint16(body[4:6], 1)
	body[6] = 0
	binary.LittleEndian.PutUint32(body[8:12], 42)
	binary.LittleEndian.PutUint64(body[12:20], 999)
	binary.LittleEndian.PutUint64(body[20:28], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(body[28:36], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(body[36:44], 3031)

	frame := buildSingleMessageFrame(t, msgTypeOrderAdd, messageHeaderSize+48, body)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r.Type != "order_add" || r.Port != "mktdata" || r.InstrumentID != 100 || r.SequenceNumber != 100 {
		t.Errorf("envelope: %+v", r)
	}
	if r.Fields["per_instrument_seq"].(uint32) != 42 || r.Fields["order_id"].(uint64) != 999 {
		t.Errorf("fields: %+v", r.Fields)
	}
}

func TestMarketByOrderParser_UnknownTypeSkipped(t *testing.T) {
	body := make([]byte, 8)
	frame := buildSingleMessageFrame(t, 0xFE, messageHeaderSize+8, body)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records for unknown type, got %d", len(recs))
	}
}

// buildOrderAddFrameWithTS constructs a complete MBO frame containing a single
// order_add message. Returns the frame bytes, the enter timestamp in nanoseconds,
// and the frame send timestamp in nanoseconds (enterNS != sendNS by design).
func buildOrderAddFrameWithTS(t *testing.T) (frame []byte, enterNS, sendNS uint64) {
	t.Helper()
	sendTS := time.Unix(1700000020, 111111111)
	enterTS := time.Unix(1700000010, 222222222)
	sendNS = uint64(sendTS.UnixNano())
	enterNS = uint64(enterTS.UnixNano())

	body := make([]byte, 48)
	binary.LittleEndian.PutUint32(body[0:4], 101)                    // InstrumentID
	binary.LittleEndian.PutUint16(body[4:6], 2)                      // SourceID
	body[6] = 0                                                      // Side = bid
	body[7] = 0                                                      // OrderFlags
	binary.LittleEndian.PutUint32(body[8:12], 55)                    // PerInstrumentSeq
	binary.LittleEndian.PutUint64(body[12:20], 888)                  // OrderID
	binary.LittleEndian.PutUint64(body[20:28], enterNS)              // EnterTimestamp
	binary.LittleEndian.PutUint64(body[28:36], uint64(int64(50000))) // PriceRaw
	binary.LittleEndian.PutUint64(body[36:44], 10)                   // QtyRaw

	msgLen := uint8(messageHeaderSize + 48)
	frameLen := frameHeaderSize + int(msgLen)
	hdr := buildFrameHeader(mboMagic, mboSchemaVersionV1, 1, 200, sendTS, 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgTypeOrderAdd
	mh[1] = msgLen
	binary.LittleEndian.PutUint16(mh[2:4], 0)
	frame = append(hdr, mh...)
	frame = append(frame, body...)
	return frame, enterNS, sendNS
}

// TestParseFrame_BatchBoundaryHasNoSourceTS guards against treating a
// batch_boundary's BatchTime as a block/venue timestamp. BatchTime is a batch
// counter, not wall-clock, so the record must carry SourceTSNS == 0 (which the
// bot maps to a NULL source_ts and excludes from source latency).
func TestParseFrame_BatchBoundaryHasNoSourceTS(t *testing.T) {
	sendTS := time.Unix(1700000020, 111111111)
	body := make([]byte, 12)
	binary.LittleEndian.PutUint32(body[0:4], 7000)        // BatchID
	binary.LittleEndian.PutUint64(body[4:12], 1025401179) // BatchTime (counter, not epoch ns)

	msgLen := uint8(messageHeaderSize + 12)
	frameLen := frameHeaderSize + int(msgLen)
	hdr := buildFrameHeader(mboMagic, mboSchemaVersionV1, 1, 200, sendTS, 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgTypeBatchBoundary
	mh[1] = msgLen
	frame := append(hdr, mh...)
	frame = append(frame, body...)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "batch_boundary" {
		t.Fatalf("got %d records, want 1 batch_boundary", len(recs))
	}
	if recs[0].SourceTSNS != 0 {
		t.Errorf("batch_boundary SourceTSNS = %d, want 0", recs[0].SourceTSNS)
	}
	if recs[0].SendTSNS != uint64(sendTS.UnixNano()) {
		t.Errorf("batch_boundary SendTSNS = %d, want %d", recs[0].SendTSNS, uint64(sendTS.UnixNano()))
	}
}

func TestParseFrame_OrderAddEmitsSourceAndSendTS(t *testing.T) {
	frame, enterNS, sendNS := buildOrderAddFrameWithTS(t)
	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	r := recs[0]
	if r.SourceTSNS != enterNS {
		t.Errorf("SourceTSNS = %d, want %d (enter_ts)", r.SourceTSNS, enterNS)
	}
	if r.SendTSNS != sendNS {
		t.Errorf("SendTSNS = %d, want %d (frame send)", r.SendTSNS, sendNS)
	}
}

func TestMarketByOrderParser_TruncatedFrame(t *testing.T) {
	body := make([]byte, 48)
	frame := buildSingleMessageFrame(t, msgTypeOrderAdd, messageHeaderSize+48, body)
	// Truncate the frame to 30 bytes — header says 76, only 30 present.
	truncated := frame[:30]

	p := &marketByOrderParser{}
	_, err := p.ParseFrame("mktdata", truncated)
	if err == nil {
		t.Fatal("expected error on truncated frame")
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
		buf := buildFrameHeader(mboMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err != nil {
			t.Errorf("schema version %d must be accepted: %v", v, err)
		}
	}
	for _, v := range []uint8{0, 2, 4, 255} {
		buf := buildFrameHeader(mboMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// A publisher cutting over from v1 to v3 mid-stream must be followed without a
// restart. This is why the version is read per frame rather than latched from
// the first frame.
func TestParseFrame_FollowsVersionSwitchMidStream(t *testing.T) {
	p := &marketByOrderParser{}
	ts := time.Unix(1700000000, 0)

	build := func(version uint8, body []byte) []byte {
		msgLen := uint8(messageHeaderSize + len(body))
		frameLen := frameHeaderSize + int(msgLen)
		hdr := buildFrameHeader(mboMagic, version, 0, 1, ts, 1, 0, uint16(frameLen))
		mh := make([]byte, messageHeaderSize)
		mh[0] = msgTypeInstrumentDefinition
		mh[1] = msgLen
		frame := append(hdr, mh...)
		return append(frame, body...)
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
		recs, err := p.ParseFrame("refdata", tc.frame)
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

// Source ID must reach the record's Fields map, where the bot reads it.
func TestParseFrame_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	p := &marketByOrderParser{}
	ts := time.Unix(1700000000, 0)
	body := buildInstDefV3("BTC-USDT")
	msgLen := uint8(messageHeaderSize + len(body))
	frameLen := frameHeaderSize + int(msgLen)
	hdr := buildFrameHeader(mboMagic, 3, 0, 1, ts, 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgTypeInstrumentDefinition
	mh[1] = msgLen
	frame := append(append(hdr, mh...), body...)

	recs, err := p.ParseFrame("refdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	if got := recs[0].Fields["source_id"]; got != uint16(77) {
		t.Errorf("source_id: got %v (%T) want uint16(77)", got, got)
	}
}
