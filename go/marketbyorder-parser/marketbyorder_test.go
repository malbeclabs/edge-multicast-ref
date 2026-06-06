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

func TestParseFrameHeader_Valid(t *testing.T) {
	ts := time.Unix(1700000000, 123456789)
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 7, 42, ts, 3, 1, frameHeaderSize)
	h, err := ParseFrameHeader(buf)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Magic != dobMagic {
		t.Errorf("magic: got %x want %x", h.Magic, dobMagic)
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
	buf := buildFrameHeader(0xDEAD, dobSchemaVersion, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errBadMagic) {
		t.Fatalf("expected errBadMagic, got %v", err)
	}
}

func TestParseFrameHeader_WrongVersion(t *testing.T) {
	buf := buildFrameHeader(dobMagic, 99, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errSchemaVersion) {
		t.Fatalf("expected errSchemaVersion, got %v", err)
	}
}

func TestParseFrameHeader_LengthMismatch(t *testing.T) {
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 0, 0, time.Now(), 0, 0, 999)
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
	buf[0] = 7  // ChannelID
	buf[1] = 1  // Valid
	binary.LittleEndian.PutUint16(buf[4:6], 100)  // ManifestSeq
	binary.LittleEndian.PutUint32(buf[8:12], 25)  // InstrumentCount
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
	binary.LittleEndian.PutUint32(buf[0:4], 12345)             // InstrumentID
	copy(buf[4:20], "BTC-USDT")                                // Symbol (null-padded)
	copy(buf[20:28], "BTC")                                    // Leg1
	copy(buf[28:36], "USDT")                                   // Leg2
	buf[36] = 1                                                // AssetClass = Crypto Spot
	priceExp, qtyExp := int8(-2), int8(-8)
	buf[37] = uint8(priceExp)                                  // PriceExponent
	buf[38] = uint8(qtyExp)                                    // QtyExponent
	buf[39] = 1                                                // MarketModel = CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1))) // TickSize
	binary.LittleEndian.PutUint64(buf[48:56], 100)             // LotSize
	binary.LittleEndian.PutUint64(buf[56:64], 0)               // ContractValue
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1                                                // SettleType
	buf[73] = 0                                                // PriceBound
	binary.LittleEndian.PutUint16(buf[74:76], 7)               // ManifestSeq

	body, err := ParseInstrumentDefinition(buf)
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
	buf[6] = 0  // bid
	buf[7] = 1  // post-only flag
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
	buf[6] = 1  // UserCancel
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
	buf[6] = 1  // Buy aggressor
	buf[7] = 1  // full-fill flag
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
	buf[4] = 1  // PublisherInconsistency
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
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 1, 100, time.Unix(1700000020, 0), 1, 0, uint16(frameLen))
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
