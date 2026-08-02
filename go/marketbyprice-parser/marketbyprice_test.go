package main

import (
	"encoding/binary"
	"testing"
	"time"
)

// buildFrame assembles a full frame from pre-encoded message byte slices.
func buildFrame(t *testing.T, channel uint8, seq uint64, ts time.Time, resetCount uint8, msgs ...[]byte) []byte {
	t.Helper()
	total := frameHeaderSize
	for _, m := range msgs {
		total += len(m)
	}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, channel, seq, ts, uint8(len(msgs)), resetCount, uint16(total))
	for _, m := range msgs {
		frame = append(frame, m...)
	}
	return frame
}

// buildMsg prefixes a 4-byte application message header onto a body.
func buildMsg(msgType uint8, flags uint16, body []byte) []byte {
	m := make([]byte, messageHeaderSize+len(body))
	m[0] = msgType
	m[1] = uint8(messageHeaderSize + len(body))
	binary.LittleEndian.PutUint16(m[2:4], flags)
	copy(m[messageHeaderSize:], body)
	return m
}

func levelUpdateBody(instID uint32, side uint8, piSeq uint32, price int64, qty uint64, orderCount, levelIdx uint16, action, reason uint8) []byte {
	b := make([]byte, 44)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint16(b[4:6], 1)
	b[6] = side
	b[7] = action
	binary.LittleEndian.PutUint32(b[8:12], piSeq)
	binary.LittleEndian.PutUint64(b[12:20], uint64(price))
	binary.LittleEndian.PutUint64(b[20:28], qty)
	binary.LittleEndian.PutUint64(b[28:36], uint64(time.Unix(1700000100, 0).UnixNano()))
	binary.LittleEndian.PutUint16(b[36:38], orderCount)
	binary.LittleEndian.PutUint16(b[38:40], levelIdx)
	b[40] = reason
	return b
}

func TestParseFrame_MultipleMessages(t *testing.T) {
	p := &marketByPriceParser{}
	ts := time.Unix(1700000200, 0)
	frame := buildFrame(t, 3, 500, ts, 2,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 100, 1000, 50, 2, 0, 1, 1)),
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 1, 101, 1010, 75, 3, 0, 2, 3)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2", len(recs))
	}
	for i, r := range recs {
		if r.Type != "level_update" {
			t.Errorf("rec %d type: %q", i, r.Type)
		}
		if r.ChannelID != 3 || r.SequenceNumber != 500 || r.ResetCount != 2 || r.Port != "mktdata" {
			t.Errorf("rec %d envelope: %+v", i, r)
		}
		if r.InstrumentID != 11 {
			t.Errorf("rec %d instrument: %d", i, r.InstrumentID)
		}
	}
	if recs[0].Fields["side"] != "bid" || recs[1].Fields["side"] != "ask" {
		t.Errorf("sides: %v %v", recs[0].Fields["side"], recs[1].Fields["side"])
	}
	if recs[0].Fields["update_reason"] != "trade" || recs[1].Fields["update_reason"] != "new_order" {
		t.Errorf("reasons: %v %v", recs[0].Fields["update_reason"], recs[1].Fields["update_reason"])
	}
	if recs[0].Fields["action"] != "new" || recs[1].Fields["action"] != "change" {
		t.Errorf("actions: %v %v", recs[0].Fields["action"], recs[1].Fields["action"])
	}
}

// 0xFFFF means absent. The key must be omitted rather than carrying 65535.
func TestParseFrame_SentinelsOmitted(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000201, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, u16Unavailable, u16Unavailable, 2, 2)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := recs[0].Fields["order_count"]; present {
		t.Error("order_count must be omitted when 0xFFFF")
	}
	if _, present := recs[0].Fields["level_index"]; present {
		t.Error("level_index must be omitted when 0xFFFF")
	}
}

// Order Count 0 is a real value on a LevelUpdate, not a sentinel.
func TestParseFrame_ZeroOrderCountPresent(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000202, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 0, 0, 2, 2)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	got, present := recs[0].Fields["order_count"]
	if !present {
		t.Fatal("order_count 0 must be present")
	}
	if got.(uint16) != 0 {
		t.Errorf("order_count: got %v want 0", got)
	}
}

// Unknown types are skipped by Message Length, and the rest of the frame decodes.
func TestParseFrame_UnknownTypeSkipped(t *testing.T) {
	p := &marketByPriceParser{}
	reserved := buildMsg(0x55, 0, make([]byte, 20)) // reserved positional-index range
	frame := buildFrame(t, 0, 1, time.Unix(1700000203, 0), 0,
		reserved,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "level_update" {
		t.Fatalf("records: %+v", recs)
	}
}

// A Message Length below the 4-byte floor must error, not advance by zero and
// spin forever on one malformed datagram.
func TestParseFrame_ZeroMessageLengthTerminates(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 1, time.Unix(1700000204, 0), 1, 0, frameHeaderSize+4)
	frame = append(frame, 0x40, 0x00, 0x00, 0x00) // Type 0x40, Length 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
			t.Error("expected error for message length 0")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ParseFrame did not terminate on message length 0")
	}
}

func TestParseFrame_MessageLengthOverrunsFrame(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 1, time.Unix(1700000205, 0), 1, 0, frameHeaderSize+4)
	frame = append(frame, 0x40, 200, 0x00, 0x00) // claims 200 bytes, only 4 present
	if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
		t.Fatal("expected error for length overrunning the frame")
	}
}

func TestParseFrame_BadMagicRejected(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(0x4444, mbpSchemaVersion, 0, 1, time.Unix(1700000206, 0), 0, 0, frameHeaderSize)
	if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
		t.Fatal("expected error for market-by-order magic")
	}
}

// A malformed BookClear is dropped from the record stream and counted, without
// failing the whole frame — its neighbors still decode.
func TestParseFrame_MalformedBookClearDropsMessageNotFrame(t *testing.T) {
	p := &marketByPriceParser{}
	bad := make([]byte, 32)
	bad[6] = 2 // Clear Side: both
	bad[7] = 1 // Scope: from price — malformed combination
	frame := buildFrame(t, 0, 1, time.Unix(1700000207, 0), 0,
		buildMsg(msgTypeBookClear, 0, bad),
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("malformed body must not fail the frame: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "level_update" {
		t.Fatalf("records: %+v", recs)
	}
	if defects.MalformedBookClear != 1 {
		t.Errorf("malformed book clear count: got %d want 1", defects.MalformedBookClear)
	}
}

// Flags bit 0 must be set on the snapshot port and clear elsewhere. Disagreement
// is a publisher defect that is counted, never used for routing.
func TestParseFrame_SnapshotFlagMismatchCounted(t *testing.T) {
	p := &marketByPriceParser{}
	// Snapshot flag set on mktdata: wrong.
	frame := buildFrame(t, 0, 1, time.Unix(1700000208, 0), 0,
		buildMsg(msgTypeLevelUpdate, flagSnapshot, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("mismatch must not drop the record: %+v", recs)
	}
	if defects.SnapshotFlagMismatch != 1 {
		t.Errorf("mismatch count: got %d want 1", defects.SnapshotFlagMismatch)
	}

	// Snapshot flag clear on the snapshot port: also wrong.
	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 11)
	frame2 := buildFrame(t, 0, 1, time.Unix(1700000209, 0), 0,
		buildMsg(msgTypeSnapshotBegin, 0, sb),
	)
	_, defects2, err := p.ParseFrame("snapshot", frame2)
	if err != nil {
		t.Fatal(err)
	}
	if defects2.SnapshotFlagMismatch != 1 {
		t.Errorf("mismatch count: got %d want 1 (counts are per frame, not cumulative)", defects2.SnapshotFlagMismatch)
	}
}

// Correctly-flagged messages produce no defects, on either port.
func TestParseFrame_CorrectFlagsNoDefects(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000213, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	if _, d, err := p.ParseFrame("mktdata", frame); err != nil || d.SnapshotFlagMismatch != 0 {
		t.Errorf("mktdata clean frame: defects=%+v err=%v", d, err)
	}

	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 11)
	frame2 := buildFrame(t, 0, 1, time.Unix(1700000214, 0), 0,
		buildMsg(msgTypeSnapshotBegin, flagSnapshot, sb),
	)
	if _, d, err := p.ParseFrame("snapshot", frame2); err != nil || d.SnapshotFlagMismatch != 0 {
		t.Errorf("snapshot clean frame: defects=%+v err=%v", d, err)
	}
}

func TestParseFrame_SnapshotGroupDecodes(t *testing.T) {
	p := &marketByPriceParser{}
	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 42)
	binary.LittleEndian.PutUint64(sb[4:12], 7000)
	binary.LittleEndian.PutUint32(sb[12:16], 1)
	binary.LittleEndian.PutUint32(sb[16:20], 5)
	binary.LittleEndian.PutUint32(sb[20:24], 600)
	binary.LittleEndian.PutUint32(sb[32:36], 25) // Depth Bound: bounded

	sl := make([]byte, 28)
	binary.LittleEndian.PutUint32(sl[0:4], 5)
	binary.LittleEndian.PutUint64(sl[4:12], uint64(int64(1234)))
	binary.LittleEndian.PutUint64(sl[12:20], 900)
	binary.LittleEndian.PutUint16(sl[20:22], 3)
	sl[22] = 1 // ask

	se := make([]byte, 16)
	binary.LittleEndian.PutUint32(se[0:4], 42)
	binary.LittleEndian.PutUint64(se[4:12], 7000)
	binary.LittleEndian.PutUint32(se[12:16], 5)

	frame := buildFrame(t, 0, 10, time.Unix(1700000210, 0), 0,
		buildMsg(msgTypeSnapshotBegin, flagSnapshot, sb),
		buildMsg(msgTypeSnapshotLevel, flagSnapshot, sl),
		buildMsg(msgTypeSnapshotEnd, flagSnapshot, se),
	)
	recs, _, err := p.ParseFrame("snapshot", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("records: got %d want 3", len(recs))
	}
	if recs[0].Type != "snapshot_begin" || recs[1].Type != "snapshot_level" || recs[2].Type != "snapshot_end" {
		t.Fatalf("types: %s %s %s", recs[0].Type, recs[1].Type, recs[2].Type)
	}
	if recs[0].Fields["depth_bound"].(uint32) != 25 {
		t.Errorf("depth_bound: %v", recs[0].Fields["depth_bound"])
	}
	if recs[0].Fields["total_levels"].(uint32) != 1 {
		t.Errorf("total_levels: %v", recs[0].Fields["total_levels"])
	}
	// SnapshotLevel carries no Instrument ID; it is implied by the group.
	if recs[1].InstrumentID != 0 {
		t.Errorf("snapshot_level must not invent an instrument id: %d", recs[1].InstrumentID)
	}
	if recs[1].Fields["side"] != "ask" {
		t.Errorf("side: %v", recs[1].Fields["side"])
	}
}

func TestParseFrame_TradeAndLiquidation(t *testing.T) {
	p := &marketByPriceParser{}
	tb := make([]byte, 48)
	binary.LittleEndian.PutUint32(tb[0:4], 9)
	tb[6] = 2 // Aggressor: sell
	binary.LittleEndian.PutUint64(tb[8:16], uint64(time.Unix(1700000211, 0).UnixNano()))
	binary.LittleEndian.PutUint64(tb[32:40], 4242)

	lb := make([]byte, 44)
	binary.LittleEndian.PutUint32(lb[0:4], 9)
	lb[7] = 1 // Method: backstop
	binary.LittleEndian.PutUint64(lb[8:16], 4242)

	frame := buildFrame(t, 0, 20, time.Unix(1700000212, 0), 0,
		buildMsg(msgTypeTrade, 0, tb),
		buildMsg(msgTypeLiquidation, 0, lb),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Type != "trade" || recs[1].Type != "liquidation" {
		t.Fatalf("records: %+v", recs)
	}
	if recs[0].Fields["aggressor_side"] != "sell" {
		t.Errorf("aggressor: %v", recs[0].Fields["aggressor_side"])
	}
	// The liquidation pairs with its trade by Trade ID, in the same frame.
	if recs[0].Fields["trade_id"].(uint64) != recs[1].Fields["trade_id"].(uint64) {
		t.Error("trade id must match between trade and liquidation")
	}
	if recs[1].Fields["method"] != "backstop" {
		t.Errorf("method: %v", recs[1].Fields["method"])
	}
}

func TestParserRegistry(t *testing.T) {
	p, err := newParser("marketbyprice")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "marketbyprice" {
		t.Errorf("name: %q", p.Name())
	}
	if _, err := newParser("nope"); err == nil {
		t.Error("expected error for unknown parser")
	}
}
