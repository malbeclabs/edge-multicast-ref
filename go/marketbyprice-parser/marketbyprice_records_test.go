package main

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// This file covers the wire→Record mapping layer: for every implemented Type ID,
// that ParseFrame produces the expected record Type and Fields. The wire-level
// Parse* functions are tested in marketbyprice_wire_test.go; what is asserted
// here is the part a wrong field name or a swapped stringer would break silently.

// recordTestTS is the venue/publisher timestamp every body below carries, so an
// assertion can distinguish a decoded timestamp from a zero value.
var recordTestTS = time.Unix(1700000300, 0).UTC()

func putTS(b []byte, t time.Time) { binary.LittleEndian.PutUint64(b, uint64(t.UnixNano())) }

func heartbeatBody(channel uint8) []byte {
	b := make([]byte, 12)
	b[0] = channel
	putTS(b[4:12], recordTestTS)
	return b
}

func instrumentDefinitionBody(instID uint32, symbol string) []byte {
	b := make([]byte, 76)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	copy(b[4:20], symbol)
	copy(b[20:28], "LEG1")
	copy(b[28:36], "LEG2")
	b[36] = 2                                     // asset class
	b[37] = 0xF8                                  // price exponent, int8 -8
	b[38] = 0xFE                                  // qty exponent, int8 -2
	b[39] = 1                                     // market model
	binary.LittleEndian.PutUint64(b[40:48], 25)   // tick size
	binary.LittleEndian.PutUint64(b[48:56], 100)  // lot size
	binary.LittleEndian.PutUint64(b[56:64], 1000) // contract value
	putTS(b[64:72], recordTestTS)                 // expiry
	b[72] = 1                                     // settle type
	b[73] = 1                                     // price bound
	binary.LittleEndian.PutUint16(b[74:76], 77)   // manifest seq
	return b
}

func tradeBody(instID uint32, aggressor uint8, tradeID uint64) []byte {
	b := make([]byte, 48)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint16(b[4:6], 1)
	b[6] = aggressor
	putTS(b[8:16], recordTestTS)
	binary.LittleEndian.PutUint64(b[16:24], uint64(int64(1234)))
	binary.LittleEndian.PutUint64(b[24:32], 10)
	binary.LittleEndian.PutUint64(b[32:40], tradeID)
	binary.LittleEndian.PutUint64(b[40:48], 999)
	return b
}

func endOfSessionBody() []byte {
	b := make([]byte, 8)
	putTS(b[0:8], recordTestTS)
	return b
}

func manifestSummaryBody(instrumentCount uint32) []byte {
	b := make([]byte, 20)
	b[0] = 0
	b[1] = 1 // valid
	binary.LittleEndian.PutUint16(b[4:6], 12)
	binary.LittleEndian.PutUint32(b[8:12], instrumentCount)
	putTS(b[12:20], recordTestTS)
	return b
}

func liquidationBody(instID uint32, flags, method uint8) []byte {
	b := make([]byte, 44)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	b[6] = flags
	b[7] = method
	binary.LittleEndian.PutUint64(b[8:16], 4242)
	binary.LittleEndian.PutUint64(b[16:24], uint64(int64(555)))
	for i := 24; i < 44; i++ {
		b[i] = 0xAB
	}
	return b
}

func batchBoundaryBody(batchID uint32) []byte {
	b := make([]byte, 12)
	binary.LittleEndian.PutUint32(b[0:4], batchID)
	putTS(b[4:12], recordTestTS)
	return b
}

func instrumentResetBody(instID uint32, reason uint8, anchor uint64) []byte {
	b := make([]byte, 24)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	b[4] = reason
	binary.LittleEndian.PutUint64(b[8:16], anchor)
	putTS(b[16:24], recordTestTS)
	return b
}

func snapshotBeginBody(instID uint32, snapshotID, depthBound uint32) []byte {
	b := make([]byte, 36)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint64(b[4:12], 7000)
	binary.LittleEndian.PutUint32(b[12:16], 3)
	binary.LittleEndian.PutUint32(b[16:20], snapshotID)
	binary.LittleEndian.PutUint32(b[20:24], 600)
	putTS(b[24:32], recordTestTS)
	binary.LittleEndian.PutUint32(b[32:36], depthBound)
	return b
}

func snapshotEndBody(instID uint32, snapshotID uint32) []byte {
	b := make([]byte, 16)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint64(b[4:12], 7000)
	binary.LittleEndian.PutUint32(b[12:16], snapshotID)
	return b
}

func snapshotLevelBody(snapshotID uint32, side uint8, orderCount uint16) []byte {
	b := make([]byte, 28)
	binary.LittleEndian.PutUint32(b[0:4], snapshotID)
	binary.LittleEndian.PutUint64(b[4:12], uint64(int64(1234)))
	binary.LittleEndian.PutUint64(b[12:20], 900)
	binary.LittleEndian.PutUint16(b[20:22], orderCount)
	b[22] = side
	b[23] = 0x01 // implied
	return b
}

// bookClearBody builds a BookClear. scope=1 with clearSide=2 is the malformed
// combination and is rejected by the wire layer.
func bookClearBody(instID uint32, clearSide, scope, reason uint8, fromPrice int64) []byte {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint16(b[4:6], 1)
	b[6] = clearSide
	b[7] = scope
	binary.LittleEndian.PutUint32(b[8:12], 300)
	binary.LittleEndian.PutUint64(b[12:20], uint64(fromPrice))
	putTS(b[20:28], recordTestTS)
	b[28] = reason
	return b
}

// field fetches a Fields key, failing the test when it is absent.
func field(t *testing.T, rec Record, key string) any {
	t.Helper()
	v, ok := rec.Fields[key]
	if !ok {
		t.Fatalf("%s: Fields[%q] missing; got keys %v", rec.Type, key, fieldKeys(rec))
	}
	return v
}

func fieldKeys(rec Record) []string {
	keys := make([]string, 0, len(rec.Fields))
	for k := range rec.Fields {
		keys = append(keys, k)
	}
	return keys
}

// TestParseFrame_AllTypesDecodeToRecords walks every implemented Type ID through
// ParseFrame and asserts the record Type plus the Fields entries most likely to
// be wrong: enum stringers, and the keys whose names are the decoder's contract
// with the bot. Each case uses its spec-assigned port and the matching snapshot
// flag, so a non-zero SnapshotFlagMismatch would also fail here.
func TestParseFrame_AllTypesDecodeToRecords(t *testing.T) {
	cases := []struct {
		name     string
		msgType  uint8
		port     string
		flags    uint16
		body     []byte
		wantType string
		check    func(t *testing.T, rec Record)
	}{
		{
			name: "heartbeat", msgType: msgTypeHeartbeat, port: "mktdata",
			body: heartbeatBody(7), wantType: "heartbeat",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "channel_id_in_body").(uint8); got != 7 {
					t.Errorf("channel_id_in_body: got %d want 7", got)
				}
				if got := field(t, rec, "timestamp").(time.Time); !got.Equal(recordTestTS) {
					t.Errorf("timestamp: got %v want %v", got, recordTestTS)
				}
			},
		},
		{
			name: "instrument_definition", msgType: msgTypeInstrumentDefinition, port: "refdata",
			body: instrumentDefinitionBody(11, "BTCUSD"), wantType: "instrument_definition",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 11 {
					t.Errorf("instrument id: got %d want 11", rec.InstrumentID)
				}
				if got := field(t, rec, "symbol").(string); got != "BTCUSD" {
					t.Errorf("symbol: got %q want BTCUSD", got)
				}
				if got := field(t, rec, "leg1").(string); got != "LEG1" {
					t.Errorf("leg1: got %q", got)
				}
				if got := field(t, rec, "price_exponent").(int8); got != -8 {
					t.Errorf("price_exponent: got %d want -8", got)
				}
				if got := field(t, rec, "manifest_seq").(uint16); got != 77 {
					t.Errorf("manifest_seq: got %d want 77", got)
				}
			},
		},
		{
			name: "trade", msgType: msgTypeTrade, port: "mktdata",
			body: tradeBody(9, 1, 4242), wantType: "trade",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 9 {
					t.Errorf("instrument id: got %d want 9", rec.InstrumentID)
				}
				if got := field(t, rec, "aggressor_side").(string); got != "buy" {
					t.Errorf("aggressor_side: got %q want buy", got)
				}
				if got := field(t, rec, "trade_id").(uint64); got != 4242 {
					t.Errorf("trade_id: got %d want 4242", got)
				}
				// Trade carries a venue Source Timestamp, so source_ts_ns is set.
				if rec.SourceTSNS != uint64(recordTestTS.UnixNano()) {
					t.Errorf("source_ts_ns: got %d want %d", rec.SourceTSNS, recordTestTS.UnixNano())
				}
			},
		},
		{
			name: "end_of_session", msgType: msgTypeEndOfSession, port: "mktdata",
			body: endOfSessionBody(), wantType: "end_of_session",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "timestamp").(time.Time); !got.Equal(recordTestTS) {
					t.Errorf("timestamp: got %v want %v", got, recordTestTS)
				}
			},
		},
		{
			name: "manifest_summary", msgType: msgTypeManifestSummary, port: "refdata",
			body: manifestSummaryBody(150), wantType: "manifest_summary",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "valid").(uint8); got != 1 {
					t.Errorf("valid: got %d want 1", got)
				}
				if got := field(t, rec, "instrument_count").(uint32); got != 150 {
					t.Errorf("instrument_count: got %d want 150", got)
				}
				if got := field(t, rec, "manifest_seq").(uint16); got != 12 {
					t.Errorf("manifest_seq: got %d want 12", got)
				}
			},
		},
		{
			// Flags bit 0 set = liquidated side short; bit 1 set = ADL.
			name: "liquidation", msgType: msgTypeLiquidation, port: "mktdata",
			body: liquidationBody(9, 0x03, 0), wantType: "liquidation",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "liquidated_side").(string); got != "short" {
					t.Errorf("liquidated_side: got %q want short", got)
				}
				if got := field(t, rec, "adl").(bool); !got {
					t.Error("adl: got false want true")
				}
				if got := field(t, rec, "method").(string); got != "market" {
					t.Errorf("method: got %q want market", got)
				}
				if got := field(t, rec, "liquidated_user").(string); len(got) != 40 {
					t.Errorf("liquidated_user: got %q, want 40 hex chars", got)
				}
			},
		},
		{
			name: "batch_boundary", msgType: msgTypeBatchBoundary, port: "mktdata",
			body: batchBoundaryBody(88), wantType: "batch_boundary",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "batch_id").(uint32); got != 88 {
					t.Errorf("batch_id: got %d want 88", got)
				}
				if got := field(t, rec, "batch_ts").(time.Time); !got.Equal(recordTestTS) {
					t.Errorf("batch_ts: got %v want %v", got, recordTestTS)
				}
				// A batch marker is not a venue timestamp for a book event.
				if rec.SourceTSNS != 0 {
					t.Errorf("batch_boundary must not set source_ts_ns, got %d", rec.SourceTSNS)
				}
			},
		},
		{
			name: "instrument_reset", msgType: msgTypeInstrumentReset, port: "mktdata",
			body: instrumentResetBody(11, 2, 9000), wantType: "instrument_reset",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 11 {
					t.Errorf("instrument id: got %d want 11", rec.InstrumentID)
				}
				if got := field(t, rec, "reason").(string); got != "venue_resync" {
					t.Errorf("reason: got %q want venue_resync", got)
				}
				if got := field(t, rec, "new_anchor_seq").(uint64); got != 9000 {
					t.Errorf("new_anchor_seq: got %d want 9000", got)
				}
			},
		},
		{
			name: "snapshot_begin", msgType: msgTypeSnapshotBegin, port: "snapshot", flags: flagSnapshot,
			body: snapshotBeginBody(42, 5, 25), wantType: "snapshot_begin",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 42 {
					t.Errorf("instrument id: got %d want 42", rec.InstrumentID)
				}
				if got := field(t, rec, "snapshot_id").(uint32); got != 5 {
					t.Errorf("snapshot_id: got %d want 5", got)
				}
				if got := field(t, rec, "depth_bound").(uint32); got != 25 {
					t.Errorf("depth_bound: got %d want 25", got)
				}
				if got := field(t, rec, "last_instrument_seq").(uint32); got != 600 {
					t.Errorf("last_instrument_seq: got %d want 600", got)
				}
			},
		},
		{
			name: "snapshot_level", msgType: msgTypeSnapshotLevel, port: "snapshot", flags: flagSnapshot,
			body: snapshotLevelBody(5, 0, 3), wantType: "snapshot_level",
			check: func(t *testing.T, rec Record) {
				// No Instrument ID on the wire; it is implied by the open group.
				if rec.InstrumentID != 0 {
					t.Errorf("snapshot_level must not invent an instrument id: %d", rec.InstrumentID)
				}
				if got := field(t, rec, "side").(string); got != "bid" {
					t.Errorf("side: got %q want bid", got)
				}
				if got := field(t, rec, "order_count").(uint16); got != 3 {
					t.Errorf("order_count: got %d want 3", got)
				}
				if got := field(t, rec, "implied").(bool); !got {
					t.Error("implied: got false want true")
				}
			},
		},
		{
			name: "snapshot_end", msgType: msgTypeSnapshotEnd, port: "snapshot", flags: flagSnapshot,
			body: snapshotEndBody(42, 5), wantType: "snapshot_end",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 42 {
					t.Errorf("instrument id: got %d want 42", rec.InstrumentID)
				}
				if got := field(t, rec, "anchor_seq").(uint64); got != 7000 {
					t.Errorf("anchor_seq: got %d want 7000", got)
				}
			},
		},
		{
			name: "level_update", msgType: msgTypeLevelUpdate, port: "mktdata",
			body: levelUpdateBody(11, 1, 100, 1000, 50, 4, 0, 3, 2), wantType: "level_update",
			check: func(t *testing.T, rec Record) {
				if got := field(t, rec, "side").(string); got != "ask" {
					t.Errorf("side: got %q want ask", got)
				}
				if got := field(t, rec, "action").(string); got != "delete" {
					t.Errorf("action: got %q want delete", got)
				}
				if got := field(t, rec, "update_reason").(string); got != "cancel" {
					t.Errorf("update_reason: got %q want cancel", got)
				}
				if got := field(t, rec, "qty_raw").(uint64); got != 50 {
					t.Errorf("qty_raw: got %d want 50", got)
				}
			},
		},
		{
			// The BookClear success path: all three stringers, and Scope=0 must
			// omit from_price_raw.
			name: "book_clear_entire_side", msgType: msgTypeBookClear, port: "mktdata",
			body: bookClearBody(11, 2, 0, 1, 7777), wantType: "book_clear",
			check: func(t *testing.T, rec Record) {
				if rec.InstrumentID != 11 {
					t.Errorf("instrument id: got %d want 11", rec.InstrumentID)
				}
				if got := field(t, rec, "clear_side").(string); got != "both" {
					t.Errorf("clear_side: got %q want both", got)
				}
				if got := field(t, rec, "scope").(string); got != "entire_side" {
					t.Errorf("scope: got %q want entire_side", got)
				}
				if got := field(t, rec, "clear_reason").(string); got != "halt" {
					t.Errorf("clear_reason: got %q want halt", got)
				}
				if _, present := rec.Fields["from_price_raw"]; present {
					t.Error("from_price_raw must be omitted when scope != 1")
				}
				if rec.SourceTSNS != uint64(recordTestTS.UnixNano()) {
					t.Errorf("source_ts_ns: got %d want %d", rec.SourceTSNS, recordTestTS.UnixNano())
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := &marketByPriceParser{}
			frame := buildFrame(t, 1, 42, time.Unix(1700000301, 0), 0,
				buildMsg(tc.msgType, tc.flags, tc.body),
			)
			recs, defects, err := p.ParseFrame(tc.port, frame)
			if err != nil {
				t.Fatalf("ParseFrame: %v", err)
			}
			if len(recs) != 1 {
				t.Fatalf("records: got %d want 1", len(recs))
			}
			if recs[0].Type != tc.wantType {
				t.Fatalf("type: got %q want %q", recs[0].Type, tc.wantType)
			}
			if defects != (Defects{}) {
				t.Errorf("a well-formed, correctly-flagged message must produce no defects: %+v", defects)
			}
			// Envelope fields come from the frame header for every type.
			if recs[0].ChannelID != 1 || recs[0].SequenceNumber != 42 || recs[0].Port != tc.port {
				t.Errorf("envelope: %+v", recs[0])
			}
			tc.check(t, recs[0])
		})
	}
}

// Scope=1 is the only case in which From Price is meaningful, so it is the only
// case in which the key may appear.
func TestParseFrame_BookClearFromPriceOnlyWhenScopeIsFromPrice(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000302, 0), 0,
		buildMsg(msgTypeBookClear, 0, bookClearBody(11, 0, 1, 3, -500)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	rec := recs[0]
	if got := field(t, rec, "scope").(string); got != "from_price" {
		t.Errorf("scope: got %q want from_price", got)
	}
	if got := field(t, rec, "clear_side").(string); got != "bid" {
		t.Errorf("clear_side: got %q want bid", got)
	}
	if got := field(t, rec, "clear_reason").(string); got != "venue_reset" {
		t.Errorf("clear_reason: got %q want venue_reset", got)
	}
	// Signed: a negative price must survive the round trip.
	if got := field(t, rec, "from_price_raw").(int64); got != -500 {
		t.Errorf("from_price_raw: got %d want -500", got)
	}
}

// An unrecognised enum value is never an error; it renders as the unknown member.
func TestParseFrame_UnrecognisedEnumValuesDegradeToUnknown(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000303, 0), 0,
		// side=9, action=200, update_reason=100: none are defined values.
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 9, 1, 1000, 50, 1, 0, 200, 100)),
		buildMsg(msgTypeBookClear, 0, bookClearBody(11, 9, 9, 9, 0)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unrecognised enum values must not fail the frame: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2", len(recs))
	}
	if defects != (Defects{}) {
		t.Errorf("unrecognised enum values are not defects: %+v", defects)
	}
	for _, key := range []string{"side", "action", "update_reason"} {
		if got := field(t, recs[0], key).(string); got != "unknown" {
			t.Errorf("level_update %s: got %q want unknown", key, got)
		}
	}
	for _, key := range []string{"clear_side", "scope"} {
		if got := field(t, recs[1], key).(string); got != "unknown" {
			t.Errorf("book_clear %s: got %q want unknown", key, got)
		}
	}
	if got := field(t, recs[1], "clear_reason").(string); got != "unknown" {
		t.Errorf("book_clear clear_reason: got %q want unknown", got)
	}
}

// Skipping an unimplemented Type ID is spec-legal, but it must be observable:
// otherwise a publisher turning on a new message type silently loses data.
func TestParseFrame_UnknownTypeCounted(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000304, 0), 0,
		buildMsg(0x55, 0, make([]byte, 20)), // reserved positional-index range
		buildMsg(0x03, 0, make([]byte, 8)),  // reserved: Quote in the top-of-book feed
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "level_update" {
		t.Fatalf("records: %+v", recs)
	}
	if defects.UnknownType != 2 {
		t.Errorf("unknown type count: got %d want 2", defects.UnknownType)
	}
	// An unimplemented type is not a publisher defect and must not be counted
	// as a malformed message.
	if defects.MalformedOther != 0 || defects.MalformedBookClear != 0 {
		t.Errorf("unknown type must not count as malformed: %+v", defects)
	}
}

// A frame whose messages do not account for every byte Frame Length declares has
// Message Lengths inconsistent with Frame Length, which the spec makes a
// malformed frame. Silently ignoring the remainder loses the extra messages.
func TestParseFrame_TrailingBytesRejected(t *testing.T) {
	t.Run("trailing garbage", func(t *testing.T) {
		p := &marketByPriceParser{}
		msg := buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1))
		total := frameHeaderSize + len(msg) + 6 // 6 bytes the walk never reaches
		frame := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 1, time.Unix(1700000305, 0), 1, 0, uint16(total))
		frame = append(frame, msg...)
		frame = append(frame, 0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x00)
		if _, _, err := p.ParseFrame("mktdata", frame); !errors.Is(err, errFrameLength) {
			t.Fatalf("expected errFrameLength, got %v", err)
		}
	})

	t.Run("understated message count", func(t *testing.T) {
		// Two real messages, Message Count says one. The second would be lost.
		p := &marketByPriceParser{}
		a := buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1))
		b := buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 1, 2, 1010, 60, 1, 0, 1, 1))
		total := frameHeaderSize + len(a) + len(b)
		frame := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 1, time.Unix(1700000306, 0), 1, 0, uint16(total))
		frame = append(frame, a...)
		frame = append(frame, b...)
		if _, _, err := p.ParseFrame("mktdata", frame); !errors.Is(err, errFrameLength) {
			t.Fatalf("expected errFrameLength, got %v", err)
		}
	})
}

// The spec gives Message Count a range of 1-255. A bare 24-byte frame is the one
// remaining shape that would otherwise decode as valid-but-empty and be counted
// nowhere, since the loop body is unreachable and no body bytes are left over.
func TestParseFrame_ZeroMessageCountRejected(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 1, time.Unix(1700000308, 0), 0, 0, frameHeaderSize)
	if _, _, err := p.ParseFrame("mktdata", frame); !errors.Is(err, errMessageCount) {
		t.Fatalf("expected errMessageCount, got %v", err)
	}
	// The reason label an operator sees must name the real cause, not fall
	// through to "other".
	if got := classifyError(errMessageCount); got != "message_count" {
		t.Errorf("classifyError: got %q want message_count", got)
	}
}

// A frame in which every message is skipped is valid and yields an empty,
// non-nil slice — the contract documented on the Parser interface.
func TestParseFrame_AllSkippedYieldsEmptyNonNilSlice(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000307, 0), 0,
		buildMsg(0x55, 0, make([]byte, 20)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if recs == nil {
		t.Error("records must be non-nil on success")
	}
	if len(recs) != 0 {
		t.Errorf("records: got %d want 0", len(recs))
	}
	if defects.UnknownType != 1 {
		t.Errorf("unknown type count: got %d want 1", defects.UnknownType)
	}
}

// FuzzParseFrame asserts that no attacker-controlled datagram can panic the
// decoder, and that ParseFrame's post-conditions hold for every input that
// decodes. The frame body is fully attacker-controlled over multicast, so the
// bounds arithmetic in the message walk is the highest-value thing to fuzz.
func FuzzParseFrame(f *testing.F) {
	valid := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 1, 10, recordTestTS, 1, 0, frameHeaderSize+48)
	valid = append(valid, buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1))...)
	f.Add(valid)

	clear := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 1, 11, recordTestTS, 1, 0, frameHeaderSize+36)
	clear = append(clear, buildMsg(msgTypeBookClear, 0, bookClearBody(11, 0, 1, 1, 100))...)
	f.Add(clear)

	f.Add([]byte{})
	f.Add(make([]byte, frameHeaderSize))
	// Bare well-formed header, Message Count 0 — must be rejected, not read as an
	// empty frame.
	f.Add(buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 1, recordTestTS, 0, 0, frameHeaderSize))
	// Message Length 0 — the walk must not advance by zero and spin.
	spin := buildFrameHeader(mbpMagic, mbpSchemaVersionV1, 0, 1, recordTestTS, 255, 0, frameHeaderSize+4)
	f.Add(append(spin, 0x40, 0x00, 0x00, 0x00))

	p := &marketByPriceParser{}
	ports := []string{"refdata", "mktdata", "snapshot"}

	f.Fuzz(func(t *testing.T, frame []byte) {
		for _, port := range ports {
			recs, defects, err := p.ParseFrame(port, frame)
			if err != nil {
				if recs != nil {
					t.Errorf("records must be nil on error, got %d", len(recs))
				}
				continue
			}
			if recs == nil {
				t.Fatal("records must be non-nil on success")
			}
			// Every message either yields a record or is counted, and the walk is
			// bounded by Message Count.
			count := int(frame[20])
			if len(recs)+defects.UnknownType+defects.MalformedBookClear+defects.MalformedOther != count {
				t.Errorf("port %s: %d records + %d unknown + %d malformed != message count %d",
					port, len(recs), defects.UnknownType,
					defects.MalformedBookClear+defects.MalformedOther, count)
			}
			for _, rec := range recs {
				if rec.Type == "" {
					t.Error("decoded record with empty type")
				}
				if rec.Port != port {
					t.Errorf("record port: got %q want %q", rec.Port, port)
				}
			}
		}
	})
}
