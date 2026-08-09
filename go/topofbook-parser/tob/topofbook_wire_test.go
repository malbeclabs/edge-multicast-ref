package tob

import (
	"encoding/binary"
	"strings"
	"testing"
)

// This parser trims null padding from Symbol/Leg1/Leg2 later, in
// topofbook.go via trimNull — decodeTopOfBookBody itself returns the raw,
// null-padded fixed-width field. Assertions below trim before comparing,
// matching how the rest of the parser consumes these values.

// buildInstDefBodyV1 builds a 76-byte v1 InstrumentDefinition body (no
// message header — just the bytes decodeTopOfBookBody receives).
func buildInstDefBodyV1(instID uint32, symbol, leg1, leg2 string) []byte {
	b := make([]byte, 76)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	copy(b[4:20], symbol)
	copy(b[20:28], leg1)
	copy(b[28:36], leg2)
	b[36] = 1 // asset class
	// Typed variables, not constant conversions: byte(int8(-2)) is a
	// compile-time overflow error because the operand is a constant.
	priceExp, qtyExp := int8(-2), int8(-8)
	b[37] = byte(priceExp)
	b[38] = byte(qtyExp)
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

// buildInstDefBodyV3 builds a 126-byte v3 InstrumentDefinition body. Source ID
// is inserted at 4:6 and Symbol widens to 64 bytes, so every field after
// Instrument ID sits 50 bytes later than in v1.
func buildInstDefBodyV3(instID uint32, symbol, leg1, leg2 string) []byte {
	b := make([]byte, 126)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint16(b[4:6], 77) // source id
	copy(b[6:70], symbol)
	copy(b[70:78], leg1)
	copy(b[78:86], leg2)
	b[86] = 1 // asset class
	priceExp, qtyExp := int8(-2), int8(-8)
	b[87] = byte(priceExp)
	b[88] = byte(qtyExp)
	b[89] = 1 // market model
	binary.LittleEndian.PutUint64(b[90:98], 50)
	binary.LittleEndian.PutUint64(b[98:106], 100)
	binary.LittleEndian.PutUint64(b[106:114], 1000)
	binary.LittleEndian.PutUint64(b[114:122], 1700000000)
	b[122] = 1 // settle type
	b[123] = 2 // price bound
	binary.LittleEndian.PutUint16(b[124:126], 7)
	return b
}

func assertInstDefFields(t *testing.T, got *topOfBookInstrumentDef, wantInstID uint32, wantSymbol, wantLeg1, wantLeg2 string) {
	t.Helper()
	if got.InstrumentID != wantInstID {
		t.Errorf("instrument id: got %d want %d", got.InstrumentID, wantInstID)
	}
	// Trim here, mirroring what topofbook.go does downstream — decode time
	// itself deliberately leaves the null padding in place.
	if trimNull(got.Symbol) != wantSymbol {
		t.Errorf("symbol: got %q want %q", trimNull(got.Symbol), wantSymbol)
	}
	if trimNull(got.Leg1) != wantLeg1 || trimNull(got.Leg2) != wantLeg2 {
		t.Errorf("legs: got %q %q want %q %q", trimNull(got.Leg1), trimNull(got.Leg2), wantLeg1, wantLeg2)
	}
	if got.AssetClass != 1 || got.MarketModel != 1 {
		t.Errorf("asset class / market model: got %d %d want 1 1", got.AssetClass, got.MarketModel)
	}
	if got.PriceExponent != -2 || got.QtyExponent != -8 {
		t.Errorf("exponents: got %d %d want -2 -8", got.PriceExponent, got.QtyExponent)
	}
	if got.TickSize != 50 || got.LotSize != 100 {
		t.Errorf("tick/lot: got %d %d want 50 100", got.TickSize, got.LotSize)
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

func TestDecodeInstrumentDef_V1(t *testing.T) {
	buf := buildInstDefBodyV1(4242, "BTC-USDT", "BTC", "USDT")
	body, err := decodeTopOfBookBody(msgInstrumentDefinition, buf, 1)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := body.(*topOfBookInstrumentDef)
	if !ok {
		t.Fatalf("wrong type: %T", body)
	}
	assertInstDefFields(t, got, 4242, "BTC-USDT", "BTC", "USDT")
}

// The v3 symbol MUST exceed 16 bytes, or this test proves nothing a v1 test
// does not. This is the whole point of the widening: a Kalshi-style ticker
// like KXNFLGAME-26SEP13NYJTEN-NYJ is 27 bytes and was previously truncated
// (or would have bled into Leg1) under the old 16-byte field.
func TestDecodeInstrumentDef_V3LongSymbol(t *testing.T) {
	const long = "KXNFLGAME-26SEP13NYJTEN-NYJ"
	if len(long) <= 16 {
		t.Fatal("fixture symbol must exceed 16 bytes to be meaningful")
	}
	buf := buildInstDefBodyV3(4242, long, "BTC", "USDT")
	got, err := decodeTopOfBookBody(msgInstrumentDefinition, buf, 3)
	if err != nil {
		t.Fatal(err)
	}
	def, ok := got.(*topOfBookInstrumentDef)
	if !ok {
		t.Fatalf("wrong body type %T", got)
	}
	assertInstDefFields(t, def, 4242, long, "BTC", "USDT")
	if def.SourceID != 77 {
		t.Errorf("source id: got %d want 77", def.SourceID)
	}
}

// v1 has no Source ID on the wire. It must decode as 0 (registry Unknown)
// rather than consuming the first two bytes of Symbol.
func TestDecodeInstrumentDef_V1SourceIDIsZero(t *testing.T) {
	buf := buildInstDefBodyV1(4242, "BTC-USDT", "BTC", "USDT")
	got, err := decodeTopOfBookBody(msgInstrumentDefinition, buf, 1)
	if err != nil {
		t.Fatal(err)
	}
	def := got.(*topOfBookInstrumentDef)
	if def.SourceID != 0 {
		t.Errorf("v1 source id: got %d want 0", def.SourceID)
	}
	if trimNull(def.Symbol) != "BTC-USDT" {
		t.Errorf("v1 symbol: got %q want BTC-USDT", trimNull(def.Symbol))
	}
}

// A symbol filling all 64 bytes has no null terminator; it must not be
// truncated or run past the field into Leg1.
func TestDecodeInstrumentDef_V3SymbolFillsField(t *testing.T) {
	full := strings.Repeat("A", 64)
	buf := buildInstDefBodyV3(4242, full, "BTC", "USDT")
	got, err := decodeTopOfBookBody(msgInstrumentDefinition, buf, 3)
	if err != nil {
		t.Fatal(err)
	}
	def := got.(*topOfBookInstrumentDef)
	if def.Symbol != full {
		t.Errorf("symbol: got %d bytes want 64", len(def.Symbol))
	}
	if trimNull(def.Leg1) != "BTC" {
		t.Errorf("a full-width symbol must not bleed into Leg1: got %q", trimNull(def.Leg1))
	}
}

// The declared version and the body length must agree, and the error text must
// land in the runner's "truncated" bucket rather than "schema_version".
func TestDecodeInstrumentDef_LengthMustMatchVersion(t *testing.T) {
	v1Body := buildInstDefBodyV1(1, "BTC-USDT", "BTC", "USDT")
	v3Body := buildInstDefBodyV3(1, "KXNFLGAME-26SEP13NYJTEN-NYJ", "BTC", "USDT")

	_, err := decodeTopOfBookBody(msgInstrumentDefinition, v1Body, 3)
	if err == nil {
		t.Fatal("schema version 3 with a 76-byte (v1) body must be rejected")
	}
	// The runner's classifyParseErr buckets on substrings in the error text
	// (see topofbook-parser/runner.go), and a length mismatch must land in the
	// same "truncated" bucket as the identical fault on marketbyorder and
	// marketbyprice — not in "schema_version" alongside a genuinely
	// unsupported version (see TestDecodeInstrumentDef_UnsupportedVersion).
	if !strings.Contains(err.Error(), "truncat") {
		t.Errorf("length-mismatch error must contain \"truncat\": %q", err.Error())
	}
	if strings.Contains(err.Error(), "schema") {
		t.Errorf("length-mismatch error must not contain \"schema\" (would misclassify as schema_version): %q", err.Error())
	}

	_, err = decodeTopOfBookBody(msgInstrumentDefinition, v3Body, 1)
	if err == nil {
		t.Fatal("schema version 1 with a 126-byte (v3) body must be rejected")
	}
	if !strings.Contains(err.Error(), "truncat") {
		t.Errorf("length-mismatch error must contain \"truncat\": %q", err.Error())
	}
	if strings.Contains(err.Error(), "schema") {
		t.Errorf("length-mismatch error must not contain \"schema\": %q", err.Error())
	}
}

// An unsupported schema version must be rejected explicitly at the
// InstrumentDefinition decode layer, with a dedicated error, rather than
// silently falling through to the v1 layout. Version 2 is in this set on
// purpose: that layout was specified upstream and superseded before any
// publisher emitted it, so it is as unimplemented here as version 255.
func TestDecodeInstrumentDef_UnsupportedVersion(t *testing.T) {
	body := buildInstDefBodyV1(1, "BTC-USDT", "BTC", "USDT")

	for _, v := range []uint8{0, 2, 4, 255} {
		got, err := decodeTopOfBookBody(msgInstrumentDefinition, body, v)
		if err == nil {
			t.Fatalf("schema version %d must be rejected, got body %+v", v, got)
		}
		if !strings.Contains(err.Error(), "schema") {
			t.Errorf("unsupported-version error must contain \"schema\" (classifies as schema_version): %q", err.Error())
		}
	}
}

// The frame header (via TopOfBookParser.Parse -> validateHeader) accepts
// schema versions 1 and 3 as a set, and rejects everything else including 2.
// A version ceiling would wrongly admit 2.
func TestValidateHeader_AcceptsV1AndV3(t *testing.T) {
	p := NewTopOfBookParser()
	ts := uint64(1700000000000000000)

	for _, v := range []uint8{1, 3} {
		frame := buildFrameWithVersion(v, 1, 100, ts, buildHeartbeat(1, ts))
		if _, err := p.Parse(frame, PacketMeta{}); err != nil {
			t.Errorf("schema version %d must be accepted: %v", v, err)
		}
	}
	for _, v := range []uint8{0, 2, 4, 255} {
		frame := buildFrameWithVersion(v, 1, 100, ts, buildHeartbeat(1, ts))
		if _, err := p.Parse(frame, PacketMeta{}); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// Source ID must reach the record's Fields map, where the bot reads it.
func TestParse_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	p := NewTopOfBookParser()
	ts := uint64(1700000000000000000)
	frame := buildFrameWithVersion(3, 1, 100, ts, buildInstrumentDefMsgV3(4242, "BTC-USDT", "BTC", "USDT"))

	recs, err := p.Parse(frame, PacketMeta{})
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

// A publisher cutting over from v1 to v3 (and back) mid-stream must be
// followed without a restart. This is why the version is read per frame
// rather than latched from the first frame.
func TestParse_FollowsVersionSwitchMidStream(t *testing.T) {
	p := NewTopOfBookParser()
	ts := uint64(1700000000000000000)

	v1Msg := buildInstrumentDefMsgV1(42, "SHORT", "BTC", "USDT")
	v3Msg := buildInstrumentDefMsgV3(42, "KXNFLGAME-26SEP13NYJTEN-NYJ", "BTC", "USDT")

	v1Frame := buildFrameWithVersion(1, 1, 100, ts, v1Msg)
	v3Frame := buildFrameWithVersion(3, 1, 101, ts, v3Msg)

	for i, tc := range []struct {
		frame []byte
		want  string
	}{
		{v1Frame, "SHORT"},
		{v3Frame, "KXNFLGAME-26SEP13NYJTEN-NYJ"},
		{v1Frame, "SHORT"}, // and back again, still no restart
	} {
		records, err := p.Parse(tc.frame, PacketMeta{})
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if len(records) != 1 {
			t.Fatalf("frame %d: expected 1 record, got %d", i, len(records))
		}
		if records[0].Symbol != tc.want {
			t.Errorf("frame %d symbol: got %q want %q", i, records[0].Symbol, tc.want)
		}
	}
}

// buildFrameWithVersion is buildFrame/buildFrameWithReset but with an
// explicit schema version, needed to exercise version-specific behavior.
func buildFrameWithVersion(schemaVersion, channelID uint8, seq uint64, sendTS uint64, msgs ...[]byte) []byte {
	headerSize := 24
	bodySize := 0
	for _, m := range msgs {
		bodySize += len(m)
	}
	frameLen := headerSize + bodySize

	buf := make([]byte, frameLen)
	buf[0] = 0x5A
	buf[1] = 0x44
	buf[2] = schemaVersion
	buf[3] = channelID
	binary.LittleEndian.PutUint64(buf[4:], seq)
	binary.LittleEndian.PutUint64(buf[12:], sendTS)
	buf[20] = uint8(len(msgs))
	buf[21] = 0
	binary.LittleEndian.PutUint16(buf[22:], uint16(frameLen))

	off := headerSize
	for _, m := range msgs {
		copy(buf[off:], m)
		off += len(m)
	}
	return buf
}

// buildInstrumentDefMsgV1 constructs a full v1 InstrumentDefinition message
// (4-byte header + 76-byte body).
func buildInstrumentDefMsgV1(instID uint32, symbol, leg1, leg2 string) []byte {
	body := buildInstDefBodyV1(instID, symbol, leg1, leg2)
	msg := make([]byte, 4+len(body))
	msg[0] = msgInstrumentDefinition
	msg[1] = uint8(len(msg))
	binary.LittleEndian.PutUint16(msg[2:4], 0)
	copy(msg[4:], body)
	return msg
}

// buildInstrumentDefMsgV3 constructs a full v3 InstrumentDefinition message
// (4-byte header + 126-byte body = 130 bytes total, still within uint8
// MsgLength range).
func buildInstrumentDefMsgV3(instID uint32, symbol, leg1, leg2 string) []byte {
	body := buildInstDefBodyV3(instID, symbol, leg1, leg2)
	msg := make([]byte, 4, 4+len(body))
	msg[0] = msgInstrumentDefinition
	msg[1] = uint8(4 + len(body))
	return append(msg, body...)
}
