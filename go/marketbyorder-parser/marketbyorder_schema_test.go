package main

import (
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// The two InstrumentDefinition fixtures below are written out byte by byte
// instead of built from a shared helper: they are the layouts this parser is
// held to, so a mistake in a fixture must not be able to cancel out the same
// mistake in the decoder. They carry identical values, so anything the decoder
// hands downstream must be identical too.

var schemaTestExpiry = time.Unix(1800000000, 0)

// schemaV1DefBody returns a hand-built 76-byte schema 1 InstrumentDefinition
// body: Symbol is char[16] at 4, and every later field follows it directly.
func schemaV1DefBody(symbol string) []byte {
	b := make([]byte, 76)
	binary.LittleEndian.PutUint32(b[0:4], 12345) // Instrument ID
	copy(b[4:20], symbol)                        // Symbol, char[16]
	copy(b[20:28], "BTC")                        // Leg 1
	copy(b[28:36], "USDT")                       // Leg 2
	b[36] = 1                                    // Asset Class: crypto spot
	// Exponents are negative; assign through typed variables, as byte(int8(-2))
	// is a compile-time overflow on a constant operand.
	priceExp, qtyExp := int8(-2), int8(-8)
	b[37] = byte(priceExp)
	b[38] = byte(qtyExp)
	b[39] = 1 // Market Model: CLOB
	binary.LittleEndian.PutUint64(b[40:48], uint64(int64(1)))
	binary.LittleEndian.PutUint64(b[48:56], 100)
	binary.LittleEndian.PutUint64(b[56:64], 0)
	binary.LittleEndian.PutUint64(b[64:72], uint64(schemaTestExpiry.UnixNano()))
	b[72] = 1 // Settle Type: cash
	b[73] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(b[74:76], 7)
	return b
}

// schemaV2DefBody returns a hand-built 124-byte schema 2 InstrumentDefinition
// body: Symbol is char[64] at 4, so every later field sits 48 bytes further on.
// The symbol is left-justified and null-padded into the wider field.
func schemaV2DefBody(symbol string) []byte {
	b := make([]byte, 124)
	binary.LittleEndian.PutUint32(b[0:4], 12345) // Instrument ID
	copy(b[4:68], symbol)                        // Symbol, char[64]
	copy(b[68:76], "BTC")                        // Leg 1
	copy(b[76:84], "USDT")                       // Leg 2
	b[84] = 1                                    // Asset Class: crypto spot
	priceExp, qtyExp := int8(-2), int8(-8)
	b[85] = byte(priceExp)
	b[86] = byte(qtyExp)
	b[87] = 1 // Market Model: CLOB
	binary.LittleEndian.PutUint64(b[88:96], uint64(int64(1)))
	binary.LittleEndian.PutUint64(b[96:104], 100)
	binary.LittleEndian.PutUint64(b[104:112], 0)
	binary.LittleEndian.PutUint64(b[112:120], uint64(schemaTestExpiry.UnixNano()))
	b[120] = 1 // Settle Type: cash
	b[121] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(b[122:124], 7)
	return b
}

func TestParseInstrumentDefinition_BothSchemasDecodeIdentically(t *testing.T) {
	v1, err := ParseInstrumentDefinition(schemaV1DefBody("BTC-USDT"), schemaV1)
	if err != nil {
		t.Fatalf("schema 1: %v", err)
	}
	v2, err := ParseInstrumentDefinition(schemaV2DefBody("BTC-USDT"), schemaV2)
	if err != nil {
		t.Fatalf("schema 2: %v", err)
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("schemas disagree:\n v1 %+v\n v2 %+v", v1, v2)
	}

	// Asserted on the schema 2 decode, and true of schema 1 by the equality above.
	if v2.InstrumentID != 12345 || v2.Symbol != "BTC-USDT" || v2.Leg1 != "BTC" || v2.Leg2 != "USDT" {
		t.Errorf("identity: %+v", v2)
	}
	if v2.AssetClass != 1 || v2.PriceExponent != -2 || v2.QtyExponent != -8 || v2.MarketModel != 1 {
		t.Errorf("scaling: %+v", v2)
	}
	if v2.TickSizeRaw != 1 || v2.LotSizeRaw != 100 || v2.ContractValue != 0 {
		t.Errorf("sizes: %+v", v2)
	}
	if !v2.Expiry.Equal(schemaTestExpiry) || v2.SettleType != 1 || v2.PriceBound != 2 || v2.ManifestSeq != 7 {
		t.Errorf("tail: %+v", v2)
	}
}

// A symbol that fills the schema 1 field leaves no null terminator. The field
// width, not a terminator, bounds Symbol; reading on would swallow Leg 1.
func TestParseInstrumentDefinition_SymbolFillsSchema1FieldWithoutTerminator(t *testing.T) {
	const symbol = "ABCDEFGHIJKLMNOP" // exactly 16 bytes

	v1, err := ParseInstrumentDefinition(schemaV1DefBody(symbol), schemaV1)
	if err != nil {
		t.Fatalf("schema 1: %v", err)
	}
	if v1.Symbol != symbol {
		t.Errorf("schema 1 symbol: got %q want %q", v1.Symbol, symbol)
	}
	if v1.Leg1 != "BTC" {
		t.Errorf("schema 1 read past Symbol into Leg 1: got %q", v1.Leg1)
	}

	// The same symbol left-justified in the 64-byte field must decode the same.
	v2, err := ParseInstrumentDefinition(schemaV2DefBody(symbol), schemaV2)
	if err != nil {
		t.Fatalf("schema 2: %v", err)
	}
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("schemas disagree:\n v1 %+v\n v2 %+v", v1, v2)
	}
}

// Schema 2 exists for the symbols char[16] could not hold, so the decoder must
// read the whole of the wider field — including when it is filled with no null
// terminator.
func TestParseInstrumentDefinition_Schema2CarriesLongSymbol(t *testing.T) {
	for _, symbol := range []string{
		"KXPRESIDENTIALWINNER-2028-DEMOCRATIC", // longer than char[16]
		strings.Repeat("S", 64),                // fills char[64], no terminator
	} {
		b, err := ParseInstrumentDefinition(schemaV2DefBody(symbol), schemaV2)
		if err != nil {
			t.Fatalf("%q: %v", symbol, err)
		}
		if b.Symbol != symbol {
			t.Errorf("symbol: got %q want %q", b.Symbol, symbol)
		}
		if b.Leg1 != "BTC" {
			t.Errorf("%q: read past Symbol into Leg 1: got %q", symbol, b.Leg1)
		}
	}
}

// Each generation's body is refused under the other version's declaration. The
// refusal is on the declared length: the schema 1 body under schema 2 is short,
// but the schema 2 body under schema 1 is longer than needed and would decode
// clean for a parser that only checked it had enough bytes.
func TestParseInstrumentDefinition_RejectsBodySizedForOtherSchema(t *testing.T) {
	if _, err := ParseInstrumentDefinition(schemaV1DefBody("BTC-USDT"), schemaV2); !errors.Is(err, errTruncated) {
		t.Errorf("76-byte body under schema 2: expected errTruncated, got %v", err)
	}
	if _, err := ParseInstrumentDefinition(schemaV2DefBody("BTC-USDT"), schemaV1); !errors.Is(err, errTruncated) {
		t.Errorf("124-byte body under schema 1: expected errTruncated, got %v", err)
	}
	if _, err := ParseInstrumentDefinition(schemaV1DefBody("BTC-USDT"), 3); !errors.Is(err, errSchemaVersion) {
		t.Errorf("schema 3: expected errSchemaVersion, got %v", err)
	}
}

// Both generations are on the wire at once during a staged publisher rollout,
// so the parser accepts either and nothing else.
func TestParseFrameHeader_SchemaVersions(t *testing.T) {
	for _, tc := range []struct {
		schema uint8
		accept bool
	}{
		{0, false}, {1, true}, {2, true}, {3, false}, {255, false},
	} {
		buf := buildFrameHeader(mboMagic, tc.schema, 0, 1, time.Unix(1700000400, 0), 1, 0, frameHeaderSize)
		_, err := ParseFrameHeader(buf)
		if tc.accept && errors.Is(err, errSchemaVersion) {
			t.Errorf("schema %d: refused, want accepted", tc.schema)
		}
		if !tc.accept && !errors.Is(err, errSchemaVersion) {
			t.Errorf("schema %d: got %v, want errSchemaVersion", tc.schema, err)
		}
	}
}

// Nothing downstream of the parser can tell which generation produced a record.
func TestParseFrame_InstrumentDefinitionRecordSameAcrossSchemas(t *testing.T) {
	p := &marketByOrderParser{}
	ts := time.Unix(1700000401, 0)

	frameFor := func(schema uint8, body []byte) []byte {
		frameLen := frameHeaderSize + messageHeaderSize + len(body)
		frame := buildFrameHeader(mboMagic, schema, 3, 77, ts, 1, 0, uint16(frameLen))
		mh := make([]byte, messageHeaderSize)
		mh[0] = msgTypeInstrumentDefinition
		mh[1] = uint8(messageHeaderSize + len(body))
		frame = append(frame, mh...)
		return append(frame, body...)
	}

	v1Recs, err := p.ParseFrame("refdata", frameFor(schemaV1, schemaV1DefBody("BTC-USDT")))
	if err != nil {
		t.Fatalf("schema 1 frame: %v", err)
	}
	v2Recs, err := p.ParseFrame("refdata", frameFor(schemaV2, schemaV2DefBody("BTC-USDT")))
	if err != nil {
		t.Fatalf("schema 2 frame: %v", err)
	}
	if !reflect.DeepEqual(v1Recs, v2Recs) {
		t.Fatalf("records differ:\n v1 %+v\n v2 %+v", v1Recs, v2Recs)
	}
	if len(v2Recs) != 1 || v2Recs[0].Type != "instrument_definition" || v2Recs[0].Fields["symbol"] != "BTC-USDT" {
		t.Fatalf("record: %+v", v2Recs)
	}
}
