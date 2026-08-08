package tob

import (
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

// The two InstrumentDefinition fixtures below are written out byte by byte
// instead of built from a shared helper: they are the layouts this decoder is
// held to, so a mistake in a fixture must not be able to cancel out the same
// mistake in the decoder. They carry identical values, so anything the decoder
// hands downstream must be identical too.

const schemaTestSendTS = uint64(1_777_050_000_333_000_000)

// schemaV1DefMsg returns a hand-built 80-byte schema 1 InstrumentDefinition
// message: a 4-byte message header, then Symbol as char[16] at body offset 4
// with every later field following it directly.
func schemaV1DefMsg(symbol string) []byte {
	m := make([]byte, 80)
	m[0] = msgInstrumentDefinition
	m[1] = 80 // Message Length
	binary.LittleEndian.PutUint16(m[2:4], 0)

	binary.LittleEndian.PutUint32(m[4:8], 42) // Instrument ID
	copy(m[8:24], symbol)                     // Symbol, char[16]
	copy(m[24:32], "BTC")                     // Leg 1
	copy(m[32:40], "USDT")                    // Leg 2
	m[40] = 1                                 // Asset Class: crypto spot
	// Exponents are negative; assign through typed variables, as byte(int8(-2))
	// is a compile-time overflow on a constant operand.
	priceExp, qtyExp := int8(-2), int8(-8)
	m[41] = byte(priceExp)
	m[42] = byte(qtyExp)
	m[43] = 1 // Market Model: CLOB
	binary.LittleEndian.PutUint64(m[44:52], uint64(int64(1)))
	binary.LittleEndian.PutUint64(m[52:60], 100)
	binary.LittleEndian.PutUint64(m[60:68], 0)
	binary.LittleEndian.PutUint64(m[68:76], 1_800_000_000_000_000_000)
	m[76] = 1 // Settle Type: cash
	m[77] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(m[78:80], 9)
	return m
}

// schemaV2DefMsg returns a hand-built 128-byte schema 2 InstrumentDefinition
// message: Symbol is char[64], so every later field sits 48 bytes further on.
// The symbol is left-justified and null-padded into the wider field.
func schemaV2DefMsg(symbol string) []byte {
	m := make([]byte, 128)
	m[0] = msgInstrumentDefinition
	m[1] = 128 // Message Length
	binary.LittleEndian.PutUint16(m[2:4], 0)

	binary.LittleEndian.PutUint32(m[4:8], 42) // Instrument ID
	copy(m[8:72], symbol)                     // Symbol, char[64]
	copy(m[72:80], "BTC")                     // Leg 1
	copy(m[80:88], "USDT")                    // Leg 2
	m[88] = 1                                 // Asset Class: crypto spot
	priceExp, qtyExp := int8(-2), int8(-8)
	m[89] = byte(priceExp)
	m[90] = byte(qtyExp)
	m[91] = 1 // Market Model: CLOB
	binary.LittleEndian.PutUint64(m[92:100], uint64(int64(1)))
	binary.LittleEndian.PutUint64(m[100:108], 100)
	binary.LittleEndian.PutUint64(m[108:116], 0)
	binary.LittleEndian.PutUint64(m[116:124], 1_800_000_000_000_000_000)
	m[124] = 1 // Settle Type: cash
	m[125] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(m[126:128], 9)
	return m
}

// schemaFrame wraps messages in a frame declaring the given Schema Version.
func schemaFrame(schema uint8, msgs ...[]byte) []byte {
	frameLen := frameHeaderBytes
	for _, m := range msgs {
		frameLen += len(m)
	}
	buf := make([]byte, frameHeaderBytes, frameLen)
	buf[0] = frameMagic0
	buf[1] = frameMagic1
	buf[2] = schema
	buf[3] = 1 // Channel ID
	binary.LittleEndian.PutUint64(buf[4:12], 100)
	binary.LittleEndian.PutUint64(buf[12:20], schemaTestSendTS)
	buf[20] = uint8(len(msgs))
	buf[21] = 0
	binary.LittleEndian.PutUint16(buf[22:24], uint16(frameLen))
	for _, m := range msgs {
		buf = append(buf, m...)
	}
	return buf
}

// schemaHeartbeatMsg returns a 16-byte Heartbeat, whose layout schema 2 left
// alone. It carries frames that must turn on the Schema Version and nothing else.
func schemaHeartbeatMsg() []byte {
	m := make([]byte, 16)
	m[0] = msgHeartbeat
	m[1] = 16
	m[4] = 1 // Channel ID
	binary.LittleEndian.PutUint64(m[8:16], schemaTestSendTS)
	return m
}

func decodeDefBody(t *testing.T, msg []byte, schema uint8) (*topOfBookInstrumentDef, error) {
	t.Helper()
	body, err := decodeTopOfBookBody(msgInstrumentDefinition, msg[4:], schema)
	if err != nil {
		return nil, err
	}
	def, ok := body.(*topOfBookInstrumentDef)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookInstrumentDef", body)
	}
	return def, nil
}

func TestDecodeInstrumentDef_BothSchemasDecodeIdentically(t *testing.T) {
	v1, err := decodeDefBody(t, schemaV1DefMsg("BTC-USDT"), schemaV1)
	if err != nil {
		t.Fatalf("schema 1: %v", err)
	}
	v2, err := decodeDefBody(t, schemaV2DefMsg("BTC-USDT"), schemaV2)
	if err != nil {
		t.Fatalf("schema 2: %v", err)
	}

	// Symbol still carries its null padding here; the parser trims it, so the
	// two are compared after the trim that every caller sees.
	if trimNull(v1.Symbol) != trimNull(v2.Symbol) {
		t.Errorf("symbol: v1 %q, v2 %q", trimNull(v1.Symbol), trimNull(v2.Symbol))
	}
	v1.Symbol, v2.Symbol = "", ""
	if !reflect.DeepEqual(v1, v2) {
		t.Fatalf("schemas disagree:\n v1 %+v\n v2 %+v", v1, v2)
	}

	// Asserted on the schema 2 decode, and true of schema 1 by the equality above.
	if v2.InstrumentID != 42 || trimNull(v2.Leg1) != "BTC" || trimNull(v2.Leg2) != "USDT" {
		t.Errorf("identity: %+v", v2)
	}
	if v2.AssetClass != 1 || v2.PriceExponent != -2 || v2.QtyExponent != -8 || v2.MarketModel != 1 {
		t.Errorf("scaling: %+v", v2)
	}
	if v2.TickSize != 1 || v2.LotSize != 100 || v2.ContractValue != 0 || v2.Expiry != 1_800_000_000_000_000_000 {
		t.Errorf("sizes: %+v", v2)
	}
	if v2.SettleType != 1 || v2.PriceBound != 2 || v2.ManifestSeq != 9 {
		t.Errorf("tail: %+v", v2)
	}
}

// A symbol that fills the schema 1 field leaves no null terminator. The field
// width, not a terminator, bounds Symbol; reading on would swallow Leg 1.
func TestDecodeInstrumentDef_SymbolFillsSchema1FieldWithoutTerminator(t *testing.T) {
	const symbol = "ABCDEFGHIJKLMNOP" // exactly 16 bytes

	v1, err := decodeDefBody(t, schemaV1DefMsg(symbol), schemaV1)
	if err != nil {
		t.Fatalf("schema 1: %v", err)
	}
	if trimNull(v1.Symbol) != symbol {
		t.Errorf("schema 1 symbol: got %q want %q", trimNull(v1.Symbol), symbol)
	}
	if trimNull(v1.Leg1) != "BTC" {
		t.Errorf("schema 1 read past Symbol into Leg 1: got %q", trimNull(v1.Leg1))
	}

	// The same symbol left-justified in the 64-byte field must decode the same.
	v2, err := decodeDefBody(t, schemaV2DefMsg(symbol), schemaV2)
	if err != nil {
		t.Fatalf("schema 2: %v", err)
	}
	if trimNull(v2.Symbol) != symbol {
		t.Errorf("schema 2 symbol: got %q want %q", trimNull(v2.Symbol), symbol)
	}
}

// Schema 2 exists for the symbols char[16] could not hold, so the decoder must
// read the whole of the wider field — including when it is filled with no null
// terminator.
func TestDecodeInstrumentDef_Schema2CarriesLongSymbol(t *testing.T) {
	for _, symbol := range []string{
		"KXPRESIDENTIALWINNER-2028-DEMOCRATIC", // longer than char[16]
		strings.Repeat("S", 64),                // fills char[64], no terminator
	} {
		def, err := decodeDefBody(t, schemaV2DefMsg(symbol), schemaV2)
		if err != nil {
			t.Fatalf("%q: %v", symbol, err)
		}
		if trimNull(def.Symbol) != symbol {
			t.Errorf("symbol: got %q want %q", trimNull(def.Symbol), symbol)
		}
		if trimNull(def.Leg1) != "BTC" {
			t.Errorf("%q: read past Symbol into Leg 1: got %q", symbol, trimNull(def.Leg1))
		}
	}
}

// Each generation's body is refused under the other version's declaration. The
// refusal is on the declared length: the schema 1 body under schema 2 is short,
// but the schema 2 body under schema 1 is longer than needed and would decode
// clean for a decoder that only read the fields it wanted off the front.
func TestDecodeInstrumentDef_RejectsBodySizedForOtherSchema(t *testing.T) {
	if _, err := decodeDefBody(t, schemaV1DefMsg("BTC-USDT"), schemaV2); err == nil {
		t.Error("76-byte body under schema 2: accepted, want refused")
	}
	if _, err := decodeDefBody(t, schemaV2DefMsg("BTC-USDT"), schemaV1); err == nil {
		t.Error("124-byte body under schema 1: accepted, want refused")
	}
}

// Both generations are on the wire at once during a staged publisher rollout,
// so the parser accepts either and nothing else. Checked on a frame carrying an
// InstrumentDefinition and on one carrying only a Heartbeat, because bodies are
// decoded before the header is validated.
func TestTopOfBookParser_SchemaVersions(t *testing.T) {
	for _, tc := range []struct {
		schema uint8
		accept bool
	}{
		{0, false}, {schemaV1, true}, {schemaV2, true}, {3, false}, {255, false},
	} {
		def := schemaV1DefMsg("BTC-USDT")
		if tc.schema == schemaV2 {
			def = schemaV2DefMsg("BTC-USDT")
		}
		for name, frame := range map[string][]byte{
			"instrument_definition": schemaFrame(tc.schema, def),
			"heartbeat":             schemaFrame(tc.schema, schemaHeartbeatMsg()),
		} {
			_, err := NewTopOfBookParser().Parse(frame, PacketMeta{})
			switch {
			case tc.accept && err != nil:
				t.Errorf("schema %d %s: refused: %v", tc.schema, name, err)
			case !tc.accept && err == nil:
				t.Errorf("schema %d %s: accepted, want refused", tc.schema, name)
			case !tc.accept && !strings.Contains(err.Error(), "schema"):
				// classifyParseErr buckets on this word; without it the refusal
				// is counted as "other".
				t.Errorf("schema %d %s: %q does not name the schema version", tc.schema, name, err)
			}
		}
	}
}

// Nothing downstream of the parser can tell which generation produced a record.
func TestTopOfBookParser_InstrumentDefRecordSameAcrossSchemas(t *testing.T) {
	v1Recs, err := NewTopOfBookParser().Parse(schemaFrame(schemaV1, schemaV1DefMsg("BTC-USDT")), PacketMeta{})
	if err != nil {
		t.Fatalf("schema 1 frame: %v", err)
	}
	v2Recs, err := NewTopOfBookParser().Parse(schemaFrame(schemaV2, schemaV2DefMsg("BTC-USDT")), PacketMeta{})
	if err != nil {
		t.Fatalf("schema 2 frame: %v", err)
	}
	if !reflect.DeepEqual(v1Recs, v2Recs) {
		t.Fatalf("records differ:\n v1 %+v\n v2 %+v", v1Recs, v2Recs)
	}
	if len(v2Recs) != 1 || v2Recs[0].Type != "instrument_definition" || v2Recs[0].Symbol != "BTC-USDT" {
		t.Fatalf("record: %+v", v2Recs)
	}
}
