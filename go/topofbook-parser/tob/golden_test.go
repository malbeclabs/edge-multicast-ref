package tob

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
)

// The golden vectors are the cross-language contract. These five were
// transcribed by hand from the field tables in edge-feed-spec rather than
// captured from an encoder; the Rust codec crates assert the same field values
// against them, and this side reads them with this parser's own decoder.
//
// That is what makes them worth having. Two implementations tested only against
// themselves agree with themselves — including when both are wrong in the same
// way. A layout change made on one side alone fails here.
//
// One limit, because a test that overstates its reach is worse than a smaller
// one: two fields a vector gives the same value cannot be told apart by any
// assertion over decoded values. The InstrumentDefinition vectors carry
// asset_class and market_model both as 1, and contract_value, expiry_ns,
// settle_type and price_bound all as 0, so a decoder exchanging either pair
// passes. Separating them means changing a vector, which is a wire change.
//
// The expected values are the `fields` block of testdata/golden/manifest.json,
// which is where an implementation in any language reads them from, and the
// names below are the manifest's names so a failure points straight at the row
// that disagrees. That is a binding and not a claim:
// TestGoldenManifestStatesWhatTheseCasesAssert compares every row below with
// the manifest's own, in both directions. Where the manifest's name and this
// parser's field spelling differ, the difference is the suffix the wire structs
// drop:
//
//	source_timestamp_ns -> SourceTimestamp
//	timestamp_ns        -> Timestamp
//	expiry_ns           -> Expiry
//
// Every other field is the manifest's name in Go's capitalisation.
//
// The files carry the application message including its 4-byte header;
// decodeTopOfBookBody takes the body, so each case slices past it.
const goldenDir = "../../../testdata/golden"

func goldenBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, name))
	if err != nil {
		// A missing vector fails the test rather than skipping it. A suite that
		// asserts nothing because it found nothing reports the same "ok" as one
		// that checked every byte, which is the one outcome worse than no suite.
		t.Fatalf("read %s: %v", name, err)
	}
	return b
}

// header asserts the application message header and returns the body. Its width
// is messageHeaderSize, the same constant decodeTopOfBookDatagram subtracts from
// each message's declared length.
//
// wantFlags is the on-wire Flags field, recorded as flags_on_wire in the
// manifest. It is 0 for every message of this feed: bit 0 marks a message
// travelling the `snapshot` port, and top-of-book runs `mktdata` and `refdata`
// only.
func header(t *testing.T, buf []byte, wantType uint8, wantSize int, wantFlags uint16) []byte {
	t.Helper()
	if len(buf) != wantSize {
		t.Fatalf("golden is %d bytes, want %d", len(buf), wantSize)
	}
	if buf[0] != wantType {
		t.Fatalf("type id 0x%02x, want 0x%02x", buf[0], wantType)
	}
	if int(buf[1]) != wantSize {
		t.Fatalf("declared length %d, want %d", buf[1], wantSize)
	}
	if got := binary.LittleEndian.Uint16(buf[2:4]); got != wantFlags {
		t.Fatalf("flags 0x%04x, want 0x%04x", got, wantFlags)
	}
	return buf[messageHeaderSize:]
}

// goldenField is one row of a vector's `fields` block: the manifest's name for
// it, the value this parser decoded, and the value the manifest states.
type goldenField struct {
	name string
	got  int64
	want int64
}

func checkFields(t *testing.T, fields []goldenField) {
	t.Helper()
	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("%s = %d, want %d", f.name, f.got, f.want)
		}
	}
}

// goldenText is a goldenField for the fixed-width ASCII fields. This decoder
// returns them null-padded and the rest of the parser trims with trimNull, so
// the comparison trims too.
type goldenText struct {
	name string
	got  string
	want string
}

func checkText(t *testing.T, fields []goldenText) {
	t.Helper()
	for _, f := range fields {
		if got := trimNull(f.got); got != f.want {
			t.Errorf("%s = %q, want %q", f.name, got, f.want)
		}
	}
}

// decodeGolden decodes one vector's body and fails if the decoder rejects it or
// returns another message's body type.
func decodeGolden[T any](t *testing.T, body []byte, msgType uint8, schemaVersion uint8) *T {
	t.Helper()
	b, err := decodeTopOfBookBody(msgType, body, schemaVersion)
	if err != nil {
		t.Fatalf("decode type 0x%02x at schema version %d: %v", msgType, schemaVersion, err)
	}
	v, ok := b.(*T)
	if !ok {
		t.Fatalf("decoded %T, want *%T", b, *new(T))
	}
	return v
}

// goldenVector is one vector's whole expectation: the header values the
// manifest records as `type_id`, `size`, `flags_on_wire` and `schema_version`,
// and a decode of the body that yields the `fields` rows.
//
// The expectation is a table rather than a statement inside a test body because
// two tests need it. The case below asserts the rows against the bytes; the
// manifest test asserts the same rows against manifest.json. Stated once, they
// cannot disagree with each other, which is the only way a comment naming the
// manifest as the source of these values can be true of the code.
type goldenVector struct {
	file   string
	typeID uint8
	size   int
	flags  uint16
	schema uint8
	rows   func(t *testing.T, body []byte) ([]goldenField, []goldenText)
}

// goldenVectors is every vector this parser reads. The other ten the manifest
// carries belong to market-by-price, whose depth and lowered vectors this
// decoder has no message types for; go/marketbyprice-parser reads those.
func goldenVectors() []goldenVector {
	return []goldenVector{
		{
			file: "quote-v3.bin", typeID: msgQuote, size: 60, flags: 0, schema: schemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				q := decodeGolden[topOfBookQuote](t, body, msgQuote, schemaVersionV3)
				return []goldenField{
					{"instrument_id", int64(q.InstrumentID), 1},
					{"source_id", int64(q.SourceID), 2},
					{"update_flags", int64(q.UpdateFlags), 3},
					{"source_timestamp_ns", int64(q.SourceTimestamp), 1700000000000000000},
					{"bid_price", q.BidPrice, 9999500},
					{"bid_qty", int64(q.BidQty), 12500},
					{"ask_price", q.AskPrice, 10000500},
					{"ask_qty", int64(q.AskQty), 7250},
					{"bid_source_count", int64(q.BidSourceCount), 3},
					{"ask_source_count", int64(q.AskSourceCount), 4},
				}, nil
			},
		},
		{
			file: "trade-v3.bin", typeID: msgTrade, size: 52, flags: 0, schema: schemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				tr := decodeGolden[topOfBookTrade](t, body, msgTrade, schemaVersionV3)
				return []goldenField{
					{"instrument_id", int64(tr.InstrumentID), 1},
					{"source_id", int64(tr.SourceID), 2},
					{"aggressor_side", int64(tr.AggressorSide), 1},
					{"trade_flags", int64(tr.TradeFlags), 2},
					{"source_timestamp_ns", int64(tr.SourceTimestamp), 1700000000000000001},
					{"trade_price", tr.TradePrice, 10000000},
					{"trade_qty", int64(tr.TradeQty), 500},
					{"trade_id", int64(tr.TradeID), 987654321},
					{"cumulative_volume", int64(tr.CumulativeVolume), 1000000},
				}, nil
			},
		},
		{
			file: "instrument-definition-v3.bin", typeID: msgInstrumentDefinition, size: 130, flags: 0, schema: schemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d := decodeGolden[topOfBookInstrumentDef](t, body, msgInstrumentDefinition, schemaVersionV3)
				return instDefFields(d, 2), instDefText(d)
			},
		},
		{
			// The schema 1 vector is decode-only: nothing here emits that
			// layout, and InstrumentDefinition is the one message in this
			// family whose layout changed between schema generations, so it is
			// the one most likely to drift.
			file: "instrument-definition-v1.bin", typeID: msgInstrumentDefinition, size: 80, flags: 0, schema: schemaVersionV1,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d := decodeGolden[topOfBookInstrumentDef](t, body, msgInstrumentDefinition, schemaVersionV1)
				return instDefFields(d, 0), instDefText(d)
			},
		},
		{
			file: "manifest-summary-v3.bin", typeID: msgManifestSummary, size: 24, flags: 0, schema: schemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				m := decodeGolden[topOfBookManifestSummary](t, body, msgManifestSummary, schemaVersionV3)
				return []goldenField{
					{"channel_id", int64(m.ChannelID), 7},
					{"valid", int64(m.Valid), 1},
					{"manifest_seq", int64(m.ManifestSeq), 9},
					{"instrument_count", int64(m.InstrumentCount), 1234},
					{"timestamp_ns", int64(m.Timestamp), 1700000000000000002},
				}, nil
			},
		},
	}
}

// instDefFields is the InstrumentDefinition expectation both schema generations
// share. Only source_id differs: schema 1 has no field for it and the manifest
// states it decodes as 0, which is the Source ID Registry's Unknown value.
func instDefFields(d *topOfBookInstrumentDef, wantSourceID int64) []goldenField {
	return []goldenField{
		{"instrument_id", int64(d.InstrumentID), 1},
		{"source_id", int64(d.SourceID), wantSourceID},
		{"asset_class", int64(d.AssetClass), 1},
		{"price_exponent", int64(d.PriceExponent), -2},
		{"qty_exponent", int64(d.QtyExponent), -8},
		{"market_model", int64(d.MarketModel), 1},
		{"tick_size", d.TickSize, 1},
		{"lot_size", int64(d.LotSize), 1000},
		{"contract_value", int64(d.ContractValue), 0},
		{"expiry_ns", int64(d.Expiry), 0},
		{"settle_type", int64(d.SettleType), 0},
		{"price_bound", int64(d.PriceBound), 0},
		{"manifest_seq", int64(d.ManifestSeq), 9},
	}
}

func instDefText(d *topOfBookInstrumentDef) []goldenText {
	return []goldenText{
		{"symbol", d.Symbol, "BTC-USDT"},
		{"leg1", d.Leg1, "BTC"},
		{"leg2", d.Leg2, "USDT"},
	}
}

// goldenVectorNamed returns the one table entry for a file, so each case below
// keeps its own name in the test output instead of becoming a subtest.
func goldenVectorNamed(t *testing.T, file string) goldenVector {
	t.Helper()
	for _, v := range goldenVectors() {
		if v.file == file {
			return v
		}
	}
	t.Fatalf("no golden vector named %s in this suite's table", file)
	return goldenVector{}
}

// decodeRows asserts the vector's header and returns the rows its body decodes
// to, paired with the values the case expects.
func (v goldenVector) decodeRows(t *testing.T) ([]goldenField, []goldenText) {
	t.Helper()
	return v.rows(t, header(t, goldenBytes(t, v.file), v.typeID, v.size, v.flags))
}

// runGoldenVector is one case: decode the vector and compare every row.
func runGoldenVector(t *testing.T, file string) {
	t.Helper()
	v := goldenVectorNamed(t, file)
	fields, text := v.decodeRows(t)
	checkFields(t, fields)
	checkText(t, text)
}

func TestGoldenQuote(t *testing.T) { runGoldenVector(t, "quote-v3.bin") }

func TestGoldenTrade(t *testing.T) { runGoldenVector(t, "trade-v3.bin") }

func TestGoldenInstrumentDefinitionV3(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v3.bin")
}

func TestGoldenInstrumentDefinitionV1(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v1.bin")
}

func TestGoldenManifestSummary(t *testing.T) { runGoldenVector(t, "manifest-summary-v3.bin") }

// ---------------------------------------------------------------------------
// The manifest, made load-bearing
// ---------------------------------------------------------------------------

// goldenManifest is testdata/golden/manifest.json reduced to the keys this
// suite holds itself to. The rest — `message`, `feed`, `note`, `lowered_from`,
// `spec_revision` — is prose about a vector rather than a value to assert.
type goldenManifest struct {
	Vectors []goldenManifestVector `json:"vectors"`
}

type goldenManifestVector struct {
	File          string `json:"file"`
	TypeID        string `json:"type_id"`
	Size          int    `json:"size"`
	SchemaVersion uint8  `json:"schema_version"`
	FlagsOnWire   uint16 `json:"flags_on_wire"`
	// Raw, so that a nanosecond timestamp is read as the integer it is. Decoded
	// into interface{} it would become a float64 and 1700000000000000003 would
	// compare equal to 1700000000000000002.
	Fields map[string]json.RawMessage `json:"fields"`
}

func readGoldenManifest(t *testing.T) map[string]goldenManifestVector {
	t.Helper()
	var m goldenManifest
	if err := json.Unmarshal(goldenBytes(t, "manifest.json"), &m); err != nil {
		t.Fatalf("parse manifest.json: %v", err)
	}
	if len(m.Vectors) == 0 {
		t.Fatalf("manifest.json lists no vectors")
	}
	byFile := make(map[string]goldenManifestVector, len(m.Vectors))
	for _, v := range m.Vectors {
		if _, dup := byFile[v.File]; dup {
			t.Fatalf("manifest.json lists %s twice", v.File)
		}
		byFile[v.File] = v
	}
	return byFile
}

// TestGoldenManifestStatesWhatTheseCasesAssert is what makes the manifest the
// source of truth this file's header calls it.
//
// Every case above states its expectation as a literal. Without this test, the
// sentence "the expected values are the `fields` block of manifest.json" is a
// claim the code does not hold: the manifest could drift from the bytes and
// from these literals in either direction with every suite still green.
//
// So each row is compared with the manifest's own, by the manifest's name for
// it, in both directions — a value the manifest states differently fails, and
// so does a field named on one side and not the other. The reverse drift is
// already covered: a literal edited here disagrees with the decoded vector and
// fails its own case. Between the two, the only arrangement that passes is one
// where the manifest, the bytes and these assertions all say the same thing.
func TestGoldenManifestStatesWhatTheseCasesAssert(t *testing.T) {
	stated := readGoldenManifest(t)
	for _, v := range goldenVectors() {
		t.Run(v.file, func(t *testing.T) {
			m, ok := stated[v.file]
			if !ok {
				t.Fatalf("manifest.json carries no entry for %s", v.file)
			}
			if m.Size != v.size {
				t.Errorf("size = %d, want %d", m.Size, v.size)
			}
			if want := fmt.Sprintf("0x%02x", v.typeID); m.TypeID != want {
				t.Errorf("type_id = %q, want %q", m.TypeID, want)
			}
			if m.FlagsOnWire != v.flags {
				t.Errorf("flags_on_wire = %d, want %d", m.FlagsOnWire, v.flags)
			}
			if m.SchemaVersion != v.schema {
				t.Errorf("schema_version = %d, want %d", m.SchemaVersion, v.schema)
			}
			fields, text := v.decodeRows(t)
			checkManifestFields(t, m, fields, text)
		})
	}
}

// checkManifestFields compares one vector's `fields` block with the rows this
// suite asserts, both ways round.
func checkManifestFields(t *testing.T, m goldenManifestVector, fields []goldenField, text []goldenText) {
	t.Helper()
	asserted := make(map[string]bool, len(fields)+len(text))
	for _, f := range fields {
		asserted[f.name] = true
		raw, ok := m.Fields[f.name]
		if !ok {
			t.Errorf("fields has no %s, which this suite asserts as %d", f.name, f.want)
			continue
		}
		got, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			t.Errorf("fields.%s = %s, which is not an integer: %v", f.name, raw, err)
			continue
		}
		if got != f.want {
			t.Errorf("fields.%s = %d, but this suite asserts %d", f.name, got, f.want)
		}
	}
	for _, f := range text {
		asserted[f.name] = true
		raw, ok := m.Fields[f.name]
		if !ok {
			t.Errorf("fields has no %s, which this suite asserts as %q", f.name, f.want)
			continue
		}
		var got string
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("fields.%s = %s, which is not a string: %v", f.name, raw, err)
			continue
		}
		if got != f.want {
			t.Errorf("fields.%s = %q, but this suite asserts %q", f.name, got, f.want)
		}
	}
	// The other direction. A field added to the manifest and asserted nowhere
	// is a value nothing holds the decoder to, which is exactly what this
	// test refuses to let the manifest carry.
	var unasserted []string
	for name := range m.Fields {
		if !asserted[name] {
			unasserted = append(unasserted, name)
		}
	}
	sort.Strings(unasserted)
	for _, name := range unasserted {
		t.Errorf("fields.%s = %s, which this suite asserts nowhere", name, m.Fields[name])
	}
}
