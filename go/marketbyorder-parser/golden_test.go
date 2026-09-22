package main

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

// The golden vectors are the cross-language contract. Four of the five below
// were transcribed by hand from the field tables in edge-feed-spec rather than
// captured from an encoder, and the fifth, snapshot-end-v3.bin, is written by
// the Rust market-by-price codec. The Rust crates assert the same field values
// against all of them, and this side reads them with this parser's own decoder.
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
// Five of the vectors bind this parser. Trade, InstrumentDefinition and
// ManifestSummary are byte-identical across the family, so this decoder has to
// read the same bytes the top-of-book one does, and SnapshotEnd is too: the
// same 16-byte body under type id 0x22 here and in market-by-price. The
// manifest tags it `"feed": "market-by-price"` because that is the feed whose
// encoder wrote the bytes, not because this parser may read them differently.
//
// The remaining depth vectors are outside that set, each for its own reason.
// SnapshotBegin carries Total Orders and a 32-byte body here against
// market-by-price's Total Levels, Depth Bound and 36. SnapshotLevel is 0x42,
// where this feed sends SnapshotOrder at 0x21. LevelUpdate and BookClear have
// no market-by-order message at all.
//
// The expected values are the `fields` block of testdata/golden/manifest.json,
// which is where an implementation in any language reads them from, and the
// names below are the manifest's names so a failure points straight at the row
// that disagrees. That is a binding and not a claim:
// TestGoldenManifestStatesWhatTheseCasesAssert compares every row below with
// the manifest's own, in both directions. Where the manifest's name and this
// parser's field spelling differ, the difference is the `Raw` suffix this parser
// puts on a value still in wire units, or the `_ns` the wire structs drop
// because they hold a time.Time:
//
//	trade_price         -> TradePriceRaw        tick_size -> TickSizeRaw
//	trade_qty           -> TradeQtyRaw          lot_size  -> LotSizeRaw
//	cumulative_volume   -> CumulativeVolumeRaw
//	source_timestamp_ns -> SourceTimestamp      expiry_ns -> Expiry
//	timestamp_ns        -> Timestamp
//
// Every other field is the manifest's name in Go's capitalisation, and every
// time.Time is compared as the nanoseconds the manifest states.
//
// The files carry the application message including its 4-byte header; the
// Parse* functions take the body, so each case slices past it.
const goldenDir = "../../testdata/golden"

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

// header asserts the 4-byte application message header and returns the body.
//
// wantFlags is the on-wire Flags field, recorded as flags_on_wire in the
// manifest. Bit 0 marks a message travelling the `snapshot` port, so it is 0 on
// the Trade, which arrives on `mktdata`, and on the InstrumentDefinition and
// ManifestSummary, which arrive on `refdata`, and set on the SnapshotEnd. The
// value is asserted rather than assumed because it is the one part of these
// bytes the encoder does not decide — the builder stamps it at push, from the
// port.
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

// goldenText is a goldenField for the fixed-width ASCII fields, which
// fixedString has already trimmed of their null padding by the time they get
// here.
type goldenText struct {
	name string
	got  string
	want string
}

func checkText(t *testing.T, fields []goldenText) {
	t.Helper()
	for _, f := range fields {
		if f.got != f.want {
			t.Errorf("%s = %q, want %q", f.name, f.got, f.want)
		}
	}
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

// goldenVectors is every vector this parser reads: the four the family shares,
// and snapshot-end-v3.bin, whose bytes this feed and market-by-price both send.
// The ten it does not are market-by-price's, whose depth and lowered messages
// this decoder has different layouts for; go/marketbyprice-parser reads those,
// and its own manifest test is what holds the corpus to having no vector nobody
// reads.
func goldenVectors() []goldenVector {
	return []goldenVector{
		{
			file: "trade-v3.bin", typeID: msgTypeTrade, size: 52, flags: 0, schema: mboSchemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				tr, err := ParseTrade(body)
				if err != nil {
					t.Fatalf("ParseTrade: %v", err)
				}
				return []goldenField{
					{"instrument_id", int64(tr.InstrumentID), 1},
					{"source_id", int64(tr.SourceID), 2},
					{"aggressor_side", int64(tr.AggressorSide), 1},
					{"trade_flags", int64(tr.TradeFlags), 2},
					{"source_timestamp_ns", tr.SourceTimestamp.UnixNano(), 1700000000000000001},
					{"trade_price", tr.TradePriceRaw, 10000000},
					{"trade_qty", int64(tr.TradeQtyRaw), 500},
					{"trade_id", int64(tr.TradeID), 987654321},
					{"cumulative_volume", int64(tr.CumulativeVolumeRaw), 1000000},
				}, nil
			},
		},
		{
			file: "instrument-definition-v3.bin", typeID: msgTypeInstrumentDefinition, size: 130, flags: 0, schema: mboSchemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d, err := ParseInstrumentDefinition(body, mboSchemaVersionV3)
				if err != nil {
					t.Fatalf("ParseInstrumentDefinition: %v", err)
				}
				return instDefFields(d, 2), instDefText(d)
			},
		},
		{
			// The schema 1 vector is decode-only: nothing here emits that
			// layout, and InstrumentDefinition is the one message in this
			// family whose layout changed between schema generations, so it is
			// the one most likely to drift.
			file: "instrument-definition-v1.bin", typeID: msgTypeInstrumentDefinition, size: 80, flags: 0, schema: mboSchemaVersionV1,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d, err := ParseInstrumentDefinition(body, mboSchemaVersionV1)
				if err != nil {
					t.Fatalf("ParseInstrumentDefinition: %v", err)
				}
				return instDefFields(d, 0), instDefText(d)
			},
		},
		{
			file: "manifest-summary-v3.bin", typeID: msgTypeManifestSummary, size: 24, flags: 0, schema: mboSchemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				m, err := ParseManifestSummary(body)
				if err != nil {
					t.Fatalf("ParseManifestSummary: %v", err)
				}
				return []goldenField{
					{"channel_id", int64(m.ChannelID), 7},
					{"valid", int64(m.Valid), 1},
					{"manifest_seq", int64(m.ManifestSeq), 9},
					{"instrument_count", int64(m.InstrumentCount), 1234},
					{"timestamp_ns", m.Timestamp.UnixNano(), 1700000000000000002},
				}, nil
			},
		},
		{
			// SnapshotEnd is the one depth message whose bytes this feed and
			// market-by-price share: the same three fields in the same 16-byte
			// body under type id 0x22. The vector was captured from the
			// market-by-price encoder, so binding it here is what keeps the two
			// decoders from drifting apart over a message neither owns alone.
			file: "snapshot-end-v3.bin", typeID: msgTypeSnapshotEnd, size: 20, flags: flagSnapshot, schema: mboSchemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				e, err := ParseSnapshotEnd(body)
				if err != nil {
					t.Fatalf("ParseSnapshotEnd: %v", err)
				}
				return []goldenField{
					{"instrument_id", int64(e.InstrumentID), 1},
					{"anchor_seq", int64(e.AnchorSeq), 918273645},
					{"snapshot_id", int64(e.SnapshotID), 77},
				}, nil
			},
		},
	}
}

// instDefFields is the InstrumentDefinition expectation both schema generations
// share. Only source_id differs: schema 1 has no field for it and the manifest
// states it decodes as 0, which is the Source ID Registry's Unknown value.
func instDefFields(d InstrumentDefinitionBody, wantSourceID int64) []goldenField {
	return []goldenField{
		{"instrument_id", int64(d.InstrumentID), 1},
		{"source_id", int64(d.SourceID), wantSourceID},
		{"asset_class", int64(d.AssetClass), 1},
		{"price_exponent", int64(d.PriceExponent), -2},
		{"qty_exponent", int64(d.QtyExponent), -8},
		{"market_model", int64(d.MarketModel), 1},
		{"tick_size", d.TickSizeRaw, 1},
		{"lot_size", int64(d.LotSizeRaw), 1000},
		{"contract_value", int64(d.ContractValue), 0},
		{"expiry_ns", d.Expiry.UnixNano(), 0},
		{"settle_type", int64(d.SettleType), 0},
		{"price_bound", int64(d.PriceBound), 0},
		{"manifest_seq", int64(d.ManifestSeq), 9},
	}
}

func instDefText(d InstrumentDefinitionBody) []goldenText {
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

func TestGoldenTrade(t *testing.T) { runGoldenVector(t, "trade-v3.bin") }

func TestGoldenInstrumentDefinitionV3(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v3.bin")
}

func TestGoldenInstrumentDefinitionV1(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v1.bin")
}

func TestGoldenManifestSummary(t *testing.T) { runGoldenVector(t, "manifest-summary-v3.bin") }

func TestGoldenSnapshotEnd(t *testing.T) { runGoldenVector(t, "snapshot-end-v3.bin") }

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
