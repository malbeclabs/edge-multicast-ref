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

// The golden vectors are the cross-language contract. The Rust codec crate
// writes the five depth files and asserts the same field values against them;
// the four below them — Trade, both InstrumentDefinition generations and
// ManifestSummary — were transcribed by hand from the field tables in
// edge-feed-spec. The five `-from-event-` files are the shared lowering's
// output for a normalized event, written by dz-publisher-lowering. This side
// reads all fourteen with this parser's own decoder.
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
// Trade, InstrumentDefinition and ManifestSummary are byte-identical across the
// family and this parser decodes its own copy of each, so the vectors bind them
// here as well as in the top-of-book and market-by-order parsers. The depth
// vectors are this feed's, with one exception in the other direction:
// SnapshotEnd is the same 16-byte body under type id 0x22 in market-by-order,
// and that parser binds snapshot-end-v3.bin too.
//
// The expected values are the `fields` block of testdata/golden/manifest.json,
// which is where an implementation in any language reads them from, and the
// names below are the manifest's names so a failure points straight at the row
// that disagrees. That is a binding and not a claim:
// TestGoldenManifestStatesWhatTheseCasesAssert compares every row below with
// the manifest's own, in both directions, and
// TestGoldenManifestHasNoVectorNobodyReads holds the corpus to having no vector
// no suite asserts. Where the manifest's name and this parser's field spelling
// differ, the difference is the `Raw` suffix this parser puts on a value still
// in wire units, or the `_ns` the wire structs drop because they hold a
// time.Time:
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
// Parse* functions below take the body, so each case slices past it.
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
// wantFlags is the on-wire Flags field, which is not decoration: bit 0 is set on
// every message travelling the snapshot port, and a message carrying the wrong
// value is one this parser counts as a SnapshotFlagMismatch defect. The vectors
// have to state it or an implementation transcribing them inherits the bug.
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

// goldenVectors is every vector this parser reads: its own five depth messages,
// the four the family shares, and the five the shared lowering produced from a
// normalized event. The one it does not read is quote-v3.bin, which this feed
// has no message type for — goldenVectorsReadElsewhere names it and the suite
// that does.
func goldenVectors() []goldenVector {
	return append(goldenDepthVectors(), append(goldenSharedVectors(), goldenLoweredVectors()...)...)
}

// goldenDepthVectors are the depth-grain messages only this feed has, written
// by the Rust codec crate from a hand-written wire struct.
func goldenDepthVectors() []goldenVector {
	return []goldenVector{
		{
			file: "level-update-v3.bin", typeID: msgTypeLevelUpdate, size: 48, flags: 0, schema: mbpSchemaVersionV3,
			rows: levelUpdateRows(1, 2, 4242, 10000500, 7250, 1700000000000000003, 5, 6, 2, 8),
		},
		{
			file: "book-clear-v3.bin", typeID: msgTypeBookClear, size: 36, flags: 0, schema: mbpSchemaVersionV3,
			rows: bookClearRows(1, 2, 4243, 10000500, 1700000000000000004, 3),
		},
		{
			file: "snapshot-begin-v3.bin", typeID: msgTypeSnapshotBegin, size: 40, flags: flagSnapshot, schema: mbpSchemaVersionV3,
			rows: snapshotBeginRows(918273645, 2, 77, 4241, 1700000000000000005, 50),
		},
		{
			file: "snapshot-level-v3.bin", typeID: msgTypeSnapshotLevel, size: 32, flags: flagSnapshot, schema: mbpSchemaVersionV3,
			rows: snapshotLevelRows(77, 9999500, 12500, 3, 4),
		},
		{
			file: "snapshot-end-v3.bin", typeID: msgTypeSnapshotEnd, size: 20, flags: flagSnapshot, schema: mbpSchemaVersionV3,
			rows: snapshotEndRows(918273645, 77),
		},
	}
}

// goldenLoweredVectors are the five the shared lowering produced from a
// normalized event, asserted on the Rust side by dz-publisher-lowering and read
// here with this parser's decoder — which is the same cross-language statement
// the codec's own vectors make, and the reason these are not left to one
// language.
//
// Their Flags is 0 on all five, including the three snapshot messages, because
// these files are what the lowering encodes rather than what leaves the
// publisher: bit 0 is stamped at push, from the port, after the body is
// encoded. The manifest records that as flags_on_wire and this suite asserts
// it, so an implementation reading these bytes is told the bit is still to come
// rather than left to notice. The codec's own snapshot vectors carry it.
func goldenLoweredVectors() []goldenVector {
	return []goldenVector{
		{
			file: "level-update-from-event-v3.bin", typeID: msgTypeLevelUpdate, size: 48, flags: 0, schema: mbpSchemaVersionV3,
			// per_instrument_seq is 1 rather than 4242: the boundary numbers
			// its own series and this is the first delta of an era. level_index
			// is 65535 — the unavailable value — and update_reason and
			// level_flags are 0, because a normalized event has nowhere for a
			// rank in the publisher's book or a reason to come from.
			rows: levelUpdateRows(1, 2, 1, 10000500, 7250, 1700000000000000003, 5, 65535, 0, 0),
		},
		{
			file: "book-clear-from-event-v3.bin", typeID: msgTypeBookClear, size: 36, flags: 0, schema: mbpSchemaVersionV3,
			// Second in the same per-instrument series, and clear_reason 0 for
			// the reason level_update's is: the boundary states none.
			rows: bookClearRows(1, 2, 2, 10000500, 1700000000000000004, 0),
		},
		{
			file: "snapshot-begin-from-event-v3.bin", typeID: msgTypeSnapshotBegin, size: 40, flags: 0, schema: mbpSchemaVersionV3,
			// last_instrument_seq is 0: no delta was lowered for this
			// instrument in this era.
			rows: snapshotBeginRows(918273645, 2, 1, 0, 1700000000000000005, 50),
		},
		{
			file: "snapshot-level-from-event-v3.bin", typeID: msgTypeSnapshotLevel, size: 32, flags: 0, schema: mbpSchemaVersionV3,
			rows: snapshotLevelRows(1, 9999500, 12500, 3, 0),
		},
		{
			file: "snapshot-end-from-event-v3.bin", typeID: msgTypeSnapshotEnd, size: 20, flags: 0, schema: mbpSchemaVersionV3,
			rows: snapshotEndRows(918273645, 1),
		},
	}
}

// goldenSharedVectors are the messages byte-identical across the family, which
// this parser decodes with its own copy of each layout.
func goldenSharedVectors() []goldenVector {
	return []goldenVector{
		{
			file: "trade-v3.bin", typeID: msgTypeTrade, size: 52, flags: 0, schema: mbpSchemaVersionV3,
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
			file: "instrument-definition-v3.bin", typeID: msgTypeInstrumentDefinition, size: 130, flags: 0, schema: mbpSchemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d, err := ParseInstrumentDefinition(body, mbpSchemaVersionV3)
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
			file: "instrument-definition-v1.bin", typeID: msgTypeInstrumentDefinition, size: 80, flags: 0, schema: mbpSchemaVersionV1,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				d, err := ParseInstrumentDefinition(body, mbpSchemaVersionV1)
				if err != nil {
					t.Fatalf("ParseInstrumentDefinition: %v", err)
				}
				return instDefFields(d, 0), instDefText(d)
			},
		},
		{
			file: "manifest-summary-v3.bin", typeID: msgTypeManifestSummary, size: 24, flags: 0, schema: mbpSchemaVersionV3,
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
	}
}

// The five depth messages' expectations, parameterised by the values that
// differ between a vector built from a wire struct and the same message lowered
// from a normalized event. Every field is named and asserted in both; what the
// arguments carry is the values, so that the two provenances cannot drift into
// asserting different sets of fields.

func levelUpdateRows(instrumentID, sourceID, seq, priceRaw, qtyRaw, timestampNs, orderCount, levelIndex, updateReason, levelFlags int64) func(*testing.T, []byte) ([]goldenField, []goldenText) {
	return func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
		b, err := ParseLevelUpdate(body)
		if err != nil {
			t.Fatalf("ParseLevelUpdate: %v", err)
		}
		return []goldenField{
			{"instrument_id", int64(b.InstrumentID), instrumentID},
			{"source_id", int64(b.SourceID), sourceID},
			{"side", int64(b.Side), 1},
			{"action", int64(b.Action), 1},
			{"per_instrument_seq", int64(b.PerInstrumentSeq), seq},
			{"price_raw", b.PriceRaw, priceRaw},
			{"qty_raw", int64(b.QtyRaw), qtyRaw},
			{"timestamp_ns", b.Timestamp.UnixNano(), timestampNs},
			{"order_count", int64(b.OrderCount), orderCount},
			{"level_index", int64(b.LevelIndex), levelIndex},
			{"update_reason", int64(b.UpdateReason), updateReason},
			{"level_flags", int64(b.LevelFlags), levelFlags},
		}, nil
	}
}

func bookClearRows(instrumentID, sourceID, seq, fromPriceRaw, timestampNs, clearReason int64) func(*testing.T, []byte) ([]goldenField, []goldenText) {
	return func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
		b, err := ParseBookClear(body)
		if err != nil {
			t.Fatalf("ParseBookClear: %v", err)
		}
		return []goldenField{
			{"instrument_id", int64(b.InstrumentID), instrumentID},
			{"source_id", int64(b.SourceID), sourceID},
			{"clear_side", int64(b.ClearSide), 1},
			{"scope", int64(b.Scope), 1},
			{"per_instrument_seq", int64(b.PerInstrumentSeq), seq},
			{"from_price_raw", b.FromPriceRaw, fromPriceRaw},
			{"timestamp_ns", b.Timestamp.UnixNano(), timestampNs},
			{"clear_reason", int64(b.ClearReason), clearReason},
		}, nil
	}
}

func snapshotBeginRows(anchorSeq, totalLevels, snapshotID, lastInstrumentSeq, timestampNs, depthBound int64) func(*testing.T, []byte) ([]goldenField, []goldenText) {
	return func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
		b, err := ParseSnapshotBegin(body)
		if err != nil {
			t.Fatalf("ParseSnapshotBegin: %v", err)
		}
		return []goldenField{
			{"instrument_id", int64(b.InstrumentID), 1},
			{"anchor_seq", int64(b.AnchorSeq), anchorSeq},
			{"total_levels", int64(b.TotalLevels), totalLevels},
			{"snapshot_id", int64(b.SnapshotID), snapshotID},
			{"last_instrument_seq", int64(b.LastInstrumentSeq), lastInstrumentSeq},
			{"timestamp_ns", b.Timestamp.UnixNano(), timestampNs},
			{"depth_bound", int64(b.DepthBound), depthBound},
		}, nil
	}
}

func snapshotLevelRows(snapshotID, priceRaw, qtyRaw, orderCount, levelFlags int64) func(*testing.T, []byte) ([]goldenField, []goldenText) {
	return func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
		b, err := ParseSnapshotLevel(body)
		if err != nil {
			t.Fatalf("ParseSnapshotLevel: %v", err)
		}
		return []goldenField{
			{"snapshot_id", int64(b.SnapshotID), snapshotID},
			{"price_raw", b.PriceRaw, priceRaw},
			{"qty_raw", int64(b.QtyRaw), qtyRaw},
			{"order_count", int64(b.OrderCount), orderCount},
			{"side", int64(b.Side), 0},
			{"level_flags", int64(b.LevelFlags), levelFlags},
		}, nil
	}
}

func snapshotEndRows(anchorSeq, snapshotID int64) func(*testing.T, []byte) ([]goldenField, []goldenText) {
	return func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
		b, err := ParseSnapshotEnd(body)
		if err != nil {
			t.Fatalf("ParseSnapshotEnd: %v", err)
		}
		return []goldenField{
			{"instrument_id", int64(b.InstrumentID), 1},
			{"anchor_seq", int64(b.AnchorSeq), anchorSeq},
			{"snapshot_id", int64(b.SnapshotID), snapshotID},
		}, nil
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

func TestGoldenLevelUpdate(t *testing.T) { runGoldenVector(t, "level-update-v3.bin") }

func TestGoldenBookClear(t *testing.T) { runGoldenVector(t, "book-clear-v3.bin") }

func TestGoldenSnapshotBegin(t *testing.T) { runGoldenVector(t, "snapshot-begin-v3.bin") }

func TestGoldenSnapshotLevel(t *testing.T) { runGoldenVector(t, "snapshot-level-v3.bin") }

func TestGoldenSnapshotEnd(t *testing.T) { runGoldenVector(t, "snapshot-end-v3.bin") }

func TestGoldenTrade(t *testing.T) { runGoldenVector(t, "trade-v3.bin") }

func TestGoldenInstrumentDefinitionV3(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v3.bin")
}

func TestGoldenInstrumentDefinitionV1(t *testing.T) {
	runGoldenVector(t, "instrument-definition-v1.bin")
}

func TestGoldenManifestSummary(t *testing.T) { runGoldenVector(t, "manifest-summary-v3.bin") }

// The lowered vectors. One case each, named like the rest, because a file read
// only inside a loop over a table is a file whose failure names a subtest and
// not a message.

func TestGoldenLoweredLevelUpdate(t *testing.T) {
	runGoldenVector(t, "level-update-from-event-v3.bin")
}

func TestGoldenLoweredBookClear(t *testing.T) {
	runGoldenVector(t, "book-clear-from-event-v3.bin")
}

func TestGoldenLoweredSnapshotBegin(t *testing.T) {
	runGoldenVector(t, "snapshot-begin-from-event-v3.bin")
}

func TestGoldenLoweredSnapshotLevel(t *testing.T) {
	runGoldenVector(t, "snapshot-level-from-event-v3.bin")
}

func TestGoldenLoweredSnapshotEnd(t *testing.T) {
	runGoldenVector(t, "snapshot-end-from-event-v3.bin")
}

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

// goldenVectorsReadElsewhere names the vectors the manifest carries that this
// parser has no message type for, and the suite that reads each.
//
// This parser decodes fourteen of the fifteen, which makes it the one place a
// vector nobody reads can be noticed at all. That is what the list is for: a
// vector no suite reads and no document mentions is invisible rather than
// merely unbound, and it stays that way until someone counts the files by
// hand.
var goldenVectorsReadElsewhere = map[string]string{
	"quote-v3.bin": "go/topofbook-parser's tob.TestGoldenQuote; market-by-price carries no Quote",
}

// TestGoldenManifestHasNoVectorNobodyReads holds the corpus to being wholly
// accounted for: every vector the manifest carries is read by a case above or
// named in goldenVectorsReadElsewhere with the suite that reads it. Adding a
// vector and binding it nowhere fails here.
func TestGoldenManifestHasNoVectorNobodyReads(t *testing.T) {
	read := make(map[string]bool)
	for _, v := range goldenVectors() {
		read[v.file] = true
	}
	stated := readGoldenManifest(t)
	for _, m := range stated {
		if read[m.File] {
			if where, ok := goldenVectorsReadElsewhere[m.File]; ok {
				t.Errorf("%s is read here and also listed as read by %s; the list is for the ones this parser cannot read", m.File, where)
			}
			continue
		}
		if _, ok := goldenVectorsReadElsewhere[m.File]; !ok {
			t.Errorf("no suite is named for %s: add a case for it, or name the suite that asserts it in goldenVectorsReadElsewhere", m.File)
		}
	}
	// And the list may not name a vector the manifest dropped, which would
	// leave it claiming coverage of a file that no longer exists.
	for file := range goldenVectorsReadElsewhere {
		if _, ok := stated[file]; !ok {
			t.Errorf("goldenVectorsReadElsewhere names %s, which the manifest no longer carries", file)
		}
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
