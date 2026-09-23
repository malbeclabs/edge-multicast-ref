package tob

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/golden"
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
type goldenField = golden.Field

func checkFields(t *testing.T, fields []goldenField) {
	t.Helper()
	for _, f := range fields {
		if f.Got != f.Want {
			t.Errorf("%s = %d, want %d", f.Name, f.Got, f.Want)
		}
	}
}

// goldenText is a goldenField for the fixed-width ASCII fields. This decoder
// returns them null-padded and the rest of the parser trims with trimNull, so
// the comparison trims too.
type goldenText = golden.Text

func checkText(t *testing.T, fields []goldenText) {
	t.Helper()
	for _, f := range fields {
		if got := trimNull(f.Got); got != f.Want {
			t.Errorf("%s = %q, want %q", f.Name, got, f.Want)
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
// manifest as the authority for these values can be true of the code.
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
					{Name: "instrument_id", Got: int64(q.InstrumentID), Want: 1},
					{Name: "source_id", Got: int64(q.SourceID), Want: 2},
					{Name: "update_flags", Got: int64(q.UpdateFlags), Want: 3},
					{Name: "source_timestamp_ns", Got: int64(q.SourceTimestamp), Want: 1700000000000000000},
					{Name: "bid_price", Got: q.BidPrice, Want: 9999500},
					{Name: "bid_qty", Got: int64(q.BidQty), Want: 12500},
					{Name: "ask_price", Got: q.AskPrice, Want: 10000500},
					{Name: "ask_qty", Got: int64(q.AskQty), Want: 7250},
					{Name: "bid_source_count", Got: int64(q.BidSourceCount), Want: 3},
					{Name: "ask_source_count", Got: int64(q.AskSourceCount), Want: 4},
				}, nil
			},
		},
		{
			file: "trade-v3.bin", typeID: msgTrade, size: 52, flags: 0, schema: schemaVersionV3,
			rows: func(t *testing.T, body []byte) ([]goldenField, []goldenText) {
				tr := decodeGolden[topOfBookTrade](t, body, msgTrade, schemaVersionV3)
				return []goldenField{
					{Name: "instrument_id", Got: int64(tr.InstrumentID), Want: 1},
					{Name: "source_id", Got: int64(tr.SourceID), Want: 2},
					{Name: "aggressor_side", Got: int64(tr.AggressorSide), Want: 1},
					{Name: "trade_flags", Got: int64(tr.TradeFlags), Want: 2},
					{Name: "source_timestamp_ns", Got: int64(tr.SourceTimestamp), Want: 1700000000000000001},
					{Name: "trade_price", Got: tr.TradePrice, Want: 10000000},
					{Name: "trade_qty", Got: int64(tr.TradeQty), Want: 500},
					{Name: "trade_id", Got: int64(tr.TradeID), Want: 987654321},
					{Name: "cumulative_volume", Got: int64(tr.CumulativeVolume), Want: 1000000},
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
					{Name: "channel_id", Got: int64(m.ChannelID), Want: 7},
					{Name: "valid", Got: int64(m.Valid), Want: 1},
					{Name: "manifest_seq", Got: int64(m.ManifestSeq), Want: 9},
					{Name: "instrument_count", Got: int64(m.InstrumentCount), Want: 1234},
					{Name: "timestamp_ns", Got: int64(m.Timestamp), Want: 1700000000000000002},
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
		{Name: "instrument_id", Got: int64(d.InstrumentID), Want: 1},
		{Name: "source_id", Got: int64(d.SourceID), Want: wantSourceID},
		{Name: "asset_class", Got: int64(d.AssetClass), Want: 1},
		{Name: "price_exponent", Got: int64(d.PriceExponent), Want: -2},
		{Name: "qty_exponent", Got: int64(d.QtyExponent), Want: -8},
		{Name: "market_model", Got: int64(d.MarketModel), Want: 1},
		{Name: "tick_size", Got: d.TickSize, Want: 1},
		{Name: "lot_size", Got: int64(d.LotSize), Want: 1000},
		{Name: "contract_value", Got: int64(d.ContractValue), Want: 0},
		{Name: "expiry_ns", Got: int64(d.Expiry), Want: 0},
		{Name: "settle_type", Got: int64(d.SettleType), Want: 0},
		{Name: "price_bound", Got: int64(d.PriceBound), Want: 0},
		{Name: "manifest_seq", Got: int64(d.ManifestSeq), Want: 9},
	}
}

func instDefText(d *topOfBookInstrumentDef) []goldenText {
	return []goldenText{
		{Name: "symbol", Got: d.Symbol, Want: "BTC-USDT"},
		{Name: "leg1", Got: d.Leg1, Want: "BTC"},
		{Name: "leg2", Got: d.Leg2, Want: "USDT"},
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
	stated := golden.Read(t, goldenDir)
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
			golden.CheckFields(t, m, fields, text)
		})
	}
}
