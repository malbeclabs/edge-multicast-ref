package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// The golden vectors are the cross-language contract. These four were
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
// Four of the vectors belong to this feed. Trade, InstrumentDefinition and
// ManifestSummary are byte-identical across the family, so this decoder has to
// read the same bytes the top-of-book one does. The depth vectors are not among
// them: SnapshotBegin carries Total Orders and a 32-byte body here against
// market-by-price's Total Levels, Depth Bound and 36, and the manifest tags
// those vectors `"feed": "market-by-price"` for that reason.
//
// The expected values are the `fields` block of testdata/golden/manifest.json,
// which is where an implementation in any language reads them from, and the
// names below are the manifest's names so a failure points straight at the row
// that disagrees. Where the manifest's name and this parser's field spelling
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
// manifest. It is 0 on all four of these: bit 0 marks a message travelling the
// `snapshot` port, and a Trade arrives on `mktdata` while an
// InstrumentDefinition and a ManifestSummary arrive on `refdata`. The value is
// asserted rather than assumed because it is the one part of these bytes the
// encoder does not decide — the builder stamps it at push, from the port.
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

func TestGoldenTrade(t *testing.T) {
	body := header(t, goldenBytes(t, "trade-v3.bin"), msgTypeTrade, 52, 0)
	tr, err := ParseTrade(body)
	if err != nil {
		t.Fatalf("ParseTrade: %v", err)
	}
	checkFields(t, []goldenField{
		{"instrument_id", int64(tr.InstrumentID), 1},
		{"source_id", int64(tr.SourceID), 2},
		{"aggressor_side", int64(tr.AggressorSide), 1},
		{"trade_flags", int64(tr.TradeFlags), 2},
		{"source_timestamp_ns", tr.SourceTimestamp.UnixNano(), 1700000000000000001},
		{"trade_price", tr.TradePriceRaw, 10000000},
		{"trade_qty", int64(tr.TradeQtyRaw), 500},
		{"trade_id", int64(tr.TradeID), 987654321},
		{"cumulative_volume", int64(tr.CumulativeVolumeRaw), 1000000},
	})
}

// instDefFields is the InstrumentDefinition expectation both schema generations
// share. Only source_id differs: schema 1 has no field for it and the manifest
// states it decodes as 0, which the instrument registry reads as Unknown.
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

func TestGoldenInstrumentDefinitionV3(t *testing.T) {
	body := header(t, goldenBytes(t, "instrument-definition-v3.bin"), msgTypeInstrumentDefinition, 130, 0)
	d, err := ParseInstrumentDefinition(body, mboSchemaVersionV3)
	if err != nil {
		t.Fatalf("ParseInstrumentDefinition: %v", err)
	}
	checkFields(t, instDefFields(d, 2))
	checkText(t, instDefText(d))
}

// The schema 1 vector is decode-only: nothing here emits that layout, and
// InstrumentDefinition is the one message in this family whose layout changed
// between schema generations, so it is the one most likely to drift.
func TestGoldenInstrumentDefinitionV1(t *testing.T) {
	body := header(t, goldenBytes(t, "instrument-definition-v1.bin"), msgTypeInstrumentDefinition, 80, 0)
	d, err := ParseInstrumentDefinition(body, mboSchemaVersionV1)
	if err != nil {
		t.Fatalf("ParseInstrumentDefinition: %v", err)
	}
	checkFields(t, instDefFields(d, 0))
	checkText(t, instDefText(d))
}

func TestGoldenManifestSummary(t *testing.T) {
	body := header(t, goldenBytes(t, "manifest-summary-v3.bin"), msgTypeManifestSummary, 24, 0)
	m, err := ParseManifestSummary(body)
	if err != nil {
		t.Fatalf("ParseManifestSummary: %v", err)
	}
	checkFields(t, []goldenField{
		{"channel_id", int64(m.ChannelID), 7},
		{"valid", int64(m.Valid), 1},
		{"manifest_seq", int64(m.ManifestSeq), 9},
		{"instrument_count", int64(m.InstrumentCount), 1234},
		{"timestamp_ns", m.Timestamp.UnixNano(), 1700000000000000002},
	})
}
