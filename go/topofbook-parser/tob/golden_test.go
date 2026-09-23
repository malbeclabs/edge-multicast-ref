package tob

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// The golden vectors in testdata/golden are the cross-language contract, and
// the top-of-book and reference-data ones were transcribed by hand from the
// field tables in edge-feed-spec rather than captured from an encoder. That is
// what gives them their force here: a fixture this package builds itself states
// the same reading of the spec that the decoder beside it was written from, so
// the two agree even when the reading is wrong. The vector was written from the
// spec by someone else, so a misreading on this side fails.
//
// go/marketbyprice-parser/golden_test.go does the same for the five
// depth-grain vectors. These are the five this feed defines: Quote, Trade,
// ManifestSummary, and InstrumentDefinition in both schema generations.
//
// testdata/golden/manifest.json carries each vector's field values, and the
// wants below are that file's values — the assertions are the manifest, not a
// transcription of what this decoder happens to return.
const goldenDir = "../../../testdata/golden"

func goldenBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(goldenDir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return b
}

// goldenBody asserts the 4-byte message header and returns the body, which is
// what decodeTopOfBookBody receives once decodeTopOfBookDatagram has sliced the
// header off.
//
// wantFlags is the on-wire Flags field. All five vectors here carry 0, and
// manifest.json states it per vector rather than leaving it to be noticed, so
// it is asserted rather than skipped past.
func goldenBody(t *testing.T, buf []byte, wantType uint8, wantSize int, wantFlags uint16) []byte {
	t.Helper()
	if len(buf) != wantSize {
		t.Fatalf("golden is %d bytes, want %d", len(buf), wantSize)
	}
	if buf[0] != wantType {
		t.Fatalf("type id 0x%02x, want 0x%02x", buf[0], wantType)
	}
	if int(buf[1]) != wantSize {
		t.Fatalf("declared msg_length %d, want %d", buf[1], wantSize)
	}
	if got := binary.LittleEndian.Uint16(buf[2:4]); got != wantFlags {
		t.Fatalf("flags 0x%04x, want 0x%04x", got, wantFlags)
	}
	return buf[messageHeaderSize:]
}

func TestGoldenQuote(t *testing.T) {
	body := goldenBody(t, goldenBytes(t, "quote-v3.bin"), msgQuote, 60, 0)
	decoded, err := decodeTopOfBookBody(msgQuote, body, schemaVersionV3)
	if err != nil {
		t.Fatalf("decode quote: %v", err)
	}
	b, ok := decoded.(*topOfBookQuote)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookQuote", decoded)
	}
	if b.InstrumentID != 1 || b.SourceID != 2 || b.UpdateFlags != 3 {
		t.Errorf("identity fields: %+v", b)
	}
	if b.SourceTimestamp != 1700000000000000000 {
		t.Errorf("source timestamp %d", b.SourceTimestamp)
	}
	if b.BidPrice != 9999500 || b.BidQty != 12500 {
		t.Errorf("bid: %+v", b)
	}
	if b.AskPrice != 10000500 || b.AskQty != 7250 {
		t.Errorf("ask: %+v", b)
	}
	if b.BidSourceCount != 3 || b.AskSourceCount != 4 {
		t.Errorf("source counts: %+v", b)
	}
}

func TestGoldenTrade(t *testing.T) {
	body := goldenBody(t, goldenBytes(t, "trade-v3.bin"), msgTrade, 52, 0)
	decoded, err := decodeTopOfBookBody(msgTrade, body, schemaVersionV3)
	if err != nil {
		t.Fatalf("decode trade: %v", err)
	}
	b, ok := decoded.(*topOfBookTrade)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookTrade", decoded)
	}
	if b.InstrumentID != 1 || b.SourceID != 2 || b.AggressorSide != 1 || b.TradeFlags != 2 {
		t.Errorf("identity fields: %+v", b)
	}
	if b.SourceTimestamp != 1700000000000000001 {
		t.Errorf("source timestamp %d", b.SourceTimestamp)
	}
	if b.TradePrice != 10000000 || b.TradeQty != 500 {
		t.Errorf("price/qty: %+v", b)
	}
	if b.TradeID != 987654321 || b.CumulativeVolume != 1000000 {
		t.Errorf("trade id / cumulative volume: %+v", b)
	}
}

// ManifestSummary is the vector that binds the Valid byte. It has carried
// valid: 1 at offset 5 since it was transcribed, so a decoder that reads the
// byte as part of the reserved run returns 0 here and this test fails.
func TestGoldenManifestSummary(t *testing.T) {
	buf := goldenBytes(t, "manifest-summary-v3.bin")
	body := goldenBody(t, buf, msgManifestSummary, 24, 0)
	decoded, err := decodeTopOfBookBody(msgManifestSummary, body, schemaVersionV3)
	if err != nil {
		t.Fatalf("decode manifest_summary: %v", err)
	}
	b, ok := decoded.(*topOfBookManifestSummary)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookManifestSummary", decoded)
	}
	if b.ChannelID != 7 {
		t.Errorf("channel id %d, want 7", b.ChannelID)
	}
	if b.Valid != 1 {
		t.Errorf("valid %d, want 1 (golden byte %d at offset 5)", b.Valid, buf[5])
	}
	if b.ManifestSeq != 9 || b.InstrumentCount != 1234 {
		t.Errorf("manifest seq / instrument count: %+v", b)
	}
	if b.Timestamp != 1700000000000000002 {
		t.Errorf("timestamp %d", b.Timestamp)
	}
}

// InstrumentDefinition is the one message in this family whose layout changed
// between schema generations, so both are pinned. The two vectors carry the
// same logical values apart from Source ID, which schema 1 has no field for and
// which must decode as 0.
func TestGoldenInstrumentDefinitionV3(t *testing.T) {
	body := goldenBody(t, goldenBytes(t, "instrument-definition-v3.bin"), msgInstrumentDefinition, 130, 0)
	decoded, err := decodeTopOfBookBody(msgInstrumentDefinition, body, schemaVersionV3)
	if err != nil {
		t.Fatalf("decode instrument_definition v3: %v", err)
	}
	b, ok := decoded.(*topOfBookInstrumentDef)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookInstrumentDef", decoded)
	}
	if b.SourceID != 2 {
		t.Errorf("source id %d, want 2", b.SourceID)
	}
	assertGoldenInstDef(t, b)
}

func TestGoldenInstrumentDefinitionV1(t *testing.T) {
	body := goldenBody(t, goldenBytes(t, "instrument-definition-v1.bin"), msgInstrumentDefinition, 80, 0)
	decoded, err := decodeTopOfBookBody(msgInstrumentDefinition, body, schemaVersionV1)
	if err != nil {
		t.Fatalf("decode instrument_definition v1: %v", err)
	}
	b, ok := decoded.(*topOfBookInstrumentDef)
	if !ok {
		t.Fatalf("decoded %T, want *topOfBookInstrumentDef", decoded)
	}
	if b.SourceID != 0 {
		t.Errorf("source id %d, want 0: schema 1 has no field for it", b.SourceID)
	}
	assertGoldenInstDef(t, b)
}

// assertGoldenInstDef checks the fields both InstrumentDefinition vectors share.
// Symbol, Leg1 and Leg2 are trimmed here because decodeTopOfBookBody returns the
// raw null-padded fixed-width field and topofbook.go trims downstream.
func assertGoldenInstDef(t *testing.T, b *topOfBookInstrumentDef) {
	t.Helper()
	if b.InstrumentID != 1 {
		t.Errorf("instrument id %d, want 1", b.InstrumentID)
	}
	if trimNull(b.Symbol) != "BTC-USDT" || trimNull(b.Leg1) != "BTC" || trimNull(b.Leg2) != "USDT" {
		t.Errorf("symbol/legs: %q %q %q", trimNull(b.Symbol), trimNull(b.Leg1), trimNull(b.Leg2))
	}
	if b.AssetClass != 1 || b.MarketModel != 1 {
		t.Errorf("asset class / market model: %d %d, want 1 1", b.AssetClass, b.MarketModel)
	}
	if b.PriceExponent != -2 || b.QtyExponent != -8 {
		t.Errorf("exponents: %d %d, want -2 -8", b.PriceExponent, b.QtyExponent)
	}
	if b.TickSize != 1 || b.LotSize != 1000 || b.ContractValue != 0 {
		t.Errorf("tick/lot/contract: %+v", b)
	}
	if b.Expiry != 0 || b.SettleType != 0 || b.PriceBound != 0 {
		t.Errorf("expiry/settle/bound: %+v", b)
	}
	if b.ManifestSeq != 9 {
		t.Errorf("manifest seq %d, want 9", b.ManifestSeq)
	}
}
