# Dual-version refdata (v1 + v3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decode `InstrumentDefinition` at wire schema versions 1 and 3 in all three parsers, carry the new `Source ID` field through to ClickHouse, and rebuild branch `feat/refdata-v2-dual-version` so the superseded v2 layout never appears in its history.

**Architecture:** Each parser owns a full copy of its wire decoder (separate Go modules, no shared package). The frame header's Schema Version byte is read per frame and passed into `InstrumentDefinition` decoding, which selects between a 76-byte v1 body and a 126-byte v3 body. The declared version and the body length cross-check each other in both directions. `Source ID` rides the parsers' existing free-form `Fields` map down to the bots, which write it to a new `source_id` column.

**Tech Stack:** Go 1.x (three independent modules under `go/`), Prometheus client_golang, ClickHouse via HTTP JSONEachRow inserts, Docker Compose for the demo stack.

**Spec:** [`docs/superpowers/specs/2026-08-08-refdata-v3-dual-version-design.md`](../specs/2026-08-08-refdata-v3-dual-version-design.md)

## Global Constraints

- **Accepted schema versions are the set `{1, 3}`.** Never a range. Version `2` must be rejected exactly like `0` and `4`. A ceiling check (`version > max`) is a bug.
- **v1 `InstrumentDefinition`:** 76-byte body, 80-byte message. `Symbol` is `char[16]` at body offset `4:20`. No `Source ID`.
- **v3 `InstrumentDefinition`:** 126-byte body, 130-byte message. `Source ID` is `u16` at body offset `4:6`; `Symbol` is `char[64]` at body offset `6:70`.
- **v3 body offsets** (body-relative, after the 4-byte message header): `InstrumentID 0:4`, `SourceID 4:6`, `Symbol 6:70`, `Leg1 70:78`, `Leg2 78:86`, `AssetClass 86`, `PriceExponent 87`, `QtyExponent 88`, `MarketModel 89`, `TickSize 90:98`, `LotSize 98:106`, `ContractValue 106:114`, `Expiry 114:122`, `SettleType 122`, `PriceBound 123`, `ManifestSeq 124:126`.
- **At v1, `SourceID` is `0`** (the Source ID Registry's Unknown). It is not read from the wire.
- **Error classification differs by parser, and `topofbook-parser` is the fragile one.** `marketbyorder-parser` and `marketbyprice-parser` classify with `errors.Is` against the sentinels `errTruncated` and `errSchemaVersion`, so their message text is free-form as long as the right sentinel is wrapped with `%w`. `topofbook-parser` has no sentinels: its `classifyParseErr` buckets on **substrings** of the message. There, a length mismatch must contain `truncat` and must **not** contain `schema`, and an unsupported version must contain `schema`.
- All multi-byte integers are little-endian.
- Every parser change is fixture-driven. No publisher emits v3 yet.

---

### Task 1: `marketbyorder-parser` decodes v1 and v3

**Files:**
- Modify: `go/marketbyorder-parser/marketbyorder_wire.go:16-17` (version constants), `:91` (header check), `:203-258` (`ParseInstrumentDefinition` and its length constants), `:209` (`InstrumentDefinitionBody` struct)
- Modify: `go/marketbyorder-parser/marketbyorder.go:83-98` (record `Fields` map)
- Modify: `go/marketbyorder-parser/README.md:9,29`
- Test: `go/marketbyorder-parser/marketbyorder_test.go:518-700`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `InstrumentDefinitionBody.SourceID uint16`; record `Fields["source_id"]` holding a `uint16`. Task 4 reads that map key.

- [ ] **Step 1: Rename the v1 constant for symmetry**

The pair currently reads `mboSchemaVersion` / `mboSchemaVersionV2`. With a gap in the version sequence, both names must carry their version explicitly.

```bash
cd go/marketbyorder-parser
grep -rl 'mboSchemaVersion' . | xargs sed -i '' 's/mboSchemaVersionV2/mboSchemaVersionV3/g; s/mboSchemaVersion\b/mboSchemaVersionV1/g'
```

Then fix the two declarations in `marketbyorder_wire.go:16-17` to read:

```go
	mboSchemaVersionV1 uint8  = 1 // v1: InstrumentDefinition, 76-byte body (80-byte message)
	mboSchemaVersionV3 uint8  = 3 // v3: InstrumentDefinition, 126-byte body (130-byte message)
```

Confirm it builds before going further: `go build ./...`

- [ ] **Step 2: Write the failing tests**

Replace `buildInstDefV2` and the four v2 tests in `marketbyorder_test.go` with these. Note `buildInstDefV3` writes a nonzero `Source ID` — without it the fixture cannot tell v3 from the v2 layout that never shipped.

```go
// buildInstDefV3 builds a 126-byte v3 body. Source ID is inserted at 4:6 and
// Symbol widens to 64 bytes, so every field after Instrument ID sits 50 bytes
// later than in v1.
func buildInstDefV3(symbol string) []byte {
	b := make([]byte, 126)
	binary.LittleEndian.PutUint32(b[0:4], 4242)
	binary.LittleEndian.PutUint16(b[4:6], 77) // source id
	copy(b[6:70], symbol)
	copy(b[70:78], "BTC")
	copy(b[78:86], "USDT")
	b[86] = 1
	priceExp, qtyExp := int8(-2), int8(-8)
	b[87] = byte(priceExp)
	b[88] = byte(qtyExp)
	b[89] = 1
	binary.LittleEndian.PutUint64(b[90:98], 50)
	binary.LittleEndian.PutUint64(b[98:106], 100)
	binary.LittleEndian.PutUint64(b[106:114], 1000)
	binary.LittleEndian.PutUint64(b[114:122], 1700000000)
	b[122] = 1
	b[123] = 2
	binary.LittleEndian.PutUint16(b[124:126], 7)
	return b
}

// The v3 symbol MUST exceed 16 bytes, or this test proves nothing a v1 test
// does not. This is the whole point of the widening: a Kalshi ticker like
// KXNFLGAME-26SEP13NYJTEN-NYJ is 27 bytes and was previously truncated.
func TestParseInstrumentDefinition_V3LongSymbol(t *testing.T) {
	const long = "KXNFLGAME-26SEP13NYJTEN-NYJ"
	if len(long) <= 16 {
		t.Fatal("fixture symbol must exceed 16 bytes to be meaningful")
	}
	got, err := ParseInstrumentDefinition(buildInstDefV3(long), 3)
	if err != nil {
		t.Fatal(err)
	}
	assertInstDefFields(t, got, long)
	if got.SourceID != 77 {
		t.Errorf("source id: got %d want 77", got.SourceID)
	}
}

// v1 has no Source ID on the wire. It must decode as 0 (registry Unknown)
// rather than picking up the first two bytes of Symbol.
func TestParseInstrumentDefinition_V1SourceIDIsZero(t *testing.T) {
	got, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceID != 0 {
		t.Errorf("v1 source id: got %d want 0", got.SourceID)
	}
}

// A symbol filling all 64 bytes has no null terminator; it must not be truncated
// or run past the field.
func TestParseInstrumentDefinition_V3SymbolFillsField(t *testing.T) {
	full := strings.Repeat("A", 64)
	got, err := ParseInstrumentDefinition(buildInstDefV3(full), 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Symbol != full {
		t.Errorf("symbol length: got %d want 64", len(got.Symbol))
	}
	if got.Leg1 != "BTC" {
		t.Errorf("a full-width symbol must not bleed into Leg1: got %q", got.Leg1)
	}
}

// The declared version and the body length must agree. A v3 frame carrying a v1
// body would otherwise read Source ID and Symbol across 66 bytes of adjacent
// fields and produce plausible garbage instead of an error.
func TestParseInstrumentDefinition_LengthMustMatchVersion(t *testing.T) {
	if _, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 3); err == nil {
		t.Error("version 3 with a 76-byte body must be rejected")
	}
	if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), 1); err == nil {
		t.Error("version 1 with a 126-byte body must be rejected")
	}
}

// Version 2 was specified and superseded before any publisher emitted it. It is
// not a layout this decoder implements, and it must be rejected as firmly as a
// version that never existed at all — a version ceiling would let it through.
func TestParseInstrumentDefinition_UnsupportedVersion(t *testing.T) {
	for _, v := range []uint8{0, 2, 4, 255} {
		if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), v); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// The frame header accepts both implemented versions and nothing else.
func TestParseFrameHeader_AcceptsV1AndV3(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	for _, v := range []uint8{1, 3} {
		buf := buildFrameHeader(mboMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err != nil {
			t.Errorf("schema version %d must be accepted: %v", v, err)
		}
	}
	for _, v := range []uint8{0, 2, 4, 255} {
		buf := buildFrameHeader(mboMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}
```

In `TestParseFrame_FollowsVersionSwitchMidStream`, change the cutover frame and its expectations:

```go
	v1Frame := build(1, buildInstDefV1("SHORT"))
	v3Frame := build(3, buildInstDefV3("KXNFLGAME-26SEP13NYJTEN-NYJ"))

	for i, tc := range []struct {
		frame []byte
		want  string
	}{
		{v1Frame, "SHORT"},
		{v3Frame, "KXNFLGAME-26SEP13NYJTEN-NYJ"},
		{v1Frame, "SHORT"}, // and back again
	} {
```

Also update the comment above that function from "v1 to v2" to "v1 to v3".

Finally add a record-level assertion so `Fields["source_id"]` is covered:

```go
// Source ID must reach the record's Fields map, where the bot reads it.
func TestParseFrame_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	p := &marketByOrderParser{}
	ts := time.Unix(1700000000, 0)
	body := buildInstDefV3("BTC-USDT")
	msgLen := uint8(messageHeaderSize + len(body))
	frameLen := frameHeaderSize + int(msgLen)
	hdr := buildFrameHeader(mboMagic, 3, 0, 1, ts, 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgTypeInstrumentDefinition
	mh[1] = msgLen
	frame := append(append(hdr, mh...), body...)

	recs, err := p.ParseFrame("refdata", frame)
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
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd go/marketbyorder-parser && go test ./... -run 'InstrumentDefinition|VersionSwitch|AcceptsV1AndV3|SourceID' -count=1`
Expected: FAIL — compile error on `got.SourceID` (undefined field) and `buildInstDefV3` producing a length the decoder rejects.

- [ ] **Step 4: Add `SourceID` to the body struct**

In `marketbyorder_wire.go`, add the field to `InstrumentDefinitionBody` immediately after `InstrumentID`, mirroring `TradeBody` and `OrderAddBody` which already order it that way:

```go
type InstrumentDefinitionBody struct {
	InstrumentID  uint32
	SourceID      uint16 // 0 at schema v1, which carries no Source ID
	Symbol        string
	Leg1          string
```

- [ ] **Step 5: Rewrite the length constants and decoder**

Replace the length constants and `ParseInstrumentDefinition` in `marketbyorder_wire.go` with:

```go
// InstrumentDefinition body lengths, excluding the 4-byte message header.
//
// v3 inserts Source ID (u16) after Instrument ID and widens Symbol from
// char[16] to char[64], shifting every field after Instrument ID by 50 bytes.
// Nothing else in this feed changed between the two schema versions.
//
// There is no version 2. A 128-byte layout carrying the widened Symbol without
// Source ID was specified upstream and superseded before any publisher emitted
// it, so version 2 is rejected here rather than decoded.
const (
	instDefBodyLenV1 = 76
	instDefBodyLenV3 = 126
)

// ParseInstrumentDefinition decodes an InstrumentDefinition body using the
// layout for the frame's schema version.
//
// The body length cross-checks the declared version. They can only disagree if a
// publisher bumped the header without the payload or the reverse, and the
// mismatch must be caught here: decoding a v1 body under the v3 layout would
// read Source ID and Symbol across 66 bytes of adjacent fields and yield a
// plausible-looking instrument rather than an error.
func ParseInstrumentDefinition(buf []byte, schemaVersion uint8) (InstrumentDefinitionBody, error) {
	var symStart, symEnd int
	var sourceID uint16
	switch schemaVersion {
	case mboSchemaVersionV1:
		if len(buf) != instDefBodyLenV1 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 1 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV1, len(buf))
		}
		// v1 carries no Source ID; sourceID stays 0 (registry Unknown).
		symStart, symEnd = 4, 20
	case mboSchemaVersionV3:
		if len(buf) != instDefBodyLenV3 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 3 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV3, len(buf))
		}
		sourceID = binary.LittleEndian.Uint16(buf[4:6])
		symStart, symEnd = 6, 70
	default:
		return InstrumentDefinitionBody{}, fmt.Errorf("%w: %d", errSchemaVersion, schemaVersion)
	}

	// Every field after Symbol is at a fixed offset from the end of Symbol, which
	// is what makes one body of code serve both layouts.
	o := symEnd
	return InstrumentDefinitionBody{
		InstrumentID:  binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:      sourceID,
		Symbol:        fixedString(buf[symStart:symEnd]),
		Leg1:          fixedString(buf[o : o+8]),
		Leg2:          fixedString(buf[o+8 : o+16]),
		AssetClass:    buf[o+16],
		PriceExponent: int8(buf[o+17]),
		QtyExponent:   int8(buf[o+18]),
		MarketModel:   buf[o+19],
		TickSizeRaw:   int64(binary.LittleEndian.Uint64(buf[o+20 : o+28])),
		LotSizeRaw:    binary.LittleEndian.Uint64(buf[o+28 : o+36]),
		ContractValue: binary.LittleEndian.Uint64(buf[o+36 : o+44]),
		Expiry:        readTSNs(buf[o+44 : o+52]),
		SettleType:    buf[o+52],
		PriceBound:    buf[o+53],
		ManifestSeq:   binary.LittleEndian.Uint16(buf[o+54 : o+56]),
	}, nil
}
```

Also update the header check at `marketbyorder_wire.go:91` if the Step 1 sed left it stale — it must read:

```go
	if h.SchemaVersion != mboSchemaVersionV1 && h.SchemaVersion != mboSchemaVersionV3 {
		return h, errSchemaVersion
	}
```

- [ ] **Step 6: Carry `source_id` on the record**

In `marketbyorder.go`, add one entry to the `instrument_definition` `Fields` map, immediately after `symbol`:

```go
		base.Fields = map[string]any{
			"symbol":         b.Symbol,
			"source_id":      b.SourceID,
			"leg1":           b.Leg1,
```

- [ ] **Step 7: Run the full module test suite**

Run: `cd go/marketbyorder-parser && go test ./... -count=1`
Expected: PASS

- [ ] **Step 8: Update the README**

In `go/marketbyorder-parser/README.md`, replace the "Dual wire schema support" paragraph (line 9) with:

```markdown
**Dual wire schema support.** `InstrumentDefinition` is decoded at schema versions 1 and 3, selected per frame from the frame header's Schema Version byte — not a build-time or CLI setting. v3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes; all other fields are unchanged. There is no version 2: that layout was specified upstream and superseded before any publisher emitted it, so the accepted versions are the set `{1, 3}` and version 2 is rejected exactly like version 0. A frame whose declared Schema Version disagrees with the length its `InstrumentDefinition` body actually carries is counted malformed (as a frame-level `parse_errors_total{reason="truncated"}`) and the whole frame is skipped, not guessed at. `frames_total{port,schema_version}` (see [Metrics](#metrics)) is how to watch a publisher's v1-to-v3 cutover in production.
```

And the metrics table row (line 29):

```markdown
| `dz_mbo_parser_frames_total{port,schema_version}` | counter | Successfully parsed frames, by port and wire Schema Version. The way to watch a publisher's v1-to-v3 cutover: `schema_version="3"` climbing while `schema_version="1"` goes flat, then to zero, is when the v1 decode path can be retired. `schema_version="2"` should never appear; a nonzero count there means a publisher is emitting a version this parser believes does not exist |
```

- [ ] **Step 9: Update the metrics test**

In `go/marketbyorder-parser/metrics_test.go`, change every `"2"` label to `"3"`, the comment on line 7 from `v1-to-v2` to `v1-to-v3`, and the error message on line 19 from `v2 frames` to `v3 frames`.

Run: `cd go/marketbyorder-parser && go test ./... -count=1`
Expected: PASS

- [ ] **Step 10: Verify no v2 references remain in this module**

Run: `grep -rn 'V2\|v2\|version 2\|124\|128' go/marketbyorder-parser/ --include='*.go' --include='*.md'`
Expected: only the deliberate "there is no version 2" / "version 2 is rejected" prose in the decoder comment and README, and the `{0, 2, 4, 255}` rejection cases in tests. No `instDefBodyLenV2`, no `buildInstDefV2`, no 124-byte or 128-byte layout.

- [ ] **Step 11: Commit**

```bash
git add go/marketbyorder-parser/
git commit -m "marketbyorder-parser: decode instrument definitions at schema v1 and v3"
```

---

### Task 2: `marketbyprice-parser` decodes v1 and v3

This module's `InstrumentDefinitionBody` and `ParseInstrumentDefinition` are byte-identical to `marketbyorder-parser`'s. The edit is the same one again, with `mbp` prefixes. The code is repeated in full below rather than referenced, because this task may be implemented without Task 1 in context.

**Files:**
- Modify: `go/marketbyprice-parser/marketbyprice_wire.go:24-25` (version constants), `:106` (header check), `:209` (`InstrumentDefinitionBody` struct), `:227-283` (`ParseInstrumentDefinition` and its length constants)
- Modify: `go/marketbyprice-parser/marketbyprice.go:155-175` (record `Fields` map)
- Modify: `go/marketbyprice-parser/README.md:7,92`
- Test: `go/marketbyprice-parser/marketbyprice_wire_test.go:572-730`, `go/marketbyprice-parser/metrics_smoke_test.go:71-90`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `InstrumentDefinitionBody.SourceID uint16`; record `Fields["source_id"]` holding a `uint16`. Task 4 reads that map key.

- [ ] **Step 1: Rename the v1 constant for symmetry**

```bash
cd go/marketbyprice-parser
grep -rl 'mbpSchemaVersion' . | xargs sed -i '' -E 's/mbpSchemaVersionV2/mbpSchemaVersionV3/g; s/mbpSchemaVersion([^V0-9])/mbpSchemaVersionV1\1/g'
grep -rn 'mbpSchemaVersion' . | grep -v 'mbpSchemaVersionV1\|mbpSchemaVersionV3'
```

**Do not use `\b`.** BSD `sed` (macOS) does not support it — a `\b` version exits 0 and silently renames nothing. The second command must print no output; anything it prints is an occurrence the substitution missed, to be fixed by hand.

Then fix the two declarations in `marketbyprice_wire.go:24-25`:

```go
	mbpSchemaVersionV1 uint8  = 1 // v1: InstrumentDefinition, 76-byte body (80-byte message)
	mbpSchemaVersionV3 uint8  = 3 // v3: InstrumentDefinition, 126-byte body (130-byte message)
```

Confirm it builds: `go build ./...`

- [ ] **Step 2: Write the failing tests**

Replace `buildInstDefV2` and the v2 tests in `marketbyprice_wire_test.go` with:

```go
// buildInstDefV3 builds a 126-byte v3 body. Source ID is inserted at 4:6 and
// Symbol widens to 64 bytes, so every field after Instrument ID sits 50 bytes
// later than in v1.
func buildInstDefV3(symbol string) []byte {
	b := make([]byte, 126)
	binary.LittleEndian.PutUint32(b[0:4], 4242)
	binary.LittleEndian.PutUint16(b[4:6], 77) // source id
	copy(b[6:70], symbol)
	copy(b[70:78], "BTC")
	copy(b[78:86], "USDT")
	b[86] = 1
	priceExp, qtyExp := int8(-2), int8(-8)
	b[87] = byte(priceExp)
	b[88] = byte(qtyExp)
	b[89] = 1
	binary.LittleEndian.PutUint64(b[90:98], 50)
	binary.LittleEndian.PutUint64(b[98:106], 100)
	binary.LittleEndian.PutUint64(b[106:114], 1000)
	binary.LittleEndian.PutUint64(b[114:122], 1700000000)
	b[122] = 1
	b[123] = 2
	binary.LittleEndian.PutUint16(b[124:126], 7)
	return b
}

// The v3 symbol MUST exceed 16 bytes, or this test proves nothing a v1 test
// does not. This is the whole point of the widening: a Kalshi ticker like
// KXNFLGAME-26SEP13NYJTEN-NYJ is 27 bytes and was previously truncated.
func TestParseInstrumentDefinition_V3LongSymbol(t *testing.T) {
	const long = "KXNFLGAME-26SEP13NYJTEN-NYJ"
	if len(long) <= 16 {
		t.Fatal("fixture symbol must exceed 16 bytes to be meaningful")
	}
	got, err := ParseInstrumentDefinition(buildInstDefV3(long), 3)
	if err != nil {
		t.Fatal(err)
	}
	assertInstDefFields(t, got, long)
	if got.SourceID != 77 {
		t.Errorf("source id: got %d want 77", got.SourceID)
	}
}

// v1 has no Source ID on the wire. It must decode as 0 (registry Unknown)
// rather than picking up the first two bytes of Symbol.
func TestParseInstrumentDefinition_V1SourceIDIsZero(t *testing.T) {
	got, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if got.SourceID != 0 {
		t.Errorf("v1 source id: got %d want 0", got.SourceID)
	}
}

// A symbol filling all 64 bytes has no null terminator; it must not be truncated
// or run past the field.
func TestParseInstrumentDefinition_V3SymbolFillsField(t *testing.T) {
	full := strings.Repeat("A", 64)
	got, err := ParseInstrumentDefinition(buildInstDefV3(full), 3)
	if err != nil {
		t.Fatal(err)
	}
	if got.Symbol != full {
		t.Errorf("symbol length: got %d want 64", len(got.Symbol))
	}
	if got.Leg1 != "BTC" {
		t.Errorf("a full-width symbol must not bleed into Leg1: got %q", got.Leg1)
	}
}

// The declared version and the body length must agree. A v3 frame carrying a v1
// body would otherwise read Source ID and Symbol across 66 bytes of adjacent
// fields and produce plausible garbage instead of an error.
func TestParseInstrumentDefinition_LengthMustMatchVersion(t *testing.T) {
	if _, err := ParseInstrumentDefinition(buildInstDefV1("BTC-USDT"), 3); err == nil {
		t.Error("version 3 with a 76-byte body must be rejected")
	}
	if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), 1); err == nil {
		t.Error("version 1 with a 126-byte body must be rejected")
	}
}

// Version 2 was specified and superseded before any publisher emitted it. It is
// not a layout this decoder implements, and it must be rejected as firmly as a
// version that never existed at all — a version ceiling would let it through.
func TestParseInstrumentDefinition_UnsupportedVersion(t *testing.T) {
	for _, v := range []uint8{0, 2, 4, 255} {
		if _, err := ParseInstrumentDefinition(buildInstDefV3("BTC-USDT"), v); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}

// The frame header accepts both implemented versions and nothing else.
func TestParseFrameHeader_AcceptsV1AndV3(t *testing.T) {
	ts := time.Unix(1700000000, 0)
	for _, v := range []uint8{1, 3} {
		buf := buildFrameHeader(mbpMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err != nil {
			t.Errorf("schema version %d must be accepted: %v", v, err)
		}
	}
	for _, v := range []uint8{0, 2, 4, 255} {
		buf := buildFrameHeader(mbpMagic, v, 0, 1, ts, 1, 0, frameHeaderSize)
		if _, err := ParseFrameHeader(buf); err == nil {
			t.Errorf("schema version %d must be rejected", v)
		}
	}
}
```

In `TestParseFrame_FollowsVersionSwitchMidStream`, swap the cutover frame:

```go
	v1Frame := build(1, buildInstDefV1("SHORT"))
	v3Frame := build(3, buildInstDefV3("KXNFLGAME-26SEP13NYJTEN-NYJ"))

	for i, tc := range []struct {
		frame []byte
		want  string
	}{
		{v1Frame, "SHORT"},
		{v3Frame, "KXNFLGAME-26SEP13NYJTEN-NYJ"},
		{v1Frame, "SHORT"}, // and back again
	} {
```

Update the comment above it from "v1 to v2" to "v1 to v3".

- [ ] **Step 3: Run tests to verify they fail**

Run: `cd go/marketbyprice-parser && go test ./... -run 'InstrumentDefinition|VersionSwitch|AcceptsV1AndV3|SourceID' -count=1`
Expected: FAIL — compile error on `got.SourceID` (undefined field).

- [ ] **Step 4: Add `SourceID` to the body struct**

In `marketbyprice_wire.go`:

```go
type InstrumentDefinitionBody struct {
	InstrumentID  uint32
	SourceID      uint16 // 0 at schema v1, which carries no Source ID
	Symbol        string
	Leg1          string
```

- [ ] **Step 5: Rewrite the length constants and decoder**

```go
// InstrumentDefinition body lengths, excluding the 4-byte message header.
//
// v3 inserts Source ID (u16) after Instrument ID and widens Symbol from
// char[16] to char[64], shifting every field after Instrument ID by 50 bytes.
// Nothing else in this feed changed between the two schema versions.
//
// There is no version 2. A 128-byte layout carrying the widened Symbol without
// Source ID was specified upstream and superseded before any publisher emitted
// it, so version 2 is rejected here rather than decoded.
const (
	instDefBodyLenV1 = 76
	instDefBodyLenV3 = 126
)

// ParseInstrumentDefinition decodes an InstrumentDefinition body using the
// layout for the frame's schema version.
//
// The body length cross-checks the declared version. They can only disagree if a
// publisher bumped the header without the payload or the reverse, and the
// mismatch must be caught here: decoding a v1 body under the v3 layout would
// read Source ID and Symbol across 66 bytes of adjacent fields and yield a
// plausible-looking instrument rather than an error.
func ParseInstrumentDefinition(buf []byte, schemaVersion uint8) (InstrumentDefinitionBody, error) {
	var symStart, symEnd int
	var sourceID uint16
	switch schemaVersion {
	case mbpSchemaVersionV1:
		if len(buf) != instDefBodyLenV1 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 1 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV1, len(buf))
		}
		// v1 carries no Source ID; sourceID stays 0 (registry Unknown).
		symStart, symEnd = 4, 20
	case mbpSchemaVersionV3:
		if len(buf) != instDefBodyLenV3 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 3 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV3, len(buf))
		}
		sourceID = binary.LittleEndian.Uint16(buf[4:6])
		symStart, symEnd = 6, 70
	default:
		return InstrumentDefinitionBody{}, fmt.Errorf("%w: %d", errSchemaVersion, schemaVersion)
	}

	// Every field after Symbol is at a fixed offset from the end of Symbol, which
	// is what makes one body of code serve both layouts.
	o := symEnd
	return InstrumentDefinitionBody{
		InstrumentID:  binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:      sourceID,
		Symbol:        fixedString(buf[symStart:symEnd]),
		Leg1:          fixedString(buf[o : o+8]),
		Leg2:          fixedString(buf[o+8 : o+16]),
		AssetClass:    buf[o+16],
		PriceExponent: int8(buf[o+17]),
		QtyExponent:   int8(buf[o+18]),
		MarketModel:   buf[o+19],
		TickSizeRaw:   int64(binary.LittleEndian.Uint64(buf[o+20 : o+28])),
		LotSizeRaw:    binary.LittleEndian.Uint64(buf[o+28 : o+36]),
		ContractValue: binary.LittleEndian.Uint64(buf[o+36 : o+44]),
		Expiry:        readTSNs(buf[o+44 : o+52]),
		SettleType:    buf[o+52],
		PriceBound:    buf[o+53],
		ManifestSeq:   binary.LittleEndian.Uint16(buf[o+54 : o+56]),
	}, nil
}
```

Confirm the header check at `marketbyprice_wire.go:106` reads:

```go
	if h.SchemaVersion != mbpSchemaVersionV1 && h.SchemaVersion != mbpSchemaVersionV3 {
		return h, errSchemaVersion
	}
```

- [ ] **Step 6: Carry `source_id` on the record**

In `marketbyprice.go`, in the `instrument_definition` branch, add to the `Fields` map immediately after `symbol`:

```go
			"source_id":      b.SourceID,
```

- [ ] **Step 7: Run the full module test suite**

Run: `cd go/marketbyprice-parser && go test ./... -count=1`
Expected: PASS

Note: `marketbyprice_wire_test.go:291` has a table entry passing `mbpSchemaVersionV1` with a 76-byte body. The Step 1 sed renames it; the length is still correct for v1, so it needs no other change.

- [ ] **Step 8: Update the README**

Replace the "Dual wire schema support" paragraph (line 7):

```markdown
**Dual wire schema support.** `InstrumentDefinition` is decoded at schema versions 1 and 3, selected per frame from the frame header's Schema Version byte — not a build-time or CLI setting. v3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes; all other fields are unchanged. There is no version 2: that layout was specified upstream and superseded before any publisher emitted it, so the accepted versions are the set `{1, 3}` and version 2 is rejected exactly like version 0. A frame whose declared Schema Version disagrees with the length its `InstrumentDefinition` body actually carries is counted malformed (as a frame-level `parse_errors_total{reason="truncated"}`) and the whole frame is skipped, not guessed at. `frames_total{port,schema_version}` (see [Metrics](#metrics)) is how to watch a publisher's v1-to-v3 cutover in production.
```

And the metrics table row (line 92):

```markdown
| `dz_mbp_parser_frames_total{port,schema_version}` | counter | Successfully parsed frames, by port and wire Schema Version. The way to watch a publisher's v1-to-v3 cutover: `schema_version="3"` climbing while `schema_version="1"` goes flat, then to zero, is when the v1 decode path can be retired. `schema_version="2"` should never appear; a nonzero count there means a publisher is emitting a version this parser believes does not exist |
```

- [ ] **Step 9: Update the metrics smoke test**

In `go/marketbyprice-parser/metrics_smoke_test.go`, change every `"2"` label to `"3"`, the comment on line 71 from `v1-to-v2` to `v1-to-v3`, and the error message on line 88 from `v2 frames` to `v3 frames`.

Run: `cd go/marketbyprice-parser && go test ./... -count=1`
Expected: PASS

- [ ] **Step 10: Verify no v2 references remain in this module**

Run: `grep -rn 'V2\|v2\|version 2\|124\|128' go/marketbyprice-parser/ --include='*.go' --include='*.md'`
Expected: only the deliberate "there is no version 2" prose and the `{0, 2, 4, 255}` rejection cases.

- [ ] **Step 11: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: decode instrument definitions at schema v1 and v3"
```

---

### Task 3: `topofbook-parser` decodes v1 and v3

This parser differs from the other two in two ways, and both matter:

1. It uses a **sequential byte reader** (`br.u32()`, `br.bytes(n)`) rather than explicit offsets, so the `Source ID` read is a conditional insertion in the read sequence rather than an offset change.
2. Its header validation uses a **version ceiling** (`SchemaVersion > maxSchemaVersion`). Raising that ceiling to 3 would admit version 2 frames into the decoder, where they would fail later on a length mismatch, in the wrong error bucket. The ceiling must become explicit set membership.

**Files:**
- Modify: `go/topofbook-parser/tob/topofbook_wire.go:39-49` (length constants), `:88-90` (`topOfBookInstrumentDef` struct), `:305-345` (`msgInstrumentDefinition` decode branch)
- Modify: `go/topofbook-parser/tob/topofbook.go:71-73` (version constant), `:118-122` (`validateHeader`), `:160-208` (`handleInstrumentDef` record `Fields`)
- Modify: `go/topofbook-parser/README.md:7,155`, `go/topofbook-parser/CLAUDE.md:62`
- Test: `go/topofbook-parser/tob/topofbook_wire_test.go`, `go/topofbook-parser/metrics_test.go`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: `topOfBookInstrumentDef.SourceID uint16`; record `Fields["source_id"]` holding a `uint16`. Task 4 reads that map key.

- [ ] **Step 1: Write the failing tests**

In `go/topofbook-parser/tob/topofbook_wire_test.go`, replace `buildInstDefBodyV2` with:

```go
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
```

Replace `buildInstrumentDefMsgV2` with the v3 equivalent (same wrapper, new body builder and length):

```go
func buildInstrumentDefMsgV3(instID uint32, symbol, leg1, leg2 string) []byte {
	body := buildInstDefBodyV3(instID, symbol, leg1, leg2)
	msg := make([]byte, 4, 4+len(body))
	msg[0] = msgInstrumentDefinition
	msg[1] = uint8(4 + len(body))
	return append(msg, body...)
}
```

Replace the v2 decode tests with:

```go
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
```

In `TestParse_FollowsVersionSwitchMidStream`, replace every `buildInstrumentDefMsgV2` call with `buildInstrumentDefMsgV3` and every schema version `2` with `3`, and update the function's comment from "v1 to v2" to "v1 to v3".

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd go/topofbook-parser && go test ./... -run 'InstrumentDef|ValidateHeader|VersionSwitch|SourceID' -count=1`
Expected: FAIL — compile error on `def.SourceID` (undefined field).

- [ ] **Step 3: Add `SourceID` to the body struct**

In `tob/topofbook_wire.go`, in `topOfBookInstrumentDef`:

```go
type topOfBookInstrumentDef struct {
	InstrumentID  uint32
	SourceID      uint16 // 0 at schema v1, which carries no Source ID
	Symbol        string // fixed 16 bytes (schema v1) or 64 bytes (v3), null-padded ASCII
	Leg1          string // fixed 8 bytes
```

- [ ] **Step 4: Rewrite the length constants**

Replace the constant block at `tob/topofbook_wire.go:39-49`:

```go
// InstrumentDefinition body lengths and Symbol widths, excluding the 4-byte
// message header. v3 inserts Source ID (u16) after Instrument ID and widens
// Symbol from char[16] to char[64]; every field after Instrument ID shifts by
// 50 bytes. Named to match instDefBodyLenV1/V3 in the marketbyorder and
// marketbyprice siblings, whose version constants live beside these;
// topofbook's accepted-version set (schemaVersionV1/schemaVersionV3) lives in
// topofbook.go.
//
// There is no version 2. A 128-byte layout carrying the widened Symbol without
// Source ID was specified upstream and superseded before any publisher emitted
// it, so version 2 is rejected here rather than decoded.
const (
	instDefSymLenV1  = 16
	instDefSymLenV3  = 64
	instDefBodyLenV1 = 76
	instDefBodyLenV3 = 126
)
```

- [ ] **Step 5: Rewrite the decode branch**

Replace the `case msgInstrumentDefinition:` branch in `decodeTopOfBookBody`:

```go
	case msgInstrumentDefinition:
		// v3 inserts Source ID (u16) after Instrument ID and widens Symbol from
		// char[16] to char[64]; every later field shifts by 50 bytes. The body
		// length cross-checks the declared version, because reading a v1 body
		// under the v3 layout would consume adjacent fields as source and symbol
		// bytes and yield a plausible instrument rather than an error.
		var symLen, wantLen int
		hasSourceID := false
		switch schemaVersion {
		case schemaVersionV1:
			symLen, wantLen = instDefSymLenV1, instDefBodyLenV1
		case schemaVersionV3:
			symLen, wantLen = instDefSymLenV3, instDefBodyLenV3
			hasSourceID = true
		default:
			// Matches the siblings (marketbyorder, marketbyprice): an unsupported
			// version is rejected here rather than falling through to the v1
			// layout. This includes version 2, which was specified upstream and
			// superseded before any publisher emitted it. Call order in Parse
			// means validateHeader's accepted-version check would otherwise
			// catch this frame too, but nothing should depend on that ordering —
			// this decoder must be correct on its own.
			return nil, fmt.Errorf("instrument_definition: unsupported schema version %d", schemaVersion)
		}
		if len(buf) != wantLen {
			// Deliberately does not use the word "schema" — classifyParseErr in
			// runner.go buckets on substrings, and this must land in the same
			// "truncated" bucket as the identical fault on marketbyorder and
			// marketbyprice, not in "schema_version" alongside the unsupported-
			// version error above.
			return nil, fmt.Errorf("instrument_definition: truncated: expected %d bytes at wire version %d, got %d",
				wantLen, schemaVersion, len(buf))
		}
		var b topOfBookInstrumentDef
		b.InstrumentID = br.u32()
		if hasSourceID {
			b.SourceID = br.u16()
		}
		b.Symbol = string(br.bytes(symLen))
		b.Leg1 = string(br.bytes(8))
		b.Leg2 = string(br.bytes(8))
		b.AssetClass = br.u8()
		b.PriceExponent = br.i8()
		b.QtyExponent = br.i8()
		b.MarketModel = br.u8()
		b.TickSize = br.i64()
		b.LotSize = br.u64()
		b.ContractValue = br.u64()
		b.Expiry = br.u64()
		b.SettleType = br.u8()
		b.PriceBound = br.u8()
		b.ManifestSeq = br.u16()
		if br.err != nil {
			return nil, br.err
		}
		return &b, nil
```

- [ ] **Step 6: Replace the version ceiling with set membership**

This is the structural change. In `tob/topofbook.go`, replace `maxSchemaVersion` in the constant block at line 71-73:

```go
const (
	frameHeaderSize = 24

	// Accepted wire schema versions, as a set rather than a range. There is no
	// version 2: a 128-byte InstrumentDefinition carrying the widened Symbol
	// without Source ID was specified upstream and superseded before any
	// publisher emitted it. A ceiling check (version <= max) would admit those
	// frames into the decoder, where they would fail later on a length mismatch
	// and be counted as truncation rather than as an unsupported version.
	schemaVersionV1 = 1
	schemaVersionV3 = 3

	maxReasonableMsgs = 200
```

And in `validateHeader`:

```go
	if h.SchemaVersion != schemaVersionV1 && h.SchemaVersion != schemaVersionV3 {
		return fmt.Errorf("unsupported schema version %d (expected %d or %d)",
			h.SchemaVersion, schemaVersionV1, schemaVersionV3)
	}
```

- [ ] **Step 7: Carry `source_id` on the record**

In `tob/topofbook.go`, in `handleInstrumentDef`, add one entry to the `Fields` map, first in the list to mirror the wire order:

```go
		Fields: map[string]any{
			"source_id":      body.SourceID,
			"leg1":           trimNull(body.Leg1),
			"leg2":           trimNull(body.Leg2),
```

- [ ] **Step 8: Run the full module test suite**

Run: `cd go/topofbook-parser && go test ./... -count=1`
Expected: PASS

If `runner_test.go`'s `TestClassifyParseErr_PinsReasons` fails, check its `buildInstDefBody76` helper and the schema versions it feeds: its "unsupported schema version at instrument_definition decode" case must now use a version in `{0, 2, 4, 255}`, and its "caught by validateHeader" case likewise.

- [ ] **Step 9: Update the metrics test**

In `go/topofbook-parser/metrics_test.go`, change every `"2"` schema-version label to `"3"` and any `v1-to-v2` / `v2 frames` wording to `v1-to-v3` / `v3 frames`.

Run: `cd go/topofbook-parser && go test ./... -count=1`
Expected: PASS

- [ ] **Step 10: Update the docs**

`go/topofbook-parser/README.md` line 7:

```markdown
**Dual wire schema support.** `InstrumentDefinition` is decoded at schema versions 1 and 3, selected per frame from the frame header's Schema Version byte — not a build-time or CLI setting. v3 inserts `Source ID` (`u16`) after `Instrument ID` and widens `Symbol` from 16 to 64 bytes; all other fields are unchanged. There is no version 2: that layout was specified upstream and superseded before any publisher emitted it, so the accepted versions are the set `{1, 3}` and version 2 is rejected exactly like version 0. A frame whose declared Schema Version disagrees with the length its `InstrumentDefinition` body actually carries is counted and skipped (as `parse_errors_total{reason="truncated"}`, matching marketbyorder and marketbyprice), not guessed at. An unsupported Schema Version itself is a separate fault and lands in `parse_errors_total{reason="schema_version"}`. `frames_total{port,schema_version}` (see [Metrics](#metrics)) is how to watch a publisher's v1-to-v3 cutover in production.
```

`go/topofbook-parser/README.md` line 155:

```markdown
| `frames_total` | counter | `port`, `schema_version` | Successfully parsed frames, by port and wire Schema Version. The way to watch a publisher's v1-to-v3 cutover: `schema_version="3"` climbing while `schema_version="1"` goes flat, then to zero, is when the v1 decode path can be retired. `schema_version="2"` should never appear; a nonzero count there means a publisher is emitting a version this parser believes does not exist |
```

`go/topofbook-parser/CLAUDE.md` line 62:

```markdown
| 0x02 | InstrumentDefinition | 80 (v1) / 130 (v3) | refdata | instrument_id → source_id, symbol, price/qty exponents. Both lengths are exact, matching the Schema Version in the frame header — a frame whose declared version disagrees with the message length it actually carries is rejected, not guessed at. There is no version 2 |
```

- [ ] **Step 11: Verify no v2 references remain in this module**

Run: `grep -rn 'V2\|v2\|version 2\|124\|128\|maxSchemaVersion' go/topofbook-parser/ --include='*.go' --include='*.md'`
Expected: only the deliberate "there is no version 2" prose and the `{0, 2, 4, 255}` rejection cases. `maxSchemaVersion` must be gone entirely.

- [ ] **Step 12: Commit**

```bash
git add go/topofbook-parser/
git commit -m "topofbook-parser: decode instrument definitions at schema v1 and v3"
```

---

### Task 4: Bots and ClickHouse store `source_id`

**Read this before writing any test in this task.** Records cross the parser/bot boundary as JSON, so a `uint16` written into `Fields` by a parser arrives at a bot as a **`float64`**. Every existing bot accessor already handles this — `toUint16` switches on `float64`, and `topofbook-bot`'s `uintOrZero` reads through `floatField`, which accepts `float64` only. That is why no new accessor is needed anywhere in this task. A type assertion to `uint16` in a bot would compile, pass a hand-built unit test, and return `0` for every instrument in production. The existing `instDefRec` test helper in `go/marketbyprice-bot/dispatch_test.go` models this correctly: it stores `float64(manifestSeq)`, not `uint16`. Match it.

**Files:**
- Modify: `demo/clickhouse/init/01_schema.sql:78-85`, `demo/clickhouse/init/02_schema_mbo.sql:4-24`, `demo/clickhouse/init/03_schema_mbp.sql:5-25`
- Modify: `go/topofbook-bot/clickhouse.go:144-154` (`EnqueueInstrument`)
- Modify: `go/marketbyorder-bot/events_writer.go:9-15` (widen `NewEventsWriter` to the existing `enqueuer` interface so it is testable) and `:27-45`
- Modify: `go/marketbyprice-bot/events_writer.go:36-59`
- Test: `go/topofbook-bot/clickhouse_test.go`, `go/marketbyorder-bot/events_writer_test.go`, `go/marketbyprice-bot/events_writer_test.go`, `go/marketbyprice-bot/dispatch_test.go`

**Interfaces:**
- Consumes: record `Fields["source_id"]`, produced by Tasks 1, 2, and 3 as a `uint16` and seen by these bots as a `float64`.
- Produces: a `source_id` key on each `instruments` row.

- [ ] **Step 1: Add the ClickHouse columns**

`demo/clickhouse/init/01_schema.sql`, in `topofbook.instruments`:

```sql
CREATE TABLE IF NOT EXISTS topofbook.instruments (
    recv_ts             DateTime64(9),
    instrument_id       UInt32,
    source_id           UInt16 DEFAULT 0,
    symbol              LowCardinality(String),
    price_exponent      Int8,
    qty_exponent        Int8
) ENGINE = ReplacingMergeTree(recv_ts)
  ORDER BY (instrument_id);
```

`demo/clickhouse/init/02_schema_mbo.sql`, in `marketbyorder.instruments`, and `demo/clickhouse/init/03_schema_mbp.sql`, in `marketbyprice.instruments`, add the same column after `instrument_id`:

```sql
    instrument_id    UInt32,
    source_id        UInt16 DEFAULT 0,
    symbol           LowCardinality(String),
```

The `ORDER BY` keys are deliberately untouched. These are `ReplacingMergeTree` tables, so a row written at v1 with `source_id = 0` is replaced by the v3 row carrying the real venue on the next merge.

- [ ] **Step 2: Note the migration for existing demo volumes**

These are `CREATE TABLE IF NOT EXISTS` init scripts, so they only apply to a fresh ClickHouse volume. Add this note directly under the `topofbook.instruments` definition in `01_schema.sql`:

```sql
-- Added with feed schema v3. An existing volume predating that column needs
--   ALTER TABLE <db>.instruments ADD COLUMN source_id UInt16 DEFAULT 0 AFTER instrument_id;
-- on each of the three instruments tables, or a volume wipe (docker compose down -v).
```

- [ ] **Step 3: Write the failing marketbyprice-bot test**

`go/marketbyprice-bot/events_writer_test.go` already has a `stubEnqueuer` (with `newStubEnqueuer` and `only`) and a `TestEventsWriter_InstrumentDefinition`. Extend the shared `instDefRec` helper in `go/marketbyprice-bot/dispatch_test.go` to carry a source ID, keeping the `float64` convention the rest of that helper uses:

```go
func instDefRec(instID uint32, symbol string, manifestSeq uint16) Record {
	return Record{
		Type:         "instrument_definition",
		Port:         "refdata",
		InstrumentID: instID,
		Fields: map[string]any{
			"symbol": symbol,
			// float64, not uint16: records reach the bot as decoded JSON, so
			// this is the type the production path actually sees.
			"source_id":      float64(77),
			"price_exponent": float64(-2),
			"qty_exponent":   float64(-8),
			"manifest_seq":   float64(manifestSeq),
		},
	}
}
```

Then add to `events_writer_test.go`:

```go
func TestEventsWriter_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       instDefRec(11, "BTC-USDT", 5),
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["source_id"] != uint16(77) {
		t.Errorf("source_id: got %v (%T) want uint16(77)", row["source_id"], row["source_id"])
	}
}

// A v1 definition carries no Source ID at all. The row must still be written,
// with the registry's Unknown value rather than being dropped.
func TestEventsWriter_InstrumentDefinitionWithoutSourceID(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := instDefRec(11, "BTC-USDT", 5)
	delete(rec.Fields, "source_id")
	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       rec,
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["source_id"] != uint16(0) {
		t.Errorf("source_id: got %v want uint16(0)", row["source_id"])
	}
}
```

- [ ] **Step 4: Write the failing marketbyorder-bot test**

`go/marketbyorder-bot/events_writer_test.go` has no enqueuer stub, but two things already exist in that package: `captureWriter` in `snapshot_writer_test.go`, and the `enqueuer` interface it satisfies, declared in `snapshot_writer.go:10`. Reuse both rather than adding a second stub.

`NewEventsWriter` currently takes a concrete `*ClickhouseClient`. Widen it to `enqueuer`, matching what `SnapshotWriter` in the same package and `EventsWriter` in marketbyprice-bot already do:

```go
type EventsWriter struct {
	ch enqueuer
}

func NewEventsWriter(ch enqueuer) *EventsWriter {
	return &EventsWriter{ch: ch}
}
```

The roughly twenty existing `NewEventsWriter(nil)` call sites in `coordinator_test.go`, `shard_test.go`, and `parity_test.go` keep working: untyped `nil` is assignable to an interface parameter and leaves the interface itself nil, so the `if w.ch == nil` guard at the top of `Write` behaves exactly as before. `main.go:73` passes a `*ClickhouseClient`, which satisfies `enqueuer`.

`ChannelEvent` is `{Kind string, InstrumentID uint32, Symbol string, Record Record}`, and `Write` switches on `rec.Type` rather than `ev.Kind`, so only `Record` needs to be set:

```go
func TestEventsWriter_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	cw := &captureWriter{}
	w := NewEventsWriter(cw)

	w.Write(ChannelEvent{
		InstrumentID: 4242,
		Record: Record{
			Type:         "instrument_definition",
			InstrumentID: 4242,
			Fields: map[string]any{
				"symbol": "BTC-USDT",
				// float64, not uint16: records reach the bot as decoded JSON.
				"source_id": float64(77),
			},
		},
	}, 0, "BTC-USDT", -2, -8)

	rows := cw.captured()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["source_id"] != uint16(77) {
		t.Errorf("source_id: got %v (%T) want uint16(77)", rows[0]["source_id"], rows[0]["source_id"])
	}
}
```

- [ ] **Step 5: Write the failing topofbook-bot test**

`go/topofbook-bot/clickhouse.go` already has pure row builders `buildQuoteRow` and `buildTradeRow`, tested directly. `EnqueueInstrument` builds its row inline instead. Extract it to match the file's own pattern, which is what makes it testable:

```go
// EnqueueInstrument serializes an InstrumentDefinition into the instruments batcher.
func (w *chWriter) EnqueueInstrument(rec *Record, recvTime time.Time) {
	w.submit("instruments", buildInstrumentRow(rec, recvTime))
}

func buildInstrumentRow(rec *Record, recvTime time.Time) map[string]any {
	return map[string]any{
		"recv_ts":       chTime(recvTime),
		"instrument_id": rec.InstrumentID,
		// uintOrZero, not intOrZero: intOrZero returns int64 and serves the
		// signed exponents below, and an unsigned venue ID must not be able to
		// arrive sign-extended. Both read through floatField, because records
		// reach this bot as decoded JSON.
		"source_id":      uintOrZero(rec, "source_id"),
		"symbol":         rec.Symbol,
		"price_exponent": intOrZero(rec, "price_exponent"),
		"qty_exponent":   intOrZero(rec, "qty_exponent"),
	}
}
```

Then in `go/topofbook-bot/clickhouse_test.go`, mirroring `TestBuildQuoteRow_WritesSourceSendRecvColumns`:

```go
func TestBuildInstrumentRow_CarriesSourceID(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	rec := &Record{
		InstrumentID: 4242,
		Symbol:       "BTC-USDT",
		// float64, not uint16: records reach this bot as decoded JSON.
		Fields: map[string]any{"source_id": float64(77)},
	}
	row := buildInstrumentRow(rec, now)
	if row["source_id"] != uint64(77) {
		t.Errorf("source_id: got %v (%T) want uint64(77)", row["source_id"], row["source_id"])
	}
}

// A v1 definition carries no Source ID at all. The row must still be built,
// with the registry's Unknown value.
func TestBuildInstrumentRow_WithoutSourceID(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	rec := &Record{InstrumentID: 4242, Symbol: "BTC-USDT", Fields: map[string]any{}}
	row := buildInstrumentRow(rec, now)
	if row["source_id"] != uint64(0) {
		t.Errorf("source_id: got %v want uint64(0)", row["source_id"])
	}
}
```

`uintOrZero` returns `uint64`, which is why the assertion is `uint64(77)`. That marshals into a ClickHouse `UInt16` column without issue; do not add a narrowing conversion just to make the assertion read `uint16`.

- [ ] **Step 6: Run tests to verify they fail**

```bash
cd go/marketbyprice-bot && go test ./... -run SourceID -count=1
cd ../marketbyorder-bot && go test ./... -run SourceID -count=1
cd ../topofbook-bot && go test ./... -run SourceID -count=1
```
Expected: all FAIL — `source_id` missing from the row map (and, for topofbook-bot, `buildInstrumentRow` undefined).

- [ ] **Step 7: Write `source_id` from the marketbyorder and marketbyprice bots**

In `go/marketbyorder-bot/events_writer.go`, in the `instrument_definition` branch, add immediately after `"instrument_id"`:

```go
			"source_id":      getUint16(rec.Fields, "source_id"),
```

Make the identical edit in `go/marketbyprice-bot/events_writer.go`'s `instrument_definition` branch. Both modules already have `getUint16`, backed by a `toUint16` that switches on `float64` as well as `uint16`; do not add a new accessor.

The topofbook-bot change was already made in Step 5 alongside its test.

- [ ] **Step 8: Run all three bot test suites**

```bash
cd go/marketbyorder-bot && go test ./... -count=1
cd ../marketbyprice-bot && go test ./... -count=1
cd ../topofbook-bot && go test ./... -count=1
```
Expected: PASS on all three.

- [ ] **Step 9: Verify the demo stack accepts the new column end to end**

```bash
cd demo && docker compose down -v && docker compose up -d
sleep 30
docker compose exec -T clickhouse clickhouse-client --query \
  "SELECT name, type FROM system.columns WHERE database IN ('topofbook','marketbyorder','marketbyprice') AND table='instruments' AND name='source_id'"
```
Expected: three rows, each `source_id  UInt16`.

If the demo stack is not runnable in this environment, say so explicitly rather than marking this step done. It is the only step that exercises the SQL.

- [ ] **Step 10: Commit**

```bash
git add demo/clickhouse/init/ go/topofbook-bot/ go/marketbyorder-bot/ go/marketbyprice-bot/
git commit -m "bots,clickhouse: store instrument source id"
```

---

### Task 5: Rebuild the branch and update PR #37

After Task 4 the working tree is the desired end state. The branch history, however, still contains eleven commits that build v1+v2 and then correct it to v1+v3. This task collapses that into a clean series in which v2 never existed.

**Files:**
- No file changes. The v2 spec and plan documents were already renamed and replaced before Task 1; this task only rewrites history.

**Interfaces:**
- Consumes: the completed working tree from Tasks 1–4.
- Produces: a rewritten `feat/refdata-v2-dual-version` branch and an updated PR #37.

- [ ] **Step 1: Confirm the tree is green before rewriting anything**

```bash
for m in topofbook-parser marketbyorder-parser marketbyprice-parser topofbook-bot marketbyorder-bot marketbyprice-bot; do
  (cd go/$m && go test ./... -count=1) || echo "FAILED: $m"
done
```
Expected: no `FAILED:` lines. Do not proceed past this step otherwise — the rewrite discards the commits you would need to bisect.

- [ ] **Step 2: Confirm no v2 documents survive**

```bash
ls docs/superpowers/plans/ docs/superpowers/specs/ | grep refdata
```
Expected: exactly `2026-08-08-refdata-v3-dual-version.md` and `2026-08-08-refdata-v3-dual-version-design.md`. If a `-v2-` file is present, `git rm` it before continuing.

- [ ] **Step 3: Record the current tree as a safety net**

```bash
git branch backup/refdata-v2-dual-version-preremake
git rev-parse HEAD
```

Note the printed SHA. If the rewrite goes wrong, `git reset --hard <sha>` restores it. Delete the backup branch only after Step 8 succeeds.

- [ ] **Step 4: Collapse to a single staged change against main**

```bash
git add -A
git commit -q -m "wip: pre-rewrite checkpoint"
git reset --soft main
git status --short
```

Expected: every file from Tasks 1–4 plus the two doc files staged, nothing unstaged, nothing untracked.

- [ ] **Step 5: Re-commit as a clean series**

Each parser is staged by directory with its metrics and runner files excluded by pathspec, rather than by an explicit file list. This matters: the constant rename in Tasks 1 and 2 touched test files beyond the ones named in those tasks (`marketbyprice_test.go`, `marketbyprice_records_test.go`), and an explicit list would silently strand them.

```bash
git reset

git add go/marketbyprice-parser/ ':!go/marketbyprice-parser/metrics*.go' ':!go/marketbyprice-parser/runner*.go'
git commit -m "marketbyprice-parser: decode instrument definitions at schema v1 and v3"

git add go/marketbyorder-parser/ ':!go/marketbyorder-parser/metrics*.go' ':!go/marketbyorder-parser/runner*.go'
git commit -m "marketbyorder-parser: decode instrument definitions at schema v1 and v3"

git add go/topofbook-parser/ ':!go/topofbook-parser/metrics*.go' ':!go/topofbook-parser/runner*.go'
git commit -m "topofbook-parser: decode instrument definitions at schema v1 and v3"

git add go/marketbyprice-parser/ go/marketbyorder-parser/ go/topofbook-parser/
git commit -m "parsers: count frames by wire schema version"

git add demo/clickhouse/init/ go/topofbook-bot/ go/marketbyorder-bot/ go/marketbyprice-bot/
git commit -m "bots,clickhouse: store instrument source id"

git add docs/
git commit -m "docs: add dual-version refdata design and plan"

git status --short
```

Expected: `git status --short` prints nothing. If anything is left over, `git add` it and amend whichever commit above it belongs to rather than creating a seventh.

- [ ] **Step 6: Verify every commit builds and tests clean**

There is no top-level Go module — each directory under `go/` is its own module — so `go build ./...` from the repo root does nothing. Check out each commit and test the modules it touches:

```bash
for sha in $(git rev-list --reverse main..HEAD); do
  git checkout -q "$sha"
  echo "=== $sha $(git log -1 --format=%s) ==="
  for m in topofbook-parser marketbyorder-parser marketbyprice-parser topofbook-bot marketbyorder-bot marketbyprice-bot; do
    (cd go/$m && go build ./... && go test ./... -count=1 >/dev/null) || echo "FAILED: $m"
  done
done
git checkout -q feat/refdata-v2-dual-version
```
Expected: no `FAILED:` lines under any commit.

The metrics commit is the one to watch. It lands after the three parser commits but its label changes (`"2"` → `"3"`) are independent of them, so if it fails here the split in Step 5 put a file in the wrong commit.

- [ ] **Step 7: Confirm v2 is gone from the whole diff**

```bash
git diff main...HEAD | grep -nE '^\+.*(instDefBodyLenV2|buildInstDefV2|buildInstDefBodyV2|maxSchemaVersion|SchemaVersionV2|\b124\b|\b128\b)'
```
Expected: no output. Any hit is a leftover from the v2 implementation.

```bash
git log --oneline main..HEAD
```
Expected: exactly six commits, none mentioning v2.

- [ ] **Step 8: Force-push and update the PR**

```bash
git push --force-with-lease origin feat/refdata-v2-dual-version
```

`--force-with-lease` rather than `--force`: it refuses the push if someone else has pushed to the branch since you last fetched.

Then retitle and rewrite the PR body:

```bash
gh pr edit 37 --title "parsers: decode refdata at schema v1 and v3"
```

Write this body to a scratch file and apply it with `gh pr edit 37 --body-file <path>`:

```markdown
Upstream bumped the feed specs to `3.0.0` (`<feed>/v3.0.0` in edge-feed-spec, [PR #29](https://github.com/malbeclabs/edge-feed-spec/pull/29)). Across all five tagged feeds the wire change is one message: `InstrumentDefinition` grows 80 → 130 bytes. Two changes stack inside it — `Source ID` (`u16`) is inserted after `Instrument ID`, and `Symbol` widens from `char[16]` to `char[64]`.

All three parsers now decode both layouts, chosen **per frame** from the header's Schema Version, so a publisher can cut over mid-run without restarting anything. A test in each parser drives v1 → v3 → v1 to prove it.

Design doc: `docs/superpowers/specs/2026-08-08-refdata-v3-dual-version-design.md`.

- Accepted versions are the **set** `{1, 3}`, not a range. Version 2 is rejected exactly like version 0: a 128-byte layout carrying the widened `Symbol` without `Source ID` was specified upstream and superseded before any publisher emitted it. `topofbook-parser`'s version ceiling became explicit set membership so those frames cannot reach the decoder and be miscounted as truncation.
- `Source ID` is stored, not discarded. New `source_id UInt16 DEFAULT 0` column on `topofbook.instruments`, `marketbyorder.instruments`, and `marketbyprice.instruments`. The event tables already carried a venue ID; the instrument dimension they join against did not.
- Message length cross-checks the declared version in both directions. A v3 header with a v1 body would otherwise read `Source ID` and `Symbol` across 66 bytes of adjacent fields and produce plausible garbage instead of an error.
- `frames_total{port,schema_version}` is how to watch the cutover: `schema_version="3"` climbing while `"1"` goes flat; v1 reaching zero is when the legacy path can be retired. `"2"` should never appear at all.
- The decoder stays duplicated per parser. A shared package would add an `internal` dependency plus a Dockerfile change to three modules for ~25 lines each.

### Migration note

The ClickHouse column ships in the demo init scripts, which only run on a fresh volume. An existing volume needs `ALTER TABLE <db>.instruments ADD COLUMN source_id UInt16 DEFAULT 0 AFTER instrument_id;` on all three tables, or `docker compose down -v`.

The `ORDER BY` keys are untouched, so the dimension self-heals: these are `ReplacingMergeTree` tables keyed on `(channel_id, instrument_id)`, and a v1 row carrying `source_id = 0` is replaced by the v3 row carrying the real venue on the next merge.

### Testing Verification

- Golden byte fixtures for both layouts in each parser, asserting every field lands at the right offset. The v3 fixture uses a 27-byte Kalshi ticker and a nonzero `Source ID`, so it distinguishes v3 from both v1 and the superseded 128-byte layout.
- A 64-byte symbol with no null terminator does not bleed into `Leg1`.
- Length cross-check in both directions: version 3 with a 76-byte body, version 1 with a 126-byte body.
- Versions 0, 2, 4, and 255 rejected at both the frame header and the `InstrumentDefinition` decoder.
- Error text pinned: a length mismatch classifies as `truncated`, an unsupported version as `schema_version`.
- v1 → v3 → v1 mid-stream, followed without a restart.
- `source_id` reaches the record's `Fields` map as `0` at v1 and the decoded value at v3, and each of the three bots writes it. The bot-side tests use `float64`, which is what the field actually is after the parser/bot JSON hop.

### Not yet verified

**No publisher emits v3 yet**, so the v3 path is fixture-only and has never seen live traffic. The demo dashboards were not checked against a 64-character symbol for the same reason — the live publisher still emits truncated 16-character symbols.

### Note on history

This branch was force-pushed. It previously carried a v1 + **v2** implementation; v2 was superseded upstream before merge and has been removed from the history entirely rather than layered over. Any stale review comments refer to a layout that no longer exists. The branch name still says `v2` — renaming it would close and reopen this PR.
```

- [ ] **Step 9: Confirm and clean up**

```bash
gh pr view 37 --json title,state,headRefName
git branch -D backup/refdata-v2-dual-version-preremake
```

Delete the backup branch only once the PR shows the new title and the push succeeded.

---

## Verification Summary

After all five tasks, these must all hold:

- `grep -rn 'instDefBodyLenV2\|buildInstDefV2\|buildInstDefBodyV2\|maxSchemaVersion\|SchemaVersionV2' go/` returns nothing.
- All six Go modules build and pass `go test ./... -count=1`.
- Each parser rejects schema versions 0, 2, 4, and 255, and accepts 1 and 3.
- Each parser decodes a 27-byte symbol and a nonzero Source ID from a v3 fixture, and `SourceID == 0` from a v1 fixture.
- `git log --oneline main..HEAD` shows six commits and no mention of v2.
- PR #37 is titled `parsers: decode refdata at schema v1 and v3`.
