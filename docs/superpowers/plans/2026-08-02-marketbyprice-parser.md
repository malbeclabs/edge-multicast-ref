# Market-by-Price parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `go/marketbyprice-parser` — a multicast subscriber that joins the three ports of a DoubleZero Market-by-Price channel, decodes the binary wire format, and emits one JSON record per application message to a Unix socket or file, with Prometheus metrics.

**Architecture:** Stateless decode. A `Runner` opens one multicast UDP socket per port (refdata, mktdata, snapshot) and runs a goroutine each. Every datagram is one frame: validate the 24-byte frame header, walk the packed application messages by `Message Length`, decode each into a `Record`, and hand the batch to an `OutputSink`. No book state, no cross-frame state except a per-port sequence-gap tracker. This mirrors `go/marketbyorder-parser` exactly, which is the reference precedent for this repo.

**Tech Stack:** Go 1.25.0 (toolchain go1.26.0 installed), package `main`, standard library plus `github.com/prometheus/client_golang v1.23.2`. Tests use the standard `testing` package. Go workspace at `go/go.work`.

**Spec:** `docs/superpowers/specs/2026-08-02-marketbyprice-design.md`

**Feed spec:** https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md

**Working dir for all commands:** `go/marketbyprice-parser` unless a step says otherwise.

## Global Constraints

- Go directive `go 1.25.0` in `go.mod`, matching every other module in the workspace.
- Module path `github.com/malbeclabs/edge-multicast-ref/go/marketbyprice-parser`.
- Package `main` for every file in this module. No subpackages.
- Prometheus metric namespace exactly `dz_mbp_parser`.
- Magic `0x4442`. Schema Version `1`. Frame header 24 bytes. Message header 4 bytes. Max frame 1232 bytes.
- All multi-byte integers little-endian.
- Body length checks are **exact equality**, never `>=`. A v1 body of unexpected length is malformed.
- The `u16` value `0xFFFF` in `Order Count` and `Level Index` means *absent*. It MUST NOT reach JSON as `65535`; the key is omitted instead.
- Write "DoubleZero" in prose, never "DZ". Binary names and env vars keep their existing `dz-` / `DZ_` prefixes.
- **Encoding negative values in tests:** assign through a typed variable, never a constant conversion. `byte(int8(-2))` and `uint64(int64(-1500))` are compile-time overflow errors in every Go version, because the operand is an untyped-constant expression. Write `exp := int8(-2); buf[i] = byte(exp)`. Do not substitute the equivalent unsigned literal (`254`) or a bit-complement trick (`^uint64(1499)`) — both compile, but they hide which signed value the test is asserting, and these tests exist to guard sign handling on `price` and exponent fields.
- Commit messages: `component: short description`, lowercase, imperative, no trailing period, no `Co-Authored-By` line.
- **Every task ends gofmt-clean.** Before committing, run `gofmt -l ./marketbyprice-parser/` from `go/` and confirm it prints nothing; run `gofmt -w` on any file it lists. The code blocks in this plan are indented for readability in Markdown and are not guaranteed gofmt-canonical — particularly inline comment alignment, which shifts whenever a literal's width changes.
- Never write a `Liquidation`, `LevelUpdate`, `BookClear`, or `SnapshotLevel` decoder by copying from `marketbyorder-parser` — those four do not exist there. Write them from the offsets in this plan.

## File map

All paths relative to `go/marketbyprice-parser/`.

- `go.mod` — module definition. Task 1.
- `.gitignore` — built binary. Task 1.
- `marketbyprice_wire.go` — constants, sentinel errors, `FrameHeader`, `MessageHeader`, and one body struct + parse function per message type. Tasks 1, 2, 3.
- `marketbyprice_wire_test.go` — byte-exact tests for every parse function. Tasks 1, 2, 3.
- `marketbyprice.go` — `marketByPriceParser`, `ParseFrame` (frame walk), `decodeMessage` (dispatch), enum stringers. Task 4.
- `marketbyprice_test.go` — frame walk and dispatch tests. Task 4.
- `parser.go` — `Record`, `Parser` interface, parser registry. Task 5.
- `metrics.go` — `Metrics`, `NewMetrics`, `ServeHTTP`, plus the two defect counters. Task 5.
- `metrics_smoke_test.go` — namespace and defect-counter registration. Task 5.
- `sink.go`, `sink_json.go`, `sink_socket.go` — output sinks. Task 6.
- `sink_json_test.go`, `sink_socket_test.go` — sink tests. Task 6.
- `runner.go` — `seqTracker`, `portConfig`, `Runner`, receive loop, latency observation, `classifyError`. Task 7.
- `seqtracker_test.go` — sequence tracker tests. Task 7.
- `timestamp_linux.go`, `timestamp_other.go` — `SO_TIMESTAMPNS` kernel receive timestamps. Task 7.
- `main.go` — flags and wiring. Task 8.
- `Dockerfile`, `README.md` — Task 8.
- `../go.work` — add this module. Task 1.

## Verification commands (read before running anything)

The Go workspace does not support `./...` from `go/`. `go build ./...`, `go vet ./...`, and `go test ./...` all fail there with `pattern ./...: directory prefix . does not contain modules listed in go.work`, and they failed that way before this work started. Do not try to fix that, and do not use those forms.

Use these instead, from `go/`:

- Type-check: `go vet ./marketbyprice-parser/...`
- Test: `go test ./marketbyprice-parser/...`
- Race check: `go test -race ./marketbyprice-parser/...`

Two more facts to save you a wrong diagnosis:

- `go build` on this module fails with `function main is undeclared in the main package` until Task 8 adds `main.go`. That is expected in Tasks 1-7. `go vet` type-checks without linking, which is why it is the build gate until then.
- `go build ./marketbyprice-parser/...` fails with `build output "marketbyprice-parser" already exists and is a directory` — building a `main` package writes a binary named after the directory into the current directory. From Task 8 on, use `go build -o /tmp/dz-marketbyprice-parser ./marketbyprice-parser/` or build from inside the module directory.

Baseline before starting: `go vet` and `go test` pass for every module except `xdp-receiver`, which does not build because its generated BPF object `xdpfilter_bpfel.o` is absent. That is pre-existing and out of scope — leave it alone.

---

## Task 1: Module scaffolding and frame/message headers

**Files:**
- Create: `go/marketbyprice-parser/go.mod`
- Create: `go/marketbyprice-parser/.gitignore`
- Create: `go/marketbyprice-parser/marketbyprice_wire.go`
- Modify: `go/go.work`
- Test: `go/marketbyprice-parser/marketbyprice_wire_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `mbpMagic uint16`, `mbpSchemaVersion uint8`, `frameHeaderSize`, `messageHeaderSize`, `maxFrameSize`, `flagSnapshot uint16`; errors `errBadMagic`, `errSchemaVersion`, `errFrameTooShort`, `errFrameLength`, `errMessageTooShort`, `errMessageLength`, `errTruncated`, `errMalformedBody`; types `FrameHeader`, `MessageHeader`; functions `ParseFrameHeader([]byte) (FrameHeader, error)`, `ParseMessageHeader([]byte) (MessageHeader, error)`, `fixedString([]byte) string`, `readTSNs([]byte) time.Time`.

- [ ] **Step 1: Create the module**

From `go/marketbyprice-parser/`, create `go.mod`:

```
module github.com/malbeclabs/edge-multicast-ref/go/marketbyprice-parser

go 1.25.0

require github.com/prometheus/client_golang v1.23.2
```

Create `.gitignore`:

```
marketbyprice-parser
dz-marketbyprice-parser
```

- [ ] **Step 2: Register the module in the workspace**

Edit `go/go.work` and add `./marketbyprice-parser` to the `use` block, directly after `./marketbyorder-parser`. The existing list is not sorted, so do not reorder it. The result:

```
go 1.25.0

use (
	./marketbyorder-bot
	./marketbyorder-parser
	./marketbyprice-parser
	./internal
	./kernel-receiver
	./topofbook-bot
	./topofbook-parser
	./xdp-receiver
)
```

- [ ] **Step 3: Write the failing header tests**

Create `marketbyprice_wire_test.go`:

```go
package main

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

// buildFrameHeader constructs a 24-byte frame header for tests.
func buildFrameHeader(magic uint16, schema, channel uint8, seq uint64, ts time.Time, msgCount, resetCount uint8, frameLen uint16) []byte {
	buf := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint16(buf[0:2], magic)
	buf[2] = schema
	buf[3] = channel
	binary.LittleEndian.PutUint64(buf[4:12], seq)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts.UnixNano()))
	buf[20] = msgCount
	buf[21] = resetCount
	binary.LittleEndian.PutUint16(buf[22:24], frameLen)
	return buf
}

func TestMagicIsMarketByPrice(t *testing.T) {
	// 0x4442 is this feed's magic. It must differ from the sibling feeds so a
	// misrouted frame is rejected rather than cross-decoded.
	if mbpMagic != 0x4442 {
		t.Fatalf("magic: got %#x want 0x4442", mbpMagic)
	}
	for name, other := range map[string]uint16{"topofbook": 0x445A, "marketbyorder": 0x4444, "midpoint": 0x4D44} {
		if mbpMagic == other {
			t.Fatalf("magic collides with %s feed", name)
		}
	}
}

func TestParseFrameHeader_Valid(t *testing.T) {
	ts := time.Unix(1700000000, 123456789)
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersion, 7, 42, ts, 3, 1, frameHeaderSize)
	h, err := ParseFrameHeader(buf)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Magic != mbpMagic {
		t.Errorf("magic: got %x want %x", h.Magic, mbpMagic)
	}
	if h.ChannelID != 7 {
		t.Errorf("channel: got %d", h.ChannelID)
	}
	if h.Sequence != 42 {
		t.Errorf("seq: got %d", h.Sequence)
	}
	if !h.SendTimestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", h.SendTimestamp, ts)
	}
	if h.MessageCount != 3 || h.ResetCount != 1 || h.FrameLength != frameHeaderSize {
		t.Errorf("fields: %+v", h)
	}
}

func TestParseFrameHeader_BadMagic(t *testing.T) {
	// A market-by-order frame must not decode here.
	buf := buildFrameHeader(0x4444, mbpSchemaVersion, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errBadMagic) {
		t.Fatalf("expected errBadMagic, got %v", err)
	}
}

func TestParseFrameHeader_WrongVersion(t *testing.T) {
	buf := buildFrameHeader(mbpMagic, 99, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errSchemaVersion) {
		t.Fatalf("expected errSchemaVersion, got %v", err)
	}
}

func TestParseFrameHeader_LengthMismatch(t *testing.T) {
	buf := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 0, time.Now(), 0, 0, 999)
	if _, err := ParseFrameHeader(buf); !errors.Is(err, errFrameLength) {
		t.Fatalf("expected errFrameLength, got %v", err)
	}
}

func TestParseFrameHeader_TooShort(t *testing.T) {
	if _, err := ParseFrameHeader(make([]byte, 10)); !errors.Is(err, errFrameTooShort) {
		t.Fatalf("expected errFrameTooShort, got %v", err)
	}
}

func TestParseMessageHeader(t *testing.T) {
	buf := []byte{0x40, 48, 0x01, 0x00}
	mh, err := ParseMessageHeader(buf)
	if err != nil {
		t.Fatal(err)
	}
	if mh.Type != 0x40 || mh.Length != 48 {
		t.Errorf("header: %+v", mh)
	}
	if mh.Flags&flagSnapshot == 0 {
		t.Error("snapshot flag should be set")
	}
}

func TestParseMessageHeader_TooShort(t *testing.T) {
	if _, err := ParseMessageHeader([]byte{0x40, 48}); !errors.Is(err, errMessageTooShort) {
		t.Fatalf("expected errMessageTooShort, got %v", err)
	}
}

func TestFixedString(t *testing.T) {
	if got := fixedString([]byte{'B', 'T', 'C', 0, 0}); got != "BTC" {
		t.Errorf("got %q", got)
	}
	// No null terminator: the whole field is the value.
	if got := fixedString([]byte{'A', 'B'}); got != "AB" {
		t.Errorf("got %q", got)
	}
}
```

- [ ] **Step 4: Run the tests to verify they fail**

Run: `go test ./... 2>&1 | head -20`
Expected: compile failure — `undefined: frameHeaderSize`, `undefined: mbpMagic`, and so on.

- [ ] **Step 5: Write the wire header implementation**

Create `marketbyprice_wire.go`:

```go
// Wire format authoritatively defined by the Market-by-Price Feed spec:
// https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md
// Keep the byte layout below in sync with that document.
//
// Body sizes in this file are message size minus the 4-byte application
// message header, because that is what each Parse* function receives.
//
// Length checks are exact equality, not >=. The spec's forward-compatibility
// rule that a decoder ignores trailing bytes only applies across a Schema
// Version bump, and ParseFrameHeader rejects unimplemented versions before any
// body is parsed. Within v1, an unexpected body length is malformed.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	mbpMagic          uint16 = 0x4442
	mbpSchemaVersion  uint8  = 1
	frameHeaderSize          = 24
	messageHeaderSize        = 4
	maxFrameSize             = 1232
)

// Wire decoding errors.
var (
	errBadMagic        = errors.New("bad magic")
	errSchemaVersion   = errors.New("unsupported schema version")
	errFrameTooShort   = errors.New("frame too short for header")
	errFrameLength     = errors.New("frame length mismatch")
	errMessageTooShort = errors.New("message too short for header")
	errMessageLength   = errors.New("message length out of range")
	errTruncated       = errors.New("truncated message body")
	errMalformedBody   = errors.New("malformed message body")
)

// FrameHeader is the 24-byte frame header common to all three ports.
type FrameHeader struct {
	Magic         uint16
	SchemaVersion uint8
	ChannelID     uint8
	Sequence      uint64
	SendTimestamp time.Time
	MessageCount  uint8
	ResetCount    uint8
	FrameLength   uint16
}

// MessageHeader is the 4-byte header preceding each application message.
type MessageHeader struct {
	Type   uint8
	Length uint8
	Flags  uint16
}

// flagSnapshot is application-header Flags bit 0. The publisher sets it on the
// snapshot port and clears it on mktdata and refdata. It MUST NOT be used to
// route a message — Type ID and port already determine that — but disagreement
// with the arrival port is a publisher defect worth counting.
const flagSnapshot uint16 = 0x0001

// ParseFrameHeader decodes the 24-byte frame header from buf.
func ParseFrameHeader(buf []byte) (FrameHeader, error) {
	if len(buf) < frameHeaderSize {
		return FrameHeader{}, errFrameTooShort
	}
	h := FrameHeader{
		Magic:         binary.LittleEndian.Uint16(buf[0:2]),
		SchemaVersion: buf[2],
		ChannelID:     buf[3],
		Sequence:      binary.LittleEndian.Uint64(buf[4:12]),
		MessageCount:  buf[20],
		ResetCount:    buf[21],
		FrameLength:   binary.LittleEndian.Uint16(buf[22:24]),
	}
	if h.Magic != mbpMagic {
		return h, errBadMagic
	}
	if h.SchemaVersion != mbpSchemaVersion {
		return h, errSchemaVersion
	}
	tsNs := binary.LittleEndian.Uint64(buf[12:20])
	h.SendTimestamp = time.Unix(0, int64(tsNs)).UTC()
	if int(h.FrameLength) != len(buf) {
		return h, errFrameLength
	}
	return h, nil
}

// ParseMessageHeader decodes a 4-byte application message header.
func ParseMessageHeader(buf []byte) (MessageHeader, error) {
	if len(buf) < messageHeaderSize {
		return MessageHeader{}, errMessageTooShort
	}
	return MessageHeader{
		Type:   buf[0],
		Length: buf[1],
		Flags:  binary.LittleEndian.Uint16(buf[2:4]),
	}, nil
}

// fixedString decodes a fixed-length null-padded ASCII field.
func fixedString(buf []byte) string {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

// readTSNs reads an 8-byte little-endian nanoseconds-since-epoch timestamp.
func readTSNs(buf []byte) time.Time {
	ns := binary.LittleEndian.Uint64(buf)
	return time.Unix(0, int64(ns)).UTC()
}
```

Note: `messageHeaderSize`, `maxFrameSize`, `errMessageLength`, `errTruncated`, and `errMalformedBody` are unused until later tasks. Go permits unused package-level identifiers (unlike unused locals), so this compiles.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./... -v -run 'TestMagic|TestParseFrameHeader|TestParseMessageHeader|TestFixedString'`
Expected: all PASS.

- [ ] **Step 7: Verify the workspace still builds**

Run from `go/`: `go vet ./marketbyprice-parser/...`
Expected: no output. Do not run `go build` — see Verification commands above; it cannot succeed until Task 8.

- [ ] **Step 8: Commit**

```bash
git add go/go.work go/marketbyprice-parser/
git commit -m "marketbyprice-parser: add module and frame header decode"
```

---

## Task 2: Inherited message bodies

Eight message types are byte-for-byte identical to `go/marketbyorder-parser`. Port them rather than re-deriving them, then prove equivalence with tests.

**Files:**
- Modify: `go/marketbyprice-parser/marketbyprice_wire.go`
- Test: `go/marketbyprice-parser/marketbyprice_wire_test.go`

**Interfaces:**
- Consumes: `errTruncated`, `fixedString`, `readTSNs` from Task 1.
- Produces: message type constants `msgTypeHeartbeat 0x01`, `msgTypeInstrumentDefinition 0x02`, `msgTypeTrade 0x04`, `msgTypeEndOfSession 0x06`, `msgTypeManifestSummary 0x07`, `msgTypeBatchBoundary 0x13`, `msgTypeInstrumentReset 0x14`, `msgTypeSnapshotEnd 0x22`; body types `HeartbeatBody`, `InstrumentDefinitionBody`, `TradeBody`, `EndOfSessionBody`, `ManifestSummaryBody`, `BatchBoundaryBody`, `InstrumentResetBody`, `SnapshotEndBody`, each with a `Parse<Name>([]byte) (<Name>Body, error)`.

- [ ] **Step 1: Write the failing tests**

Append to `marketbyprice_wire_test.go`:

```go
func TestParseHeartbeat(t *testing.T) {
	ts := time.Unix(1700000001, 0)
	buf := make([]byte, 12)
	buf[0] = 5 // Channel ID
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))
	body, err := ParseHeartbeat(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.ChannelID != 5 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseHeartbeat_WrongLength(t *testing.T) {
	if _, err := ParseHeartbeat(make([]byte, 11)); !errors.Is(err, errTruncated) {
		t.Fatalf("expected errTruncated, got %v", err)
	}
}

func TestParseEndOfSession(t *testing.T) {
	ts := time.Unix(1700000002, 0)
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(ts.UnixNano()))
	body, err := ParseEndOfSession(buf)
	if err != nil {
		t.Fatal(err)
	}
	if !body.Timestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", body.Timestamp, ts)
	}
}

func TestParseManifestSummary(t *testing.T) {
	ts := time.Unix(1700000003, 0)
	buf := make([]byte, 20)
	buf[0] = 7 // Channel ID
	buf[1] = 1 // Valid
	binary.LittleEndian.PutUint16(buf[4:6], 100)
	binary.LittleEndian.PutUint32(buf[8:12], 25)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(ts.UnixNano()))
	body, err := ParseManifestSummary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.ChannelID != 7 || body.Valid != 1 || body.ManifestSeq != 100 || body.InstrumentCount != 25 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentDefinition(t *testing.T) {
	expiry := time.Unix(1800000000, 0)
	buf := make([]byte, 76)
	binary.LittleEndian.PutUint32(buf[0:4], 4242)
	copy(buf[4:20], "BTC-USDT")
	copy(buf[20:28], "BTC")
	copy(buf[28:36], "USDT")
	buf[36] = 1                                              // Asset Class: crypto spot
	// Exponents are negative. Assign through typed variables: `byte(int8(-2))`
	// is a compile-time overflow error, because the operand is a constant.
	priceExp, qtyExp := int8(-2), int8(-8)
	buf[37] = byte(priceExp)
	buf[38] = byte(qtyExp)
	buf[39] = 1                                              // Market Model: CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1)))
	binary.LittleEndian.PutUint64(buf[48:56], 100)
	binary.LittleEndian.PutUint64(buf[56:64], 0)
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1 // Settle Type: cash
	buf[73] = 2 // Price Bound: non-negative
	binary.LittleEndian.PutUint16(buf[74:76], 9)

	body, err := ParseInstrumentDefinition(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 4242 || body.Symbol != "BTC-USDT" || body.Leg1 != "BTC" || body.Leg2 != "USDT" {
		t.Errorf("identity: %+v", body)
	}
	if body.AssetClass != 1 || body.PriceExponent != -2 || body.QtyExponent != -8 || body.MarketModel != 1 {
		t.Errorf("scaling: %+v", body)
	}
	if body.TickSizeRaw != 1 || body.LotSizeRaw != 100 || body.ContractValue != 0 {
		t.Errorf("sizes: %+v", body)
	}
	if !body.Expiry.Equal(expiry) || body.SettleType != 1 || body.PriceBound != 2 || body.ManifestSeq != 9 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseTrade(t *testing.T) {
	ts := time.Unix(1700000004, 500)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 7)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // Aggressor Side: buy
	buf[7] = 0x02 // Trade Flags: sweep
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts.UnixNano()))
	tradePrice := int64(-1500) // typed variable; see the note on exponents above
	binary.LittleEndian.PutUint64(buf[16:24], uint64(tradePrice))
	binary.LittleEndian.PutUint64(buf[24:32], 250)
	binary.LittleEndian.PutUint64(buf[32:40], 99887766)
	binary.LittleEndian.PutUint64(buf[40:48], 1000000)

	body, err := ParseTrade(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 7 || body.SourceID != 1 || body.AggressorSide != 1 || body.TradeFlags != 0x02 {
		t.Errorf("head: %+v", body)
	}
	if !body.SourceTimestamp.Equal(ts) || body.TradePriceRaw != -1500 || body.TradeQtyRaw != 250 {
		t.Errorf("exec: %+v", body)
	}
	if body.TradeID != 99887766 || body.CumulativeVolumeRaw != 1000000 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseBatchBoundary(t *testing.T) {
	ts := time.Unix(1700000005, 0)
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], 123456)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))
	body, err := ParseBatchBoundary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.BatchID != 123456 || !body.BatchTime.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentReset(t *testing.T) {
	ts := time.Unix(1700000006, 0)
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], 55)
	buf[4] = 3 // Reason: upstream gap
	binary.LittleEndian.PutUint64(buf[8:16], 9000)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(ts.UnixNano()))
	body, err := ParseInstrumentReset(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 55 || body.Reason != 3 || body.NewAnchorSeq != 9000 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotEnd(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], 77)
	binary.LittleEndian.PutUint64(buf[4:12], 12345)
	binary.LittleEndian.PutUint32(buf[12:16], 9)
	body, err := ParseSnapshotEnd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 77 || body.AnchorSeq != 12345 || body.SnapshotID != 9 {
		t.Errorf("body: %+v", body)
	}
}

// Every inherited body rejects a length that is off by one in either direction.
func TestInheritedBodies_ExactLengthOnly(t *testing.T) {
	cases := []struct {
		name string
		size int
		fn   func([]byte) error
	}{
		{"heartbeat", 12, func(b []byte) error { _, err := ParseHeartbeat(b); return err }},
		{"instrument_definition", 76, func(b []byte) error { _, err := ParseInstrumentDefinition(b); return err }},
		{"trade", 48, func(b []byte) error { _, err := ParseTrade(b); return err }},
		{"end_of_session", 8, func(b []byte) error { _, err := ParseEndOfSession(b); return err }},
		{"manifest_summary", 20, func(b []byte) error { _, err := ParseManifestSummary(b); return err }},
		{"batch_boundary", 12, func(b []byte) error { _, err := ParseBatchBoundary(b); return err }},
		{"instrument_reset", 24, func(b []byte) error { _, err := ParseInstrumentReset(b); return err }},
		{"snapshot_end", 16, func(b []byte) error { _, err := ParseSnapshotEnd(b); return err }},
	}
	for _, c := range cases {
		if err := c.fn(make([]byte, c.size)); err != nil {
			t.Errorf("%s: exact size %d rejected: %v", c.name, c.size, err)
		}
		if err := c.fn(make([]byte, c.size-1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: size %d accepted or wrong error: %v", c.name, c.size-1, err)
		}
		if err := c.fn(make([]byte, c.size+1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: trailing byte accepted (must be exact-length): %v", c.name, err)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... 2>&1 | head -20`
Expected: compile failure — `undefined: ParseHeartbeat` and the other seven.

- [ ] **Step 3: Port the eight inherited bodies**

Open `go/marketbyorder-parser/marketbyorder_wire.go` and copy these declarations into `marketbyprice_wire.go`, unchanged except as noted:

- The message type constant block, keeping only `msgTypeHeartbeat` (`0x01`), `msgTypeInstrumentDefinition` (`0x02`), `msgTypeTrade` (`0x04`), `msgTypeEndOfSession` (`0x06`), `msgTypeManifestSummary` (`0x07`), `msgTypeBatchBoundary` (`0x13`), `msgTypeInstrumentReset` (`0x14`), `msgTypeSnapshotEnd` (`0x22`). Drop `msgTypeOrderAdd`, `msgTypeOrderCancel`, `msgTypeOrderExecute`, `msgTypeSnapshotBegin`, and `msgTypeSnapshotOrder` — the first three do not exist on this feed and the last two are defined in Task 3.
- `HeartbeatBody` + `ParseHeartbeat`
- `InstrumentDefinitionBody` + `ParseInstrumentDefinition`
- `TradeBody` + `ParseTrade`
- `EndOfSessionBody` + `ParseEndOfSession`
- `ManifestSummaryBody` + `ParseManifestSummary`
- `BatchBoundaryBody` + `ParseBatchBoundary`
- `InstrumentResetBody` + `ParseInstrumentReset`
- `SnapshotEndBody` + `ParseSnapshotEnd`

Add a comment above the constant block:

```go
// Message type IDs. Types 0x03 and 0x05 are reserved and intentionally unused
// (Quote in top-of-book, and a reserved slot) so a misrouted sibling frame
// cannot cross-decode. 0x13, 0x14, 0x22, and the 0x02/0x04 bodies are
// byte-for-byte identical to the market-by-order feed.
```

Do not change any offset, any field name, or any error wrapping. The MBP spec defines these as byte-identical, and the existing implementations are the reference.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: decode inherited message bodies"
```

---

## Task 3: New message bodies

Five bodies are new code. `Liquidation` is byte-identical to the top-of-book feed on the wire but **no parser in this repo decodes it**, so write it from the offsets here. `SnapshotBegin` extends the market-by-order layout with `Depth Bound`. `LevelUpdate`, `BookClear`, and `SnapshotLevel` are unique to this feed.

**Files:**
- Modify: `go/marketbyprice-parser/marketbyprice_wire.go`
- Test: `go/marketbyprice-parser/marketbyprice_wire_test.go`

**Interfaces:**
- Consumes: `errTruncated`, `errMalformedBody`, `readTSNs` from Task 1.
- Produces: constants `msgTypeLiquidation 0x08`, `msgTypeSnapshotBegin 0x20`, `msgTypeLevelUpdate 0x40`, `msgTypeBookClear 0x41`, `msgTypeSnapshotLevel 0x42`; constant `u16Unavailable uint16 = 0xFFFF`; types and functions:
  - `LiquidationBody{InstrumentID uint32; SourceID uint16; Flags, Method uint8; TradeID uint64; MarkPriceRaw int64; LiquidatedUser [20]byte}` / `ParseLiquidation`
  - `SnapshotBeginBody{InstrumentID uint32; AnchorSeq uint64; TotalLevels, SnapshotID, LastInstrumentSeq uint32; Timestamp time.Time; DepthBound uint32}` / `ParseSnapshotBegin`
  - `LevelUpdateBody{InstrumentID uint32; SourceID uint16; Side, Action uint8; PerInstrumentSeq uint32; PriceRaw int64; QtyRaw uint64; Timestamp time.Time; OrderCount, LevelIndex uint16; UpdateReason, LevelFlags uint8}` / `ParseLevelUpdate`
  - `BookClearBody{InstrumentID uint32; SourceID uint16; ClearSide, Scope uint8; PerInstrumentSeq uint32; FromPriceRaw int64; Timestamp time.Time; ClearReason uint8}` / `ParseBookClear`
  - `SnapshotLevelBody{SnapshotID uint32; PriceRaw int64; QtyRaw uint64; OrderCount uint16; Side, LevelFlags uint8}` / `ParseSnapshotLevel`

- [ ] **Step 1: Write the failing tests**

Append to `marketbyprice_wire_test.go`:

```go
func TestParseLiquidation(t *testing.T) {
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint32(buf[0:4], 12)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 0x03 // Flags: short liquidated (bit 0) + ADL (bit 1)
	buf[7] = 1    // Method: backstop
	binary.LittleEndian.PutUint64(buf[8:16], 5150)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(int64(432100)))
	for i := 0; i < 20; i++ {
		buf[24+i] = byte(0xA0 + i)
	}
	body, err := ParseLiquidation(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 12 || body.SourceID != 1 || body.Flags != 0x03 || body.Method != 1 {
		t.Errorf("head: %+v", body)
	}
	if body.TradeID != 5150 || body.MarkPriceRaw != 432100 {
		t.Errorf("pairing: %+v", body)
	}
	if body.LiquidatedUser[0] != 0xA0 || body.LiquidatedUser[19] != 0xB3 {
		t.Errorf("user: %x", body.LiquidatedUser)
	}
}

func TestParseSnapshotBegin(t *testing.T) {
	ts := time.Unix(1700000007, 0)
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint32(buf[0:4], 77)
	binary.LittleEndian.PutUint64(buf[4:12], 12345)
	binary.LittleEndian.PutUint32(buf[12:16], 400) // Total Levels
	binary.LittleEndian.PutUint32(buf[16:20], 9)   // Snapshot ID
	binary.LittleEndian.PutUint32(buf[20:24], 8888) // Last Instrument Seq
	binary.LittleEndian.PutUint64(buf[24:32], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint32(buf[32:36], 0) // Depth Bound: complete book

	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 77 || body.AnchorSeq != 12345 || body.TotalLevels != 400 {
		t.Errorf("head: %+v", body)
	}
	if body.SnapshotID != 9 || body.LastInstrumentSeq != 8888 || !body.Timestamp.Equal(ts) {
		t.Errorf("ids: %+v", body)
	}
	if body.DepthBound != 0 {
		t.Errorf("depth bound: got %d want 0", body.DepthBound)
	}
}

// The 36-byte body is the market-by-order 32-byte layout plus Depth Bound.
// A 32-byte body is a market-by-order message and must be rejected here: the
// prefix-superset rule lets an MBO decoder read an MBP frame, not the reverse.
func TestParseSnapshotBegin_RejectsShortSiblingLayout(t *testing.T) {
	if _, err := ParseSnapshotBegin(make([]byte, 32)); !errors.Is(err, errTruncated) {
		t.Fatalf("expected errTruncated for 32-byte body, got %v", err)
	}
}

func TestParseSnapshotBegin_BoundedDepth(t *testing.T) {
	buf := make([]byte, 36)
	binary.LittleEndian.PutUint32(buf[32:36], 50)
	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.DepthBound != 50 {
		t.Errorf("depth bound: got %d want 50", body.DepthBound)
	}
}

func TestParseLevelUpdate(t *testing.T) {
	ts := time.Unix(1700000008, 42)
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint32(buf[0:4], 101)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1 // Side: ask
	buf[7] = 2 // Action: change
	binary.LittleEndian.PutUint32(buf[8:12], 777)
	// Negative prices are legal. Encode through a typed variable: a constant
	// conversion such as uint64(int64(-2500)) is a compile-time overflow error.
	priceRaw := int64(-2500)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(priceRaw))
	binary.LittleEndian.PutUint64(buf[20:28], 12345)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint16(buf[36:38], 4)  // Order Count
	binary.LittleEndian.PutUint16(buf[38:40], 2)  // Level Index
	buf[40] = 1 // Update Reason: trade
	buf[41] = 0x02 // Level Flags: AMM-synthetic

	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 101 || body.SourceID != 1 || body.Side != 1 || body.Action != 2 {
		t.Errorf("head: %+v", body)
	}
	if body.PerInstrumentSeq != 777 || body.PriceRaw != -2500 || body.QtyRaw != 12345 {
		t.Errorf("level: %+v", body)
	}
	if !body.Timestamp.Equal(ts) || body.OrderCount != 4 || body.LevelIndex != 2 {
		t.Errorf("meta: %+v", body)
	}
	if body.UpdateReason != 1 || body.LevelFlags != 0x02 {
		t.Errorf("tail: %+v", body)
	}
}

// Quantity 0 is the delete signal and must decode cleanly, not be treated as absent.
func TestParseLevelUpdate_ZeroQtyIsValid(t *testing.T) {
	buf := make([]byte, 44)
	buf[7] = 3 // Action: delete
	binary.LittleEndian.PutUint64(buf[12:20], uint64(int64(500)))
	binary.LittleEndian.PutUint64(buf[20:28], 0)
	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.QtyRaw != 0 || body.Action != 3 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseLevelUpdate_Sentinels(t *testing.T) {
	buf := make([]byte, 44)
	binary.LittleEndian.PutUint16(buf[36:38], u16Unavailable)
	binary.LittleEndian.PutUint16(buf[38:40], u16Unavailable)
	body, err := ParseLevelUpdate(buf)
	if err != nil {
		t.Fatal(err)
	}
	// The wire value is preserved here; omission from JSON happens in decodeMessage.
	if body.OrderCount != u16Unavailable || body.LevelIndex != u16Unavailable {
		t.Errorf("sentinels not preserved: %+v", body)
	}
}

func TestParseBookClear(t *testing.T) {
	ts := time.Unix(1700000009, 0)
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[0:4], 202)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 2 // Clear Side: both
	buf[7] = 0 // Scope: entire side
	binary.LittleEndian.PutUint32(buf[8:12], 900)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(int64(0)))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(ts.UnixNano()))
	buf[28] = 1 // Clear Reason: halt

	body, err := ParseBookClear(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 202 || body.SourceID != 1 || body.ClearSide != 2 || body.Scope != 0 {
		t.Errorf("head: %+v", body)
	}
	if body.PerInstrumentSeq != 900 || !body.Timestamp.Equal(ts) || body.ClearReason != 1 {
		t.Errorf("tail: %+v", body)
	}
}

func TestParseBookClear_ScopedFromPrice(t *testing.T) {
	buf := make([]byte, 32)
	buf[6] = 0 // Clear Side: bid
	buf[7] = 1 // Scope: from price outward
	// Typed variable, not a constant conversion — see TestParseLevelUpdate.
	fromPrice := int64(-777)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(fromPrice))
	body, err := ParseBookClear(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.Scope != 1 || body.FromPriceRaw != -777 {
		t.Errorf("body: %+v", body)
	}
}

// Scope=1 with Clear Side=2 is malformed: one price cannot bound both sides.
// The spec requires the subscriber to discard and count it.
func TestParseBookClear_ScopeBothSidesMalformed(t *testing.T) {
	buf := make([]byte, 32)
	buf[6] = 2 // Clear Side: both
	buf[7] = 1 // Scope: from price outward
	if _, err := ParseBookClear(buf); !errors.Is(err, errMalformedBody) {
		t.Fatalf("expected errMalformedBody, got %v", err)
	}
}

func TestParseSnapshotLevel(t *testing.T) {
	buf := make([]byte, 28)
	binary.LittleEndian.PutUint32(buf[0:4], 9)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(int64(998877)))
	binary.LittleEndian.PutUint64(buf[12:20], 654321)
	binary.LittleEndian.PutUint16(buf[20:22], 12)
	buf[22] = 0    // Side: bid
	buf[23] = 0x01 // Level Flags: implied

	body, err := ParseSnapshotLevel(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.SnapshotID != 9 || body.PriceRaw != 998877 || body.QtyRaw != 654321 {
		t.Errorf("level: %+v", body)
	}
	if body.OrderCount != 12 || body.Side != 0 || body.LevelFlags != 0x01 {
		t.Errorf("meta: %+v", body)
	}
}

func TestNewBodies_ExactLengthOnly(t *testing.T) {
	cases := []struct {
		name string
		size int
		fn   func([]byte) error
	}{
		{"liquidation", 44, func(b []byte) error { _, err := ParseLiquidation(b); return err }},
		{"snapshot_begin", 36, func(b []byte) error { _, err := ParseSnapshotBegin(b); return err }},
		{"level_update", 44, func(b []byte) error { _, err := ParseLevelUpdate(b); return err }},
		{"book_clear", 32, func(b []byte) error { _, err := ParseBookClear(b); return err }},
		{"snapshot_level", 28, func(b []byte) error { _, err := ParseSnapshotLevel(b); return err }},
	}
	for _, c := range cases {
		if err := c.fn(make([]byte, c.size)); err != nil {
			t.Errorf("%s: exact size %d rejected: %v", c.name, c.size, err)
		}
		if err := c.fn(make([]byte, c.size-1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: size %d accepted or wrong error: %v", c.name, c.size-1, err)
		}
		if err := c.fn(make([]byte, c.size+1)); !errors.Is(err, errTruncated) {
			t.Errorf("%s: trailing byte accepted (must be exact-length): %v", c.name, err)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... 2>&1 | head -20`
Expected: compile failure — `undefined: ParseLiquidation` and the other four.

- [ ] **Step 3: Implement the five new bodies**

Append to `marketbyprice_wire.go`. Add the new type constants to the existing constant block first:

```go
	msgTypeLiquidation   uint8 = 0x08
	msgTypeSnapshotBegin uint8 = 0x20
	msgTypeLevelUpdate   uint8 = 0x40
	msgTypeBookClear     uint8 = 0x41
	msgTypeSnapshotLevel uint8 = 0x42
```

Then:

```go
// u16Unavailable is the shared sentinel for Order Count and Level Index. It
// means "not provided, or beyond what this field can express", and saturates
// rather than wrapping. It MUST NOT be read as a magnitude: it is neither a
// count nor a rank of 65535.
const u16Unavailable uint16 = 0xFFFF

// LiquidationBody is the 44-byte body of a Liquidation message. Byte-identical
// to the top-of-book feed's 0x08, though no other parser in this repo decodes it.
// Annotates a forced Trade, keyed on Trade ID, in the same frame as that Trade.
type LiquidationBody struct {
	InstrumentID   uint32
	SourceID       uint16
	Flags          uint8 // bit 0: liquidated side (0=long, 1=short); bit 1: ADL
	Method         uint8 // 0=market, 1=backstop, 0xFF=unknown
	TradeID        uint64
	MarkPriceRaw   int64
	LiquidatedUser [20]byte
}

// ParseLiquidation decodes a Liquidation body. buf must be exactly 44 bytes.
func ParseLiquidation(buf []byte) (LiquidationBody, error) {
	if len(buf) != 44 {
		return LiquidationBody{}, fmt.Errorf("%w: expected 44 bytes for liquidation body, got %d", errTruncated, len(buf))
	}
	b := LiquidationBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:     binary.LittleEndian.Uint16(buf[4:6]),
		Flags:        buf[6],
		Method:       buf[7],
		TradeID:      binary.LittleEndian.Uint64(buf[8:16]),
		MarkPriceRaw: int64(binary.LittleEndian.Uint64(buf[16:24])),
	}
	copy(b.LiquidatedUser[:], buf[24:44])
	return b, nil
}

// SnapshotBeginBody is the 36-byte body of a SnapshotBegin message.
//
// Bytes 0-31 are byte-for-byte the market-by-order feed's 32-byte body, with
// Total Orders reading as Total Levels. Depth Bound is appended at offset 32.
// That prefix-superset rule exists so a market-by-order decoder can read a
// market-by-price frame; it does not license this decoder to accept a 32-byte
// body, so the length check is exact.
type SnapshotBeginBody struct {
	InstrumentID      uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	SnapshotID        uint32
	LastInstrumentSeq uint32
	Timestamp         time.Time
	DepthBound        uint32 // 0 = complete book; N = bounded at N levels per side
}

// ParseSnapshotBegin decodes a SnapshotBegin body. buf must be exactly 36 bytes.
func ParseSnapshotBegin(buf []byte) (SnapshotBeginBody, error) {
	if len(buf) != 36 {
		return SnapshotBeginBody{}, fmt.Errorf("%w: expected 36 bytes for snapshot_begin body, got %d", errTruncated, len(buf))
	}
	return SnapshotBeginBody{
		InstrumentID:      binary.LittleEndian.Uint32(buf[0:4]),
		AnchorSeq:         binary.LittleEndian.Uint64(buf[4:12]),
		TotalLevels:       binary.LittleEndian.Uint32(buf[12:16]),
		SnapshotID:        binary.LittleEndian.Uint32(buf[16:20]),
		LastInstrumentSeq: binary.LittleEndian.Uint32(buf[20:24]),
		Timestamp:         readTSNs(buf[24:32]),
		DepthBound:        binary.LittleEndian.Uint32(buf[32:36]),
	}, nil
}

// LevelUpdateBody is the 44-byte body of a LevelUpdate message — the core
// message of this feed. Quantity is the ABSOLUTE aggregate resting quantity at
// the price after the change, never a delta; 0 removes the level.
type LevelUpdateBody struct {
	InstrumentID     uint32
	SourceID         uint16
	Side             uint8 // 0=bid, 1=ask
	Action           uint8 // informational only; MUST NOT gate the apply
	PerInstrumentSeq uint32
	PriceRaw         int64  // the level's key
	QtyRaw           uint64 // absolute; 0 = delete
	Timestamp        time.Time
	OrderCount       uint16 // u16Unavailable = absent
	LevelIndex       uint16 // informational only; u16Unavailable = absent
	UpdateReason     uint8
	LevelFlags       uint8
}

// ParseLevelUpdate decodes a LevelUpdate body. buf must be exactly 44 bytes.
func ParseLevelUpdate(buf []byte) (LevelUpdateBody, error) {
	if len(buf) != 44 {
		return LevelUpdateBody{}, fmt.Errorf("%w: expected 44 bytes for level_update body, got %d", errTruncated, len(buf))
	}
	return LevelUpdateBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		Side:             buf[6],
		Action:           buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		PriceRaw:         int64(binary.LittleEndian.Uint64(buf[12:20])),
		QtyRaw:           binary.LittleEndian.Uint64(buf[20:28]),
		Timestamp:        readTSNs(buf[28:36]),
		OrderCount:       binary.LittleEndian.Uint16(buf[36:38]),
		LevelIndex:       binary.LittleEndian.Uint16(buf[38:40]),
		UpdateReason:     buf[40],
		LevelFlags:       buf[41],
		// bytes 42-43 are reserved padding
	}, nil
}

// BookClearBody is the 32-byte body of a BookClear message. Bulk removal of
// levels. Not a resynchronization signal: a subscriber that applies one stays
// ready.
type BookClearBody struct {
	InstrumentID     uint32
	SourceID         uint16
	ClearSide        uint8 // 0=bid, 1=ask, 2=both
	Scope            uint8 // 0=entire side, 1=from FromPrice outward
	PerInstrumentSeq uint32
	FromPriceRaw     int64 // inclusive bound when Scope=1
	Timestamp        time.Time
	ClearReason      uint8
}

// ParseBookClear decodes a BookClear body. buf must be exactly 32 bytes.
//
// Scope=1 with ClearSide=2 is malformed — one price cannot bound both sides —
// and is rejected so the caller discards and counts it.
func ParseBookClear(buf []byte) (BookClearBody, error) {
	if len(buf) != 32 {
		return BookClearBody{}, fmt.Errorf("%w: expected 32 bytes for book_clear body, got %d", errTruncated, len(buf))
	}
	b := BookClearBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		ClearSide:        buf[6],
		Scope:            buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		FromPriceRaw:     int64(binary.LittleEndian.Uint64(buf[12:20])),
		Timestamp:        readTSNs(buf[20:28]),
		ClearReason:      buf[28],
		// bytes 29-31 are reserved padding
	}
	if b.Scope == 1 && b.ClearSide == 2 {
		return b, fmt.Errorf("%w: book_clear scope=1 with clear_side=both", errMalformedBody)
	}
	return b, nil
}

// SnapshotLevelBody is the 28-byte body of a SnapshotLevel message. The
// Instrument ID is implied by the containing SnapshotBegin and is not repeated.
// Quantity is non-zero by rule; an empty level is represented by its absence.
type SnapshotLevelBody struct {
	SnapshotID uint32
	PriceRaw   int64
	QtyRaw     uint64
	OrderCount uint16 // u16Unavailable = absent
	Side       uint8  // 0=bid, 1=ask
	LevelFlags uint8
}

// ParseSnapshotLevel decodes a SnapshotLevel body. buf must be exactly 28 bytes.
func ParseSnapshotLevel(buf []byte) (SnapshotLevelBody, error) {
	if len(buf) != 28 {
		return SnapshotLevelBody{}, fmt.Errorf("%w: expected 28 bytes for snapshot_level body, got %d", errTruncated, len(buf))
	}
	return SnapshotLevelBody{
		SnapshotID: binary.LittleEndian.Uint32(buf[0:4]),
		PriceRaw:   int64(binary.LittleEndian.Uint64(buf[4:12])),
		QtyRaw:     binary.LittleEndian.Uint64(buf[12:20]),
		OrderCount: binary.LittleEndian.Uint16(buf[20:22]),
		Side:       buf[22],
		LevelFlags: buf[23],
		// bytes 24-27 are reserved padding
	}, nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 5: Cross-check every offset against the feed spec**

Open the feed spec's Message Definitions section and confirm, for each of the five new types, that the message-relative offset in the spec table minus 4 equals the body offset used above. Fix any mismatch and re-run the tests. This is the one step where a silent error produces plausible-looking garbage rather than a test failure.

- [ ] **Step 6: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: decode level update, book clear, and snapshot bodies"
```

---

## Task 4: Frame walk and record dispatch

**Files:**
- Create: `go/marketbyprice-parser/marketbyprice.go`
- Create: `go/marketbyprice-parser/parser.go`
- Test: `go/marketbyprice-parser/marketbyprice_test.go`

`parser.go` is created here rather than in Task 5 because `decodeMessage` returns a `Record` and cannot compile without it.

**Interfaces:**
- Consumes: everything from Tasks 1–3.
- Produces:
  - `Record` struct with JSON tags (fields listed in Step 3).
  - `Defects struct { SnapshotFlagMismatch int; MalformedBookClear int }` — per-frame publisher-defect counts.
  - `Parser` interface: `Name() string`, `ParseFrame(port string, frame []byte) ([]Record, Defects, error)`. **This three-value signature differs from the sibling parsers' two-value one**, which is deliberate: see Step 3.
  - `registerParser(name string, ctor func() Parser)`, `newParser(name string) (Parser, error)`.
  - `marketByPriceParser` implementing `Parser`, registered as `"marketbyprice"`. It has **no fields** and holds no state.
  - Stringers: `sideString`, `clearSideString`, `actionString`, `updateReasonString`, `clearReasonString`, `resetReasonString`, `aggressorString`, `liquidationMethodString`, `tsNS(time.Time) uint64`.

- [ ] **Step 1: Write the failing tests**

Create `marketbyprice_test.go`:

```go
package main

import (
	"encoding/binary"
	"testing"
	"time"
)

// buildFrame assembles a full frame from pre-encoded message byte slices.
func buildFrame(t *testing.T, channel uint8, seq uint64, ts time.Time, resetCount uint8, msgs ...[]byte) []byte {
	t.Helper()
	total := frameHeaderSize
	for _, m := range msgs {
		total += len(m)
	}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, channel, seq, ts, uint8(len(msgs)), resetCount, uint16(total))
	for _, m := range msgs {
		frame = append(frame, m...)
	}
	return frame
}

// buildMsg prefixes a 4-byte application message header onto a body.
func buildMsg(msgType uint8, flags uint16, body []byte) []byte {
	m := make([]byte, messageHeaderSize+len(body))
	m[0] = msgType
	m[1] = uint8(messageHeaderSize + len(body))
	binary.LittleEndian.PutUint16(m[2:4], flags)
	copy(m[messageHeaderSize:], body)
	return m
}

func levelUpdateBody(instID uint32, side uint8, piSeq uint32, price int64, qty uint64, orderCount, levelIdx uint16, action, reason uint8) []byte {
	b := make([]byte, 44)
	binary.LittleEndian.PutUint32(b[0:4], instID)
	binary.LittleEndian.PutUint16(b[4:6], 1)
	b[6] = side
	b[7] = action
	binary.LittleEndian.PutUint32(b[8:12], piSeq)
	binary.LittleEndian.PutUint64(b[12:20], uint64(price))
	binary.LittleEndian.PutUint64(b[20:28], qty)
	binary.LittleEndian.PutUint64(b[28:36], uint64(time.Unix(1700000100, 0).UnixNano()))
	binary.LittleEndian.PutUint16(b[36:38], orderCount)
	binary.LittleEndian.PutUint16(b[38:40], levelIdx)
	b[40] = reason
	return b
}

func TestParseFrame_MultipleMessages(t *testing.T) {
	p := &marketByPriceParser{}
	ts := time.Unix(1700000200, 0)
	frame := buildFrame(t, 3, 500, ts, 2,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 100, 1000, 50, 2, 0, 1, 1)),
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 1, 101, 1010, 75, 3, 0, 2, 3)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("records: got %d want 2", len(recs))
	}
	for i, r := range recs {
		if r.Type != "level_update" {
			t.Errorf("rec %d type: %q", i, r.Type)
		}
		if r.ChannelID != 3 || r.SequenceNumber != 500 || r.ResetCount != 2 || r.Port != "mktdata" {
			t.Errorf("rec %d envelope: %+v", i, r)
		}
		if r.InstrumentID != 11 {
			t.Errorf("rec %d instrument: %d", i, r.InstrumentID)
		}
	}
	if recs[0].Fields["side"] != "bid" || recs[1].Fields["side"] != "ask" {
		t.Errorf("sides: %v %v", recs[0].Fields["side"], recs[1].Fields["side"])
	}
	if recs[0].Fields["update_reason"] != "trade" || recs[1].Fields["update_reason"] != "new_order" {
		t.Errorf("reasons: %v %v", recs[0].Fields["update_reason"], recs[1].Fields["update_reason"])
	}
	if recs[0].Fields["action"] != "new" || recs[1].Fields["action"] != "change" {
		t.Errorf("actions: %v %v", recs[0].Fields["action"], recs[1].Fields["action"])
	}
}

// 0xFFFF means absent. The key must be omitted rather than carrying 65535.
func TestParseFrame_SentinelsOmitted(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000201, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, u16Unavailable, u16Unavailable, 2, 2)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if _, present := recs[0].Fields["order_count"]; present {
		t.Error("order_count must be omitted when 0xFFFF")
	}
	if _, present := recs[0].Fields["level_index"]; present {
		t.Error("level_index must be omitted when 0xFFFF")
	}
}

// Order Count 0 is a real value on a LevelUpdate, not a sentinel.
func TestParseFrame_ZeroOrderCountPresent(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000202, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 0, 0, 2, 2)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	got, present := recs[0].Fields["order_count"]
	if !present {
		t.Fatal("order_count 0 must be present")
	}
	if got.(uint16) != 0 {
		t.Errorf("order_count: got %v want 0", got)
	}
}

// Unknown types are skipped by Message Length, and the rest of the frame decodes.
func TestParseFrame_UnknownTypeSkipped(t *testing.T) {
	p := &marketByPriceParser{}
	reserved := buildMsg(0x55, 0, make([]byte, 20)) // reserved positional-index range
	frame := buildFrame(t, 0, 1, time.Unix(1700000203, 0), 0,
		reserved,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "level_update" {
		t.Fatalf("records: %+v", recs)
	}
}

// A Message Length below the 4-byte floor must error, not advance by zero and
// spin forever on one malformed datagram.
func TestParseFrame_ZeroMessageLengthTerminates(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 1, time.Unix(1700000204, 0), 1, 0, frameHeaderSize+4)
	frame = append(frame, 0x40, 0x00, 0x00, 0x00) // Type 0x40, Length 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
			t.Error("expected error for message length 0")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ParseFrame did not terminate on message length 0")
	}
}

func TestParseFrame_MessageLengthOverrunsFrame(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(mbpMagic, mbpSchemaVersion, 0, 1, time.Unix(1700000205, 0), 1, 0, frameHeaderSize+4)
	frame = append(frame, 0x40, 200, 0x00, 0x00) // claims 200 bytes, only 4 present
	if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
		t.Fatal("expected error for length overrunning the frame")
	}
}

func TestParseFrame_BadMagicRejected(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrameHeader(0x4444, mbpSchemaVersion, 0, 1, time.Unix(1700000206, 0), 0, 0, frameHeaderSize)
	if _, _, err := p.ParseFrame("mktdata", frame); err == nil {
		t.Fatal("expected error for market-by-order magic")
	}
}

// A malformed BookClear is dropped from the record stream and counted, without
// failing the whole frame — its neighbors still decode.
func TestParseFrame_MalformedBookClearDropsMessageNotFrame(t *testing.T) {
	p := &marketByPriceParser{}
	bad := make([]byte, 32)
	bad[6] = 2 // Clear Side: both
	bad[7] = 1 // Scope: from price — malformed combination
	frame := buildFrame(t, 0, 1, time.Unix(1700000207, 0), 0,
		buildMsg(msgTypeBookClear, 0, bad),
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("malformed body must not fail the frame: %v", err)
	}
	if len(recs) != 1 || recs[0].Type != "level_update" {
		t.Fatalf("records: %+v", recs)
	}
	if defects.MalformedBookClear != 1 {
		t.Errorf("malformed book clear count: got %d want 1", defects.MalformedBookClear)
	}
}

// Flags bit 0 must be set on the snapshot port and clear elsewhere. Disagreement
// is a publisher defect that is counted, never used for routing.
func TestParseFrame_SnapshotFlagMismatchCounted(t *testing.T) {
	p := &marketByPriceParser{}
	// Snapshot flag set on mktdata: wrong.
	frame := buildFrame(t, 0, 1, time.Unix(1700000208, 0), 0,
		buildMsg(msgTypeLevelUpdate, flagSnapshot, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	recs, defects, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("mismatch must not drop the record: %+v", recs)
	}
	if defects.SnapshotFlagMismatch != 1 {
		t.Errorf("mismatch count: got %d want 1", defects.SnapshotFlagMismatch)
	}

	// Snapshot flag clear on the snapshot port: also wrong.
	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 11)
	frame2 := buildFrame(t, 0, 1, time.Unix(1700000209, 0), 0,
		buildMsg(msgTypeSnapshotBegin, 0, sb),
	)
	_, defects2, err := p.ParseFrame("snapshot", frame2)
	if err != nil {
		t.Fatal(err)
	}
	if defects2.SnapshotFlagMismatch != 1 {
		t.Errorf("mismatch count: got %d want 1 (counts are per frame, not cumulative)", defects2.SnapshotFlagMismatch)
	}
}

// Correctly-flagged messages produce no defects, on either port.
func TestParseFrame_CorrectFlagsNoDefects(t *testing.T) {
	p := &marketByPriceParser{}
	frame := buildFrame(t, 0, 1, time.Unix(1700000213, 0), 0,
		buildMsg(msgTypeLevelUpdate, 0, levelUpdateBody(11, 0, 1, 1000, 50, 1, 0, 1, 1)),
	)
	if _, d, err := p.ParseFrame("mktdata", frame); err != nil || d.SnapshotFlagMismatch != 0 {
		t.Errorf("mktdata clean frame: defects=%+v err=%v", d, err)
	}

	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 11)
	frame2 := buildFrame(t, 0, 1, time.Unix(1700000214, 0), 0,
		buildMsg(msgTypeSnapshotBegin, flagSnapshot, sb),
	)
	if _, d, err := p.ParseFrame("snapshot", frame2); err != nil || d.SnapshotFlagMismatch != 0 {
		t.Errorf("snapshot clean frame: defects=%+v err=%v", d, err)
	}
}

func TestParseFrame_SnapshotGroupDecodes(t *testing.T) {
	p := &marketByPriceParser{}
	sb := make([]byte, 36)
	binary.LittleEndian.PutUint32(sb[0:4], 42)
	binary.LittleEndian.PutUint64(sb[4:12], 7000)
	binary.LittleEndian.PutUint32(sb[12:16], 1)
	binary.LittleEndian.PutUint32(sb[16:20], 5)
	binary.LittleEndian.PutUint32(sb[20:24], 600)
	binary.LittleEndian.PutUint32(sb[32:36], 25) // Depth Bound: bounded

	sl := make([]byte, 28)
	binary.LittleEndian.PutUint32(sl[0:4], 5)
	binary.LittleEndian.PutUint64(sl[4:12], uint64(int64(1234)))
	binary.LittleEndian.PutUint64(sl[12:20], 900)
	binary.LittleEndian.PutUint16(sl[20:22], 3)
	sl[22] = 1 // ask

	se := make([]byte, 16)
	binary.LittleEndian.PutUint32(se[0:4], 42)
	binary.LittleEndian.PutUint64(se[4:12], 7000)
	binary.LittleEndian.PutUint32(se[12:16], 5)

	frame := buildFrame(t, 0, 10, time.Unix(1700000210, 0), 0,
		buildMsg(msgTypeSnapshotBegin, flagSnapshot, sb),
		buildMsg(msgTypeSnapshotLevel, flagSnapshot, sl),
		buildMsg(msgTypeSnapshotEnd, flagSnapshot, se),
	)
	recs, _, err := p.ParseFrame("snapshot", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("records: got %d want 3", len(recs))
	}
	if recs[0].Type != "snapshot_begin" || recs[1].Type != "snapshot_level" || recs[2].Type != "snapshot_end" {
		t.Fatalf("types: %s %s %s", recs[0].Type, recs[1].Type, recs[2].Type)
	}
	if recs[0].Fields["depth_bound"].(uint32) != 25 {
		t.Errorf("depth_bound: %v", recs[0].Fields["depth_bound"])
	}
	if recs[0].Fields["total_levels"].(uint32) != 1 {
		t.Errorf("total_levels: %v", recs[0].Fields["total_levels"])
	}
	// SnapshotLevel carries no Instrument ID; it is implied by the group.
	if recs[1].InstrumentID != 0 {
		t.Errorf("snapshot_level must not invent an instrument id: %d", recs[1].InstrumentID)
	}
	if recs[1].Fields["side"] != "ask" {
		t.Errorf("side: %v", recs[1].Fields["side"])
	}
}

func TestParseFrame_TradeAndLiquidation(t *testing.T) {
	p := &marketByPriceParser{}
	tb := make([]byte, 48)
	binary.LittleEndian.PutUint32(tb[0:4], 9)
	tb[6] = 2 // Aggressor: sell
	binary.LittleEndian.PutUint64(tb[8:16], uint64(time.Unix(1700000211, 0).UnixNano()))
	binary.LittleEndian.PutUint64(tb[32:40], 4242)

	lb := make([]byte, 44)
	binary.LittleEndian.PutUint32(lb[0:4], 9)
	lb[7] = 1 // Method: backstop
	binary.LittleEndian.PutUint64(lb[8:16], 4242)

	frame := buildFrame(t, 0, 20, time.Unix(1700000212, 0), 0,
		buildMsg(msgTypeTrade, 0, tb),
		buildMsg(msgTypeLiquidation, 0, lb),
	)
	recs, _, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || recs[0].Type != "trade" || recs[1].Type != "liquidation" {
		t.Fatalf("records: %+v", recs)
	}
	if recs[0].Fields["aggressor_side"] != "sell" {
		t.Errorf("aggressor: %v", recs[0].Fields["aggressor_side"])
	}
	// The liquidation pairs with its trade by Trade ID, in the same frame.
	if recs[0].Fields["trade_id"].(uint64) != recs[1].Fields["trade_id"].(uint64) {
		t.Error("trade id must match between trade and liquidation")
	}
	if recs[1].Fields["method"] != "backstop" {
		t.Errorf("method: %v", recs[1].Fields["method"])
	}
}

func TestParserRegistry(t *testing.T) {
	p, err := newParser("marketbyprice")
	if err != nil {
		t.Fatal(err)
	}
	if p.Name() != "marketbyprice" {
		t.Errorf("name: %q", p.Name())
	}
	if _, err := newParser("nope"); err == nil {
		t.Error("expected error for unknown parser")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `go test ./... 2>&1 | head -20`
Expected: compile failure — `undefined: marketByPriceParser`, `undefined: Record`.

- [ ] **Step 3: Write `parser.go`**

Copy `go/marketbyorder-parser/parser.go`. `Record`, `parserRegistry`, `registerParser`, and `newParser` are feed-independent and need no changes. Make exactly one change — the `Parser` interface gains a third return value:

```go
// Parser decodes a wire frame received on a given port and returns zero or
// more Records, plus any publisher defects observed in that frame.
//
// Defects are returned per frame rather than accumulated on the Parser because
// the Runner shares one Parser across all three port goroutines; a counter
// field on the Parser would be a data race. This is why the signature differs
// from the topofbook and marketbyorder parsers, which surface no defect counts.
//
// A return of (nil, _, nil) means the frame was valid but produced no records.
// A non-nil error indicates the frame should be dropped and a counter incremented.
type Parser interface {
	Name() string
	ParseFrame(port string, frame []byte) ([]Record, Defects, error)
}
```

For reference, `Record` is unchanged from the sibling:

```go
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	SourceTSNS     uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS       uint64         `json:"send_ts_ns,omitempty"`
	RecvTSNS       uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind     string         `json:"recv_ts_kind,omitempty"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}
```

- [ ] **Step 4: Write `marketbyprice.go`**

Create it with the registration, defect counters, frame walk, dispatch, and stringers:

```go
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

func init() {
	registerParser("marketbyprice", func() Parser { return &marketByPriceParser{} })
}

// Defects counts publisher-side protocol violations the spec asks a subscriber
// to surface, for one frame. Observability only; they never change decoding.
type Defects struct {
	SnapshotFlagMismatch int
	MalformedBookClear   int
}

// marketByPriceParser is stateless. It deliberately holds no counters: the
// Runner shares one instance across all three port goroutines, so any mutable
// field here would be a data race. Defect counts are returned per frame instead.
type marketByPriceParser struct{}

func (p *marketByPriceParser) Name() string { return "marketbyprice" }

// ParseFrame decodes one frame and returns one Record per application message,
// plus the defects observed in this frame.
//
// A malformed individual message is dropped and counted; it does not fail the
// frame, because its neighbors are independently valid. A malformed frame
// structure (bad header, or a Message Length that cannot be trusted to advance
// the walk) fails the frame.
func (p *marketByPriceParser) ParseFrame(port string, frame []byte) ([]Record, Defects, error) {
	var defects Defects

	hdr, err := ParseFrameHeader(frame)
	if err != nil {
		return nil, defects, fmt.Errorf("header: %w", err)
	}

	body := frame[frameHeaderSize:]
	records := make([]Record, 0, hdr.MessageCount)

	for i := uint8(0); i < hdr.MessageCount; i++ {
		mh, err := ParseMessageHeader(body)
		if err != nil {
			return nil, defects, fmt.Errorf("msg %d header: %w", i, err)
		}
		// The < 4 floor matters for more than validation: without it a length
		// of 0 advances the walk by zero bytes and spins forever.
		if int(mh.Length) < messageHeaderSize {
			return nil, defects, fmt.Errorf("%w: msg %d length %d", errMessageLength, i, mh.Length)
		}
		if int(mh.Length) > len(body) {
			return nil, defects, fmt.Errorf("%w: msg %d length %d > %d remaining", errMessageLength, i, mh.Length, len(body))
		}
		msgBody := body[messageHeaderSize:mh.Length]

		// Flags bit 0 must be set on the snapshot port and clear on the other
		// two. Disagreement is a publisher defect; it never affects routing,
		// which uses Type ID and port only.
		if set := mh.Flags&flagSnapshot != 0; (port == "snapshot") != set {
			defects.SnapshotFlagMismatch++
		}

		rec, ok, decErr := p.decodeMessage(port, hdr, mh, msgBody)

		// Advance BEFORE any early-continue below. A `continue` that skips this
		// leaves the walk pointing at the message just consumed, so the next
		// iteration re-parses it and every subsequent message is misaligned.
		body = body[mh.Length:]

		if decErr != nil {
			// A body the spec declares malformed is dropped and counted, not
			// escalated to a frame failure — its neighbors are independently valid.
			if errors.Is(decErr, errMalformedBody) {
				if mh.Type == msgTypeBookClear {
					defects.MalformedBookClear++
				}
				continue
			}
			return nil, defects, fmt.Errorf("msg %d type 0x%02x: %w", i, mh.Type, decErr)
		}
		if ok {
			records = append(records, rec)
		}
	}

	return records, defects, nil
}

func (p *marketByPriceParser) decodeMessage(port string, hdr FrameHeader, mh MessageHeader, body []byte) (Record, bool, error) {
	base := Record{
		Timestamp:      hdr.SendTimestamp,
		SendTSNS:       tsNS(hdr.SendTimestamp),
		ChannelID:      hdr.ChannelID,
		Port:           port,
		SequenceNumber: hdr.Sequence,
		ResetCount:     hdr.ResetCount,
	}

	switch mh.Type {
	case msgTypeHeartbeat:
		b, err := ParseHeartbeat(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "heartbeat"
		base.Fields = map[string]any{
			"channel_id_in_body": b.ChannelID,
			"timestamp":          b.Timestamp,
		}
		return base, true, nil

	case msgTypeInstrumentDefinition:
		b, err := ParseInstrumentDefinition(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "instrument_definition"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"symbol":         b.Symbol,
			"leg1":           b.Leg1,
			"leg2":           b.Leg2,
			"asset_class":    b.AssetClass,
			"price_exponent": b.PriceExponent,
			"qty_exponent":   b.QtyExponent,
			"market_model":   b.MarketModel,
			"tick_size_raw":  b.TickSizeRaw,
			"lot_size_raw":   b.LotSizeRaw,
			"contract_value": b.ContractValue,
			"expiry":         b.Expiry,
			"settle_type":    b.SettleType,
			"price_bound":    b.PriceBound,
			"manifest_seq":   b.ManifestSeq,
		}
		return base, true, nil

	case msgTypeTrade:
		b, err := ParseTrade(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "trade"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.SourceTimestamp)
		base.Fields = map[string]any{
			"source_id":             b.SourceID,
			"aggressor_side":        aggressorString(b.AggressorSide),
			"trade_flags":           b.TradeFlags,
			"source_timestamp":      b.SourceTimestamp,
			"trade_price_raw":       b.TradePriceRaw,
			"trade_qty_raw":         b.TradeQtyRaw,
			"trade_id":              b.TradeID,
			"cumulative_volume_raw": b.CumulativeVolumeRaw,
		}
		return base, true, nil

	case msgTypeEndOfSession:
		b, err := ParseEndOfSession(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "end_of_session"
		base.Fields = map[string]any{"timestamp": b.Timestamp}
		return base, true, nil

	case msgTypeManifestSummary:
		b, err := ParseManifestSummary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "manifest_summary"
		base.Fields = map[string]any{
			"valid":            b.Valid,
			"manifest_seq":     b.ManifestSeq,
			"instrument_count": b.InstrumentCount,
			"timestamp":        b.Timestamp,
		}
		return base, true, nil

	case msgTypeLiquidation:
		b, err := ParseLiquidation(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "liquidation"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"liquidation_flags":  b.Flags,
			"liquidated_side":    liquidatedSideString(b.Flags),
			"adl":                b.Flags&0x02 != 0,
			"method":             liquidationMethodString(b.Method),
			"trade_id":           b.TradeID,
			"mark_price_raw":     b.MarkPriceRaw,
			"liquidated_user":    hex.EncodeToString(b.LiquidatedUser[:]),
		}
		return base, true, nil

	case msgTypeLevelUpdate:
		b, err := ParseLevelUpdate(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "level_update"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"side":               sideString(b.Side),
			"action":             actionString(b.Action),
			"per_instrument_seq": b.PerInstrumentSeq,
			"price_raw":          b.PriceRaw,
			"qty_raw":            b.QtyRaw,
			"timestamp":          b.Timestamp,
			"update_reason":      updateReasonString(b.UpdateReason),
			"level_flags":        b.LevelFlags,
			"implied":            b.LevelFlags&0x01 != 0,
			"amm_synthetic":      b.LevelFlags&0x02 != 0,
		}
		// 0xFFFF means absent. Omit rather than emit a number that would read
		// as a count or rank of 65535.
		if b.OrderCount != u16Unavailable {
			base.Fields["order_count"] = b.OrderCount
		}
		if b.LevelIndex != u16Unavailable {
			base.Fields["level_index"] = b.LevelIndex
		}
		return base, true, nil

	case msgTypeBookClear:
		b, err := ParseBookClear(body)
		if err != nil {
			// ParseFrame counts the malformed case and drops the message.
			return Record{}, false, err
		}
		base.Type = "book_clear"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"clear_side":         clearSideString(b.ClearSide),
			"scope":              clearScopeString(b.Scope),
			"per_instrument_seq": b.PerInstrumentSeq,
			"timestamp":          b.Timestamp,
			"clear_reason":       clearReasonString(b.ClearReason),
		}
		// From Price is only meaningful when Scope = 1.
		if b.Scope == 1 {
			base.Fields["from_price_raw"] = b.FromPriceRaw
		}
		return base, true, nil

	case msgTypeBatchBoundary:
		b, err := ParseBatchBoundary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "batch_boundary"
		// A framing/control message. Batch Time is a batch marker rather than a
		// venue timestamp for a book event, so it gets no source_ts.
		base.Fields = map[string]any{
			"batch_id": b.BatchID,
			"batch_ts": b.BatchTime,
		}
		return base, true, nil

	case msgTypeInstrumentReset:
		b, err := ParseInstrumentReset(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "instrument_reset"
		base.InstrumentID = b.InstrumentID
		base.SourceTSNS = tsNS(b.Timestamp)
		base.Fields = map[string]any{
			"reason":         resetReasonString(b.Reason),
			"new_anchor_seq": b.NewAnchorSeq,
			"timestamp":      b.Timestamp,
		}
		return base, true, nil

	case msgTypeSnapshotBegin:
		b, err := ParseSnapshotBegin(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_begin"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"anchor_seq":          b.AnchorSeq,
			"total_levels":        b.TotalLevels,
			"snapshot_id":         b.SnapshotID,
			"last_instrument_seq": b.LastInstrumentSeq,
			"timestamp":           b.Timestamp,
			"depth_bound":         b.DepthBound,
		}
		return base, true, nil

	case msgTypeSnapshotLevel:
		// No Instrument ID on the wire: the containing SnapshotBegin implies it,
		// so InstrumentID stays 0 and the consumer associates by snapshot_id.
		b, err := ParseSnapshotLevel(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_level"
		base.Fields = map[string]any{
			"snapshot_id":   b.SnapshotID,
			"price_raw":     b.PriceRaw,
			"qty_raw":       b.QtyRaw,
			"side":          sideString(b.Side),
			"level_flags":   b.LevelFlags,
			"implied":       b.LevelFlags&0x01 != 0,
			"amm_synthetic": b.LevelFlags&0x02 != 0,
		}
		if b.OrderCount != u16Unavailable {
			base.Fields["order_count"] = b.OrderCount
		}
		return base, true, nil

	case msgTypeSnapshotEnd:
		b, err := ParseSnapshotEnd(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_end"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"anchor_seq":  b.AnchorSeq,
			"snapshot_id": b.SnapshotID,
		}
		return base, true, nil

	default:
		// Unknown type — skip per the forward-compatibility rule. This covers the
		// reserved 0x50-0x5F positional-index range. Caller advances by mh.Length.
		return Record{}, false, nil
	}
}

// --- enum stringers ---
//
// The spec requires receivers to accept any u8 and to treat unrecognised values
// as the unknown member, and permits new values without a Schema Version bump.
// An unrecognised value is therefore never an error.

func sideString(s uint8) string {
	switch s {
	case 0:
		return "bid"
	case 1:
		return "ask"
	default:
		return "unknown"
	}
}

func clearSideString(s uint8) string {
	switch s {
	case 0:
		return "bid"
	case 1:
		return "ask"
	case 2:
		return "both"
	default:
		return "unknown"
	}
}

func clearScopeString(s uint8) string {
	switch s {
	case 0:
		return "entire_side"
	case 1:
		return "from_price"
	default:
		return "unknown"
	}
}

func actionString(a uint8) string {
	switch a {
	case 0:
		return "unknown"
	case 1:
		return "new"
	case 2:
		return "change"
	case 3:
		return "delete"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func updateReasonString(r uint8) string {
	switch r {
	case 0:
		return "unknown"
	case 1:
		return "trade"
	case 2:
		return "cancel"
	case 3:
		return "new_order"
	case 4:
		return "amend"
	case 5:
		return "venue_action"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func clearReasonString(r uint8) string {
	switch r {
	case 0:
		return "unspecified"
	case 1:
		return "halt"
	case 2:
		return "session_end"
	case 3:
		return "venue_reset"
	case 4:
		return "settled"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func resetReasonString(r uint8) string {
	switch r {
	case 0:
		return "unspecified"
	case 1:
		return "publisher_inconsistency"
	case 2:
		return "venue_resync"
	case 3:
		return "upstream_gap"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func aggressorString(s uint8) string {
	switch s {
	case 1:
		return "buy"
	case 2:
		return "sell"
	default:
		return "unknown"
	}
}

func liquidationMethodString(m uint8) string {
	switch m {
	case 0:
		return "market"
	case 1:
		return "backstop"
	case 255:
		return "unknown"
	default:
		return "unknown"
	}
}

// liquidatedSideString reads Liquidation Flags bit 0.
func liquidatedSideString(flags uint8) string {
	if flags&0x01 != 0 {
		return "short"
	}
	return "long"
}

// tsNS returns Unix-nanos for a non-zero time, else 0 (absent).
func tsNS(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./... -v 2>&1 | tail -40`
Expected: all PASS.

- [ ] **Step 6: Vet and commit**

```bash
go vet ./...
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: add frame walk and record dispatch"
```

---

## Task 5: Metrics

`metrics.go` comes before the sinks and the runner because both reference `*Metrics` and neither compiles without it.

**Files:**
- Create: `go/marketbyprice-parser/metrics.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `Metrics` struct with fields `IngressPackets`, `IngressBytes`, `ParseErrors`, `RecordsTotal`, `SourceLatency`, `SendLatency`, `SocketClients`, `SocketClientDrops`, `SocketRecordsSent`, `SinkWriteErrors`, `FrameSeqGaps`, `FramesMissing`, `SnapshotFlagMismatch`, `MalformedMessages`, `BuildInfo`, `UptimeSeconds`; `NewMetrics(version, commit string) *Metrics`; `(*Metrics).ServeHTTP(ctx context.Context, addr string, logErr func(error))`.

- [ ] **Step 1: Copy the sibling metrics file**

Copy `go/marketbyorder-parser/metrics.go` to `go/marketbyprice-parser/metrics.go` verbatim.

- [ ] **Step 2: Change the namespace**

```go
const metricsNamespace = "dz_mbp_parser"
```

- [ ] **Step 3: Add the two defect counter fields**

In the `Metrics` struct, directly after the `FramesMissing` field:

```go
	// Publisher defects the spec asks a subscriber to surface. Observability
	// only; neither affects decoding or routing.
	SnapshotFlagMismatch *prometheus.CounterVec
	MalformedMessages    *prometheus.CounterVec
```

- [ ] **Step 4: Construct them**

In `NewMetrics`, immediately before the `m.BuildInfo = ...` block:

```go
	m.SnapshotFlagMismatch = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "snapshot_flag_mismatch_total",
		Help: "Application-header Flags bit 0 disagreeing with the arrival port; a publisher defect. Never used for routing.",
	}, []string{"port"})

	m.MalformedMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "malformed_total",
		Help: "Individual messages the spec declares malformed, dropped without failing their frame.",
	}, []string{"reason"})
```

- [ ] **Step 5: Register them**

In the `reg.MustRegister(...)` call, extend the line reading `m.FrameSeqGaps, m.FramesMissing,` to:

```go
		m.FrameSeqGaps, m.FramesMissing, m.SnapshotFlagMismatch, m.MalformedMessages,
```

- [ ] **Step 6: Verify it compiles and metrics are registered**

Write a temporary check — create `metrics_smoke_test.go`:

Only two probes are valid before any observation: `build_info` (set inside `NewMetrics`) and `uptime_seconds` (a `GaugeFunc`). Every `*Vec` reports no metric family until a label set is observed, so each one must be touched before it can be asserted on.

```go
package main

import (
	"strings"
	"testing"
)

// gatheredNames returns the metric family names the registry currently reports.
func gatheredNames(t *testing.T, m *Metrics) []string {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}

func mustContain(t *testing.T, names []string, want string) {
	t.Helper()
	for _, n := range names {
		if n == want {
			return
		}
	}
	t.Errorf("missing %s in %v", want, names)
}

func TestMetricsNamespaceAndDefectCounters(t *testing.T) {
	m := NewMetrics("test", "abc123")

	// build_info is set and uptime_seconds is a GaugeFunc, so both carry values
	// as soon as NewMetrics runs. They are gathered without any observation,
	// which makes them the right probes for the namespace prefix.
	names := gatheredNames(t, m)
	mustContain(t, names, "dz_mbp_parser_build_info")
	mustContain(t, names, "dz_mbp_parser_uptime_seconds")

	// A CounterVec reports no metric family until a label set is observed, so
	// touch each vec before asserting on it.
	m.FrameSeqGaps.WithLabelValues("mktdata").Inc()
	m.SnapshotFlagMismatch.WithLabelValues("mktdata").Inc()
	m.MalformedMessages.WithLabelValues("bookclear_scope_side").Inc()

	names = gatheredNames(t, m)
	for _, want := range []string{
		"dz_mbp_parser_frame_seq_gaps_total",
		"dz_mbp_parser_snapshot_flag_mismatch_total",
		"dz_mbp_parser_malformed_total",
	} {
		mustContain(t, names, want)
	}

	// This module must not register anything under a sibling feed's namespace.
	// Copying metrics.go from marketbyorder-parser and missing the namespace
	// constant is the exact mistake this guards.
	for _, n := range names {
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under a sibling feed namespace", n)
		}
	}
}
```

Run: `go test ./... -run TestMetricsNamespace -v`
Expected: PASS. Keep this test — it guards the namespace against a copy-paste regression.

- [ ] **Step 7: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: add metrics with defect counters"
```

---

## Task 6: Output sinks

**Files:**
- Create: `go/marketbyprice-parser/sink.go`
- Create: `go/marketbyprice-parser/sink_json.go`
- Create: `go/marketbyprice-parser/sink_socket.go`
- Test: `go/marketbyprice-parser/sink_json_test.go`
- Test: `go/marketbyprice-parser/sink_socket_test.go`

**Interfaces:**
- Consumes: `Record` from Task 4, `*Metrics` from Task 5.
- Produces: `OutputSink` interface (`Write([]Record) error`, `Close() error`), `SinkConfig{Format, Path string; Metrics *Metrics}`, `NewSink(SinkConfig) (OutputSink, error)`, `NewJSONFileSink(path string) (OutputSink, error)`, `NewSocketSink(format, path string, m *Metrics) (OutputSink, error)`.

- [ ] **Step 1: Copy the three sink files**

Copy from `go/marketbyorder-parser/`: `sink.go`, `sink_json.go`, `sink_socket.go`. Change exactly one thing — the error string in `NewSink`'s default branch:

```go
	default:
		return nil, fmt.Errorf("unsupported format: %q (marketbyprice supports json only)", cfg.Format)
```

Everything else is feed-independent: the socket sink broadcasts newline-delimited JSON to all connected clients and drops slow ones, which is identical behavior for this feed.

- [ ] **Step 2: Copy the two sink test files**

Copy `sink_json_test.go` and `sink_socket_test.go` from `go/marketbyorder-parser/`. Inside them, replace any market-by-order-specific record fixture with a market-by-price one — the record `Type` values `"order_add"` / `"order_cancel"` become `"level_update"` / `"book_clear"`, and any `fields` map contents become the level-update field names from Task 4. The sink is generic over `Record`, so no other change is needed.

- [ ] **Step 3: Run the tests**

Run: `go test ./... -run 'Sink'`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: add json and socket sinks"
```

---

## Task 7: Runner, sequence tracking, and receive timestamps

**Files:**
- Create: `go/marketbyprice-parser/runner.go`
- Create: `go/marketbyprice-parser/timestamp_linux.go`
- Create: `go/marketbyprice-parser/timestamp_other.go`
- Test: `go/marketbyprice-parser/seqtracker_test.go`

**Interfaces:**
- Consumes: `Parser`, `Record`, `Defects` (Task 4); `*Metrics` (Task 5); `OutputSink` (Task 6).
- Produces: `seqTracker` with `observe(seq uint64) (gaps, missing uint64)`; `portConfig{Label string; Port int}`; `Runner` with `NewRunner(parser Parser, sink OutputSink, metrics *Metrics, group, iface string, refdata, mktdata, snapshot int) (*Runner, error)` and `Run(ctx context.Context) error`; `enableTimestamping(*net.UDPConn) error`, `readDatagram(*net.UDPConn, []byte) (int, time.Time, string, error)`; `classifyError(error) string`.

- [ ] **Step 1: Write the failing sequence tracker tests**

Copy `go/marketbyorder-parser/seqtracker_test.go` verbatim into `go/marketbyprice-parser/seqtracker_test.go`. It covers first-observation initialization, contiguous advance, a forward gap reporting both the event and the magnitude, and reorders and duplicates being ignored. Those semantics are identical for this feed.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./... 2>&1 | head -10`
Expected: compile failure — `undefined: seqTracker`.

- [ ] **Step 3: Write the timestamp files**

Copy `timestamp_linux.go` and `timestamp_other.go` from `go/marketbyorder-parser/` verbatim. They provide `enableTimestamping` and `readDatagram`, giving `SO_TIMESTAMPNS` kernel receive timestamps on Linux and a userspace `time.Now()` fallback elsewhere. Feed-independent.

- [ ] **Step 4: Write `runner.go`**

Copy `go/marketbyorder-parser/runner.go`, then make these changes:

1. `ParseFrame` returns per-frame defect counts as its second value (see Task 4). Change the call in `receive` from the sibling's two-value form to:

```go
		records, defects, perr := r.parser.ParseFrame(port, buf[:n])
		if perr != nil {
			r.metrics.ParseErrors.WithLabelValues(port, classifyError(perr)).Inc()
			continue
		}

		// Per-frame defect counts. Returned rather than accumulated on the
		// parser, because one Parser is shared by all three port goroutines and
		// a counter field on it would be a data race.
		if defects.SnapshotFlagMismatch > 0 {
			r.metrics.SnapshotFlagMismatch.WithLabelValues(port).Add(float64(defects.SnapshotFlagMismatch))
		}
		if defects.MalformedBookClear > 0 {
			r.metrics.MalformedMessages.WithLabelValues("bookclear_scope_side").Add(float64(defects.MalformedBookClear))
		}
```

2. In `classifyError`, add the `errMalformedBody` case before `default`:

```go
	case errors.Is(err, errMalformedBody):
		return "malformed_body"
```

3. Add `message_length_underflow` accounting: in `classifyError`, `errMessageLength` already maps to `"frame_length"`. Leave that mapping alone for parity with the sibling parsers — the dedicated `malformed_total` counter covers the spec-specific cases and the parse-error histogram keeps its existing shape.

Everything else — the multicast open with a 64 MiB read buffer, the 500 ms read deadline so `ctx` cancellation is observed, the `refdata` exclusion from frame-sequence gap tracking, and `observeLatencies` — copies unchanged.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 6: Check for data races**

Run: `go test -race ./...`
Expected: PASS with no race reports. One `Parser` is shared by three port goroutines, so this is the check that the per-frame defect return actually avoided shared mutable state.

- [ ] **Step 7: Vet and commit**

```bash
go vet ./...
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: add runner and receive timestamps"
```

---

## Task 8: Binary entry point, container, and docs

**Files:**
- Create: `go/marketbyprice-parser/main.go`
- Create: `go/marketbyprice-parser/Dockerfile`
- Create: `go/marketbyprice-parser/README.md`
- Modify: `go/marketbyorder-parser/Dockerfile` (add this module's `go.mod` to the workspace copy list)
- Modify: `go/topofbook-parser/Dockerfile`, `go/marketbyorder-bot/Dockerfile`, `go/topofbook-bot/Dockerfile` (same one-line addition)

Every Dockerfile in the workspace copies each `go.work` member's `go.mod` so `go mod download` resolves. Adding a module to `go.work` breaks all of them until they copy the new file, so this task fixes them.

**Interfaces:**
- Consumes: `newParser` (Task 4), `NewSink`/`SinkConfig` (Task 5), `NewMetrics`/`NewRunner` (Task 6).
- Produces: the `dz-marketbyprice-parser` binary. Flags: `--group`, `--refdata-port`, `--mktdata-port`, `--snapshot-port`, `--interface`, `--output`, `--format`, `--parser`, `--metrics-addr`, `-v`, `--version`.

- [ ] **Step 1: Write `main.go`**

Copy `go/marketbyorder-parser/main.go` and change every occurrence of the feed name:

- `parserName` flag default becomes `"marketbyprice"`, help text `"parser name from registry"`.
- `--version` output becomes `fmt.Printf("marketbyprice-parser %s (%s)\n", version, commit)`.
- The startup log line becomes:

```go
	log.Printf("marketbyprice-parser %s started: group=%s refdata=:%d mktdata=:%d snapshot=:%d output=%s",
		version, *group, *refdataPort, *mktdataPort, *snapshotPort, *output)
```

The required-flag validation (`--group`, all three ports, `--output`), signal handling, and metrics server wiring are unchanged.

- [ ] **Step 2: Verify the binary builds and its flags work**

Run:
```bash
go build -o /tmp/dz-marketbyprice-parser . && /tmp/dz-marketbyprice-parser --version
```
Expected: prints `marketbyprice-parser 0.1.0-dev (unknown)`.

Run: `/tmp/dz-marketbyprice-parser 2>&1 | head -3`
Expected: the required-flags error and usage, exit status 2.

- [ ] **Step 3: Write the Dockerfile**

Copy `go/marketbyorder-parser/Dockerfile` and change every `marketbyorder-parser` to `marketbyprice-parser` and every `dz-marketbyorder-parser` to `dz-marketbyprice-parser`. In the dependency-copy block, replace the line copying this module's own `go.mod` with the sibling's, so the block lists every *other* workspace member plus its own:

```dockerfile
COPY go/marketbyprice-parser/go.mod ./go/marketbyprice-parser/
...
COPY go/marketbyorder-parser/go.mod ./go/marketbyorder-parser/
COPY go/marketbyorder-bot/go.mod ./go/marketbyorder-bot/
```

Then `WORKDIR /src/go/marketbyprice-parser` and `COPY go/marketbyprice-parser/ ./`.

- [ ] **Step 4: Add this module to the four existing Dockerfiles**

In each of `go/marketbyorder-parser/Dockerfile`, `go/topofbook-parser/Dockerfile`, `go/marketbyorder-bot/Dockerfile`, and `go/topofbook-bot/Dockerfile`, add this line to the workspace-member copy block:

```dockerfile
COPY go/marketbyprice-parser/go.mod ./go/marketbyprice-parser/
```

- [ ] **Step 5: Verify the container builds**

Run from the repo root:
```bash
docker build -f go/marketbyprice-parser/Dockerfile -t dz/marketbyprice-parser:test .
```
Expected: build succeeds. If Docker is unavailable in this environment, say so explicitly in the task report rather than marking this step done — do not claim a build that did not run.

- [ ] **Step 6: Write `README.md`**

Model it on `go/marketbyorder-parser/README.md`. Cover: what the parser does, the three-port channel model and which message types arrive on each port, a build command, a run example with all required flags, the JSON record envelope with one real `level_update` example line, the full metric list including the two defect counters, and a link to the feed spec. State plainly that the parser is stateless and does not reconstruct books, and point at the bot for that.

- [ ] **Step 7: Full verification**

Run from `go/`:
```bash
go vet ./marketbyprice-parser/... && go test ./marketbyprice-parser/... && go build -o /tmp/dz-marketbyprice-parser ./marketbyprice-parser/
```
Expected: all green. This is the first task where `go build` can succeed, because `main.go` now exists.

Then confirm the other modules still vet and test, from `go/`:
```bash
for m in marketbyorder-bot marketbyorder-parser internal kernel-receiver topofbook-bot topofbook-parser; do (cd $m && go vet ./... && go test ./... >/dev/null && echo "$m ok"); done
```
Expected: six `ok` lines. `xdp-receiver` is excluded on purpose — it does not build for a pre-existing reason unrelated to this work (see Verification commands). Do not attempt to fix it.

- [ ] **Step 8: Commit**

```bash
git add go/marketbyprice-parser/ go/marketbyorder-parser/Dockerfile go/topofbook-parser/Dockerfile go/marketbyorder-bot/Dockerfile go/topofbook-bot/Dockerfile
git commit -m "marketbyprice-parser: add entry point, container, and readme"
```

---

## Done criteria

- From `go/`: `go vet ./marketbyprice-parser/...`, `go test ./marketbyprice-parser/...`, and `go test -race ./marketbyprice-parser/...` are green, and `go build -o /tmp/dz-marketbyprice-parser ./marketbyprice-parser/` succeeds.
- The six other buildable modules still vet and test clean; `xdp-receiver` remains untouched in its pre-existing broken state.
- Every one of the 13 message types has a byte-exact decode test.
- Sentinel omission, unknown-type skip, `Message Length = 0` termination, and malformed-`BookClear` drop-without-frame-failure each have a test.
- `dz-marketbyprice-parser --version` runs; the container builds.
- No book state anywhere in the module.

## Follow-on plans (not this plan)

- **Bot** — `go/marketbyprice-bot`: book state machine, the five spec behaviors in §Component 2 of the design, ClickHouse persistence, `03_schema_mbp.sql`.
- **Demo stack** — compose services, `.env.example`, Prometheus jobs, Grafana dashboard, README and `docs/hyperliquid.md` port table. Blocked on the live feed's group, port sets, and channel ID.
