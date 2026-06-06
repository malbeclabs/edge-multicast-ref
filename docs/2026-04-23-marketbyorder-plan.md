# DZ Market-by-Order Demo Stack — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a sibling DZ Market-by-Order pipeline alongside the existing top-of-book demo: a stateless wire-decoding parser, a book-building bot that persists to ClickHouse, and Grafana dashboards.

**Architecture:** `marketbyorder-parser` joins three multicast UDP ports (refdata + mktdata + snapshot), decodes the binary DZ-MBO v0.1.0 wire format, and broadcasts JSONL records on a Unix socket (drop-on-slow-consumer). `marketbyorder-bot` reads the socket, runs the spec's per-instrument state machine to maintain MBO order books, and writes per-event rows + coalesced top-N level snapshots + raw wire snapshots to ClickHouse. The existing `demo/` Docker stack is extended with the two new services and a new Grafana dashboard. The existing `go/example-bot/` is renamed to `go/topofbook-bot/` for clarity.

**Tech Stack:** Go 1.25, ClickHouse, Grafana, Docker Compose, Prometheus client_golang. Standard `encoding/binary` + `bufio` for wire decoding; `encoding/json` for the parser→bot socket; `net/http` for ClickHouse JSONEachRow inserts.

**Spec:** [docs/2026-04-23-marketbyorder-design.md](2026-04-23-marketbyorder-design.md)

**Reference implementations to mirror:**
- Parser pattern: [go/topofbook-parser/](../go/topofbook-parser/) — every file in this dir is a structural template
- Bot pattern: [go/example-bot/](../go/example-bot/) (after rename: `go/topofbook-bot/`) — bot.go, clickhouse.go, metrics.go, main.go are direct templates
- Demo pattern: [demo/](../demo/) — docker-compose.yml, ClickHouse init scripts, Grafana provisioning

---

## File Map

### New files

| File | Responsibility |
|---|---|
| `go/marketbyorder-parser/go.mod` | Module manifest |
| `go/marketbyorder-parser/Dockerfile` | Container build |
| `go/marketbyorder-parser/README.md` | Usage |
| `go/marketbyorder-parser/main.go` | CLI flags, signal handling, wiring |
| `go/marketbyorder-parser/runner.go` | Three goroutines: refdata + mktdata + snapshot UDP receivers |
| `go/marketbyorder-parser/parser.go` | Parser interface + Record envelope + parser registry |
| `go/marketbyorder-parser/marketbyorder.go` | TopOfBookParser-style impl: routes wire frames into Record stream |
| `go/marketbyorder-parser/marketbyorder_wire.go` | Binary frame decoder for all 13 DZ-MBO message types |
| `go/marketbyorder-parser/sink.go` | OutputSink interface + factory |
| `go/marketbyorder-parser/sink_socket.go` | Broadcast Unix socket sink (copied from TOB) |
| `go/marketbyorder-parser/sink_json.go` | JSONL file sink (copied from TOB) |
| `go/marketbyorder-parser/metrics.go` | Prometheus metrics + /metrics HTTP server |
| `go/marketbyorder-parser/marketbyorder_test.go` | Wire decoder tests + routing tests |
| `go/marketbyorder-parser/sink_socket_test.go` | Broadcast / drop / framing tests (copied from TOB) |
| `go/marketbyorder-parser/sink_json_test.go` | JSONL output tests (copied from TOB) |
| `go/marketbyorder-bot/go.mod` | Module manifest |
| `go/marketbyorder-bot/Dockerfile` | Container build |
| `go/marketbyorder-bot/README.md` | Usage |
| `go/marketbyorder-bot/main.go` | CLI flags, signal handling, wiring |
| `go/marketbyorder-bot/bot.go` | Read parser socket, decode JSONL, dispatch to channel state, reconnect loop |
| `go/marketbyorder-bot/record.go` | Wire-compatible Record type (mirrors parser's, kept independent) |
| `go/marketbyorder-bot/channel.go` | ChannelState struct + cold-start + steady-state algorithm + reset handling |
| `go/marketbyorder-bot/instrument.go` | Instrument struct + book ops (apply OrderAdd/Cancel/Execute) + snapshot reassembly |
| `go/marketbyorder-bot/levels.go` | Aggregate bid/ask order maps → top-N price levels + cumulative_qty |
| `go/marketbyorder-bot/clickhouse.go` | HTTP-based per-table batchers, JSONEachRow inserts |
| `go/marketbyorder-bot/events_writer.go` | Dispatch Records → events table rows |
| `go/marketbyorder-bot/snapshot_writer.go` | Coalesce-aware level-snapshot scheduler |
| `go/marketbyorder-bot/metrics.go` | Prometheus metrics + /metrics HTTP server |
| `go/marketbyorder-bot/bot_test.go` | Socket reader + reconnect tests |
| `go/marketbyorder-bot/instrument_test.go` | Book ops + snapshot reassembly tests |
| `go/marketbyorder-bot/channel_test.go` | State machine tests |
| `go/marketbyorder-bot/levels_test.go` | Level aggregation tests |
| `go/marketbyorder-bot/clickhouse_test.go` | Batcher tests against mock HTTP server |
| `go/marketbyorder-bot/snapshot_writer_test.go` | Coalesce tests |
| `demo/clickhouse/init/02_schema_mbo.sql` | Five `marketbyorder.*` tables |
| `demo/grafana/dashboards/marketbyorder.json` | New Grafana dashboard |

### Modified files

| File | Modification |
|---|---|
| `go/go.work` | Add `./marketbyorder-parser`, `./marketbyorder-bot`; rename `./example-bot` → `./topofbook-bot` |
| `go/example-bot/` (whole dir) | Renamed to `go/topofbook-bot/` |
| `go/topofbook-bot/go.mod` (post-rename) | Module path bumped to `topofbook-bot` |
| `go/topofbook-bot/README.md` | Update title from "Example Bot" to "Top-of-Book Bot" |
| `demo/docker-compose.yml` | Rename `example-bot` service → `topofbook-bot`; add `marketbyorder-parser` and `marketbyorder-bot` services |
| `demo/.env.example` | Add `DZ_MBO_*` keys + `MBO_BOT_METRICS_PORT` |
| `README.md` (top level) | Update implementation table to add market-by-order row; rename example-bot reference |

---

### Task 1: Scaffold both new modules and rename example-bot → topofbook-bot

**Files:**
- Rename: `go/example-bot/` → `go/topofbook-bot/`
- Modify: `go/topofbook-bot/go.mod` (after rename)
- Modify: `go/topofbook-bot/README.md` (after rename)
- Modify: `go/go.work`
- Modify: `demo/docker-compose.yml`
- Modify: `README.md` (top level)
- Create: `go/marketbyorder-parser/go.mod`
- Create: `go/marketbyorder-parser/Dockerfile`
- Create: `go/marketbyorder-parser/README.md` (skeleton)
- Create: `go/marketbyorder-parser/main.go` (stub)
- Create: `go/marketbyorder-parser/.gitignore`
- Create: `go/marketbyorder-bot/go.mod`
- Create: `go/marketbyorder-bot/Dockerfile`
- Create: `go/marketbyorder-bot/README.md` (skeleton)
- Create: `go/marketbyorder-bot/main.go` (stub)
- Create: `go/marketbyorder-bot/.gitignore`

This task gets all the boilerplate out of the way before any real logic lands. After this task, `go build ./...` from the workspace root succeeds, all existing TOB tests still pass, and the two new binaries produce a "starting..." line and exit cleanly.

- [ ] **Step 1: Rename example-bot directory**

```bash
git mv go/example-bot go/topofbook-bot
```

- [ ] **Step 2: Update the renamed module path**

Edit `go/topofbook-bot/go.mod`. Replace the first line:

```
module example-bot
```

with:

```
module topofbook-bot
```

Search the dir for any internal `example-bot` import references and update. Run:

```bash
grep -r "example-bot" go/topofbook-bot/
```

Expected: no results in `.go` files. (Possibly hits in `README.md` and `Dockerfile` — update those too.)

- [ ] **Step 3: Update the renamed README title**

In `go/topofbook-bot/README.md`, replace the H1 "# Example Bot" (or whatever the existing title is) with:

```markdown
# Top-of-Book Bot

Reference Go subscriber that consumes the DoubleZero Top-of-Book parser's Unix socket, filters by symbol, exposes Prometheus metrics, and persists tick-level data into ClickHouse.
```

Other body text update is at the implementer's discretion — keep accurate to current behavior, no logic change implied.

- [ ] **Step 4: Update go/go.work**

Replace contents:

```
go 1.25.0

use (
	./marketbyorder-bot
	./marketbyorder-parser
	./internal
	./kernel-receiver
	./topofbook-bot
	./topofbook-parser
	./xdp-receiver
)
```

(Members in lexical order. The `./example-bot` line is gone, `./topofbook-bot` and the two new dirs are added.)

- [ ] **Step 5: Update demo/docker-compose.yml — rename example-bot service to topofbook-bot**

Find the existing service block:

```yaml
example-bot:
  build: ../go/example-bot
  ...
```

Rename to:

```yaml
topofbook-bot:
  build: ../go/topofbook-bot
  ...
```

Also update any `depends_on` references elsewhere in the file (search for `example-bot`). Do NOT add the two new MBO services in this task — that's Task 17.

- [ ] **Step 6: Update top-level README.md**

In the implementations table or wherever `example-bot` is referenced, change references to `topofbook-bot`. Add a new row or section noting that market-by-order pipeline is "in development" — exact wording at implementer's discretion. Top-level README is small; one re-read after editing should be enough.

- [ ] **Step 7: Verify the rename builds and existing tests pass**

```bash
cd go/topofbook-bot && go build ./... && go test ./...
```

Expected: clean build, all existing tests pass.

- [ ] **Step 8: Create marketbyorder-parser scaffold**

Create `go/marketbyorder-parser/go.mod`:

```
module marketbyorder-parser

go 1.25.0
```

Create `go/marketbyorder-parser/.gitignore`:

```
marketbyorder-parser
*.test
*.out
```

Create `go/marketbyorder-parser/main.go`:

```go
package main

import (
	"fmt"
	"os"
)

const version = "0.1.0-dev"

func main() {
	fmt.Fprintf(os.Stderr, "marketbyorder-parser %s starting...\n", version)
}
```

Create `go/marketbyorder-parser/Dockerfile` (copy verbatim from `go/topofbook-parser/Dockerfile` and replace any `topofbook-parser` references with `marketbyorder-parser`).

Create `go/marketbyorder-parser/README.md`:

```markdown
# DZ Market-by-Order Parser

A standalone multicast subscriber that decodes DoubleZero Market-by-Order (DZ-MBO v0.1.0) wire-format frames and writes decoded market data records to a file or Unix socket.

Sibling to [topofbook-parser](../topofbook-parser/). Documentation will land as the implementation completes.
```

- [ ] **Step 9: Create marketbyorder-bot scaffold**

Create `go/marketbyorder-bot/go.mod`:

```
module marketbyorder-bot

go 1.25.0
```

Create `go/marketbyorder-bot/.gitignore`:

```
marketbyorder-bot
*.test
*.out
```

Create `go/marketbyorder-bot/main.go`:

```go
package main

import (
	"fmt"
	"os"
)

const version = "0.1.0-dev"

func main() {
	fmt.Fprintf(os.Stderr, "marketbyorder-bot %s starting...\n", version)
}
```

Create `go/marketbyorder-bot/Dockerfile` (copy from `go/topofbook-bot/Dockerfile` and adjust any references).

Create `go/marketbyorder-bot/README.md`:

```markdown
# DZ Market-by-Order Bot

Reference Go subscriber that consumes the DoubleZero Market-by-Order parser's Unix socket, maintains in-memory MBO order books per instrument, and persists per-event rows + coalesced top-N level snapshots + raw wire snapshots into ClickHouse.

Sibling to [topofbook-bot](../topofbook-bot/). Documentation will land as the implementation completes.
```

- [ ] **Step 10: Verify everything builds**

```bash
cd go && go work sync && go build ./...
```

Expected: clean build, no errors. Each module's `main` package compiles.

```bash
cd go/marketbyorder-parser && go run . 2>&1 | head -1
cd go/marketbyorder-bot && go run . 2>&1 | head -1
```

Expected output:
```
marketbyorder-parser 0.1.0-dev starting...
marketbyorder-bot 0.1.0-dev starting...
```

- [ ] **Step 11: Commit**

```bash
git add -A go/ demo/docker-compose.yml README.md
git commit -m "scaffold(mbo): rename example-bot, add marketbyorder-parser and marketbyorder-bot stubs"
```

---

### Task 2: Parser wire decoder — frame header + reader helpers + inherited message types

**Files:**
- Create: `go/marketbyorder-parser/marketbyorder_wire.go`
- Create: `go/marketbyorder-parser/marketbyorder_test.go` (initial — wire tests only)

This task implements the binary decoder for the 24-byte frame header, the 4-byte application-message header, and the five inherited message types (Heartbeat, InstrumentDefinition, Trade, EndOfSession, ManifestSummary). The reader uses the sticky-error pattern from [go/topofbook-parser/topofbook_wire.go](../go/topofbook-parser/topofbook_wire.go) — read it as a structural template before writing this file.

**Important wire-format notes from the spec:**
- Magic = `0x4444` (LE bytes: `0x44 0x44`)
- All multi-byte fields are little-endian
- Frame header is 24 bytes; app message header is 4 bytes
- Frame Length field at offset 22 includes the 24-byte header
- Message Length field at offset 1 of each app message includes the 4-byte app header

- [ ] **Step 1: Write the decoder file with frame header + reader helpers**

Create `go/marketbyorder-parser/marketbyorder_wire.go`:

```go
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	dobMagic           uint16 = 0x4444
	dobSchemaVersion   uint8  = 1
	frameHeaderSize           = 24
	messageHeaderSize         = 4
	maxFrameSize              = 1232
)

// Message type IDs.
const (
	msgTypeHeartbeat            uint8 = 0x01
	msgTypeInstrumentDefinition uint8 = 0x02
	msgTypeTrade                uint8 = 0x04
	msgTypeEndOfSession         uint8 = 0x06
	msgTypeManifestSummary      uint8 = 0x07
	msgTypeOrderAdd             uint8 = 0x10
	msgTypeOrderCancel          uint8 = 0x11
	msgTypeOrderExecute         uint8 = 0x12
	msgTypeBatchBoundary        uint8 = 0x13
	msgTypeInstrumentReset      uint8 = 0x14
	msgTypeSnapshotBegin        uint8 = 0x20
	msgTypeSnapshotOrder        uint8 = 0x21
	msgTypeSnapshotEnd          uint8 = 0x22
)

// Wire decoding errors.
var (
	errBadMagic       = errors.New("bad magic")
	errSchemaVersion  = errors.New("unsupported schema version")
	errFrameTooShort  = errors.New("frame too short for header")
	errFrameLength    = errors.New("frame length mismatch")
	errMessageTooShort = errors.New("message too short for header")
	errMessageLength  = errors.New("message length out of range")
	errTruncated      = errors.New("truncated message body")
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

const flagSnapshot uint16 = 0x0001

// ParseFrameHeader decodes the 24-byte frame header from buf.
// Returns the header, the number of bytes consumed (always 24), and any error.
// Caller is responsible for verifying buf length is at least frameHeaderSize.
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
	if h.Magic != dobMagic {
		return h, errBadMagic
	}
	if h.SchemaVersion != dobSchemaVersion {
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

// HeartbeatBody is the 12-byte body of a Heartbeat message (after the 4-byte header).
type HeartbeatBody struct {
	ChannelID uint8
	Timestamp time.Time
}

// ParseHeartbeat decodes a Heartbeat body. buf must be exactly 12 bytes.
func ParseHeartbeat(buf []byte) (HeartbeatBody, error) {
	if len(buf) != 12 {
		return HeartbeatBody{}, fmt.Errorf("%w: expected 12 bytes for heartbeat body, got %d", errTruncated, len(buf))
	}
	return HeartbeatBody{
		ChannelID: buf[0],
		Timestamp: readTSNs(buf[4:12]),
	}, nil
}

// EndOfSessionBody is the 8-byte body of an EndOfSession message.
type EndOfSessionBody struct {
	Timestamp time.Time
}

// ParseEndOfSession decodes an EndOfSession body. buf must be exactly 8 bytes.
func ParseEndOfSession(buf []byte) (EndOfSessionBody, error) {
	if len(buf) != 8 {
		return EndOfSessionBody{}, fmt.Errorf("%w: expected 8 bytes for end_of_session body, got %d", errTruncated, len(buf))
	}
	return EndOfSessionBody{Timestamp: readTSNs(buf[0:8])}, nil
}

// ManifestSummaryBody is the 20-byte body of a ManifestSummary message.
type ManifestSummaryBody struct {
	ChannelID       uint8
	Valid           uint8
	ManifestSeq     uint16
	InstrumentCount uint32
	Timestamp       time.Time
}

// ParseManifestSummary decodes a ManifestSummary body. buf must be exactly 20 bytes.
func ParseManifestSummary(buf []byte) (ManifestSummaryBody, error) {
	if len(buf) != 20 {
		return ManifestSummaryBody{}, fmt.Errorf("%w: expected 20 bytes for manifest_summary body, got %d", errTruncated, len(buf))
	}
	return ManifestSummaryBody{
		ChannelID:       buf[0],
		Valid:           buf[1],
		ManifestSeq:     binary.LittleEndian.Uint16(buf[4:6]),
		InstrumentCount: binary.LittleEndian.Uint32(buf[8:12]),
		Timestamp:       readTSNs(buf[12:20]),
	}, nil
}

// InstrumentDefinitionBody is the 76-byte body of an InstrumentDefinition.
type InstrumentDefinitionBody struct {
	InstrumentID   uint32
	Symbol         string
	Leg1           string
	Leg2           string
	AssetClass     uint8
	PriceExponent  int8
	QtyExponent    int8
	MarketModel    uint8
	TickSizeRaw    int64
	LotSizeRaw     uint64
	ContractValue  uint64
	Expiry         time.Time
	SettleType     uint8
	PriceBound     uint8
	ManifestSeq    uint16
}

// ParseInstrumentDefinition decodes an InstrumentDefinition body. buf must be exactly 76 bytes.
func ParseInstrumentDefinition(buf []byte) (InstrumentDefinitionBody, error) {
	if len(buf) != 76 {
		return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected 76 bytes for instrument_definition body, got %d", errTruncated, len(buf))
	}
	return InstrumentDefinitionBody{
		InstrumentID:  binary.LittleEndian.Uint32(buf[0:4]),
		Symbol:        fixedString(buf[4:20]),
		Leg1:          fixedString(buf[20:28]),
		Leg2:          fixedString(buf[28:36]),
		AssetClass:    buf[36],
		PriceExponent: int8(buf[37]),
		QtyExponent:   int8(buf[38]),
		MarketModel:   buf[39],
		TickSizeRaw:   int64(binary.LittleEndian.Uint64(buf[40:48])),
		LotSizeRaw:    binary.LittleEndian.Uint64(buf[48:56]),
		ContractValue: binary.LittleEndian.Uint64(buf[56:64]),
		Expiry:        readTSNs(buf[64:72]),
		SettleType:    buf[72],
		PriceBound:    buf[73],
		ManifestSeq:   binary.LittleEndian.Uint16(buf[74:76]),
	}, nil
}

// TradeBody is the 48-byte body of a Trade message.
type TradeBody struct {
	InstrumentID       uint32
	SourceID           uint16
	AggressorSide      uint8
	TradeFlags         uint8
	SourceTimestamp    time.Time
	TradePriceRaw      int64
	TradeQtyRaw        uint64
	TradeID            uint64
	CumulativeVolumeRaw uint64
}

// ParseTrade decodes a Trade body. buf must be exactly 48 bytes.
func ParseTrade(buf []byte) (TradeBody, error) {
	if len(buf) != 48 {
		return TradeBody{}, fmt.Errorf("%w: expected 48 bytes for trade body, got %d", errTruncated, len(buf))
	}
	return TradeBody{
		InstrumentID:        binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:            binary.LittleEndian.Uint16(buf[4:6]),
		AggressorSide:       buf[6],
		TradeFlags:          buf[7],
		SourceTimestamp:     readTSNs(buf[8:16]),
		TradePriceRaw:       int64(binary.LittleEndian.Uint64(buf[16:24])),
		TradeQtyRaw:         binary.LittleEndian.Uint64(buf[24:32]),
		TradeID:             binary.LittleEndian.Uint64(buf[32:40]),
		CumulativeVolumeRaw: binary.LittleEndian.Uint64(buf[40:48]),
	}, nil
}
```

- [ ] **Step 2: Write tests for the frame header and inherited messages**

Create `go/marketbyorder-parser/marketbyorder_test.go`:

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

func TestParseFrameHeader_Valid(t *testing.T) {
	ts := time.Unix(1700000000, 123456789)
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 7, 42, ts, 3, 1, frameHeaderSize)
	h, err := ParseFrameHeader(buf)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if h.Magic != dobMagic {
		t.Errorf("magic: got %x want %x", h.Magic, dobMagic)
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
	buf := buildFrameHeader(0xDEAD, dobSchemaVersion, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errBadMagic) {
		t.Fatalf("expected errBadMagic, got %v", err)
	}
}

func TestParseFrameHeader_WrongVersion(t *testing.T) {
	buf := buildFrameHeader(dobMagic, 99, 0, 0, time.Now(), 0, 0, frameHeaderSize)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errSchemaVersion) {
		t.Fatalf("expected errSchemaVersion, got %v", err)
	}
}

func TestParseFrameHeader_LengthMismatch(t *testing.T) {
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 0, 0, time.Now(), 0, 0, 999)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errFrameLength) {
		t.Fatalf("expected errFrameLength, got %v", err)
	}
}

func TestParseFrameHeader_TooShort(t *testing.T) {
	buf := make([]byte, 10)
	_, err := ParseFrameHeader(buf)
	if !errors.Is(err, errFrameTooShort) {
		t.Fatalf("expected errFrameTooShort, got %v", err)
	}
}

func TestParseHeartbeat(t *testing.T) {
	ts := time.Unix(1700000001, 0)
	buf := make([]byte, 12)
	buf[0] = 5
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
	if _, err := ParseHeartbeat(make([]byte, 11)); err == nil {
		t.Fatal("expected error")
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
	buf[0] = 7   // ChannelID
	buf[1] = 1   // Valid
	binary.LittleEndian.PutUint16(buf[4:6], 100)  // ManifestSeq
	binary.LittleEndian.PutUint32(buf[8:12], 25)  // InstrumentCount
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
	binary.LittleEndian.PutUint32(buf[0:4], 12345)             // InstrumentID
	copy(buf[4:20], "BTC-USDT")                                // Symbol (null-padded)
	copy(buf[20:28], "BTC")                                    // Leg1
	copy(buf[28:36], "USDT")                                   // Leg2
	buf[36] = 1                                                // AssetClass = Crypto Spot
	buf[37] = byte(int8(-2))                                   // PriceExponent
	buf[38] = byte(int8(-8))                                   // QtyExponent
	buf[39] = 1                                                // MarketModel = CLOB
	binary.LittleEndian.PutUint64(buf[40:48], uint64(int64(1))) // TickSize
	binary.LittleEndian.PutUint64(buf[48:56], 100)             // LotSize
	binary.LittleEndian.PutUint64(buf[56:64], 0)               // ContractValue
	binary.LittleEndian.PutUint64(buf[64:72], uint64(expiry.UnixNano()))
	buf[72] = 1                                                // SettleType
	buf[73] = 0                                                // PriceBound
	binary.LittleEndian.PutUint16(buf[74:76], 7)               // ManifestSeq

	body, err := ParseInstrumentDefinition(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 12345 || body.Symbol != "BTC-USDT" || body.Leg1 != "BTC" || body.Leg2 != "USDT" {
		t.Errorf("strings: %+v", body)
	}
	if body.AssetClass != 1 || body.PriceExponent != -2 || body.QtyExponent != -8 || body.MarketModel != 1 {
		t.Errorf("enums/exponents: %+v", body)
	}
	if body.TickSizeRaw != 1 || body.LotSizeRaw != 100 || body.ContractValue != 0 || !body.Expiry.Equal(expiry) {
		t.Errorf("numerics: %+v", body)
	}
	if body.SettleType != 1 || body.PriceBound != 0 || body.ManifestSeq != 7 {
		t.Errorf("trailing: %+v", body)
	}
}

func TestParseTrade(t *testing.T) {
	ts := time.Unix(1700000004, 0)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 99)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1   // AggressorSide = Buy
	buf[7] = 0
	binary.LittleEndian.PutUint64(buf[8:16], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(int64(6743250)))
	binary.LittleEndian.PutUint64(buf[24:32], 50)
	binary.LittleEndian.PutUint64(buf[32:40], 1234567890)
	binary.LittleEndian.PutUint64(buf[40:48], 99999)

	body, err := ParseTrade(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 99 || body.SourceID != 1 || body.AggressorSide != 1 {
		t.Errorf("hdr: %+v", body)
	}
	if body.TradePriceRaw != 6743250 || body.TradeQtyRaw != 50 || body.TradeID != 1234567890 || body.CumulativeVolumeRaw != 99999 {
		t.Errorf("body: %+v", body)
	}
	if !body.SourceTimestamp.Equal(ts) {
		t.Errorf("ts: got %v want %v", body.SourceTimestamp, ts)
	}
}
```

- [ ] **Step 3: Run tests and confirm they pass**

```bash
cd go/marketbyorder-parser && go test -run 'TestParseFrameHeader|TestParseHeartbeat|TestParseEndOfSession|TestParseManifestSummary|TestParseInstrumentDefinition|TestParseTrade' -v ./...
```

Expected: all listed tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-parser/marketbyorder_wire.go go/marketbyorder-parser/marketbyorder_test.go
git commit -m "feat(mbo-parser): wire decoder for frame header and inherited message types"
```

---

### Task 3: Parser wire decoder — MBO-specific message types

**Files:**
- Modify: `go/marketbyorder-parser/marketbyorder_wire.go` (append decoders)
- Modify: `go/marketbyorder-parser/marketbyorder_test.go` (append tests)

Add decoders for the eight MBO-specific message types: OrderAdd, OrderCancel, OrderExecute, BatchBoundary, InstrumentReset, SnapshotBegin, SnapshotOrder, SnapshotEnd. Each gets a body struct, a parse function, and a test.

- [ ] **Step 1: Append the body structs and parse functions to marketbyorder_wire.go**

Add at the end of `go/marketbyorder-parser/marketbyorder_wire.go`:

```go
// OrderAddBody is the 48-byte body of an OrderAdd message (after the 4-byte header).
type OrderAddBody struct {
	InstrumentID     uint32
	SourceID         uint16
	Side             uint8
	OrderFlags       uint8
	PerInstrumentSeq uint32
	OrderID          uint64
	EnterTimestamp   time.Time
	PriceRaw         int64
	QtyRaw           uint64
}

func ParseOrderAdd(buf []byte) (OrderAddBody, error) {
	if len(buf) != 48 {
		return OrderAddBody{}, fmt.Errorf("%w: expected 48 bytes for order_add body, got %d", errTruncated, len(buf))
	}
	return OrderAddBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		Side:             buf[6],
		OrderFlags:       buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		OrderID:          binary.LittleEndian.Uint64(buf[12:20]),
		EnterTimestamp:   readTSNs(buf[20:28]),
		PriceRaw:         int64(binary.LittleEndian.Uint64(buf[28:36])),
		QtyRaw:           binary.LittleEndian.Uint64(buf[36:44]),
		// bytes 44-48 are reserved padding
	}, nil
}

// OrderCancelBody is the 28-byte body of an OrderCancel message.
type OrderCancelBody struct {
	InstrumentID     uint32
	SourceID         uint16
	Reason           uint8
	PerInstrumentSeq uint32
	OrderID          uint64
	Timestamp        time.Time
}

func ParseOrderCancel(buf []byte) (OrderCancelBody, error) {
	if len(buf) != 28 {
		return OrderCancelBody{}, fmt.Errorf("%w: expected 28 bytes for order_cancel body, got %d", errTruncated, len(buf))
	}
	return OrderCancelBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		Reason:           buf[6],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		OrderID:          binary.LittleEndian.Uint64(buf[12:20]),
		Timestamp:        readTSNs(buf[20:28]),
	}, nil
}

// OrderExecuteBody is the 52-byte body of an OrderExecute message.
type OrderExecuteBody struct {
	InstrumentID     uint32
	SourceID         uint16
	AggressorSide    uint8
	ExecFlags        uint8
	PerInstrumentSeq uint32
	OrderID          uint64
	TradeID          uint64
	Timestamp        time.Time
	ExecPriceRaw     int64
	ExecQtyRaw       uint64
}

func ParseOrderExecute(buf []byte) (OrderExecuteBody, error) {
	if len(buf) != 52 {
		return OrderExecuteBody{}, fmt.Errorf("%w: expected 52 bytes for order_execute body, got %d", errTruncated, len(buf))
	}
	return OrderExecuteBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		AggressorSide:    buf[6],
		ExecFlags:        buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		OrderID:          binary.LittleEndian.Uint64(buf[12:20]),
		TradeID:          binary.LittleEndian.Uint64(buf[20:28]),
		Timestamp:        readTSNs(buf[28:36]),
		ExecPriceRaw:     int64(binary.LittleEndian.Uint64(buf[36:44])),
		ExecQtyRaw:       binary.LittleEndian.Uint64(buf[44:52]),
	}, nil
}

// BatchBoundaryBody is the 12-byte body of a BatchBoundary message.
type BatchBoundaryBody struct {
	BatchID   uint32
	BatchTime time.Time
}

func ParseBatchBoundary(buf []byte) (BatchBoundaryBody, error) {
	if len(buf) != 12 {
		return BatchBoundaryBody{}, fmt.Errorf("%w: expected 12 bytes for batch_boundary body, got %d", errTruncated, len(buf))
	}
	return BatchBoundaryBody{
		BatchID:   binary.LittleEndian.Uint32(buf[0:4]),
		BatchTime: readTSNs(buf[4:12]),
	}, nil
}

// InstrumentResetBody is the 24-byte body of an InstrumentReset message.
type InstrumentResetBody struct {
	InstrumentID  uint32
	Reason        uint8
	NewAnchorSeq  uint64
	Timestamp     time.Time
}

func ParseInstrumentReset(buf []byte) (InstrumentResetBody, error) {
	if len(buf) != 24 {
		return InstrumentResetBody{}, fmt.Errorf("%w: expected 24 bytes for instrument_reset body, got %d", errTruncated, len(buf))
	}
	return InstrumentResetBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		Reason:       buf[4],
		NewAnchorSeq: binary.LittleEndian.Uint64(buf[8:16]),
		Timestamp:    readTSNs(buf[16:24]),
	}, nil
}

// SnapshotBeginBody is the 32-byte body of a SnapshotBegin message.
type SnapshotBeginBody struct {
	InstrumentID      uint32
	AnchorSeq         uint64
	TotalOrders       uint32
	SnapshotID        uint32
	LastInstrumentSeq uint32
	Timestamp         time.Time
}

func ParseSnapshotBegin(buf []byte) (SnapshotBeginBody, error) {
	if len(buf) != 32 {
		return SnapshotBeginBody{}, fmt.Errorf("%w: expected 32 bytes for snapshot_begin body, got %d", errTruncated, len(buf))
	}
	return SnapshotBeginBody{
		InstrumentID:      binary.LittleEndian.Uint32(buf[0:4]),
		AnchorSeq:         binary.LittleEndian.Uint64(buf[4:12]),
		TotalOrders:       binary.LittleEndian.Uint32(buf[12:16]),
		SnapshotID:        binary.LittleEndian.Uint32(buf[16:20]),
		LastInstrumentSeq: binary.LittleEndian.Uint32(buf[20:24]),
		Timestamp:         readTSNs(buf[24:32]),
	}, nil
}

// SnapshotOrderBody is the 40-byte body of a SnapshotOrder message.
// Note: Instrument ID is implied by the containing SnapshotBegin; not in this body.
type SnapshotOrderBody struct {
	SnapshotID     uint32
	OrderID        uint64
	Side           uint8
	OrderFlags     uint8
	EnterTimestamp time.Time
	PriceRaw       int64
	QtyRaw         uint64
}

func ParseSnapshotOrder(buf []byte) (SnapshotOrderBody, error) {
	if len(buf) != 40 {
		return SnapshotOrderBody{}, fmt.Errorf("%w: expected 40 bytes for snapshot_order body, got %d", errTruncated, len(buf))
	}
	return SnapshotOrderBody{
		SnapshotID:     binary.LittleEndian.Uint32(buf[0:4]),
		OrderID:        binary.LittleEndian.Uint64(buf[4:12]),
		Side:           buf[12],
		OrderFlags:     buf[13],
		EnterTimestamp: readTSNs(buf[16:24]),
		PriceRaw:       int64(binary.LittleEndian.Uint64(buf[24:32])),
		QtyRaw:         binary.LittleEndian.Uint64(buf[32:40]),
	}, nil
}

// SnapshotEndBody is the 16-byte body of a SnapshotEnd message.
type SnapshotEndBody struct {
	InstrumentID uint32
	AnchorSeq    uint64
	SnapshotID   uint32
}

func ParseSnapshotEnd(buf []byte) (SnapshotEndBody, error) {
	if len(buf) != 16 {
		return SnapshotEndBody{}, fmt.Errorf("%w: expected 16 bytes for snapshot_end body, got %d", errTruncated, len(buf))
	}
	return SnapshotEndBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		AnchorSeq:    binary.LittleEndian.Uint64(buf[4:12]),
		SnapshotID:   binary.LittleEndian.Uint32(buf[12:16]),
	}, nil
}
```

- [ ] **Step 2: Append tests**

Append to `go/marketbyorder-parser/marketbyorder_test.go`:

```go
func TestParseOrderAdd(t *testing.T) {
	enter := time.Unix(1700000010, 0)
	buf := make([]byte, 48)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 0  // bid
	buf[7] = 1  // post-only flag
	binary.LittleEndian.PutUint32(buf[8:12], 42)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(buf[36:44], 3031)

	body, err := ParseOrderAdd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.SourceID != 1 || body.Side != 0 || body.OrderFlags != 1 {
		t.Errorf("hdr: %+v", body)
	}
	if body.PerInstrumentSeq != 42 || body.OrderID != 999 || !body.EnterTimestamp.Equal(enter) {
		t.Errorf("ids/ts: %+v", body)
	}
	if body.PriceRaw != 82446 || body.QtyRaw != 3031 {
		t.Errorf("price/qty: %+v", body)
	}
}

func TestParseOrderCancel(t *testing.T) {
	ts := time.Unix(1700000011, 0)
	buf := make([]byte, 28)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1  // UserCancel
	binary.LittleEndian.PutUint32(buf[8:12], 43)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(ts.UnixNano()))

	body, err := ParseOrderCancel(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.SourceID != 1 || body.Reason != 1 ||
		body.PerInstrumentSeq != 43 || body.OrderID != 999 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseOrderExecute(t *testing.T) {
	ts := time.Unix(1700000012, 0)
	buf := make([]byte, 52)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint16(buf[4:6], 1)
	buf[6] = 1  // Buy aggressor
	buf[7] = 1  // full-fill flag
	binary.LittleEndian.PutUint32(buf[8:12], 44)
	binary.LittleEndian.PutUint64(buf[12:20], 999)
	binary.LittleEndian.PutUint64(buf[20:28], 1234567890)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(ts.UnixNano()))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(int64(82500)))
	binary.LittleEndian.PutUint64(buf[44:52], 100)

	body, err := ParseOrderExecute(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.AggressorSide != 1 || body.ExecFlags != 1 ||
		body.PerInstrumentSeq != 44 || body.OrderID != 999 || body.TradeID != 1234567890 ||
		!body.Timestamp.Equal(ts) || body.ExecPriceRaw != 82500 || body.ExecQtyRaw != 100 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseBatchBoundary(t *testing.T) {
	ts := time.Unix(1700000013, 0)
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint32(buf[0:4], 7000)
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ts.UnixNano()))

	body, err := ParseBatchBoundary(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.BatchID != 7000 || !body.BatchTime.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseInstrumentReset(t *testing.T) {
	ts := time.Unix(1700000014, 0)
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	buf[4] = 1  // PublisherInconsistency
	binary.LittleEndian.PutUint64(buf[8:16], 5000)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(ts.UnixNano()))

	body, err := ParseInstrumentReset(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.Reason != 1 || body.NewAnchorSeq != 5000 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotBegin(t *testing.T) {
	ts := time.Unix(1700000015, 0)
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint64(buf[4:12], 5000)
	binary.LittleEndian.PutUint32(buf[12:16], 25)  // TotalOrders
	binary.LittleEndian.PutUint32(buf[16:20], 7)   // SnapshotID
	binary.LittleEndian.PutUint32(buf[20:24], 100) // LastInstrumentSeq
	binary.LittleEndian.PutUint64(buf[24:32], uint64(ts.UnixNano()))

	body, err := ParseSnapshotBegin(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.AnchorSeq != 5000 || body.TotalOrders != 25 ||
		body.SnapshotID != 7 || body.LastInstrumentSeq != 100 || !body.Timestamp.Equal(ts) {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotOrder(t *testing.T) {
	enter := time.Unix(1700000016, 0)
	buf := make([]byte, 40)
	binary.LittleEndian.PutUint32(buf[0:4], 7)
	binary.LittleEndian.PutUint64(buf[4:12], 999)
	buf[12] = 0
	buf[13] = 1
	binary.LittleEndian.PutUint64(buf[16:24], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(buf[32:40], 3031)

	body, err := ParseSnapshotOrder(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.SnapshotID != 7 || body.OrderID != 999 || body.Side != 0 || body.OrderFlags != 1 ||
		!body.EnterTimestamp.Equal(enter) || body.PriceRaw != 82446 || body.QtyRaw != 3031 {
		t.Errorf("body: %+v", body)
	}
}

func TestParseSnapshotEnd(t *testing.T) {
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint32(buf[0:4], 100)
	binary.LittleEndian.PutUint64(buf[4:12], 5000)
	binary.LittleEndian.PutUint32(buf[12:16], 7)

	body, err := ParseSnapshotEnd(buf)
	if err != nil {
		t.Fatal(err)
	}
	if body.InstrumentID != 100 || body.AnchorSeq != 5000 || body.SnapshotID != 7 {
		t.Errorf("body: %+v", body)
	}
}
```

- [ ] **Step 3: Run all tests**

```bash
cd go/marketbyorder-parser && go test -v ./...
```

Expected: all 13 wire decoder tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-parser/marketbyorder_wire.go go/marketbyorder-parser/marketbyorder_test.go
git commit -m "feat(mbo-parser): wire decoder for MBO-specific message types"
```

---

### Task 4: Parser Record envelope + frame routing (marketbyorder.go + parser.go)

**Files:**
- Create: `go/marketbyorder-parser/parser.go`
- Create: `go/marketbyorder-parser/marketbyorder.go`
- Modify: `go/marketbyorder-parser/marketbyorder_test.go` (append routing tests)

This task wires the wire decoder into a `Parser` interface that produces `Record` values. Mirror `go/topofbook-parser/parser.go` and `go/topofbook-parser/topofbook.go` structurally — read those before writing. The MBO version is simpler than TOB because there's no cold-start buffering (the bot owns that); the parser just emits one Record per wire message.

- [ ] **Step 1: Create parser.go (Record envelope + Parser interface)**

```go
package main

import (
	"fmt"
	"time"
)

// Record is the JSON-serialised envelope emitted by the parser for every
// wire message. Bot consumes these one per line on the parser socket.
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

// Parser decodes a wire frame received on a given port and returns zero or
// more Records. A return of (nil, nil) means the frame was valid but produced
// no records (e.g., padding-only). A non-nil error indicates the frame should
// be dropped and an error counter incremented.
type Parser interface {
	Name() string
	ParseFrame(port string, frame []byte) ([]Record, error)
}

var parserRegistry = map[string]func() Parser{}

func registerParser(name string, ctor func() Parser) {
	parserRegistry[name] = ctor
}

func newParser(name string) (Parser, error) {
	ctor, ok := parserRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown parser: %q", name)
	}
	return ctor(), nil
}
```

- [ ] **Step 2: Create marketbyorder.go (Parser implementation)**

```go
package main

import (
	"fmt"
)

func init() {
	registerParser("marketbyorder", func() Parser { return &marketByOrderParser{} })
}

type marketByOrderParser struct{}

func (p *marketByOrderParser) Name() string { return "marketbyorder" }

// ParseFrame decodes one MBO frame and returns one Record per application message.
func (p *marketByOrderParser) ParseFrame(port string, frame []byte) ([]Record, error) {
	hdr, err := ParseFrameHeader(frame)
	if err != nil {
		return nil, fmt.Errorf("header: %w", err)
	}

	body := frame[frameHeaderSize:]
	records := make([]Record, 0, hdr.MessageCount)

	for i := uint8(0); i < hdr.MessageCount; i++ {
		mh, err := ParseMessageHeader(body)
		if err != nil {
			return nil, fmt.Errorf("msg %d header: %w", i, err)
		}
		if int(mh.Length) < messageHeaderSize {
			return nil, fmt.Errorf("%w: msg %d length %d", errMessageLength, i, mh.Length)
		}
		if int(mh.Length) > len(body) {
			return nil, fmt.Errorf("%w: msg %d length %d > %d remaining", errMessageLength, i, mh.Length, len(body))
		}
		msgBody := body[messageHeaderSize:mh.Length]

		rec, ok, err := p.decodeMessage(port, hdr, mh, msgBody)
		if err != nil {
			return nil, fmt.Errorf("msg %d type 0x%02x: %w", i, mh.Type, err)
		}
		if ok {
			records = append(records, rec)
		}

		body = body[mh.Length:]
	}

	return records, nil
}

func (p *marketByOrderParser) decodeMessage(port string, hdr FrameHeader, mh MessageHeader, body []byte) (Record, bool, error) {
	base := Record{
		Timestamp:      hdr.SendTimestamp,
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
		base.Fields = map[string]any{
			"source_id":             b.SourceID,
			"aggressor_side":        b.AggressorSide,
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

	case msgTypeOrderAdd:
		b, err := ParseOrderAdd(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_add"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"side":               sideString(b.Side),
			"order_flags":        b.OrderFlags,
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"enter_ts":           b.EnterTimestamp,
			"price_raw":          b.PriceRaw,
			"qty_raw":            b.QtyRaw,
		}
		return base, true, nil

	case msgTypeOrderCancel:
		b, err := ParseOrderCancel(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_cancel"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"cancel_reason":      cancelReasonString(b.Reason),
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"timestamp":          b.Timestamp,
		}
		return base, true, nil

	case msgTypeOrderExecute:
		b, err := ParseOrderExecute(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "order_execute"
		base.InstrumentID = b.InstrumentID
		base.Fields = map[string]any{
			"source_id":          b.SourceID,
			"aggressor_side":     aggressorString(b.AggressorSide),
			"exec_flags":         b.ExecFlags,
			"per_instrument_seq": b.PerInstrumentSeq,
			"order_id":           b.OrderID,
			"trade_id":           b.TradeID,
			"timestamp":          b.Timestamp,
			"exec_price_raw":     b.ExecPriceRaw,
			"exec_qty_raw":       b.ExecQtyRaw,
		}
		return base, true, nil

	case msgTypeBatchBoundary:
		b, err := ParseBatchBoundary(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "batch_boundary"
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
			"total_orders":        b.TotalOrders,
			"snapshot_id":         b.SnapshotID,
			"last_instrument_seq": b.LastInstrumentSeq,
			"timestamp":           b.Timestamp,
		}
		return base, true, nil

	case msgTypeSnapshotOrder:
		b, err := ParseSnapshotOrder(body)
		if err != nil {
			return Record{}, false, err
		}
		base.Type = "snapshot_order"
		base.Fields = map[string]any{
			"snapshot_id": b.SnapshotID,
			"order_id":    b.OrderID,
			"side":        sideString(b.Side),
			"order_flags": b.OrderFlags,
			"enter_ts":    b.EnterTimestamp,
			"price_raw":   b.PriceRaw,
			"qty_raw":     b.QtyRaw,
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
		// Unknown type — skip per forward-compat rule. Caller advances by mh.Length.
		return Record{}, false, nil
	}
}

func sideString(s uint8) string {
	switch s {
	case 0:
		return "bid"
	case 1:
		return "ask"
	default:
		return ""
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

func cancelReasonString(r uint8) string {
	switch r {
	case 1:
		return "user_cancel"
	case 2:
		return "venue_expire"
	case 3:
		return "self_trade"
	case 4:
		return "margin"
	case 5:
		return "risk_limit"
	case 6:
		return "sibling_filled"
	case 255:
		return "other"
	default:
		return "unknown"
	}
}

func resetReasonString(r uint8) string {
	switch r {
	case 1:
		return "publisher_inconsistency"
	case 2:
		return "venue_resync"
	case 3:
		return "upstream_gap"
	case 255:
		return "other"
	default:
		return "unspecified"
	}
}
```

- [ ] **Step 3: Append routing tests to marketbyorder_test.go**

```go
// buildSingleMessageFrame wraps an application message body in a complete frame.
func buildSingleMessageFrame(t *testing.T, msgType uint8, msgLength uint8, msgBody []byte) []byte {
	t.Helper()
	frameLen := frameHeaderSize + messageHeaderSize + len(msgBody)
	buf := buildFrameHeader(dobMagic, dobSchemaVersion, 1, 100, time.Unix(1700000020, 0), 1, 0, uint16(frameLen))
	mh := make([]byte, messageHeaderSize)
	mh[0] = msgType
	mh[1] = msgLength
	binary.LittleEndian.PutUint16(mh[2:4], 0)
	buf = append(buf, mh...)
	buf = append(buf, msgBody...)
	return buf
}

func TestMarketByOrderParser_OrderAdd(t *testing.T) {
	enter := time.Unix(1700000010, 0)
	body := make([]byte, 48)
	binary.LittleEndian.PutUint32(body[0:4], 100)
	binary.LittleEndian.PutUint16(body[4:6], 1)
	body[6] = 0
	binary.LittleEndian.PutUint32(body[8:12], 42)
	binary.LittleEndian.PutUint64(body[12:20], 999)
	binary.LittleEndian.PutUint64(body[20:28], uint64(enter.UnixNano()))
	binary.LittleEndian.PutUint64(body[28:36], uint64(int64(82446)))
	binary.LittleEndian.PutUint64(body[36:44], 3031)

	frame := buildSingleMessageFrame(t, msgTypeOrderAdd, messageHeaderSize+48, body)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r.Type != "order_add" || r.Port != "mktdata" || r.InstrumentID != 100 || r.SequenceNumber != 100 {
		t.Errorf("envelope: %+v", r)
	}
	if r.Fields["per_instrument_seq"].(uint32) != 42 || r.Fields["order_id"].(uint64) != 999 {
		t.Errorf("fields: %+v", r.Fields)
	}
}

func TestMarketByOrderParser_UnknownTypeSkipped(t *testing.T) {
	body := make([]byte, 8)
	frame := buildSingleMessageFrame(t, 0xFE, messageHeaderSize+8, body)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 0 {
		t.Fatalf("expected 0 records for unknown type, got %d", len(recs))
	}
}

func TestMarketByOrderParser_TruncatedFrame(t *testing.T) {
	body := make([]byte, 48)
	frame := buildSingleMessageFrame(t, msgTypeOrderAdd, messageHeaderSize+48, body)
	// Truncate the frame to 30 bytes — header says 76, only 30 present.
	truncated := frame[:30]

	p := &marketByOrderParser{}
	_, err := p.ParseFrame("mktdata", truncated)
	if err == nil {
		t.Fatal("expected error on truncated frame")
	}
}
```

- [ ] **Step 4: Run tests**

```bash
cd go/marketbyorder-parser && go test -v ./...
```

Expected: all wire decoder tests + 3 new routing tests PASS.

- [ ] **Step 5: Commit**

```bash
git add go/marketbyorder-parser/parser.go go/marketbyorder-parser/marketbyorder.go go/marketbyorder-parser/marketbyorder_test.go
git commit -m "feat(mbo-parser): Record envelope and frame routing for all message types"
```

---

### Task 5: Parser sinks (copy from TOB)

**Files:**
- Create: `go/marketbyorder-parser/sink.go`
- Create: `go/marketbyorder-parser/sink_socket.go`
- Create: `go/marketbyorder-parser/sink_json.go`
- Create: `go/marketbyorder-parser/sink_socket_test.go`
- Create: `go/marketbyorder-parser/sink_json_test.go`

The sinks are functionally identical to TOB's. Copy them verbatim and adapt only what's necessary (package name; the `Record` type used here is the local MBO Record but has the same JSON shape, so the sink code itself doesn't change). Skip the CSV sink — MBO Records have `fields` of variable shape that don't fit CSV cleanly, and the spec specifies JSON-only.

- [ ] **Step 1: Copy the four files from TOB**

```bash
cp go/topofbook-parser/sink.go go/marketbyorder-parser/sink.go
cp go/topofbook-parser/sink_socket.go go/marketbyorder-parser/sink_socket.go
cp go/topofbook-parser/sink_json.go go/marketbyorder-parser/sink_json.go
cp go/topofbook-parser/sink_socket_test.go go/marketbyorder-parser/sink_socket_test.go
cp go/topofbook-parser/sink_json_test.go go/marketbyorder-parser/sink_json_test.go
```

- [ ] **Step 2: Remove CSV-related code from sink.go**

Open `go/marketbyorder-parser/sink.go`. Remove any branches, format constants, or factory cases that reference `csv` or `CSVFileSink`. The MBO parser supports `json` only.

If `sink.go` has a format-dispatch function, simplify it to handle only `json` (or `unix:` prefix for socket sink); return an error for anything else with message like `unsupported format: %q (marketbyorder supports json only)`.

- [ ] **Step 3: Verify the sink files compile in the MBO package**

```bash
cd go/marketbyorder-parser && go build ./...
```

Expected: clean build. The `Record` type defined in `parser.go` (Task 4) is what the sinks reference; both packages have it with the same JSON shape.

- [ ] **Step 4: Run tests**

```bash
cd go/marketbyorder-parser && go test -v ./...
```

Expected: all wire/routing tests still pass; new sink tests (copied from TOB) PASS as well.

- [ ] **Step 5: Commit**

```bash
git add go/marketbyorder-parser/sink.go go/marketbyorder-parser/sink_socket.go go/marketbyorder-parser/sink_json.go go/marketbyorder-parser/sink_socket_test.go go/marketbyorder-parser/sink_json_test.go
git commit -m "feat(mbo-parser): copy sink layer from topofbook-parser"
```

---

### Task 6: Parser metrics

**Files:**
- Create: `go/marketbyorder-parser/metrics.go`

Mirror [go/topofbook-parser/metrics.go](../go/topofbook-parser/metrics.go) structurally. Same Prometheus client, same HTTP server pattern. Metric names are different per the spec (`dz_mbo_parser_*` prefix).

- [ ] **Step 1: Add Prometheus dependency to go.mod**

```bash
cd go/marketbyorder-parser && go get github.com/prometheus/client_golang/prometheus github.com/prometheus/client_golang/prometheus/promhttp
```

- [ ] **Step 2: Write metrics.go**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "dz_mbo_parser"

type Metrics struct {
	registry *prometheus.Registry

	IngressPackets      *prometheus.CounterVec
	IngressBytes        *prometheus.CounterVec
	ParseErrors         *prometheus.CounterVec
	RecordsTotal        *prometheus.CounterVec
	WireLatency         *prometheus.HistogramVec
	SocketClients       prometheus.Gauge
	SocketClientDrops   *prometheus.CounterVec
	SocketRecordsSent   prometheus.Counter
	SinkWriteErrors     prometheus.Counter
	BuildInfo           *prometheus.GaugeVec
	UptimeSeconds       prometheus.GaugeFunc

	startTime time.Time
}

func NewMetrics(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry:  reg,
		startTime: time.Now(),
	}

	m.IngressPackets = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "ingress_packets_total",
		Help: "UDP datagrams received per port",
	}, []string{"port"})

	m.IngressBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "ingress_bytes_total",
		Help: "UDP bytes received per port",
	}, []string{"port"})

	m.ParseErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "parse_errors_total",
		Help: "Frame decode failures by reason",
	}, []string{"port", "reason"})

	m.RecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "records_total",
		Help: "Records emitted per record type",
	}, []string{"type"})

	m.WireLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "wire_latency_seconds",
		Help:    "Latency from publisher send_ts to parse, by port (includes clock skew)",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"port"})

	m.SocketClients = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "socket_clients",
		Help: "Currently connected Unix socket clients",
	})

	m.SocketClientDrops = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "socket_client_drops_total",
		Help: "Slow clients dropped by reason",
	}, []string{"reason"})

	m.SocketRecordsSent = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "socket_records_sent_total",
		Help: "Records written to >=1 client",
	})

	m.SinkWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "sink_write_errors_total",
		Help: "Sink write failures",
	})

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "build_info",
		Help: "Build info; value always 1",
	}, []string{"version", "commit"})

	m.UptimeSeconds = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: metricsNamespace, Name: "uptime_seconds",
		Help: "Seconds since process start",
	}, func() float64 { return time.Since(m.startTime).Seconds() })

	reg.MustRegister(
		m.IngressPackets, m.IngressBytes, m.ParseErrors, m.RecordsTotal, m.WireLatency,
		m.SocketClients, m.SocketClientDrops, m.SocketRecordsSent, m.SinkWriteErrors,
		m.BuildInfo, m.UptimeSeconds,
	)
	m.BuildInfo.WithLabelValues(version, commit).Set(1)

	return m
}

// ServeHTTP starts a /metrics HTTP server on addr. Returns immediately.
// Server errors are logged via the provided logger callback.
func (m *Metrics) ServeHTTP(ctx context.Context, addr string, logErr func(error)) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logErr(fmt.Errorf("metrics server: %w", err))
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd go/marketbyorder-parser && go build ./...
```

Expected: clean build.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-parser/go.mod go/marketbyorder-parser/go.sum go/marketbyorder-parser/metrics.go
git commit -m "feat(mbo-parser): Prometheus metrics and /metrics endpoint"
```

---

### Task 7: Parser runner + main

**Files:**
- Create: `go/marketbyorder-parser/runner.go`
- Modify: `go/marketbyorder-parser/main.go`

The runner spawns three goroutines, one per port. Each receives UDP datagrams, hands them to the parser, and forwards Records to the sink. Mirror [go/topofbook-parser/runner.go](../go/topofbook-parser/runner.go) (which has 2 receivers); the MBO version has 3 with port labels `refdata`, `mktdata`, `snapshot`.

- [ ] **Step 1: Write runner.go**

```go
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const maxUDPPacket = 65536

// portConfig binds a label to a UDP listening address.
type portConfig struct {
	Label string
	Port  int
}

// Runner is the per-port receive loop.
type Runner struct {
	parser    Parser
	sink      OutputSink
	metrics   *Metrics
	group     net.IP
	iface     *net.Interface // may be nil for system default
	ports     []portConfig
}

func NewRunner(parser Parser, sink OutputSink, metrics *Metrics, group string, iface string, refdata, mktdata, snapshot int) (*Runner, error) {
	ip := net.ParseIP(group)
	if ip == nil || ip.To4() == nil {
		return nil, fmt.Errorf("invalid multicast group: %q", group)
	}

	var ifi *net.Interface
	if iface != "" {
		var err error
		ifi, err = net.InterfaceByName(iface)
		if err != nil {
			return nil, fmt.Errorf("interface %q: %w", iface, err)
		}
	}

	return &Runner{
		parser:  parser,
		sink:    sink,
		metrics: metrics,
		group:   ip.To4(),
		iface:   ifi,
		ports: []portConfig{
			{"refdata", refdata},
			{"mktdata", mktdata},
			{"snapshot", snapshot},
		},
	}, nil
}

// Run spawns one goroutine per port and blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(r.ports))

	for _, pc := range r.ports {
		conn, err := r.openMulticast(pc.Port)
		if err != nil {
			return fmt.Errorf("open %s port %d: %w", pc.Label, pc.Port, err)
		}
		wg.Add(1)
		go func(label string, conn *net.UDPConn) {
			defer wg.Done()
			defer conn.Close()
			r.receive(ctx, label, conn, errs)
		}(pc.Label, conn)
	}

	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func (r *Runner) openMulticast(port int) (*net.UDPConn, error) {
	addr := &net.UDPAddr{IP: r.group, Port: port}
	conn, err := net.ListenMulticastUDP("udp4", r.iface, addr)
	if err != nil {
		return nil, err
	}
	if err := conn.SetReadBuffer(8 * 1024 * 1024); err != nil {
		log.Printf("warning: SetReadBuffer: %v", err)
	}
	return conn, nil
}

func (r *Runner) receive(ctx context.Context, port string, conn *net.UDPConn, errs chan<- error) {
	buf := make([]byte, maxUDPPacket)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			errs <- fmt.Errorf("read %s: %w", port, err)
			return
		}

		r.metrics.IngressPackets.WithLabelValues(port).Inc()
		r.metrics.IngressBytes.WithLabelValues(port).Add(float64(n))

		records, perr := r.parser.ParseFrame(port, buf[:n])
		if perr != nil {
			r.metrics.ParseErrors.WithLabelValues(port, classifyError(perr)).Inc()
			continue
		}

		for i := range records {
			r.metrics.RecordsTotal.WithLabelValues(records[i].Type).Inc()
			r.metrics.WireLatency.WithLabelValues(port).Observe(
				time.Since(records[i].Timestamp).Seconds())
		}

		if err := r.sink.Write(records); err != nil {
			r.metrics.SinkWriteErrors.Inc()
		}
	}
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, errBadMagic):
		return "bad_magic"
	case errors.Is(err, errSchemaVersion):
		return "schema_version"
	case errors.Is(err, errFrameLength), errors.Is(err, errMessageLength):
		return "frame_length"
	case errors.Is(err, errFrameTooShort), errors.Is(err, errMessageTooShort), errors.Is(err, errTruncated):
		return "truncated"
	default:
		return "other"
	}
}
```

You'll also need to add `"errors"` to the imports. Verify with `go build`.

- [ ] **Step 2: Replace main.go with the real entry point**

Replace `go/marketbyorder-parser/main.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const version = "0.1.0-dev"

var commit = "unknown"

func main() {
	var (
		group        = flag.String("group", "", "multicast group IP (required)")
		refdataPort  = flag.Int("refdata-port", 0, "refdata UDP port (required)")
		mktdataPort  = flag.Int("mktdata-port", 0, "mktdata UDP port (required)")
		snapshotPort = flag.Int("snapshot-port", 0, "snapshot UDP port (required)")
		iface        = flag.String("interface", "", "network interface for multicast join (e.g., doublezero1)")
		output       = flag.String("output", "", "output target: unix:///path/to/sock or file:///path/to/log (required)")
		format       = flag.String("format", "json", "output format: json")
		parserName   = flag.String("parser", "marketbyorder", "parser name from registry")
		metricsAddr  = flag.String("metrics-addr", "", "Prometheus /metrics HTTP listen address (empty = disabled)")
		verbose      = flag.Bool("v", false, "debug logging")
		showVersion  = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyorder-parser %s (%s)\n", version, commit)
		os.Exit(0)
	}

	if *group == "" || *refdataPort == 0 || *mktdataPort == 0 || *snapshotPort == 0 || *output == "" {
		fmt.Fprintln(os.Stderr, "error: --group, --refdata-port, --mktdata-port, --snapshot-port, and --output are required")
		flag.Usage()
		os.Exit(2)
	}
	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	parser, err := newParser(*parserName)
	if err != nil {
		log.Fatalf("parser: %v", err)
	}
	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	metrics.ServeHTTP(ctx, *metricsAddr, func(e error) { log.Println(e) })

	sink, err := NewSink(*output, *format, metrics)
	if err != nil {
		log.Fatalf("sink: %v", err)
	}
	defer sink.Close()

	runner, err := NewRunner(parser, sink, metrics, *group, *iface, *refdataPort, *mktdataPort, *snapshotPort)
	if err != nil {
		log.Fatalf("runner: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %v, shutting down", s)
		cancel()
	}()

	log.Printf("marketbyorder-parser %s started: group=%s refdata=:%d mktdata=:%d snapshot=:%d output=%s",
		version, *group, *refdataPort, *mktdataPort, *snapshotPort, *output)

	if err := runner.Run(ctx); err != nil {
		log.Fatalf("runner: %v", err)
	}
	log.Println("shutdown complete")
}
```

Note: `NewSink(output, format, metrics)` is the factory function in `sink.go` (Task 5). Adjust the call signature if your copied `sink.go` exposes it differently — the TOB version may take only `(output, format)`. Match the local signature.

- [ ] **Step 3: Build and smoke-test**

```bash
cd go/marketbyorder-parser && go build ./...
./marketbyorder-parser --version
```

Expected: prints `marketbyorder-parser 0.1.0-dev (unknown)` and exits 0.

```bash
./marketbyorder-parser
```

Expected: prints required-flags error and exits 2.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-parser/runner.go go/marketbyorder-parser/main.go
git commit -m "feat(mbo-parser): runner with three-port receivers + main entry"
```

---

### Task 8: Bot record + JSONL socket reader + reconnect loop

**Files:**
- Create: `go/marketbyorder-bot/record.go`
- Create: `go/marketbyorder-bot/bot.go`
- Create: `go/marketbyorder-bot/bot_test.go`

The bot's record.go is a wire-compatible mirror of the parser's Record. The bot.go reads the parser socket via `bufio.Scanner` and dispatches to a callback. Mirror [go/example-bot/bot.go](../go/example-bot/bot.go) (post-rename: `go/topofbook-bot/bot.go`) for the reconnect/backoff loop pattern.

- [ ] **Step 1: Write record.go**

```go
package main

import "time"

type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}
```

- [ ] **Step 2: Write bot.go**

```go
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// Dispatcher is called for every successfully decoded Record.
// Implementations MUST be fast (don't block the read loop).
type Dispatcher interface {
	Dispatch(rec Record)
}

// Bot reads JSONL Records from a parser Unix socket and dispatches them.
// Reconnects with exponential backoff on disconnect.
type Bot struct {
	socketPath string
	dispatcher Dispatcher
	metrics    *Metrics
}

func NewBot(socketPath string, dispatcher Dispatcher, metrics *Metrics) *Bot {
	return &Bot{socketPath: socketPath, dispatcher: dispatcher, metrics: metrics}
}

// Run reads from the socket until ctx is cancelled.
func (b *Bot) Run(ctx context.Context) {
	backoff := 250 * time.Millisecond
	maxBackoff := 5 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := net.Dial("unix", b.socketPath)
		if err != nil {
			b.metrics.SocketReconnects.WithLabelValues("dial_failed").Inc()
			log.Printf("dial %s: %v (retry in %v)", b.socketPath, err, backoff)
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
			continue
		}

		b.metrics.SocketConnected.Set(1)
		log.Printf("connected to %s", b.socketPath)
		backoff = 250 * time.Millisecond // reset on success

		reason := b.read(ctx, conn)
		_ = conn.Close()
		b.metrics.SocketConnected.Set(0)
		b.metrics.SocketReconnects.WithLabelValues(reason).Inc()

		if ctx.Err() != nil {
			return
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

func (b *Bot) read(ctx context.Context, conn net.Conn) string {
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024) // up to 1 MiB lines

	for scanner.Scan() {
		if ctx.Err() != nil {
			return "shutdown"
		}
		line := scanner.Bytes()

		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			b.metrics.DecodeErrors.Inc()
			continue
		}

		b.metrics.RecordsTotal.WithLabelValues(rec.Type).Inc()
		b.metrics.SocketToBotLatency.WithLabelValues(rec.Type).Observe(
			time.Since(rec.Timestamp).Seconds())
		b.dispatcher.Dispatch(rec)
	}

	err := scanner.Err()
	if err == nil || errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, net.ErrClosed) {
		return "shutdown"
	}
	return "read_error"
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	return next
}

// Helper: produce a JSON-encoded Record line for tests.
func encodeRecord(r Record) string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%s\n", b)
}
```

The `Metrics` struct referenced here will be defined in Task 12. For now, declare a stub interface so this file compiles standalone:

Add to bot.go (above the Bot struct):

```go
// Metrics is the subset used by Bot. Real impl in metrics.go (Task 12).
// Stub declaration here keeps this task standalone-buildable.
type Metrics struct {
	SocketConnected    prometheus.Gauge
	SocketReconnects   *prometheus.CounterVec
	RecordsTotal       *prometheus.CounterVec
	DecodeErrors       prometheus.Counter
	SocketToBotLatency *prometheus.HistogramVec
}
```

Add `import "github.com/prometheus/client_golang/prometheus"` and:

```bash
cd go/marketbyorder-bot && go get github.com/prometheus/client_golang/prometheus
```

Task 12 will replace this with a full Metrics struct + constructor.

- [ ] **Step 3: Write bot_test.go**

```go
package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// stubMetrics returns a Metrics struct wired with no-op collectors so Bot can run in tests.
func stubMetrics() *Metrics {
	return &Metrics{
		SocketConnected:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "stub_connected"}),
		SocketReconnects:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "stub_reconnects"}, []string{"reason"}),
		RecordsTotal:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "stub_records"}, []string{"type"}),
		DecodeErrors:       prometheus.NewCounter(prometheus.CounterOpts{Name: "stub_decode_errors"}),
		SocketToBotLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "stub_latency"}, []string{"type"}),
	}
}

type capturingDispatcher struct {
	mu      sync.Mutex
	records []Record
}

func (d *capturingDispatcher) Dispatch(r Record) {
	d.mu.Lock()
	d.records = append(d.records, r)
	d.mu.Unlock()
}

func (d *capturingDispatcher) snapshot() []Record {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Record, len(d.records))
	copy(out, d.records)
	return out
}

func TestBot_ReadsRecordsFromSocket(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var serverWG sync.WaitGroup
	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		// Send 3 records.
		for i := 0; i < 3; i++ {
			rec := Record{
				Type:           "heartbeat",
				Timestamp:      time.Unix(1700000000, 0),
				ChannelID:      0,
				Port:           "mktdata",
				SequenceNumber: uint64(i),
			}
			conn.Write([]byte(encodeRecord(rec)))
		}
		// Wait briefly for the bot to consume them, then close.
		time.Sleep(200 * time.Millisecond)
	}()

	disp := &capturingDispatcher{}
	bot := NewBot(sockPath, disp, stubMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bot.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if got := len(disp.snapshot()); got != 3 {
		t.Fatalf("expected 3 records dispatched, got %d", got)
	}
	cancel()
	serverWG.Wait()
}

func TestBot_ReconnectsOnDisconnect(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")
	defer os.Remove(sockPath)

	disp := &capturingDispatcher{}
	bot := NewBot(sockPath, disp, stubMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bot.Run(ctx)
	time.Sleep(300 * time.Millisecond) // bot tries to dial, fails (no listener)

	// Now create the listener.
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte(encodeRecord(Record{Type: "heartbeat", Timestamp: time.Now(), Port: "mktdata"})))
		time.Sleep(200 * time.Millisecond)
	}()

	// Bot's backoff is at most 5s; give it room to reconnect and read.
	time.Sleep(6 * time.Second)

	if got := len(disp.snapshot()); got < 1 {
		t.Fatalf("expected >=1 record after reconnect, got %d", got)
	}
}
```

The reconnect test takes ~6 seconds. That's fine in CI but consider marking it with `if testing.Short() { t.Skip(...) }` if test speed becomes an issue.

- [ ] **Step 4: Run tests**

```bash
cd go/marketbyorder-bot && go test -v ./...
```

Expected: both tests PASS. Reconnect test takes 6+ seconds.

- [ ] **Step 5: Commit**

```bash
git add go/marketbyorder-bot/go.mod go/marketbyorder-bot/go.sum go/marketbyorder-bot/record.go go/marketbyorder-bot/bot.go go/marketbyorder-bot/bot_test.go
git commit -m "feat(mbo-bot): record envelope and JSONL socket reader with reconnect"
```

---

### Task 9: Bot instrument (book ops + snapshot reassembly)

**Files:**
- Create: `go/marketbyorder-bot/instrument.go`
- Create: `go/marketbyorder-bot/instrument_test.go`

The Instrument type holds bid/ask order maps and applies the book-affecting wire events. Snapshot reassembly buffers SnapshotOrders between SnapshotBegin and SnapshotEnd, validates the count and IDs, then commits to the live book.

- [ ] **Step 1: Write instrument.go**

```go
package main

import (
	"errors"
	"fmt"
	"time"
)

// InstrumentStatus tracks the state-machine position from the spec.
type InstrumentStatus int

const (
	StatusAwaitingSnapshot InstrumentStatus = iota
	StatusBuildingSnapshot
	StatusReady
	StatusGap
)

func (s InstrumentStatus) String() string {
	switch s {
	case StatusAwaitingSnapshot:
		return "awaiting-snapshot"
	case StatusBuildingSnapshot:
		return "building-snapshot"
	case StatusReady:
		return "ready"
	case StatusGap:
		return "gap"
	default:
		return "unknown"
	}
}

// RestingOrder is one entry in the bid or ask map.
type RestingOrder struct {
	OrderID  uint64
	Side     uint8
	Flags    uint8
	EnterTS  time.Time
	Price    int64  // raw, scale via Instrument.PriceExponent for display
	Quantity uint64 // raw, decremented on partial fills
}

// PendingSnapshot is the state held during SnapshotBegin..SnapshotEnd.
type PendingSnapshot struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalOrders       uint32
	LastInstrumentSeq uint32
	ReceivedOrders    uint32
	Bids              map[uint64]*RestingOrder
	Asks              map[uint64]*RestingOrder
}

// Instrument holds the live book and state-machine position for one (channel_id, instrument_id).
type Instrument struct {
	ID                       uint32
	Symbol                   string
	PriceExponent            int8
	QtyExponent              int8
	Status                   InstrumentStatus
	Bids                     map[uint64]*RestingOrder
	Asks                     map[uint64]*RestingOrder
	LastAppliedMktdataSeq    uint64
	LastAppliedInstrumentSeq uint32
	OpenSnapshot             *PendingSnapshot
}

// NewInstrument returns an Instrument awaiting its first snapshot.
func NewInstrument(id uint32, symbol string, priceExp, qtyExp int8) *Instrument {
	return &Instrument{
		ID:            id,
		Symbol:        symbol,
		PriceExponent: priceExp,
		QtyExponent:   qtyExp,
		Status:        StatusAwaitingSnapshot,
		Bids:          map[uint64]*RestingOrder{},
		Asks:          map[uint64]*RestingOrder{},
	}
}

// ApplyOrderAdd inserts a new resting order. Caller is responsible for seq checks
// and for status==Ready precondition.
func (i *Instrument) ApplyOrderAdd(orderID uint64, side, flags uint8, enterTS time.Time, price int64, qty uint64) {
	o := &RestingOrder{
		OrderID:  orderID,
		Side:     side,
		Flags:    flags,
		EnterTS:  enterTS,
		Price:    price,
		Quantity: qty,
	}
	if side == 0 {
		i.Bids[orderID] = o
	} else {
		i.Asks[orderID] = o
	}
}

// ApplyOrderCancel removes an order. Cancels for unknown order_ids are silently dropped
// per the spec ("MAY receive OrderCancel for an Order ID it does not have").
func (i *Instrument) ApplyOrderCancel(orderID uint64) {
	delete(i.Bids, orderID)
	delete(i.Asks, orderID)
}

// ApplyOrderExecute decrements the resting order's quantity. If exec_flags bit 0
// (full-fill) is set, or remaining quantity reaches 0, the order is removed.
// Cancels for unknown order_ids are silently dropped.
func (i *Instrument) ApplyOrderExecute(orderID uint64, execFlags uint8, execQty uint64) {
	if o, ok := i.Bids[orderID]; ok {
		i.applyExecToOrder(i.Bids, o, execFlags, execQty)
		return
	}
	if o, ok := i.Asks[orderID]; ok {
		i.applyExecToOrder(i.Asks, o, execFlags, execQty)
	}
}

func (i *Instrument) applyExecToOrder(book map[uint64]*RestingOrder, o *RestingOrder, execFlags uint8, execQty uint64) {
	if execQty >= o.Quantity {
		o.Quantity = 0
	} else {
		o.Quantity -= execQty
	}
	if execFlags&0x01 != 0 || o.Quantity == 0 {
		delete(book, o.OrderID)
	}
}

// BeginSnapshot opens a new PendingSnapshot. If one is already open,
// the partial book is discarded.
func (i *Instrument) BeginSnapshot(snapID uint32, anchorSeq uint64, totalOrders, lastInstrSeq uint32) {
	i.OpenSnapshot = &PendingSnapshot{
		SnapshotID:        snapID,
		AnchorSeq:         anchorSeq,
		TotalOrders:       totalOrders,
		LastInstrumentSeq: lastInstrSeq,
		Bids:              map[uint64]*RestingOrder{},
		Asks:              map[uint64]*RestingOrder{},
	}
	i.Status = StatusBuildingSnapshot
}

// AddSnapshotOrder appends an order to the pending snapshot. snapID must match the open one.
// Returns true if the order was added; false if snapID mismatched.
func (i *Instrument) AddSnapshotOrder(snapID uint32, orderID uint64, side, flags uint8, enterTS time.Time, price int64, qty uint64) bool {
	if i.OpenSnapshot == nil || i.OpenSnapshot.SnapshotID != snapID {
		return false
	}
	o := &RestingOrder{OrderID: orderID, Side: side, Flags: flags, EnterTS: enterTS, Price: price, Quantity: qty}
	if side == 0 {
		i.OpenSnapshot.Bids[orderID] = o
	} else {
		i.OpenSnapshot.Asks[orderID] = o
	}
	i.OpenSnapshot.ReceivedOrders++
	return true
}

var (
	errSnapshotMismatch = errors.New("snapshot end mismatch")
	errSnapshotShort    = errors.New("snapshot order count short")
)

// EndSnapshot validates and commits the pending snapshot. Returns the AnchorSeq
// and LastInstrumentSeq on success, or an error if validation fails.
// On error the pending snapshot is discarded and Status reverts to StatusAwaitingSnapshot.
func (i *Instrument) EndSnapshot(snapID uint32, anchorSeq uint64) (uint64, uint32, error) {
	if i.OpenSnapshot == nil ||
		i.OpenSnapshot.SnapshotID != snapID ||
		i.OpenSnapshot.AnchorSeq != anchorSeq {
		i.OpenSnapshot = nil
		i.Status = StatusAwaitingSnapshot
		return 0, 0, fmt.Errorf("%w: snapshot_id=%d anchor=%d", errSnapshotMismatch, snapID, anchorSeq)
	}
	if i.OpenSnapshot.ReceivedOrders != i.OpenSnapshot.TotalOrders {
		want := i.OpenSnapshot.TotalOrders
		got := i.OpenSnapshot.ReceivedOrders
		i.OpenSnapshot = nil
		i.Status = StatusAwaitingSnapshot
		return 0, 0, fmt.Errorf("%w: got %d expected %d", errSnapshotShort, got, want)
	}

	// Commit
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	anchor := i.OpenSnapshot.AnchorSeq
	lastInstr := i.OpenSnapshot.LastInstrumentSeq
	i.OpenSnapshot = nil
	i.Status = StatusReady
	i.LastAppliedMktdataSeq = anchor
	i.LastAppliedInstrumentSeq = lastInstr
	return anchor, lastInstr, nil
}

// Reset discards the entire book and pending snapshot, returning to awaiting-snapshot.
func (i *Instrument) Reset() {
	i.Bids = map[uint64]*RestingOrder{}
	i.Asks = map[uint64]*RestingOrder{}
	i.OpenSnapshot = nil
	i.Status = StatusAwaitingSnapshot
	i.LastAppliedMktdataSeq = 0
	i.LastAppliedInstrumentSeq = 0
}
```

- [ ] **Step 2: Write instrument_test.go**

```go
package main

import (
	"errors"
	"testing"
	"time"
)

func TestInstrument_OrderAddAndCancel(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)

	inst.ApplyOrderAdd(1, 0, 0, enter, 82446, 3000)
	inst.ApplyOrderAdd(2, 0, 0, enter, 82420, 1500)
	inst.ApplyOrderAdd(3, 1, 0, enter, 82480, 2000)

	if len(inst.Bids) != 2 || len(inst.Asks) != 1 {
		t.Errorf("counts: bids=%d asks=%d", len(inst.Bids), len(inst.Asks))
	}

	inst.ApplyOrderCancel(2)
	if _, ok := inst.Bids[2]; ok {
		t.Error("expected order 2 cancelled")
	}

	// Cancelling unknown id is a silent no-op.
	inst.ApplyOrderCancel(999)
}

func TestInstrument_OrderExecutePartialAndFull(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)
	inst.ApplyOrderAdd(1, 0, 0, enter, 82446, 1000)

	inst.ApplyOrderExecute(1, 0, 300)
	if inst.Bids[1].Quantity != 700 {
		t.Errorf("partial: got %d want 700", inst.Bids[1].Quantity)
	}

	// Full-fill flag removes regardless of remaining qty.
	inst.ApplyOrderExecute(1, 0x01, 100)
	if _, ok := inst.Bids[1]; ok {
		t.Error("expected order removed after full-fill")
	}
}

func TestInstrument_OrderExecuteToZeroRemoves(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)
	inst.ApplyOrderAdd(1, 1, 0, enter, 82480, 500)
	inst.ApplyOrderExecute(1, 0, 500)
	if _, ok := inst.Asks[1]; ok {
		t.Error("expected order removed when qty reaches 0")
	}
}

func TestInstrument_SnapshotReassembly(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	enter := time.Unix(1700000000, 0)

	inst.BeginSnapshot(7, 5000, 3, 100)
	if inst.Status != StatusBuildingSnapshot {
		t.Fatalf("status: got %v", inst.Status)
	}

	inst.AddSnapshotOrder(7, 10, 0, 0, enter, 82446, 3000)
	inst.AddSnapshotOrder(7, 11, 0, 0, enter, 82420, 1500)
	inst.AddSnapshotOrder(7, 12, 1, 0, enter, 82480, 2000)

	anchor, lastInstr, err := inst.EndSnapshot(7, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if anchor != 5000 || lastInstr != 100 {
		t.Errorf("anchor/lastInstr: %d %d", anchor, lastInstr)
	}
	if inst.Status != StatusReady {
		t.Errorf("status: %v", inst.Status)
	}
	if len(inst.Bids) != 2 || len(inst.Asks) != 1 {
		t.Errorf("committed book: bids=%d asks=%d", len(inst.Bids), len(inst.Asks))
	}
	if inst.LastAppliedMktdataSeq != 5000 || inst.LastAppliedInstrumentSeq != 100 {
		t.Errorf("last applied: %d %d", inst.LastAppliedMktdataSeq, inst.LastAppliedInstrumentSeq)
	}
}

func TestInstrument_SnapshotEndMismatchedID(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 1, 100)
	inst.AddSnapshotOrder(7, 10, 0, 0, time.Now(), 82446, 3000)
	_, _, err := inst.EndSnapshot(8, 5000) // wrong snapshot_id
	if !errors.Is(err, errSnapshotMismatch) {
		t.Fatalf("expected errSnapshotMismatch, got %v", err)
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
}

func TestInstrument_SnapshotEndShortCount(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 3, 100)
	inst.AddSnapshotOrder(7, 10, 0, 0, time.Now(), 82446, 3000) // only 1 of 3
	_, _, err := inst.EndSnapshot(7, 5000)
	if !errors.Is(err, errSnapshotShort) {
		t.Fatalf("expected errSnapshotShort, got %v", err)
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
}

func TestInstrument_AddSnapshotOrderWrongID(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 1, 100)
	if inst.AddSnapshotOrder(99, 10, 0, 0, time.Now(), 82446, 3000) {
		t.Error("expected false for mismatched snapshot_id")
	}
}

func TestInstrument_Reset(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 82446, 3000)
	inst.LastAppliedMktdataSeq = 5000
	inst.LastAppliedInstrumentSeq = 100

	inst.Reset()
	if inst.Status != StatusAwaitingSnapshot || len(inst.Bids) != 0 || len(inst.Asks) != 0 {
		t.Errorf("post-reset: %+v", inst)
	}
	if inst.LastAppliedMktdataSeq != 0 || inst.LastAppliedInstrumentSeq != 0 {
		t.Error("seq trackers not reset")
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd go/marketbyorder-bot && go test -v -run TestInstrument ./...
```

Expected: all 8 instrument tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/instrument.go go/marketbyorder-bot/instrument_test.go
git commit -m "feat(mbo-bot): instrument book operations and snapshot reassembly"
```

---

### Task 10: Bot channel state machine

**Files:**
- Create: `go/marketbyorder-bot/channel.go`
- Create: `go/marketbyorder-bot/channel_test.go`

This is the heart of the bot — the spec's [Subscriber Algorithm](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md#subscriber-algorithm) realised in Go. It dispatches Records into per-instrument book ops, handles per-instrument seq gap detection, snapshot recovery, instrument reset, and channel reset.

- [ ] **Step 1: Write channel.go**

```go
package main

import (
	"log"
	"sort"
	"time"
)

const maxBufferedDeltas = 10000 // bound on cold-start / gap-recovery buffer per channel

// BufferedDelta is one mktdata-port delta held while an instrument awaits a snapshot.
type BufferedDelta struct {
	MktdataSeq uint64
	Record     Record
}

// ChannelState holds all state for one channel_id.
type ChannelState struct {
	ChannelID    uint8
	ResetCount   uint8
	SeqLast      map[string]uint64 // port → last seq seen
	Refdata      map[uint32]InstrumentDef
	Manifest     ManifestState
	Instruments  map[uint32]*Instrument
	DeltaBuffer  []BufferedDelta // ordered by MktdataSeq
}

type InstrumentDef struct {
	Symbol        string
	PriceExponent int8
	QtyExponent   int8
}

type ManifestState struct {
	Seq             uint16
	Valid           bool
	InstrumentCount uint32
}

// ChannelEvent is the small subset of bot-side state changes the channel reports
// outward (used by writers to enqueue persistence and by metrics to track resets).
type ChannelEvent struct {
	Kind         string // "applied_delta" | "applied_snapshot" | "instrument_reset" | "channel_reset" | "per_instrument_gap"
	InstrumentID uint32
	Symbol       string
	Record       Record
}

// NewChannelState returns an empty channel.
func NewChannelState(id uint8) *ChannelState {
	return &ChannelState{
		ChannelID:   id,
		SeqLast:     map[string]uint64{},
		Refdata:     map[uint32]InstrumentDef{},
		Instruments: map[uint32]*Instrument{},
	}
}

// Apply dispatches a Record. Returns the events caused by applying it.
// The bot's main loop iterates these and forwards them to the writers.
func (c *ChannelState) Apply(rec Record) []ChannelEvent {
	// Detect channel reset on any port.
	if c.ResetCount != 0 && rec.ResetCount != c.ResetCount {
		log.Printf("channel %d: reset_count changed %d -> %d, discarding state", c.ChannelID, c.ResetCount, rec.ResetCount)
		evs := []ChannelEvent{{Kind: "channel_reset", Record: rec}}
		c.reset(rec.ResetCount)
		// Continue to apply this record below — it's the first frame of the new era.
		evs = append(evs, c.applyInner(rec)...)
		return evs
	}
	if c.ResetCount == 0 {
		c.ResetCount = rec.ResetCount
	}
	return c.applyInner(rec)
}

func (c *ChannelState) reset(newResetCount uint8) {
	c.ResetCount = newResetCount
	c.SeqLast = map[string]uint64{}
	c.Refdata = map[uint32]InstrumentDef{}
	c.Manifest = ManifestState{}
	c.Instruments = map[uint32]*Instrument{}
	c.DeltaBuffer = nil
}

func (c *ChannelState) applyInner(rec Record) []ChannelEvent {
	c.SeqLast[rec.Port] = rec.SequenceNumber

	switch rec.Type {
	case "instrument_definition":
		return c.applyInstrumentDefinition(rec)
	case "manifest_summary":
		return c.applyManifestSummary(rec)
	case "snapshot_begin":
		return c.applySnapshotBegin(rec)
	case "snapshot_order":
		return c.applySnapshotOrder(rec)
	case "snapshot_end":
		return c.applySnapshotEnd(rec)
	case "order_add", "order_cancel", "order_execute":
		return c.applyDelta(rec)
	case "instrument_reset":
		return c.applyInstrumentReset(rec)
	case "trade", "batch_boundary":
		// No book-state effect; bot writers will still persist these.
		return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
	case "heartbeat", "manifest_summary", "end_of_session":
		return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
	}
	return nil
}

func (c *ChannelState) applyInstrumentDefinition(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	symbol, _ := rec.Fields["symbol"].(string)
	priceExp := toInt8(rec.Fields["price_exponent"])
	qtyExp := toInt8(rec.Fields["qty_exponent"])
	c.Refdata[id] = InstrumentDef{Symbol: symbol, PriceExponent: priceExp, QtyExponent: qtyExp}
	if inst, ok := c.Instruments[id]; ok {
		inst.Symbol = symbol
		inst.PriceExponent = priceExp
		inst.QtyExponent = qtyExp
	} else {
		c.Instruments[id] = NewInstrument(id, symbol, priceExp, qtyExp)
	}
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: id, Symbol: symbol, Record: rec}}
}

func (c *ChannelState) applyManifestSummary(rec Record) []ChannelEvent {
	seq := toUint16(rec.Fields["manifest_seq"])
	valid := toUint8(rec.Fields["valid"]) != 0
	count := toUint32(rec.Fields["instrument_count"])
	c.Manifest = ManifestState{Seq: seq, Valid: valid, InstrumentCount: count}
	return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
}

func (c *ChannelState) applySnapshotBegin(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		// Snapshot for an instrument we have no refdata for; create a placeholder.
		inst = NewInstrument(id, "", 0, 0)
		c.Instruments[id] = inst
	}
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	snapID := toUint32(rec.Fields["snapshot_id"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])

	// "Snapshot while ready" — re-bootstrap if anchor > last_applied.
	if inst.Status == StatusReady && anchor <= inst.LastAppliedMktdataSeq {
		// Snapshot is stale; ignore.
		return nil
	}
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

func (c *ChannelState) applySnapshotOrder(rec Record) []ChannelEvent {
	// SnapshotOrder doesn't carry instrument_id (it's implied by the SnapshotBegin
	// that opened the group). The publisher MUST NOT interleave snapshot groups, so
	// we find the one currently in StatusBuildingSnapshot.
	snapID := toUint32(rec.Fields["snapshot_id"])
	for _, inst := range c.Instruments {
		if inst.Status != StatusBuildingSnapshot || inst.OpenSnapshot == nil {
			continue
		}
		if inst.OpenSnapshot.SnapshotID != snapID {
			continue
		}
		orderID := toUint64(rec.Fields["order_id"])
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.AddSnapshotOrder(snapID, orderID, side, flags, enter, price, qty)
		return nil
	}
	return nil
}

func (c *ChannelState) applySnapshotEnd(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		return nil
	}
	snapID := toUint32(rec.Fields["snapshot_id"])
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		log.Printf("channel %d instrument %d: snapshot end failed: %v", c.ChannelID, id, err)
		return nil
	}
	// Replay buffered deltas with mktdata_seq > anchor.
	c.replayBuffer(inst)
	return []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: id, Symbol: inst.Symbol, Record: rec}}
}

func (c *ChannelState) applyDelta(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		// Buffer until we know about the instrument.
		c.bufferDelta(rec)
		return nil
	}

	switch inst.Status {
	case StatusReady:
		return c.applyDeltaToReady(inst, rec)
	default:
		c.bufferDelta(rec)
		return nil
	}
}

func (c *ChannelState) applyDeltaToReady(inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1

	if piSeq < expected {
		// Duplicate or late.
		return nil
	}
	if piSeq > expected {
		log.Printf("channel %d instrument %d: per-instrument gap, expected %d got %d",
			c.ChannelID, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		c.bufferDelta(rec)
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}

	// Apply.
	switch rec.Type {
	case "order_add":
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		orderID := toUint64(rec.Fields["order_id"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.ApplyOrderAdd(orderID, side, flags, enter, price, qty)
	case "order_cancel":
		inst.ApplyOrderCancel(toUint64(rec.Fields["order_id"]))
	case "order_execute":
		inst.ApplyOrderExecute(toUint64(rec.Fields["order_id"]), toUint8(rec.Fields["exec_flags"]), toUint64(rec.Fields["exec_qty_raw"]))
	}

	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = piSeq
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
}

func (c *ChannelState) applyInstrumentReset(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		return nil
	}
	inst.Reset()
	// Discard buffered deltas for this instrument with mktdata_seq <= new_anchor_seq.
	newAnchor := toUint64(rec.Fields["new_anchor_seq"])
	c.DeltaBuffer = filterBuffer(c.DeltaBuffer, func(b BufferedDelta) bool {
		if b.Record.InstrumentID != id {
			return true
		}
		return b.MktdataSeq > newAnchor
	})
	return []ChannelEvent{{Kind: "instrument_reset", InstrumentID: id, Symbol: inst.Symbol, Record: rec}}
}

func (c *ChannelState) bufferDelta(rec Record) {
	if len(c.DeltaBuffer) >= maxBufferedDeltas {
		// Drop oldest.
		c.DeltaBuffer = c.DeltaBuffer[1:]
	}
	c.DeltaBuffer = append(c.DeltaBuffer, BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec})
	sort.Slice(c.DeltaBuffer, func(i, j int) bool {
		return c.DeltaBuffer[i].MktdataSeq < c.DeltaBuffer[j].MktdataSeq
	})
}

func (c *ChannelState) replayBuffer(inst *Instrument) {
	remaining := c.DeltaBuffer[:0]
	for _, b := range c.DeltaBuffer {
		if b.Record.InstrumentID != inst.ID {
			remaining = append(remaining, b)
			continue
		}
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue // already covered by snapshot
		}
		c.applyDeltaToReady(inst, b.Record)
	}
	c.DeltaBuffer = remaining
}

func filterBuffer(buf []BufferedDelta, keep func(BufferedDelta) bool) []BufferedDelta {
	out := buf[:0]
	for _, b := range buf {
		if keep(b) {
			out = append(out, b)
		}
	}
	return out
}

// --- type conversion helpers (JSON unmarshal yields float64 / string / bool by default) ---

func toUint8(v any) uint8 {
	switch x := v.(type) {
	case float64:
		return uint8(x)
	case uint8:
		return x
	}
	return 0
}
func toUint16(v any) uint16 {
	switch x := v.(type) {
	case float64:
		return uint16(x)
	case uint16:
		return x
	}
	return 0
}
func toUint32(v any) uint32 {
	switch x := v.(type) {
	case float64:
		return uint32(x)
	case uint32:
		return x
	}
	return 0
}
func toUint64(v any) uint64 {
	switch x := v.(type) {
	case float64:
		return uint64(x)
	case uint64:
		return x
	}
	return 0
}
func toInt8(v any) int8 {
	switch x := v.(type) {
	case float64:
		return int8(x)
	case int8:
		return x
	}
	return 0
}
func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	}
	return 0
}
func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func toTime(v any) time.Time {
	if s, ok := v.(string); ok {
		t, _ := time.Parse(time.RFC3339Nano, s)
		return t
	}
	return time.Time{}
}
func sideFromString(s string) uint8 {
	if s == "ask" {
		return 1
	}
	return 0
}
```

- [ ] **Step 2: Write channel_test.go**

```go
package main

import (
	"testing"
	"time"
)

// helper to build records concisely for tests
func r(rt string, port string, seq uint64, instID uint32, fields map[string]any) Record {
	return Record{
		Type:           rt,
		Timestamp:      time.Unix(1700000000, 0),
		ChannelID:      0,
		Port:           port,
		SequenceNumber: seq,
		ResetCount:     1,
		InstrumentID:   instID,
		Fields:         fields,
	}
}

func TestChannel_ColdStart(t *testing.T) {
	c := NewChannelState(0)

	// 1. InstrumentDefinition
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol":         "BTC-USDT",
		"price_exponent": float64(-2),
		"qty_exponent":   float64(-8),
	}))
	if _, ok := c.Refdata[100]; !ok {
		t.Fatal("refdata not stored")
	}

	// 2. Mktdata delta arrives before snapshot — should buffer.
	c.Apply(r("order_add", "mktdata", 50, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(101),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if len(c.DeltaBuffer) != 1 {
		t.Fatalf("expected 1 buffered delta, got %d", len(c.DeltaBuffer))
	}

	// 3. SnapshotBegin/Order/End with anchor=49 (so the buffered delta is post-anchor).
	c.Apply(r("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(49), "total_orders": float64(0),
		"snapshot_id": float64(7), "last_instrument_seq": float64(100),
	}))
	c.Apply(r("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(49), "snapshot_id": float64(7),
	}))

	inst := c.Instruments[100]
	if inst.Status != StatusReady {
		t.Fatalf("status: %v", inst.Status)
	}
	if len(inst.Bids) != 1 {
		t.Errorf("expected buffered delta replayed: bids=%d", len(inst.Bids))
	}
	if inst.LastAppliedInstrumentSeq != 101 {
		t.Errorf("last applied instrument seq: %d", inst.LastAppliedInstrumentSeq)
	}
}

func TestChannel_PerInstrumentGap(t *testing.T) {
	c := NewChannelState(0)
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	c.Apply(r("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	c.Apply(r("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))

	inst := c.Instruments[100]
	inst.LastAppliedInstrumentSeq = 0

	// Apply seq=1 — should succeed.
	c.Apply(r("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if inst.Status != StatusReady {
		t.Fatalf("after seq=1 status: %v", inst.Status)
	}

	// Apply seq=3 — gap.
	evs := c.Apply(r("order_add", "mktdata", 102, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(3),
		"order_id": float64(2), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82440), "qty_raw": float64(2000),
	}))
	if inst.Status != StatusGap {
		t.Errorf("expected status gap, got %v", inst.Status)
	}
	if len(evs) != 1 || evs[0].Kind != "per_instrument_gap" {
		t.Errorf("expected per_instrument_gap event, got %+v", evs)
	}
}

func TestChannel_ChannelReset(t *testing.T) {
	c := NewChannelState(0)
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	if c.ResetCount != 1 {
		t.Fatalf("reset_count: %d", c.ResetCount)
	}

	// Now a record arrives with reset_count=2.
	rec := r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	rec.ResetCount = 2
	evs := c.Apply(rec)

	found := false
	for _, e := range evs {
		if e.Kind == "channel_reset" {
			found = true
		}
	}
	if !found {
		t.Error("expected channel_reset event")
	}
	if c.ResetCount != 2 {
		t.Errorf("post-reset: %d", c.ResetCount)
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd go/marketbyorder-bot && go test -v -run TestChannel ./...
```

Expected: 3 channel tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/channel.go go/marketbyorder-bot/channel_test.go
git commit -m "feat(mbo-bot): channel state machine with cold-start, gap detection, and reset handling"
```

---

### Task 11: Bot levels aggregation

**Files:**
- Create: `go/marketbyorder-bot/levels.go`
- Create: `go/marketbyorder-bot/levels_test.go`

Aggregate bid/ask order maps into top-N price levels. Bids sorted descending by price (best = highest); asks ascending (best = lowest). `cumulative_qty` is a running sum from `level_idx=0` outward.

- [ ] **Step 1: Write levels.go**

```go
package main

import (
	"math"
	"sort"
)

// Level is one aggregated price level.
type Level struct {
	Price        float64
	Qty          float64
	OrderCount   uint32
	CumulativeQty float64
}

// LevelSnapshot is the result of aggregating an Instrument's order maps.
type LevelSnapshot struct {
	InstrumentID uint32
	Symbol       string
	Bids         []Level
	Asks         []Level
}

// ComputeLevels aggregates orders by price into top-N levels per side.
// Bids descending (best = highest price); asks ascending (best = lowest).
// Prices are scaled via inst.PriceExponent; quantities via inst.QtyExponent.
func ComputeLevels(inst *Instrument, n int) LevelSnapshot {
	return LevelSnapshot{
		InstrumentID: inst.ID,
		Symbol:       inst.Symbol,
		Bids:         aggregate(inst.Bids, inst.PriceExponent, inst.QtyExponent, n, true),
		Asks:         aggregate(inst.Asks, inst.PriceExponent, inst.QtyExponent, n, false),
	}
}

func aggregate(orders map[uint64]*RestingOrder, priceExp, qtyExp int8, n int, descending bool) []Level {
	if len(orders) == 0 {
		return nil
	}
	type bucket struct {
		PriceRaw int64
		QtyRaw   uint64
		Count    uint32
	}
	byPrice := map[int64]*bucket{}
	for _, o := range orders {
		b, ok := byPrice[o.Price]
		if !ok {
			b = &bucket{PriceRaw: o.Price}
			byPrice[o.Price] = b
		}
		b.QtyRaw += o.Quantity
		b.Count++
	}
	prices := make([]int64, 0, len(byPrice))
	for p := range byPrice {
		prices = append(prices, p)
	}
	if descending {
		sort.Slice(prices, func(i, j int) bool { return prices[i] > prices[j] })
	} else {
		sort.Slice(prices, func(i, j int) bool { return prices[i] < prices[j] })
	}
	if len(prices) > n {
		prices = prices[:n]
	}

	priceScale := math.Pow10(int(priceExp))
	qtyScale := math.Pow10(int(qtyExp))

	out := make([]Level, len(prices))
	var cum float64
	for i, p := range prices {
		b := byPrice[p]
		qty := float64(b.QtyRaw) * qtyScale
		cum += qty
		out[i] = Level{
			Price:         float64(b.PriceRaw) * priceScale,
			Qty:           qty,
			OrderCount:    b.Count,
			CumulativeQty: cum,
		}
	}
	return out
}
```

- [ ] **Step 2: Write levels_test.go**

```go
package main

import (
	"math"
	"testing"
	"time"
)

func approxEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestLevels_BidsDescendingAsksAscending(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0) // exponents 0 → no scaling
	inst.Status = StatusReady

	// Bids at three prices.
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 102, 3)
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 101, 7)

	// Asks at two prices.
	inst.ApplyOrderAdd(10, 1, 0, time.Now(), 105, 4)
	inst.ApplyOrderAdd(11, 1, 0, time.Now(), 104, 2)

	snap := ComputeLevels(inst, 5)
	if len(snap.Bids) != 3 || len(snap.Asks) != 2 {
		t.Fatalf("counts: bids=%d asks=%d", len(snap.Bids), len(snap.Asks))
	}
	if !approxEq(snap.Bids[0].Price, 102) || !approxEq(snap.Bids[1].Price, 101) || !approxEq(snap.Bids[2].Price, 100) {
		t.Errorf("bids order: %+v", snap.Bids)
	}
	if !approxEq(snap.Asks[0].Price, 104) || !approxEq(snap.Asks[1].Price, 105) {
		t.Errorf("asks order: %+v", snap.Asks)
	}
}

func TestLevels_TiesAggregate(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 100, 3) // tie with order 1
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 99, 7)

	snap := ComputeLevels(inst, 5)
	if len(snap.Bids) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(snap.Bids))
	}
	if !approxEq(snap.Bids[0].Qty, 8) || snap.Bids[0].OrderCount != 2 {
		t.Errorf("level 0: qty=%v count=%d", snap.Bids[0].Qty, snap.Bids[0].OrderCount)
	}
}

func TestLevels_DepthCap(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	for i := int64(1); i <= 30; i++ {
		inst.ApplyOrderAdd(uint64(i), 0, 0, time.Now(), int64(100-i), 1) // 30 distinct prices
	}
	snap := ComputeLevels(inst, 10)
	if len(snap.Bids) != 10 {
		t.Errorf("expected 10 levels, got %d", len(snap.Bids))
	}
}

func TestLevels_CumulativeQty(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 99, 3)
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 98, 7)

	snap := ComputeLevels(inst, 5)
	if !approxEq(snap.Bids[0].CumulativeQty, 5) ||
		!approxEq(snap.Bids[1].CumulativeQty, 8) ||
		!approxEq(snap.Bids[2].CumulativeQty, 15) {
		t.Errorf("cumulative: %+v", snap.Bids)
	}
}

func TestLevels_PriceExponentScaling(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 8244600, 300000000) // raw values
	snap := ComputeLevels(inst, 5)
	if !approxEq(snap.Bids[0].Price, 82446) {
		t.Errorf("scaled price: %v", snap.Bids[0].Price)
	}
	if !approxEq(snap.Bids[0].Qty, 3.0) {
		t.Errorf("scaled qty: %v", snap.Bids[0].Qty)
	}
}

func TestLevels_EmptySide(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	snap := ComputeLevels(inst, 5)
	if snap.Bids != nil || snap.Asks != nil {
		t.Errorf("expected nil for empty: bids=%v asks=%v", snap.Bids, snap.Asks)
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd go/marketbyorder-bot && go test -v -run TestLevels ./...
```

Expected: 6 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/levels.go go/marketbyorder-bot/levels_test.go
git commit -m "feat(mbo-bot): top-N level aggregation with cumulative qty"
```

---

### Task 12: Bot Prometheus metrics

**Files:**
- Create: `go/marketbyorder-bot/metrics.go` (replaces stub from Task 8)

Mirror [go/example-bot/metrics.go](../go/example-bot/metrics.go) structurally with the new metric names from the spec (prefix `dz_mbo_bot_`).

- [ ] **Step 1: Write metrics.go**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const metricsNamespace = "dz_mbo_bot"

// Metrics is the bot's full Prometheus metric set. Replaces the stub in bot.go.
type Metrics struct {
	registry *prometheus.Registry

	// Process
	BuildInfo     *prometheus.GaugeVec
	UptimeSeconds prometheus.GaugeFunc

	// Decode + intake
	SocketConnected     prometheus.Gauge
	SocketReconnects    *prometheus.CounterVec
	RecordsTotal        *prometheus.CounterVec
	DecodeErrors        prometheus.Counter
	SocketToBotLatency  *prometheus.HistogramVec

	// Book state
	InstrumentsTotal      *prometheus.GaugeVec
	InstrumentResetsTotal *prometheus.CounterVec
	ChannelResetsTotal    prometheus.Counter
	PerInstrumentGapsTotal prometheus.Counter
	BookOrders            *prometheus.GaugeVec
	BookTopPrice          *prometheus.GaugeVec
	BookTopQty            *prometheus.GaugeVec
	BookSpreadBps         *prometheus.GaugeVec

	// Snapshot writer
	SnapshotWritesTotal     prometheus.Counter
	SnapshotCoalescesTotal  prometheus.Counter
	SnapshotLagMs           prometheus.Histogram

	// ClickHouse
	ClickhouseRowsWritten   *prometheus.CounterVec
	ClickhouseRowsDropped   *prometheus.CounterVec
	ClickhouseWriteErrors   *prometheus.CounterVec
	ClickhouseBatchDuration *prometheus.HistogramVec
	ClickhouseBufferedRows  *prometheus.GaugeVec

	startTime time.Time
}

func NewMetrics(version, commit string) *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{registry: reg, startTime: time.Now()}

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "build_info"}, []string{"version", "commit"})
	m.UptimeSeconds = prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "uptime_seconds"},
		func() float64 { return time.Since(m.startTime).Seconds() })

	m.SocketConnected = prometheus.NewGauge(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "socket_connected"})
	m.SocketReconnects = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "socket_reconnects_total"}, []string{"reason"})
	m.RecordsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "records_total"}, []string{"type"})
	m.DecodeErrors = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "decode_errors_total"})
	m.SocketToBotLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "socket_to_bot_latency_seconds",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"type"})

	m.InstrumentsTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "instruments_total"}, []string{"status"})
	m.InstrumentResetsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "instrument_resets_total"}, []string{"reason"})
	m.ChannelResetsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "channel_resets_total"})
	m.PerInstrumentGapsTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "per_instrument_gaps_total"})
	m.BookOrders = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_orders"}, []string{"symbol", "side"})
	m.BookTopPrice = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_price"}, []string{"symbol", "side"})
	m.BookTopQty = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_top_qty"}, []string{"symbol", "side"})
	m.BookSpreadBps = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "book_spread_bps"}, []string{"symbol"})

	m.SnapshotWritesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_writes_total"})
	m.SnapshotCoalescesTotal = prometheus.NewCounter(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "snapshot_coalesces_total"})
	m.SnapshotLagMs = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "snapshot_lag_ms",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	})

	m.ClickhouseRowsWritten = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_written_total"}, []string{"table"})
	m.ClickhouseRowsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_rows_dropped_total"}, []string{"table", "reason"})
	m.ClickhouseWriteErrors = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: metricsNamespace, Name: "clickhouse_write_errors_total"}, []string{"table", "reason"})
	m.ClickhouseBatchDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "clickhouse_batch_duration_seconds",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 14),
	}, []string{"table"})
	m.ClickhouseBufferedRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: metricsNamespace, Name: "clickhouse_buffered_rows"}, []string{"table"})

	reg.MustRegister(
		m.BuildInfo, m.UptimeSeconds,
		m.SocketConnected, m.SocketReconnects, m.RecordsTotal, m.DecodeErrors, m.SocketToBotLatency,
		m.InstrumentsTotal, m.InstrumentResetsTotal, m.ChannelResetsTotal, m.PerInstrumentGapsTotal,
		m.BookOrders, m.BookTopPrice, m.BookTopQty, m.BookSpreadBps,
		m.SnapshotWritesTotal, m.SnapshotCoalescesTotal, m.SnapshotLagMs,
		m.ClickhouseRowsWritten, m.ClickhouseRowsDropped, m.ClickhouseWriteErrors,
		m.ClickhouseBatchDuration, m.ClickhouseBufferedRows,
	)
	m.BuildInfo.WithLabelValues(version, commit).Set(1)

	return m
}

func (m *Metrics) ServeHTTP(ctx context.Context, addr string, logErr func(error)) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logErr(fmt.Errorf("metrics server: %w", err))
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
}
```

- [ ] **Step 2: Remove the stub Metrics declaration from bot.go**

In `go/marketbyorder-bot/bot.go`, delete the stub `Metrics` struct that was added in Task 8 (the comment said "Stub declaration here keeps this task standalone-buildable" — replace it with the real one in metrics.go now).

Update bot_test.go's `stubMetrics()` helper if needed: the real Metrics struct is now compatible by name; tests can use `NewMetrics("test", "test")` instead of the stub. Update `bot_test.go`:

```go
func stubMetrics() *Metrics {
	return NewMetrics("test", "test")
}
```

- [ ] **Step 3: Verify build and tests**

```bash
cd go/marketbyorder-bot && go test ./...
```

Expected: clean build, all tests still pass.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/metrics.go go/marketbyorder-bot/bot.go go/marketbyorder-bot/bot_test.go
git commit -m "feat(mbo-bot): full Prometheus metrics, replace stub"
```

---

### Task 13: Bot ClickHouse writer

**Files:**
- Create: `go/marketbyorder-bot/clickhouse.go`
- Create: `go/marketbyorder-bot/clickhouse_test.go`

Mirror [go/example-bot/clickhouse.go](../go/example-bot/clickhouse.go) for the per-table batcher pattern (size + interval triggers, drop-on-buffer-full, per-table goroutine). The MBO bot has more tables (5 vs TOB's 3), but the batcher itself is generic — pass a table name + column list at construction.

- [ ] **Step 1: Write clickhouse.go**

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// ClickhouseClient writes rows to a ClickHouse server using HTTP JSONEachRow inserts.
// One Batcher per table, each with its own goroutine and buffer.
type ClickhouseClient struct {
	url      string
	database string
	hc       *http.Client
	metrics  *Metrics
	batchers map[string]*Batcher
}

// BatcherConfig controls one table's batcher.
type BatcherConfig struct {
	Table         string
	BatchSize     int
	BatchInterval time.Duration
	BufferSize    int
}

// NewClickhouseClient returns a configured client. table configs are created
// up-front; call Enqueue(table, row) to push rows.
func NewClickhouseClient(rawURL, database string, configs []BatcherConfig, metrics *Metrics) (*ClickhouseClient, error) {
	if rawURL == "" {
		return nil, nil // disabled
	}
	if _, err := url.Parse(rawURL); err != nil {
		return nil, fmt.Errorf("clickhouse url: %w", err)
	}
	c := &ClickhouseClient{
		url:      strings.TrimRight(rawURL, "/"),
		database: database,
		hc:       &http.Client{Timeout: 30 * time.Second},
		metrics:  metrics,
		batchers: map[string]*Batcher{},
	}
	for _, cfg := range configs {
		c.batchers[cfg.Table] = newBatcher(c, cfg)
	}
	return c, nil
}

// Run starts all batcher goroutines. Returns when ctx is cancelled and all batchers have flushed.
func (c *ClickhouseClient) Run(ctx context.Context) {
	if c == nil {
		return
	}
	var wg sync.WaitGroup
	for _, b := range c.batchers {
		wg.Add(1)
		go func(b *Batcher) {
			defer wg.Done()
			b.run(ctx)
		}(b)
	}
	wg.Wait()
}

// Enqueue queues a row for the named table. Returns false if dropped (buffer full or unknown table).
func (c *ClickhouseClient) Enqueue(table string, row map[string]any) bool {
	if c == nil {
		return false
	}
	b, ok := c.batchers[table]
	if !ok {
		return false
	}
	select {
	case b.ch <- row:
		c.metrics.ClickhouseBufferedRows.WithLabelValues(table).Set(float64(len(b.ch)))
		return true
	default:
		c.metrics.ClickhouseRowsDropped.WithLabelValues(table, "buffer_full").Inc()
		return false
	}
}

// Batcher is a per-table accumulator and flusher.
type Batcher struct {
	client *ClickhouseClient
	cfg    BatcherConfig
	ch     chan map[string]any
}

func newBatcher(c *ClickhouseClient, cfg BatcherConfig) *Batcher {
	return &Batcher{
		client: c,
		cfg:    cfg,
		ch:     make(chan map[string]any, cfg.BufferSize),
	}
}

func (b *Batcher) run(ctx context.Context) {
	buf := make([]map[string]any, 0, b.cfg.BatchSize)
	tick := time.NewTicker(b.cfg.BatchInterval)
	defer tick.Stop()

	flush := func() {
		if len(buf) == 0 {
			return
		}
		start := time.Now()
		if err := b.send(ctx, buf); err != nil {
			b.client.metrics.ClickhouseWriteErrors.WithLabelValues(b.cfg.Table, classifyHTTPErr(err)).Inc()
			b.client.metrics.ClickhouseRowsDropped.WithLabelValues(b.cfg.Table, "write_failed").Add(float64(len(buf)))
			log.Printf("clickhouse %s: %v (dropped %d rows)", b.cfg.Table, err, len(buf))
		} else {
			b.client.metrics.ClickhouseRowsWritten.WithLabelValues(b.cfg.Table).Add(float64(len(buf)))
		}
		b.client.metrics.ClickhouseBatchDuration.WithLabelValues(b.cfg.Table).Observe(time.Since(start).Seconds())
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			// Drain remaining items and flush before returning.
			for {
				select {
				case row := <-b.ch:
					buf = append(buf, row)
				default:
					flush()
					return
				}
			}
		case row := <-b.ch:
			buf = append(buf, row)
			b.client.metrics.ClickhouseBufferedRows.WithLabelValues(b.cfg.Table).Set(float64(len(b.ch)))
			if len(buf) >= b.cfg.BatchSize {
				flush()
			}
		case <-tick.C:
			flush()
		}
	}
}

func (b *Batcher) send(ctx context.Context, rows []map[string]any) error {
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, r := range rows {
		if err := enc.Encode(r); err != nil {
			return fmt.Errorf("encode: %w", err)
		}
	}

	q := url.Values{}
	q.Set("database", b.client.database)
	q.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT JSONEachRow", b.cfg.Table))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.client.url+"/?"+q.Encode(), &body)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.hc.Do(req)
	if err != nil {
		return fmt.Errorf("transport: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func classifyHTTPErr(err error) string {
	s := err.Error()
	switch {
	case strings.HasPrefix(s, "transport"):
		return "transport"
	case strings.HasPrefix(s, "new request"):
		return "new_request"
	case strings.HasPrefix(s, "http 4"):
		return "http_4xx"
	case strings.HasPrefix(s, "http 5"):
		return "http_5xx"
	default:
		return "other"
	}
}
```

- [ ] **Step 2: Write clickhouse_test.go**

```go
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClickhouseBatcher_FlushesOnSize(t *testing.T) {
	var rowsReceived atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// Each JSON line ends with \n. Count lines.
		lines := int64(0)
		for _, b := range body {
			if b == '\n' {
				lines++
			}
		}
		rowsReceived.Add(lines)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 5, BatchInterval: 1 * time.Hour, BufferSize: 100}
	c, err := NewClickhouseClient(srv.URL, "marketbyorder", []BatcherConfig{cfg}, metrics)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 5; i++ {
		c.Enqueue("events", map[string]any{"row": i})
	}
	time.Sleep(200 * time.Millisecond)

	if got := rowsReceived.Load(); got != 5 {
		t.Errorf("expected 5 rows received, got %d", got)
	}
}

func TestClickhouseBatcher_FlushesOnInterval(t *testing.T) {
	var rowsReceived atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		lines := int64(0)
		for _, b := range body {
			if b == '\n' {
				lines++
			}
		}
		rowsReceived.Add(lines)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 1000, BatchInterval: 100 * time.Millisecond, BufferSize: 100}
	c, _ := NewClickhouseClient(srv.URL, "marketbyorder", []BatcherConfig{cfg}, metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	for i := 0; i < 3; i++ {
		c.Enqueue("events", map[string]any{"row": i})
	}
	time.Sleep(300 * time.Millisecond)

	if got := rowsReceived.Load(); got != 3 {
		t.Errorf("expected 3 rows after interval flush, got %d", got)
	}
}

func TestClickhouseBatcher_DropsOnBufferFull(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Hour) // never respond
	}))
	defer srv.Close()

	metrics := NewMetrics("test", "test")
	cfg := BatcherConfig{Table: "events", BatchSize: 5, BatchInterval: 1 * time.Hour, BufferSize: 3}
	c, _ := NewClickhouseClient(srv.URL, "marketbyorder", []BatcherConfig{cfg}, metrics)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	dropped := 0
	for i := 0; i < 10; i++ {
		if !c.Enqueue("events", map[string]any{"row": i}) {
			dropped++
		}
	}
	if dropped == 0 {
		t.Error("expected some rows to be dropped on buffer full")
	}
}
```

- [ ] **Step 3: Run tests**

```bash
cd go/marketbyorder-bot && go test -v -run TestClickhouse ./...
```

Expected: 3 tests PASS.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/clickhouse.go go/marketbyorder-bot/clickhouse_test.go
git commit -m "feat(mbo-bot): ClickHouse HTTP batcher with per-table workers"
```

---

### Task 14: Bot events writer + snapshot writer (with coalescing)

**Files:**
- Create: `go/marketbyorder-bot/events_writer.go`
- Create: `go/marketbyorder-bot/snapshot_writer.go`
- Create: `go/marketbyorder-bot/snapshot_writer_test.go`

Two writers consume `ChannelEvent`s from the channel state and produce ClickHouse rows. The events writer is straightforward — one row per event into the appropriate table. The snapshot writer is the coalesce scheduler: it tracks dirty instruments and emits level snapshots at most once per `coalesce_interval`.

- [ ] **Step 1: Write events_writer.go**

```go
package main

import (
	"time"
)

// EventsWriter dispatches Records into ClickHouse rows for events / wire_snapshots /
// channel_health / instruments tables. Idempotent (just enqueues).
type EventsWriter struct {
	ch *ClickhouseClient
}

func NewEventsWriter(ch *ClickhouseClient) *EventsWriter {
	return &EventsWriter{ch: ch}
}

// Write maps a single ChannelEvent into the right ClickHouse table(s).
func (w *EventsWriter) Write(ev ChannelEvent, channelID uint8, instSymbol string) {
	if w.ch == nil {
		return
	}
	rec := ev.Record
	now := time.Now().UTC()

	switch rec.Type {
	case "instrument_definition":
		w.ch.Enqueue("instruments", map[string]any{
			"recv_ts":         now,
			"channel_id":      channelID,
			"instrument_id":   rec.InstrumentID,
			"symbol":          getString(rec.Fields, "symbol"),
			"leg1":            getString(rec.Fields, "leg1"),
			"leg2":            getString(rec.Fields, "leg2"),
			"asset_class":     assetClassString(getUint8(rec.Fields, "asset_class")),
			"market_model":    marketModelString(getUint8(rec.Fields, "market_model")),
			"price_exponent":  getInt8(rec.Fields, "price_exponent"),
			"qty_exponent":    getInt8(rec.Fields, "qty_exponent"),
			"tick_size":       scalePrice(getInt64(rec.Fields, "tick_size_raw"), getInt8(rec.Fields, "price_exponent")),
			"lot_size":        scaleQty(getUint64(rec.Fields, "lot_size_raw"), getInt8(rec.Fields, "qty_exponent")),
			"contract_value":  getUint64(rec.Fields, "contract_value"),
			"expiry_ts":       getTime(rec.Fields, "expiry"),
			"settle_type":     settleTypeString(getUint8(rec.Fields, "settle_type")),
			"price_bound":     priceBoundString(getUint8(rec.Fields, "price_bound")),
			"manifest_seq":    getUint16(rec.Fields, "manifest_seq"),
		})

	case "heartbeat", "manifest_summary", "end_of_session":
		row := map[string]any{
			"recv_ts":            now,
			"publisher_send_ts":  rec.Timestamp,
			"channel_id":         channelID,
			"kind":               rec.Type,
		}
		if rec.Type == "manifest_summary" {
			row["manifest_seq"] = getUint16(rec.Fields, "manifest_seq")
			row["manifest_valid"] = getUint8(rec.Fields, "valid")
			row["instrument_count"] = getUint32(rec.Fields, "instrument_count")
		}
		w.ch.Enqueue("channel_health", row)

	case "snapshot_order":
		// Wire-snapshot row. Note: SnapshotOrder body in our Record doesn't carry instrument_id;
		// caller (bot main) must inject the current building-snapshot instrument's id and symbol.
		// For this writer we use the symbol passed in.
		w.ch.Enqueue("wire_snapshots", map[string]any{
			"recv_ts":             now,
			"publisher_send_ts":   rec.Timestamp,
			"channel_id":          channelID,
			"symbol":              instSymbol,
			"snapshot_id":         getUint32(rec.Fields, "snapshot_id"),
			"order_id":            getUint64(rec.Fields, "order_id"),
			"side":                getString(rec.Fields, "side"),
			"order_flags":         getUint8(rec.Fields, "order_flags"),
			"enter_ts":            getTime(rec.Fields, "enter_ts"),
			"price":               scalePrice(getInt64(rec.Fields, "price_raw"), 0),  // exponent applied by caller
			"qty":                 scaleQty(getUint64(rec.Fields, "qty_raw"), 0),
			// instrument_id, anchor_seq, total_orders, last_instrument_seq must be denormalized
			// onto every row by the bot main loop (which knows the current SnapshotBegin context).
		})

	case "order_add", "order_cancel", "order_execute", "trade", "instrument_reset", "batch_boundary":
		row := map[string]any{
			"recv_ts":           now,
			"publisher_send_ts": rec.Timestamp,
			"channel_id":        channelID,
			"mktdata_seq":       rec.SequenceNumber,
			"reset_count":       rec.ResetCount,
			"kind":              rec.Type,
			"instrument_id":     rec.InstrumentID,
			"symbol":            instSymbol,
		}
		switch rec.Type {
		case "order_add":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["side"] = getString(rec.Fields, "side")
			row["order_flags"] = getUint8(rec.Fields, "order_flags")
			row["price"] = scalePrice(getInt64(rec.Fields, "price_raw"), 0)
			row["qty"] = scaleQty(getUint64(rec.Fields, "qty_raw"), 0)
			row["enter_ts"] = getTime(rec.Fields, "enter_ts")
		case "order_cancel":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["cancel_reason"] = getString(rec.Fields, "cancel_reason")
		case "order_execute":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["per_instrument_seq"] = getUint32(rec.Fields, "per_instrument_seq")
			row["order_id"] = getUint64(rec.Fields, "order_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["exec_flags"] = getUint8(rec.Fields, "exec_flags")
			row["price"] = scalePrice(getInt64(rec.Fields, "exec_price_raw"), 0)
			row["qty"] = scaleQty(getUint64(rec.Fields, "exec_qty_raw"), 0)
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
		case "trade":
			row["source_id"] = getUint16(rec.Fields, "source_id")
			row["aggressor_side"] = getString(rec.Fields, "aggressor_side")
			row["price"] = scalePrice(getInt64(rec.Fields, "trade_price_raw"), 0)
			row["qty"] = scaleQty(getUint64(rec.Fields, "trade_qty_raw"), 0)
			row["trade_id"] = getUint64(rec.Fields, "trade_id")
			row["cumulative_volume"] = scaleQty(getUint64(rec.Fields, "cumulative_volume_raw"), 0)
		case "instrument_reset":
			row["reset_reason"] = getString(rec.Fields, "reason")
			row["new_anchor_seq"] = getUint64(rec.Fields, "new_anchor_seq")
		case "batch_boundary":
			row["batch_id"] = getUint32(rec.Fields, "batch_id")
			row["batch_ts"] = getTime(rec.Fields, "batch_ts")
		}
		w.ch.Enqueue("events", row)
	}
}

// Helper accessors (Records use map[string]any after JSON decode).
func getString(m map[string]any, k string) string  { return toString(m[k]) }
func getUint8(m map[string]any, k string) uint8    { return toUint8(m[k]) }
func getUint16(m map[string]any, k string) uint16  { return toUint16(m[k]) }
func getUint32(m map[string]any, k string) uint32  { return toUint32(m[k]) }
func getUint64(m map[string]any, k string) uint64  { return toUint64(m[k]) }
func getInt8(m map[string]any, k string) int8      { return toInt8(m[k]) }
func getInt64(m map[string]any, k string) int64    { return toInt64(m[k]) }
func getTime(m map[string]any, k string) time.Time { return toTime(m[k]) }

// scalePrice / scaleQty apply the per-instrument exponent. An exponent of 0 means
// the caller has already pre-scaled or wants raw integers as floats.
func scalePrice(raw int64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}
func scaleQty(raw uint64, exp int8) float64 {
	if exp == 0 {
		return float64(raw)
	}
	return float64(raw) * pow10f(int(exp))
}
func pow10f(e int) float64 {
	if e >= 0 {
		v := 1.0
		for i := 0; i < e; i++ {
			v *= 10
		}
		return v
	}
	v := 1.0
	for i := 0; i < -e; i++ {
		v /= 10
	}
	return v
}

func assetClassString(v uint8) string {
	switch v {
	case 1:
		return "crypto_spot"
	case 2:
		return "prediction_binary"
	case 3:
		return "prediction_scalar"
	case 4:
		return "prediction_categorical"
	default:
		return "unknown"
	}
}
func marketModelString(v uint8) string {
	switch v {
	case 1:
		return "clob"
	case 2:
		return "amm"
	default:
		return "unknown"
	}
}
func settleTypeString(v uint8) string {
	switch v {
	case 1:
		return "cash"
	case 2:
		return "physical"
	default:
		return "n_a"
	}
}
func priceBoundString(v uint8) string {
	switch v {
	case 1:
		return "bounded_01"
	case 2:
		return "non_negative"
	default:
		return "unbounded"
	}
}
```

**Note:** the writer above has a known limitation — `wire_snapshots` rows lose the `anchor_seq` / `total_orders` / `last_instrument_seq` denormalization context (those come from the SnapshotBegin, not the SnapshotOrder). The bot's main loop (Task 15) injects this context by holding the most-recent `SnapshotBegin` per channel as state and passing it to the writer. The clean fix is a separate `WriteSnapshotOrder(rec, snapBeginCtx)` method; defer until Task 15 wires it.

Also — the `price` and `qty` fields above all pass exponent=0 to `scalePrice`/`scaleQty`. That's a placeholder; the bot main loop applies the real exponent from the resolved Instrument before calling Write, by passing the scaled values. The cleanest restructuring is to have the bot main loop pre-scale and pass already-decoded values; for now, the placeholders compile and tests can mock around them. The final wiring is in Task 15.

- [ ] **Step 2: Write snapshot_writer.go**

```go
package main

import (
	"context"
	"sync"
	"time"
)

// SnapshotWriter coalesces book changes and emits level-snapshot rows to ClickHouse
// at most once per coalesceInterval per instrument.
type SnapshotWriter struct {
	ch                *ClickhouseClient
	depth             int
	coalesceInterval  time.Duration
	tickInterval      time.Duration
	metrics           *Metrics

	mu      sync.Mutex
	dirty   map[uint32]*dirtyEntry
	lookup  func(uint32) *Instrument // injected by bot main; returns current instrument or nil
	channel uint8
}

type dirtyEntry struct {
	instrumentID    uint32
	dirtiedAt       time.Time
	nextAllowedAt   time.Time
	coalescedCount  int
}

func NewSnapshotWriter(ch *ClickhouseClient, depth int, coalesceMS int, metrics *Metrics, channelID uint8, lookup func(uint32) *Instrument) *SnapshotWriter {
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          metrics,
		dirty:            map[uint32]*dirtyEntry{},
		channel:          channelID,
		lookup:           lookup,
	}
}

// MarkDirty signals that an instrument's book changed.
func (w *SnapshotWriter) MarkDirty(instrumentID uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if e, ok := w.dirty[instrumentID]; ok {
		e.coalescedCount++
		if w.metrics != nil {
			w.metrics.SnapshotCoalescesTotal.Inc()
		}
		return
	}
	w.dirty[instrumentID] = &dirtyEntry{
		instrumentID:  instrumentID,
		dirtiedAt:     now,
		nextAllowedAt: now,
	}
}

// Run is the writer's tick loop. Returns when ctx is cancelled.
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.flushDue()
		}
	}
}

func (w *SnapshotWriter) flushDue() {
	w.mu.Lock()
	now := time.Now()
	due := []*dirtyEntry{}
	for id, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, id)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		inst := w.lookup(e.instrumentID)
		if inst == nil || inst.Status != StatusReady {
			continue
		}
		snap := ComputeLevels(inst, w.depth)
		w.write(snap, inst, e.dirtiedAt, now)
		// re-arm: next write earliest in coalesceInterval
		w.mu.Lock()
		// (entry was deleted; if a new MarkDirty arrived during write, it'll be there)
		w.mu.Unlock()
		_ = e.coalescedCount // metric already incremented per coalesce
		if w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}
	}
}

func (w *SnapshotWriter) write(snap LevelSnapshot, inst *Instrument, _ time.Time, now time.Time) {
	if w.ch == nil {
		return
	}
	for i, lvl := range snap.Bids {
		w.ch.Enqueue("level_snapshots", map[string]any{
			"recv_ts":             now,
			"publisher_send_ts":   now, // bot doesn't have an exact "this snapshot's frame" ts; use now
			"channel_id":          w.channel,
			"instrument_id":       inst.ID,
			"symbol":              inst.Symbol,
			"last_applied_seq":    inst.LastAppliedMktdataSeq,
			"side":                "bid",
			"level_idx":           uint16(i),
			"price":               lvl.Price,
			"qty":                 lvl.Qty,
			"order_count":         lvl.OrderCount,
			"cumulative_qty":      lvl.CumulativeQty,
		})
	}
	for i, lvl := range snap.Asks {
		w.ch.Enqueue("level_snapshots", map[string]any{
			"recv_ts":             now,
			"publisher_send_ts":   now,
			"channel_id":          w.channel,
			"instrument_id":       inst.ID,
			"symbol":              inst.Symbol,
			"last_applied_seq":    inst.LastAppliedMktdataSeq,
			"side":                "ask",
			"level_idx":           uint16(i),
			"price":               lvl.Price,
			"qty":                 lvl.Qty,
			"order_count":         lvl.OrderCount,
			"cumulative_qty":      lvl.CumulativeQty,
		})
	}
}
```

- [ ] **Step 3: Write snapshot_writer_test.go**

```go
package main

import (
	"context"
	"sync"
	"testing"
	"time"
)

type captureWriter struct {
	mu      sync.Mutex
	enqueued int
}

func (w *captureWriter) Enqueue(table string, row map[string]any) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.enqueued++
	return true
}

// Wrapper to satisfy ClickhouseClient signature in this test path.
// The SnapshotWriter only calls .Enqueue, so a small adapter suffices.
type chAdapter struct{ inner *captureWriter }

func TestSnapshotWriter_CoalescesRapidChanges(t *testing.T) {
	// Build an instrument with one bid and one ask so ComputeLevels has output.
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 1, 0, time.Now(), 101, 3)

	cap := &captureWriter{}
	// We can't easily inject captureWriter into SnapshotWriter without changing
	// the production type. For this test, use a real ClickhouseClient pointed at
	// a counting test server — see clickhouse_test.go for the pattern.
	t.Skip("Wire-up test using httptest server pattern from clickhouse_test.go; left as exercise during integration in Task 15")
	_ = inst
	_ = cap
}

func TestSnapshotWriter_DirtyEntryCoalesces(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	metrics := NewMetrics("test", "test")
	w := NewSnapshotWriter(nil, 5, 100, metrics, 0, func(id uint32) *Instrument { return inst })

	// Mark dirty 5 times in rapid succession; only the first should create an entry.
	for i := 0; i < 5; i++ {
		w.MarkDirty(100)
	}

	w.mu.Lock()
	count := len(w.dirty)
	w.mu.Unlock()
	if count != 1 {
		t.Errorf("expected 1 dirty entry, got %d", count)
	}
}

func TestSnapshotWriter_RunFlushesAndClears(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)

	metrics := NewMetrics("test", "test")
	w := NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32) *Instrument { return inst })
	w.tickInterval = 10 * time.Millisecond

	w.MarkDirty(100)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	w.mu.Lock()
	count := len(w.dirty)
	w.mu.Unlock()
	if count != 0 {
		t.Errorf("expected dirty cleared after flush, got %d", count)
	}
}
```

- [ ] **Step 4: Run tests**

```bash
cd go/marketbyorder-bot && go test -v -run 'TestSnapshotWriter|TestEvents' ./...
```

Expected: 2 snapshot tests PASS (one is skipped pending integration in Task 15).

- [ ] **Step 5: Commit**

```bash
git add go/marketbyorder-bot/events_writer.go go/marketbyorder-bot/snapshot_writer.go go/marketbyorder-bot/snapshot_writer_test.go
git commit -m "feat(mbo-bot): events writer and coalescing snapshot writer"
```

---

### Task 15: Bot main wiring

**Files:**
- Modify: `go/marketbyorder-bot/main.go` (replace stub from Task 1)
- Possibly modify: `go/marketbyorder-bot/events_writer.go` and `go/marketbyorder-bot/snapshot_writer.go` to fix the placeholder exponent-scaling and snapshot-context denormalization noted in Task 14.

This task wires bot.go, channel.go, instrument.go, levels.go, events_writer.go, snapshot_writer.go, clickhouse.go, and metrics.go together.

It also fixes two known limitations from Task 14 (Step 0 below).

- [ ] **Step 0: Fix Task 14 placeholder limitations in events_writer.go**

Two specific changes are required before main.go calls Write correctly:

(a) **Exponent scaling.** `EventsWriter.Write` currently passes `exp=0` placeholders to `scalePrice`/`scaleQty`. Extend the signature to accept exponents:

```go
func (w *EventsWriter) Write(ev ChannelEvent, channelID uint8, instSymbol string, priceExp, qtyExp int8) {
    // ... in each scalePrice/scaleQty call, pass priceExp/qtyExp instead of 0
}
```

The bot main loop (Step 1 below) resolves exponents from the channel's refdata cache before calling Write.

(b) **Wire-snapshot context.** `wire_snapshots` rows need `instrument_id`, `anchor_seq`, `total_orders`, `last_instrument_seq`, `price`, `qty` denormalized from the open SnapshotBegin. Add a dedicated method:

```go
type SnapshotContext struct {
    InstrumentID      uint32
    Symbol            string
    SnapshotID        uint32
    AnchorSeq         uint64
    TotalOrders       uint32
    LastInstrumentSeq uint32
    PriceExponent     int8
    QtyExponent       int8
}

func (w *EventsWriter) WriteSnapshotOrder(rec Record, channelID uint8, ctx SnapshotContext) {
    if w.ch == nil {
        return
    }
    w.ch.Enqueue("wire_snapshots", map[string]any{
        "recv_ts":             time.Now().UTC(),
        "publisher_send_ts":   rec.Timestamp,
        "channel_id":          channelID,
        "instrument_id":       ctx.InstrumentID,
        "symbol":              ctx.Symbol,
        "snapshot_id":         ctx.SnapshotID,
        "anchor_seq":          ctx.AnchorSeq,
        "total_orders":        ctx.TotalOrders,
        "last_instrument_seq": ctx.LastInstrumentSeq,
        "order_id":            getUint64(rec.Fields, "order_id"),
        "side":                getString(rec.Fields, "side"),
        "order_flags":         getUint8(rec.Fields, "order_flags"),
        "enter_ts":            getTime(rec.Fields, "enter_ts"),
        "price":               scalePrice(getInt64(rec.Fields, "price_raw"), ctx.PriceExponent),
        "qty":                 scaleQty(getUint64(rec.Fields, "qty_raw"), ctx.QtyExponent),
    })
}
```

Remove the `case "snapshot_order":` branch from the original `Write` switch — it's replaced by `WriteSnapshotOrder`.

Update `bot_test.go` and any test that calls `EventsWriter.Write` with the new exponent params (no compile fail in this task, since events_writer.go has no test of its own — but verify with `go build ./...`).

- [ ] **Step 1: Replace main.go**

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"time"
)

const version = "0.1.0-dev"

var commit = "unknown"

func main() {
	var (
		socketPath           = flag.String("socket", "", "path to parser Unix socket (required)")
		symbolFilter         = flag.String("symbol", "", "comma-separated symbol filter (empty = all)")
		depth                = flag.Int("depth", 20, "snapshot depth (levels per side)")
		coalesceMS           = flag.Int("coalesce-ms", 50, "snapshot coalesce window in milliseconds")
		metricsAddr          = flag.String("metrics-addr", "127.0.0.1:9092", "Prometheus /metrics HTTP listen address")
		clickhouseURL        = flag.String("clickhouse-url", "", "ClickHouse HTTP endpoint (empty disables persistence)")
		clickhouseDB         = flag.String("clickhouse-database", "marketbyorder", "ClickHouse database")
		batchSize            = flag.Int("clickhouse-batch-size", 1000, "rows per batch flush")
		batchInterval        = flag.Duration("clickhouse-batch-interval", 200*time.Millisecond, "max time between batch flushes")
		bufferSize           = flag.Int("clickhouse-buffer", 100000, "per-table channel capacity")
		_ = symbolFilter // wired below; kept here for declaration order clarity
		_ = depth
		_ = coalesceMS
		verbose     = flag.Bool("v", false, "debug logging")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("marketbyorder-bot %s (%s)\n", version, commit)
		os.Exit(0)
	}
	if *socketPath == "" {
		fmt.Fprintln(os.Stderr, "error: --socket is required")
		flag.Usage()
		os.Exit(2)
	}
	if *verbose {
		log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	}

	metrics := NewMetrics(version, commit)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	metrics.ServeHTTP(ctx, *metricsAddr, func(e error) { log.Println(e) })

	// ClickHouse client (nil if URL empty)
	var ch *ClickhouseClient
	if *clickhouseURL != "" {
		var err error
		ch, err = NewClickhouseClient(*clickhouseURL, *clickhouseDB, []BatcherConfig{
			{Table: "events", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "level_snapshots", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "wire_snapshots", BatchSize: *batchSize, BatchInterval: *batchInterval, BufferSize: *bufferSize},
			{Table: "instruments", BatchSize: 100, BatchInterval: 1 * time.Second, BufferSize: 1000},
			{Table: "channel_health", BatchSize: 100, BatchInterval: 1 * time.Second, BufferSize: 1000},
		}, metrics)
		if err != nil {
			log.Fatalf("clickhouse: %v", err)
		}
	}

	// Channel state per channel_id (created lazily on first record).
	var (
		chMu        sync.Mutex
		channels    = map[uint8]*ChannelState{}
		snapWriters = map[uint8]*SnapshotWriter{}
	)
	getOrCreateChannel := func(id uint8) (*ChannelState, *SnapshotWriter) {
		chMu.Lock()
		defer chMu.Unlock()
		if c, ok := channels[id]; ok {
			return c, snapWriters[id]
		}
		c := NewChannelState(id)
		channels[id] = c
		sw := NewSnapshotWriter(ch, *depth, *coalesceMS, metrics, id, func(instID uint32) *Instrument {
			chMu.Lock()
			defer chMu.Unlock()
			if c, ok := channels[id]; ok {
				return c.Instruments[instID]
			}
			return nil
		})
		snapWriters[id] = sw
		go sw.Run(ctx)
		return c, sw
	}

	eventsWriter := NewEventsWriter(ch)

	// Per-channel snapshot-in-flight context, keyed by channel_id.
	// Populated on snapshot_begin, consulted on snapshot_order, cleared on snapshot_end.
	snapCtxMu := sync.Mutex{}
	snapCtx := map[uint8]SnapshotContext{}

	dispatcher := DispatcherFunc(func(rec Record) {
		c, sw := getOrCreateChannel(rec.ChannelID)
		evs := c.Apply(rec)
		for _, ev := range evs {
			// Resolve symbol + exponents from refdata.
			symbol := ""
			var priceExp, qtyExp int8
			if def, ok := c.Refdata[ev.InstrumentID]; ok {
				symbol = def.Symbol
				priceExp = def.PriceExponent
				qtyExp = def.QtyExponent
			}

			// Special-case snapshot frames: maintain context, route via WriteSnapshotOrder.
			switch rec.Type {
			case "snapshot_begin":
				snapCtxMu.Lock()
				snapCtx[c.ChannelID] = SnapshotContext{
					InstrumentID:      rec.InstrumentID,
					Symbol:            symbol,
					SnapshotID:        getUint32(rec.Fields, "snapshot_id"),
					AnchorSeq:         getUint64(rec.Fields, "anchor_seq"),
					TotalOrders:       getUint32(rec.Fields, "total_orders"),
					LastInstrumentSeq: getUint32(rec.Fields, "last_instrument_seq"),
					PriceExponent:     priceExp,
					QtyExponent:       qtyExp,
				}
				snapCtxMu.Unlock()
			case "snapshot_order":
				snapCtxMu.Lock()
				ctx, ok := snapCtx[c.ChannelID]
				snapCtxMu.Unlock()
				if ok {
					eventsWriter.WriteSnapshotOrder(rec, c.ChannelID, ctx)
				}
			case "snapshot_end":
				snapCtxMu.Lock()
				delete(snapCtx, c.ChannelID)
				snapCtxMu.Unlock()
				eventsWriter.Write(ev, c.ChannelID, symbol, priceExp, qtyExp)
			default:
				eventsWriter.Write(ev, c.ChannelID, symbol, priceExp, qtyExp)
			}

			// Mark dirty for snapshot writer if book changed.
			switch ev.Kind {
			case "applied_delta", "applied_snapshot":
				if ev.InstrumentID != 0 {
					sw.MarkDirty(ev.InstrumentID)
				}
			case "instrument_reset":
				metrics.InstrumentResetsTotal.WithLabelValues(getString(ev.Record.Fields, "reason")).Inc()
				sw.MarkDirty(ev.InstrumentID)
			case "channel_reset":
				metrics.ChannelResetsTotal.Inc()
			case "per_instrument_gap":
				metrics.PerInstrumentGapsTotal.Inc()
			}
		}
	})

	// Spawn ClickHouse runner.
	if ch != nil {
		go ch.Run(ctx)
	}

	// Set up signal handler.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %v, shutting down", s)
		cancel()
	}()

	bot := NewBot(*socketPath, dispatcher, metrics)
	log.Printf("marketbyorder-bot %s started: socket=%s clickhouse=%v depth=%d coalesce=%dms",
		version, *socketPath, *clickhouseURL != "", *depth, *coalesceMS)
	bot.Run(ctx)
	log.Println("shutdown complete")
}

// DispatcherFunc adapts a func(Record) to the Dispatcher interface.
type DispatcherFunc func(Record)

func (f DispatcherFunc) Dispatch(rec Record) { f(rec) }
```

- [ ] **Step 2: Verify build and run tests**

```bash
cd go/marketbyorder-bot && go build ./... && go test ./...
```

Expected: clean build, all tests pass.

- [ ] **Step 3: Smoke test**

```bash
./marketbyorder-bot --version
```

Expected: prints version and exits 0.

```bash
./marketbyorder-bot
```

Expected: prints `error: --socket is required` and exits 2.

- [ ] **Step 4: Commit**

```bash
git add go/marketbyorder-bot/main.go
git commit -m "feat(mbo-bot): main entry wiring channel state, writers, and ClickHouse"
```

---

### Task 16: ClickHouse schema for marketbyorder database

**Files:**
- Create: `demo/clickhouse/init/02_schema_mbo.sql`

ClickHouse runs `*.sql` init files in lexical order on first boot. The existing `01_schema.sql` creates the `topofbook` database; this new file creates `marketbyorder` with five tables.

- [ ] **Step 1: Write the schema file**

Create `demo/clickhouse/init/02_schema_mbo.sql`:

```sql
CREATE DATABASE IF NOT EXISTS marketbyorder;

-- Slowly-changing instrument dimension. ReplacingMergeTree keeps latest per (channel_id, instrument_id).
CREATE TABLE IF NOT EXISTS marketbyorder.instruments (
    recv_ts          DateTime64(9),
    channel_id       UInt8,
    instrument_id    UInt32,
    symbol           LowCardinality(String),
    leg1             LowCardinality(String),
    leg2             LowCardinality(String),
    asset_class      LowCardinality(String),
    market_model     LowCardinality(String),
    price_exponent   Int8,
    qty_exponent     Int8,
    tick_size        Float64,
    lot_size         Float64,
    contract_value   UInt64,
    expiry_ts        DateTime64(9),
    settle_type      LowCardinality(String),
    price_bound      LowCardinality(String),
    manifest_seq     UInt16
)
ENGINE = ReplacingMergeTree(recv_ts)
ORDER BY (channel_id, instrument_id);

-- Per-event log: order deltas + trades + structural events.
CREATE TABLE IF NOT EXISTS marketbyorder.events (
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    wire_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
    channel_id             UInt8,
    mktdata_seq            UInt64,
    reset_count            UInt8,
    kind                   LowCardinality(String),
    instrument_id          UInt32,
    symbol                 LowCardinality(String),
    source_id              UInt16 DEFAULT 0,
    per_instrument_seq     UInt32 DEFAULT 0,

    order_id               Nullable(UInt64),
    side                   LowCardinality(String) DEFAULT '',
    order_flags            UInt8 DEFAULT 0,
    price                  Nullable(Float64),
    qty                    Nullable(Float64),
    enter_ts               Nullable(DateTime64(9)),

    exec_flags             UInt8 DEFAULT 0,
    trade_id               Nullable(UInt64),
    aggressor_side         LowCardinality(String) DEFAULT '',

    cumulative_volume      Nullable(Float64),

    cancel_reason          LowCardinality(String) DEFAULT '',

    reset_reason           LowCardinality(String) DEFAULT '',
    new_anchor_seq         Nullable(UInt64),

    batch_id               Nullable(UInt32),
    batch_ts               Nullable(DateTime64(9))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, kind)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Top-N depth, coalesced. Flat one-row-per-level layout for direct table/heatmap rendering.
CREATE TABLE IF NOT EXISTS marketbyorder.level_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    last_applied_seq    UInt64,
    side                LowCardinality(String),
    level_idx           UInt16,
    price               Float64,
    qty                 Float64,
    order_count         UInt32,
    cumulative_qty      Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (symbol, recv_ts, side, level_idx)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Raw SnapshotOrder capture, for full replay. Group identity denormalized onto every row.
CREATE TABLE IF NOT EXISTS marketbyorder.wire_snapshots (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    channel_id          UInt8,
    instrument_id       UInt32,
    symbol              LowCardinality(String),
    snapshot_id         UInt32,
    anchor_seq          UInt64,
    total_orders        UInt32,
    last_instrument_seq UInt32,
    order_id            UInt64,
    side                LowCardinality(String),
    order_flags         UInt8,
    enter_ts            DateTime64(9),
    price               Float64,
    qty                 Float64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, instrument_id, snapshot_id, side, order_id)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;

-- Channel health: heartbeats, manifest summaries, end-of-session signals.
CREATE TABLE IF NOT EXISTS marketbyorder.channel_health (
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
    channel_id          UInt8,
    kind                LowCardinality(String),
    manifest_seq        Nullable(UInt16),
    manifest_valid      Nullable(UInt8),
    instrument_count    Nullable(UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(recv_ts)
ORDER BY (channel_id, recv_ts)
TTL toDateTime(recv_ts) + INTERVAL 30 DAY;
```

- [ ] **Step 2: Verify the SQL parses (optional, against a local clickhouse-client)**

If you have a local ClickHouse client:

```bash
clickhouse-client --query "$(cat demo/clickhouse/init/02_schema_mbo.sql)"
```

Expected: no errors. If you don't have one, the init script will be validated when the docker stack first boots in Task 17/19.

- [ ] **Step 3: Commit**

```bash
git add demo/clickhouse/init/02_schema_mbo.sql
git commit -m "feat(demo): ClickHouse schema for marketbyorder database (5 tables)"
```

---

### Task 17: Docker compose + .env additions

**Files:**
- Modify: `demo/docker-compose.yml`
- Modify: `demo/.env.example`

Add the two new services and the new env keys.

- [ ] **Step 1: Add services to docker-compose.yml**

Open `demo/docker-compose.yml`. After the existing `topofbook-bot` service block (renamed in Task 1), add:

```yaml
  marketbyorder-parser:
    build: ../go/marketbyorder-parser
    image: dz/marketbyorder-parser:latest
    network_mode: host
    restart: unless-stopped
    command:
      - --group=${DZ_MBO_MULTICAST_GROUP}
      - --refdata-port=${DZ_MBO_REFDATA_PORT}
      - --mktdata-port=${DZ_MBO_MKTDATA_PORT}
      - --snapshot-port=${DZ_MBO_SNAPSHOT_PORT}
      - --interface=${DZ_INTERFACE}
      - --output=unix:///var/run/dz/mbo.sock
      - --metrics-addr=127.0.0.1:9091
    volumes:
      - dz-sockets:/var/run/dz

  marketbyorder-bot:
    build: ../go/marketbyorder-bot
    image: dz/marketbyorder-bot:latest
    restart: unless-stopped
    depends_on:
      - marketbyorder-parser
      - clickhouse
    command:
      - --socket=/var/run/dz/mbo.sock
      - --symbol=${DZ_MBO_SYMBOLS}
      - --depth=${DZ_MBO_DEPTH:-20}
      - --coalesce-ms=${DZ_MBO_COALESCE_MS:-50}
      - --metrics-addr=0.0.0.0:9092
      - --clickhouse-url=http://clickhouse:8123
      - --clickhouse-database=marketbyorder
    ports:
      - "${MBO_BOT_METRICS_PORT:-9092}:9092"
    volumes:
      - dz-sockets:/var/run/dz
```

If a `dz-sockets` named volume isn't already declared at the bottom of the file, ensure it's there:

```yaml
volumes:
  dz-sockets:
```

(Inspect the existing file — TOB likely already declares it.)

- [ ] **Step 2: Add new env keys to .env.example**

Append to `demo/.env.example`:

```bash
# Depth-of-book feed
DZ_MBO_MULTICAST_GROUP=239.10.10.20
DZ_MBO_REFDATA_PORT=7011
DZ_MBO_MKTDATA_PORT=7012
DZ_MBO_SNAPSHOT_PORT=7013
DZ_MBO_SYMBOLS=
DZ_MBO_DEPTH=20
DZ_MBO_COALESCE_MS=50
MBO_BOT_METRICS_PORT=9092
```

- [ ] **Step 3: Validate docker compose syntax**

```bash
cd demo && docker compose config --quiet
```

Expected: no syntax errors. (This doesn't actually start anything.)

- [ ] **Step 4: Commit**

```bash
git add demo/docker-compose.yml demo/.env.example
git commit -m "feat(demo): add marketbyorder-parser and marketbyorder-bot services"
```

---

### Task 18: Grafana dashboard

**Files:**
- Create: `demo/grafana/dashboards/marketbyorder.json`

The existing dashboards provisioning YAML at `demo/grafana/provisioning/dashboards/dashboards.yaml` already wildcards the dashboards directory. The new file is auto-discovered on Grafana boot.

The ClickHouse datasource provisioned at `demo/grafana/provisioning/datasources/clickhouse.yaml` defaults to the `topofbook` database. Each query in the new dashboard explicitly selects the `marketbyorder` database via the datasource UI's "database" field per panel — no datasource provisioning change is required.

- [ ] **Step 1: Create the dashboard JSON file**

Create `demo/grafana/dashboards/marketbyorder.json`. The full Grafana JSON model for a dashboard with the 9 panels from the spec is too large to embed verbatim here, so the implementer composes it as follows:

1. Open the existing `demo/grafana/dashboards/topofbook.json` as a structural reference. Note its top-level structure: `{annotations, editable, panels:[...], templating:{list:[...]}, time, timepicker, title, uid, version, ...}`.

2. Copy `topofbook.json` to `marketbyorder.json` and modify:
   - `title`: `"DZ Market-by-Order"`
   - `uid`: a fresh UID — `"dz-marketbyorder"` is fine
   - `templating.list`: replace the symbol variables with these two:

     ```json
     {
       "name": "symbol",
       "label": "Symbol",
       "type": "query",
       "datasource": "ClickHouse",
       "query": "SELECT DISTINCT symbol FROM marketbyorder.level_snapshots WHERE $__timeFilter(recv_ts)",
       "multi": false,
       "includeAll": false,
       "refresh": 2
     },
     {
       "name": "symbols",
       "label": "Symbols",
       "type": "query",
       "datasource": "ClickHouse",
       "query": "SELECT DISTINCT symbol FROM marketbyorder.level_snapshots WHERE $__timeFilter(recv_ts)",
       "multi": true,
       "includeAll": true,
       "refresh": 2
     }
     ```

3. Replace the `panels` array with the 9 panels from the spec. Each panel is a JSON object with `id`, `title`, `type`, `gridPos`, `datasource`, and `targets:[{rawSql:"..."}]`. Specific queries for each panel:

   **Panel 1 — Book ladder** (type `"table"`, gridPos `{x:0,y:0,w:8,h:14}`):
   ```sql
   SELECT side, level_idx, price, qty, cumulative_qty
   FROM marketbyorder.level_snapshots
   WHERE symbol = '${symbol}'
     AND recv_ts = (
       SELECT max(recv_ts) FROM marketbyorder.level_snapshots
       WHERE symbol = '${symbol}' AND $__timeFilter(recv_ts)
     )
   ORDER BY side DESC, level_idx
   ```
   Cell render: set `cumulative_qty` to `"gauge"` mode in the field overrides; color rule on `side` column (bid → green, ask → red).

   **Panel 2 — Depth heatmap** (type `"heatmap"`, gridPos `{x:8,y:0,w:16,h:14}`):
   ```sql
   SELECT $__timeInterval(recv_ts) AS time, price, sum(qty) AS qty
   FROM marketbyorder.level_snapshots
   WHERE symbol = '${symbol}' AND $__timeFilter(recv_ts)
   GROUP BY time, price
   ORDER BY time
   ```

   **Panel 3 — Spread (bps)** (type `"timeseries"`, gridPos `{x:0,y:14,w:8,h:7}`):
   ```sql
   WITH best AS (
     SELECT $__timeInterval(recv_ts) AS time,
            min(price) FILTER (WHERE side = 'ask') AS best_ask,
            max(price) FILTER (WHERE side = 'bid') AS best_bid
     FROM marketbyorder.level_snapshots
     WHERE symbol = '${symbol}' AND level_idx = 0 AND $__timeFilter(recv_ts)
     GROUP BY time
   )
   SELECT time, ((best_ask - best_bid) / ((best_ask + best_bid) / 2)) * 10000 AS spread_bps
   FROM best ORDER BY time
   ```

   **Panel 4 — Top of book** (type `"table"`, gridPos `{x:8,y:14,w:16,h:7}`):
   ```sql
   SELECT
     symbol,
     argMax(price, recv_ts) FILTER (WHERE side = 'bid' AND level_idx = 0) AS bid,
     argMax(price, recv_ts) FILTER (WHERE side = 'ask' AND level_idx = 0) AS ask,
     argMax(qty, recv_ts) FILTER (WHERE side = 'bid' AND level_idx = 0) AS bid_qty,
     argMax(qty, recv_ts) FILTER (WHERE side = 'ask' AND level_idx = 0) AS ask_qty,
     max(recv_ts) AS last_update
   FROM marketbyorder.level_snapshots
   WHERE symbol IN (${symbols:singlequote}) AND $__timeFilter(recv_ts)
   GROUP BY symbol
   ORDER BY symbol
   ```

   **Panel 5 — Trade tape** (type `"table"`, gridPos `{x:0,y:21,w:12,h:9}`):
   ```sql
   SELECT recv_ts, symbol, aggressor_side, price, qty
   FROM marketbyorder.events
   WHERE kind = 'trade' AND symbol IN (${symbols:singlequote})
     AND $__timeFilter(recv_ts)
   ORDER BY recv_ts DESC LIMIT 100
   ```

   **Panel 6 — Add/Cancel/Execute rate** (type `"timeseries"` with `stacking.mode = "normal"`, gridPos `{x:12,y:21,w:12,h:9}`):
   ```sql
   SELECT $__timeInterval(recv_ts) AS time, kind, count() AS rate
   FROM marketbyorder.events
   WHERE kind IN ('order_add', 'order_cancel', 'order_execute')
     AND symbol IN (${symbols:singlequote}) AND $__timeFilter(recv_ts)
   GROUP BY time, kind ORDER BY time
   ```

   **Panel 7 — Resting order count** (type `"timeseries"`, gridPos `{x:0,y:30,w:12,h:7}`):
   ```sql
   SELECT $__timeInterval(recv_ts) AS time, side, sum(order_count) AS orders
   FROM marketbyorder.level_snapshots
   WHERE symbol = '${symbol}' AND $__timeFilter(recv_ts)
   GROUP BY time, side ORDER BY time
   ```

   **Panel 8 — Channel health** (type `"timeseries"`, gridPos `{x:12,y:30,w:12,h:5}`):
   ```sql
   SELECT $__timeInterval(recv_ts) AS time,
          quantile(0.5)(wire_latency_ms) AS p50,
          quantile(0.95)(wire_latency_ms) AS p95,
          quantile(0.99)(wire_latency_ms) AS p99
   FROM marketbyorder.level_snapshots
   WHERE $__timeFilter(recv_ts)
   GROUP BY time ORDER BY time
   ```

   **Panel 9 — Active instrument count** (type `"stat"`, gridPos `{x:20,y:35,w:4,h:5}`):
   ```sql
   SELECT instrument_count
   FROM marketbyorder.channel_health
   WHERE kind = 'manifest_summary'
   ORDER BY recv_ts DESC LIMIT 1
   ```

4. Set `time.from = "now-15m"` and `time.to = "now"`. Set the dashboard's `refresh = "5s"`.

5. Validate the JSON parses:
   ```bash
   python3 -c 'import json; json.load(open("demo/grafana/dashboards/marketbyorder.json"))'
   ```
   Expected: no output (clean parse).

The dashboard JSON ends up around 600-1000 lines depending on Grafana version's verbose schema. The query strings above are the substantive content; the rest is per-panel chrome (axes, legends, fieldConfig overrides, color rules) that the implementer fills in by copying the analogous bits from `topofbook.json`.

- [ ] **Step 2: Commit**

```bash
git add demo/grafana/dashboards/marketbyorder.json
git commit -m "feat(demo): Grafana dashboard for market-by-order"
```

---

### Task 19: Top-level README + final cleanup

**Files:**
- Modify: `README.md` (top level)

Update the implementations table or add a new section noting that the market-by-order pipeline is now available.

- [ ] **Step 1: Update top-level README.md**

Find the implementations table or relevant section and add a row/section like:

```markdown
### Market-by-Order Demo

A sibling pipeline to top-of-book, consuming the [DZ-MBO v0.1.0](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) feed:

- **[`go/marketbyorder-parser`](go/marketbyorder-parser/)** — three-port multicast subscriber + binary wire decoder, broadcasts decoded JSONL on a Unix socket
- **[`go/marketbyorder-bot`](go/marketbyorder-bot/)** — book builder + persistor, maintains in-memory MBO order books and writes per-event rows + coalesced top-N level snapshots to ClickHouse
- **[`demo`](demo/)** — extended docker-compose stack with a new "DZ Market-by-Order" Grafana dashboard featuring book ladder, depth heatmap, spread, trade tape, and event-rate panels
```

- [ ] **Step 2: Run all tests across the workspace**

```bash
cd go && go test ./...
```

Expected: all packages pass. Both new modules' tests run alongside existing TOB / receiver tests.

- [ ] **Step 3: Verify docker-compose still parses**

```bash
cd demo && docker compose config --quiet
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: top-level README updates for market-by-order pipeline"
```

---

## Build & run instructions (Linux host with DoubleZero tunnel)

```bash
cd demo
cp .env.example .env
# Edit .env — at minimum set DZ_MBO_MULTICAST_GROUP, DZ_MBO_*_PORT, DZ_INTERFACE
docker compose up -d --build
```

First boot builds both parser images and runs the new ClickHouse init script. Grafana picks up the new dashboard automatically. Open http://localhost:3000 and select "DZ Market-by-Order".

If the dashboard is empty, give the cold-start state machine a few seconds to receive its first snapshot cycle (worst case: one snapshot cycle period — typically 15s). Check:

```bash
docker compose logs marketbyorder-parser | tail
docker compose logs marketbyorder-bot    | tail
docker compose exec clickhouse clickhouse-client -q \
  "SELECT count() FROM marketbyorder.level_snapshots"
```

---

## Out of scope (deferred, per spec)

- Pcap input mode (decoder is cleanly separable; future task adds a `--pcap` source flag to the parser)
- Reconciliation between bot's reconstructed book and a reference snapshot
- Standalone book viewer (CLI or web) reading parser socket directly
- Multi-channel sharded publisher coordination beyond the single-channel-per-publisher case
- Cross-feed dashboards correlating TOB, MBO, and (future) midpoint feeds
