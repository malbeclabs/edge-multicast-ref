# Cross-Feed Latency Normalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make TOB and MBO feeds report two consistently-defined latencies — `source_latency` (block/venue → kernel NIC) and `send_latency` (publisher egress → kernel NIC) — and add sequence-gap dashboard panels for both feeds.

**Architecture:** Both parsers emit four top-level JSON timestamp fields (`source_ts_ns`, `send_ts_ns`, `parser_kernel_recv_ts_ns`, `recv_ts_kind`). MBO gains the kernel `SO_TIMESTAMPNS` capture TOB already has. Both bots read those fields, use the kernel recv time as `recv_ts`, and write `publisher_send_ts` (egress) + nullable `source_ts` (block). ClickHouse materializes `send_latency_ms` and `source_latency_ms`. Grafana panels are reworked to show both latencies plus seq-gaps.

**Tech Stack:** Go (`encoding/binary`, `golang.org/x/sys/unix`, Prometheus client), ClickHouse SQL, Grafana JSON dashboards, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-06-06-cross-feed-latency-normalization-design.md`

**Conventions:**
- Build/test a Go module: `cd go/<module> && go build ./... && go test ./...`
- Go workspace: `go/go.work` ties the modules together.
- No `testify`; standard `testing` only. Synthetic wire bytes built by test helpers.
- Commit after each task. Do NOT add a `Co-Authored-By` trailer (repo preference).
- The `recv_ts_kind` values are the existing TOB constants: `kernel_udp_software`, `app_udp_fallback`.

---

### Task 1: MBO parser — capture kernel NIC receive timestamp

Port TOB's `SO_TIMESTAMPNS` path to the MBO parser. TOB's reference implementation is `go/topofbook-parser/timestamp_linux.go`.

**Files:**
- Create: `go/marketbyorder-parser/timestamp_linux.go`
- Create: `go/marketbyorder-parser/timestamp_other.go`
- Create: `go/marketbyorder-parser/timestamp_linux_test.go`

- [ ] **Step 1: Write the failing test**

Create `go/marketbyorder-parser/timestamp_linux_test.go`:

```go
//go:build linux

package main

import (
	"encoding/binary"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestExtractKernelTimestamp_ParsesScmTimestampns(t *testing.T) {
	want := time.Unix(1717689600, 123456789).UTC()
	data := make([]byte, 16)
	binary.LittleEndian.PutUint64(data[0:8], uint64(want.Unix()))
	binary.LittleEndian.PutUint64(data[8:16], uint64(want.Nanosecond()))

	oob := buildCmsg(unix.SOL_SOCKET, unix.SCM_TIMESTAMPNS, data)

	got, ok := extractKernelTimestamp(oob)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestExtractKernelTimestamp_EmptyReturnsFalse(t *testing.T) {
	if _, ok := extractKernelTimestamp(nil); ok {
		t.Fatal("expected ok=false for empty oob")
	}
}

// buildCmsg constructs a single socket control message for testing.
func buildCmsg(level, typ int, data []byte) []byte {
	buf := make([]byte, unix.CmsgSpace(len(data)))
	h := (*unix.Cmsghdr)(unsafePointer(&buf[0]))
	h.Level = int32(level)
	h.Type = int32(typ)
	h.SetLen(unix.CmsgLen(len(data)))
	copy(buf[unix.CmsgLen(0):], data)
	return buf
}
```

Add the `unsafePointer` helper in the test file:

```go
import "unsafe"

func unsafePointer(p *byte) unsafe.Pointer { return unsafe.Pointer(p) }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/marketbyorder-parser && go test -run TestExtractKernelTimestamp ./...`
Expected: FAIL — `undefined: extractKernelTimestamp`.

- [ ] **Step 3: Create `timestamp_linux.go`**

Copy TOB's implementation verbatim (it is package-`main` and self-contained). Create `go/marketbyorder-parser/timestamp_linux.go`:

```go
//go:build linux

package main

import (
	"encoding/binary"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

const (
	recvTimestampKindKernelSoftware = "kernel_udp_software"
	recvTimestampKindAppFallback    = "app_udp_fallback"
)

func enableTimestamping(conn *net.UDPConn) error {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	var setsockoptErr error
	err = rawConn.Control(func(fd uintptr) {
		setsockoptErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1)
	})
	if err != nil {
		return err
	}
	return setsockoptErr
}

// readDatagram reads one datagram and returns the kernel receive timestamp when
// available, otherwise an application-time fallback.
func readDatagram(conn *net.UDPConn, buf []byte) (int, time.Time, string, error) {
	oob := make([]byte, unix.CmsgSpace(16))
	n, oobn, _, _, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, time.Time{}, "", err
	}
	if recvTime, ok := extractKernelTimestamp(oob[:oobn]); ok {
		return n, recvTime.UTC(), recvTimestampKindKernelSoftware, nil
	}
	return n, time.Now().UTC(), recvTimestampKindAppFallback, nil
}

func extractKernelTimestamp(oob []byte) (time.Time, bool) {
	if len(oob) == 0 {
		return time.Time{}, false
	}
	cmsgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return time.Time{}, false
	}
	for _, cmsg := range cmsgs {
		if cmsg.Header.Level != unix.SOL_SOCKET || cmsg.Header.Type != unix.SCM_TIMESTAMPNS {
			continue
		}
		if len(cmsg.Data) < 16 {
			continue
		}
		sec := int64(binary.LittleEndian.Uint64(cmsg.Data[0:8]))
		nsec := int64(binary.LittleEndian.Uint64(cmsg.Data[8:16]))
		return time.Unix(sec, nsec).UTC(), true
	}
	return time.Time{}, false
}
```

- [ ] **Step 4: Create `timestamp_other.go` (non-Linux fallback)**

```go
//go:build !linux

package main

import (
	"net"
	"time"
)

const (
	recvTimestampKindKernelSoftware = "kernel_udp_software"
	recvTimestampKindAppFallback    = "app_udp_fallback"
)

// enableTimestamping is a no-op on non-Linux platforms.
func enableTimestamping(conn *net.UDPConn) error { return nil }

// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, time.Time, string, error) {
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, time.Time{}, "", err
	}
	return n, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd go/marketbyorder-parser && go test -run TestExtractKernelTimestamp ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go/marketbyorder-parser/timestamp_linux.go go/marketbyorder-parser/timestamp_other.go go/marketbyorder-parser/timestamp_linux_test.go
git commit -m "marketbyorder-parser: capture kernel NIC receive timestamp"
```

---

### Task 2: MBO parser — emit four timestamp fields on every Record

Add `SourceTSNS`, `SendTSNS`, `RecvTSNS`, `RecvTSKind` to the parser Record and populate them. `send_ts_ns` = frame header send time; `source_ts_ns` = per-type block/venue time; recv fields come from the runner (Task 3 wires the runner).

**Files:**
- Modify: `go/marketbyorder-parser/parser.go:10-19` (Record struct)
- Modify: `go/marketbyorder-parser/marketbyorder.go:52-59` (base) and per-type source assignment
- Test: `go/marketbyorder-parser/marketbyorder_test.go` (existing file; add a test)

- [ ] **Step 1: Write the failing test**

Find an existing helper in `marketbyorder_test.go` that builds an `order_add` frame and parses it (search for `OrderAdd` / `ParseFrame`). Add:

```go
func TestParseFrame_OrderAddEmitsSourceAndSendTS(t *testing.T) {
	// Reuse the existing order_add frame builder used by other tests in this file.
	// enterNS is the block (source) time; sendNS is the frame header send time.
	frame, enterNS, sendNS := buildOrderAddFrameWithTS(t)

	p := &marketByOrderParser{}
	recs, err := p.ParseFrame("mktdata", frame)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d records, want 1", len(recs))
	}
	r := recs[0]
	if r.SourceTSNS != enterNS {
		t.Errorf("SourceTSNS = %d, want %d (enter_ts)", r.SourceTSNS, enterNS)
	}
	if r.SendTSNS != sendNS {
		t.Errorf("SendTSNS = %d, want %d (frame send)", r.SendTSNS, sendNS)
	}
}
```

If no reusable `buildOrderAddFrameWithTS` helper exists, write one in the test file that constructs a minimal valid MBO frame with one `order_add` message, returning the frame plus the `enter_timestamp` ns and frame `send_timestamp` ns it encoded. Model it on the existing frame-construction helpers already in `marketbyorder_test.go`.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/marketbyorder-parser && go test -run TestParseFrame_OrderAddEmitsSourceAndSendTS ./...`
Expected: FAIL — `r.SourceTSNS undefined`.

- [ ] **Step 3: Add fields to the Record struct**

In `go/marketbyorder-parser/parser.go`, extend `Record`:

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

- [ ] **Step 4: Populate `SendTSNS` on the base record and `SourceTSNS` per type**

In `go/marketbyorder-parser/marketbyorder.go`, set `SendTSNS` in the `base` (it applies to every message):

```go
base := Record{
	Timestamp:      hdr.SendTimestamp,
	SendTSNS:       uint64(hdr.SendTimestamp.UnixNano()),
	ChannelID:      hdr.ChannelID,
	Port:           port,
	SequenceNumber: hdr.Sequence,
	ResetCount:     hdr.ResetCount,
}
```

Then set `base.SourceTSNS` in the per-type cases that carry a block/venue time, immediately before each `return base, true, nil`. Use this helper (add near the bottom of `marketbyorder.go`):

```go
// tsNS returns Unix-nanos for a non-zero time, else 0 (absent).
func tsNS(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}
```

Add `"time"` to the `marketbyorder.go` import block. Set source per type:

- `order_add` case: `base.SourceTSNS = tsNS(b.EnterTimestamp)`
- `order_cancel` case: `base.SourceTSNS = tsNS(b.Timestamp)`
- `order_execute` case: `base.SourceTSNS = tsNS(b.Timestamp)`
- `trade` case: `base.SourceTSNS = tsNS(b.SourceTimestamp)`
- `batch_boundary` case: `base.SourceTSNS = tsNS(b.BatchTime)`
- `instrument_reset` case: `base.SourceTSNS = tsNS(b.Timestamp)`
- `snapshot_order` case: `base.SourceTSNS = tsNS(b.EnterTimestamp)`

Leave `heartbeat`, `manifest_summary`, `end_of_session`, `instrument_definition`, `snapshot_begin`, `snapshot_end` with `SourceTSNS = 0` (no per-event venue time).

- [ ] **Step 5: Run test to verify it passes**

Run: `cd go/marketbyorder-parser && go test ./...`
Expected: PASS (the new test and all existing tests).

- [ ] **Step 6: Commit**

```bash
git add go/marketbyorder-parser/parser.go go/marketbyorder-parser/marketbyorder.go go/marketbyorder-parser/marketbyorder_test.go
git commit -m "marketbyorder-parser: emit source_ts_ns/send_ts_ns on records"
```

---

### Task 3: MBO parser runner — wire kernel recv time into records and observe both latencies

Switch the MBO receive loop to `readDatagram` (kernel recv) and stamp `RecvTSNS`/`RecvTSKind` onto every record. Replace the single `WireLatency` observation with `SourceLatency` + `SendLatency` (the metrics themselves are added in Task 4; this task references them and will not compile until Task 4 — so do Task 4 first if executing strictly, or combine. To keep TDD ordering, **do Task 4 before Task 3's metric lines**).

**Files:**
- Modify: `go/marketbyorder-parser/runner.go:105-145` (receive loop)

- [ ] **Step 1: Enable timestamping when opening the socket**

In `openMulticast` (`runner.go:93-103`), after `SetReadBuffer`, add:

```go
	if err := enableTimestamping(conn); err != nil {
		log.Printf("warning: enableTimestamping: %v", err)
	}
```

- [ ] **Step 2: Use `readDatagram` and stamp recv fields**

Replace the read + per-record loop in `receive` (`runner.go:115-139`) with:

```go
		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, recvTime, recvKind, err := readDatagram(conn, buf)
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

		recvNS := uint64(recvTime.UnixNano())
		for i := range records {
			records[i].RecvTSNS = recvNS
			records[i].RecvTSKind = recvKind
			r.metrics.RecordsTotal.WithLabelValues(records[i].Type).Inc()
			observeLatencies(r.metrics, port, recvTime, records[i])
		}

		if err := r.sink.Write(records); err != nil {
			r.metrics.SinkWriteErrors.Inc()
		}
```

- [ ] **Step 3: Add the `observeLatencies` helper at the bottom of `runner.go`**

```go
// observeLatencies records send→recv and (when present) source→recv latency.
// Negatives are clamped to 0 for the histogram; raw signed values still reach
// ClickHouse via the bot.
func observeLatencies(m *Metrics, port string, recvTime time.Time, rec Record) {
	if rec.SendTSNS != 0 {
		lat := recvTime.Sub(time.Unix(0, int64(rec.SendTSNS))).Seconds()
		if lat < 0 {
			lat = 0
		}
		m.SendLatency.WithLabelValues(port).Observe(lat)
	}
	if rec.SourceTSNS != 0 {
		lat := recvTime.Sub(time.Unix(0, int64(rec.SourceTSNS))).Seconds()
		if lat < 0 {
			lat = 0
		}
		m.SourceLatency.WithLabelValues(port).Observe(lat)
	}
}
```

- [ ] **Step 4: Build to verify it compiles (requires Task 4's metrics)**

Run: `cd go/marketbyorder-parser && go build ./...`
Expected: PASS once Task 4 is done. If `SendLatency`/`SourceLatency` are undefined, complete Task 4 first.

- [ ] **Step 5: Run tests**

Run: `cd go/marketbyorder-parser && go test ./...`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go/marketbyorder-parser/runner.go
git commit -m "marketbyorder-parser: stamp kernel recv ts and observe both latencies"
```

---

### Task 4: Parser Prometheus metrics — replace `wire_latency` with `source_latency` + `send_latency` (both parsers)

**Files:**
- Modify: `go/marketbyorder-parser/metrics.go:23,61-65` (struct field + registration)
- Modify: `go/topofbook-parser/metrics.go:48,113-117` and `go/topofbook-parser/runner.go` (observe site)

- [ ] **Step 1: MBO metrics struct + registration**

In `go/marketbyorder-parser/metrics.go`, replace the `WireLatency` field (line 23) with two:

```go
	SourceLatency *prometheus.HistogramVec
	SendLatency   *prometheus.HistogramVec
```

Replace the `WireLatency` constructor block (lines 61-65) with:

```go
	m.SourceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "source_latency_seconds",
		Help:    "Latency from block/venue source timestamp to kernel receive, by port (crosses validator and local clocks)",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"port"})

	m.SendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: metricsNamespace, Name: "send_latency_seconds",
		Help:    "Latency from publisher egress send timestamp to kernel receive, by port",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 16),
	}, []string{"port"})
```

In the `reg.MustRegister(...)` call in this file, replace `m.WireLatency` with `m.SourceLatency, m.SendLatency`.

- [ ] **Step 2: Build MBO parser**

Run: `cd go/marketbyorder-parser && go build ./...`
Expected: PASS (Task 3's `observeLatencies` now resolves).

- [ ] **Step 3: TOB metrics struct + registration**

In `go/topofbook-parser/metrics.go`, replace the `wireLatency` field (line 48) with:

```go
	sourceLatency *prometheus.HistogramVec
	sendLatency   *prometheus.HistogramVec
```

Replace the `m.wireLatency = ...` block (lines 113-117) with:

```go
	m.sourceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_subscriber_source_latency_seconds",
		Help:    "Latency from block/venue source timestamp to kernel receive, by record type (crosses validator and local clocks).",
		Buckets: latencyBuckets,
	}, []string{"type"})

	m.sendLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "dz_subscriber_send_latency_seconds",
		Help:    "Latency from publisher egress send timestamp to kernel receive, by record type.",
		Buckets: latencyBuckets,
	}, []string{"type"})
```

In the `reg.MustRegister(...)` block in this file, replace `m.wireLatency` with `m.sourceLatency, m.sendLatency`.

- [ ] **Step 4: TOB runner observe site**

Open `go/topofbook-parser/runner.go` and find the existing observe site (search `wireLatency`, ~line 165). It currently does `lat := <recvTime>.Sub(rec.Timestamp)`. The TOB runner already has the kernel `recvTime` from `readDatagram`. Replace the single observe with both, reading the explicit ns fields that Task 5-precursor (Task 3 analog for TOB, done in Task 7) will set — but TOB records already carry `RecvTimestampNS` and (after Task 7) `SourceTSNS`/`SendTSNS`. For now observe using the record's source/send:

```go
			recvT := time.Unix(0, int64(rec.RecvTimestampNS))
			if rec.SendTSNS != 0 {
				if lat := recvT.Sub(time.Unix(0, int64(rec.SendTSNS))).Seconds(); lat >= 0 {
					r.cfg.Metrics.sendLatency.WithLabelValues(rec.Type).Observe(lat)
				}
			}
			if rec.SourceTSNS != 0 {
				if lat := recvT.Sub(time.Unix(0, int64(rec.SourceTSNS))).Seconds(); lat >= 0 {
					r.cfg.Metrics.sourceLatency.WithLabelValues(rec.Type).Observe(lat)
				}
			}
```

Note: `rec.SourceTSNS`/`rec.SendTSNS` are added to the TOB `tob.Record` in Task 7. **Execute Task 7 before building TOB.** Ensure `"time"` is imported in `runner.go` (it already is).

- [ ] **Step 5: Build (after Task 7) and test both parsers**

Run: `cd go/marketbyorder-parser && go test ./... && cd ../topofbook-parser && go build ./... && go test ./...`
Expected: PASS once Task 7 lands the TOB record fields.

- [ ] **Step 6: Commit**

```bash
git add go/marketbyorder-parser/metrics.go go/topofbook-parser/metrics.go go/topofbook-parser/runner.go
git commit -m "parsers: split wire_latency into source_latency and send_latency metrics"
```

---

### Task 7: TOB parser — promote `source_ts_ns`/`send_ts_ns` to top-level Record fields

(Numbered 7 to match the build-order note above; execute it before Task 4 Step 4/5.)

**Files:**
- Modify: `go/topofbook-parser/tob/parser.go:6-22` (Record)
- Modify: `go/topofbook-parser/tob/topofbook.go` handleQuote (~233) and handleTrade (~294)
- Test: `go/topofbook-parser/tob/topofbook_test.go`

- [ ] **Step 1: Write the failing test**

In `go/topofbook-parser/tob/topofbook_test.go`, find the existing quote-decode test (search `handleQuote` or a `ParseFrame`-style helper that yields a quote Record). Add:

```go
func TestQuoteEmitsTopLevelSourceAndSendTS(t *testing.T) {
	// Reuse the existing helper that builds a frame with one instrument def
	// followed by one quote, returning the decoded quote Record plus the
	// source and send ns it encoded.
	rec, sourceNS, sendNS := decodeOneQuoteWithTS(t)

	if rec.SourceTSNS != sourceNS {
		t.Errorf("SourceTSNS = %d, want %d", rec.SourceTSNS, sourceNS)
	}
	if rec.SendTSNS != sendNS {
		t.Errorf("SendTSNS = %d, want %d", rec.SendTSNS, sendNS)
	}
}
```

If no `decodeOneQuoteWithTS` helper exists, adapt the existing quote test's frame builder into one that also returns the encoded source/send ns.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/topofbook-parser && go test ./tob/ -run TestQuoteEmitsTopLevelSourceAndSendTS`
Expected: FAIL — `rec.SourceTSNS undefined`.

- [ ] **Step 3: Add fields to `tob.Record`**

In `go/topofbook-parser/tob/parser.go`, add after `Timestamp` (line 8):

```go
	SourceTSNS      uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS        uint64         `json:"send_ts_ns,omitempty"`
```

- [ ] **Step 4: Set them in handleQuote and handleTrade**

In `go/topofbook-parser/tob/topofbook.go` handleQuote, add to the Record literal (alongside `Timestamp`):

```go
			SourceTSNS:     body.SourceTimestamp,
			SendTSNS:       sendTS,
```

In handleTrade, add the same two lines to its Record literal:

```go
			SourceTSNS:     body.SourceTimestamp,
			SendTSNS:       sendTS,
```

(`sendTS` is already a parameter of both handlers; `body.SourceTimestamp` is the per-message venue ns.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd go/topofbook-parser && go test ./tob/`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add go/topofbook-parser/tob/parser.go go/topofbook-parser/tob/topofbook.go go/topofbook-parser/tob/topofbook_test.go
git commit -m "topofbook-parser: promote source_ts_ns/send_ts_ns to top-level record fields"
```

---

### Task 5: TOB bot — read new fields, use kernel recv as recv_ts, write source/send columns

**Files:**
- Modify: `go/topofbook-bot/record.go:7-15` (Record + helpers)
- Modify: `go/topofbook-bot/bot.go:~165` (recvTime source)
- Modify: `go/topofbook-bot/clickhouse.go:89-124` (EnqueueQuote/EnqueueTrade rows)
- Test: `go/topofbook-bot/clickhouse_test.go`

- [ ] **Step 1: Write the failing test**

In `go/topofbook-bot/clickhouse_test.go`, add a test asserting the row carries the new columns. Model setup on the existing `clickhouse_test.go` patterns (search `EnqueueQuote`):

```go
func TestEnqueueQuote_WritesSourceSendRecvColumns(t *testing.T) {
	source := time.Unix(1717689600, 0).UTC()
	send := source.Add(150 * time.Millisecond)
	recv := source.Add(230 * time.Millisecond)

	rec := &Record{
		Type:       "quote",
		SourceTSNS: uint64(source.UnixNano()),
		SendTSNS:   uint64(send.UnixNano()),
		RecvTSNS:   uint64(recv.UnixNano()),
		RecvTSKind: "kernel_udp_software",
		Symbol:     "TEST",
	}

	row := buildQuoteRow(rec) // see Step 3

	if got := row["publisher_send_ts"]; got != chTime(send) {
		t.Errorf("publisher_send_ts = %v, want %v", got, chTime(send))
	}
	if got := row["source_ts"]; got != chTime(source) {
		t.Errorf("source_ts = %v, want %v", got, chTime(source))
	}
	if got := row["recv_ts"]; got != chTime(recv) {
		t.Errorf("recv_ts = %v, want %v", got, chTime(recv))
	}
	if got := row["recv_ts_kind"]; got != "kernel_udp_software" {
		t.Errorf("recv_ts_kind = %v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/topofbook-bot && go test -run TestEnqueueQuote_WritesSourceSendRecvColumns ./...`
Expected: FAIL — `rec.SourceTSNS undefined` / `buildQuoteRow undefined`.

- [ ] **Step 3: Extend Record + add timestamp helpers**

In `go/topofbook-bot/record.go`, extend `Record` and add helpers:

```go
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	SourceTSNS     uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS       uint64         `json:"send_ts_ns,omitempty"`
	RecvTSNS       uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind     string         `json:"recv_ts_kind,omitempty"`
	ChannelID      uint8          `json:"channel_id"`
	SequenceNumber uint64         `json:"seq"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Symbol         string         `json:"symbol,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

// recvTime is the kernel NIC receive time; falls back to fallback when absent.
func (r *Record) recvTime(fallback time.Time) time.Time {
	if r.RecvTSNS != 0 {
		return time.Unix(0, int64(r.RecvTSNS)).UTC()
	}
	return fallback
}

func (r *Record) sourceTime() (time.Time, bool) {
	if r.SourceTSNS == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(r.SourceTSNS)).UTC(), true
}

func (r *Record) sendTime() time.Time {
	return time.Unix(0, int64(r.SendTSNS)).UTC()
}
```

- [ ] **Step 4: Refactor row construction into testable builders**

In `go/topofbook-bot/clickhouse.go`, extract the row maps into `buildQuoteRow`/`buildTradeRow` and add the new columns. Replace `EnqueueQuote`:

```go
func buildQuoteRow(rec *Record) map[string]any {
	row := map[string]any{
		"recv_ts":           chTime(rec.recvTime(time.Now().UTC())),
		"publisher_send_ts": chTime(rec.sendTime()),
		"recv_ts_kind":      rec.RecvTSKind,
		"channel_id":        rec.ChannelID,
		"seq":               rec.SequenceNumber,
		"instrument_id":     rec.InstrumentID,
		"symbol":            rec.Symbol,
		"bid_price":         floatOrZero(rec, "bid_price"),
		"bid_qty":           floatOrZero(rec, "bid_qty"),
		"ask_price":         floatOrZero(rec, "ask_price"),
		"ask_qty":           floatOrZero(rec, "ask_qty"),
		"source_id":         uintOrZero(rec, "source_id"),
	}
	if src, ok := rec.sourceTime(); ok {
		row["source_ts"] = chTime(src)
	}
	return row
}

func (w *chWriter) EnqueueQuote(rec *Record, recvTime time.Time) {
	w.submit("quotes", buildQuoteRow(rec))
}
```

Do the analogous extraction for `buildTradeRow` / `EnqueueTrade`, adding `publisher_send_ts: chTime(rec.sendTime())`, `recv_ts: chTime(rec.recvTime(time.Now().UTC()))`, `recv_ts_kind`, and conditional `source_ts`. The `recvTime` parameter is now unused inside the bodies (kept for signature stability); if the linter complains, drop the parameter and update callers in `bot.go`.

- [ ] **Step 5: Update `bot.go` callers if signature changed**

If you dropped the `recvTime` param, update `go/topofbook-bot/bot.go` (~lines 175-185) calls `b.chw.EnqueueQuote(rec, recvTime)` → `b.chw.EnqueueQuote(rec)`. The `recvTime := time.Now()` line at ~165 can stay (still used for the Prometheus latency metric in the bot) or be removed if now unused.

- [ ] **Step 6: Run test to verify it passes**

Run: `cd go/topofbook-bot && go test ./...`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add go/topofbook-bot/record.go go/topofbook-bot/clickhouse.go go/topofbook-bot/bot.go go/topofbook-bot/clickhouse_test.go
git commit -m "topofbook-bot: write source_ts/send/recv columns using kernel recv time"
```

---

### Task 6: MBO bot — read new fields, use kernel recv as recv_ts, write source/send columns

**Files:**
- Modify: `go/marketbyorder-bot/record.go:5-14` (Record + helpers)
- Modify: `go/marketbyorder-bot/events_writer.go:23,47-70` (and the other table rows)
- Test: `go/marketbyorder-bot/events_writer_test.go` (create if absent, else add to existing writer test file)

- [ ] **Step 1: Write the failing test**

Create or extend a writer test asserting the events row carries the new columns. Find how existing tests construct a `ChannelEvent`/`Record` (search `EventsWriter` usage in `*_test.go`). Add:

```go
func TestEventsRow_HasSourceSendRecvColumns(t *testing.T) {
	source := time.Unix(1717689600, 0).UTC()
	send := source.Add(150 * time.Millisecond)
	recv := source.Add(230 * time.Millisecond)

	rec := Record{
		Type:       "order_add",
		SourceTSNS: uint64(source.UnixNano()),
		SendTSNS:   uint64(send.UnixNano()),
		RecvTSNS:   uint64(recv.UnixNano()),
		RecvTSKind: "kernel_udp_software",
	}

	row := buildEventRow(rec, 1, "TEST") // see Step 3

	if row["publisher_send_ts"] != chTime(send) {
		t.Errorf("publisher_send_ts = %v, want %v", row["publisher_send_ts"], chTime(send))
	}
	if row["source_ts"] != chTime(source) {
		t.Errorf("source_ts = %v, want %v", row["source_ts"], chTime(source))
	}
	if row["recv_ts"] != chTime(recv) {
		t.Errorf("recv_ts = %v, want %v", row["recv_ts"], chTime(recv))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd go/marketbyorder-bot && go test -run TestEventsRow_HasSourceSendRecvColumns ./...`
Expected: FAIL — undefined fields / `buildEventRow`.

- [ ] **Step 3: Extend Record + helpers, add the row builder**

In `go/marketbyorder-bot/record.go`:

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

func (r Record) recvTime(fallback time.Time) time.Time {
	if r.RecvTSNS != 0 {
		return time.Unix(0, int64(r.RecvTSNS)).UTC()
	}
	return fallback
}

func (r Record) sourceTime() (time.Time, bool) {
	if r.SourceTSNS == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(r.SourceTSNS)).UTC(), true
}

func (r Record) sendTime() time.Time {
	return time.Unix(0, int64(r.SendTSNS)).UTC()
}
```

In `go/marketbyorder-bot/events_writer.go`, add a `buildEventRow` helper and use it in the market-data case (lines 61+). Replace the row literal in the `"order_add", ...` case with:

```go
	case "order_add", "order_cancel", "order_execute", "trade", "instrument_reset", "batch_boundary":
		row := buildEventRow(rec, channelID, instSymbol)
		// ... keep existing per-type field additions below, appending to row ...
		w.ch.Enqueue("events", row)
```

Add the builder (it owns the timestamp columns; per-type fields are still appended by the existing code after the call):

```go
func buildEventRow(rec Record, channelID uint8, instSymbol string) map[string]any {
	row := map[string]any{
		"recv_ts":           chTime(rec.recvTime(time.Now().UTC())),
		"publisher_send_ts": chTime(rec.sendTime()),
		"recv_ts_kind":      rec.RecvTSKind,
		"channel_id":        channelID,
		"mktdata_seq":       rec.SequenceNumber,
		"reset_count":       rec.ResetCount,
		"kind":              rec.Type,
		"instrument_id":     rec.InstrumentID,
		"symbol":            instSymbol,
	}
	if src, ok := rec.sourceTime(); ok {
		row["source_ts"] = chTime(src)
	}
	return row
}
```

Apply the same `recv_ts`/`publisher_send_ts`/`recv_ts_kind` change to the `channel_health` case (lines 47-59) — use `rec.recvTime(...)` and `rec.sendTime()` there too. Leave `level_snapshots` (in `snapshot_writer.go`) untouched.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd go/marketbyorder-bot && go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go/marketbyorder-bot/record.go go/marketbyorder-bot/events_writer.go go/marketbyorder-bot/events_writer_test.go
git commit -m "marketbyorder-bot: write source_ts/send/recv columns using kernel recv time"
```

---

### Task 8: ClickHouse schema — new timestamp columns + materialized latencies

**Files:**
- Modify: `demo/clickhouse/init/01_schema.sql` (topofbook.quotes, topofbook.trades)
- Modify: `demo/clickhouse/init/02_schema_mbo.sql` (marketbyorder.events, marketbyorder.channel_health)

- [ ] **Step 1: TOB quotes + trades**

In `demo/clickhouse/init/01_schema.sql`, for both `quotes` and `trades`, replace the `wire_latency_ms` materialized column with the new columns. The block currently reads:

```sql
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    wire_latency_ms     Float64 MATERIALIZED
        (toFloat64(recv_ts) - toFloat64(publisher_send_ts)) * 1000,
```

Replace with:

```sql
    recv_ts             DateTime64(9),
    publisher_send_ts   DateTime64(9),
    source_ts           Nullable(DateTime64(9)),
    recv_ts_kind        LowCardinality(String) DEFAULT '',
    send_latency_ms     Float64 MATERIALIZED
        (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms   Nullable(Float64) MATERIALIZED
        if(source_ts IS NULL, NULL,
           (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
```

- [ ] **Step 2: MBO events + channel_health**

In `demo/clickhouse/init/02_schema_mbo.sql`, for `events` and `channel_health`, the block currently reads:

```sql
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    wire_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1000000.0,
```

Replace (both tables) with:

```sql
    recv_ts                DateTime64(9),
    publisher_send_ts      DateTime64(9),
    source_ts              Nullable(DateTime64(9)),
    recv_ts_kind           LowCardinality(String) DEFAULT '',
    send_latency_ms        Float64 MATERIALIZED (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(publisher_send_ts)) / 1e6,
    source_latency_ms      Nullable(Float64) MATERIALIZED if(source_ts IS NULL, NULL, (toUnixTimestamp64Nano(recv_ts) - toUnixTimestamp64Nano(assumeNotNull(source_ts))) / 1e6),
```

Leave `marketbyorder.events`' existing `enter_ts` / `batch_ts` columns as-is. Leave `level_snapshots` and `wire_snapshots` unchanged (no `source_latency`).

- [ ] **Step 3: Validate SQL syntax against a throwaway ClickHouse**

Run (uses the already-running clickhouse container):

```bash
docker compose -f demo/docker-compose.yml exec -T clickhouse \
  clickhouse-client --multiquery < demo/clickhouse/init/02_schema_mbo.sql
```

Expected: no error (CREATE ... IF NOT EXISTS is idempotent; for a clean test the fresh-boot in Task 11 is authoritative). If it errors on the materialized expression, fix the SQL.

- [ ] **Step 4: Commit**

```bash
git add demo/clickhouse/init/01_schema.sql demo/clickhouse/init/02_schema_mbo.sql
git commit -m "clickhouse: add source_ts/recv_ts_kind + source_latency/send_latency columns"
```

---

### Task 9: Grafana — rework latency panels (both dashboards)

**Files:**
- Modify: `demo/grafana/dashboards/topofbook.json`
- Modify: `demo/grafana/dashboards/marketbyorder.json`

- [ ] **Step 1: TOB — replace the three latency panels' SQL**

In `demo/grafana/dashboards/topofbook.json`, locate the panels titled "Average wire latency", "P99 wire latency", and "Wire latency quantiles". Retitle and repoint each to the two new metrics. Replace:
- "Average wire latency" → keep title "Avg send→recv latency"; SQL:
```sql
SELECT $__timeInterval(recv_ts) AS time, avg(greatest(send_latency_ms,0)) AS avg_send_latency_ms
FROM topofbook.quotes WHERE $__timeFilter(recv_ts) GROUP BY time ORDER BY time
```
- "P99 wire latency" → add a sibling panel "Avg source→recv latency"; SQL:
```sql
SELECT $__timeInterval(recv_ts) AS time, avg(greatest(source_latency_ms,0)) AS avg_source_latency_ms
FROM topofbook.quotes WHERE source_ts IS NOT NULL AND $__timeFilter(recv_ts) GROUP BY time ORDER BY time
```
- "Wire latency quantiles" → "Latency quantiles (send vs source)"; SQL:
```sql
SELECT $__timeInterval(recv_ts) AS time,
  quantile(0.5)(greatest(send_latency_ms,0))   AS send_p50,
  quantile(0.99)(greatest(send_latency_ms,0))  AS send_p99,
  quantile(0.5)(greatest(source_latency_ms,0))  AS source_p50,
  quantile(0.99)(greatest(source_latency_ms,0)) AS source_p99
FROM topofbook.quotes
WHERE symbol IN (${symbols:singlequote}) AND $__timeFilter(recv_ts)
GROUP BY time ORDER BY time
```

- [ ] **Step 2: MBO — same rework**

In `demo/grafana/dashboards/marketbyorder.json`, apply the same three SQL replacements against `marketbyorder.events` (no symbol filter exists there today — add `WHERE source_ts IS NOT NULL` to the source-based queries; keep the existing global scope otherwise).

- [ ] **Step 3: Validate JSON**

Run:
```bash
python3 -c "import json; json.load(open('demo/grafana/dashboards/topofbook.json')); json.load(open('demo/grafana/dashboards/marketbyorder.json')); print('valid')"
```
Expected: `valid`.

- [ ] **Step 4: Commit**

```bash
git add demo/grafana/dashboards/topofbook.json demo/grafana/dashboards/marketbyorder.json
git commit -m "grafana: show send→recv and source→recv latency on both dashboards"
```

---

### Task 10: Grafana — sequence-gap panels (both dashboards)

**Files:**
- Modify: `demo/grafana/dashboards/marketbyorder.json`
- Modify: `demo/grafana/dashboards/topofbook.json`

- [ ] **Step 1: MBO seq-gap panel**

Add a `timeseries` panel titled "Sequence gaps (per-instrument)" to `marketbyorder.json`. Copy the JSON shape of an existing timeseries panel in the same file (datasource, gridPos, fieldConfig) and set this `rawSql` (ClickHouse window function over the dense `per_instrument_seq`):

```sql
SELECT time, sum(missing) AS missing_messages FROM (
  SELECT $__timeInterval(recv_ts) AS time,
         per_instrument_seq
           - lagInFrame(per_instrument_seq) OVER (
               PARTITION BY channel_id, instrument_id ORDER BY per_instrument_seq) - 1 AS missing
  FROM marketbyorder.events
  WHERE per_instrument_seq > 0 AND $__timeFilter(recv_ts)
)
WHERE missing > 0
GROUP BY time ORDER BY time
```

- [ ] **Step 2: TOB seq-gap panel**

Add a "Sequence gaps (per-channel)" timeseries panel to `topofbook.json` using the per-channel header `seq`:

```sql
SELECT time, sum(missing) AS missing_messages FROM (
  SELECT $__timeInterval(recv_ts) AS time,
         seq - lagInFrame(seq) OVER (PARTITION BY channel_id ORDER BY seq) - 1 AS missing
  FROM topofbook.quotes
  WHERE $__timeFilter(recv_ts)
)
WHERE missing > 0 AND missing < 1000000
GROUP BY time ORDER BY time
```

(The `missing < 1000000` guard drops the partition-boundary artifact where the window restarts.)

- [ ] **Step 3: Validate JSON**

Run:
```bash
python3 -c "import json; json.load(open('demo/grafana/dashboards/topofbook.json')); json.load(open('demo/grafana/dashboards/marketbyorder.json')); print('valid')"
```
Expected: `valid`.

- [ ] **Step 4: Commit**

```bash
git add demo/grafana/dashboards/marketbyorder.json demo/grafana/dashboards/topofbook.json
git commit -m "grafana: add sequence-gap panels for both feeds"
```

---

### Task 11: Full workspace build, rebuild stack, E2E verification

**Files:** none (verification only)

- [ ] **Step 1: Whole-workspace build + test**

Run:
```bash
cd go && go build ./... && go test ./... 2>&1 | tail -30
```
Expected: all modules build; tests PASS (eBPF modules may be skipped — that's fine).

- [ ] **Step 2: Validate compose config**

Run: `cd demo && docker compose --env-file .env config >/dev/null && echo OK`
Expected: `OK`.

- [ ] **Step 3: Rebuild and restart the stack with a fresh ClickHouse volume**

The schema changed, so the persisted volume must be wiped for init to re-run:
```bash
cd demo
docker compose down --remove-orphans
docker volume rm dz-tob-demo_clickhouse-data
docker compose up -d --build
sleep 25
docker compose ps --format '{{.Name}}\t{{.Status}}'
```
Expected: all six containers `Up`, clickhouse `(healthy)`.

- [ ] **Step 4: Verify columns exist and are populated**

```bash
cd demo
docker compose exec -T clickhouse clickhouse-client -q "DESCRIBE marketbyorder.events" | grep -E 'source_ts|send_latency|source_latency|recv_ts_kind'
docker compose exec -T clickhouse clickhouse-client -q "
SELECT 'MBO' f, round(avg(send_latency_ms),1) send_ms, round(avgIf(source_latency_ms, source_ts IS NOT NULL),1) source_ms,
       countIf(recv_ts_kind='kernel_udp_software')*100.0/count() pct_kernel
FROM marketbyorder.events WHERE recv_ts > now()-INTERVAL 2 MINUTE
UNION ALL
SELECT 'TOB', round(avg(send_latency_ms),1), round(avgIf(source_latency_ms, source_ts IS NOT NULL),1),
       countIf(recv_ts_kind='kernel_udp_software')*100.0/count()
FROM topofbook.quotes WHERE recv_ts > now()-INTERVAL 2 MINUTE"
```
Expected: both feeds show a `send_ms` (tens of ms) and a larger `source_ms`; `pct_kernel` ≈ 100. Crucially, **`source_ms` should now be comparable across the two feeds** for the same symbol (the original 240-vs-82 artifact is gone).

- [ ] **Step 5: Verify the seq-gap query returns rows without error**

```bash
cd demo
docker compose exec -T clickhouse clickhouse-client -q "
SELECT count() FROM (
  SELECT per_instrument_seq - lagInFrame(per_instrument_seq)
    OVER (PARTITION BY channel_id, instrument_id ORDER BY per_instrument_seq) - 1 AS missing
  FROM marketbyorder.events WHERE per_instrument_seq > 0 AND recv_ts > now()-INTERVAL 5 MINUTE
) WHERE missing > 0"
```
Expected: a number (0+), no SQL error.

- [ ] **Step 6: Confirm dashboards load**

Run: `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3000/api/health`
Expected: `200`. Spot-check the reworked panels render in Grafana (manual).

- [ ] **Step 7: Final commit (if any verification fixes were needed)**

```bash
git add -A && git commit -m "fix: verification adjustments for latency normalization" || echo "nothing to commit"
```

---

## Self-Review notes

- **Build ordering:** Tasks reference symbols across modules. Strict order: 1 → 2 → 7 → 4 → 3 → 5 → 6 → 8 → 9 → 10 → 11. (Task 7 lands TOB record fields before Task 4 Step 4 observes them; Task 4 lands MBO metrics before Task 3 observes them.) A subagent runner should respect this dependency, not the numeric order.
- **Spec coverage:** three timestamp fields + recv kind (T2,T7), MBO kernel capture (T1,T3), both latencies in CH (T8), Prometheus split (T4), dashboards + seq-gap (T9,T10), edge cases — null source (T2 leaves 0; T8 `if(... NULL ...)`), fallback (T1 `timestamp_other.go`), level_snapshots excluded (T6 Step 3). All covered.
- **Tests:** kernel extraction (T1), parser field emission (T2,T7), bot row mapping (T5,T6), live schema/latency/gap (T11).
