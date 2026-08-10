# Per-publisher sequence tracking and reset counting — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Key every per-publisher counter by the publisher that owns it — frame sequence numbers in all three parsers, and Reset Count in `marketbyorder-bot` — so loss metrics and book state stop conflating two interleaved publishers.

**Architecture:** One multicast group and port carries two redundant publishers, distinguished by source IP and by `Channel ID` (frame header byte 3). `seqTracker` moves from a single `last uint64` per port to a `map[pubKey]uint64` keyed by `(source_ip, channel_id)`, and `readDatagram` starts returning the sender address it currently discards. `marketbyorder-bot` gets the same per-channel reset treatment already applied to `marketbyprice-bot` in PR #38.

**Tech Stack:** Go 1.25.0, `net/netip`, `prometheus/client_golang`, standard `testing`.

**Spec:** `docs/superpowers/specs/2026-08-10-per-channel-seq-tracking-design.md`

## Global Constraints

- Go 1.25.0. Each parser and bot is its own module under a `go.work` workspace — run `go test` from inside the module directory.
- **Never name the upstream venue in commit messages, PR titles, or PR bodies.** Venue names are fine inside code and comments, so grep the existing source if you need to know which ones are meant. In commit and PR prose, describe the feed as "the live feed", "the publishers", or by lane (top-of-book, market-by-price, market-by-order).
- **Never quote live symbol strings** in commit messages or PR text. Numeric counts, port numbers, channel ids, and field names are all fine.
- Commit message style follows the repo: `<component>: <lowercase description>`, e.g. `marketbyprice-parser: key sequence tracking by publisher`.
- Do **not** add a `Co-Authored-By` trailer.
- Every task ends with `gofmt -l .` reporting nothing and `go vet ./...` clean.
- Tests must pass under `-race`.
- `refdata` stays excluded from gap tracking in all three parsers. Do not "fix" that exclusion.

---

## File Structure

Three parsers receive the identical change; each is a self-contained module, so each is its own task and its own commit.

| File | Responsibility | Change |
|---|---|---|
| `go/<parser>/runner.go` | receive loop, `seqTracker` | `pubKey` type, map-backed tracker, `frameHeaderChannelOffset`, `srcAddr` helper, call-site update |
| `go/<parser>/timestamp_linux.go` | kernel-timestamp read path | return sender address |
| `go/<parser>/timestamp_other.go` | portable read path | return sender address |
| `go/<parser>/metrics.go` | Prometheus definitions | add `source_ip`, `channel_id` labels |
| `go/<parser>/seqtracker_test.go` | tracker unit tests | new signature, per-publisher cases |
| `go/marketbyorder-bot/coordinator.go` | dispatch, reset barrier | `resetCount` per channel |
| `go/marketbyorder-bot/shard.go` | shard state, `shardMsg` | `resetChannel(ch)`, `ch` field on `shardMsg` |
| `go/marketbyorder-bot/coordinator_test.go` | dispatch tests | two new regression tests |

`srcAddr` lives in `runner.go` (no build tag) so both build-tagged read paths share one implementation.

**Delivery:** Tasks 1–4 are PR 1 (parsers). Task 5 is PR 2 (`marketbyorder-bot`).

---

### Task 1: marketbyprice-parser — per-publisher sequence tracking

**Files:**
- Modify: `go/marketbyprice-parser/runner.go` (tracker at lines 19–46, receive loop at 141–174)
- Modify: `go/marketbyprice-parser/timestamp_linux.go:35-45`
- Modify: `go/marketbyprice-parser/timestamp_other.go:19-25`
- Modify: `go/marketbyprice-parser/metrics.go:113-122`
- Test: `go/marketbyprice-parser/seqtracker_test.go`

**Interfaces:**
- Produces: `pubKey{src netip.Addr; ch uint8}`, `seqTracker.observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64)`, `srcAddr(addr *net.UDPAddr) netip.Addr`, `readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error)`, `frameHeaderChannelOffset = 3`. Tasks 2 and 3 reproduce these same shapes in their own modules; nothing is shared across modules.

- [ ] **Step 1: Replace the tracker test with per-publisher cases**

Replace the entire body of `go/marketbyprice-parser/seqtracker_test.go` with:

```go
package main

import (
	"net/netip"
	"testing"
)

// obs is one observation in a test case: which publisher sent it, and its seq.
type obs struct {
	src string
	ch  uint8
	seq uint64
}

func TestSeqTracker(t *testing.T) {
	const a = "10.0.0.1"
	const b = "10.0.0.2"

	tests := []struct {
		name        string
		obs         []obs
		wantGaps    uint64
		wantMissing uint64
	}{
		{
			name:        "no gaps",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 12}, {a, 1, 13}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "one gap",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 15}},
			wantGaps:    1,
			wantMissing: 3,
		},
		{
			name:        "two gaps",
			obs:         []obs{{a, 1, 1}, {a, 1, 2}, {a, 1, 5}, {a, 1, 6}, {a, 1, 10}},
			wantGaps:    2,
			wantMissing: 5,
		},
		{
			name:        "dup/reorder ignored",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 10}, {a, 1, 11}, {a, 1, 12}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "gap then dup",
			obs:         []obs{{a, 1, 1}, {a, 1, 3}, {a, 1, 2}, {a, 1, 4}},
			wantGaps:    1,
			wantMissing: 1,
		},
		{
			name:        "first frame sets baseline",
			obs:         []obs{{a, 1, 100}},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The regression this change exists for. Two publishers interleave on one
		// port with unrelated sequence spaces; a single tracker read that as a
		// storm of gaps.
		{
			name: "interleaved channels are independent",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1001}, {b, 110, 5001},
				{a, 10, 1002}, {b, 110, 5002},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The case a channel-only key cannot reach: same channel id, two sources.
		{
			name: "same channel id from two sources stays separate",
			obs: []obs{
				{a, 1, 1000}, {b, 1, 7000},
				{a, 1, 1001}, {b, 1, 7001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name: "same source on two channels stays separate",
			obs: []obs{
				{a, 1, 1000}, {a, 2, 9000},
				{a, 1, 1001}, {a, 2, 9001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// A real gap must still be caught when publishers interleave.
		{
			name: "gap in one publisher counted while the other is clean",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1004}, {b, 110, 5001},
			},
			wantGaps:    1,
			wantMissing: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker seqTracker
			var totalGaps, totalMissing uint64
			for _, o := range tc.obs {
				g, m := tracker.observe(netip.MustParseAddr(o.src), o.ch, o.seq)
				totalGaps += g
				totalMissing += m
			}
			if totalGaps != tc.wantGaps {
				t.Errorf("gaps: got %d, want %d", totalGaps, tc.wantGaps)
			}
			if totalMissing != tc.wantMissing {
				t.Errorf("missing: got %d, want %d", totalMissing, tc.wantMissing)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd go/marketbyprice-parser && go test ./... -run TestSeqTracker`
Expected: FAIL to build — `too many arguments in call to tracker.observe`.

- [ ] **Step 3: Make the tracker per-publisher**

In `go/marketbyprice-parser/runner.go`, add `"net/netip"` to the import block, then replace lines 19–46 (the `frameHeaderSeqOffset`/`frameHeaderMinLen` consts and the whole `seqTracker` type and `observe` method) with:

```go
const frameHeaderSeqOffset = 4
const frameHeaderChannelOffset = 3
const frameHeaderMinLen = 12 // need at least bytes 0..11 to read the seq field

// pubKey identifies one publisher's sequence space.
//
// A group and port carries two redundant publishers interleaved packet by
// packet, distinguished by source address and by Channel ID in the frame
// header, and each numbers its frames independently. Keyed by port alone, the
// tracker read the alternation between them as continuous loss.
//
// netip.Addr rather than a string: comparable, usable directly as a map key,
// and no allocation per datagram.
type pubKey struct {
	src netip.Addr
	ch  uint8
}

// seqTracker tracks the frame header sequence number per publisher to detect
// real UDP datagram loss (gaps in the header seq).
type seqTracker struct {
	last map[pubKey]uint64
}

// observe records seq for one publisher and returns (gaps, missing) where gaps
// is 1 if a discontinuity was detected and missing is the number of missing
// frames. Reorders/dups (seq <= last) are ignored and return (0, 0).
//
// The first frame seen from a publisher establishes its baseline and returns
// (0, 0). That is what makes a rehomed or newly-appearing publisher safe: it
// under-reports once rather than inventing a gap the size of the sequence.
func (s *seqTracker) observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64) {
	if s.last == nil {
		s.last = make(map[pubKey]uint64)
	}
	k := pubKey{src: src, ch: ch}
	last, seen := s.last[k]
	if !seen {
		s.last[k] = seq
		return 0, 0
	}
	if seq > last+1 {
		gaps = 1
		missing = seq - (last + 1)
	}
	if seq >= last {
		s.last[k] = seq
	}
	return gaps, missing
}

// srcAddr normalises a datagram's sender address so one publisher always
// produces one map key. Shared by both build-tagged readDatagram variants.
func srcAddr(addr *net.UDPAddr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	return addr.AddrPort().Addr().Unmap()
}
```

- [ ] **Step 4: Run the tracker test to verify it passes**

Run: `cd go/marketbyprice-parser && go test ./... -run TestSeqTracker -v`
Expected: PASS, all nine subtests. The package as a whole will still fail to build until Step 5 — that is expected; `-run` still compiles the package, so if you see `not enough arguments in call to readDatagram`, continue to Step 5 and re-run.

- [ ] **Step 5: Return the sender address from both read paths**

In `go/marketbyprice-parser/timestamp_linux.go`, add `"net/netip"` to the imports and replace `readDatagram` (lines 35–45) with:

```go
// readDatagram reads one datagram and returns the sender address plus the
// kernel receive timestamp when available, otherwise an application-time
// fallback.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	oob := make([]byte, unix.CmsgSpace(16))
	n, oobn, _, addr, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	src := srcAddr(addr)
	if recvTime, ok := extractKernelTimestamp(oob[:oobn]); ok {
		return n, src, recvTime.UTC(), recvTimestampKindKernelSoftware, nil
	}
	return n, src, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

In `go/marketbyprice-parser/timestamp_other.go`, add `"net/netip"` to the imports and replace `readDatagram` (lines 19–25) with:

```go
// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	return n, srcAddr(addr), time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

- [ ] **Step 6: Add the metric labels**

In `go/marketbyprice-parser/metrics.go`, replace the `FrameSeqGaps` and `FramesMissing` registrations (lines 113–122) with:

```go
	m.FrameSeqGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "frame_seq_gaps_total",
		Help: "Number of UDP frame header sequence discontinuities (real datagram loss events), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.FramesMissing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNamespace, Name: "frames_missing_total",
		Help: "Total UDP frames missing (sum of gap magnitudes in header seq), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})
```

- [ ] **Step 7: Update the receive loop**

In `go/marketbyprice-parser/runner.go`, replace the `readDatagram` call (line 153) and the gap block (lines 162–171) with:

```go
		n, src, recvTime, recvKind, err := readDatagram(conn, buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			errs <- fmt.Errorf("read %s: %w", port, err)
			return
		}

		// Refdata is a low-rate periodic-retransmit stream; frame-seq gaps there
		// are not a meaningful loss signal, so it's excluded.
		if n >= frameHeaderMinLen && port != "refdata" {
			ch := buf[frameHeaderChannelOffset]
			seq := binary.LittleEndian.Uint64(buf[frameHeaderSeqOffset : frameHeaderSeqOffset+8])
			if gaps, missing := tracker.observe(src, ch, seq); gaps > 0 {
				srcLabel, chLabel := src.String(), strconv.Itoa(int(ch))
				r.metrics.FrameSeqGaps.WithLabelValues(port, srcLabel, chLabel).Add(float64(gaps))
				r.metrics.FramesMissing.WithLabelValues(port, srcLabel, chLabel).Add(float64(missing))
			}
		}
```

- [ ] **Step 8: Run the full module test suite**

Run: `cd go/marketbyprice-parser && gofmt -l . && go vet ./... && go test -race -count=1 ./...`
Expected: `gofmt` lists no files, vet silent, all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add go/marketbyprice-parser/
git commit -m "marketbyprice-parser: key sequence tracking by publisher

Two redundant publishers interleave on one port, distinguished by source
address and Channel ID, and each numbers its frames independently. One
tracker per port read that alternation as continuous loss.

Key the tracker by (source_ip, channel_id) and label the gap and missing
counters with the same tuple, so loss is attributable to a publisher.
readDatagram now returns the sender address it previously discarded."
```

---

### Task 2: marketbyorder-parser — per-publisher sequence tracking

**Files:**
- Modify: `go/marketbyorder-parser/runner.go` (tracker at lines 17–46, receive loop at 141–174)
- Modify: `go/marketbyorder-parser/timestamp_linux.go:35-45`
- Modify: `go/marketbyorder-parser/timestamp_other.go:19-25`
- Modify: `go/marketbyorder-parser/metrics.go` (the `FrameSeqGaps` and `FramesMissing` registrations)
- Test: `go/marketbyorder-parser/seqtracker_test.go`

**Interfaces:**
- Consumes: nothing from Task 1 — this is a separate Go module with its own copy.
- Produces: the same `pubKey`, `seqTracker.observe`, `srcAddr`, and `readDatagram` shapes, private to this module.

- [ ] **Step 1: Replace the tracker test with per-publisher cases**

Replace the entire body of `go/marketbyorder-parser/seqtracker_test.go` with:

```go
package main

import (
	"net/netip"
	"testing"
)

// obs is one observation in a test case: which publisher sent it, and its seq.
type obs struct {
	src string
	ch  uint8
	seq uint64
}

func TestSeqTracker(t *testing.T) {
	const a = "10.0.0.1"
	const b = "10.0.0.2"

	tests := []struct {
		name        string
		obs         []obs
		wantGaps    uint64
		wantMissing uint64
	}{
		{
			name:        "no gaps",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 12}, {a, 1, 13}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "one gap",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 15}},
			wantGaps:    1,
			wantMissing: 3,
		},
		{
			name:        "two gaps",
			obs:         []obs{{a, 1, 1}, {a, 1, 2}, {a, 1, 5}, {a, 1, 6}, {a, 1, 10}},
			wantGaps:    2,
			wantMissing: 5,
		},
		{
			name:        "dup/reorder ignored",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 10}, {a, 1, 11}, {a, 1, 12}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "gap then dup",
			obs:         []obs{{a, 1, 1}, {a, 1, 3}, {a, 1, 2}, {a, 1, 4}},
			wantGaps:    1,
			wantMissing: 1,
		},
		{
			name:        "first frame sets baseline",
			obs:         []obs{{a, 1, 100}},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The regression this change exists for. Two publishers interleave on one
		// port with unrelated sequence spaces; a single tracker read that as a
		// storm of gaps.
		{
			name: "interleaved channels are independent",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1001}, {b, 110, 5001},
				{a, 10, 1002}, {b, 110, 5002},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The case a channel-only key cannot reach: same channel id, two sources.
		{
			name: "same channel id from two sources stays separate",
			obs: []obs{
				{a, 1, 1000}, {b, 1, 7000},
				{a, 1, 1001}, {b, 1, 7001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name: "same source on two channels stays separate",
			obs: []obs{
				{a, 1, 1000}, {a, 2, 9000},
				{a, 1, 1001}, {a, 2, 9001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// A real gap must still be caught when publishers interleave.
		{
			name: "gap in one publisher counted while the other is clean",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1004}, {b, 110, 5001},
			},
			wantGaps:    1,
			wantMissing: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker seqTracker
			var totalGaps, totalMissing uint64
			for _, o := range tc.obs {
				g, m := tracker.observe(netip.MustParseAddr(o.src), o.ch, o.seq)
				totalGaps += g
				totalMissing += m
			}
			if totalGaps != tc.wantGaps {
				t.Errorf("gaps: got %d, want %d", totalGaps, tc.wantGaps)
			}
			if totalMissing != tc.wantMissing {
				t.Errorf("missing: got %d, want %d", totalMissing, tc.wantMissing)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd go/marketbyorder-parser && go test ./... -run TestSeqTracker`
Expected: FAIL to build — `too many arguments in call to tracker.observe`.

- [ ] **Step 3: Make the tracker per-publisher**

In `go/marketbyorder-parser/runner.go`, add `"net/netip"` to the import block, then replace the `frameHeaderSeqOffset`/`frameHeaderMinLen` consts and the whole `seqTracker` type and `observe` method (lines 17–46) with:

```go
const frameHeaderSeqOffset = 4
const frameHeaderChannelOffset = 3
const frameHeaderMinLen = 12 // need at least bytes 0..11 to read the seq field

// pubKey identifies one publisher's sequence space.
//
// A group and port carries two redundant publishers interleaved packet by
// packet, distinguished by source address and by Channel ID in the frame
// header, and each numbers its frames independently. Keyed by port alone, the
// tracker read the alternation between them as continuous loss.
//
// netip.Addr rather than a string: comparable, usable directly as a map key,
// and no allocation per datagram.
type pubKey struct {
	src netip.Addr
	ch  uint8
}

// seqTracker tracks the frame header sequence number per publisher to detect
// real UDP datagram loss (gaps in the header seq).
type seqTracker struct {
	last map[pubKey]uint64
}

// observe records seq for one publisher and returns (gaps, missing) where gaps
// is 1 if a discontinuity was detected and missing is the number of missing
// frames. Reorders/dups (seq <= last) are ignored and return (0, 0).
//
// The first frame seen from a publisher establishes its baseline and returns
// (0, 0). That is what makes a rehomed or newly-appearing publisher safe: it
// under-reports once rather than inventing a gap the size of the sequence.
func (s *seqTracker) observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64) {
	if s.last == nil {
		s.last = make(map[pubKey]uint64)
	}
	k := pubKey{src: src, ch: ch}
	last, seen := s.last[k]
	if !seen {
		s.last[k] = seq
		return 0, 0
	}
	if seq > last+1 {
		gaps = 1
		missing = seq - (last + 1)
	}
	if seq >= last {
		s.last[k] = seq
	}
	return gaps, missing
}

// srcAddr normalises a datagram's sender address so one publisher always
// produces one map key. Shared by both build-tagged readDatagram variants.
func srcAddr(addr *net.UDPAddr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	return addr.AddrPort().Addr().Unmap()
}
```

- [ ] **Step 4: Run the tracker test to verify it passes**

Run: `cd go/marketbyorder-parser && go test ./... -run TestSeqTracker -v`
Expected: PASS, all nine subtests.

- [ ] **Step 5: Return the sender address from both read paths**

In `go/marketbyorder-parser/timestamp_linux.go`, add `"net/netip"` to the imports and replace `readDatagram` with:

```go
// readDatagram reads one datagram and returns the sender address plus the
// kernel receive timestamp when available, otherwise an application-time
// fallback.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	oob := make([]byte, unix.CmsgSpace(16))
	n, oobn, _, addr, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	src := srcAddr(addr)
	if recvTime, ok := extractKernelTimestamp(oob[:oobn]); ok {
		return n, src, recvTime.UTC(), recvTimestampKindKernelSoftware, nil
	}
	return n, src, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

In `go/marketbyorder-parser/timestamp_other.go`, add `"net/netip"` to the imports and replace `readDatagram` with:

```go
// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	return n, srcAddr(addr), time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

Note this module also has `timestamp_linux_test.go`, but it only exercises `extractKernelTimestamp` and needs no change.

- [ ] **Step 6: Add the metric labels**

In `go/marketbyorder-parser/metrics.go`, change the `FrameSeqGaps` and `FramesMissing` registrations from `[]string{"port"}` to `[]string{"port", "source_ip", "channel_id"}`, and update each `Help` string to end `..., by port and publisher.`

- [ ] **Step 7: Update the receive loop**

In `go/marketbyorder-parser/runner.go`, replace the `readDatagram` call (line 153) and the gap block (lines 162–171) with:

```go
		n, src, recvTime, recvKind, err := readDatagram(conn, buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			errs <- fmt.Errorf("read %s: %w", port, err)
			return
		}

		// Refdata is a low-rate periodic-retransmit stream; frame-seq gaps there
		// are not a meaningful loss signal, so it's excluded.
		if n >= frameHeaderMinLen && port != "refdata" {
			ch := buf[frameHeaderChannelOffset]
			seq := binary.LittleEndian.Uint64(buf[frameHeaderSeqOffset : frameHeaderSeqOffset+8])
			if gaps, missing := tracker.observe(src, ch, seq); gaps > 0 {
				srcLabel, chLabel := src.String(), strconv.Itoa(int(ch))
				r.metrics.FrameSeqGaps.WithLabelValues(port, srcLabel, chLabel).Add(float64(gaps))
				r.metrics.FramesMissing.WithLabelValues(port, srcLabel, chLabel).Add(float64(missing))
			}
		}
```

- [ ] **Step 8: Run the full module test suite**

Run: `cd go/marketbyorder-parser && gofmt -l . && go vet ./... && go test -race -count=1 ./...`
Expected: `gofmt` lists no files, vet silent, all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add go/marketbyorder-parser/
git commit -m "marketbyorder-parser: key sequence tracking by publisher

Same conflated-sequence-space bug as the market-by-price parser: one
tracker per port, but a port carries two independently-numbered
publishers.

Key the tracker by (source_ip, channel_id), label the counters with the
same tuple, and return the sender address from readDatagram."
```

---

### Task 3: topofbook-parser — per-publisher sequence tracking

This module differs from the other two in three ways that matter: the receive loop's port variable is named `label`, the metrics struct fields are unexported (`m.frameSeqGaps`), and `r.cfg.Metrics` is nil-checked at the call site. The code below accounts for all three.

**Files:**
- Modify: `go/topofbook-parser/runner.go` (tracker at lines 21–46, receive loop at 145–176)
- Modify: `go/topofbook-parser/timestamp_linux.go:35-45`
- Modify: `go/topofbook-parser/timestamp_other.go:19-25`
- Modify: `go/topofbook-parser/metrics.go:135-144`
- Test: `go/topofbook-parser/seqtracker_test.go`

**Interfaces:**
- Consumes: nothing from Tasks 1–2 — separate module.
- Produces: the same shapes, private to this module.

- [ ] **Step 1: Replace the tracker test with per-publisher cases**

Replace the entire body of `go/topofbook-parser/seqtracker_test.go` with:

```go
package main

import (
	"net/netip"
	"testing"
)

// obs is one observation in a test case: which publisher sent it, and its seq.
type obs struct {
	src string
	ch  uint8
	seq uint64
}

func TestSeqTracker(t *testing.T) {
	const a = "10.0.0.1"
	const b = "10.0.0.2"

	tests := []struct {
		name        string
		obs         []obs
		wantGaps    uint64
		wantMissing uint64
	}{
		{
			name:        "no gaps",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 12}, {a, 1, 13}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "one gap",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 15}},
			wantGaps:    1,
			wantMissing: 3,
		},
		{
			name:        "two gaps",
			obs:         []obs{{a, 1, 1}, {a, 1, 2}, {a, 1, 5}, {a, 1, 6}, {a, 1, 10}},
			wantGaps:    2,
			wantMissing: 5,
		},
		{
			name:        "dup/reorder ignored",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 10}, {a, 1, 11}, {a, 1, 12}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "gap then dup",
			obs:         []obs{{a, 1, 1}, {a, 1, 3}, {a, 1, 2}, {a, 1, 4}},
			wantGaps:    1,
			wantMissing: 1,
		},
		{
			name:        "first frame sets baseline",
			obs:         []obs{{a, 1, 100}},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The regression this change exists for. Two publishers interleave on one
		// port with unrelated sequence spaces; a single tracker read that as a
		// storm of gaps.
		{
			name: "interleaved channels are independent",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1001}, {b, 110, 5001},
				{a, 10, 1002}, {b, 110, 5002},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The case a channel-only key cannot reach: same channel id, two sources.
		{
			name: "same channel id from two sources stays separate",
			obs: []obs{
				{a, 1, 1000}, {b, 1, 7000},
				{a, 1, 1001}, {b, 1, 7001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name: "same source on two channels stays separate",
			obs: []obs{
				{a, 1, 1000}, {a, 2, 9000},
				{a, 1, 1001}, {a, 2, 9001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// A real gap must still be caught when publishers interleave.
		{
			name: "gap in one publisher counted while the other is clean",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1004}, {b, 110, 5001},
			},
			wantGaps:    1,
			wantMissing: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker seqTracker
			var totalGaps, totalMissing uint64
			for _, o := range tc.obs {
				g, m := tracker.observe(netip.MustParseAddr(o.src), o.ch, o.seq)
				totalGaps += g
				totalMissing += m
			}
			if totalGaps != tc.wantGaps {
				t.Errorf("gaps: got %d, want %d", totalGaps, tc.wantGaps)
			}
			if totalMissing != tc.wantMissing {
				t.Errorf("missing: got %d, want %d", totalMissing, tc.wantMissing)
			}
		})
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd go/topofbook-parser && go test ./... -run TestSeqTracker`
Expected: FAIL to build — `too many arguments in call to tracker.observe`.

- [ ] **Step 3: Make the tracker per-publisher**

In `go/topofbook-parser/runner.go`, add `"net/netip"` to the import block, then replace the const declarations and the whole `seqTracker` type and `observe` method (lines 21–46) with:

```go
const frameHeaderSeqOffset = 4
const frameHeaderChannelOffset = 3
const frameHeaderMinLen = 12 // need at least bytes 0..11 to read the seq field

// pubKey identifies one publisher's sequence space.
//
// A group and port carries two redundant publishers interleaved packet by
// packet, distinguished by source address and by Channel ID in the frame
// header, and each numbers its frames independently. Keyed by port alone, the
// tracker read the alternation between them as continuous loss.
//
// netip.Addr rather than a string: comparable, usable directly as a map key,
// and no allocation per datagram.
type pubKey struct {
	src netip.Addr
	ch  uint8
}

// seqTracker tracks the frame header sequence number per publisher to detect
// real UDP datagram loss (gaps in the header seq).
type seqTracker struct {
	last map[pubKey]uint64
}

// observe records seq for one publisher and returns (gaps, missing) where gaps
// is 1 if a discontinuity was detected and missing is the number of missing
// frames. Reorders/dups (seq <= last) are ignored and return (0, 0).
//
// The first frame seen from a publisher establishes its baseline and returns
// (0, 0). That is what makes a rehomed or newly-appearing publisher safe: it
// under-reports once rather than inventing a gap the size of the sequence.
func (s *seqTracker) observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64) {
	if s.last == nil {
		s.last = make(map[pubKey]uint64)
	}
	k := pubKey{src: src, ch: ch}
	last, seen := s.last[k]
	if !seen {
		s.last[k] = seq
		return 0, 0
	}
	if seq > last+1 {
		gaps = 1
		missing = seq - (last + 1)
	}
	if seq >= last {
		s.last[k] = seq
	}
	return gaps, missing
}

// srcAddr normalises a datagram's sender address so one publisher always
// produces one map key. Shared by both build-tagged readDatagram variants.
func srcAddr(addr *net.UDPAddr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	return addr.AddrPort().Addr().Unmap()
}
```

Keep this module's existing comment style on the seq-offset const if it differs; only the tracker type, the `observe` method, the new `frameHeaderChannelOffset` const, and `srcAddr` need to match.

- [ ] **Step 4: Run the tracker test to verify it passes**

Run: `cd go/topofbook-parser && go test ./... -run TestSeqTracker -v`
Expected: PASS, all nine subtests.

- [ ] **Step 5: Return the sender address from both read paths**

In `go/topofbook-parser/timestamp_linux.go`, add `"net/netip"` to the imports and replace `readDatagram` with:

```go
// readDatagram reads one datagram and returns the sender address plus the
// kernel receive timestamp when available, otherwise an application-time
// fallback.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	oob := make([]byte, unix.CmsgSpace(16))
	n, oobn, _, addr, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	src := srcAddr(addr)
	if recvTime, ok := extractKernelTimestamp(oob[:oobn]); ok {
		return n, src, recvTime.UTC(), recvTimestampKindKernelSoftware, nil
	}
	return n, src, time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

In `go/topofbook-parser/timestamp_other.go`, add `"net/netip"` to the imports and replace `readDatagram` with:

```go
// readDatagram falls back to application time on non-Linux platforms.
func readDatagram(conn *net.UDPConn, buf []byte) (int, netip.Addr, time.Time, string, error) {
	n, addr, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, netip.Addr{}, time.Time{}, "", err
	}
	return n, srcAddr(addr), time.Now().UTC(), recvTimestampKindAppFallback, nil
}
```

- [ ] **Step 6: Add the metric labels**

In `go/topofbook-parser/metrics.go`, replace the `frameSeqGaps` and `framesMissing` registrations (lines 135–144) with:

```go
	m.frameSeqGaps = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frame_seq_gaps_total",
		Help: "Number of UDP frame header sequence discontinuities (real datagram loss events), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})

	m.framesMissing = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dz_subscriber_frames_missing_total",
		Help: "Total UDP frames missing (sum of gap magnitudes in header seq), by port and publisher.",
	}, []string{"port", "source_ip", "channel_id"})
```

Leave the `ingressPackets` metric's existing `channel` label alone. It confusingly holds the port name, but renaming it breaks the dashboard and is out of scope.

- [ ] **Step 7: Update the receive loop**

In `go/topofbook-parser/runner.go`, change the `readDatagram` call (line 158) to capture the address, and replace the gap block (lines 167–176) with:

```go
		n, src, recvTime, recvKind, err := readDatagram(conn, buf)
```

and

```go
		// Refdata is a low-rate periodic-retransmit stream; frame-seq gaps there
		// are not a meaningful loss signal, so it's excluded.
		if n >= frameHeaderMinLen && label != "refdata" {
			ch := buf[frameHeaderChannelOffset]
			seq := binary.LittleEndian.Uint64(buf[frameHeaderSeqOffset : frameHeaderSeqOffset+8])
			if gaps, missing := tracker.observe(src, ch, seq); gaps > 0 && r.cfg.Metrics != nil {
				srcLabel, chLabel := src.String(), strconv.Itoa(int(ch))
				r.cfg.Metrics.frameSeqGaps.WithLabelValues(label, srcLabel, chLabel).Add(float64(gaps))
				r.cfg.Metrics.framesMissing.WithLabelValues(label, srcLabel, chLabel).Add(float64(missing))
			}
		}
```

- [ ] **Step 8: Run the full module test suite**

Run: `cd go/topofbook-parser && gofmt -l . && go vet ./... && go test -race -count=1 ./...`
Expected: `gofmt` lists no files, vet silent, all tests PASS.

- [ ] **Step 9: Commit**

```bash
git add go/topofbook-parser/
git commit -m "topofbook-parser: key sequence tracking by publisher

This lane's publisher pair happens to stay frame-synchronised, so the
conflated tracker reported almost no gaps -- luck, not design. The bug is
identical to the other two parsers and surfaces the moment the pair drifts.

Key the tracker by (source_ip, channel_id), label the counters with the
same tuple, and return the sender address from readDatagram."
```

---

### Task 4: Verify PR 1 against the live feed and open it

Unit tests prove the tracker splits sequence spaces. Only the live feed shows what the real loss is, which is the entire point of the change.

**Files:** none modified.

**Interfaces:**
- Consumes: the three parser binaries built by Tasks 1–3.

- [ ] **Step 1: Record the pre-change baseline**

Run:
```bash
curl -s localhost:9095/metrics | grep -E '^dz_mbp_parser_(frames_missing|frame_seq_gaps|frames_total)'
```
Write the numbers down. Expect roughly 7% of snapshot-port frames counted missing, with no `source_ip` or `channel_id` labels present.

- [ ] **Step 2: Rebuild and restart the parsers**

Run:
```bash
cd demo && docker compose up -d --build parser marketbyprice-parser marketbyorder-parser
```
Expected: three containers recreated and started.

- [ ] **Step 3: Let a measurement window accumulate**

Wait at least 120 seconds so the snapshot port — the highest-rate stream — accumulates a meaningful sample.

- [ ] **Step 4: Read the per-publisher counters**

Run:
```bash
curl -s localhost:9095/metrics | grep -E '^dz_mbp_parser_(frames_missing|frame_seq_gaps)'
curl -s localhost:9090/metrics | grep -E '^dz_subscriber_(frames_missing|frame_seq_gaps)'
```

Expected: each counter now carries `source_ip` and `channel_id`, with roughly two series per port. Compare the new snapshot-port total against the Step 1 baseline rate.

Interpretation, to be written into the PR body:
- If the total collapses toward zero, the old number was an artefact of conflated sequence spaces.
- If a large figure survives on one `source_ip`, that publisher is genuinely lossy and is now identified.
- Either outcome is a valid result. Do not tune anything to force the first.

- [ ] **Step 5: Confirm the host is not dropping datagrams**

Run: `grep -A1 "^Udp:" /proc/net/snmp | tail -1`
Expected: `RcvbufErrors` and `InErrors` both 0, confirming any surviving loss is upstream rather than local socket overflow.

- [ ] **Step 6: Push and open PR 1**

```bash
git push -u origin <branch>
```

Open the PR with a body covering: the conflated-sequence-space root cause, the `(port, source_ip, channel_id)` tuple, the source-address plumbing, and the before/after numbers from Steps 1 and 4. **Check the title and body for venue names and symbol strings before submitting** (Global Constraints).

---

### Task 5: marketbyorder-bot — Reset Count per channel (PR 2)

A direct port of the `marketbyprice-bot` fix in PR #38. Land that PR first so this follows a reviewed shape. The one structural difference: this shard also owns `snapCtx`, keyed by `snapKey{ch, snap}`, which the channel-scoped reset must clear too.

**Files:**
- Modify: `go/marketbyorder-bot/coordinator.go:19-24` (struct), `:32-42` (constructor), `:45-55` (Dispatch), `:108-138` (runResetBarrier)
- Modify: `go/marketbyorder-bot/shard.go:89-94` (`reset`), `:483-486` (msgReset case), `:507-511` (`shardMsg`)
- Test: `go/marketbyorder-bot/coordinator_test.go`

**Interfaces:**
- Consumes: nothing from Tasks 1–4 — different module, unrelated change.
- Produces: `Shard.resetChannel(ch uint8)`, `Coordinator.resetCount map[uint8]uint8`, `shardMsg.ch uint8`.

- [ ] **Step 1: Write the failing tests**

Append to `go/marketbyorder-bot/coordinator_test.go`, adding `"sync/atomic"` to its import block. Note this module's existing helper `newCoordWithCapture` returns raw inboxes, but these tests need the `*Shard` values to assert on state, so they build their own:

```go
// newCoordWithShards is newCoordWithCapture's sibling, returning the shards
// themselves so a test can assert on the state a barrier did or did not wipe.
func newCoordWithShards(n int) (*Coordinator, []*Shard) {
	metrics := stubMetrics()
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		shards[i] = NewShard(i, n, NewEventsWriter(nil), nil, metrics)
	}
	return NewCoordinator(context.Background(), shards, NewEventsWriter(nil), metrics), shards
}

// countResetBarriers dispatches recs and reports how many reset barriers fired,
// acking each one so a barrier that does fire cannot wedge the ack wait, and
// applying the wipe the barrier orders so shard state reflects it.
func countResetBarriers(t *testing.T, c *Coordinator, shards []*Shard, recs []Record) int {
	t.Helper()
	var n int64
	done := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			for i := range shards {
				select {
				case m := <-shards[i].inbox:
					if m.kind == msgReset {
						atomic.AddInt64(&n, 1)
						shards[i].resetChannel(m.ch)
						m.ack <- i
					}
				default:
				}
			}
		}
	}()
	for _, r := range recs {
		c.Dispatch(r)
	}
	close(stop)
	<-done
	return int(atomic.LoadInt64(&n))
}

// A group can carry two redundant publishers interleaved on the same ports,
// distinguished only by channel_id. Reset Count is per publisher and is stable
// while neither is resetting, so the differing values must NOT be read as a
// reset.
func TestDispatch_InterleavedChannelsWithDistinctResetCountsRunNoBarrier(t *testing.T) {
	c, shards := newCoordWithShards(2)

	var recs []Record
	for i := 0; i < 8; i++ {
		a := Record{Type: "trade", Port: "mktdata", ChannelID: 10, InstrumentID: 2,
			ResetCount: 200, Fields: map[string]any{}}
		b := Record{Type: "trade", Port: "mktdata", ChannelID: 110, InstrumentID: 2,
			ResetCount: 194, Fields: map[string]any{}}
		recs = append(recs, a, b)
	}

	if got := countResetBarriers(t, c, shards, recs); got != 0 {
		t.Errorf("interleaving two steady channels ran %d reset barriers, want 0", got)
	}
}

// A genuine Reset Count change on one channel must still run a barrier, and must
// leave the other channel's instruments, refdata and snapshot contexts intact.
func TestDispatch_ResetOnOneChannelSparesTheOther(t *testing.T) {
	c, shards := newCoordWithShards(1)
	s := shards[0]

	keep := instKey{ch: 110, id: 2}
	wipe := instKey{ch: 10, id: 2}
	s.instruments[keep] = NewInstrument(2, "KEEP", -2, -8)
	s.instruments[wipe] = NewInstrument(2, "WIPE", -2, -8)
	s.refdata[keep] = InstrumentDef{Symbol: "KEEP"}
	s.refdata[wipe] = InstrumentDef{Symbol: "WIPE"}
	s.snapCtx[snapKey{ch: 110, snap: 1}] = SnapshotContext{}
	s.snapCtx[snapKey{ch: 10, snap: 1}] = SnapshotContext{}

	steady := Record{Type: "trade", Port: "mktdata", ChannelID: 10, InstrumentID: 2,
		ResetCount: 200, Fields: map[string]any{}}
	bumped := Record{Type: "trade", Port: "mktdata", ChannelID: 10, InstrumentID: 2,
		ResetCount: 201, Fields: map[string]any{}}

	if got := countResetBarriers(t, c, shards, []Record{steady, bumped}); got != 1 {
		t.Fatalf("a real Reset Count change ran %d barriers, want 1", got)
	}
	if _, ok := s.instruments[keep]; !ok {
		t.Error("channel 110 instrument was wiped by a channel 10 reset")
	}
	if _, ok := s.refdata[keep]; !ok {
		t.Error("channel 110 refdata was wiped by a channel 10 reset")
	}
	if _, ok := s.snapCtx[snapKey{ch: 110, snap: 1}]; !ok {
		t.Error("channel 110 snapshot context was wiped by a channel 10 reset")
	}
	if _, ok := s.instruments[wipe]; ok {
		t.Error("channel 10 instrument survived its own channel's reset")
	}
	if _, ok := s.snapCtx[snapKey{ch: 10, snap: 1}]; ok {
		t.Error("channel 10 snapshot context survived its own channel's reset")
	}
}
```

These signatures were checked against the module: `NewShard(idx, n int, eventsW *EventsWriter, sw *SnapshotWriter, metrics *Metrics)`, `NewInstrument(id uint32, symbol string, priceExp, qtyExp int8)`, `stubMetrics()` in `bot_test.go`, and `SnapshotContext` in `events_writer.go`. The shard fields are `instruments`, `refdata`, `deltaBuf` (all `map[instKey]...`) and `snapCtx map[snapKey]SnapshotContext`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd go/marketbyorder-bot && go test ./... -run 'TestDispatch_InterleavedChannels|TestDispatch_ResetOnOneChannel'`
Expected: FAIL to build — `shards[i].resetChannel undefined` and `m.ch undefined`.

- [ ] **Step 3: Add the channel to the shard message**

In `go/marketbyorder-bot/shard.go`, replace the `shardMsg` struct (lines 507–511) with:

```go
type shardMsg struct {
	rec  *Record
	kind shardMsgKind
	ch   uint8 // channel to wipe, for msgReset
	ack  chan int
}
```

- [ ] **Step 4: Make the shard reset channel-scoped**

In `go/marketbyorder-bot/shard.go`, replace `reset` (lines 89–94) with:

```go
// resetChannel discards every instrument owned by one channel.
//
// Scoped to a channel, not the whole shard, because a group can carry two
// redundant publishers interleaved on the same ports under different
// channel_ids. Reset Count is per publisher, so a reset on one says nothing
// about the other, and wiping both would throw away books that never reset.
func (s *Shard) resetChannel(ch uint8) {
	for k := range s.instruments {
		if k.ch == ch {
			delete(s.instruments, k)
		}
	}
	for k := range s.refdata {
		if k.ch == ch {
			delete(s.refdata, k)
		}
	}
	for k := range s.deltaBuf {
		if k.ch == ch {
			delete(s.deltaBuf, k)
		}
	}
	for k := range s.snapCtx {
		if k.ch == ch {
			delete(s.snapCtx, k)
		}
	}
}
```

Then update the `msgReset` case (line 485) from `s.reset()` to `s.resetChannel(msg.ch)`.

- [ ] **Step 5: Key the coordinator's Reset Count by channel**

In `go/marketbyorder-bot/coordinator.go`, replace the `resetSeen`/`resetCount` fields (lines 19–20) with:

```go
	// Reset Count is per publisher, and a group can carry two redundant
	// publishers interleaved on the same ports under different channel_ids.
	// Held as one global value, their differing-but-steady counts read as a
	// reset on every alternation between them, wiping instrument state
	// faster than it could be relearned.
	resetCount map[uint8]uint8 // per channel_id
```

Add `resetCount: map[uint8]uint8{},` to the `NewCoordinator` literal, and update the struct doc comment on line 9 to say `resetCount/snapshotRoute/seqLast/manifest`.

Replace the reset check in `Dispatch` (lines 47–54) with:

```go
	if prev, seen := c.resetCount[rec.ChannelID]; seen && rec.ResetCount != prev {
		c.runResetBarrier(rec)
		return
	} else if !seen {
		c.resetCount[rec.ChannelID] = rec.ResetCount
	}
```

- [ ] **Step 6: Scope the barrier to the resetting channel**

In `go/marketbyorder-bot/coordinator.go`, in `runResetBarrier`, capture the channel and pass it on the message:

```go
func (c *Coordinator) runResetBarrier(held Record) {
	ch := held.ChannelID
	acks := make(chan int, c.n)
	for _, s := range c.shards {
		go func(s *Shard) {
			select {
			case s.inbox <- shardMsg{kind: msgReset, ch: ch, ack: acks}:
			case <-c.ctx.Done():
			}
		}(s)
	}
```

Then replace the post-barrier state wipe (lines 129–132) with:

```go
	for k := range c.snapshotRoute {
		if k.ch == ch {
			delete(c.snapshotRoute, k)
		}
	}
	c.seqLast = map[string]uint64{}
	c.manifest = ManifestState{}
	c.resetCount[ch] = held.ResetCount
```

and update the trailing comment to read:

```go
	// Route the held record as the first new-era frame, via the full classifier.
	// resetCount[ch] now equals held.ResetCount, so this re-entry into Dispatch
	// falls through to normal classification.
```

- [ ] **Step 7: Run the new tests to verify they pass**

Run: `cd go/marketbyorder-bot && go test ./... -run 'TestDispatch_InterleavedChannels|TestDispatch_ResetOnOneChannel' -v`
Expected: both PASS.

- [ ] **Step 8: Run the full module test suite**

Run: `cd go/marketbyorder-bot && gofmt -l . && go vet ./... && go test -race -count=1 ./...`
Expected: `gofmt` lists no files, vet silent, all tests PASS.

If a pre-existing test asserts on `c.resetCount` as a scalar, update it to index by channel: `c.resetCount[held.ChannelID]`.

- [ ] **Step 9: Commit and open PR 2**

```bash
git add go/marketbyorder-bot/
git commit -m "marketbyorder-bot: track Reset Count per channel

Port of the market-by-price bot fix. Reset Count is per publisher, and a
group can carry two publishers interleaved on the same ports, so two
steady-but-different values must not be read as a reset.

Key resetCount by channel_id and scope the wipe to the resetting channel.
This shard also owns snapCtx, keyed by (channel, snapshot_id), so
resetChannel clears that too."
```

Open PR 2 against `main`. **Check the title and body for venue names and symbol strings before submitting.**

---

## Notes for the implementer

- `Coordinator.seqLast` in `marketbyorder-bot` is written on every record and never read. It is dead bookkeeping, not a correctness bug, and is deliberately left alone. Do not extend this plan to "fix" it.
- The known limitation from the spec stands and is out of scope: if a publisher restarts and its sequence drops low, that publisher's `last` stays high and loss is under-reported until the sequence climbs back.
- Tasks 1–3 are independent of each other and of Task 5. Only Task 4 depends on 1–3.
