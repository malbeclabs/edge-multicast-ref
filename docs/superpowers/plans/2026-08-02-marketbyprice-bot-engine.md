# Market-by-Price bot: book engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the book-building engine of `go/marketbyprice-bot` — read JSONL records from the `marketbyprice-parser` Unix socket, maintain a price-keyed L2 order book per instrument, and implement the feed's snapshot/delta recovery state machine. **No persistence in this plan**; ClickHouse writers and the schema are a follow-on.

**Architecture:** Socket reader (`bot.go`) → single-goroutine `Coordinator` owning channel-scoped state and routing → N `Shard`s, each owning a disjoint set of instruments by `instrument_id % N` and all their book state → `Instrument`, a price-keyed book plus state-machine position. Mirrors `go/marketbyorder-bot`'s sharded structure, which was chosen for throughput and already handles reset barriers and FIFO fences.

**Tech Stack:** Go 1.25.0, package `main` under `go/marketbyprice-bot`, `github.com/prometheus/client_golang v1.23.2`, standard `testing`.

**Spec:** `docs/superpowers/specs/2026-08-02-marketbyprice-design.md` — Component 2 is authoritative, especially "Five behaviors the market-by-order bot does not have".

**Feed spec:** https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md

**Upstream:** consumes the output of `go/marketbyprice-parser` (PR #29). Read its `README.md` for the record envelope and field names.

**Working dir for all commands:** `go/marketbyprice-bot` unless a step says otherwise.

## Global Constraints

- Module path `github.com/malbeclabs/edge-multicast-ref/go/marketbyprice-bot`, Go directive `go 1.25.0`.
- Package `main`, no subpackages.
- Prometheus namespace exactly `dz_mbp_bot`.
- **Run `go mod tidy` in the module as part of Task 1** and commit `go.mod` *and* `go.sum`. Do not hand-write `go.mod` and stop there: without a `go.sum` the module resolves only through the workspace's union build list, is not verifiable, and fails a `GOWORK=off` build. This bit the parser module and had to be repaired later.
- Add the module to `go/go.work` **and** add `COPY go/marketbyprice-bot/go.mod go/marketbyprice-bot/go.sum ./go/marketbyprice-bot/` to every other module's Dockerfile. Adding a `go.work` member breaks every sibling Dockerfile until they copy the new files.
- **Add `- go/marketbyprice-bot` to the `matrix.module` list in `.github/workflows/go-tests.yml`.** A module absent from that matrix has its tests silently never run.
- Prices and quantities are held **raw** (`int64` / `uint64`) throughout the engine and scaled by `PriceExponent` / `QtyExponent` only at read-out. Never store floats in book state.
- Every task ends gofmt-clean: `gofmt -l ./marketbyprice-bot/` from `go/` must print nothing.
- Encoding negative values in tests: assign through a typed variable, never a constant conversion. `uint64(int64(-1500))` is a compile-time overflow error in every Go version.
- Commit messages: `component: short description`, lowercase, imperative, no trailing period, no `Co-Authored-By` line, no "Generated with" footer.
- Write "DoubleZero" in prose, never "DZ". Binary names and env vars keep their `dz-` / `DZ_` prefixes.

## Verification commands

`./...` does **not** work from `go/` in this workspace — it fails with `pattern ./...: directory prefix . does not contain modules listed in go.work`. This is pre-existing. Use, from `go/`:

- Type-check: `go vet ./marketbyprice-bot/...`
- Test: `go test ./marketbyprice-bot/...`
- Race: `go test -race ./marketbyprice-bot/...`

`go build` on this module fails with `function main is undeclared in the main package` until Task 6 adds `main.go`; that is expected in Tasks 1–5, and `go vet` is the build gate until then. From Task 6, build with `go build -o /tmp/dz-marketbyprice-bot ./marketbyprice-bot/` — a bare `go build ./marketbyprice-bot/` fails because the output name collides with the directory.

Baseline: every module except `xdp-receiver` vets and tests clean. `xdp-receiver` does not build (missing generated BPF object `xdpfilter_bpfel.o`) — pre-existing, out of scope, leave it alone.

## Provenance of the code in this plan, and a known deviation

The `instrument.go` and `instrument_test.go` code in Task 2 was written, compiled, and executed in a scratch module before this plan was committed: `go vet` clean, `gofmt` clean, and all 12 tests passing. Transcribe it as given. If it fails to compile, you have introduced a transcription error — re-read the plan rather than redesigning.

**Tasks 3–6 specify their tests by scenario and assertion rather than as verbatim code, which is a deliberate deviation from this repo's usual plan style.** The reason: those tests exercise types that the same task defines, so they cannot be compiled ahead of the task, and the preceding parser plan established that unexecuted Go in a plan document is the single largest source of defects — five of that plan's six defects were code I wrote into Markdown and never ran. Writing thirty more unverified test bodies would add risk, not remove it.

The mitigation is to compile-verify each task's code in a scratch module immediately before that task is dispatched, and expand the brief with the verified bodies at that point. Every scenario below names its exact setup and expected outcome, so the requirement is unambiguous even where the code is not yet written. An implementer who finds a scenario ambiguous should ask rather than guess.

## File map

All paths relative to `go/marketbyprice-bot/`.

- `go.mod`, `go.sum`, `.gitignore` — Task 1.
- `record.go` — the parser's JSON envelope plus timestamp helpers. Task 1.
- `metrics.go` — `Metrics`, `NewMetrics`, `ServeHTTP`, namespace `dz_mbp_bot`. Task 1.
- `bot.go` — Unix socket reader with reconnect backoff, `Dispatcher` interface. Task 1.
- `instrument.go` — `Instrument`, `LevelState`, `PendingSnapshot`, apply rules, snapshot lifecycle, crossed-book test. Task 2.
- `instrument_test.go` — Task 2.
- `shard.go` — `Shard`, inbox goroutine, per-record dispatch, sequencing, bounded delta buffer, consistency-point crossed-book evaluation. Tasks 3–4.
- `shard_test.go` — Tasks 3–4.
- `coordinator.go` — channel-scoped state, record routing, open-snapshot-group routing, reset barrier, FIFO fence. Task 5.
- `coordinator_test.go` — Task 5.
- `levels.go` — top-N read-out with exponent scaling and cumulative quantity. Task 6.
- `levels_test.go` — Task 6.
- `main.go`, `README.md` — Task 6.
- `../go.work`, four sibling Dockerfiles, `.github/workflows/go-tests.yml` — Task 1.

---

## Task 1: Module scaffolding, record envelope, metrics, socket reader

**Files:**
- Create: `go/marketbyprice-bot/go.mod`, `.gitignore`, `record.go`, `metrics.go`, `bot.go`
- Modify: `go/go.work`, `.github/workflows/go-tests.yml`, and the Dockerfiles of `marketbyorder-parser`, `marketbyorder-bot`, `topofbook-parser`, `topofbook-bot`, `marketbyprice-parser`
- Test: `go/marketbyprice-bot/bot_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `Record` with `recvTime(fallback time.Time) time.Time`, `sourceTime() (time.Time, bool)`, `sendTime() time.Time`; `Dispatcher` interface with `Dispatch(rec Record)`; `Bot` with `NewBot(socketPath string, dispatcher Dispatcher, metrics *Metrics) *Bot` and `Run(ctx context.Context)`; `Metrics` with `NewMetrics(version, commit string) *Metrics` and `ServeHTTP(ctx, addr, logErr)`.

- [ ] **Step 1: Create the module and register it everywhere**

Create `go.mod` with the module path and Go directive from Global Constraints and `require github.com/prometheus/client_golang v1.23.2`. Create `.gitignore` containing `marketbyprice-bot` and `dz-marketbyprice-bot`.

Add `./marketbyprice-bot` to the `use` block in `go/go.work`, directly after `./marketbyprice-parser`. The existing list is not sorted — do not reorder it.

Add `- go/marketbyprice-bot` to `matrix.module` in `.github/workflows/go-tests.yml`, next to the other bot entries.

Add this line to the dependency-copy block of all five sibling Dockerfiles (`marketbyprice-parser`, `marketbyorder-parser`, `marketbyorder-bot`, `topofbook-parser`, `topofbook-bot`):

```dockerfile
COPY go/marketbyprice-bot/go.mod go/marketbyprice-bot/go.sum ./go/marketbyprice-bot/
```

- [ ] **Step 2: Copy `record.go` and `bot.go` from the sibling bot**

Copy `go/marketbyorder-bot/record.go` and `go/marketbyorder-bot/bot.go` verbatim. Both are feed-independent: `record.go` is the parser's JSON envelope, and `bot.go` is the socket reader with exponential-backoff reconnect, a 1 MiB scanner buffer, and per-record `socket_to_bot_latency` observation.

One thing to check rather than assume: the parser emits `snapshot_level` records with **no** `instrument_id`. Confirm `record.go` leaves `InstrumentID` at 0 for those rather than rejecting the line.

- [ ] **Step 3: Write `metrics.go`**

Copy `go/marketbyorder-bot/metrics.go`, then:

1. Set `const metricsNamespace = "dz_mbp_bot"`.
2. Drop the order-keyed metrics that have no meaning on this feed: `SnapshotOrderDroppedTotal`, `BookOrders`.
3. Add the metrics this feed's engine needs:

```go
	// Book state
	BookLevels    *prometheus.GaugeVec // labels: symbol, side
	BookTopPrice  *prometheus.GaugeVec // labels: symbol, side
	BookTopQty    *prometheus.GaugeVec // labels: symbol, side
	BookSpreadBps *prometheus.GaugeVec // labels: symbol

	// Feed-specific defect and health counters
	CrossedBookEventsTotal   prometheus.Counter
	CrossedInstruments       prometheus.Gauge
	BookDivergenceTotal      *prometheus.CounterVec // label: kind
	DeltaBufferOverflowTotal prometheus.Counter
	DeltaBufferedRecords     prometheus.Gauge
	SnapshotDiscardedTotal   *prometheus.CounterVec // label: reason
	SnapshotLevelDroppedTotal prometheus.Counter
	DepthBoundedInstruments  prometheus.Gauge
	PerInstrumentGapsTotal   prometheus.Counter
	InstrumentResetsTotal    *prometheus.CounterVec // label: reason
	ChannelResetsTotal       prometheus.Counter
	InstrumentsTotal         *prometheus.GaugeVec // label: status
```

Keep `BuildInfo`, `UptimeSeconds`, `SocketConnected`, `SocketReconnects`, `RecordsTotal`, `DecodeErrors`, `SocketToBotLatency`. Register every metric in the `reg.MustRegister(...)` call — a constructed but unregistered collector compiles, looks right, and silently reports nothing forever.

`CrossedBookEventsTotal` is deliberately unlabeled: per-symbol labels blow up Prometheus cardinality on a high-symbol venue, and `.env.example` already warns about that. Per-symbol detail belongs in the persisted rows, not the metric.

- [ ] **Step 4: Write the namespace guard test**

Create `bot_test.go`:

```go
package main

import (
	"strings"
	"testing"
)

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

func TestMetricsNamespace(t *testing.T) {
	m := NewMetrics("test", "abc123")
	// build_info is set inside NewMetrics and uptime_seconds is a GaugeFunc, so
	// both are gathered without any observation. Every *Vec reports no metric
	// family until a label set is observed, which is why they are not probed here.
	names := gatheredNames(t, m)
	var sawBuildInfo bool
	for _, n := range names {
		if n == "dz_mbp_bot_build_info" {
			sawBuildInfo = true
		}
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under a sibling feed namespace", n)
		}
	}
	if !sawBuildInfo {
		t.Errorf("missing dz_mbp_bot_build_info in %v", names)
	}
}
```

- [ ] **Step 5: Tidy, verify, commit**

Run from `go/marketbyprice-bot`: `go mod tidy`. Confirm `go.sum` appears and the module builds standalone:

```bash
GOWORK=off go vet ./...
```

Then from `go/`: `gofmt -l ./marketbyprice-bot/` (must print nothing), `go vet ./marketbyprice-bot/...`, `go test ./marketbyprice-bot/...`. Validate the workflow YAML parses: `python3 -c "import yaml;yaml.safe_load(open('../.github/workflows/go-tests.yml'))"`.

```bash
git add go/go.work go/marketbyprice-bot/ .github/workflows/go-tests.yml go/*/Dockerfile
git commit -m "marketbyprice-bot: add module, record envelope, metrics, and socket reader"
```

---

## Task 2: The `Instrument` book and state machine

This is the core of the plan. **The code below was compiled and its tests executed before this plan was committed** — transcribe it exactly.

**Files:**
- Create: `go/marketbyprice-bot/instrument.go`
- Test: `go/marketbyprice-bot/instrument_test.go`

**Interfaces:**
- Consumes: `Record` from Task 1.
- Produces: `InstrumentStatus` (`StatusAwaitingSnapshot`, `StatusReady`, `StatusGap`) with `String()`; `LevelState{QtyRaw uint64; OrderCount uint16; Flags uint8}`; `u16Unavailable uint16 = 0xFFFF`; `PendingSnapshot`; `Instrument`; `NewInstrument(id uint32, symbol string, priceExp, qtyExp int8) *Instrument`; `DivergenceKind` with the four constants; methods `ApplyLevelUpdate(sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags, action uint8) []DivergenceKind`, `ApplyBookClear(clearSide, scope uint8, fromPriceRaw int64) error`, `BeginSnapshot(snapID uint32, anchorSeq uint64, totalLevels, lastInstrSeq, depthBound uint32)`, `AddSnapshotLevel(snapID uint32, sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags uint8) bool`, `EndSnapshot(snapID uint32, anchorSeq uint64) error`, `SnapshotAcceptable(anchorSeq uint64, lastInstrSeq uint32) (bool, error)`, `Crossed() bool`, `Reset(requiredAnchor *uint64)`; errors `errBookClearScopeSide`, `errSnapshotMismatch`, `errSnapshotShort`, `errNoOpenSnapshot`, `errStaleAnchor`.

- [ ] **Step 1: Write the failing tests**

Create `instrument_test.go` with the 12 tests below. They encode the spec behaviors this feed's engine exists to get right, so read each one's comment before implementing — several assert the *absence* of a tempting behavior.

```go
package main

import (
	"errors"
	"testing"
)

func ready(t *testing.T) *Instrument {
	t.Helper()
	i := NewInstrument(7, "BTC-USDT", -2, -8)
	i.Status = StatusReady
	return i
}

// Quantity is absolute, not a delta, and 0 removes the level.
func TestApplyLevelUpdate_AbsoluteAndDelete(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 3, 0, 1)
	if got := i.Bids[1000].QtyRaw; got != 50 {
		t.Fatalf("qty: got %d want 50", got)
	}
	// Absolute: 75 replaces 50, it does not add to it.
	i.ApplyLevelUpdate(0, 1000, 75, 3, 0, 2)
	if got := i.Bids[1000].QtyRaw; got != 75 {
		t.Fatalf("absolute apply: got %d want 75", got)
	}
	i.ApplyLevelUpdate(0, 1000, 0, 0, 0, 3)
	if _, present := i.Bids[1000]; present {
		t.Fatal("qty 0 must remove the level")
	}
}

// Action must never gate the apply: a wrong Action byte cannot corrupt a book.
func TestApplyLevelUpdate_ActionDoesNotGate(t *testing.T) {
	i := ready(t)
	// Action=Delete but non-zero qty: the level must still be set to 90.
	div := i.ApplyLevelUpdate(1, 2000, 90, 1, 0, 3)
	if i.Asks[2000] == nil || i.Asks[2000].QtyRaw != 90 {
		t.Fatalf("level must be set despite Action=Delete: %+v", i.Asks[2000])
	}
	if len(div) != 1 || div[0] != DivergenceDeleteNonzeroQty {
		t.Fatalf("expected delete_nonzero_qty divergence, got %v", div)
	}
}

func TestApplyLevelUpdate_DivergenceCounters(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)

	if div := i.ApplyLevelUpdate(0, 1000, 60, 1, 0, 1); len(div) != 1 || div[0] != DivergenceNewOnPresent {
		t.Errorf("New on present level: got %v", div)
	}
	if div := i.ApplyLevelUpdate(0, 9999, 60, 1, 0, 2); len(div) != 1 || div[0] != DivergenceChangeOnAbsent {
		t.Errorf("Change on absent level: got %v", div)
	}
	if div := i.ApplyLevelUpdate(0, 1000, 0, 0, 0, 2); len(div) != 1 || div[0] != DivergenceZeroQtyBadAction {
		t.Errorf("qty 0 with Action != Delete: got %v", div)
	}
	// A correct New on an absent level diverges not at all.
	if div := i.ApplyLevelUpdate(0, 1234, 10, 1, 0, 1); len(div) != 0 {
		t.Errorf("clean apply must not diverge: got %v", div)
	}
}

func TestApplyBookClear_EntireSide(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	i.ApplyLevelUpdate(0, 900, 5, 1, 0, 1)
	i.ApplyLevelUpdate(1, 1100, 5, 1, 0, 1)
	if err := i.ApplyBookClear(0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if len(i.Bids) != 0 {
		t.Errorf("bids should be empty: %v", i.Bids)
	}
	if len(i.Asks) != 1 {
		t.Errorf("asks must be untouched: %v", i.Asks)
	}
}

// Scope=1 on bids clears at or BELOW the bound; on asks at or ABOVE it.
func TestApplyBookClear_FromPriceOutward(t *testing.T) {
	i := ready(t)
	for _, p := range []int64{800, 900, 1000, 1100} {
		i.ApplyLevelUpdate(0, p, 5, 1, 0, 1)
		i.ApplyLevelUpdate(1, p, 5, 1, 0, 1)
	}
	if err := i.ApplyBookClear(0, 1, 900); err != nil {
		t.Fatal(err)
	}
	if _, gone := i.Bids[800]; gone {
		t.Error("bid 800 is below the bound and must be cleared")
	}
	if _, gone := i.Bids[900]; gone {
		t.Error("bound is inclusive; 900 must be cleared")
	}
	if i.Bids[1000] == nil || i.Bids[1100] == nil {
		t.Error("bids above the bound must survive")
	}
	if err := i.ApplyBookClear(1, 1, 1000); err != nil {
		t.Fatal(err)
	}
	if i.Asks[800] == nil || i.Asks[900] == nil {
		t.Error("asks below the bound must survive")
	}
	if _, gone := i.Asks[1000]; gone {
		t.Error("bound is inclusive; ask 1000 must be cleared")
	}
	if _, gone := i.Asks[1100]; gone {
		t.Error("ask 1100 is above the bound and must be cleared")
	}
}

func TestApplyBookClear_ScopeBothSidesMalformed(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	if err := i.ApplyBookClear(2, 1, 1000); !errors.Is(err, errBookClearScopeSide) {
		t.Fatalf("expected errBookClearScopeSide, got %v", err)
	}
	if i.Bids[1000] == nil {
		t.Error("a malformed BookClear must not mutate the book")
	}
}

// A short snapshot must NOT evict a live, correct book. This is the deliberate
// deviation from the spec's literal cold-start step 6.
func TestEndSnapshot_ShortDoesNotDemoteReadyBook(t *testing.T) {
	i := ready(t)
	i.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)
	i.LastAppliedInstrumentSeq = 42

	i.BeginSnapshot(9, 5000, 2 /*total*/, 60, 0)
	if i.Status != StatusReady {
		t.Fatal("BeginSnapshot must not change Status")
	}
	i.AddSnapshotLevel(9, 0, 1111, 7, 1, 0) // only 1 of 2
	err := i.EndSnapshot(9, 5000)
	if !errors.Is(err, errSnapshotShort) {
		t.Fatalf("expected errSnapshotShort, got %v", err)
	}
	if i.Status != StatusReady {
		t.Errorf("status must stay ready, got %v", i.Status)
	}
	if i.Bids[1000] == nil || i.Bids[1000].QtyRaw != 50 {
		t.Error("live book must survive a short snapshot")
	}
	if i.LastAppliedInstrumentSeq != 42 {
		t.Errorf("trackers must survive, got %d", i.LastAppliedInstrumentSeq)
	}
	if i.OpenSnapshot != nil {
		t.Error("shadow must be discarded")
	}
}

func TestEndSnapshot_CommitsAndSetsDepthBound(t *testing.T) {
	i := NewInstrument(7, "X", 0, 0)
	i.BeginSnapshot(3, 5000, 2, 77, 25)
	i.AddSnapshotLevel(3, 0, 1000, 10, 2, 0)
	i.AddSnapshotLevel(3, 1, 1100, 20, 4, 0)
	if err := i.EndSnapshot(3, 5000); err != nil {
		t.Fatal(err)
	}
	if i.Status != StatusReady {
		t.Errorf("status: %v", i.Status)
	}
	if i.LastAppliedMktdataSeq != 5000 || i.LastAppliedInstrumentSeq != 77 {
		t.Errorf("trackers: %d %d", i.LastAppliedMktdataSeq, i.LastAppliedInstrumentSeq)
	}
	if i.DepthBound == nil || *i.DepthBound != 25 {
		t.Errorf("depth bound: %v", i.DepthBound)
	}
}

// Depth bound defaults to unknown, never 0. A never-snapshotted instrument must
// not assert completeness.
func TestDepthBound_DefaultsUnknown(t *testing.T) {
	i := NewInstrument(1, "X", 0, 0)
	if i.DepthBound != nil {
		t.Fatalf("depth bound must start nil (unknown), got %v", *i.DepthBound)
	}
	i.BeginSnapshot(1, 1, 0, 0, 0)
	if err := i.EndSnapshot(1, 1); err != nil {
		t.Fatal(err)
	}
	if i.DepthBound == nil || *i.DepthBound != 0 {
		t.Fatal("after a complete snapshot the bound is a positive claim of 0")
	}
	i.Reset(nil)
	if i.DepthBound != nil {
		t.Fatal("reset must return the bound to unknown, not 0")
	}
}

// The discriminator is Last Instrument Seq, not Anchor Seq.
func TestSnapshotAcceptable_ReadyDiscriminator(t *testing.T) {
	i := ready(t)
	i.LastAppliedInstrumentSeq = 100
	i.LastAppliedMktdataSeq = 500

	// Behind: snapshot captured after deltas we never applied.
	if ok, err := i.SnapshotAcceptable(600, 101); err != nil || !ok {
		t.Errorf("K > tracker must re-bootstrap: ok=%v err=%v", ok, err)
	}
	// Current: ordinary case, ignore.
	if ok, _ := i.SnapshotAcceptable(600, 100); ok {
		t.Error("K == tracker must be ignored")
	}
	if ok, _ := i.SnapshotAcceptable(600, 99); ok {
		t.Error("K < tracker must be ignored")
	}
	// A far-advanced Anchor Seq alone must NOT trigger a rebuild — this is the
	// trap that would rebuild every book on every rotation.
	if ok, _ := i.SnapshotAcceptable(999999, 100); ok {
		t.Error("anchor seq must not drive the decision")
	}
	// Not ready: always acceptable.
	i.Status = StatusGap
	if ok, _ := i.SnapshotAcceptable(1, 1); !ok {
		t.Error("a gap instrument must accept any snapshot")
	}
}

// A snapshot captured before an InstrumentReset but delivered after it must be
// discarded, or the instrument ends ready holding the diverged book the reset
// existed to discard — with no gap and no counter.
func TestRequiredAnchor_DiscardsStaleSnapshot(t *testing.T) {
	i := ready(t)
	anchor := uint64(9000)
	i.Reset(&anchor)

	if ok, err := i.SnapshotAcceptable(8999, 1); ok || !errors.Is(err, errStaleAnchor) {
		t.Fatalf("older anchor must be rejected: ok=%v err=%v", ok, err)
	}
	if ok, err := i.SnapshotAcceptable(9000, 1); !ok || err != nil {
		t.Fatalf("exact anchor must be accepted: ok=%v err=%v", ok, err)
	}
	// Cleared by ANY snapshot at or after S', not only an exact match — the
	// mandated snapshot at S' can itself be lost.
	i.BeginSnapshot(1, 9500, 0, 0, 0)
	if err := i.EndSnapshot(1, 9500); err != nil {
		t.Fatal(err)
	}
	if i.RequiredAnchorSeq != nil {
		t.Error("a newer accepted snapshot must clear the required anchor")
	}
}

func TestCrossed(t *testing.T) {
	i := ready(t)
	if i.Crossed() {
		t.Error("an empty book is not crossed")
	}
	i.ApplyLevelUpdate(0, 1000, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("one-sided book is not crossed")
	}
	i.ApplyLevelUpdate(1, 1100, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("bid 1000 < ask 1100 is not crossed")
	}
	// Locked book: routine on some venues, must not count as crossed.
	i.ApplyLevelUpdate(1, 1000, 5, 1, 0, 1)
	if i.Crossed() {
		t.Error("locked book (bid == ask) must not count as crossed")
	}
	i.ApplyLevelUpdate(0, 1200, 5, 1, 0, 1)
	if !i.Crossed() {
		t.Error("bid 1200 > ask 1000 is crossed")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run from `go/`: `go test ./marketbyprice-bot/... 2>&1 | head -20`
Expected: compile failure — `undefined: NewInstrument`, `undefined: StatusReady`, and so on.

- [ ] **Step 3: Write `instrument.go`**

Transcribe exactly:

```go
package main

import (
	"errors"
	"fmt"
)

// InstrumentStatus is the serving status of one instrument's book.
//
// The spec's five-state machine collapses to three here, because two of its
// states are represented orthogonally: "awaiting-refdata" is absence from the
// shard's instrument map, and "building-snapshot" is OpenSnapshot != nil, which
// is deliberately independent of serving status so that building a snapshot can
// never affect whether the current book is usable.
type InstrumentStatus int

const (
	StatusAwaitingSnapshot InstrumentStatus = iota
	StatusReady
	StatusGap
)

func (s InstrumentStatus) String() string {
	switch s {
	case StatusAwaitingSnapshot:
		return "awaiting-snapshot"
	case StatusReady:
		return "ready"
	case StatusGap:
		return "gap"
	default:
		return "unknown"
	}
}

// LevelState is one aggregated price level. Quantity is absolute.
type LevelState struct {
	QtyRaw     uint64
	OrderCount uint16 // u16Unavailable (0xFFFF) means the venue did not supply it
	Flags      uint8
}

// u16Unavailable mirrors the parser's sentinel: absent, or too large to express.
const u16Unavailable uint16 = 0xFFFF

// PendingSnapshot is the shadow built between SnapshotBegin and SnapshotEnd.
// It is never the live book: on any validation failure only the shadow is
// discarded, so a short snapshot cannot evict a book the deltas are keeping
// correct.
type PendingSnapshot struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	LastInstrumentSeq uint32
	DepthBound        uint32
	ReceivedLevels    uint32
	Bids, Asks        map[int64]*LevelState
}

// Instrument holds the book and state-machine position for one
// (channel_id, instrument_id).
type Instrument struct {
	ID            uint32
	Symbol        string
	PriceExponent int8
	QtyExponent   int8
	Status        InstrumentStatus

	// Books keyed by RAW price. Rank is derived by sorting keys at read time;
	// the spec forbids keying book state on rank.
	Bids, Asks map[int64]*LevelState

	// DepthBound: nil = unknown, 0 = publisher claims a complete book,
	// N = bounded at N levels per side. Defaults to unknown and MUST NOT
	// default to 0 — a never-snapshotted instrument must not assert
	// completeness through the subscriber's own initialisation.
	DepthBound *uint32

	LastAppliedMktdataSeq    uint64
	LastAppliedInstrumentSeq uint32

	// RequiredAnchorSeq is set by InstrumentReset. While non-nil, any
	// SnapshotBegin with an older Anchor Seq MUST be discarded.
	RequiredAnchorSeq *uint64

	OpenSnapshot *PendingSnapshot
	Pending      map[uint32]Record // out-of-order deltas keyed by per_instrument_seq
}

func NewInstrument(id uint32, symbol string, priceExp, qtyExp int8) *Instrument {
	return &Instrument{
		ID:            id,
		Symbol:        symbol,
		PriceExponent: priceExp,
		QtyExponent:   qtyExp,
		Status:        StatusAwaitingSnapshot,
		Bids:          map[int64]*LevelState{},
		Asks:          map[int64]*LevelState{},
	}
}

func (i *Instrument) side(s uint8) map[int64]*LevelState {
	if s == 1 {
		return i.Asks
	}
	return i.Bids
}

// DivergenceKind classifies a publisher/subscriber disagreement that the spec
// asks a subscriber to count without altering the applied result.
type DivergenceKind string

const (
	DivergenceNewOnPresent     DivergenceKind = "new_on_present"
	DivergenceChangeOnAbsent   DivergenceKind = "change_on_absent"
	DivergenceDeleteNonzeroQty DivergenceKind = "delete_nonzero_qty"
	DivergenceZeroQtyBadAction DivergenceKind = "zero_qty_wrong_action"
)

// ApplyLevelUpdate applies the spec's absolute-quantity rule and returns any
// divergence observed. Action NEVER gates the apply: every LevelUpdate states
// the complete resulting state of one level, so applying by quantity alone
// always produces the correct level regardless of what Action claims.
func (i *Instrument) ApplyLevelUpdate(sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags, action uint8) []DivergenceKind {
	book := i.side(sideByte)
	_, present := book[priceRaw]

	// Independent checks, deliberately NOT a switch. The spec's four divergence
	// conditions are not mutually exclusive — Quantity=0 with Action=New on an
	// already-present level violates two of them at once — and the spec asks a
	// subscriber to surface each. A switch fires at most one case and would
	// silently drop the rest, under-reporting exactly the doubly-malformed
	// messages that most deserve attention.
	var div []DivergenceKind
	if qtyRaw == 0 && action != 3 {
		// Publisher rule: Quantity 0 is only legal with Action=Delete.
		div = append(div, DivergenceZeroQtyBadAction)
	}
	if qtyRaw != 0 && action == 3 {
		div = append(div, DivergenceDeleteNonzeroQty)
	}
	if action == 1 && present {
		div = append(div, DivergenceNewOnPresent)
	}
	if action == 2 && !present {
		div = append(div, DivergenceChangeOnAbsent)
	}

	if qtyRaw == 0 {
		delete(book, priceRaw)
		return div
	}
	book[priceRaw] = &LevelState{QtyRaw: qtyRaw, OrderCount: orderCount, Flags: flags}
	return div
}

var errBookClearScopeSide = errors.New("book_clear scope=1 with clear_side=both")

// ApplyBookClear removes levels in bulk. clearSide 0=bid, 1=ask, 2=both.
// scope 0 clears the whole side(s); scope 1 clears from fromPriceRaw outward —
// for bids every level at or below it, for asks every level at or above it.
//
// A BookClear is not a resynchronisation signal: an instrument that applies one
// stays ready.
func (i *Instrument) ApplyBookClear(clearSide, scope uint8, fromPriceRaw int64) error {
	if scope == 1 && clearSide == 2 {
		// One price cannot bound both sides.
		return fmt.Errorf("%w", errBookClearScopeSide)
	}
	clear := func(book map[int64]*LevelState, isBid bool) {
		if scope == 0 {
			for p := range book {
				delete(book, p)
			}
			return
		}
		for p := range book {
			if (isBid && p <= fromPriceRaw) || (!isBid && p >= fromPriceRaw) {
				delete(book, p)
			}
		}
	}
	if clearSide == 0 || clearSide == 2 {
		clear(i.Bids, true)
	}
	if clearSide == 1 || clearSide == 2 {
		clear(i.Asks, false)
	}
	return nil
}

// BeginSnapshot opens a shadow. Status and the live book are untouched.
func (i *Instrument) BeginSnapshot(snapID uint32, anchorSeq uint64, totalLevels, lastInstrSeq, depthBound uint32) {
	i.OpenSnapshot = &PendingSnapshot{
		SnapshotID:        snapID,
		AnchorSeq:         anchorSeq,
		TotalLevels:       totalLevels,
		LastInstrumentSeq: lastInstrSeq,
		DepthBound:        depthBound,
		Bids:              map[int64]*LevelState{},
		Asks:              map[int64]*LevelState{},
	}
}

// AddSnapshotLevel inserts into the shadow. Returns false when snapID does not
// match the open shadow, which the caller counts and discards.
func (i *Instrument) AddSnapshotLevel(snapID uint32, sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags uint8) bool {
	if i.OpenSnapshot == nil || i.OpenSnapshot.SnapshotID != snapID {
		return false
	}
	book := i.OpenSnapshot.Bids
	if sideByte == 1 {
		book = i.OpenSnapshot.Asks
	}
	book[priceRaw] = &LevelState{QtyRaw: qtyRaw, OrderCount: orderCount, Flags: flags}
	i.OpenSnapshot.ReceivedLevels++
	return true
}

var (
	errSnapshotMismatch = errors.New("snapshot end mismatch")
	errSnapshotShort    = errors.New("snapshot level count mismatch")
	errNoOpenSnapshot   = errors.New("snapshot end with no open snapshot")
	errStaleAnchor      = errors.New("snapshot anchor older than required anchor")
)

// EndSnapshot validates and commits the shadow. On ANY failure only the shadow
// is discarded: Status, Bids, and Asks are never touched. For an instrument that
// was already Ready this deliberately departs from the spec's literal "discard
// the partial book and revert to awaiting-snapshot", because dropping a book the
// deltas are keeping correct costs a full round-robin cycle of availability and
// buys nothing — the spec's own gap-recovery schedule repairs a bad book on the
// next snapshot either way.
func (i *Instrument) EndSnapshot(snapID uint32, anchorSeq uint64) error {
	if i.OpenSnapshot == nil {
		return errNoOpenSnapshot
	}
	if i.OpenSnapshot.SnapshotID != snapID || i.OpenSnapshot.AnchorSeq != anchorSeq {
		i.OpenSnapshot = nil
		return fmt.Errorf("%w: snapshot_id=%d anchor=%d", errSnapshotMismatch, snapID, anchorSeq)
	}
	if i.OpenSnapshot.ReceivedLevels != i.OpenSnapshot.TotalLevels {
		got, want := i.OpenSnapshot.ReceivedLevels, i.OpenSnapshot.TotalLevels
		i.OpenSnapshot = nil
		return fmt.Errorf("%w: got %d expected %d", errSnapshotShort, got, want)
	}

	depth := i.OpenSnapshot.DepthBound
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	i.LastAppliedMktdataSeq = i.OpenSnapshot.AnchorSeq
	i.LastAppliedInstrumentSeq = i.OpenSnapshot.LastInstrumentSeq
	i.DepthBound = &depth
	// Clear the required anchor on ANY accepted snapshot at or after it, not
	// only an exact match: the publisher's mandated snapshot at S' can itself be
	// lost, and the next round-robin snapshot carries a newer anchor and is a
	// perfectly good recovery. Clearing only on exact match would leave the
	// required anchor set permanently.
	if i.RequiredAnchorSeq != nil && i.OpenSnapshot.AnchorSeq >= *i.RequiredAnchorSeq {
		i.RequiredAnchorSeq = nil
	}
	i.OpenSnapshot = nil
	i.Status = StatusReady
	return nil
}

// SnapshotAcceptable decides whether a SnapshotBegin should be processed.
//
// The discriminator is Last Instrument Seq, NOT Anchor Seq. Anchor Seq is a
// channel-wide mktdata sequence that advances on every other instrument's
// deltas and on every heartbeat, so comparing it against this instrument's
// tracker would be true for nearly every instrument on nearly every cycle and
// would rebuild every good book on every rotation.
func (i *Instrument) SnapshotAcceptable(anchorSeq uint64, lastInstrSeq uint32) (bool, error) {
	if i.RequiredAnchorSeq != nil && anchorSeq < *i.RequiredAnchorSeq {
		return false, errStaleAnchor
	}
	if i.Status != StatusReady {
		return true, nil
	}
	// Ready: only re-bootstrap when the snapshot was captured after deltas this
	// subscriber never applied.
	return lastInstrSeq > i.LastAppliedInstrumentSeq, nil
}

// Crossed reports whether the inside market is crossed. Strict >, so a locked
// book (best bid == best ask), which is routine on some venues, is not counted.
func (i *Instrument) Crossed() bool {
	if len(i.Bids) == 0 || len(i.Asks) == 0 {
		return false
	}
	bestBid, bestAsk := int64(0), int64(0)
	first := true
	for p := range i.Bids {
		if first || p > bestBid {
			bestBid, first = p, false
		}
	}
	first = true
	for p := range i.Asks {
		if first || p < bestAsk {
			bestAsk, first = p, false
		}
	}
	return bestBid > bestAsk
}

// Reset discards all level state and returns to awaiting-snapshot, recording
// the required anchor from an InstrumentReset.
func (i *Instrument) Reset(requiredAnchor *uint64) {
	i.Bids = map[int64]*LevelState{}
	i.Asks = map[int64]*LevelState{}
	i.OpenSnapshot = nil
	i.Pending = nil
	i.Status = StatusAwaitingSnapshot
	i.LastAppliedMktdataSeq = 0
	i.LastAppliedInstrumentSeq = 0
	i.DepthBound = nil // back to unknown, never 0
	i.RequiredAnchorSeq = requiredAnchor
}
```

Note `u16Unavailable` is declared but unused until Task 6 reads it out. Go permits unused package-level identifiers; do not delete it.

- [ ] **Step 4: Run the tests to verify they pass**

Run from `go/`: `go test ./marketbyprice-bot/... -v -run 'TestApply|TestEndSnapshot|TestDepthBound|TestSnapshotAcceptable|TestRequiredAnchor|TestCrossed'`
Expected: 12 tests PASS.

- [ ] **Step 5: Commit**

```bash
gofmt -l ./marketbyprice-bot/   # from go/, must print nothing
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add price-keyed instrument book and state machine"
```

---

## Task 3: Sequencing and the bounded delta buffer

**Files:**
- Create: `go/marketbyprice-bot/shard.go`
- Test: `go/marketbyprice-bot/shard_test.go`

**Interfaces:**
- Consumes: `Instrument` and its methods (Task 2), `Record` and `Metrics` (Task 1).
- Produces: `instKey{ch uint8; id uint32}`; `BufferedDelta{MktdataSeq uint64; Record Record}`; `InstrumentDef{Symbol string; PriceExponent, QtyExponent int8; ManifestSeq uint16}`; `Shard` with `NewShard(idx, n int, metrics *Metrics) *Shard`; `applyDelta`, `applyDeltaToReady`, `applyOne`, `bufferDelta`, `evictLargestBuffer`, `replayBuffer`, `filterBuffer`; `ChannelEvent{Kind string; InstrumentID uint32; Symbol string; Record Record}`; the JSON coercion helpers `toUint8/toUint16/toUint32/toUint64/toInt8/toInt64/toString/toTime`, `orderCountFrom`, `sideFromString`, `clearSideFromString`, `scopeFromString`, `actionFromString`; constants `maxBufferedDeltasPerShard = 200000` and `reorderWindow = 16`.

**This task's code was compiled and its 9 tests executed in a scratch module before this plan was committed** — `go vet` clean, gofmt clean, all passing alongside Task 2's 12 tests. Transcribe it as given.

Writing it surfaced three things worth stating up front, because the obvious implementation gets each of them wrong:

1. **`order_count` absent means the sentinel, not zero.** The parser *omits* the key when the wire carried `0xFFFF`, because that value means "not provided, or too large to express". A bare `toUint16(fields["order_count"])` returns `0` on an absent key, silently converting "unknown" into "zero resting orders". `orderCountFrom` maps absent back to `u16Unavailable`, and `0` stays a real count.
2. **A malformed `BookClear` must not advance the sequence trackers.** Nothing was applied, so advancing `last_applied` would classify the *next* delta against a wrong expected seq and let a real gap pass undetected.
3. **`maxBuffered` is a field, not the bare constant, and `bufferedN` is a running total.** The field lets tests drive the overflow path without allocating 200,000 records; the running total makes overflow detection O(1) instead of summing the map on every buffered delta.

Sequencing rules, from the spec's steady state, per `(channel_id, instrument_id)`:

- `Per-Instrument Seq == last_applied + 1` → apply.
- `<= last_applied` → duplicate or late, discard silently. A duplicated frame during bootstrap must not cost a re-bootstrap.
- `> last_applied + 1` → hold in `Pending` within a small reorder window; beyond the window it is a genuine gap, so mark the instrument `gap`, drop `Pending`, buffer the delta, and count `PerInstrumentGapsTotal`.

The reorder window is carried over from the sibling bot, where the snapshot stream was observed reordering on the live path.

**Delta buffer.** The spec requires a bounded buffer *and* a declared overflow policy, and sizes the cold-start worst case at ~1.4 GB for a 60 s cycle — noting the cycle-period knob and the memory knob are the same knob. The sibling bot caps at 10,000 deltas per instrument and drops the oldest silently, which loses the tail of a recovery without recording it.

Implement the spec's recommended policy instead: a **per-shard** budget by record count, and on overflow drop the buffered deltas for the instrument holding the most buffered data, mark that instrument `gap`, continue, and count `DeltaBufferOverflowTotal`. Sustained overflow means the cycle period is too long for the deployment's memory budget, which is a tuning signal an operator needs to see.

- [ ] **Step 1: Write the failing tests**

Create `shard_test.go`. Cover exactly these cases:

```go
package main

import "testing"

func newTestShard(t *testing.T) *Shard {
	t.Helper()
	return NewShard(0, 1, nil)
}

// A record helper: build a level_update Record the way the parser emits one.
func levelUpdateRec(instID uint32, mktSeq uint64, piSeq uint32, side string, priceRaw int64, qtyRaw uint64) Record {
	return Record{
		Type:           "level_update",
		Port:           "mktdata",
		SequenceNumber: mktSeq,
		InstrumentID:   instID,
		Fields: map[string]any{
			"side":               side,
			"action":             "new",
			"per_instrument_seq": float64(piSeq), // JSON numbers decode as float64
			"price_raw":          float64(priceRaw),
			"qty_raw":            float64(qtyRaw),
			"update_reason":      "new_order",
			"level_flags":        float64(0),
		},
	}
}
```

Note the helper sets `"order_count": float64(1)` — include it, because an absent key means the sentinel (see point 1 above) and several tests would then assert against `0xFFFF` rather than a real count.

Then add these nine tests. The verified bodies are in the scratch prototype; each is listed here with its exact setup and assertions so the requirement is unambiguous:

1. `TestApplyDelta_ContiguousApplies` — ready instrument at `LastAppliedInstrumentSeq = 5`; apply mkt seq 900 / pi seq 6. Expect one `applied_delta` event, both trackers advanced to 6 and 900, and the level present with the right quantity.
2. `TestApplyDelta_DuplicateDiscardedSilently` — same instrument; deliver pi seqs 5, 3, and 1. Each must produce zero events, leave the tracker at 5, leave the book empty, and leave `deltaBuf` empty. Duplicates are neither applied nor buffered.
3. `TestApplyDelta_ReorderWithinWindowHeldThenDrained` — deliver pi seq 8, then 7 (both held, zero events, tracker still 5), then 6. The 6 delivery must return **three** events and leave the tracker at 8, with `Pending` nil and all three levels present.
4. `TestApplyDelta_GapBeyondWindowDemotes` — deliver pi seq `5 + reorderWindow + 2`. Expect one `per_instrument_gap` event, `StatusGap`, `Pending == nil`, and the triggering delta buffered.
5. `TestApplyDelta_NotReadyBuffers` — an `awaiting-snapshot` instrument buffers rather than applies; book stays empty.
6. `TestApplyDelta_UnknownInstrumentBuffers` — an instrument absent from the map buffers (it is `awaiting-refdata`).
7. `TestReplayBuffer_SkipsAtOrBelowAnchor` — buffer four deltas at mkt seqs 500–503 with pi seqs 1–4, then apply a snapshot with `anchor_seq = 501` and `last_instrument_seq = 2`, then replay. Expect the tracker at 4, the two pre-anchor levels absent, the two post-anchor levels present, the `deltaBuf` entry deleted, and `bufferedN == 0`.
8. `TestDeltaBuffer_OverflowEvictsLargestAndMarksGap` — set `s.maxBuffered = 10`, buffer 8 records for instrument 1 and 2 for instrument 2, assert no eviction yet, then buffer one more. Expect instrument 1's buffer evicted and its status `gap`, instrument 2's 3 records intact and its status unchanged, and `bufferedN == 3`.
9. `TestOrderCountFrom_AbsentMeansSentinel` — absent key → `u16Unavailable`; explicit `0` → `0`; `7` → `7`.

Plus one test for the tracker rule in point 2 above:

10. `TestApplyOne_MalformedBookClearDoesNotAdvanceTrackers` — a ready instrument at tracker 5 with a level in its book receives a `book_clear` with `clear_side: "both"` and `scope: "from_price"` (the malformed combination) at pi seq 6. The tracker must stay at 5 and the book must be untouched.

- [ ] **Step 2: Run to verify failure**

Run from `go/`: `go test ./marketbyprice-bot/... 2>&1 | head -20`
Expected: compile failure — `undefined: NewShard`.

- [ ] **Step 3: Implement `shard.go`'s sequencing and buffer**

Write `Shard` with `mu sync.Mutex` guarding `instruments`, `refdata`, and `deltaBuf` (a later task adds a reader goroutine; the mutex exists for it). Implement:

- `applyDelta(k, rec)` — the three-way sequence classification above, delegating the actual mutation to `applyOne`.
- `applyOne(inst, rec)` — switch on `rec.Type`: `level_update` calls `inst.ApplyLevelUpdate`, `book_clear` calls `inst.ApplyBookClear`. Then advance `LastAppliedMktdataSeq` and `LastAppliedInstrumentSeq`. Feed each returned `DivergenceKind` to `metrics.BookDivergenceTotal.WithLabelValues(string(kind)).Inc()` when metrics is non-nil. A `book_clear` that returns `errBookClearScopeSide` is discarded and counted — it must not advance the trackers, because a malformed message was never applied.
- `bufferDelta(k, rec)` — append, keep the per-instrument slice ordered by `MktdataSeq`, and enforce the shard budget with the eviction policy above.
- `replayBuffer(k, inst)` — drop entries at or below `inst.LastAppliedMktdataSeq`, replay the rest in ascending order through the same classification as steady state, then delete the map entry.

The JSON coercion helpers can be copied from `go/marketbyorder-bot/shard.go` (`toUint8` … `toTime`, `sideFromString`), which exist because `encoding/json` yields `float64` for every number. Add `clearSideFromString` (`"bid"`→0, `"ask"`→1, `"both"`→2), `scopeFromString` (`"entire_side"`→0, `"from_price"`→1), and `actionFromString` (`"new"`→1, `"change"`→2, `"delete"`→3, anything else→0), matching the parser's stringers exactly — read `go/marketbyprice-parser/marketbyprice.go` for the emitted values rather than guessing.

- [ ] **Step 4: Verify and commit**

Run from `go/`: `gofmt -l ./marketbyprice-bot/`, `go vet ./marketbyprice-bot/...`, `go test ./marketbyprice-bot/... -v`.

```bash
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add delta sequencing and bounded delta buffer"
```

---

## Task 4: Shard record dispatch, snapshot lifecycle, crossed-book monitoring

**Files:**
- Modify: `go/marketbyprice-bot/shard.go`
- Test: `go/marketbyprice-bot/shard_test.go`

**Interfaces:**
- Produces: `Shard.apply(rec Record) []ChannelEvent`, `Shard.handle(rec Record)`, `Shard.Run(ctx context.Context)`, `Shard.reset()`; `shardMsg{rec *Record; kind shardMsgKind; ack chan int}` with `msgRecord`, `msgReset`, `msgFence`; `Shard.inbox chan shardMsg`.

- [ ] **Step 1: Write the failing tests**

Add to `shard_test.go`:

1. `TestApply_InstrumentDefinitionCreatesInstrument` — an `instrument_definition` record populates `refdata` and creates the `Instrument` with the right symbol and exponents.
2. `TestApply_SnapshotLifecycleCommits` — `snapshot_begin` → two `snapshot_level` → `snapshot_end` leaves the instrument `StatusReady` with both levels and the depth bound set.
3. `TestApply_SnapshotLevelWrongIDDropped` — a `snapshot_level` whose `snapshot_id` does not match the open shadow is dropped and counts `SnapshotLevelDroppedTotal`; the shadow's `ReceivedLevels` does not advance.
4. `TestApply_SnapshotWhileReadyIgnoredWhenCurrent` — a ready instrument at `LastAppliedInstrumentSeq = 100` receiving `snapshot_begin` with `last_instrument_seq = 100` does **not** open a shadow.
5. `TestApply_SnapshotWhileReadyRebootstrapsWhenBehind` — same but `last_instrument_seq = 150` opens a shadow and, after `snapshot_end`, replaces the book.
6. `TestApply_InstrumentResetSetsRequiredAnchorAndDropsBuffer` — `instrument_reset` with `new_anchor_seq = S'` clears the book, sets the required anchor, and drops buffered deltas with `MktdataSeq <= S'` while keeping later ones.
7. `TestApply_StaleSnapshotAfterResetDiscarded` — after that reset, a `snapshot_begin` with `anchor_seq < S'` is discarded, the instrument stays `awaiting-snapshot`, and `SnapshotDiscardedTotal{reason="stale_anchor"}` increments.
8. `TestCrossedBook_PerDeltaWhenNoBatchBoundary` — on a channel with no `batch_boundary` seen, applying a delta that crosses the book increments `CrossedBookEventsTotal`.
9. `TestCrossedBook_AtBoundaryWhenBatching` — once a `batch_boundary` has been seen, a crossing delta does **not** count immediately; the count happens when the next `batch_boundary` arrives, and only for instruments touched since the previous boundary.
10. `TestCrossedBook_DoesNotChangeStatus` — a crossed book leaves `Status == StatusReady` and the book intact. Crossed-book monitoring is observability, never control flow.

- [ ] **Step 2: Run to verify failure, then implement**

Extend `shard.go` with `apply(rec)` switching on `rec.Type`:

- `instrument_definition` → record in `refdata`, create or update the `Instrument`.
- `snapshot_begin` → call `inst.SnapshotAcceptable(anchorSeq, lastInstrSeq)`. On `errStaleAnchor`, count `SnapshotDiscardedTotal{reason="stale_anchor"}` and return. When not acceptable (ready and current), return without opening a shadow. Otherwise `inst.BeginSnapshot(...)`.
- `snapshot_level` → `inst.AddSnapshotLevel(...)`; a false return counts `SnapshotLevelDroppedTotal`.
- `snapshot_end` → `inst.EndSnapshot(...)`; on error count `SnapshotDiscardedTotal` with reason `short`, `mismatch`, or `no_open_snapshot` and return without touching status. On success call `replayBuffer` and emit an `applied_snapshot` event.
- `level_update`, `book_clear` → `applyDelta`.
- `instrument_reset` → read `new_anchor_seq`, call `inst.Reset(&anchor)`, filter `deltaBuf[k]` to entries with `MktdataSeq > anchor`, count `InstrumentResetsTotal{reason}`.
- `trade`, `liquidation` → no book effect; emit an event for the future persistence layer and return.

**Crossed-book monitoring.** Track on the shard: `sawBatchBoundary bool` and `touchedSinceBoundary map[instKey]struct{}`. After a delta applies, if `!sawBatchBoundary`, evaluate `inst.Crossed()` immediately; otherwise record the instrument in `touchedSinceBoundary`. On a `batch_boundary` record, set `sawBatchBoundary = true`, evaluate every instrument in `touchedSinceBoundary`, then clear it. Maintain `CrossedInstruments` as a gauge of the currently-crossed set. Evaluating only at boundaries is what makes the counter meaningful on a batching channel: intermediate states within a batch are explicitly not consistency points, so a transient cross there is legal rather than a defect.

Then add the inbox goroutine: `Run(ctx)` selecting on `ctx.Done()` and `s.inbox`, handling `msgRecord` via `handle`, `msgReset` by wiping state and acking, `msgFence` by acking only. Copy this structure from `go/marketbyorder-bot/shard.go`'s `Run` — it is feed-independent.

- [ ] **Step 3: Verify and commit**

Run `gofmt -l`, `go vet`, `go test -v`, and `go test -race ./marketbyprice-bot/...`.

```bash
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add snapshot lifecycle and crossed-book monitoring"
```

---

## Task 5: Coordinator — routing, reset barrier, fence

**Files:**
- Create: `go/marketbyprice-bot/coordinator.go`
- Test: `go/marketbyprice-bot/coordinator_test.go`

**Interfaces:**
- Consumes: `Shard`, `shardMsg`, `Record`, `Metrics`.
- Produces: `Coordinator` implementing `Dispatcher`, with `NewCoordinator(ctx context.Context, shards []*Shard, metrics *Metrics) *Coordinator`, `Dispatch(rec Record)`, `runResetBarrier(held Record)`, `runFence(rec Record)`; `ManifestState{Seq uint16; Valid bool; InstrumentCount uint32}`.

**The routing rule that matters most.** `snapshot_level` records carry **no `instrument_id`** — the wire omits it because the containing `SnapshotBegin` implies it. The coordinator must therefore track the **single currently-open snapshot group** on the `snapshot` port and route `snapshot_level` records to that instrument's shard.

**Do not key snapshot routing by `snapshot_id`.** `Snapshot ID` is monotonic per `(channel_id, instrument_id)`, not per channel, so two instruments routinely sit at the same value within one cycle. Keying a route by `{channel_id, snapshot_id}` sends levels to whichever instrument last claimed that id — a different shard in general, where they are silently dropped. This is not hypothetical: it is issue #30 against `marketbyorder-bot`, which does exactly that. Use `snapshot_id` only to validate that a level belongs to the open group, and discard on mismatch.

Because the publisher MUST NOT interleave snapshot groups, one open group per channel is sufficient state:

```go
// openSnapshot is the currently-open snapshot group on the snapshot port, per
// channel. Publishers MUST NOT interleave groups, so one entry per channel is
// enough. snapshot_level records carry no instrument_id and are routed here.
type openGroup struct {
	instrumentID uint32
	snapshotID   uint32
	shard        int
}
```

Set it on `snapshot_begin`, clear it on `snapshot_end`. A `snapshot_level` arriving with no open group, or with a mismatched `snapshot_id`, is dropped and counted as `SnapshotLevelDroppedTotal`.

- [ ] **Step 1: Write the failing tests**

Create `coordinator_test.go` covering:

1. `TestDispatch_RoutesInstrumentRecordsByModulo` — a `level_update` for instrument 5 with 4 shards lands in shard 1.
2. `TestDispatch_SnapshotLevelRoutedToOpenGroupsShard` — `snapshot_begin` for instrument 5 (shard 1), then a `snapshot_level` with the matching `snapshot_id`, arrives at shard 1 even though the level record carries no `instrument_id`.
3. `TestDispatch_SnapshotLevelWithNoOpenGroupDropped` — a `snapshot_level` before any `snapshot_begin` is dropped and counted.
4. `TestDispatch_SnapshotLevelMismatchedIDDropped` — with a group open at `snapshot_id = 7`, a level carrying `snapshot_id = 8` is dropped and counted.
5. `TestDispatch_TwoInstrumentsSameSnapshotIDRouteIndependently` — open a group for instrument 4 at `snapshot_id = 5`, close it, then open one for instrument 7 also at `snapshot_id = 5`; levels after the second `snapshot_begin` must reach instrument 7's shard (3) and not instrument 4's (0).

   **This is not a regression test, despite appearances.** I built the wrong `{channel, snapshot_id}` route in a prototype and this test passed against it, because for sequential complete groups the later `snapshot_begin` overwrites the bad entry before any of its levels arrive. Keep the test — it pins the correct routing — but do not claim it discriminates between the two models. Test 5a is the one that does.

5a. **`TestDispatch_StrayLevelAfterSnapshotEndIsDroppedNotRouted`** — the test that actually discriminates. Run a complete group for instrument 4 at `snapshot_id = 5` (begin, one level, end), drain every shard, then dispatch one more `snapshot_level` bearing `snapshot_id = 5`. Assert **no shard receives it** and `SnapshotLevelDroppedTotal` is exactly 1.

   Verified: this fails against a `{channel, snapshot_id}` route that keeps its entry past the group's end (the stray level is routed to a shard and silently swallowed, counter stays 0) and passes against the open-group model, which deletes the group on `snapshot_end` so the level has nowhere to go and is counted. Requires a real `*Metrics` rather than nil, plus a small helper to read a counter's value via `prometheus.Metric.Write` into a `dto.Metric`.
6. `TestDispatch_ResetCountChangeRunsBarrier` — a `reset_count` change drains every shard via `msgReset`, clears coordinator state, counts `ChannelResetsTotal`, and then routes the triggering record as the first record of the new era.
7. `TestDispatch_EndOfSessionRunsFence` — `end_of_session` sends `msgFence` to every shard and waits for all acks.
8. `TestDispatch_ManifestSummaryUpdatesState` — a `manifest_summary` updates `ManifestState` without touching shards.
9. `TestDispatch_ManifestSeqBumpPrunesStaleInstruments` — see Step 2a below: after a `manifest_seq` bump, an instrument whose last `instrument_definition` carried the older seq is discarded, while one re-advertised under the new seq keeps its book and `StatusReady`.

- [ ] **Step 2a: Manifest-seq pruning**

The design spec requires it and the tasks above do not yet cover it: *"instruments no longer in the manifest are discarded, new ones enter awaiting-snapshot, and existing `ready` instruments that remain in the manifest retain their state."*

`ManifestSummary` carries only an instrument *count*, not the set, so membership has to come from `InstrumentDefinition` records, which each carry the `manifest_seq` they were emitted under. Two changes:

1. Add `ManifestSeq uint16` to `InstrumentDef` (Task 3) and populate it in the `instrument_definition` case from `rec.Fields["manifest_seq"]`.
2. Add a `msgManifestPrune` shard message carrying the new seq. On a `manifest_seq` increase, the coordinator broadcasts it; each shard drops every instrument (and its buffered deltas) whose `refdata` entry carries a `ManifestSeq` **older** than the new one, leaving the rest untouched.

Broadcast the prune rather than fencing on it: pruning is per-instrument state, so it belongs to the shard that owns the instrument.

One caution to encode in a comment: definitions are retransmitted continuously across a definition cycle (recommended 30 s), so instruments are re-advertised under a new `manifest_seq` gradually rather than all at once. Pruning immediately on the bump would evict instruments that are still in the manifest but have not been re-advertised yet. Prune on the bump only for instruments whose definition is older than the *previous* seq as well — that is, keep a one-generation grace window — or defer the prune by a definition cycle. Choose the grace-window form and state the reasoning in the code, because the naive version silently drops live books once per manifest change.

- [ ] **Step 2: Implement `coordinator.go`**

Model it on `go/marketbyorder-bot/coordinator.go` — the reset-barrier and fence machinery, including their `ctx`-aware ack waits that prevent wedging on shutdown, is feed-independent and should be copied. Replace its `snapshotRoute map[snapKey]int` with the `openGroup` model above, and replace the order-keyed record types in `Dispatch`'s switch with this feed's: `level_update`, `book_clear`, `instrument_definition`, `instrument_reset`, `trade`, `liquidation` route by instrument; `snapshot_begin` / `snapshot_level` / `snapshot_end` follow the open-group rule; `heartbeat` and `manifest_summary` are channel-scoped; `end_of_session` and `batch_boundary` run a fence.

`batch_boundary` must reach **every** shard, not one, because it carries no `instrument_id` and each shard evaluates crossed-book for the instruments it touched. Send it as a record to all shards rather than only fencing.

- [ ] **Step 3: Verify and commit**

Run `gofmt -l`, `go vet`, `go test -v`, `go test -race`.

```bash
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add coordinator routing and reset barrier"
```

---

## Task 6: Level read-out, entry point, README

**Files:**
- Create: `go/marketbyprice-bot/levels.go`, `main.go`, `README.md`
- Test: `go/marketbyprice-bot/levels_test.go`

**Interfaces:**
- Produces: `Level{Price, Qty float64; OrderCount uint32; CumulativeQty float64}`; `LevelSnapshot{InstrumentID uint32; Symbol string; Bids, Asks []Level; DepthBound *uint32; Crossed bool}`; `ComputeLevels(inst *Instrument, n int) LevelSnapshot`.

- [ ] **Step 1: Write the failing tests**

Create `levels_test.go` covering:

1. `TestComputeLevels_SortsBidsDescendingAsksAscending` — the first element of each side is the inside market.
2. `TestComputeLevels_ScalesByExponents` — with `PriceExponent = -2` and `QtyExponent = -8`, raw price `123456` reads as `1234.56` and raw qty `100000000` as `1.0`.
3. `TestComputeLevels_TopNTruncates` — with more levels than `n`, only the best `n` per side are returned, and `CumulativeQty` accumulates over the returned levels in rank order.
4. `TestComputeLevels_OrderCountSentinelBecomesZero` — a `LevelState.OrderCount` of `u16Unavailable` reads out as `0` rather than `65535`, because the sentinel means absent. Note this in the README so nobody mistakes it for a real count.
5. `TestComputeLevels_CarriesDepthBoundAndCrossed` — the returned snapshot reflects `inst.DepthBound` (including nil for unknown) and `inst.Crossed()`.

- [ ] **Step 2: Implement `levels.go`**

Sort the map keys per side and take the best `n`. Scale with `math.Pow10(int(exp))`. Unlike the sibling bot there is no aggregation step: the wire is already price-aggregated, so a level is a direct read of the map.

Carry `DepthBound` and `Crossed` onto the result. `CumulativeQty` is only meaningful as exhaustive depth when `DepthBound` is a non-nil `0`; say so in a comment, because a caller summing it under a non-zero bound is understating available liquidity — the exact failure the field exists to prevent.

- [ ] **Step 3: Write `main.go`**

Flags: `--socket` (required), `--symbol` (comma-separated filter, empty = all), `--depth` (default 20), `--shards` (0 = auto from `GOMAXPROCS-2`, clamped to `[1,8]`), `--metrics-addr` (default `127.0.0.1:9094`), `-v`, `--version`. No ClickHouse flags — persistence is a follow-on plan.

Wire: `NewMetrics` → `ServeHTTP` → N shards each with `go s.Run(ctx)` → `NewCoordinator` → `NewBot(socket, coordinator, metrics)` → `bot.Run(ctx)`, with SIGINT/SIGTERM cancelling the context. Model the structure on `go/marketbyorder-bot/main.go`, minus the ClickHouse client, `EventsWriter`, and `SnapshotWriter`.

- [ ] **Step 4: Write `README.md`**

Cover: what the bot does and that persistence is not yet implemented; the socket input and record envelope it expects (link to the parser's README); the state machine's three statuses and how an instrument moves between them; the five feed-specific behaviors from the design spec, each in a sentence or two — the `Last Instrument Seq` discriminator, shadow commit, depth bound, crossed-book monitoring, and the bounded delta buffer with its eviction policy; the full metric list with one line each; and the `--depth` / `--shards` flags. State plainly that `CumulativeQty` is exhaustive only when the depth bound is `0`.

- [ ] **Step 5: Full verification**

From `go/`:
```bash
gofmt -l ./marketbyprice-bot/ && go vet ./marketbyprice-bot/... && go test ./marketbyprice-bot/... && go test -race ./marketbyprice-bot/... && go build -o /tmp/dz-marketbyprice-bot ./marketbyprice-bot/ && /tmp/dz-marketbyprice-bot --version
```

Confirm the module stands alone, from `go/marketbyprice-bot`:
```bash
GOWORK=off go build -o /dev/null . && GOWORK=off GOOS=linux go build -o /dev/null .
```

Confirm the other modules still pass, from `go/`:
```bash
for m in marketbyorder-bot marketbyorder-parser marketbyprice-parser internal kernel-receiver topofbook-bot topofbook-parser; do (cd $m && go vet ./... && go test ./... >/dev/null && echo "$m ok"); done
```
Expected: seven `ok` lines. `xdp-receiver` is excluded on purpose (pre-existing breakage).

- [ ] **Step 6: Commit**

```bash
git add go/marketbyprice-bot/
git commit -m "marketbyprice-bot: add level read-out, entry point, and readme"
```

---

## Done criteria

- The engine maintains a correct price-keyed book from snapshot + delta input, with all 12 `instrument_test.go` behaviors passing.
- All five design-spec behaviors are implemented and individually tested: the `Last Instrument Seq` discriminator, shadow commit that never demotes a ready book, depth bound defaulting to unknown, crossed-book monitoring at consistency points, and the bounded delta buffer with largest-buffer eviction.
- `snapshot_level` routing is keyed on the open group, never on `snapshot_id`, with a regression test that a `{channel, snapshot_id}` route would fail.
- `go test -race` is clean.
- The module is in `go.work`, in the CI matrix, and in all five sibling Dockerfiles, and builds standalone with `GOWORK=off` for darwin and linux.
- No ClickHouse code anywhere in the module.

## Follow-on plan (not this plan)

**Persistence** — `clickhouse.go`, `events_writer.go`, `snapshot_writer.go` with its coalescing goroutine, the metrics those add, and `demo/clickhouse/init/03_schema_mbp.sql` with the five tables from the design spec's Component 3. Then the demo stack: compose services, `.env.example`, Prometheus jobs, Grafana dashboard, and the `docs/hyperliquid.md` port table — the last of which needs the live feed's group, port sets, and channel ID.
