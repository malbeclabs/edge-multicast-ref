package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func newTestShard(t *testing.T) *Shard {
	t.Helper()
	return NewShard(0, 1, nil)
}

// levelUpdateRec builds a level_update the way the parser emits one: JSON
// numbers arrive as float64, side/action/reason as strings.
func levelUpdateRec(instID uint32, mktSeq uint64, piSeq uint32, side string, priceRaw int64, qtyRaw uint64) Record {
	return Record{
		Type:           "level_update",
		Port:           "mktdata",
		SequenceNumber: mktSeq,
		InstrumentID:   instID,
		Fields: map[string]any{
			"side":               side,
			"action":             "new",
			"per_instrument_seq": float64(piSeq),
			"price_raw":          float64(priceRaw),
			"qty_raw":            float64(qtyRaw),
			"update_reason":      "new_order",
			"level_flags":        float64(0),
			"order_count":        float64(1),
		},
	}
}

func readyInstrumentInShard(t *testing.T, s *Shard, k instKey, lastPiSeq uint32) *Instrument {
	t.Helper()
	inst := NewInstrument(k.id, "SYM", 0, 0)
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = lastPiSeq
	s.instruments[k] = inst
	return inst
}

func TestApplyDelta_ContiguousApplies(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	evs := s.applyDelta(k, levelUpdateRec(11, 900, 6, "bid", 1000, 50))
	if len(evs) != 1 || evs[0].Kind != "applied_delta" {
		t.Fatalf("events: %+v", evs)
	}
	if inst.LastAppliedInstrumentSeq != 6 || inst.LastAppliedMktdataSeq != 900 {
		t.Errorf("trackers: %d %d", inst.LastAppliedInstrumentSeq, inst.LastAppliedMktdataSeq)
	}
	if inst.Bids[1000] == nil || inst.Bids[1000].QtyRaw != 50 {
		t.Errorf("book: %+v", inst.Bids)
	}
}

func TestApplyDelta_DuplicateDiscardedSilently(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	for _, piSeq := range []uint32{5, 3, 1} {
		if evs := s.applyDelta(k, levelUpdateRec(11, 900, piSeq, "bid", 1000, 50)); len(evs) != 0 {
			t.Errorf("piSeq %d must produce no events, got %+v", piSeq, evs)
		}
	}
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("tracker must not move: %d", inst.LastAppliedInstrumentSeq)
	}
	if len(inst.Bids) != 0 {
		t.Errorf("book must not change: %+v", inst.Bids)
	}
	if len(s.deltaBuf[k]) != 0 {
		t.Errorf("duplicates must not be buffered: %+v", s.deltaBuf[k])
	}
}

func TestApplyDelta_ReorderWithinWindowHeldThenDrained(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// 8 and 7 arrive before 6: held, nothing applied.
	if evs := s.applyDelta(k, levelUpdateRec(11, 902, 8, "bid", 1200, 8)); len(evs) != 0 {
		t.Fatalf("seq 8 should be held: %+v", evs)
	}
	if evs := s.applyDelta(k, levelUpdateRec(11, 901, 7, "bid", 1100, 7)); len(evs) != 0 {
		t.Fatalf("seq 7 should be held: %+v", evs)
	}
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Fatalf("nothing should have applied yet: %d", inst.LastAppliedInstrumentSeq)
	}
	// 6 fills the hole: 6, 7, 8 all apply in order.
	evs := s.applyDelta(k, levelUpdateRec(11, 900, 6, "bid", 1000, 6))
	if len(evs) != 3 {
		t.Fatalf("expected 3 events (6,7,8), got %d: %+v", len(evs), evs)
	}
	if inst.LastAppliedInstrumentSeq != 8 {
		t.Errorf("tracker: got %d want 8", inst.LastAppliedInstrumentSeq)
	}
	if inst.Pending != nil {
		t.Errorf("pending should be drained: %+v", inst.Pending)
	}
	for price, wantQty := range map[int64]uint64{1000: 6, 1100: 7, 1200: 8} {
		if inst.Bids[price] == nil || inst.Bids[price].QtyRaw != wantQty {
			t.Errorf("level %d: %+v", price, inst.Bids[price])
		}
	}
}

func TestApplyDelta_GapBeyondWindowDemotes(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	far := 5 + reorderWindow + 2
	evs := s.applyDelta(k, levelUpdateRec(11, 999, uint32(far), "bid", 1000, 50))
	if len(evs) != 1 || evs[0].Kind != "per_instrument_gap" {
		t.Fatalf("expected a per_instrument_gap event, got %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Errorf("status: got %v want gap", inst.Status)
	}
	if inst.Pending != nil {
		t.Errorf("pending must be dropped on a real gap: %+v", inst.Pending)
	}
	if len(s.deltaBuf[k]) != 1 {
		t.Errorf("the triggering delta must be buffered: %+v", s.deltaBuf[k])
	}
}

func TestApplyDelta_NotReadyBuffers(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := NewInstrument(11, "SYM", 0, 0) // awaiting-snapshot
	s.instruments[k] = inst

	if evs := s.applyDelta(k, levelUpdateRec(11, 900, 1, "bid", 1000, 50)); len(evs) != 0 {
		t.Fatalf("events: %+v", evs)
	}
	if len(s.deltaBuf[k]) != 1 {
		t.Errorf("delta must be buffered: %+v", s.deltaBuf[k])
	}
	if len(inst.Bids) != 0 {
		t.Errorf("book must be untouched: %+v", inst.Bids)
	}
}

func TestApplyDelta_UnknownInstrumentBuffers(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 42}
	if evs := s.applyDelta(k, levelUpdateRec(42, 900, 1, "bid", 1000, 50)); len(evs) != 0 {
		t.Fatalf("events: %+v", evs)
	}
	if len(s.deltaBuf[k]) != 1 {
		t.Errorf("awaiting-refdata delta must be buffered: %+v", s.deltaBuf[k])
	}
}

func TestReplayBuffer_SkipsAtOrBelowAnchor(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := NewInstrument(11, "SYM", 0, 0)
	s.instruments[k] = inst

	// Buffer four deltas spanning the anchor.
	for i, piSeq := range []uint32{1, 2, 3, 4} {
		s.bufferDelta(k, levelUpdateRec(11, uint64(500+i), piSeq, "bid", int64(1000+i), uint64(10+i)))
	}
	// Snapshot lands with anchor 501, last_instrument_seq 2.
	inst.BeginSnapshot(1, 501, 0, 2, 0)
	if err := inst.EndSnapshot(1, 501); err != nil {
		t.Fatal(err)
	}
	s.replayBuffer(k, inst)

	// mktdata seqs 500 and 501 are covered by the anchor; 502 and 503 replay.
	if inst.LastAppliedInstrumentSeq != 4 {
		t.Errorf("tracker: got %d want 4", inst.LastAppliedInstrumentSeq)
	}
	if inst.Bids[1000] != nil || inst.Bids[1001] != nil {
		t.Errorf("pre-anchor deltas must not replay: %+v", inst.Bids)
	}
	if inst.Bids[1002] == nil || inst.Bids[1003] == nil {
		t.Errorf("post-anchor deltas must replay: %+v", inst.Bids)
	}
	if _, present := s.deltaBuf[k]; present {
		t.Error("buffer entry should be deleted after replay")
	}
	if s.bufferedN != 0 {
		t.Errorf("bufferedN: got %d want 0", s.bufferedN)
	}
}

func TestDeltaBuffer_OverflowEvictsLargestAndMarksGap(t *testing.T) {
	s := newTestShard(t)
	s.maxBuffered = 10

	big := instKey{0, 1}
	small := instKey{0, 2}
	bigInst := NewInstrument(1, "BIG", 0, 0)
	smallInst := NewInstrument(2, "SMALL", 0, 0)
	s.instruments[big] = bigInst
	s.instruments[small] = smallInst

	for i := 0; i < 8; i++ {
		s.bufferDelta(big, levelUpdateRec(1, uint64(i), uint32(i+1), "bid", 1000, 5))
	}
	for i := 0; i < 2; i++ {
		s.bufferDelta(small, levelUpdateRec(2, uint64(i), uint32(i+1), "bid", 1000, 5))
	}
	if s.bufferedN != 10 {
		t.Fatalf("setup: bufferedN got %d want 10", s.bufferedN)
	}
	if bigInst.Status == StatusGap {
		t.Fatal("no eviction should have happened yet")
	}

	// One more record trips the budget.
	s.bufferDelta(small, levelUpdateRec(2, 99, 3, "bid", 1000, 5))

	if _, present := s.deltaBuf[big]; present {
		t.Error("the largest buffer must be evicted")
	}
	if bigInst.Status != StatusGap {
		t.Errorf("evicted instrument must be marked gap, got %v", bigInst.Status)
	}
	if len(s.deltaBuf[small]) != 3 {
		t.Errorf("the smaller buffer must survive intact: %+v", s.deltaBuf[small])
	}
	if smallInst.Status == StatusGap {
		t.Error("the surviving instrument must not be marked gap")
	}
	if s.bufferedN != 3 {
		t.Errorf("bufferedN after eviction: got %d want 3", s.bufferedN)
	}
}

// The parser omits order_count when the wire carried 0xFFFF, so an absent key
// must map back to the sentinel rather than to 0, which is a real count.
func TestOrderCountFrom_AbsentMeansSentinel(t *testing.T) {
	if got := orderCountFrom(map[string]any{}); got != u16Unavailable {
		t.Errorf("absent: got %d want %d", got, u16Unavailable)
	}
	if got := orderCountFrom(map[string]any{"order_count": float64(0)}); got != 0 {
		t.Errorf("explicit 0 is a real count: got %d", got)
	}
	if got := orderCountFrom(map[string]any{"order_count": float64(7)}); got != 7 {
		t.Errorf("got %d want 7", got)
	}
}

// A malformed BookClear must not advance the sequence trackers, because nothing
// was applied — otherwise the next delta is classified against a wrong expected
// seq and a real gap goes undetected.
func TestApplyOne_MalformedBookClearDoesNotAdvanceTrackers(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)

	bad := Record{
		Type:           "book_clear",
		Port:           "mktdata",
		SequenceNumber: 900,
		InstrumentID:   11,
		Fields: map[string]any{
			"clear_side":         "both",
			"scope":              "from_price", // malformed with clear_side=both
			"per_instrument_seq": float64(6),
			"from_price_raw":     float64(1000),
			"clear_reason":       "halt",
		},
	}
	s.applyDelta(k, bad)

	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("trackers must not advance on a malformed message: got %d want 5", inst.LastAppliedInstrumentSeq)
	}
	if inst.Bids[1000] == nil {
		t.Error("book must be untouched")
	}
}

// A hole discovered mid-replay must declare exactly ONE gap, not one per
// remaining record. Without a status re-check inside replayBuffer's loop, every
// trailing entry re-enters applyDeltaToReady and re-declares the same gap,
// inflating the operator-facing counter by the size of the backlog.
func TestReplayBuffer_MidReplayGapDeclaredOnce(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	k := instKey{0, 11}
	inst := NewInstrument(11, "SYM", 0, 0)
	s.instruments[k] = inst

	// Buffer a contiguous run, then a run that skips well past the reorder
	// window, so the hole can never fill.
	for i := 0; i < 3; i++ {
		s.bufferDelta(k, levelUpdateRec(11, uint64(600+i), uint32(101+i), "bid", int64(1000+i), 5))
	}
	for i := 0; i < 20; i++ {
		s.bufferDelta(k, levelUpdateRec(11, uint64(700+i), uint32(200+i), "bid", int64(2000+i), 5))
	}

	// Snapshot lands at anchor 599 / last_instrument_seq 100, so everything replays.
	inst.BeginSnapshot(1, 599, 0, 100, 0)
	if err := inst.EndSnapshot(1, 599); err != nil {
		t.Fatal(err)
	}
	s.replayBuffer(k, inst)

	if inst.Status != StatusGap {
		t.Fatalf("a hole in the replayed run must gap the instrument, got %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("exactly one gap should be declared for one hole: got %v want 1", got)
	}
	// The contiguous prefix applied; the post-hole backlog is buffered for the
	// next snapshot rather than discarded.
	if inst.LastAppliedInstrumentSeq != 103 {
		t.Errorf("prefix should have applied through 103, got %d", inst.LastAppliedInstrumentSeq)
	}
	if len(s.deltaBuf[k]) == 0 {
		t.Error("post-gap backlog must be re-buffered for the next snapshot")
	}
	if s.bufferedN != len(s.deltaBuf[k]) {
		t.Errorf("bufferedN %d must match actual buffered records %d", s.bufferedN, len(s.deltaBuf[k]))
	}
}

// Build Pending right up to the reorder window, then exceed it, to pin the
// boundary rather than only the single-shot far jump.
func TestApplyDelta_ReorderWindowBoundary(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// Fill Pending to exactly reorderWindow entries, all within the distance
	// bound. Nothing should apply and no gap should be declared.
	for i := 0; i < reorderWindow; i++ {
		piSeq := uint32(7 + i) // 6 is the hole; 7..22 held
		s.applyDelta(k, levelUpdateRec(11, uint64(900+i), piSeq, "bid", int64(3000+i), 5))
	}
	if inst.Status != StatusReady {
		t.Fatalf("at the window boundary the instrument must stay ready, got %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Fatalf("no gap should be declared at the boundary: got %v", got)
	}
	if len(inst.Pending) != reorderWindow {
		t.Fatalf("Pending should hold %d entries, got %d", reorderWindow, len(inst.Pending))
	}

	// One more held record exceeds the count bound and declares the gap.
	s.applyDelta(k, levelUpdateRec(11, 999, uint32(7+reorderWindow), "bid", 4000, 5))
	if inst.Status != StatusGap {
		t.Errorf("exceeding the window must gap the instrument, got %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("exactly one gap: got %v want 1", got)
	}
	if inst.Pending != nil {
		t.Error("Pending must be dropped when the window is exceeded")
	}
}

// A snapshot commit must free the reorder-window budget. Pending entries held
// against the pre-snapshot sequence can never drain once the snapshot jumps
// LastAppliedInstrumentSeq forward, so if the commit leaks them they sit there
// consuming the window and the next ordinary reorder is misread as a gap —
// demoting a healthy instrument and inflating per_instrument_gaps_total, the
// counter an operator uses to judge feed loss.
func TestApplyDelta_SnapshotCommitFreesReorderBudget(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// Fill Pending to the window bound behind a hole at 6.
	for i := 0; i < reorderWindow; i++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(900+i), uint32(7+i), "bid", int64(3000+i), 5))
	}
	if len(inst.Pending) != reorderWindow {
		t.Fatalf("setup: Pending should hold %d, got %d", reorderWindow, len(inst.Pending))
	}

	// A snapshot arrives and commits, carrying the instrument well past the hole.
	inst.BeginSnapshot(1, 5000, 0, 77, 0)
	if err := inst.EndSnapshot(1, 5000); err != nil {
		t.Fatalf("setup: snapshot commit: %v", err)
	}

	// One out-of-order delta, comfortably inside the window (expected 78, got 80).
	// This must be held, not treated as a gap.
	evs := s.applyDelta(k, levelUpdateRec(11, 1000, 80, "bid", 4000, 5))

	for _, e := range evs {
		if e.Kind == "per_instrument_gap" {
			t.Fatal("a reorder within the window must not declare a gap after a snapshot commit")
		}
	}
	if inst.Status != StatusReady {
		t.Errorf("instrument must stay ready, got %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("no gap should be counted: got %v want 0", got)
	}
	if len(inst.Pending) != 1 {
		t.Errorf("Pending should hold only the new record, got %d", len(inst.Pending))
	}
}

// price_raw is an int64 on the wire. Decoding Fields as plain map[string]any
// makes every number a float64, which silently truncates above 2^53 — the book
// then holds a price the venue never quoted, with no error and no counter. The
// reader decodes with UseNumber so the literal text survives to the coercion
// helpers. This drives the real decode path, not a hand-built Fields map.
func TestCoercion_LargeIntegersSurviveDecode(t *testing.T) {
	const bigPrice int64 = 1000000000000000001 // 2^53 < this; not representable as float64
	const bigQty uint64 = 18446744073709551615 // math.MaxUint64
	const bigSeq uint64 = 9007199254740993     // 2^53 + 1

	line := []byte(`{"type":"level_update","port":"mktdata","fields":{` +
		`"price_raw":1000000000000000001,` +
		`"qty_raw":18446744073709551615,` +
		`"per_instrument_seq":4294967295,` +
		`"anchor_seq":9007199254740993,` +
		`"price_exponent":-8,` +
		`"side":"bid"}}`)

	var rec Record
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	if err := dec.Decode(&rec); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got := toInt64(rec.Fields["price_raw"]); got != bigPrice {
		t.Errorf("price_raw: got %d want %d (diff %d)", got, bigPrice, got-bigPrice)
	}
	if got := toUint64(rec.Fields["qty_raw"]); got != bigQty {
		t.Errorf("qty_raw: got %d want %d", got, bigQty)
	}
	if got := toUint64(rec.Fields["anchor_seq"]); got != bigSeq {
		t.Errorf("anchor_seq: got %d want %d", got, bigSeq)
	}
	if got := toUint32(rec.Fields["per_instrument_seq"]); got != 4294967295 {
		t.Errorf("per_instrument_seq: got %d want 4294967295", got)
	}
	if got := toInt8(rec.Fields["price_exponent"]); got != -8 {
		t.Errorf("price_exponent: got %d want -8", got)
	}
	if got := toString(rec.Fields["side"]); got != "bid" {
		t.Errorf("side: got %q want \"bid\"", got)
	}
}

// The float64 path must keep working: every other test builds Fields directly.
func TestCoercion_Float64PathUnchanged(t *testing.T) {
	fields := map[string]any{
		"price_raw":          float64(-1500),
		"qty_raw":            float64(250),
		"per_instrument_seq": float64(7),
		"price_exponent":     float64(-2),
		"level_flags":        float64(3),
	}
	if got := toInt64(fields["price_raw"]); got != -1500 {
		t.Errorf("price_raw: got %d want -1500", got)
	}
	if got := toUint64(fields["qty_raw"]); got != 250 {
		t.Errorf("qty_raw: got %d want 250", got)
	}
	if got := toUint32(fields["per_instrument_seq"]); got != 7 {
		t.Errorf("per_instrument_seq: got %d want 7", got)
	}
	if got := toInt8(fields["price_exponent"]); got != -2 {
		t.Errorf("price_exponent: got %d want -2", got)
	}
	if got := toUint8(fields["level_flags"]); got != 3 {
		t.Errorf("level_flags: got %d want 3", got)
	}
}

// A snapshot whose Last Instrument Seq is far ahead of reality commits, sets the
// tracker high, and then silently swallows every real delta while every later
// snapshot is declined as current — a frozen book that still reads as ready.
// The discard counter is the only thing that makes that state visible.
func TestApplyDeltaToReady_StaleSeqDiscardIsCounted(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// A snapshot has pushed the tracker far past the live feed.
	inst.LastAppliedInstrumentSeq = 10000

	for i := 0; i < 3; i++ {
		evs := s.applyDelta(k, levelUpdateRec(11, uint64(900+i), uint32(6+i), "bid", 1000, 5))
		if len(evs) != 0 {
			t.Fatalf("a stale delta must not produce events: %+v", evs)
		}
	}

	if inst.Status != StatusReady {
		t.Errorf("the instrument still reads as ready, which is the trap: %v", inst.Status)
	}
	if got := counterValue(m.DeltasDiscardedTotal.WithLabelValues("stale_seq")); got != 3 {
		t.Errorf("stale_seq discards: got %v want 3", got)
	}
}

// bufferDelta keeps its buffer sorted by mktdata seq through insertion rather
// than a per-append re-sort. replayBuffer depends on that ordering to drop
// everything the snapshot anchor already covers and replay the rest in order, so
// the invariant is worth pinning directly — including the out-of-order path,
// which is the only one that still moves elements.
func TestBufferDelta_StaysSortedOnOutOfOrderArrival(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}

	// Deliberately jumbled, including a duplicate seq and one that sorts first.
	for i, seq := range []uint64{100, 300, 200, 700, 50, 500, 200, 400} {
		s.bufferDelta(k, levelUpdateRec(11, seq, uint32(i+1), "bid", 1000, 5))
	}

	buf := s.deltaBuf[k]
	if len(buf) != 8 || s.bufferedN != 8 {
		t.Fatalf("all records must be retained: len=%d bufferedN=%d", len(buf), s.bufferedN)
	}
	for i := 1; i < len(buf); i++ {
		if buf[i-1].MktdataSeq > buf[i].MktdataSeq {
			t.Fatalf("buffer out of order at %d: %v", i, seqsOf(buf))
		}
	}
	want := []uint64{50, 100, 200, 200, 300, 400, 500, 700}
	got := seqsOf(buf)
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("buffer order: got %v want %v", got, want)
		}
	}
}

func seqsOf(buf []BufferedDelta) []uint64 {
	out := make([]uint64, len(buf))
	for i, b := range buf {
		out[i] = b.MktdataSeq
	}
	return out
}

// bookClearRec builds a book_clear the way the parser emits one.
func bookClearRec(instID uint32, mktSeq uint64, piSeq uint32, clearSide, scope string, fromPriceRaw int64) Record {
	return Record{
		Type:           "book_clear",
		Port:           "mktdata",
		SequenceNumber: mktSeq,
		InstrumentID:   instID,
		Fields: map[string]any{
			"clear_side":         clearSide,
			"scope":              scope,
			"per_instrument_seq": float64(piSeq),
			"from_price_raw":     float64(fromPriceRaw),
			"clear_reason":       "halt",
		},
	}
}

// A discarded BookClear must not report itself as applied. The persistence layer
// keys off Kind, and "applied_delta" here would record a mutation the book never
// saw. Scope=1 with ClearSide=both is the malformed case: one price cannot bound
// both sides.
func TestApplyOne_MalformedBookClearReportsDistinctKind(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.Bids[900] = &LevelState{QtyRaw: 7}
	inst.Asks[1100] = &LevelState{QtyRaw: 7}

	evs := s.applyDelta(k, bookClearRec(11, 900, 6, "both", "from_price", 1000))

	if len(evs) != 1 {
		t.Fatalf("events: %+v", evs)
	}
	if evs[0].Kind != "malformed_delta" {
		t.Errorf("kind: got %q want \"malformed_delta\"", evs[0].Kind)
	}
	// Nothing applied: the book is untouched and the trackers have not advanced,
	// so the next delta is still classified against the correct expected seq.
	if inst.Bids[900] == nil || inst.Asks[1100] == nil {
		t.Error("a discarded book_clear must not mutate the book")
	}
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("trackers must not advance on a discard: got %d want 5", inst.LastAppliedInstrumentSeq)
	}
}

// evictLargestBuffer must survive a victim that has buffered deltas but no
// Instrument yet — the awaiting-refdata case, where deltas arrive before the
// definition that would create it.
func TestEvictLargestBuffer_VictimAbsentFromInstruments(t *testing.T) {
	s := newTestShard(t)
	s.maxBuffered = 2
	k := instKey{0, 42} // deliberately never added to s.instruments

	for i := 0; i < 3; i++ {
		s.bufferDelta(k, levelUpdateRec(42, uint64(i), uint32(i+1), "bid", 1000, 5))
	}

	if _, ok := s.instruments[k]; ok {
		t.Fatal("fixture: the victim must have no Instrument")
	}
	if _, ok := s.deltaBuf[k]; ok {
		t.Error("the overflowing buffer should have been evicted")
	}
	if s.bufferedN != 0 {
		t.Errorf("bufferedN must track the eviction: got %d want 0", s.bufferedN)
	}
}

// When the reorder window is exceeded the instrument goes gap and Pending is
// dropped. Those dropped records are not lost: each was already buffered or is
// superseded by the snapshot anchor, so the recovery snapshot plus replay
// restores the book. This pins that reasoning to an executable check.
func TestApplyDeltaToReady_PendingDroppedAtGapIsCoveredByAnchor(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// A record far beyond the reorder window: straight to gap.
	evs := s.applyDelta(k, levelUpdateRec(11, 900, 5+reorderWindow+2, "bid", 1000, 50))
	if len(evs) != 1 || evs[0].Kind != "per_instrument_gap" {
		t.Fatalf("expected a gap event: %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Fatalf("status: %v", inst.Status)
	}
	if inst.Pending != nil {
		t.Error("Pending must be dropped when the window is exceeded")
	}
	// The triggering record was buffered rather than discarded.
	if len(s.deltaBuf[k]) != 1 {
		t.Fatalf("the gap record must be buffered: %+v", s.deltaBuf[k])
	}

	// A snapshot anchored at or past that record supersedes the buffer entirely:
	// replay drops everything at or below the anchor, leaving the snapshot's book.
	inst.Status = StatusReady
	inst.LastAppliedMktdataSeq = 900
	inst.LastAppliedInstrumentSeq = 5 + reorderWindow + 2
	s.replayBuffer(k, inst)

	if s.bufferedN != 0 || len(s.deltaBuf[k]) != 0 {
		t.Errorf("the anchor must cover the buffered record: bufferedN=%d buf=%+v", s.bufferedN, s.deltaBuf[k])
	}
	if inst.Status != StatusReady {
		t.Errorf("replay must not re-declare a gap: %v", inst.Status)
	}
}
