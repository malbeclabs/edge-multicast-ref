package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func newTestShard(t *testing.T) *Shard {
	t.Helper()
	return NewShard(0, 1, NewEventsWriter(nil), nil)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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

// An eviction whose victim has no Instrument yet must still record the hole.
//
// applyDelta buffers awaiting-refdata deltas under a key with no instrument,
// and at cold start those buffers are the largest, so they are the likeliest
// victims. Skipping them recorded nothing: when the definition landed, the
// fresh instrument had no requirement and took a snapshot from inside the very
// range this shard had chosen to discard — a self-inflicted gap, uncounted.
func TestEvictLargestBuffer_VictimAbsentFromInstruments(t *testing.T) {
	s := newTestShard(t)
	s.maxBuffered = 2
	k := instKey{0, 42} // never added to s.instruments before the eviction

	for i := 0; i < 3; i++ {
		s.bufferDelta(k, levelUpdateRec(42, uint64(i), uint32(i+1), "bid", 1000, 5))
	}

	if _, ok := s.deltaBuf[k]; ok {
		t.Error("the overflowing buffer should have been evicted")
	}
	if s.bufferedN != 0 {
		t.Errorf("bufferedN must track the eviction: got %d want 0", s.bufferedN)
	}

	// The eviction creates the instrument so it has somewhere to record the
	// hole, the way applyInstrumentReset does.
	inst, ok := s.instruments[k]
	if !ok {
		t.Fatal("the eviction must create the instrument to record the hole on")
	}
	if inst.Status != StatusGap {
		t.Errorf("an evicted instrument is gapped: got %v", inst.Status)
	}
	if inst.RequiredInstrumentSeq == nil {
		t.Fatal("the eviction must record the highest discarded seq")
	}
	// Three deltas at per-instrument seq 1..3 were discarded, so a snapshot has
	// to reach 3 — not 1, and not the book's own next-expected.
	if got := *inst.RequiredInstrumentSeq; got != 3 {
		t.Errorf("required seq: got %d want 3", got)
	}

	// And a snapshot from inside the discarded range is refused.
	inst.BeginSnapshot(1, 100, 0, 2, 0)
	if err := inst.EndSnapshot(1, 100); err == nil {
		t.Error("a snapshot from inside the discarded range must not commit")
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

// Only a real book mutation may dirty an instrument for snapshotting. A
// non-mutating kind marking the book dirty would rewrite an unchanged book on
// every batch boundary and every instrument definition.
func TestHandle_OnlyMutatingEventsMarkDirty(t *testing.T) {
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[k])
	}, nil)

	// instrument_definition and batch_boundary are non-mutating.
	s.handle(instDefRec(11, "SYM", 1))
	s.handle(Record{Type: "batch_boundary", Port: "mktdata", Fields: map[string]any{}})

	s.sw.mu.Lock()
	n := len(s.sw.dirty)
	s.sw.mu.Unlock()
	if n != 0 {
		t.Fatalf("non-mutating events must not dirty a book, got %d entries", n)
	}

	// An applied delta must.
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 1000, 50))

	s.sw.mu.Lock()
	n = len(s.sw.dirty)
	_, present := s.sw.dirty[instKey{0, 11}]
	s.sw.mu.Unlock()
	if n != 1 || !present {
		t.Errorf("an applied delta must dirty its instrument: n=%d present=%v", n, present)
	}
}

// Events reach the writer with the instrument's symbol and exponents attached,
// which is what lets the writer scale raw prices at the persistence boundary.
func TestHandle_WritesEventsWithRefdata(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) }, nil)

	s.handle(instDefRec(11, "BTC-USDT", 1)) // price_exponent -2, qty_exponent -8
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 123456, 500))

	rows := st.rows["events"]
	if len(rows) != 1 {
		t.Fatalf("expected one events row, got %d", len(rows))
	}
	if rows[0]["symbol"] != "BTC-USDT" {
		t.Errorf("symbol must come from refdata: %v", rows[0]["symbol"])
	}
	if got := rows[0]["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled by the instrument exponent: got %v", got)
	}
}

// A socket reconnect must NOT reset the snapshot writer. OnDisconnect clears
// in-flight shadows because a half-built shadow spans the break, but live books
// stay valid and keep being served, so pending dirty entries still point at real
// state. Resetting here would discard queued writes for books that never changed.
// This asserts a deliberate absence, which is exactly the kind of decision that
// regresses silently when someone later "tidies up" the disconnect path.
func TestOnDisconnect_DoesNotResetSnapshotWriter(t *testing.T) {
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		fn(s.instruments[k])
	}, nil)
	c := NewCoordinator(context.Background(), []*Shard{s}, NewEventsWriter(nil), m)

	s.sw.MarkDirty(instKey{0, 11})
	c.OnDisconnect()

	s.sw.mu.Lock()
	n := len(s.sw.dirty)
	s.sw.mu.Unlock()
	if n != 1 {
		t.Errorf("a disconnect must leave queued snapshot work intact, got %d entries", n)
	}
}

// Snapshot levels are captured for replay even when the snapshot was declined.
func TestHandle_CapturesWireLevelsForDeclinedSnapshot(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) }, nil)

	s.handle(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	s.handle(snapBeginRec(11, 4, 2, 100, 0, 9999)) // declined: K == tracker
	if inst.OpenSnapshot != nil {
		t.Fatal("setup: the snapshot should have been declined")
	}
	s.handle(snapLevelRec(11, 4, "bid", 1000, 5))

	if got := len(st.rows["wire_levels"]); got != 1 {
		t.Errorf("a declined snapshot's levels must still be captured, got %d rows", got)
	}
}

func TestParseSymbolFilter(t *testing.T) {
	if got := parseSymbolFilter(""); got != nil {
		t.Errorf("an empty filter means no filter, got %v", got)
	}
	got := parseSymbolFilter(" BTC-USDT , ETH-USDT ")
	if len(got) != 2 {
		t.Fatalf("expected 2 symbols, got %v", got)
	}
	if _, ok := got["BTC-USDT"]; !ok {
		t.Error("BTC-USDT missing; entries must be trimmed")
	}
}

// A filtered symbol must be absent from every table, while the book engine still
// applies its deltas — sequencing and gap detection are only correct if every
// record is processed.
func TestSymbolFilter_GatesPersistenceNotTheEngine(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.symbols = parseSymbolFilter("WANTED")
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) }, nil)

	// An instrument that is NOT in the filter.
	s.handle(instDefRec(11, "IGNORED", 1))
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.handle(levelUpdateRec(11, 900, 1, "bid", 1000, 50))

	if got := len(st.rows["events"]); got != 0 {
		t.Errorf("a filtered symbol must not be persisted, got %d event rows", got)
	}
	if got := len(st.rows["instruments"]); got != 0 {
		t.Errorf("a filtered symbol's definition must not be persisted, got %d rows", got)
	}

	// The book engine must still have applied it.
	inst := s.instruments[instKey{0, 11}]
	if inst.LastAppliedInstrumentSeq != 1 {
		t.Errorf("the engine must still apply filtered instruments: seq %d", inst.LastAppliedInstrumentSeq)
	}
	if inst.Bids[1000] == nil {
		t.Error("the book must still be maintained for a filtered instrument")
	}

	// A wanted symbol still persists.
	s.handle(instDefRec(12, "WANTED", 1))
	s.instruments[instKey{0, 12}].Status = StatusReady
	s.handle(levelUpdateRec(12, 901, 1, "bid", 1000, 50))
	if got := len(st.rows["events"]); got != 1 {
		t.Errorf("an unfiltered symbol must persist, got %d event rows", got)
	}
}

// `events` is an applied-delta log. Two engine paths report a record that was
// deliberately NOT applied while carrying an ordinary delta Record.Type, so a
// writer that switches on Record.Type alone — as EventsWriter does — records a
// mutation the book never saw:
//
//   - per_instrument_gap carries Record.Type "level_update"; the record was
//     buffered for replay, not applied.
//   - malformed_delta carries Record.Type "book_clear"; nothing was applied and
//     the sequence trackers deliberately did not advance.
//
// The engine computes the Kind correctly, so handle must gate on it.
func TestHandle_UnappliedDeltaKindsAreNotPersisted(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) }, nil)

	s.handle(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 5

	// A genuinely applied delta DOES produce a row — the control for the two
	// negative assertions below.
	s.handle(levelUpdateRec(11, 900, 6, "bid", 1000, 50))
	if got := len(st.rows["events"]); got != 1 {
		t.Fatalf("an applied delta must produce one events row, got %d", got)
	}

	// A per-instrument gap: far beyond the reorder window, so the record is
	// buffered and the instrument demoted.
	s.handle(levelUpdateRec(11, 999, 6+reorderWindow+2, "bid", 1100, 50))
	if inst.Status != StatusGap {
		t.Fatalf("setup: the instrument should have gapped, got %v", inst.Status)
	}
	if got := len(st.rows["events"]); got != 1 {
		t.Errorf("a per-instrument gap buffers the record rather than applying it, so it must not "+
			"be persisted as an applied delta: got %d events rows want 1", got)
	}

	// A malformed book_clear: scope=from_price with clear_side=both, which
	// ApplyBookClear rejects without touching the book.
	inst.Status = StatusReady
	s.handle(bookClearRec(11, 1000, 7, "both", "from_price", 1000))
	if inst.LastAppliedInstrumentSeq != 6 {
		t.Fatalf("setup: a malformed book_clear must not advance the tracker, got %d", inst.LastAppliedInstrumentSeq)
	}
	if got := len(st.rows["events"]); got != 1 {
		t.Errorf("a malformed book_clear applied nothing and must not be persisted: got %d events rows want 1", got)
	}
}

// The --symbol filter must fail CLOSED. Every one of these three paths runs
// before the instrument's definition arrives — the ordinary cold-start ordering,
// since the refdata cycle lags mktdata — and each resolves an empty symbol. An
// empty symbol that persists is a filtered instrument leaking into ClickHouse
// under a blank symbol.
func TestSymbolFilter_FailsClosedBeforeRefdataArrives(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.symbols = parseSymbolFilter("WANTED")
	s.sw = NewSnapshotWriter(st, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[k])
	}, s.persists)

	// 1. instrument_reset before the definition. applyInstrumentReset creates the
	//    instrument through instrumentFor precisely so the required anchor is not
	//    lost, so refdataFor still returns a zero InstrumentDef.
	s.handle(Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{
		"reason": "venue_resync", "new_anchor_seq": float64(0),
	}})
	if got := len(st.rows["events"]); got != 0 {
		t.Errorf("an instrument_reset for a filtered instrument must not be persisted, got %d events rows", got)
	}

	// 2. snapshot_level capture, which reads s.refdata after instrumentFor has
	//    already created the instrument.
	s.handle(snapBeginRec(11, 3, 1, 0, 0, 5000))
	s.handle(snapLevelRec(11, 3, "bid", 1000, 10))
	if got := len(st.rows["wire_levels"]); got != 0 {
		t.Errorf("wire levels for a filtered instrument must not be persisted, got %d rows", got)
	}

	// 3. the snapshot writer's read-out, which reads inst.Symbol.
	s.handle(snapEndRec(11, 3, 5000))
	if s.instruments[instKey{0, 11}].Status != StatusReady {
		t.Fatal("setup: the snapshot should have committed, leaving a servable book")
	}
	s.sw.flushDue()
	if got := len(st.rows["level_snapshots"]); got != 0 {
		t.Errorf("a filtered instrument's read-out must not be persisted, got %d rows", got)
	}
}

// Channel-scoped records carry no symbol and must never be filtered out.
//
// The brief's version of this test called s.handle on a heartbeat directly
// against a Shard, but channel-scoped records never reach a shard at all:
// Coordinator.Dispatch handles heartbeat, manifest_summary and end_of_session
// itself and calls c.eventsW.Write directly (see coordinator.go). Shard.apply
// has no case for "heartbeat", so s.handle would produce zero events and this
// test would assert a false negative instead of exercising the filter.
//
// The Coordinator holds no symbol filter of its own — persists() lives on
// Shard — so the real assertion of the same intent is that a heartbeat
// dispatched through a Coordinator still produces exactly one channel_health
// row while a shard-level symbol filter is active.
func TestSymbolFilter_KeepsChannelScopedRecords(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.symbols = parseSymbolFilter("WANTED")
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) { fn(nil) }, nil)

	c := NewCoordinator(context.Background(), []*Shard{s}, NewEventsWriter(st), m)
	c.Dispatch(Record{Type: "heartbeat", Port: "mktdata", Fields: map[string]any{}})

	if got := len(st.rows["channel_health"]); got != 1 {
		t.Errorf("channel health must not be symbol-filtered, got %d rows", got)
	}
}

// malformedCount reads malformed_deltas_total for one reason.
func malformedCount(m *Metrics, reason string) float64 {
	return counterValue(m.MalformedDeltasTotal.WithLabelValues(reason))
}

// A malformed BookClear must gap its instrument ON THE MALFORMED MESSAGE, not
// after the reorder window, and must count as a publisher defect rather than as
// mktdata loss.
//
// The publisher consumed a Per-Instrument Seq for the message this book engine
// discards, so every later delta reads as a forward gap. Waiting the window out
// holds and discards ~reorderWindow deltas for a hole that can never fill and
// then reaches the same gap, and it lands the event in per_instrument_gaps_total
// — the counter an operator reads to judge feed loss.
func TestApplyDelta_MalformedBookClearGapsImmediately(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.Bids[900] = &LevelState{QtyRaw: 7}

	evs := s.applyDelta(k, bookClearRec(11, 900, 6, "both", "from_price", 1000))

	if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
		t.Fatalf("events: %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Fatalf("the malformed message itself must gap the instrument, got %v", inst.Status)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("malformed_deltas_total{bookclear_scope_side}: got %v want 1", got)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("a publisher defect must not read as mktdata loss: per_instrument_gaps_total = %v want 0", got)
	}
	if inst.Pending != nil {
		t.Error("Pending must be cleared on the demotion")
	}
	// The malformed record itself is worthless: buffering it would only re-demote
	// the instrument when the buffer replays.
	if len(s.deltaBuf[k]) != 0 || s.bufferedN != 0 {
		t.Errorf("the malformed record must not be buffered: %d records, bufferedN=%d", len(s.deltaBuf[k]), s.bufferedN)
	}
	// Nothing was applied, so the trackers stay where they were.
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("trackers must not advance: got %d want 5", inst.LastAppliedInstrumentSeq)
	}

	// Everything that follows is buffered for the recovery snapshot rather than
	// held in the reorder window. Feed more than a full window to prove the
	// instrument does not travel through it: no further gap is ever declared, and
	// nothing accumulates in Pending.
	for i := 0; i <= reorderWindow; i++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(901+i), uint32(7+i), "bid", int64(3000+i), 5))
	}
	if inst.Pending != nil {
		t.Errorf("a gapped instrument must buffer, not hold in the reorder window: Pending=%v", inst.Pending)
	}
	if got := len(s.deltaBuf[k]); got != reorderWindow+1 {
		t.Errorf("post-demotion deltas must be buffered for replay: got %d want %d", got, reorderWindow+1)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("no sequence gap may be declared after the demotion: got %v want 0", got)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("exactly one malformed delta: got %v want 1", got)
	}
}

// The arrival path must hand Pending to the delta buffer exactly as the drain
// path does. A held record is a valid, unapplied delta, and the recovery snapshot
// is not guaranteed to cover it: dropping it would leave replay staring at a hole
// and declare a per-instrument gap this book engine created itself. The map is cleared,
// not discarded — its records move.
//
// demoteMalformed ranges over a map, so the order it offers the records in is
// unspecified; the buffer must still come out ordered by mktdata seq, because
// replayBuffer's anchor filter reads it that way.
func TestApplyDelta_MalformedBookClearOnArrivalBuffersPending(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// 7, 8 and 9 arrive ahead of the hole at 6 and are held for reordering.
	for i := uint32(7); i <= 9; i++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(900+i), i, "bid", int64(2000+i), 5))
	}
	if inst.Status != StatusReady || len(inst.Pending) != 3 {
		t.Fatalf("setup: want ready with 3 held, got %v with %d", inst.Status, len(inst.Pending))
	}
	if s.bufferedN != 0 {
		t.Fatalf("setup: held records belong in Pending, not the buffer: bufferedN=%d", s.bufferedN)
	}

	// 6 is malformed, so the demotion happens on arrival, before any drain.
	s.applyDelta(k, bookClearRec(11, 900, 6, "both", "from_price", 1000))

	if inst.Status != StatusGap {
		t.Fatalf("status: got %v want gap", inst.Status)
	}
	if inst.Pending != nil {
		t.Error("Pending must be cleared on the demotion")
	}
	buf := s.deltaBuf[k]
	if len(buf) != 3 || s.bufferedN != 3 {
		t.Fatalf("the held valid deltas must move to the buffer, got %d records bufferedN=%d", len(buf), s.bufferedN)
	}
	for i, b := range buf {
		if got := toUint32(b.Record.Fields["per_instrument_seq"]); got != uint32(7+i) {
			t.Errorf("buffer must stay ordered by mktdata seq: index %d per_instrument_seq %d want %d", i, got, 7+i)
		}
		if b.Record.Type == "book_clear" {
			t.Errorf("the malformed record must not be buffered: index %d", i)
		}
	}
}

// A demotion taken on receipt, before the hole ahead of the malformed record
// fills, must still leave the instrument recoverable by one snapshot: every
// record that arrives after it is buffered for replay, including the ones that
// fill the hole, so recovery declares no gap of its own.
//
// This is what the malformed record owes the deltas around it. Nothing may be
// applied on top of a book that lost a mutation, so the records behind it cannot
// be applied now; and a recovery snapshot is not guaranteed to cover them, so
// they cannot be dropped either.
func TestApplyDelta_MalformedBookClearAheadOfExpectedRecoversFromSnapshot(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// 7 is malformed and arrives while 6 is still missing: demoted on receipt.
	evs := s.applyDelta(k, bookClearRec(11, 901, 7, "both", "from_price", 1000))
	if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
		t.Fatalf("want one malformed_delta, got %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Fatalf("status: got %v want gap", inst.Status)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("malformed_deltas_total{bookclear_scope_side}: got %v want 1", got)
	}
	// Nothing was applied, so the trackers stay where they were: 7 never reached
	// the book, and neither did anything ahead of it.
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("trackers must not advance: got %d want 5", inst.LastAppliedInstrumentSeq)
	}

	// 8 arrives behind the malformed record, and 6 finally fills the hole. Both
	// are buffered: the instrument is gapped, so neither may touch the book.
	s.applyDelta(k, levelUpdateRec(11, 902, 8, "bid", 2000, 5))
	s.applyDelta(k, levelUpdateRec(11, 900, 6, "bid", 1000, 5))

	buf := s.deltaBuf[k]
	if len(buf) != 2 || s.bufferedN != 2 {
		t.Fatalf("both valid deltas must be buffered for replay, got %d records bufferedN=%d", len(buf), s.bufferedN)
	}
	if buf[0].MktdataSeq != 900 || buf[1].MktdataSeq != 902 {
		t.Errorf("the buffer must stay ordered by mktdata seq, got %d then %d", buf[0].MktdataSeq, buf[1].MktdataSeq)
	}
	for _, b := range buf {
		if b.Record.Type == "book_clear" {
			t.Error("the malformed record must not be buffered: replaying it would only demote the instrument again")
		}
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("a publisher defect must not read as mktdata loss: per_instrument_gaps_total = %v want 0", got)
	}

	// A snapshot captured at seq 7 recovers the instrument in one step: 8 is still
	// buffered to apply on top of it, and 6 is behind the anchor.
	inst.BeginSnapshot(1, 899, 0, 7, 0)
	if err := inst.EndSnapshot(1, 899); err != nil {
		t.Fatalf("snapshot commit: %v", err)
	}
	s.replayBuffer(k, inst)

	if inst.Status != StatusReady {
		t.Errorf("replaying the buffered deltas must recover the instrument, got %v", inst.Status)
	}
	if inst.LastAppliedInstrumentSeq != 8 {
		t.Errorf("the buffered delta must replay: tracker %d want 8", inst.LastAppliedInstrumentSeq)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("recovery must not declare a gap: per_instrument_gaps_total = %v want 0", got)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("the malformed record is counted once, not again on replay: got %v want 1", got)
	}
}

// applyOne is the sole writer of the sequence trackers, so it refuses a malformed
// record on its own rather than trusting its caller's classification. The
// trackers advancing for a mutation the book never took would misclassify every
// later delta on the instrument.
func TestApplyOne_RefusesAMalformedBookClearItsCallerLetThrough(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.LastAppliedMktdataSeq = 899
	inst.Bids[900] = &LevelState{QtyRaw: 7}
	inst.Asks[1100] = &LevelState{QtyRaw: 7}

	ev, err := s.applyOne(inst, bookClearRec(11, 900, 6, "both", "from_price", 1000))

	if !errors.Is(err, errBookClearScopeSide) {
		t.Fatalf("error: got %v want errBookClearScopeSide", err)
	}
	if ev.Kind != KindMalformedDelta {
		t.Errorf("kind: got %q want %q", ev.Kind, KindMalformedDelta)
	}
	if inst.Bids[900] == nil || inst.Asks[1100] == nil {
		t.Error("a refused book_clear must not mutate the book")
	}
	if inst.LastAppliedInstrumentSeq != 5 || inst.LastAppliedMktdataSeq != 899 {
		t.Errorf("trackers must not advance: %d %d want 5 899",
			inst.LastAppliedInstrumentSeq, inst.LastAppliedMktdataSeq)
	}
}

// The demotion is for messages that LOSE A BOOK MUTATION, not for every
// publisher defect. A LevelUpdate whose Action disagrees with this book engine's book
// still states the level's complete resulting state, so the absolute-apply rule
// produces the correct book: it is counted as divergence and the instrument stays
// ready. Demoting here would turn a decode nit into a resynchronization cycle.
func TestApplyDelta_LevelUpdateDivergenceDoesNotDemote(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.Bids[1000] = &LevelState{QtyRaw: 7}

	// Action=new on a level already present, and a zero quantity carried with an
	// Action the publisher rule forbids for it: two divergences at once.
	evs := s.applyDelta(k, levelUpdateRec(11, 900, 6, "bid", 1000, 0))

	if len(evs) != 1 || evs[0].Kind != KindAppliedDelta {
		t.Fatalf("a divergent LevelUpdate still applies: %+v", evs)
	}
	if inst.Status != StatusReady {
		t.Fatalf("a divergence must not demote the instrument, got %v", inst.Status)
	}
	if got := malformedCount(m, reasonBookClearScopeSide) + malformedCount(m, reasonMalformedOther); got != 0 {
		t.Errorf("a divergence is not a malformed delta: malformed_deltas_total = %v want 0", got)
	}
	if got := counterValue(m.BookDivergenceTotal.WithLabelValues(string(DivergenceNewOnPresent))); got != 1 {
		t.Errorf("book_divergence_total{new_on_present}: got %v want 1", got)
	}
	if got := counterValue(m.BookDivergenceTotal.WithLabelValues(string(DivergenceZeroQtyBadAction))); got != 1 {
		t.Errorf("book_divergence_total{zero_qty_wrong_action}: got %v want 1", got)
	}
	// The apply happened, so the trackers advanced.
	if inst.LastAppliedInstrumentSeq != 6 {
		t.Errorf("trackers must advance on an applied delta: got %d want 6", inst.LastAppliedInstrumentSeq)
	}
}

// A malformed BookClear that arrives AHEAD of expected must be classified on
// receipt, not when it becomes contiguous.
//
// Reordering is why the record is held at all, and the hold is what puts the
// counter contract at risk: the record enters Pending unlooked-at, and if its
// predecessor never arrives, the reorder window's gap branch clears Pending. The
// malformed record leaves with it — never counted in malformed_deltas_total, and
// the demotion an operator sees is per_instrument_gaps_total, the counter that
// means mktdata never reached this process. A publisher defect reads as feed
// loss, which is the one thing this demotion exists to prevent.
//
// Nothing about the hole changes that verdict. The malformed record consumed a
// Per-Instrument Seq at the publisher and its mutation is lost whether or not the
// records before it arrive, so waiting cannot turn it into anything else.
func TestApplyDelta_MalformedBookClearAheadOfExpectedCountsOnReceipt(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// 7 is malformed and arrives ahead of the hole at 6, which never fills.
	evs := s.applyDelta(k, bookClearRec(11, 901, 7, "both", "from_price", 1000))

	if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
		t.Fatalf("want one malformed_delta on receipt, got %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Fatalf("the malformed record must gap the instrument on receipt, got %v", inst.Status)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("malformed_deltas_total{bookclear_scope_side}: got %v want 1", got)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("a publisher defect must not read as mktdata loss: per_instrument_gaps_total = %v want 0", got)
	}
	if inst.Pending != nil {
		t.Errorf("a record known to be lost must not be held for reordering: Pending=%v", inst.Pending)
	}
	if len(s.deltaBuf[k]) != 0 || s.bufferedN != 0 {
		t.Errorf("the malformed record must not be buffered: %d records, bufferedN=%d", len(s.deltaBuf[k]), s.bufferedN)
	}
	if inst.LastAppliedInstrumentSeq != 5 {
		t.Errorf("trackers must not advance: got %d want 5", inst.LastAppliedInstrumentSeq)
	}

	// Feed past the reorder window with the hole at 6 still open. Under a
	// classification deferred to contiguity this is where the record would have
	// been dropped and the demotion relabelled: the window is exceeded, the gap
	// branch fires, and Pending goes with it.
	for i := uint32(8); i <= 8+reorderWindow+1; i++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(894+i), i, "bid", int64(2000+i), 5))
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("the instrument is already gapped: per_instrument_gaps_total = %v want 0", got)
	}
	if got := malformedCount(m, reasonBookClearScopeSide); got != 1 {
		t.Errorf("exactly one malformed delta, counted once: got %v want 1", got)
	}
}

// A BookClear carrying a reserved Clear Side or Scope byte is a publisher
// defect, and applying it would delete levels on a side the message never
// named.
//
// The parser renders a byte outside the wire enumeration as "unknown", and this
// bot's mappers used to fall through to 0 — so Clear Side 3 read as "bid",
// scope 0 wiped the entire bid side, and the instrument stayed ready with a
// half-empty book and nothing counted. The reserved value now reaches
// bookClearMalformed as a sentinel and demotes under reasonMalformedOther.
func TestApplyDelta_BookClearWithAReservedEnumIsMalformed(t *testing.T) {
	for _, tc := range []struct{ name, clearSide, scope string }{
		{"reserved clear side", "unknown", "entire_side"},
		{"reserved scope", "bid", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetrics("test", "test")
			s := NewShard(0, 1, NewEventsWriter(nil), m)
			k := instKey{0, 11}
			inst := readyInstrumentInShard(t, s, k, 5)
			inst.Bids[900] = &LevelState{QtyRaw: 7}
			inst.Asks[1100] = &LevelState{QtyRaw: 4}

			evs := s.applyDelta(k, bookClearRec(11, 900, 6, tc.clearSide, tc.scope, 1000))

			if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
				t.Fatalf("events: %+v", evs)
			}
			if inst.Status != StatusGap {
				t.Errorf("a reserved value must gap the instrument, got %v", inst.Status)
			}
			if got := malformedCount(m, reasonMalformedOther); got != 1 {
				t.Errorf("malformed_deltas_total{other}: got %v want 1", got)
			}
			// The whole point: nothing was deleted on a side the message never
			// named.
			if len(inst.Bids) != 1 || len(inst.Asks) != 1 {
				t.Errorf("no level may be cleared by a reserved value: bids=%d asks=%d", len(inst.Bids), len(inst.Asks))
			}
			if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
				t.Errorf("a publisher defect must not read as mktdata loss: got %v want 0", got)
			}
		})
	}
}

// A snapshot from before the malformed record's own sequence does not repair
// the instrument, and the one that does applies every delta held behind it.
//
// Two mechanisms meet here and this pins both. RequireSnapshotAtLeast refuses
// the early snapshot outright, so the instrument stays gapped rather than
// coming back with a book missing the buffered deltas. MarkInstrumentSeqLost
// records that the malformed record's sequence can never arrive, so once a
// usable snapshot commits the replay steps over that number instead of
// treating it as a hole — without it the buffered deltas read as a forward
// gap, fill the reorder window, and the gap branch clears them and declares
// per_instrument_gaps_total, charging this book engine's own demotion to the
// counter that means mktdata never arrived.
func TestApplyDelta_SnapshotBehindAMalformedSeqIsRefusedAndTheNextOneReplays(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	// The malformed BookClear consumes per-instrument seq 6.
	evs := s.applyDelta(k, bookClearRec(11, 900, 6, "both", "from_price", 1000))
	if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
		t.Fatalf("want one malformed_delta, got %+v", evs)
	}
	if inst.Status != StatusGap {
		t.Fatalf("status: got %v want gap", inst.Status)
	}

	// One more than a full reorder window follows and is buffered for replay.
	// One past the window is what makes the gap branch reachable: at exactly
	// reorderWindow the held records still fit and no gap is declared.
	const following = reorderWindow + 1
	for i := 0; i < following; i++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(901+i), uint32(7+i), "bid", int64(3000+i), 5))
	}
	if got := len(s.deltaBuf[k]); got != following {
		t.Fatalf("buffered: got %d want %d", got, following)
	}

	// A snapshot captured BEFORE the malformed message is refused: its book is
	// the state before a mutation that is gone, and committing it would serve a
	// book the buffered deltas were never applied to.
	inst.BeginSnapshot(1, 899, 0, 5, 0)
	if err := inst.EndSnapshot(1, 899); err == nil {
		t.Fatal("a snapshot from before the hole must not commit")
	}
	if inst.Status != StatusGap {
		t.Errorf("a refused snapshot leaves the instrument gapped, got %v", inst.Status)
	}

	// The snapshot at the hole does repair it, and every held delta applies.
	inst.BeginSnapshot(2, 900, 0, 6, 0)
	if err := inst.EndSnapshot(2, 900); err != nil {
		t.Fatalf("snapshot commit: %v", err)
	}
	s.replayBuffer(k, inst)

	if inst.Status != StatusReady {
		t.Errorf("the buffered deltas must replay onto the snapshot, got %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 0 {
		t.Errorf("the book engine must not charge its own demotion to mktdata loss: per_instrument_gaps_total = %v want 0", got)
	}
	if want := uint32(6 + following); inst.LastAppliedInstrumentSeq != want {
		t.Errorf("every buffered delta must apply: tracker %d want %d", inst.LastAppliedInstrumentSeq, want)
	}
	if len(s.deltaBuf[k]) != 0 || s.bufferedN != 0 {
		t.Errorf("nothing may be left buffered: %d records bufferedN=%d", len(s.deltaBuf[k]), s.bufferedN)
	}
	if inst.LostInstrumentSeq != nil {
		t.Errorf("the lost-sequence set must be pruned once passed, got %v", inst.LostInstrumentSeq)
	}
}

// The gap's requirement must name the NEWEST delta it discards, not the oldest.
//
// `expected` is only the first missing seq. Pending holds everything that
// arrived ahead of it, and the gap branch clears it without buffering, so a
// snapshot at `expected` predates all of them: it commits, returns the
// instrument to ready holding a book those deltas were never applied to, and
// the next live delta walks the reorder window to a second gap. One loss,
// counted twice.
func TestApplyDelta_GapRequiresASnapshotPastEveryDiscardedDelta(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 100)

	// 101 is lost. 102..117 arrive and are held, which is one past the window
	// and so trips the gap on the last of them.
	const firstHeld, lastHeld = 102, 102 + reorderWindow
	for seq := firstHeld; seq <= lastHeld; seq++ {
		s.applyDelta(k, levelUpdateRec(11, uint64(seq), uint32(seq), "bid", int64(1000+seq), 5))
	}
	if inst.Status != StatusGap {
		t.Fatalf("the window must have been exceeded: status %v", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Fatalf("one gap for one loss: got %v want 1", got)
	}
	if inst.RequiredInstrumentSeq == nil {
		t.Fatal("the gap must state a requirement")
	}
	// lastHeld is the record that tripped the branch: it is buffered and
	// replays, so the requirement stops at the highest seq actually discarded.
	if got, want := *inst.RequiredInstrumentSeq, uint32(lastHeld-1); got != want {
		t.Errorf("required seq: got %d want %d (the newest discarded, not the hole at 101)", got, want)
	}

	// A snapshot at the hole is refused: it predates every held delta.
	// The anchor stays below the buffered record's mktdata seq, or replayBuffer
	// would filter it as already covered and the assertion below would pass for
	// the wrong reason.
	inst.BeginSnapshot(1, 50, 0, 101, 0)
	if err := inst.EndSnapshot(1, 50); err == nil {
		t.Error("a snapshot at the hole must not commit while newer deltas were discarded")
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("one loss must not be counted twice: per_instrument_gaps_total = %v want 1", got)
	}

	// The snapshot that covers the discarded range does commit, and the record
	// that tripped the gap replays on top of it.
	inst.BeginSnapshot(2, 51, 0, lastHeld-1, 0)
	if err := inst.EndSnapshot(2, 51); err != nil {
		t.Fatalf("snapshot commit: %v", err)
	}
	s.replayBuffer(k, inst)
	if inst.Status != StatusReady {
		t.Errorf("status after recovery: got %v want ready", inst.Status)
	}
	if got := inst.LastAppliedInstrumentSeq; got != lastHeld {
		t.Errorf("the buffered record must replay: tracker %d want %d", got, lastHeld)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("recovery must not declare another gap: got %v want 1", got)
	}
}

// The gap branch also trips on DISTANCE, and there the discarded-range scan is
// not enough on its own.
//
// When one delta arrives far past the hole, Pending holds almost nothing: the
// deltas in between never arrived at all rather than being dropped here. A
// requirement read only from Pending names the hole, a snapshot there commits,
// and the replay of the held record finds the whole run still missing and
// declares a second gap — one loss counted twice, the same outcome the
// discarded-range fix exists to prevent, reached the other way. The floor is
// the seq before the record that will replay.
func TestApplyDelta_GapOnDistanceRequiresTheWholeMissingRun(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 100)

	// 101..150 never arrive; 151 is 50 past the hole, well beyond the window.
	const held = 151
	s.applyDelta(k, levelUpdateRec(11, held, held, "bid", 1000, 5))
	if inst.Status != StatusGap {
		t.Fatalf("status %v want gap", inst.Status)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Fatalf("one gap for one loss: got %v want 1", got)
	}
	if inst.RequiredInstrumentSeq == nil {
		t.Fatal("the gap must state a requirement")
	}
	if got, want := *inst.RequiredInstrumentSeq, uint32(held-1); got != want {
		t.Errorf("required seq: got %d want %d (the whole missing run, not the hole at 101)", got, want)
	}

	// A snapshot at the hole is refused: 102..150 are still missing.
	inst.BeginSnapshot(1, 50, 0, 101, 0)
	if err := inst.EndSnapshot(1, 50); err == nil {
		t.Error("a snapshot at the hole must not commit while the run behind it is missing")
	}

	// The one that covers the run does, and the held record replays on top.
	inst.BeginSnapshot(2, 51, 0, held-1, 0)
	if err := inst.EndSnapshot(2, 51); err != nil {
		t.Fatalf("snapshot commit: %v", err)
	}
	s.replayBuffer(k, inst)
	if inst.Status != StatusReady {
		t.Errorf("status after recovery: got %v want ready", inst.Status)
	}
	if got := inst.LastAppliedInstrumentSeq; got != held {
		t.Errorf("the held record must replay: tracker %d want %d", got, held)
	}
	if got := counterValue(m.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("one loss must not be counted twice: got %v want 1", got)
	}
}
