package main

import "testing"

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
