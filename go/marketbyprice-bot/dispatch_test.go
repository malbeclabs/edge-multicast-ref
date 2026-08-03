package main

import (
	"testing"
)

func instDefRec(instID uint32, symbol string, manifestSeq uint16) Record {
	return Record{
		Type:         "instrument_definition",
		Port:         "refdata",
		InstrumentID: instID,
		Fields: map[string]any{
			"symbol":         symbol,
			"price_exponent": float64(-2),
			"qty_exponent":   float64(-8),
			"manifest_seq":   float64(manifestSeq),
		},
	}
}

func snapBeginRec(instID, snapID, total, lastInstr, depth uint32, anchor uint64) Record {
	return Record{
		Type:         "snapshot_begin",
		Port:         "snapshot",
		InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id":         float64(snapID),
			"anchor_seq":          float64(anchor),
			"total_levels":        float64(total),
			"last_instrument_seq": float64(lastInstr),
			"depth_bound":         float64(depth),
		},
	}
}

// snapLevelRec models what the SHARD receives: the wire omits instrument_id on
// snapshot_level, and the coordinator stamps it from the open group before
// forwarding. Tests that call shard.apply directly must stamp it too.
func snapLevelRec(instID, snapID uint32, side string, priceRaw int64, qtyRaw uint64) Record {
	return Record{
		Type:         "snapshot_level",
		Port:         "snapshot",
		InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"side":        side,
			"price_raw":   float64(priceRaw),
			"qty_raw":     float64(qtyRaw),
			"level_flags": float64(0),
			"order_count": float64(2),
		},
	}
}

func snapEndRec(instID, snapID uint32, anchor uint64) Record {
	return Record{
		Type:         "snapshot_end",
		Port:         "snapshot",
		InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"anchor_seq":  float64(anchor),
		},
	}
}

func TestApply_InstrumentDefinitionCreatesInstrument(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(11, "BTC-USDT", 5))

	k := instKey{0, 11}
	def, ok := s.refdata[k]
	if !ok || def.Symbol != "BTC-USDT" || def.ManifestSeq != 5 {
		t.Fatalf("refdata: %+v", def)
	}
	inst, ok := s.instruments[k]
	if !ok {
		t.Fatal("instrument should be created")
	}
	if inst.PriceExponent != -2 || inst.QtyExponent != -8 {
		t.Errorf("exponents: %d %d", inst.PriceExponent, inst.QtyExponent)
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
}

func TestApply_SnapshotLifecycleCommits(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	s.apply(snapBeginRec(11, 3, 2, 77, 25, 5000))
	s.apply(snapLevelRec(11, 3, "bid", 1000, 10))
	s.apply(snapLevelRec(11, 3, "ask", 1100, 20))
	s.apply(snapEndRec(11, 3, 5000))

	inst := s.instruments[instKey{0, 11}]
	if inst.Status != StatusReady {
		t.Fatalf("status: %v", inst.Status)
	}
	if inst.Bids[1000] == nil || inst.Asks[1100] == nil {
		t.Errorf("book: bids=%+v asks=%+v", inst.Bids, inst.Asks)
	}
	if inst.DepthBound == nil || *inst.DepthBound != 25 {
		t.Errorf("depth bound: %v", inst.DepthBound)
	}
	if inst.LastAppliedInstrumentSeq != 77 {
		t.Errorf("tracker: %d", inst.LastAppliedInstrumentSeq)
	}
}

func TestApply_SnapshotLevelWrongIDDropped(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	s.apply(snapBeginRec(11, 3, 1, 0, 0, 5000))
	s.apply(snapLevelRec(11, 99, "bid", 1000, 10)) // wrong snapshot id

	inst := s.instruments[instKey{0, 11}]
	if inst.OpenSnapshot.ReceivedLevels != 0 {
		t.Errorf("mismatched level must not enter the shadow: %d", inst.OpenSnapshot.ReceivedLevels)
	}
	if got := counterValue(m.SnapshotLevelDroppedTotal); got != 1 {
		t.Errorf("dropped counter: got %v want 1", got)
	}
}

// A ready, current instrument must ignore a periodic snapshot: no shadow opens.
func TestApply_SnapshotWhileReadyIgnoredWhenCurrent(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	s.apply(snapBeginRec(11, 4, 1, 100, 0, 9999)) // K == tracker
	if inst.OpenSnapshot != nil {
		t.Error("a current ready instrument must not open a shadow")
	}
}

func TestApply_SnapshotWhileReadyRebootstrapsWhenBehind(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100
	inst.ApplyLevelUpdate(0, 500, 5, 1, 0, 1) // stale level that must be replaced

	s.apply(snapBeginRec(11, 5, 1, 150, 0, 9999)) // K > tracker
	if inst.OpenSnapshot == nil {
		t.Fatal("a behind ready instrument must open a shadow")
	}
	s.apply(snapLevelRec(11, 5, "bid", 1000, 10))
	s.apply(snapEndRec(11, 5, 9999))

	if inst.Bids[500] != nil {
		t.Error("the stale level must be gone after re-bootstrap")
	}
	if inst.Bids[1000] == nil {
		t.Error("the snapshot level must be present")
	}
	if inst.LastAppliedInstrumentSeq != 150 {
		t.Errorf("tracker: got %d want 150", inst.LastAppliedInstrumentSeq)
	}
}

func TestApply_InstrumentResetSetsAnchorAndTrimsBuffer(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	k := instKey{0, 11}
	inst := s.instruments[k]
	inst.Status = StatusReady

	// Buffer deltas either side of the reset anchor.
	for i, seq := range []uint64{100, 200, 300, 400} {
		s.bufferDelta(k, levelUpdateRec(11, seq, uint32(i+1), "bid", 1000, 5))
	}
	if s.bufferedN != 4 {
		t.Fatalf("setup bufferedN: %d", s.bufferedN)
	}

	s.apply(Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{
		"reason": "upstream_gap", "new_anchor_seq": float64(250),
	}})

	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
	if inst.RequiredAnchorSeq == nil || *inst.RequiredAnchorSeq != 250 {
		t.Errorf("required anchor: %v", inst.RequiredAnchorSeq)
	}
	if got := len(s.deltaBuf[k]); got != 2 {
		t.Errorf("only deltas above the anchor survive: got %d want 2", got)
	}
	if s.bufferedN != 2 {
		t.Errorf("bufferedN must track the trim: got %d want 2", s.bufferedN)
	}
	if inst.DepthBound != nil {
		t.Error("reset must return depth bound to unknown")
	}
}

// A snapshot captured before the reset but delivered after it must be discarded.
func TestApply_StaleSnapshotAfterResetDiscarded(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]

	s.apply(Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{
		"reason": "venue_resync", "new_anchor_seq": float64(9000),
	}})
	s.apply(snapBeginRec(11, 7, 1, 0, 0, 8500)) // anchor older than required

	if inst.OpenSnapshot != nil {
		t.Error("a stale-anchor snapshot must not open a shadow")
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status must stay awaiting-snapshot: %v", inst.Status)
	}
	if got := counterValue(m.SnapshotDiscardedTotal.WithLabelValues("stale_anchor")); got != 1 {
		t.Errorf("stale_anchor discard counter: got %v want 1", got)
	}
}

// With no BatchBoundary seen, every applied delta is a consistency point.
func TestCrossedBook_PerDeltaWhenNoBatchBoundary(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	k := instKey{0, 11}
	inst := s.instruments[k]
	inst.Status = StatusReady

	// Ask at 1000, then a bid at 1200 crosses it.
	s.apply(levelUpdateRec(11, 900, 1, "ask", 1000, 5))
	if got := counterValue(m.CrossedBookEventsTotal); got != 0 {
		t.Fatalf("one-sided book is not crossed: got %v", got)
	}
	s.apply(levelUpdateRec(11, 901, 2, "bid", 1200, 5))
	if got := counterValue(m.CrossedBookEventsTotal); got != 1 {
		t.Errorf("crossing delta must count immediately: got %v want 1", got)
	}
	if got := gaugeRead(m.CrossedInstruments); got != 1 {
		t.Errorf("crossed gauge: got %v want 1", got)
	}
	// Status and book untouched: this is observability, not control flow.
	if inst.Status != StatusReady {
		t.Errorf("crossed book must not change status: %v", inst.Status)
	}
	if inst.Bids[1200] == nil || inst.Asks[1000] == nil {
		t.Error("crossed book must not be discarded")
	}
}

// Once a BatchBoundary is seen, evaluation defers to the boundary.
func TestCrossedBook_AtBoundaryWhenBatching(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, m)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady

	boundary := Record{Type: "batch_boundary", Port: "mktdata", Fields: map[string]any{
		"batch_id": float64(1), "batch_ts": "2026-08-02T00:00:00Z",
	}}
	s.apply(boundary) // channel is now known to batch

	s.apply(levelUpdateRec(11, 900, 1, "ask", 1000, 5))
	s.apply(levelUpdateRec(11, 901, 2, "bid", 1200, 5)) // crosses mid-batch
	if got := counterValue(m.CrossedBookEventsTotal); got != 0 {
		t.Fatalf("a transient cross inside a batch is legal and must not count: got %v", got)
	}
	s.apply(boundary)
	if got := counterValue(m.CrossedBookEventsTotal); got != 1 {
		t.Errorf("the boundary is the consistency point: got %v want 1", got)
	}

	// A cross resolved before the next boundary must not count at all.
	s.apply(levelUpdateRec(11, 902, 3, "bid", 1200, 0)) // delete the crossing bid
	s.apply(boundary)
	if got := counterValue(m.CrossedBookEventsTotal); got != 1 {
		t.Errorf("resolved cross must not count again: got %v want 1", got)
	}
	if got := gaugeRead(m.CrossedInstruments); got != 0 {
		t.Errorf("crossed gauge should clear: got %v", got)
	}
}

// Definitions are retransmitted gradually across a definition cycle, so pruning
// everything below the new seq would evict instruments still in the manifest.
func TestPruneManifest_GraceWindowKeepsPreviousGeneration(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(1, "OLD", 3))     // two generations back
	s.apply(instDefRec(2, "RECENT", 4))  // one generation back — inside grace
	s.apply(instDefRec(3, "CURRENT", 5)) // current

	s.pruneManifest(5)

	if _, ok := s.instruments[instKey{0, 1}]; ok {
		t.Error("an instrument two generations stale must be pruned")
	}
	if _, ok := s.instruments[instKey{0, 2}]; !ok {
		t.Error("the previous generation is inside the grace window and must survive")
	}
	if _, ok := s.instruments[instKey{0, 3}]; !ok {
		t.Error("the current generation must survive")
	}
}

func TestPruneManifest_EarlySeqDoesNotPrune(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(1, "A", 0))
	s.apply(instDefRec(2, "B", 1))
	s.pruneManifest(1) // no generation is old enough to be stale
	if len(s.instruments) != 2 {
		t.Errorf("nothing should be pruned at seq 1, got %d instruments", len(s.instruments))
	}
}

func TestPruneManifest_AdjustsBufferedN(t *testing.T) {
	s := NewShard(0, 1, nil)
	s.apply(instDefRec(1, "STALE", 2))
	k := instKey{0, 1}
	for i := 0; i < 3; i++ {
		s.bufferDelta(k, levelUpdateRec(1, uint64(i), uint32(i+1), "bid", 1000, 5))
	}
	if s.bufferedN != 3 {
		t.Fatalf("setup: %d", s.bufferedN)
	}
	s.pruneManifest(5)
	if _, ok := s.instruments[k]; ok {
		t.Fatal("stale instrument should be pruned")
	}
	if s.bufferedN != 0 {
		t.Errorf("bufferedN must drop with the pruned buffer: got %d want 0", s.bufferedN)
	}
}
