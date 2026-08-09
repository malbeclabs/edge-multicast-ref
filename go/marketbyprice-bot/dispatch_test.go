package main

import (
	"strconv"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

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
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	s.apply(snapBeginRec(11, 4, 1, 100, 0, 9999)) // K == tracker
	if inst.OpenSnapshot != nil {
		t.Error("a current ready instrument must not open a shadow")
	}

	// The publisher sends the group's levels regardless of our decision to decline
	// it, and the coordinator forwards them. None of these is a defect, so
	// snapshot_level_dropped_total must not move — otherwise the counter that
	// exists to expose misrouted levels is dominated by healthy steady state.
	s.apply(snapLevelRec(11, 4, "bid", 1000, 5))
	s.apply(snapLevelRec(11, 4, "ask", 1100, 5))
	s.apply(snapLevelRec(11, 4, "bid", 999, 5))

	if got := counterValue(m.SnapshotLevelDroppedTotal); got != 0 {
		t.Errorf("levels of a declined snapshot are not drops: got %v want 0", got)
	}
}

// The counter must still fire on the case it was built for: a level whose
// Snapshot ID does not match the open group is a genuine misroute.
func TestApply_SnapshotLevelMismatchStillCounted(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	s.apply(snapBeginRec(11, 5, 2, 150, 0, 9999)) // K > tracker, shadow opens
	if inst.OpenSnapshot == nil {
		t.Fatal("setup: shadow should be open")
	}

	s.apply(snapLevelRec(11, 5, "bid", 1000, 5)) // belongs to the group
	s.apply(snapLevelRec(11, 6, "bid", 1000, 5)) // wrong snapshot_id: a misroute

	if got := counterValue(m.SnapshotLevelDroppedTotal); got != 1 {
		t.Errorf("a snapshot_id mismatch must count: got %v want 1", got)
	}
	if inst.OpenSnapshot.ReceivedLevels != 1 {
		t.Errorf("only the matching level joins the shadow: got %d", inst.OpenSnapshot.ReceivedLevels)
	}
}

func TestApply_SnapshotWhileReadyRebootstrapsWhenBehind(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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

// The same discard must hold when the reset arrives BEFORE the instrument's
// definition, which is the ordinary cold-start ordering: the refdata cycle lags
// mktdata, so a reset routinely lands while the instrument is still unknown.
// Dropping it there loses RequiredAnchorSeq, and the stale snapshot the reset
// existed to invalidate then commits — leaving the instrument ready and serving
// exactly the diverged book, with no discard counted.
func TestApply_InstrumentResetBeforeDefinitionStillDiscardsStaleSnapshot(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)

	// Reset first, while the instrument is entirely unknown to the shard.
	s.apply(Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{
		"reason": "venue_resync", "new_anchor_seq": float64(9000),
	}})
	// Definition lands afterwards, as it does at cold start.
	s.apply(instDefRec(11, "SYM", 1))
	// A snapshot captured before the reset must still be refused.
	s.apply(snapBeginRec(11, 7, 1, 0, 0, 8500))

	inst := s.instruments[instKey{0, 11}]
	if inst == nil {
		t.Fatal("instrument should exist after the definition")
	}
	if inst.RequiredAnchorSeq == nil || *inst.RequiredAnchorSeq != 9000 {
		t.Errorf("the reset's required anchor must survive: %v", inst.RequiredAnchorSeq)
	}
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

// The definition must not clobber the identity the reset established, and must
// still populate symbol and exponents on the instrument the reset created.
func TestApply_DefinitionAfterResetPopulatesRefdata(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), NewMetrics("test", "test"))
	s.apply(Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{
		"reason": "venue_resync", "new_anchor_seq": float64(9000),
	}})
	s.apply(instDefRec(11, "SYM", 1))

	inst := s.instruments[instKey{0, 11}]
	if inst.Symbol != "SYM" || inst.PriceExponent != -2 || inst.QtyExponent != -8 {
		t.Errorf("definition must populate refdata: symbol=%q priceExp=%d qtyExp=%d",
			inst.Symbol, inst.PriceExponent, inst.QtyExponent)
	}
}

// Only KindAppliedDelta and KindAppliedSnapshot may assert a book mutation.
// A consumer persisting events keys off exactly that, and noteConsistencyPoint
// keys off it to decide when to evaluate crossed-book, so every non-mutating
// path must carry its own kind.
func TestChannelEvent_NonMutatingPathsDoNotClaimBookChange(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), NewMetrics("test", "test"))

	cases := []struct {
		name string
		rec  Record
		want string
	}{
		{"instrument_definition", instDefRec(11, "SYM", 1), KindInstrumentDefinition},
		{"batch_boundary", Record{Type: "batch_boundary", Port: "mktdata"}, KindBatchBoundary},
		{"trade", Record{Type: "trade", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{}}, KindTrade},
		{"liquidation", Record{Type: "liquidation", Port: "mktdata", InstrumentID: 11, Fields: map[string]any{}}, KindTrade},
		{"instrument_reset", Record{Type: "instrument_reset", Port: "mktdata", InstrumentID: 11,
			Fields: map[string]any{"reason": "venue_resync", "new_anchor_seq": float64(10)}}, KindInstrumentReset},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evs := s.apply(tc.rec)
			if len(evs) != 1 {
				t.Fatalf("expected one event, got %+v", evs)
			}
			if evs[0].Kind != tc.want {
				t.Errorf("kind: got %q want %q", evs[0].Kind, tc.want)
			}
			if evs[0].Kind == KindAppliedDelta || evs[0].Kind == KindAppliedSnapshot {
				t.Errorf("%s must not claim a book mutation", tc.name)
			}
		})
	}
}

// A malformed BookClear is the one non-mutating path reached through applyOne,
// and it must not report as applied either.
func TestChannelEvent_MalformedBookClearIsNotApplied(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	readyInstrumentInShard(t, s, k, 5)

	evs := s.apply(bookClearRec(11, 900, 6, "both", "from_price", 1000))
	if len(evs) != 1 || evs[0].Kind != KindMalformedDelta {
		t.Fatalf("expected a malformed_delta event, got %+v", evs)
	}
}

// gaugeVecSum totals a shard-labelled gauge across the given shard indices,
// which is how an operator reads it: sum(dz_mbp_bot_...) with no shard selector.
func gaugeVecSum(g *prometheus.GaugeVec, shards int) float64 {
	total := 0.0
	for i := 0; i < shards; i++ {
		total += gaugeRead(g.WithLabelValues(strconv.Itoa(i)))
	}
	return total
}

// Shards own disjoint instruments and each can only report its own count. As
// bare process-wide gauges every shard Set the same series to its local number,
// so two crossed books on two shards read as 1 rather than 2. Every other test
// in this package runs a single shard, which is why that passed.
func TestCrossedInstruments_SumsAcrossShards(t *testing.T) {
	m := NewMetrics("test", "test")
	shards := []*Shard{NewShard(0, 2, NewEventsWriter(nil), m), NewShard(1, 2, NewEventsWriter(nil), m)}

	// One crossed instrument on each shard: bid above ask.
	for i, s := range shards {
		instID := uint32(10 + i)
		s.apply(instDefRec(instID, "SYM", 1))
		s.instruments[instKey{0, instID}].Status = StatusReady
		s.apply(levelUpdateRec(instID, 900, 1, "ask", 1000, 5))
		s.apply(levelUpdateRec(instID, 901, 2, "bid", 1200, 5))
	}

	if got := gaugeVecSum(m.CrossedInstruments, 2); got != 2 {
		t.Errorf("crossed_instruments must total both shards: got %v want 2", got)
	}
}

func TestDeltaBufferedRecords_SumsAcrossShards(t *testing.T) {
	m := NewMetrics("test", "test")
	shards := []*Shard{NewShard(0, 2, NewEventsWriter(nil), m), NewShard(1, 2, NewEventsWriter(nil), m)}

	// Four buffered on shard 0, two on shard 1.
	for i := 0; i < 4; i++ {
		shards[0].bufferDelta(instKey{0, 10}, levelUpdateRec(10, uint64(100+i), uint32(i+1), "bid", 1000, 5))
	}
	for i := 0; i < 2; i++ {
		shards[1].bufferDelta(instKey{0, 11}, levelUpdateRec(11, uint64(100+i), uint32(i+1), "bid", 1000, 5))
	}

	if got := gaugeVecSum(m.DeltaBufferedRecords, 2); got != 6 {
		t.Errorf("delta_buffered_records must total both shards: got %v want 6", got)
	}
}

// A channel reset wipes shard state, and both gauges must be re-exported. Left
// unpublished they hold their pre-reset values indefinitely on a shard that then
// goes quiet, because nothing else writes them until the next crossed book or
// buffered delta — which may never come.
func TestReset_RepublishesGauges(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)

	s.apply(instDefRec(11, "SYM", 1))
	s.instruments[instKey{0, 11}].Status = StatusReady
	s.apply(levelUpdateRec(11, 900, 1, "ask", 1000, 5))
	s.apply(levelUpdateRec(11, 901, 2, "bid", 1200, 5)) // crossed
	s.bufferDelta(instKey{0, 12}, levelUpdateRec(12, 100, 1, "bid", 1000, 5))

	if got := gaugeRead(m.CrossedInstruments.WithLabelValues("0")); got != 1 {
		t.Fatalf("setup: crossed gauge should be 1, got %v", got)
	}
	if got := gaugeRead(m.DeltaBufferedRecords.WithLabelValues("0")); got != 1 {
		t.Fatalf("setup: buffered gauge should be 1, got %v", got)
	}

	s.reset()

	if got := gaugeRead(m.CrossedInstruments.WithLabelValues("0")); got != 0 {
		t.Errorf("crossed_instruments must be republished as 0 after reset: got %v", got)
	}
	if got := gaugeRead(m.DeltaBufferedRecords.WithLabelValues("0")); got != 0 {
		t.Errorf("delta_buffered_records must be republished as 0 after reset: got %v", got)
	}
}

// With no BatchBoundary seen, every applied delta is a consistency point.
func TestCrossedBook_PerDeltaWhenNoBatchBoundary(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	if got := gaugeRead(m.CrossedInstruments.WithLabelValues("0")); got != 1 {
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
	s := NewShard(0, 1, NewEventsWriter(nil), m)
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
	if got := gaugeRead(m.CrossedInstruments.WithLabelValues("0")); got != 0 {
		t.Errorf("crossed gauge should clear: got %v", got)
	}
}

// Definitions are retransmitted gradually across a definition cycle, so pruning
// everything below the new seq would evict instruments still in the manifest.
func TestPruneManifest_GraceWindowKeepsPreviousGeneration(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
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
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
	s.apply(instDefRec(1, "A", 0))
	s.apply(instDefRec(2, "B", 1))
	s.pruneManifest(1) // no generation is old enough to be stale
	if len(s.instruments) != 2 {
		t.Errorf("nothing should be pruned at seq 1, got %d instruments", len(s.instruments))
	}
}

func TestPruneManifest_AdjustsBufferedN(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
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

// The last SnapshotBegin's identity must be recorded even when the snapshot is
// DECLINED, because wire_levels denormalizes it onto every captured level and
// declining is the steady-state case.
func TestApply_LastBeginRecordedEvenWhenDeclined(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), NewMetrics("test", "test"))
	s.apply(instDefRec(11, "SYM", 1))
	inst := s.instruments[instKey{0, 11}]
	inst.Status = StatusReady
	inst.LastAppliedInstrumentSeq = 100

	// K == tracker, so the begin is declined and no shadow opens.
	s.apply(snapBeginRec(11, 4, 3, 100, 25, 9999))

	if inst.OpenSnapshot != nil {
		t.Fatal("setup: a current ready instrument must not open a shadow")
	}
	if inst.LastBegin == nil {
		t.Fatal("LastBegin must be recorded even for a declined snapshot")
	}
	if inst.LastBegin.SnapshotID != 4 || inst.LastBegin.AnchorSeq != 9999 {
		t.Errorf("LastBegin identity: %+v", inst.LastBegin)
	}
	if inst.LastBegin.TotalLevels != 3 || inst.LastBegin.LastInstrumentSeq != 100 {
		t.Errorf("LastBegin counts: %+v", inst.LastBegin)
	}
	if inst.LastBegin.DepthBound != 25 {
		t.Errorf("LastBegin depth bound: %+v", inst.LastBegin)
	}
}

// It must also be recorded on the accepted path, so recovery captures too.
func TestApply_LastBeginRecordedWhenAccepted(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), NewMetrics("test", "test"))
	s.apply(instDefRec(11, "SYM", 1))
	s.apply(snapBeginRec(11, 7, 2, 50, 0, 5000))

	inst := s.instruments[instKey{0, 11}]
	if inst.LastBegin == nil || inst.LastBegin.SnapshotID != 7 {
		t.Fatalf("LastBegin: %+v", inst.LastBegin)
	}
}

// Deltas buffered while an instrument is awaiting-snapshot or gapped are applied
// to the live book by replayBuffer, so their events must reach the caller. When
// they did not, the events log carried a mktdata_seq hole on every bootstrap and
// every gap recovery — the exact continuity a consumer queries that table for.
//
// This is the reviewer's repro from PR #35: a definition, one buffered
// level_update at per_instrument_seq 6, then a snapshot committing at
// last_instrument_seq 5. The book advances to 6; the events must say so.
func TestApply_ReplayedDeltasAreReported(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(st), m)
	s.sw = NewSnapshotWriter(nil, 5, 0, m, func(k instKey, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[k])
	}, nil)

	s.handle(instDefRec(11, "SYM", 1))
	// Buffered: the instrument is still awaiting its first snapshot.
	s.handle(levelUpdateRec(11, 600, 6, "bid", 1000, 50))
	// The definition lands in `instruments`; nothing has reached `events` yet,
	// because the delta is sitting in the buffer.
	if got := len(st.rows["events"]); got != 0 {
		t.Fatalf("setup: a buffered delta must not be written yet, got %d events rows", got)
	}
	k := instKey{0, 11}
	if len(s.deltaBuf[k]) != 1 {
		t.Fatalf("setup: the delta should be buffered, got %+v", s.deltaBuf[k])
	}

	// Snapshot commits at anchor 500, last_instrument_seq 5, so the buffered
	// delta at mktdata seq 600 / piSeq 6 replays on top of it.
	s.handle(snapBeginRec(11, 3, 1, 5, 0, 500))
	s.handle(snapLevelRec(11, 3, "bid", 900, 10))
	s.handle(snapEndRec(11, 3, 500))

	inst := s.instruments[k]
	if inst.LastAppliedInstrumentSeq != 6 {
		t.Fatalf("the book must have advanced through the replayed delta: got %d want 6",
			inst.LastAppliedInstrumentSeq)
	}

	// The replayed level_update is the only events row: snapshot_end matches no
	// events case by design, and the definition went to `instruments`.
	var replayed []map[string]any
	for _, row := range st.rows["events"] {
		if row["kind"] == "level_update" {
			replayed = append(replayed, row)
		}
	}
	if len(replayed) != 1 {
		t.Fatalf("the replayed delta must produce exactly one events row, got %d", len(replayed))
	}
	if got := replayed[0]["mktdata_seq"]; got != uint64(600) {
		t.Errorf("mktdata_seq: got %v want 600", got)
	}
	if got := replayed[0]["per_instrument_seq"]; got != uint32(6) {
		t.Errorf("per_instrument_seq: got %v want 6", got)
	}
}
