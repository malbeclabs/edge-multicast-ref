package main

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func testCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}

// sr builds a record for shard tests (channel 0, reset_count 1).
func sr(rt, port string, seq uint64, instID uint32, fields map[string]any) Record {
	return Record{
		Type: rt, Timestamp: time.Unix(1700000000, 0), ChannelID: 0,
		Port: port, SequenceNumber: seq, ResetCount: 1,
		InstrumentID: instID, Fields: fields,
	}
}

func newTestShard(t *testing.T) *Shard {
	t.Helper()
	return NewShard(0, 1, NewEventsWriter(nil), nil, NewMetrics("test", "test"))
}

func TestShard_ColdStart(t *testing.T) {
	s := newTestShard(t)
	s.apply(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	if _, ok := s.refdata[instKey{0, 100}]; !ok {
		t.Fatal("refdata not stored")
	}

	s.apply(sr("order_add", "mktdata", 50, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(101),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if got := len(s.deltaBuf[instKey{0, 100}]); got != 1 {
		t.Fatalf("expected 1 buffered delta, got %d", got)
	}

	s.apply(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(49), "total_orders": float64(0),
		"snapshot_id": float64(7), "last_instrument_seq": float64(100),
	}))
	s.apply(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(49), "snapshot_id": float64(7),
	}))

	inst := s.instruments[instKey{0, 100}]
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

func TestShard_PerInstrumentGap(t *testing.T) {
	s := newTestShard(t)
	s.apply(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.apply(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.apply(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))
	inst := s.instruments[instKey{0, 100}]
	inst.LastAppliedInstrumentSeq = 0

	s.apply(sr("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if inst.Status != StatusReady {
		t.Fatalf("after seq=1 status: %v", inst.Status)
	}

	// Flood with deltas beyond the reorder window (hole at seq=2 never fills).
	// The window only holds up to reorderWindow entries; once exceeded a gap is declared.
	var lastEvs []ChannelEvent
	for piSeq := uint32(3); piSeq <= 3+uint32(reorderWindow)+1; piSeq++ {
		lastEvs = s.apply(sr("order_add", "mktdata", uint64(100+piSeq), 100, map[string]any{
			"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(piSeq),
			"order_id": float64(piSeq), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
			"price_raw": float64(82440), "qty_raw": float64(2000),
		}))
		if inst.Status == StatusGap {
			break
		}
	}
	if inst.Status != StatusGap {
		t.Errorf("expected status gap, got %v", inst.Status)
	}
	if len(lastEvs) != 1 || lastEvs[0].Kind != "per_instrument_gap" {
		t.Errorf("expected per_instrument_gap event, got %+v", lastEvs)
	}
}

func TestShard_HandleMarksDirtyOnAppliedDelta(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	// give it a SnapshotWriter so MarkDirty has a target
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})

	s.handle(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.handle(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.handle(sr("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))

	s.sw.mu.Lock()
	_, dirty := s.sw.dirty[100]
	s.sw.mu.Unlock()
	if !dirty {
		t.Errorf("expected instrument 100 marked dirty after applied_snapshot")
	}
}

func TestShard_HandlePerInstrumentGapMetric(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	s.handle(sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	s.handle(sr("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0), "snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	s.handle(sr("snapshot_end", "snapshot", 2, 100, map[string]any{"anchor_seq": float64(0), "snapshot_id": float64(1)}))
	s.instruments[instKey{0, 100}].LastAppliedInstrumentSeq = 0
	s.handle(sr("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(1), "qty_raw": float64(1),
	}))
	// Flood with deltas beyond the reorder window (hole at seq=2 never fills).
	// Once the window is exceeded a gap is declared and handle() increments the metric.
	for piSeq := uint32(3); piSeq <= 3+uint32(reorderWindow)+1; piSeq++ {
		s.handle(sr("order_add", "mktdata", uint64(100+piSeq), 100, map[string]any{
			"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(piSeq),
			"order_id": float64(piSeq), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
			"price_raw": float64(1), "qty_raw": float64(1),
		}))
		if s.instruments[instKey{0, 100}].Status == StatusGap {
			break
		}
	}
	if got := testCounter(t, metrics.PerInstrumentGapsTotal); got != 1 {
		t.Errorf("per_instrument_gaps_total = %v, want 1", got)
	}
}

func TestShard_RunProcessesRecordsThenResetAcks(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.sw.Run(ctx)
	go s.Run(ctx)

	rec := sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	s.inbox <- shardMsg{kind: msgRecord, rec: &rec}

	acks := make(chan int, 1)
	s.inbox <- shardMsg{kind: msgReset, ch: 0, ack: acks}
	select {
	case got := <-acks:
		if got != 0 {
			t.Errorf("ack idx = %d, want 0", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for reset ack")
	}

	// Reset must have wiped instrument state (processed before the marker, FIFO).
	s.mu.Lock()
	n := len(s.instruments)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("instruments not wiped after reset: %d", n)
	}
}

func TestShard_FenceAcksWithoutWipe(t *testing.T) {
	metrics := stubMetrics()
	s := NewShard(0, 1, NewEventsWriter(nil), nil, metrics)
	s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
		s.mu.Lock()
		defer s.mu.Unlock()
		fn(s.instruments[instKey{0, id}])
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.sw.Run(ctx)
	go s.Run(ctx)

	rec := sr("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "X", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	s.inbox <- shardMsg{kind: msgRecord, rec: &rec}
	acks := make(chan int, 1)
	s.inbox <- shardMsg{kind: msgFence, ack: acks}
	select {
	case <-acks:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fence ack")
	}
	s.mu.Lock()
	n := len(s.instruments)
	s.mu.Unlock()
	if n != 1 {
		t.Errorf("fence must NOT wipe state: instruments=%d want 1", n)
	}
}

// --- record builder helpers for snapshot tests ---

func snapshotBeginRec(ch uint8, instID, snapID, total uint32, anchor uint64, lastInstr uint32) Record {
	return Record{
		Type: "snapshot_begin", ChannelID: ch, InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id":         float64(snapID),
			"total_orders":        float64(total),
			"anchor_seq":          float64(anchor),
			"last_instrument_seq": float64(lastInstr),
		},
	}
}

func snapshotOrderRec(ch uint8, snapID uint32, orderID uint64, side uint8, price int64, qty uint64) Record {
	sideStr := "bid"
	if side != 0 {
		sideStr = "ask"
	}
	return Record{
		Type: "snapshot_order", ChannelID: ch,
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"order_id":    float64(orderID),
			"side":        sideStr,
			"order_flags": float64(0),
			"enter_ts":    "",
			"price_raw":   float64(price),
			"qty_raw":     float64(qty),
		},
	}
}

func snapshotEndRec(ch uint8, instID, snapID uint32, anchor uint64) Record {
	return Record{
		Type: "snapshot_end", ChannelID: ch, InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"anchor_seq":  float64(anchor),
		},
	}
}

func testCounterVec(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	var m dto.Metric
	if err := cv.WithLabelValues(labels...).Write(&m); err != nil {
		t.Fatalf("counter vec write: %v", err)
	}
	return m.GetCounter().GetValue()
}

// --- Task 2 tests ---

func TestReadyInstrumentIgnoresSnapshot(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusReady
	s.instruments[k].Bids[1] = &RestingOrder{OrderID: 1}
	s.instruments[k].LastAppliedInstrumentSeq = 10

	s.apply(snapshotBeginRec(0, 7, 99, 5, 2000, 20))
	if s.instruments[k].OpenSnapshot != nil {
		t.Fatal("Ready instrument must not start a shadow build")
	}
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("snapshot end on a Ready (no-shadow) instrument must be a no-op")
	}
}

func TestShortSnapshotKeepsReadyBook(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap
	s.apply(snapshotBeginRec(0, 7, 99, 2, 2000, 20))
	if s.instruments[k].OpenSnapshot == nil {
		t.Fatal("Gap instrument must start a shadow build")
	}
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5)) // only 1 of 2
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].OpenSnapshot != nil {
		t.Fatal("short snapshot shadow must be discarded")
	}
	if s.instruments[k].Status != StatusGap {
		t.Fatal("short snapshot must leave a Gap instrument in Gap, not demote further")
	}
}

func TestCompleteSnapshotRepairsGap(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap
	s.apply(snapshotBeginRec(0, 7, 99, 1, 2000, 20))
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5))
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("complete snapshot must repair a Gap to Ready")
	}
	if _, ok := s.instruments[k].Bids[11]; !ok {
		t.Fatal("repaired book must contain snapshot order")
	}
}

// --- Task 3 tests ---

func TestShortSnapshotIncrementsDiscardedNotDemotion(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	s.instruments[k] = NewInstrument(7, "BTC", 0, 0)
	s.instruments[k].Status = StatusGap
	s.apply(snapshotBeginRec(0, 7, 99, 2, 2000, 20))
	s.apply(snapshotOrderRec(0, 99, 11, 0, 100, 5))
	s.apply(snapshotEndRec(0, 7, 99, 2000))
	if got := testCounterVec(t, s.metrics.SnapshotDiscardedTotal, "short"); got != 1 {
		t.Fatalf("snapshot_discarded_total{short} = %v, want 1", got)
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 0 {
		t.Fatalf("book_demotions_total = %v, want 0", got)
	}
}

// --- Task 4 tests ---

func orderAddRec(ch uint8, instID uint32, piSeq uint32, mktSeq uint64, price int64, qty uint64) Record {
	return Record{
		Type: "order_add", ChannelID: ch, InstrumentID: instID,
		SequenceNumber: mktSeq,
		Fields: map[string]any{
			"per_instrument_seq": float64(piSeq),
			"side":               "bid",
			"order_flags":        float64(0),
			"order_id":           float64(piSeq),
			"enter_ts":           "",
			"price_raw":          float64(price),
			"qty_raw":            float64(qty),
		},
	}
}

func TestReorderedDeltaDoesNotGap(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	in := NewInstrument(7, "BTC", 0, 0)
	in.Status = StatusReady
	in.LastAppliedInstrumentSeq = 10
	s.instruments[k] = in
	// seq 12 arrives before 11 (reorder)
	s.apply(orderAddRec(0, 7, 12 /*piSeq*/, 120, 100, 1))
	if in.Status == StatusGap {
		t.Fatal("a single reordered delta must not declare a gap")
	}
	s.apply(orderAddRec(0, 7, 11 /*piSeq*/, 110, 100, 1))
	if in.Status != StatusReady || in.LastAppliedInstrumentSeq != 12 {
		t.Fatalf("reorder must drain to seq 12 Ready; got status=%v seq=%d", in.Status, in.LastAppliedInstrumentSeq)
	}
}

func TestRealGapEscalatesPastWindow(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	in := NewInstrument(7, "BTC", 0, 0)
	in.Status = StatusReady
	in.LastAppliedInstrumentSeq = 10
	s.instruments[k] = in
	// A burst of far-ahead deltas with a permanent hole at 11.
	for piSeq := uint32(12); piSeq <= 12+uint32(reorderWindow)+1; piSeq++ {
		s.apply(orderAddRec(0, 7, piSeq, uint64(piSeq), 100, 1))
	}
	if in.Status != StatusGap {
		t.Fatal("a hole beyond the reorder window must declare a gap")
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 1 {
		t.Fatalf("book_demotions_total = %v, want 1", got)
	}
}

// --- Task 6 tests ---

func TestSteadyStateIgnoresLossySnapshots(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 7}
	// cold start
	s.apply(snapshotBeginRec(0, 7, 1, 1, 1000, 0))
	s.apply(snapshotOrderRec(0, 1, 100, 0, 50, 10))
	s.apply(snapshotEndRec(0, 7, 1, 1000))
	if s.instruments[k].Status != StatusReady {
		t.Fatal("cold-start snapshot should make it Ready")
	}
	// interleave deltas with lossy re-snapshots
	for i := uint32(1); i <= 30; i++ {
		s.apply(orderAddRec(0, 7, i, uint64(1000+i), int64(50+i), 1))
		if i%5 == 0 {
			snap := 10 + i
			s.apply(snapshotBeginRec(0, 7, snap, 3, uint64(2000+i), i))
			s.apply(snapshotOrderRec(0, snap, 200, 0, 60, 1))
			if i%3 != 0 { // drop an order on some snapshots
				s.apply(snapshotOrderRec(0, snap, 201, 0, 61, 1))
				s.apply(snapshotOrderRec(0, snap, 202, 0, 62, 1))
			}
			s.apply(snapshotEndRec(0, 7, snap, uint64(2000+i)))
		}
	}
	if s.instruments[k].Status != StatusReady {
		t.Fatalf("Ready instrument must ignore lossy re-snapshots; got %v", s.instruments[k].Status)
	}
	if got := testCounter(t, s.metrics.BookDemotionsTotal); got != 0 {
		t.Fatalf("book_demotions_total = %v, want 0", got)
	}
}

// --- SnapshotOrder association: the open group, validated by Snapshot ID ---

// snapshotShardWithCapture builds a single shard whose ClickHouse rows are captured.
func snapshotShardWithCapture(t *testing.T) (*Shard, *captureWriter) {
	t.Helper()
	cw := &captureWriter{}
	return NewShard(0, 1, NewEventsWriter(cw), nil, NewMetrics("test", "test")), cw
}

// wireSnapshotRows selects the wire_snapshots rows from everything captured.
// total_orders is carried by no other table.
func wireSnapshotRows(cw *captureWriter) []map[string]any {
	var out []map[string]any
	for _, row := range cw.captured() {
		if _, ok := row["total_orders"]; ok {
			out = append(out, row)
		}
	}
	return out
}

// Snapshot ID is monotonic per (channel_id, instrument_id), so a lost
// SnapshotEnd leaves a shadow open at an id the next instrument's cycle reuses.
// The order belongs to the instrument whose SnapshotBegin is open, never to
// whichever lingering shadow shares the id — eight decoys and -count=20 make
// Go's randomized map order an unreliable way to be right.
func TestSnapshotOrderFollowsOpenGroupNotMatchingSnapshotID(t *testing.T) {
	const snapID = 7
	s, cw := snapshotShardWithCapture(t)

	stale := []uint32{11, 12, 13, 14, 15, 16, 17, 18}
	for _, id := range stale {
		s.handle(sr("instrument_definition", "refdata", 1, id, map[string]any{"symbol": "STALE"}))
		s.handle(snapshotBeginRec(0, id, snapID, 3, 2000, 20)) // no SnapshotEnd follows
	}
	s.handle(sr("instrument_definition", "refdata", 1, 99, map[string]any{"symbol": "OPEN-99"}))
	s.handle(snapshotBeginRec(0, 99, snapID, 1, 3000, 30))
	s.handle(snapshotOrderRec(0, snapID, 555, 0, 100, 5))

	open := s.instruments[instKey{0, 99}].OpenSnapshot
	if open.ReceivedOrders != 1 {
		t.Errorf("open group received %d orders, want 1", open.ReceivedOrders)
	}
	if _, ok := open.Bids[555]; !ok {
		t.Error("the order is missing from the open group's shadow")
	}
	for _, id := range stale {
		if got := s.instruments[instKey{0, id}].OpenSnapshot.ReceivedOrders; got != 0 {
			t.Errorf("lingering shadow for instrument %d absorbed %d orders, want 0", id, got)
		}
	}

	rows := wireSnapshotRows(cw)
	if len(rows) != 1 {
		t.Fatalf("wire_snapshots rows = %d, want 1", len(rows))
	}
	if rows[0]["instrument_id"] != uint32(99) || rows[0]["symbol"] != "OPEN-99" {
		t.Errorf("wire_snapshots row = instrument %v %q, want 99 \"OPEN-99\"",
			rows[0]["instrument_id"], rows[0]["symbol"])
	}
}

// A Ready, current instrument declines its own snapshot, and the publisher still
// sends every order of the group. Those orders must not join a lingering shadow
// left by another instrument at the same Snapshot ID: doing so overruns its
// order count and costs that instrument a recovery cycle. Declining is the
// steady state, so the drop counter must stay silent for it.
func TestDeclinedGroupOrdersDoNotInflateALingeringShadow(t *testing.T) {
	s, _ := snapshotShardWithCapture(t)

	// Instrument 41 is recovering. Its shadow completes, then its SnapshotEnd is lost.
	s.handle(snapshotBeginRec(0, 41, 7, 1, 2000, 20))
	s.handle(snapshotOrderRec(0, 7, 900, 0, 100, 5))

	// Instrument 42 is Ready and current, so it declines its own group at the
	// same Snapshot ID one cycle later.
	k42 := instKey{0, 42}
	s.instruments[k42] = NewInstrument(42, "READY-42", 0, 0)
	s.instruments[k42].Status = StatusReady
	s.handle(snapshotBeginRec(0, 42, 7, 2, 4000, 40))

	before := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 7, 901, 0, 101, 6))
	s.handle(snapshotOrderRec(0, 7, 902, 1, 102, 7))

	lingering := s.instruments[instKey{0, 41}].OpenSnapshot
	if lingering.ReceivedOrders != 1 {
		t.Errorf("lingering shadow received %d orders, want 1", lingering.ReceivedOrders)
	}
	if _, ok := lingering.Bids[901]; ok {
		t.Error("a declined group's order joined another instrument's shadow")
	}
	if s.instruments[k42].OpenSnapshot != nil {
		t.Error("a Ready instrument must not build a shadow")
	}
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 0 {
		t.Errorf("snapshot_order_dropped_total += %v for a declined group, want 0", got)
	}
}

// The snapshot context is keyed by (channel, instrument). Keyed by
// (channel, snapshot_id) it would hold one entry per lost SnapshotEnd, since the
// id advances every cycle and the entry is never overwritten.
func TestSnapshotContextKeyedByInstrumentNotSnapshotID(t *testing.T) {
	s, cw := snapshotShardWithCapture(t)
	s.handle(sr("instrument_definition", "refdata", 1, 55, map[string]any{"symbol": "SYM-55"}))

	// Three cycles, each SnapshotEnd lost, each at its own Snapshot ID.
	for snapID := uint32(7); snapID <= 9; snapID++ {
		s.handle(snapshotBeginRec(0, 55, snapID, 1, uint64(1000*snapID), 20))
		s.handle(snapshotOrderRec(0, snapID, uint64(snapID), 0, 100, 5))
	}

	if len(s.snapCtx) != 1 {
		t.Errorf("snapCtx holds %d entries after three lost SnapshotEnds, want 1", len(s.snapCtx))
	}
	ctx, ok := s.snapCtx[instKey{0, 55}]
	if !ok {
		t.Fatal("snapshot context is not keyed by (channel, instrument)")
	}
	if ctx.SnapshotID != 9 {
		t.Errorf("snapshot context holds id %d, want the open group's 9", ctx.SnapshotID)
	}

	rows := wireSnapshotRows(cw)
	if len(rows) != 3 {
		t.Fatalf("wire_snapshots rows = %d, want 3", len(rows))
	}
	for i, row := range rows {
		if row["instrument_id"] != uint32(55) || row["symbol"] != "SYM-55" {
			t.Errorf("row %d = instrument %v %q, want 55 \"SYM-55\"", i, row["instrument_id"], row["symbol"])
		}
	}
}

// Snapshot ID validates membership: an order whose id disagrees with the open
// group belongs to no group this shard can name, so it is dropped and counted,
// and nothing is persisted for it.
func TestSnapshotOrderWithMismatchedSnapshotIDIsDroppedAndCounted(t *testing.T) {
	s, cw := snapshotShardWithCapture(t)
	s.handle(snapshotBeginRec(0, 61, 7, 1, 2000, 20))

	before := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 8, 700, 0, 100, 5))

	if got := s.instruments[instKey{0, 61}].OpenSnapshot.ReceivedOrders; got != 0 {
		t.Errorf("open group received %d orders at a mismatched id, want 0", got)
	}
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 1 {
		t.Errorf("snapshot_order_dropped_total += %v, want 1", got)
	}
	if rows := wireSnapshotRows(cw); len(rows) != 0 {
		t.Errorf("wire_snapshots rows = %d for an order matching no open group, want 0", len(rows))
	}
}

// SnapshotEnd closes the group, so an order trailing it has no instrument to
// belong to: dropped, counted, and kept out of the committed book.
func TestSnapshotOrderAfterEndIsDroppedAndCounted(t *testing.T) {
	s, cw := snapshotShardWithCapture(t)
	s.handle(sr("instrument_definition", "refdata", 1, 71, map[string]any{"symbol": "SYM-71"}))
	s.handle(snapshotBeginRec(0, 71, 7, 1, 2000, 20))
	s.handle(snapshotOrderRec(0, 7, 800, 0, 100, 5))
	s.handle(snapshotEndRec(0, 71, 7, 2000))

	inst := s.instruments[instKey{0, 71}]
	if inst.Status != StatusReady {
		t.Fatalf("instrument status %v, want ready", inst.Status)
	}
	before := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 7, 801, 0, 101, 6))

	if _, ok := s.open[0]; ok {
		t.Error("SnapshotEnd left the group open")
	}
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 1 {
		t.Errorf("snapshot_order_dropped_total += %v for an order after SnapshotEnd, want 1", got)
	}
	if len(inst.Bids) != 1 {
		t.Errorf("committed book holds %d bids, want the snapshot's 1", len(inst.Bids))
	}
	if rows := wireSnapshotRows(cw); len(rows) != 1 {
		t.Errorf("wire_snapshots rows = %d, want 1 (the in-group order only)", len(rows))
	}
}

// A SnapshotEnd delayed behind the next SnapshotBegin names a group that is
// already finished. It must leave the live group open, or the rest of that
// group's orders are dropped as belonging to nothing and persist no rows.
func TestDelayedSnapshotEndDoesNotCloseTheLiveGroup(t *testing.T) {
	s, cw := snapshotShardWithCapture(t)
	s.handle(sr("instrument_definition", "refdata", 1, 55, map[string]any{"symbol": "SYM-55"}))

	// One cycle at id 7 whose SnapshotEnd is lost, then the next at id 8.
	s.handle(snapshotBeginRec(0, 55, 7, 1, 1000, 10))
	s.handle(snapshotBeginRec(0, 55, 8, 2, 2000, 20))
	s.handle(snapshotOrderRec(0, 8, 801, 0, 100, 5))
	// The id-7 end arrives now, behind the id-8 begin.
	s.handle(snapshotEndRec(0, 55, 7, 1000))

	g, ok := s.open[0]
	if !ok || g.snapID != 8 || g.inst != (instKey{0, 55}) {
		t.Fatalf("open group = %+v (present %v), want instrument 55 at the live id 8", g, ok)
	}

	before := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 8, 802, 0, 101, 6))
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 0 {
		t.Errorf("snapshot_order_dropped_total += %v for an order of the live group, want 0", got)
	}
	if rows := wireSnapshotRows(cw); len(rows) != 2 {
		t.Errorf("wire_snapshots rows = %d, want 2 (both orders of the live group)", len(rows))
	}
}

// Snapshot ID validates membership whether or not the open group's instrument
// accepted the snapshot. A ready instrument declines its group and builds no
// shadow, and the drop counter must still tell that group's own orders — the
// steady state — apart from an order belonging to no open group at all.
func TestMismatchedSnapshotIDIsCountedEvenWhenTheGroupWasDeclined(t *testing.T) {
	s, cw := snapshotShardWithCapture(t)
	s.handle(sr("instrument_definition", "refdata", 1, 42, map[string]any{"symbol": "READY-42"}))
	s.instruments[instKey{0, 42}].Status = StatusReady
	s.handle(snapshotBeginRec(0, 42, 7, 2, 4000, 40))
	if s.instruments[instKey{0, 42}].OpenSnapshot != nil {
		t.Fatal("a Ready instrument must decline the group and build no shadow")
	}

	before := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 7, 901, 0, 101, 6))
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 0 {
		t.Errorf("snapshot_order_dropped_total += %v for the declined group's own order, want 0", got)
	}

	s.handle(snapshotOrderRec(0, 999, 902, 0, 102, 7))
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - before; got != 1 {
		t.Errorf("snapshot_order_dropped_total += %v for an id matching no open group, want 1", got)
	}

	rows := wireSnapshotRows(cw)
	if len(rows) != 1 {
		t.Fatalf("wire_snapshots rows = %d, want 1 (the declined group's own order only)", len(rows))
	}
	if rows[0]["instrument_id"] != uint32(42) || rows[0]["snapshot_id"] != uint32(7) {
		t.Errorf("row = instrument %v id %v, want 42 at id 7", rows[0]["instrument_id"], rows[0]["snapshot_id"])
	}
}

// A SnapshotEnd delayed behind the next SnapshotBegin names a group that is
// already finished, and the shadow in progress belongs to the newer group. The
// end must leave that shadow alone: offering it to EndSnapshot fails the id
// check there, which discards the shadow, and the rest of the live group's
// orders then have nothing to build into — the instrument loses the recovery
// cycle it was about to complete.
func TestDelayedSnapshotEndDoesNotDiscardTheLiveShadow(t *testing.T) {
	s, _ := snapshotShardWithCapture(t)
	k := instKey{0, 55}
	s.handle(sr("instrument_definition", "refdata", 1, 55, map[string]any{"symbol": "SYM-55"}))

	// One cycle at id 7 whose SnapshotEnd is lost, then the next at id 8,
	// carrying two orders.
	s.handle(snapshotBeginRec(0, 55, 7, 1, 1000, 10))
	s.handle(snapshotBeginRec(0, 55, 8, 2, 2000, 20))
	s.handle(snapshotOrderRec(0, 8, 801, 0, 100, 5))

	// The id-7 end arrives now, behind the id-8 begin.
	beforeMismatch := testCounterVec(t, s.metrics.SnapshotDiscardedTotal, "mismatch")
	s.handle(snapshotEndRec(0, 55, 7, 1000))

	shadow := s.instruments[k].OpenSnapshot
	if shadow == nil {
		t.Fatal("the delayed end discarded the live group's shadow")
	}
	if shadow.SnapshotID != 8 || shadow.ReceivedOrders != 1 {
		t.Fatalf("shadow = id %d holding %d orders, want the live id 8 holding 1",
			shadow.SnapshotID, shadow.ReceivedOrders)
	}
	if got := testCounterVec(t, s.metrics.SnapshotDiscardedTotal, "mismatch") - beforeMismatch; got != 0 {
		t.Errorf("snapshot_discarded_total{reason=\"mismatch\"} += %v for a delayed end, want 0", got)
	}

	// The live group completes, so the instrument recovers on this cycle.
	beforeDropped := testCounter(t, s.metrics.SnapshotOrderDroppedTotal)
	s.handle(snapshotOrderRec(0, 8, 802, 1, 101, 6))
	s.handle(snapshotEndRec(0, 55, 8, 2000))

	inst := s.instruments[k]
	if inst.Status != StatusReady {
		t.Fatalf("instrument status %v, want ready", inst.Status)
	}
	if len(inst.Bids) != 1 || len(inst.Asks) != 1 {
		t.Errorf("committed book = %d bids / %d asks, want the live group's 1 and 1",
			len(inst.Bids), len(inst.Asks))
	}
	if got := testCounter(t, s.metrics.SnapshotOrderDroppedTotal) - beforeDropped; got != 0 {
		t.Errorf("snapshot_order_dropped_total += %v for an order of the live group, want 0", got)
	}
	if _, ok := s.open[0]; ok {
		t.Error("the live group's own end left it open")
	}
}
