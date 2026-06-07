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

	evs := s.apply(sr("order_add", "mktdata", 102, 100, map[string]any{
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
	// seq jump 1 -> 3 triggers a gap; handle() must increment the metric.
	s.handle(sr("order_add", "mktdata", 102, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(3),
		"order_id": float64(2), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(1), "qty_raw": float64(1),
	}))
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
	s.inbox <- shardMsg{kind: msgReset, ack: acks}
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
			"snapshot_id":          float64(snapID),
			"total_orders":         float64(total),
			"anchor_seq":           float64(anchor),
			"last_instrument_seq":  float64(lastInstr),
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
