package main

import (
	"testing"
	"time"
)

// collectShards builds n shards whose inboxes we drain into a slice for assertions.
func newCoordWithCapture(n int) (*Coordinator, []chan shardMsg) {
	metrics := stubMetrics()
	shards := make([]*Shard, n)
	inboxes := make([]chan shardMsg, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		shards[i] = s
		inboxes[i] = s.inbox
	}
	return NewCoordinator(shards, NewEventsWriter(nil), metrics), inboxes
}

func TestCoordinator_RoutesInstrumentRecordByMod(t *testing.T) {
	c, inboxes := newCoordWithCapture(4)
	rec := Record{Type: "order_add", ChannelID: 0, InstrumentID: 6, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}}
	c.Dispatch(rec)
	// 6 % 4 == 2
	select {
	case m := <-inboxes[2]:
		if m.kind != msgRecord || m.rec.InstrumentID != 6 {
			t.Fatalf("wrong msg on shard 2: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("expected record on shard 2")
	}
	for i, in := range inboxes {
		if i == 2 {
			continue
		}
		select {
		case m := <-in:
			t.Fatalf("unexpected msg on shard %d: %+v", i, m)
		default:
		}
	}
}

func TestCoordinator_SnapshotOrderFollowsBeginRoute(t *testing.T) {
	c, inboxes := newCoordWithCapture(4)
	begin := Record{Type: "snapshot_begin", ChannelID: 0, InstrumentID: 9, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(42)}}
	c.Dispatch(begin) // 9 % 4 == 1
	order := Record{Type: "snapshot_order", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(42)}}
	c.Dispatch(order)

	// begin + order both land on shard 1, in order.
	m1 := <-inboxes[1]
	if m1.rec.Type != "snapshot_begin" {
		t.Fatalf("shard1 first msg: %s", m1.rec.Type)
	}
	m2 := <-inboxes[1]
	if m2.rec.Type != "snapshot_order" {
		t.Fatalf("shard1 second msg: %s", m2.rec.Type)
	}
}

func TestCoordinator_SnapshotOrderNoRouteDropsAndCounts(t *testing.T) {
	c, _ := newCoordWithCapture(2)
	order := Record{Type: "snapshot_order", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{"snapshot_id": float64(7)}}
	c.Dispatch(order)
	if got := testCounter(t, c.metrics.SnapshotOrderDroppedTotal); got != 1 {
		t.Errorf("snapshot_order_dropped_total = %v, want 1", got)
	}
}
