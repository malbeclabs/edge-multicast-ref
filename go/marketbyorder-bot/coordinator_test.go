package main

import (
	"context"
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
	return NewCoordinator(context.Background(), shards, NewEventsWriter(nil), metrics), inboxes
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

func TestCoordinator_ResetBarrierWipesShardsThenRoutesHeldRecord(t *testing.T) {
	metrics := stubMetrics()
	n := 3
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
			s.mu.Lock()
			defer s.mu.Unlock()
			fn(s.instruments[instKey{0, id}])
		})
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}

	// Era 1: define instrument 3 (3 % 3 == 0).
	c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: 3, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"symbol": "A", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	time.Sleep(50 * time.Millisecond)

	// Era 2: reset_count bump on a new instrument_definition (the held first new-era frame).
	c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: 5, ResetCount: 2,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"symbol": "B", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	time.Sleep(50 * time.Millisecond)

	shards[0].mu.Lock()
	_, oldGone := shards[0].instruments[instKey{0, 3}]
	shards[0].mu.Unlock()
	if oldGone {
		t.Error("old-era instrument 3 should have been wiped by reset barrier")
	}
	shards[2].mu.Lock()
	_, newHere := shards[2].instruments[instKey{0, 5}] // 5 % 3 == 2
	shards[2].mu.Unlock()
	if !newHere {
		t.Error("held first new-era record (instrument 5) not applied to shard 2")
	}
	if c.resetCount != 2 {
		t.Errorf("coordinator resetCount = %d, want 2", c.resetCount)
	}
	if got := testCounter(t, metrics.ChannelResetsTotal); got != 1 {
		t.Errorf("channel_resets_total = %v, want 1", got)
	}
}

// This test is deterministic (no sleeps-as-assertions): with shards PAUSED
// (Run not started), a correct fence blocks on shard acks; the stub returns
// immediately. We assert "blocked while paused" then "unblocks once shards run".
func TestCoordinator_FenceBlocksUntilShardsDrain(t *testing.T) {
	metrics := stubMetrics()
	n := 3
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(s *Shard) func(uint32, func(*Instrument)) {
			return func(id uint32, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[instKey{0, id}])
			}
		}(s))
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)
	defer cancel()
	// SnapshotWriters can run; shard.Run is intentionally NOT started yet.
	for _, s := range shards {
		go s.sw.Run(ctx)
	}

	// Pre-load instrument records into shard inboxes (buffered, won't block).
	for id := uint32(1); id <= 9; id++ {
		c.Dispatch(Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
				"symbol": "S", "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
	}

	done := make(chan struct{})
	go func() {
		c.Dispatch(Record{Type: "end_of_session", ChannelID: 0, ResetCount: 1,
			Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
		close(done)
	}()

	// While shards are paused a correct fence MUST still be blocked on acks.
	// The stub runFence writes immediately and returns -> done closes here -> FAIL.
	select {
	case <-done:
		t.Fatal("fence returned while shards were paused: it did not drain/ack")
	case <-time.After(200 * time.Millisecond):
	}

	// Start the shards; they drain FIFO (9 records then the fence marker) and ack.
	for _, s := range shards {
		go s.Run(ctx)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("fence did not return after shards started draining")
	}
	for i, s := range shards {
		if len(s.inbox) != 0 {
			t.Errorf("shard %d inbox not drained after fence: %d", i, len(s.inbox))
		}
	}
}

func TestCoordinator_HeartbeatNotFenced(t *testing.T) {
	c, inboxes := newCoordWithCapture(2)
	c.Dispatch(Record{Type: "heartbeat", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
	for i, in := range inboxes {
		select {
		case m := <-in:
			t.Fatalf("heartbeat must not reach shard %d: %+v", i, m)
		default:
		}
	}
}

func TestCoordinator_ResetBarrierHandlesChannelScopedFirstFrame(t *testing.T) {
	metrics := stubMetrics()
	n := 2
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(id uint32, fn func(*Instrument)) {
			s.mu.Lock()
			defer s.mu.Unlock()
			fn(s.instruments[instKey{0, id}])
		})
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}
	c.Dispatch(Record{Type: "heartbeat", ChannelID: 0, ResetCount: 1,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
	// First new-era frame is channel-scoped (manifest_summary) — must not panic / not hash.
	c.Dispatch(Record{Type: "manifest_summary", ChannelID: 0, ResetCount: 2,
		Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{
			"manifest_seq": float64(1), "valid": float64(1), "instrument_count": float64(0)}})
	if c.resetCount != 2 {
		t.Errorf("resetCount = %d, want 2", c.resetCount)
	}
}

// Closes the shutdown-during-reset hazard: if ctx is cancelled mid-barrier
// (shards/SnapshotWriters already exiting), the coordinator must abandon the
// barrier instead of hanging forever on the ack-wait.
func TestCoordinator_ResetBarrierEscapesOnCtxCancel(t *testing.T) {
	metrics := stubMetrics()
	n := 2
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)
	// Prime the barrier predicate. Do NOT start shard.Run, so no acks can arrive.
	c.resetSeen = true
	c.resetCount = 1

	dispatchDone := make(chan struct{})
	go func() {
		c.Dispatch(Record{Type: "heartbeat", ChannelID: 0, ResetCount: 2,
			Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
		close(dispatchDone)
	}()

	// Barrier must be blocked (no shards draining).
	select {
	case <-dispatchDone:
		t.Fatal("Dispatch returned without acks — barrier did not actually block")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case <-dispatchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator barrier hung after ctx cancel")
	}
}

// Closes the same hazard for the fence path.
func TestCoordinator_FenceEscapesOnCtxCancel(t *testing.T) {
	metrics := stubMetrics()
	n := 2
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)

	dispatchDone := make(chan struct{})
	go func() {
		c.Dispatch(Record{Type: "end_of_session", ChannelID: 0, ResetCount: 1,
			Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})
		close(dispatchDone)
	}()
	select {
	case <-dispatchDone:
		t.Fatal("Dispatch returned before ctx cancelled — fence did not block")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case <-dispatchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator fence hung after ctx cancel")
	}
}
