package main

import (
	"context"
	"testing"
)

func newTestCoordinator(t *testing.T, n int) (*Coordinator, []*Shard) {
	t.Helper()
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		shards[i] = NewShard(i, n, NewEventsWriter(nil), nil)
	}
	return NewCoordinator(context.Background(), shards, nil), shards
}

// drain returns the record types sitting in a shard's inbox, without running the
// shard goroutine.
func drain(s *Shard) []Record {
	var out []Record
	for {
		select {
		case m := <-s.inbox:
			if m.kind == msgRecord && m.rec != nil {
				out = append(out, *m.rec)
			}
		default:
			return out
		}
	}
}

func snapBegin(ch uint8, instID, snapID uint32) Record {
	return Record{
		Type:         "snapshot_begin",
		Port:         "snapshot",
		ChannelID:    ch,
		InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id":         float64(snapID),
			"anchor_seq":          float64(5000),
			"total_levels":        float64(1),
			"last_instrument_seq": float64(0),
			"depth_bound":         float64(0),
		},
	}
}

func snapLevel(ch uint8, snapID uint32, priceRaw int64) Record {
	return Record{
		Type:      "snapshot_level",
		Port:      "snapshot",
		ChannelID: ch,
		// NOTE: no InstrumentID — the wire omits it.
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"price_raw":   float64(priceRaw),
			"qty_raw":     float64(10),
			"side":        "bid",
			"level_flags": float64(0),
		},
	}
}

func snapEnd(ch uint8, instID, snapID uint32) Record {
	return Record{
		Type:         "snapshot_end",
		Port:         "snapshot",
		ChannelID:    ch,
		InstrumentID: instID,
		Fields: map[string]any{
			"snapshot_id": float64(snapID),
			"anchor_seq":  float64(5000),
		},
	}
}

func TestDispatch_RoutesInstrumentRecordsByModulo(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)
	c.Dispatch(levelUpdateRec(5, 100, 1, "bid", 1000, 50))

	if got := len(drain(shards[1])); got != 1 { // 5 % 4 == 1
		t.Errorf("shard 1 should hold the record, got %d", got)
	}
	for _, i := range []int{0, 2, 3} {
		if got := len(drain(shards[i])); got != 0 {
			t.Errorf("shard %d should be empty, got %d", i, got)
		}
	}
}

// snapshot_level carries no instrument_id, so it must follow the open group.
func TestDispatch_SnapshotLevelRoutedToOpenGroupsShard(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)
	c.Dispatch(snapBegin(0, 5, 7)) // instrument 5 -> shard 1
	c.Dispatch(snapLevel(0, 7, 1000))
	c.Dispatch(snapEnd(0, 5, 7))

	got := drain(shards[1])
	if len(got) != 3 {
		t.Fatalf("shard 1 should hold begin+level+end, got %d: %+v", len(got), got)
	}
	if got[1].Type != "snapshot_level" {
		t.Errorf("second record: %s", got[1].Type)
	}
	for _, i := range []int{0, 2, 3} {
		if n := len(drain(shards[i])); n != 0 {
			t.Errorf("shard %d should be empty, got %d", i, n)
		}
	}
}

func TestDispatch_SnapshotLevelWithNoOpenGroupDropped(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)
	c.Dispatch(snapLevel(0, 7, 1000))
	for i := range shards {
		if n := len(drain(shards[i])); n != 0 {
			t.Errorf("shard %d must be empty; an orphan level must be dropped, got %d", i, n)
		}
	}
}

func TestDispatch_SnapshotLevelMismatchedIDDropped(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)
	c.Dispatch(snapBegin(0, 5, 7))
	drain(shards[1]) // discard the begin
	c.Dispatch(snapLevel(0, 8, 1000))
	if n := len(drain(shards[1])); n != 0 {
		t.Errorf("a level with a mismatched snapshot_id must be dropped, got %d", n)
	}
}

// Regression test for the issue-#30 bug class. Two instruments legitimately share
// a snapshot_id, because Snapshot ID is monotonic PER INSTRUMENT. Routing keyed on
// {channel, snapshot_id} would send instrument 7's levels to instrument 4's shard.
// 4 % 4 == 0 and 7 % 4 == 3, so the two land on different shards and the wrong
// route is observable.
func TestDispatch_TwoInstrumentsSameSnapshotIDRouteIndependently(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)

	c.Dispatch(snapBegin(0, 4, 5)) // instrument 4 -> shard 0, snapshot_id 5
	c.Dispatch(snapLevel(0, 5, 1000))
	c.Dispatch(snapEnd(0, 4, 5))

	first := drain(shards[0])
	if len(first) != 3 {
		t.Fatalf("shard 0 should hold instrument 4's group, got %d", len(first))
	}

	c.Dispatch(snapBegin(0, 7, 5)) // instrument 7 -> shard 3, SAME snapshot_id 5
	c.Dispatch(snapLevel(0, 5, 2000))
	c.Dispatch(snapEnd(0, 7, 5))

	second := drain(shards[3])
	if len(second) != 3 {
		t.Fatalf("shard 3 should hold instrument 7's group, got %d: %+v", len(second), second)
	}
	if got := toInt64(second[1].Fields["price_raw"]); got != 2000 {
		t.Errorf("shard 3 got the wrong level: price_raw %d", got)
	}
	// Instrument 4's shard must not have received the second group's level.
	if n := len(drain(shards[0])); n != 0 {
		t.Errorf("shard 0 must not receive instrument 7's records, got %d", n)
	}
}

func TestDispatch_BatchBoundaryBroadcastsToAllShards(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)
	c.Dispatch(Record{Type: "batch_boundary", Port: "mktdata", Fields: map[string]any{
		"batch_id": float64(1), "batch_ts": "2026-08-02T00:00:00Z",
	}})
	for i := range shards {
		if n := len(drain(shards[i])); n != 1 {
			t.Errorf("shard %d should receive the boundary, got %d", i, n)
		}
	}
}

func TestDispatch_ResetCountChangeRunsBarrierThenRoutesHeldRecord(t *testing.T) {
	c, shards := newTestCoordinator(t, 2)

	// Establish era 0 and leave some coordinator state behind.
	c.Dispatch(snapBegin(0, 2, 9))
	if len(c.open) != 1 {
		t.Fatal("expected an open group before the reset")
	}
	for i := range shards {
		drain(shards[i])
	}

	// Drain reset markers concurrently so the barrier's ack wait completes.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range shards {
			for m := range shards[i].inbox {
				if m.kind == msgReset {
					m.ack <- i
					break
				}
			}
		}
	}()

	held := levelUpdateRec(3, 1, 1, "bid", 1000, 50)
	held.ResetCount = 1
	c.Dispatch(held)
	<-done

	if c.resetCount != 1 {
		t.Errorf("resetCount: got %d want 1", c.resetCount)
	}
	if len(c.open) != 0 {
		t.Errorf("open groups must be cleared by the barrier: %+v", c.open)
	}
	// The held record is re-dispatched as the first record of the new era.
	if n := len(drain(shards[1])); n != 1 { // 3 % 2 == 1
		t.Errorf("held record should be routed after the barrier, got %d", n)
	}
}

func TestDispatch_ManifestSeqBumpBroadcastsPrune(t *testing.T) {
	c, shards := newTestCoordinator(t, 3)

	manifest := func(seq uint16, valid uint8) Record {
		return Record{Type: "manifest_summary", Port: "refdata", Fields: map[string]any{
			"manifest_seq": float64(seq), "valid": float64(valid), "instrument_count": float64(10),
		}}
	}

	countPrunes := func(s *Shard) int {
		n := 0
		for {
			select {
			case m := <-s.inbox:
				if m.kind == msgManifestPrune {
					n++
				}
			default:
				return n
			}
		}
	}

	c.Dispatch(manifest(5, 1))
	for i := range shards {
		if got := countPrunes(shards[i]); got != 1 {
			t.Errorf("shard %d: first valid manifest should prune once, got %d", i, got)
		}
	}
	// Same seq again: no prune.
	c.Dispatch(manifest(5, 1))
	for i := range shards {
		if got := countPrunes(shards[i]); got != 0 {
			t.Errorf("shard %d: repeated seq must not prune, got %d", i, got)
		}
	}
	// Invalid manifest: no prune even on a higher seq.
	c.Dispatch(manifest(6, 0))
	for i := range shards {
		if got := countPrunes(shards[i]); got != 0 {
			t.Errorf("shard %d: invalid manifest must not prune, got %d", i, got)
		}
	}
	if c.manifest.Seq != 6 || c.manifest.Valid {
		t.Errorf("manifest state: %+v", c.manifest)
	}
}

// The two models genuinely differ AFTER a group closes. The open-group model
// deletes the group on snapshot_end, so a stray level bearing that snapshot_id
// has no open group and is dropped and counted. A {channel, snapshot_id} route
// keeps its entry, so the same stray level is routed to a shard and silently
// swallowed — no counter, no signal.
func TestDispatch_StrayLevelAfterSnapshotEndIsDroppedNotRouted(t *testing.T) {
	shards := make([]*Shard, 4)
	for i := range shards {
		shards[i] = NewShard(i, 4, NewEventsWriter(nil), nil)
	}
	m := NewMetrics("test", "test")
	c := NewCoordinator(context.Background(), shards, m)

	c.Dispatch(snapBegin(0, 4, 5))
	c.Dispatch(snapLevel(0, 5, 1000))
	c.Dispatch(snapEnd(0, 4, 5))
	for i := range shards {
		drain(shards[i])
	}

	// A level for the now-closed group arrives late.
	c.Dispatch(snapLevel(0, 5, 9999))

	for i := range shards {
		if n := len(drain(shards[i])); n != 0 {
			t.Errorf("shard %d must not receive a level for a closed group, got %d", i, n)
		}
	}
	if got := counterValue(m.SnapshotLevelDroppedTotal); got != 1 {
		t.Errorf("stray level must be counted as dropped: got %v want 1", got)
	}
}

// The wire omits instrument_id on snapshot_level. The shard keys all state by
// (channel_id, instrument_id), so the coordinator must stamp the identity the
// open group establishes — otherwise the record resolves to instrument 0 at the
// shard and the level is silently dropped.
func TestDispatch_SnapshotLevelStampedWithOpenGroupInstrument(t *testing.T) {
	c, shards := newTestCoordinator(t, 4)

	c.Dispatch(snapBegin(0, 5, 7)) // instrument 5 -> shard 1
	c.Dispatch(snapLevel(0, 7, 1000))

	got := drain(shards[1])
	if len(got) != 2 {
		t.Fatalf("shard 1 should hold begin+level, got %d", len(got))
	}
	level := got[1]
	if level.Type != "snapshot_level" {
		t.Fatalf("second record: %s", level.Type)
	}
	if level.InstrumentID != 5 {
		t.Errorf("level must be stamped with the open group's instrument: got %d want 5", level.InstrumentID)
	}
	// The incoming record genuinely carried no instrument id, so the stamp is
	// the only source of that identity.
	if snapLevel(0, 7, 1000).InstrumentID != 0 {
		t.Fatal("test fixture should model the wire: no instrument_id on snapshot_level")
	}
}

// A socket drop mid-snapshot-group must not leave the open group behind. The
// reader simply resumes after reconnecting, and Reset Count is unchanged by a
// socket-only drop, so the reset barrier does not cover this. Without an
// explicit signal the next snapshot_level — which carries no instrument_id — is
// stamped with the instrument that was in flight when the socket died and filed
// into an orphaned shadow on that instrument's shard.
func TestOnDisconnect_ClearsOpenGroup(t *testing.T) {
	c, shards := newTestCoordinator(t, 2)

	// A group opens on channel 0 for instrument 7, then the socket drops.
	c.Dispatch(snapBegin(0, 7, 3))
	if _, open := c.open[0]; !open {
		t.Fatal("setup: a group should be open")
	}
	for _, s := range shards {
		drain(s)
	}

	c.OnDisconnect()

	if len(c.open) != 0 {
		t.Errorf("the open group must not survive a disconnect: %+v", c.open)
	}

	// Every shard must be told to drop in-flight shadows.
	for i, s := range shards {
		var sawClear bool
		for {
			done := false
			select {
			case m := <-s.inbox:
				if m.kind == msgClearShadows {
					sawClear = true
				}
			default:
				done = true
			}
			if done {
				break
			}
		}
		if !sawClear {
			t.Errorf("shard %d was not told to clear shadows", i)
		}
	}

	// A level arriving before the next begin is now discarded, not misrouted.
	c.Dispatch(snapLevel(0, 3, 1000))
	for i, s := range shards {
		if recs := drain(s); len(recs) != 0 {
			t.Errorf("shard %d received an orphaned level: %+v", i, recs)
		}
	}
}

// clearShadows must abandon the half-built shadow without touching the live
// book: a shadow is never the live book, so a ready instrument keeps serving.
func TestClearShadows_DropsShadowButKeepsReadyBook(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)
	inst.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1)
	inst.BeginSnapshot(3, 5000, 10, 77, 0)
	inst.AddSnapshotLevel(3, 0, 900, 10, 1, 0)

	s.clearShadows()

	if inst.OpenSnapshot != nil {
		t.Error("the in-flight shadow must be dropped")
	}
	if inst.Status != StatusReady {
		t.Errorf("a ready instrument must keep serving: %v", inst.Status)
	}
	if inst.Bids[1000] == nil || inst.Bids[1000].QtyRaw != 50 {
		t.Errorf("the live book must be untouched: %+v", inst.Bids)
	}
}
