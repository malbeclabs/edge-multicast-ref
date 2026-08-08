package main

import (
	"context"
	"testing"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// newTestSnapshotWriter wires a writer over a fixed instrument map.
func newTestSnapshotWriter(t *testing.T, st enqueuer, m *Metrics, insts map[instKey]*Instrument) *SnapshotWriter {
	t.Helper()
	return NewSnapshotWriter(st, 5, 0 /*coalesce off for determinism*/, m, func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	}, nil)
}

func readySnapshotInstrument(id uint32, symbol string) *Instrument {
	inst := NewInstrument(id, symbol, 0, 0)
	inst.Status = StatusReady
	inst.ApplyLevelUpdate(0, 1000, 50, 1, 0, 1) // bid
	inst.ApplyLevelUpdate(1, 1100, 60, 1, 0, 1) // ask
	return inst
}

// Two channels sharing an instrument ID must not collide. A dirty map keyed by
// bare uint32 — as the sibling market-by-order bot uses — would fold these two
// books into one entry and persist whichever flushed last.
func TestSnapshotWriter_KeysByChannelAndInstrument(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{
		{0, 11}: readySnapshotInstrument(11, "SYM-CH0"),
		{1, 11}: readySnapshotInstrument(11, "SYM-CH1"),
	}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	w.MarkDirty(instKey{0, 11})
	w.MarkDirty(instKey{1, 11})
	w.flushDue()

	seen := map[string]bool{}
	for _, row := range st.rows["level_snapshots"] {
		seen[row["symbol"].(string)] = true
	}
	if !seen["SYM-CH0"] || !seen["SYM-CH1"] {
		t.Errorf("both channels' books must be written, saw %v", seen)
	}
}

// Marking the same instrument repeatedly before a flush coalesces into one write.
func TestSnapshotWriter_CoalescesRepeatedMarks(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")}
	w := newTestSnapshotWriter(t, st, m, insts)

	for i := 0; i < 5; i++ {
		w.MarkDirty(instKey{0, 11})
	}
	w.flushDue()

	// depth 5, one bid and one ask level present => 2 rows for one flush.
	if got := len(st.rows["level_snapshots"]); got != 2 {
		t.Errorf("one coalesced flush should write 2 rows, got %d", got)
	}
	if got := counterValue(m.SnapshotCoalescesTotal); got != 4 {
		t.Errorf("coalesces: got %v want 4", got)
	}
	if got := counterValue(m.SnapshotWritesTotal); got != 1 {
		t.Errorf("writes: got %v want 1", got)
	}
}

// crossed and depth_bound must reach the row: they are the two columns this feed
// adds over the sibling's schema.
func TestSnapshotWriter_WritesCrossedAndDepthBound(t *testing.T) {
	st := newStubEnqueuer()
	inst := NewInstrument(11, "SYM", 0, 0)
	inst.Status = StatusReady
	inst.ApplyLevelUpdate(0, 1200, 50, 1, 0, 1) // bid above ask: crossed
	inst.ApplyLevelUpdate(1, 1000, 60, 1, 0, 1)
	bound := uint32(25)
	inst.DepthBound = &bound

	insts := map[instKey]*Instrument{{0, 11}: inst}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	rows := st.rows["level_snapshots"]
	if len(rows) == 0 {
		t.Fatal("expected rows")
	}
	if rows[0]["crossed"] != uint8(1) {
		t.Errorf("crossed: got %v want 1", rows[0]["crossed"])
	}
	if rows[0]["depth_bound"] != uint32(25) {
		t.Errorf("depth_bound: got %v want 25", rows[0]["depth_bound"])
	}
}

// An unknown depth bound is NULL, never 0 — 0 is the publisher's positive claim
// of a complete book, which is a different statement.
func TestSnapshotWriter_UnknownDepthBoundIsNull(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")} // DepthBound nil
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	if rows := st.rows["level_snapshots"]; rows[0]["depth_bound"] != nil {
		t.Errorf("unknown depth bound must be nil, got %v", rows[0]["depth_bound"])
	}
}

// An instrument that has never been snapshotted has no servable book.
func TestSnapshotWriter_SkipsAwaitingSnapshot(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: NewInstrument(11, "SYM", 0, 0)} // awaiting
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	if got := len(st.rows["level_snapshots"]); got != 0 {
		t.Errorf("awaiting-snapshot instrument must not be written, got %d rows", got)
	}
}

// A gapped instrument is still written, flagged stale, so a consumer can see the
// book exists but is not current.
func TestSnapshotWriter_MarksGappedBookStale(t *testing.T) {
	st := newStubEnqueuer()
	inst := readySnapshotInstrument(11, "SYM")
	inst.Status = StatusGap
	insts := map[instKey]*Instrument{{0, 11}: inst}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	rows := st.rows["level_snapshots"]
	if len(rows) == 0 || rows[0]["stale"] != uint8(1) {
		t.Errorf("gapped book must be flagged stale: %+v", rows)
	}
}

// Reset clears pending work and bumps the generation so an in-flight batch is
// abandoned rather than written against post-reset state.
func TestSnapshotWriter_ResetClearsDirty(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.Run(ctx)

	w.MarkDirty(instKey{0, 11})
	w.Reset(ctx)

	w.mu.Lock()
	n := len(w.dirty)
	w.mu.Unlock()
	if n != 0 {
		t.Errorf("Reset must clear the dirty map, %d entries remain", n)
	}
}

// Reset must not wedge when Run has already exited on shutdown.
func TestSnapshotWriter_ResetDoesNotWedgeAfterShutdown(t *testing.T) {
	st := newStubEnqueuer()
	insts := map[instKey]*Instrument{}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Run never starts

	done := make(chan struct{})
	go func() { w.Reset(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Reset wedged after shutdown")
	}
}

func TestSnapshotWriter_NilClientIsNoOp(t *testing.T) {
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")}
	w := NewSnapshotWriter(nil, 5, 0, NewMetrics("t", "t"), func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	}, nil)
	w.MarkDirty(instKey{0, 11})
	w.flushDue() // must not panic
}

// With persistence disabled nothing is enqueued, so snapshot_writes_total must
// stay at zero. Incrementing it unconditionally reported writes that never
// happened — a metric with no writer behind it, which is precisely what this bot
// refuses to register.
func TestSnapshotWriter_NilClientCountsNoWrites(t *testing.T) {
	m := NewMetrics("t", "t")
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")}
	w := NewSnapshotWriter(enqueuerFor(nil), 5, 0, m, func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	}, nil)

	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	if got := counterValue(m.SnapshotWritesTotal); got != 0 {
		t.Errorf("a nil client writes nothing, so snapshot_writes_total must stay 0: got %v", got)
	}
	// The read-out itself still happened, so the book gauges are still current.
	if got := gaugeRead(m.BookLevels.WithLabelValues("SYM", "bid")); got != 1 {
		t.Errorf("book gauges are the read-out and must still populate: got %v want 1", got)
	}
}

// wire_latency_ms on level_snapshots is MATERIALIZED as
// recv_ts - publisher_send_ts. Writing the flush timestamp into both columns
// pinned it at 0.0 for every row that could ever exist. publisher_send_ts must
// instead carry the send timestamp of the last record the book actually applied.
func TestSnapshotWriter_PublisherSendTSComesFromTheLastAppliedRecord(t *testing.T) {
	st := newStubEnqueuer()
	inst := readySnapshotInstrument(11, "SYM")
	sendTS := time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC)
	inst.LastAppliedSendTS = sendTS

	insts := map[instKey]*Instrument{{0, 11}: inst}
	w := newTestSnapshotWriter(t, st, NewMetrics("t", "t"), insts)
	w.MarkDirty(instKey{0, 11})
	w.flushDue()

	rows := st.rows["level_snapshots"]
	if len(rows) == 0 {
		t.Fatal("expected rows")
	}
	if got, want := rows[0]["publisher_send_ts"], clickhouse.ChTime(sendTS); got != want {
		t.Errorf("publisher_send_ts: got %v want %v", got, want)
	}
	if rows[0]["recv_ts"] == rows[0]["publisher_send_ts"] {
		t.Error("recv_ts and publisher_send_ts must differ, or wire_latency_ms is structurally zero")
	}
}

// applyOne is what maintains that timestamp, alongside the sequence trackers —
// and only on the path that genuinely mutated the book.
func TestApplyOne_RecordsLastAppliedSendTS(t *testing.T) {
	s := newTestShard(t)
	k := instKey{0, 11}
	inst := readyInstrumentInShard(t, s, k, 5)

	rec := levelUpdateRec(11, 900, 6, "bid", 1000, 50)
	rec.SendTSNS = 1754568000000000000
	s.applyDelta(k, rec)

	if !inst.LastAppliedSendTS.Equal(rec.sendTime()) {
		t.Errorf("an applied delta must record its send timestamp: got %v want %v",
			inst.LastAppliedSendTS, rec.sendTime())
	}

	// A malformed book_clear applies nothing, so it must not move the timestamp
	// any more than it moves the sequence trackers.
	bad := bookClearRec(11, 901, 7, "both", "from_price", 1000)
	bad.SendTSNS = 1754568999000000000
	s.applyDelta(k, bad)

	if !inst.LastAppliedSendTS.Equal(rec.sendTime()) {
		t.Errorf("a discarded book_clear must not advance the send timestamp: got %v", inst.LastAppliedSendTS)
	}
}

// fakeClock lets a test advance simulated time deterministically instead of
// sleeping for real milliseconds.
type fakeClock struct {
	t time.Time
}

func (c *fakeClock) now() time.Time { return c.t }

// The coalesce interval must pace writes across the quiet gap between a write
// and the next MarkDirty, not just within the microseconds a single flush takes.
// A dirty entry is deleted from the map the moment it is extracted in flushDue,
// so if nothing records when an instrument last actually wrote, the very next
// MarkDirty starts a brand new entry with no memory of that write — and the next
// tick flushes it immediately, regardless of the configured coalesce interval.
//
// This drives 30 simulated 10ms ticks (300ms of simulated time) against a 250ms
// coalesce interval, marking the instrument dirty on every tick as a 1kHz feed
// would. A correct writer paces to at most 2 writes (one at t=0, one once the
// 250ms window elapses); the pre-fix writer wrote on every tick — 30 writes.
func TestSnapshotWriter_CoalesceIntervalPacesWritesAcrossTicks(t *testing.T) {
	st := newStubEnqueuer()
	m := NewMetrics("t", "t")
	insts := map[instKey]*Instrument{{0, 11}: readySnapshotInstrument(11, "SYM")}
	clock := &fakeClock{t: time.Unix(0, 0)}

	w := NewSnapshotWriter(st, 5, 250, m, func(k instKey, fn func(*Instrument)) {
		fn(insts[k])
	}, nil)
	w.now = clock.now

	const tick = 10 * time.Millisecond
	for i := 0; i < 30; i++ {
		w.MarkDirty(instKey{0, 11})
		w.flushDue()
		clock.t = clock.t.Add(tick)
	}

	if got := counterValue(m.SnapshotWritesTotal); got > 2 {
		t.Errorf("coalesce interval must pace writes: got %v writes over 300ms of simulated ticks at a 250ms interval, want <= 2 (tick-rate pacing would give ~30)", got)
	}
	if got := counterValue(m.SnapshotWritesTotal); got < 1 {
		t.Errorf("expected at least one write, got %v", got)
	}
}
