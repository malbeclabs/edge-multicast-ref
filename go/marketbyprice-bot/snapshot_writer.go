package main

import (
	"context"
	"sync"
	"time"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/clickhouse"
)

// SnapshotWriter coalesces book changes and writes a top-N read-out to
// level_snapshots at most once per coalesce interval per instrument.
//
// One writer per shard. The flush loop reaches instruments through
// withInstrument, which takes that shard's own mutex, so a flush never contends
// with another shard's goroutine.
type SnapshotWriter struct {
	ch               enqueuer
	depth            int
	coalesceInterval time.Duration
	tickInterval     time.Duration
	metrics          *Metrics

	// withInstrument runs fn under the owning shard's lock with the current
	// instrument, or nil when the shard does not have it.
	withInstrument func(instKey, func(*Instrument))

	// persists reports whether an instrument's rows should be written. Supplied
	// by the owning shard so the symbol filter applies identically to snapshots.
	persists func(symbol string) bool

	// now is the clock flushDue and MarkDirty read. A field, not a bare
	// time.Now() call, so a test can pace a coalesce window deterministically
	// without sleeping for real time.
	now func() time.Time

	mu sync.Mutex
	// dirty is keyed by instKey, NOT by a bare instrument ID. A shard owns
	// instruments across every channel for its id-modulo, so an id-only key would
	// fold two channels' books into one entry and silently persist whichever
	// flushed last.
	dirty map[instKey]*dirtyEntry
	// lastWrittenAt is the pacing state that survives the gap between a write and
	// the next MarkDirty. dirty entries are deleted at extraction (flushDue), so
	// nothing about a just-flushed instrument otherwise persists between writes;
	// without this map the very next MarkDirty would start a fresh entry with no
	// memory of when the instrument last wrote, and the coalesce interval would
	// throttle nothing.
	lastWrittenAt map[instKey]time.Time
	// generation is bumped by Reset to invalidate a batch already extracted from
	// dirty but not yet written.
	generation uint64
	resetCh    chan chan struct{}
}

type dirtyEntry struct {
	key            instKey
	dirtiedAt      time.Time
	nextAllowedAt  time.Time
	coalescedCount int
}

func NewSnapshotWriter(ch enqueuer, depth, coalesceMS int, m *Metrics, withInstrument func(instKey, func(*Instrument)), persists func(symbol string) bool) *SnapshotWriter {
	if persists == nil {
		persists = func(string) bool { return true }
	}
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          m,
		withInstrument:   withInstrument,
		persists:         persists,
		now:              time.Now,
		dirty:            map[instKey]*dirtyEntry{},
		lastWrittenAt:    map[instKey]time.Time{},
		resetCh:          make(chan chan struct{}, 1),
	}
}

// MarkDirty signals that an instrument's book changed. Only a real book mutation
// may call this: dirtying on a non-mutating event would rewrite an unchanged
// book on every batch boundary.
func (w *SnapshotWriter) MarkDirty(k instKey) {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if e, ok := w.dirty[k]; ok {
		e.coalescedCount++
		if w.metrics != nil {
			w.metrics.SnapshotCoalescesTotal.Inc()
		}
		return
	}
	// nextAllowedAt is derived from the last actual write, not from now: a fresh
	// dirty entry created shortly after a write must still honor the remainder of
	// that write's coalesce window, or the window never has any effect between
	// writes — only within the microseconds a batch spends being written.
	nextAllowedAt := now
	if last, ok := w.lastWrittenAt[k]; ok {
		if allowed := last.Add(w.coalesceInterval); allowed.After(now) {
			nextAllowedAt = allowed
		}
	}
	w.dirty[k] = &dirtyEntry{key: k, dirtiedAt: now, nextAllowedAt: nextAllowedAt}
}

// Reset clears pending work and invalidates any in-flight batch. It is
// serialized onto the writer goroutine and blocks until applied, so the caller
// can rely on no concurrent flush.
//
// It is ctx-aware so a shutdown already in flight — Run having returned via
// ctx.Done — cannot wedge the caller.
func (w *SnapshotWriter) Reset(ctx context.Context) {
	if w == nil {
		return
	}
	done := make(chan struct{})
	select {
	case w.resetCh <- done:
	case <-ctx.Done():
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (w *SnapshotWriter) doReset() {
	w.mu.Lock()
	w.dirty = map[instKey]*dirtyEntry{}
	// lastWrittenAt is cleared too: it is pacing history for book state that
	// Reset is about to discard, so honoring it after a reset would throttle the
	// first post-reset write against a book that no longer exists.
	w.lastWrittenAt = map[instKey]time.Time{}
	w.generation++
	w.mu.Unlock()
}

// Run is the tick loop. Returns when ctx is cancelled.
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			// Release a Reset caller whose ctx is broader than Run's, so it cannot
			// wait on a goroutine that has already returned. resetCh is buffered at
			// 1, so a non-blocking peek suffices.
			select {
			case done := <-w.resetCh:
				close(done)
			default:
			}
			return
		case done := <-w.resetCh:
			w.doReset()
			close(done)
		case <-tick.C:
			w.flushDue()
		}
	}
}

func (w *SnapshotWriter) flushDue() {
	w.mu.Lock()
	now := w.now()
	gen := w.generation
	var due []*dirtyEntry
	for k, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, k)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		w.mu.Lock()
		stale := w.generation != gen
		w.mu.Unlock()
		if stale {
			return // a Reset landed after this batch was extracted; abandon it
		}

		var (
			snap       LevelSnapshot
			lastSeq    uint64
			lastSendTS time.Time
			servable   bool
			bookStale  bool
		)
		w.withInstrument(e.key, func(inst *Instrument) {
			if inst == nil || inst.Status == StatusAwaitingSnapshot {
				return
			}
			snap = ComputeLevels(inst, w.depth)
			lastSeq = inst.LastAppliedMktdataSeq
			lastSendTS = inst.LastAppliedSendTS
			servable = true
			bookStale = inst.Status == StatusGap
		})
		if !servable {
			continue
		}
		if !w.persists(snap.Symbol) {
			continue
		}

		// The gauges are the read-out and are useful with persistence disabled, so
		// they are refreshed either way.
		w.updateBookGauges(snap)

		// Counted only when rows were actually enqueued. With persistence disabled
		// write is a no-op, and incrementing here regardless made
		// snapshot_writes_total report writes that never happened — a metric with
		// no writer behind it, which is exactly what this bot refuses to register.
		if n := w.write(e.key, snap, lastSeq, lastSendTS, bookStale, now); n > 0 && w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}

		// Record when this instrument actually wrote, so the NEXT MarkDirty — which
		// may land long after this flush loop returns — can compute a correctly
		// paced nextAllowedAt instead of starting from a clean slate.
		w.mu.Lock()
		w.lastWrittenAt[e.key] = now
		w.mu.Unlock()
	}
}

// updateBookGauges refreshes the book-state gauges from a freshly computed
// read-out, so they stay current without a separate sweep.
func (w *SnapshotWriter) updateBookGauges(snap LevelSnapshot) {
	if w.metrics == nil {
		return
	}
	m, sym := w.metrics, snap.Symbol

	m.BookLevels.WithLabelValues(sym, "bid").Set(float64(len(snap.Bids)))
	m.BookLevels.WithLabelValues(sym, "ask").Set(float64(len(snap.Asks)))

	if len(snap.Bids) > 0 {
		m.BookTopPrice.WithLabelValues(sym, "bid").Set(snap.Bids[0].Price)
		m.BookTopQty.WithLabelValues(sym, "bid").Set(snap.Bids[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(sym, "bid")
		m.BookTopQty.DeleteLabelValues(sym, "bid")
	}
	if len(snap.Asks) > 0 {
		m.BookTopPrice.WithLabelValues(sym, "ask").Set(snap.Asks[0].Price)
		m.BookTopQty.WithLabelValues(sym, "ask").Set(snap.Asks[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(sym, "ask")
		m.BookTopQty.DeleteLabelValues(sym, "ask")
	}

	if len(snap.Bids) > 0 && len(snap.Asks) > 0 {
		bestBid, bestAsk := snap.Bids[0].Price, snap.Asks[0].Price
		if mid := (bestBid + bestAsk) / 2; mid != 0 {
			m.BookSpreadBps.WithLabelValues(sym).Set((bestAsk - bestBid) / mid * 10000)
		}
	} else {
		m.BookSpreadBps.DeleteLabelValues(sym)
	}
}

// write enqueues one read-out and returns how many rows it produced. Zero means
// nothing reached ClickHouse — persistence is disabled, or the book held no
// levels — and the caller must not count it as a write.
func (w *SnapshotWriter) write(k instKey, snap LevelSnapshot, lastSeq uint64, lastSendTS time.Time, bookStale bool, now time.Time) int {
	if w.ch == nil {
		return 0
	}
	staleFlag, crossedFlag := uint8(0), uint8(0)
	if bookStale {
		staleFlag = 1
	}
	if snap.Crossed {
		crossedFlag = 1
	}
	// A nil DepthBound is unknown and must encode as SQL NULL. Zero is the
	// publisher's positive claim of a complete book — a different statement, and
	// the only value under which cumulative_qty is exhaustive.
	var depthBound any
	if snap.DepthBound != nil {
		depthBound = *snap.DepthBound
	}
	nowStr := clickhouse.ChTime(now)

	// publisher_send_ts is the send timestamp of the last wire record this book
	// reflects, NOT recv_ts. The schema derives wire_latency_ms as
	// recv_ts - publisher_send_ts, so writing recv_ts into both pinned that
	// column at 0.0 for every row — advertising a latency measurement that could
	// not exist. A zero value here means no record has been applied yet, which
	// encodes as the epoch, matching how `events` already spells an absent send
	// timestamp rather than silently reading as zero latency.
	sendTS := lastSendTS
	if sendTS.IsZero() {
		sendTS = time.Unix(0, 0).UTC()
	}
	sendStr := clickhouse.ChTime(sendTS)

	rows := 0
	emit := func(side string, levels []Level) {
		for i, lvl := range levels {
			rows++
			w.ch.Enqueue("level_snapshots", map[string]any{
				"recv_ts":           nowStr,
				"publisher_send_ts": sendStr,
				"channel_id":        k.ch,
				"instrument_id":     k.id,
				"symbol":            snap.Symbol,
				"last_applied_seq":  lastSeq,
				"side":              side,
				"level_idx":         uint16(i),
				"price":             lvl.Price,
				"qty":               lvl.Qty,
				"order_count":       lvl.OrderCount,
				"cumulative_qty":    lvl.CumulativeQty,
				"stale":             staleFlag,
				"crossed":           crossedFlag,
				"depth_bound":       depthBound,
			})
		}
	}
	emit("bid", snap.Bids)
	emit("ask", snap.Asks)
	return rows
}
