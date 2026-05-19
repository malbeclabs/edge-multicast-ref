package main

import (
	"context"
	"sync"
	"time"
)

// SnapshotWriter coalesces book changes and emits level-snapshot rows to ClickHouse
// at most once per coalesceInterval per instrument.
type SnapshotWriter struct {
	ch               *ClickhouseClient
	depth            int
	coalesceInterval time.Duration
	tickInterval     time.Duration
	metrics          *Metrics

	mu             sync.Mutex
	dirty          map[uint32]*dirtyEntry
	withInstrument func(uint32, func(*Instrument)) // runs fn under the channel lock with the current instrument (or nil)
	channel        uint8
	generation     uint64 // guarded by mu; bumped on Reset to invalidate in-flight flush batches
	resetCh        chan chan struct{}
}

type dirtyEntry struct {
	instrumentID   uint32
	dirtiedAt      time.Time
	nextAllowedAt  time.Time
	coalescedCount int
}

func NewSnapshotWriter(ch *ClickhouseClient, depth int, coalesceMS int, metrics *Metrics, channelID uint8, withInstrument func(uint32, func(*Instrument))) *SnapshotWriter {
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          metrics,
		dirty:            map[uint32]*dirtyEntry{},
		channel:          channelID,
		withInstrument:   withInstrument,
		resetCh:          make(chan chan struct{}),
	}
}

// MarkDirty signals that an instrument's book changed.
func (w *SnapshotWriter) MarkDirty(instrumentID uint32) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if e, ok := w.dirty[instrumentID]; ok {
		e.coalescedCount++
		if w.metrics != nil {
			w.metrics.SnapshotCoalescesTotal.Inc()
		}
		return
	}
	w.dirty[instrumentID] = &dirtyEntry{
		instrumentID:  instrumentID,
		dirtiedAt:     now,
		nextAllowedAt: now,
	}
}

// Reset clears pending dirty state and invalidates any in-flight flush batch.
// It is serialized onto the writer goroutine (via resetCh) and blocks until that
// goroutine has applied the reset, so the caller can rely on no concurrent flush.
func (w *SnapshotWriter) Reset() {
	done := make(chan struct{})
	w.resetCh <- done
	<-done
}

func (w *SnapshotWriter) doReset() {
	w.mu.Lock()
	w.dirty = map[uint32]*dirtyEntry{}
	w.generation++
	w.mu.Unlock()
}

// Run is the writer's tick loop. Returns when ctx is cancelled.
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
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
	now := time.Now()
	gen := w.generation
	due := []*dirtyEntry{}
	for id, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, id)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		w.mu.Lock()
		stale := w.generation != gen
		w.mu.Unlock()
		if stale {
			return // a Reset happened after this batch was extracted; abandon it
		}
		var (
			snap    LevelSnapshot
			instID  uint32
			symbol  string
			lastSeq uint64
			ready   bool
		)
		w.withInstrument(e.instrumentID, func(inst *Instrument) {
			if inst == nil || inst.Status != StatusReady {
				return
			}
			snap = ComputeLevels(inst, w.depth)
			instID = inst.ID
			symbol = inst.Symbol
			lastSeq = inst.LastAppliedMktdataSeq
			ready = true
		})
		if !ready {
			continue
		}
		w.write(snap, instID, symbol, lastSeq, now)
		_ = e.coalescedCount
		if w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}
		w.mu.Lock()
		if e2, ok := w.dirty[e.instrumentID]; ok {
			rearm := now.Add(w.coalesceInterval)
			if e2.nextAllowedAt.Before(rearm) {
				e2.nextAllowedAt = rearm
			}
		}
		w.mu.Unlock()
	}
}

func (w *SnapshotWriter) write(snap LevelSnapshot, instID uint32, symbol string, lastSeq uint64, now time.Time) {
	if w.ch == nil {
		return
	}
	nowStr := chTime(now)
	for i, lvl := range snap.Bids {
		w.ch.Enqueue("level_snapshots", map[string]any{
			"recv_ts":           nowStr,
			"publisher_send_ts": nowStr,
			"channel_id":        w.channel,
			"instrument_id":     instID,
			"symbol":            symbol,
			"last_applied_seq":  lastSeq,
			"side":              "bid",
			"level_idx":         uint16(i),
			"price":             lvl.Price,
			"qty":               lvl.Qty,
			"order_count":       lvl.OrderCount,
			"cumulative_qty":    lvl.CumulativeQty,
		})
	}
	for i, lvl := range snap.Asks {
		w.ch.Enqueue("level_snapshots", map[string]any{
			"recv_ts":           nowStr,
			"publisher_send_ts": nowStr,
			"channel_id":        w.channel,
			"instrument_id":     instID,
			"symbol":            symbol,
			"last_applied_seq":  lastSeq,
			"side":              "ask",
			"level_idx":         uint16(i),
			"price":             lvl.Price,
			"qty":               lvl.Qty,
			"order_count":       lvl.OrderCount,
			"cumulative_qty":    lvl.CumulativeQty,
		})
	}
}
