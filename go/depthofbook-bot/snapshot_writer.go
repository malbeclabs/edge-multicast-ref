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

	mu      sync.Mutex
	dirty   map[uint32]*dirtyEntry
	lookup  func(uint32) *Instrument // injected by bot main; returns current instrument or nil
	channel uint8
}

type dirtyEntry struct {
	instrumentID   uint32
	dirtiedAt      time.Time
	nextAllowedAt  time.Time
	coalescedCount int
}

func NewSnapshotWriter(ch *ClickhouseClient, depth int, coalesceMS int, metrics *Metrics, channelID uint8, lookup func(uint32) *Instrument) *SnapshotWriter {
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          metrics,
		dirty:            map[uint32]*dirtyEntry{},
		channel:          channelID,
		lookup:           lookup,
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

// Run is the writer's tick loop. Returns when ctx is cancelled.
func (w *SnapshotWriter) Run(ctx context.Context) {
	tick := time.NewTicker(w.tickInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			w.flushDue()
		}
	}
}

func (w *SnapshotWriter) flushDue() {
	w.mu.Lock()
	now := time.Now()
	due := []*dirtyEntry{}
	for id, e := range w.dirty {
		if !e.nextAllowedAt.After(now) {
			due = append(due, e)
			delete(w.dirty, id)
		}
	}
	w.mu.Unlock()

	for _, e := range due {
		inst := w.lookup(e.instrumentID)
		if inst == nil || inst.Status != StatusReady {
			continue
		}
		snap := ComputeLevels(inst, w.depth)
		w.write(snap, inst, e.dirtiedAt, now)
		_ = e.coalescedCount // metric already incremented per coalesce
		if w.metrics != nil {
			w.metrics.SnapshotWritesTotal.Inc()
			w.metrics.SnapshotLagMs.Observe(float64(now.Sub(e.dirtiedAt).Milliseconds()))
		}
	}
}

func (w *SnapshotWriter) write(snap LevelSnapshot, inst *Instrument, _ time.Time, now time.Time) {
	if w.ch == nil {
		return
	}
	for i, lvl := range snap.Bids {
		w.ch.Enqueue("level_snapshots", map[string]any{
			"recv_ts":           now,
			"publisher_send_ts": now,
			"channel_id":        w.channel,
			"instrument_id":     inst.ID,
			"symbol":            inst.Symbol,
			"last_applied_seq":  inst.LastAppliedMktdataSeq,
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
			"recv_ts":           now,
			"publisher_send_ts": now,
			"channel_id":        w.channel,
			"instrument_id":     inst.ID,
			"symbol":            inst.Symbol,
			"last_applied_seq":  inst.LastAppliedMktdataSeq,
			"side":              "ask",
			"level_idx":         uint16(i),
			"price":             lvl.Price,
			"qty":               lvl.Qty,
			"order_count":       lvl.OrderCount,
			"cumulative_qty":    lvl.CumulativeQty,
		})
	}
}
