package main

import (
	"context"
	"sync"
	"time"
)

// enqueuer is satisfied by *ClickhouseClient and by test stubs.
type enqueuer interface {
	Enqueue(table string, row map[string]any) bool
}

// SnapshotWriter coalesces book changes and emits level-snapshot rows to ClickHouse
// at most once per coalesceInterval per instrument.
type SnapshotWriter struct {
	ch               enqueuer
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

func NewSnapshotWriter(ch enqueuer, depth int, coalesceMS int, metrics *Metrics, channelID uint8, withInstrument func(uint32, func(*Instrument))) *SnapshotWriter {
	return &SnapshotWriter{
		ch:               ch,
		depth:            depth,
		coalesceInterval: time.Duration(coalesceMS) * time.Millisecond,
		tickInterval:     10 * time.Millisecond,
		metrics:          metrics,
		dirty:            map[uint32]*dirtyEntry{},
		channel:          channelID,
		withInstrument:   withInstrument,
		resetCh:          make(chan chan struct{}, 1),
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
//
// Reset is ctx-aware so a shutdown in flight (Run already returned via
// ctx.Done) cannot wedge the caller. If ctx is cancelled before Run sees the
// request, Reset returns without applying it — which is safe because the
// writer is also shutting down.
func (w *SnapshotWriter) Reset(ctx context.Context) {
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
			// Defense-in-depth: drain any pending Reset() caller so their
			// <-done escape is not solely dependent on their own ctx. In
			// production all SnapshotWriter callers (shards) pass the same
			// ctx as Run, so Reset's own ctx.Done fires alongside Run's and
			// this drain is redundant; we keep it so a future caller with a
			// broader ctx than Run's still cannot wedge. resetCh is buffered
			// (cap 1), so a non-blocking peek suffices.
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
			snap      LevelSnapshot
			instID    uint32
			symbol    string
			lastSeq   uint64
			ready     bool
			bookStale bool
		)
		w.withInstrument(e.instrumentID, func(inst *Instrument) {
			if inst == nil || inst.Status == StatusAwaitingSnapshot {
				return
			}
			snap = ComputeLevels(inst, w.depth)
			instID = inst.ID
			symbol = inst.Symbol
			lastSeq = inst.LastAppliedMktdataSeq
			ready = true
			bookStale = inst.Status == StatusGap
		})
		if !ready {
			continue
		}
		w.updateBookGauges(snap, symbol)
		w.write(snap, instID, symbol, lastSeq, bookStale, now)
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

// updateBookGauges sets the book-state Prometheus gauges from a freshly computed
// LevelSnapshot. It is called on every flush so the gauges stay current.
func (w *SnapshotWriter) updateBookGauges(snap LevelSnapshot, symbol string) {
	if w.metrics == nil {
		return
	}
	m := w.metrics

	// Order counts: use raw order maps via snap — snap.Bids/Asks are aggregated
	// price levels, not individual orders. We want individual order counts, which
	// are available as the OrderCount fields summed across levels, but the simpler
	// and more accurate source is the instrument's map sizes. However, we only have
	// the snapshot here, so derive from the level OrderCount totals.
	var bidOrders, askOrders uint32
	for _, lvl := range snap.Bids {
		bidOrders += lvl.OrderCount
	}
	for _, lvl := range snap.Asks {
		askOrders += lvl.OrderCount
	}
	m.BookOrders.WithLabelValues(symbol, "bid").Set(float64(bidOrders))
	m.BookOrders.WithLabelValues(symbol, "ask").Set(float64(askOrders))

	// Top-of-book price and qty.
	if len(snap.Bids) > 0 {
		m.BookTopPrice.WithLabelValues(symbol, "bid").Set(snap.Bids[0].Price)
		m.BookTopQty.WithLabelValues(symbol, "bid").Set(snap.Bids[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(symbol, "bid")
		m.BookTopQty.DeleteLabelValues(symbol, "bid")
	}
	if len(snap.Asks) > 0 {
		m.BookTopPrice.WithLabelValues(symbol, "ask").Set(snap.Asks[0].Price)
		m.BookTopQty.WithLabelValues(symbol, "ask").Set(snap.Asks[0].Qty)
	} else {
		m.BookTopPrice.DeleteLabelValues(symbol, "ask")
		m.BookTopQty.DeleteLabelValues(symbol, "ask")
	}

	// Spread in bps.
	if len(snap.Bids) > 0 && len(snap.Asks) > 0 {
		bestBid := snap.Bids[0].Price
		bestAsk := snap.Asks[0].Price
		mid := (bestBid + bestAsk) / 2
		if mid != 0 {
			m.BookSpreadBps.WithLabelValues(symbol).Set((bestAsk - bestBid) / mid * 10000)
		}
	} else {
		m.BookSpreadBps.DeleteLabelValues(symbol)
	}
}

func (w *SnapshotWriter) write(snap LevelSnapshot, instID uint32, symbol string, lastSeq uint64, stale bool, now time.Time) {
	if w.ch == nil {
		return
	}
	staleUInt8 := uint8(0)
	if stale {
		staleUInt8 = 1
	}
	nowStr := chTime(now)
	enqueue := func(row map[string]any) {
		w.ch.Enqueue("level_snapshots", row)
	}
	for i, lvl := range snap.Bids {
		enqueue(map[string]any{
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
			"stale":             staleUInt8,
		})
	}
	for i, lvl := range snap.Asks {
		enqueue(map[string]any{
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
			"stale":             staleUInt8,
		})
	}
}
