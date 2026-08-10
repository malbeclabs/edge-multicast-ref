package main

import "context"

// ManifestState is parity bookkeeping for the refdata manifest.
type ManifestState struct {
	Seq             uint16
	Valid           bool
	InstrumentCount uint32
}

// openGroup is the currently-open snapshot group on the snapshot port, per
// channel.
//
// This exists because `snapshot_level` records carry NO instrument_id — the wire
// omits it since the containing SnapshotBegin implies it. Routing must therefore
// follow the open group.
//
// Do NOT key snapshot routing by snapshot_id. Snapshot ID is monotonic per
// (channel_id, instrument_id), not per channel, so two instruments routinely sit
// at the same value within one cycle. A {channel_id, snapshot_id} route sends
// levels to whichever instrument last claimed that id — a different shard in
// general, where they are silently dropped. That is issue #30 against
// marketbyorder-bot. snapshot_id is used only to validate membership.
//
// Publishers MUST NOT interleave snapshot groups, so one open group per channel
// is sufficient state.
type openGroup struct {
	instrumentID uint32
	snapshotID   uint32
	shard        int
}

// Coordinator is the single-goroutine Dispatcher. It owns channel-scoped state
// and routes each record to exactly one shard, or to a broadcast/barrier/fence
// path. Shards own all instrument-scoped state.
//
// Dispatch is NOT safe for concurrent callers: it mutates its maps without
// locks, on the assumption that the only caller is the synchronous bot read loop.
type Coordinator struct {
	ctx     context.Context // escapes barrier/fence ack waits on shutdown
	shards  []*Shard
	n       int
	eventsW *EventsWriter
	metrics *Metrics

	// Reset Count is per publisher, and a group can carry two redundant
	// publishers interleaved on the same ports under different channel_ids. Held
	// as one global value, their differing-but-steady counts read as a reset on
	// every alternation between them: the barrier fired thousands of times a
	// minute, wiping refdata faster than it could be relearned, and every book
	// read-out went out with an empty symbol.
	resetCount map[uint8]uint8 // per channel_id
	manifest   ManifestState
	open       map[uint8]openGroup // per channel_id
}

func NewCoordinator(ctx context.Context, shards []*Shard, eventsW *EventsWriter, metrics *Metrics) *Coordinator {
	return &Coordinator{
		ctx:     ctx,
		shards:  shards,
		n:       len(shards),
		eventsW: eventsW,
		metrics: metrics,
		open:    map[uint8]openGroup{},

		resetCount: map[uint8]uint8{},
	}
}

// The reader discovers OnDisconnect through a runtime type assertion, which
// would silently stop firing if this method were ever renamed or removed. Fail
// the build instead.
var (
	_ Dispatcher      = (*Coordinator)(nil)
	_ DisconnectAware = (*Coordinator)(nil)
)

func (c *Coordinator) shardFor(instrumentID uint32) int {
	return int(instrumentID) % c.n
}

// Dispatch implements Dispatcher. Called synchronously from the bot read loop.
func (c *Coordinator) Dispatch(rec Record) {
	if prev, seen := c.resetCount[rec.ChannelID]; seen && rec.ResetCount != prev {
		c.runResetBarrier(rec)
		return
	} else if !seen {
		c.resetCount[rec.ChannelID] = rec.ResetCount
	}
	switch rec.Type {
	case "level_update", "book_clear", "instrument_definition", "instrument_reset", "trade", "liquidation":
		c.routeInstrument(rec)

	case "snapshot_begin":
		idx := c.shardFor(rec.InstrumentID)
		c.open[rec.ChannelID] = openGroup{
			instrumentID: rec.InstrumentID,
			snapshotID:   getUint32(rec.Fields, "snapshot_id"),
			shard:        idx,
		}
		c.send(idx, rec)

	case "snapshot_level":
		g, ok := c.open[rec.ChannelID]
		if !ok || g.snapshotID != getUint32(rec.Fields, "snapshot_id") {
			// No open group, or the level does not belong to it. Discard and
			// count — never guess an instrument.
			if c.metrics != nil {
				c.metrics.SnapshotLevelDroppedTotal.Inc()
			}
			return
		}
		// Stamp the instrument the open group identifies. The wire omits
		// instrument_id on snapshot_level, and the shard keys everything by
		// (channel_id, instrument_id) — without this the record resolves to
		// instrument 0 and the level is silently dropped.
		//
		// Stamping here, where the identity is known from SnapshotBegin, is what
		// lets the shard stay uniform. The alternative the sibling bot uses —
		// scanning every instrument for one whose open snapshot matches the
		// snapshot_id — picks arbitrarily when two instruments share an id, which
		// is issue #30.
		stamped := rec
		stamped.InstrumentID = g.instrumentID
		c.send(g.shard, stamped)

	case "snapshot_end":
		idx := c.shardFor(rec.InstrumentID)
		c.send(idx, rec)
		delete(c.open, rec.ChannelID)

	case "batch_boundary":
		// Carries no instrument_id and every shard evaluates crossed-book for the
		// instruments it touched, so it must reach all of them.
		for i := range c.shards {
			c.send(i, rec)
		}
		// Persisted HERE, exactly once, the same way channel_health is — never
		// from the shard path. A boundary is channel-scoped: it carries no
		// instrument_id and no symbol, and the broadcast above hands the same wire
		// message to every shard. Writing it from Shard.handle turned one wire
		// message into N near-identical `events` rows, and inconsistent ones at
		// that, since only the shard owning instrument 0 ever resolved a symbol
		// for it.
		c.eventsW.Write(ChannelEvent{Kind: KindBatchBoundary, Record: rec}, rec.ChannelID, "", 0, 0)

	case "heartbeat":
		// Channel-scoped, no book effect, no instrument — so no per-instrument
		// symbol or exponents.
		c.eventsW.Write(ChannelEvent{Record: rec}, rec.ChannelID, "", 0, 0)

	case "manifest_summary":
		c.applyManifest(rec)
		c.eventsW.Write(ChannelEvent{Record: rec}, rec.ChannelID, "", 0, 0)

	case "end_of_session":
		// Written before the fence so the row is enqueued even if the fence's
		// ack wait exits early on ctx cancellation.
		c.eventsW.Write(ChannelEvent{Record: rec}, rec.ChannelID, "", 0, 0)
		c.runFence(rec)
	}
}

// OnDisconnect drops the channel-scoped snapshot state that a socket drop
// invalidates, and tells every shard to discard its in-flight shadows.
//
// A reconnect resumes dispatching with no other signal. Without this, c.open
// still names the group that was in flight when the socket died, so any
// snapshot_level arriving before the next snapshot_begin is stamped with that
// stale instrument and filed into an orphaned shadow. A Reset Count change would
// clear it through the reset barrier, but a socket-only drop leaves Reset Count
// untouched, so nothing else covers this.
//
// No ack barrier is needed: each shard's inbox is FIFO, so the clear is ordered
// ahead of every record dispatched after the reconnect.
func (c *Coordinator) OnDisconnect() {
	c.open = map[uint8]openGroup{}
	for i := range c.shards {
		select {
		case c.shards[i].inbox <- shardMsg{kind: msgClearShadows}:
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Coordinator) routeInstrument(rec Record) {
	c.send(c.shardFor(rec.InstrumentID), rec)
}

func (c *Coordinator) send(idx int, rec Record) {
	r := rec
	select {
	case c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: &r}:
	case <-c.ctx.Done():
	}
}

// applyManifest records manifest state and, on a seq increase, broadcasts a
// prune so each shard can drop instruments that have fallen out of the manifest.
func (c *Coordinator) applyManifest(rec Record) {
	newSeq := toUint16(rec.Fields["manifest_seq"])
	prev := c.manifest.Seq
	c.manifest = ManifestState{
		Seq:             newSeq,
		Valid:           toUint8(rec.Fields["valid"]) != 0,
		InstrumentCount: toUint32(rec.Fields["instrument_count"]),
	}
	if !c.manifest.Valid || newSeq <= prev {
		return
	}
	for i := range c.shards {
		select {
		case c.shards[i].inbox <- shardMsg{kind: msgManifestPrune, seq: newSeq}:
		case <-c.ctx.Done():
			return
		}
	}
}

// runResetBarrier drains every shard, wipes the resetting channel's state, then
// re-routes the triggering record as the first record of that channel's new era.
// Sends and ack-waits are ctx-aware so a shutdown mid-barrier cannot wedge the
// read loop.
//
// Every shard is drained even though only one channel is wiped: the barrier's
// job is to order the wipe after all records already in flight, and any shard
// may hold records for this channel.
func (c *Coordinator) runResetBarrier(held Record) {
	ch := held.ChannelID
	acks := make(chan int, c.n)
	for _, s := range c.shards {
		go func(s *Shard) {
			select {
			case s.inbox <- shardMsg{kind: msgReset, ch: ch, ack: acks}:
			case <-c.ctx.Done():
			}
		}(s)
	}
	for i := 0; i < c.n; i++ {
		select {
		case <-acks:
		case <-c.ctx.Done():
			return
		}
	}

	if c.metrics != nil {
		c.metrics.ChannelResetsTotal.Inc()
	}
	delete(c.open, ch)
	// The manifest is published per channel but tracked once. Clearing it on any
	// channel's reset keeps the old behaviour of relearning it from the next
	// manifest_summary.
	c.manifest = ManifestState{}
	c.resetCount[ch] = held.ResetCount

	// resetCount[ch] now equals held.ResetCount, so this re-entry falls through
	// to normal classification.
	c.Dispatch(held)
}

// runFence drains every shard so a channel-scoped record is ordered strictly
// after all preceding instrument records. No state is wiped.
func (c *Coordinator) runFence(rec Record) {
	acks := make(chan int, c.n)
	for _, s := range c.shards {
		go func(s *Shard) {
			select {
			case s.inbox <- shardMsg{kind: msgFence, ack: acks}:
			case <-c.ctx.Done():
			}
		}(s)
	}
	for i := 0; i < c.n; i++ {
		select {
		case <-acks:
		case <-c.ctx.Done():
			return
		}
	}
}

func getUint32(fields map[string]any, key string) uint32 {
	return toUint32(fields[key])
}
