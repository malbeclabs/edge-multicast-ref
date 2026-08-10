package main

import "context"

// Coordinator is the single-goroutine Dispatcher. It owns channel-scoped state
// and routes each record to exactly one shard (by instrument_id % N), or to a
// direct-write / barrier / fence path. Shards own all instrument-scoped state.
//
// Dispatch is NOT safe for concurrent callers: it mutates resetCount/
// snapshotRoute/seqLast/manifest without locks, on the assumption that the
// only caller is the synchronous bot read loop.
type Coordinator struct {
	ctx     context.Context // used to escape barrier/fence ack waits on shutdown
	shards  []*Shard
	n       int
	eventsW *EventsWriter
	metrics *Metrics

	// Reset Count is per publisher, and a group can carry two redundant
	// publishers interleaved on the same ports under different channel_ids.
	// Held as one global value, their differing-but-steady counts read as a
	// reset on every alternation between them, wiping instrument state
	// faster than it could be relearned.
	resetCount    map[uint8]uint8 // per channel_id
	manifest      ManifestState   // parity bookkeeping; not read for logic
	seqLast       map[string]uint64
	snapshotRoute map[snapKey]int
}

// snapKey is defined in shard.go (Task 3). Do NOT redeclare it here — a second
// declaration in the same package is a compile error.

// NewCoordinator builds a Coordinator. ctx is used solely to break barrier and
// fence ack-waits on shutdown so the coordinator cannot wedge when shards or
// SnapshotWriters have exited.
func NewCoordinator(ctx context.Context, shards []*Shard, eventsW *EventsWriter, metrics *Metrics) *Coordinator {
	return &Coordinator{
		ctx:           ctx,
		shards:        shards,
		n:             len(shards),
		eventsW:       eventsW,
		metrics:       metrics,
		seqLast:       map[string]uint64{},
		snapshotRoute: map[snapKey]int{},

		resetCount: map[uint8]uint8{},
	}
}

// Dispatch implements Dispatcher. Called synchronously from the bot read loop.
func (c *Coordinator) Dispatch(rec Record) {
	// Channel-reset barrier: reset_count change. (Implemented in Task 7.)
	if prev, seen := c.resetCount[rec.ChannelID]; seen && rec.ResetCount != prev {
		c.runResetBarrier(rec)
		return
	} else if !seen {
		c.resetCount[rec.ChannelID] = rec.ResetCount
	}
	c.seqLast[rec.Port] = rec.SequenceNumber

	switch rec.Type {
	case "order_add", "order_cancel", "order_execute",
		"instrument_definition", "instrument_reset", "trade":
		c.routeInstrument(rec)

	case "snapshot_begin":
		idx := int(rec.InstrumentID) % c.n
		c.snapshotRoute[snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")}] = idx
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_order":
		key := snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")}
		idx, ok := c.snapshotRoute[key]
		if !ok {
			if c.metrics != nil {
				c.metrics.SnapshotOrderDroppedTotal.Inc()
			}
			return
		}
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_end":
		idx := int(rec.InstrumentID) % c.n
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}
		delete(c.snapshotRoute, snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")})

	case "heartbeat", "manifest_summary":
		c.writeChannelHealth(rec) // implemented in Task 8

	case "end_of_session", "batch_boundary":
		c.runFence(rec) // implemented in Task 8
	}
}

func (c *Coordinator) routeInstrument(rec Record) {
	idx := int(rec.InstrumentID) % c.n
	c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}
}

func recPtr(rec Record) *Record {
	r := rec
	return &r
}

// --- barrier / fence / channel-health ---

// runResetBarrier executes the in-band FIFO reset barrier, then routes the
// held triggering record as the first new-era frame.
//
// Barrier sends and ack-waits are ctx-aware: if ctx is cancelled mid-barrier
// (the bot is shutting down), we abandon the barrier and return without
// routing the held record. No consistency requirement to uphold post-shutdown.
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
	for k := range c.snapshotRoute {
		if k.ch == ch {
			delete(c.snapshotRoute, k)
		}
	}
	c.seqLast = map[string]uint64{}
	c.manifest = ManifestState{}
	c.resetCount[ch] = held.ResetCount

	// Route the held record as the first new-era frame, via the full classifier.
	// resetCount[ch] now equals held.ResetCount, so this re-entry into Dispatch
	// falls through to normal classification.
	c.Dispatch(held)
}

// runFence drains every shard (FIFO marker/ack, no state wipe) so the fence
// record's ClickHouse row lands strictly after all preceding instrument rows.
// Ctx-aware on the same shutdown-safety grounds as runResetBarrier.
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
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}

// writeChannelHealth writes heartbeat / manifest_summary directly (no fence).
func (c *Coordinator) writeChannelHealth(rec Record) {
	if rec.Type == "manifest_summary" {
		c.manifest = ManifestState{
			Seq:             toUint16(rec.Fields["manifest_seq"]),
			Valid:           toUint8(rec.Fields["valid"]) != 0,
			InstrumentCount: toUint32(rec.Fields["instrument_count"]),
		}
	}
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}
