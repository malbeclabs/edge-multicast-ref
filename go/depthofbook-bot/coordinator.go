package main

// Coordinator is the single-goroutine Dispatcher. It owns channel-scoped state
// and routes each record to exactly one shard (by instrument_id % N), or to a
// direct-write / barrier / fence path. Shards own all instrument-scoped state.
type Coordinator struct {
	shards  []*Shard
	n       int
	eventsW *EventsWriter
	metrics *Metrics

	resetSeen     bool
	resetCount    uint8
	manifest      ManifestState // parity bookkeeping; not read for logic
	seqLast       map[string]uint64
	snapshotRoute map[snapKey]int
}

// snapKey is defined in shard.go (Task 3). Do NOT redeclare it here — a second
// declaration in the same package is a compile error.

func NewCoordinator(shards []*Shard, eventsW *EventsWriter, metrics *Metrics) *Coordinator {
	return &Coordinator{
		shards:        shards,
		n:             len(shards),
		eventsW:       eventsW,
		metrics:       metrics,
		seqLast:       map[string]uint64{},
		snapshotRoute: map[snapKey]int{},
	}
}

// Dispatch implements Dispatcher. Called synchronously from the bot read loop.
func (c *Coordinator) Dispatch(rec Record) {
	// Channel-reset barrier: reset_count change. (Implemented in Task 7.)
	if c.resetSeen && rec.ResetCount != c.resetCount {
		c.runResetBarrier(rec)
		return
	}
	if !c.resetSeen {
		c.resetSeen = true
		c.resetCount = rec.ResetCount
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

// --- temporary stubs, replaced in Tasks 7 and 8 ---

func (c *Coordinator) runResetBarrier(rec Record) {
	// Task 7 replaces this. Minimal placeholder keeps build green:
	c.resetCount = rec.ResetCount
}

func (c *Coordinator) runFence(rec Record) {
	// Task 8 replaces this.
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}

func (c *Coordinator) writeChannelHealth(rec Record) {
	// Task 8 replaces this.
	c.eventsW.Write(ChannelEvent{Kind: "applied_delta", Record: rec}, rec.ChannelID, "", 0, 0)
}
