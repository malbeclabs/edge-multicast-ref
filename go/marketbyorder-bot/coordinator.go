package main

import "context"

// Coordinator is the single-goroutine Dispatcher. It owns channel-scoped state
// and routes each record to exactly one shard (by instrument_id % N), or to a
// direct-write / barrier / fence path. Shards own all instrument-scoped state.
//
// Dispatch is NOT safe for concurrent callers: it mutates resetCount/open/
// seqLast/manifest without locks, on the assumption that the only caller is the
// synchronous book-builder read loop.
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
	resetCount map[uint8]uint8 // per channel_id
	manifest   ManifestState   // parity bookkeeping; not read for logic
	seqLast    map[string]uint64
	open       map[uint8]openRoute // currently-open snapshot group, per channel_id
}

// openRoute is where one channel's currently-open snapshot group sends its
// orders: the group this channel's last SnapshotBegin opened, whose SnapshotEnd
// has not yet arrived, and the shard owning the instrument that opened it.
//
// Keyed by channel, never by (channel, snapshot_id). Snapshot ID is monotonic
// per (channel_id, instrument_id) rather than per channel, so a map keyed by it
// gains an entry per SnapshotBegin and gives one back only to a matching end:
// every lost SnapshotEnd leaves its key behind for good, since the next cycle's
// id is a different key and nothing overwrites the dead one. One route per
// channel is bounded by the channel count instead, which is the model
// marketbyprice-bot's coordinator uses.
//
// The route only has to survive from a SnapshotBegin to its own SnapshotEnd:
// publishers MUST NOT interleave snapshot groups, so the group that opened last
// is the group whose orders follow. Snapshot ID is not an instrument identity —
// the shard resolves that from the open group its SnapshotBegin established —
// and it is carried here to validate membership, so an order left over from a
// group that has since been replaced is dropped rather than handed to the
// instrument now holding the channel.
//
// The instrument is carried so that only that group's own end releases the
// route: ids are per-instrument, so the next group to open belongs to another
// instrument, and an end delayed behind that begin names an instrument the route
// no longer carries. Releasing on such an end would take the route away from a
// group whose orders are still arriving, and every one of them would be dropped
// for having none.
type openRoute struct {
	inst   uint32
	snapID uint32
	shard  int
}

// NewCoordinator builds a Coordinator. ctx is used solely to break barrier and
// fence ack-waits on shutdown so the coordinator cannot wedge when shards or
// SnapshotWriters have exited.
func NewCoordinator(ctx context.Context, shards []*Shard, eventsW *EventsWriter, metrics *Metrics) *Coordinator {
	return &Coordinator{
		ctx:     ctx,
		shards:  shards,
		n:       len(shards),
		eventsW: eventsW,
		metrics: metrics,
		seqLast: map[string]uint64{},
		open:    map[uint8]openRoute{},

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

// Dispatch implements Dispatcher. Called synchronously from the book-builder read loop.
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
		c.open[rec.ChannelID] = openRoute{
			inst:   rec.InstrumentID,
			snapID: getUint32(rec.Fields, "snapshot_id"),
			shard:  idx,
		}
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_order":
		// No open group on this channel, or an id that disagrees with the one
		// that is open: the order belongs to a group this coordinator can no
		// longer name. Drop and count — never guess an instrument.
		g, ok := c.open[rec.ChannelID]
		if !ok || g.snapID != getUint32(rec.Fields, "snapshot_id") {
			if c.metrics != nil {
				c.metrics.SnapshotOrderDroppedTotal.Inc()
			}
			return
		}
		c.shards[g.shard].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}

	case "snapshot_end":
		idx := int(rec.InstrumentID) % c.n
		c.shards[idx].inbox <- shardMsg{kind: msgRecord, rec: recPtr(rec)}
		// Release the route only while it is still this group's. SnapshotEnd
		// names its instrument, so an end delayed behind the next group's begin
		// is recognisable here and leaves that live route alone.
		if g, ok := c.open[rec.ChannelID]; ok && g.inst == rec.InstrumentID &&
			g.snapID == getUint32(rec.Fields, "snapshot_id") {
			delete(c.open, rec.ChannelID)
		}

	case "heartbeat", "manifest_summary":
		c.writeChannelHealth(rec) // implemented in Task 8

	case "end_of_session", "batch_boundary":
		c.runFence(rec) // implemented in Task 8
	}
}

// OnDisconnect drops the snapshot state that a parser socket drop invalidates:
// the open group on every channel here, and every shard's own open group,
// in-flight shadow and snapshot context.
//
// A reconnect resumes dispatching with no other signal. Without this, c.open
// still names the group that was in flight when the socket died, so the first
// snapshot_order after the reconnect — carrying no instrument_id, its own
// snapshot_begin lost in the outage — is routed to that stale group's shard and
// filed into that instrument's shadow. It counts toward a total_orders it has
// nothing to do with, and the shadow commits a book of another instrument's
// orders with nothing dropped and nothing discarded. A Reset Count change would
// clear it through the reset barrier, but a socket-only drop leaves Reset Count
// untouched, so nothing else covers this.
//
// No ack barrier is needed: each shard's inbox is FIFO, so the clear is ordered
// ahead of every record dispatched after the reconnect.
func (c *Coordinator) OnDisconnect() {
	c.open = map[uint8]openRoute{}
	for i := range c.shards {
		select {
		case c.shards[i].inbox <- shardMsg{kind: msgClearShadows}:
		case <-c.ctx.Done():
			return
		}
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
// held triggering record as the first new-era datagram.
//
// Barrier sends and ack-waits are ctx-aware: if ctx is cancelled mid-barrier
// (the book-builder is shutting down), we abandon the barrier and return without
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
	delete(c.open, ch)
	c.seqLast = map[string]uint64{}
	c.manifest = ManifestState{}
	c.resetCount[ch] = held.ResetCount

	// Route the held record as the first new-era datagram, via the full classifier.
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
