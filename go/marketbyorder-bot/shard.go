package main

import (
	"context"
	"errors"
	"log"
	"sort"
	"sync"
	"time"
)

const maxBufferedDeltasPerInstrument = 10000
const reorderWindow = 16

type instKey struct {
	ch uint8
	id uint32
}

type BufferedDelta struct {
	MktdataSeq uint64
	Record     Record
}

type InstrumentDef struct {
	Symbol        string
	PriceExponent int8
	QtyExponent   int8
}

type ManifestState struct {
	Seq             uint16
	Valid           bool
	InstrumentCount uint32
}

// ChannelEvent is the small subset of book-builder-side state changes a shard reports
// outward (used by writers to enqueue persistence and by metrics to track resets).
type ChannelEvent struct {
	Kind         string // "applied_delta" | "applied_snapshot" | "instrument_reset" | "channel_reset" | "per_instrument_gap"
	InstrumentID uint32
	Symbol       string
	Record       Record
}

// openGroup identifies the currently-open snapshot group on one channel: the
// group this shard's last SnapshotBegin opened, whose SnapshotEnd has not yet
// arrived.
//
// SnapshotOrder records carry no instrument_id — the containing SnapshotBegin
// implies it — and Snapshot ID is monotonic per (channel_id, instrument_id)
// rather than per channel, so two instruments routinely sit at the same value
// within one cycle. The instrument therefore comes from the open group, and
// Snapshot ID only validates membership (spec 0x20: discard any SnapshotOrder
// whose Snapshot ID does not match the currently-open SnapshotBegin).
//
// The id is held here rather than read from the instrument's shadow because a
// ready instrument declines its snapshot and builds no shadow, and that group's
// orders still have to be recognized as its own.
//
// Publishers MUST NOT interleave snapshot groups, so one open group per channel
// is sufficient state.
type openGroup struct {
	inst   instKey
	snapID uint32
}

// Shard owns a disjoint subset of instruments (by instrument_id % N) and all
// their state. Its goroutine is the only writer of that state; mu guards book
// mutation only so the per-shard SnapshotWriter goroutine can read levels.
type Shard struct {
	idx int
	n   int

	mu          sync.Mutex
	instruments map[instKey]*Instrument
	refdata     map[instKey]InstrumentDef
	deltaBuf    map[instKey][]BufferedDelta // per instrument, ordered by MktdataSeq
	snapCtx     map[instKey]SnapshotContext // keyed by (channel, instrument)
	open        map[uint8]openGroup         // currently-open snapshot group, per channel

	inbox   chan shardMsg
	sw      *SnapshotWriter
	eventsW *EventsWriter
	metrics *Metrics
}

// NewShard builds shard idx of n. sw may be nil in unit tests that only call apply().
func NewShard(idx, n int, eventsW *EventsWriter, sw *SnapshotWriter, metrics *Metrics) *Shard {
	return &Shard{
		idx: idx, n: n,
		instruments: map[instKey]*Instrument{},
		refdata:     map[instKey]InstrumentDef{},
		deltaBuf:    map[instKey][]BufferedDelta{},
		snapCtx:     map[instKey]SnapshotContext{},
		open:        map[uint8]openGroup{},
		inbox:       make(chan shardMsg, 4096),
		sw:          sw,
		eventsW:     eventsW,
		metrics:     metrics,
	}
}

// resetChannel discards every instrument owned by one channel.
//
// Scoped to a channel, not the whole shard, because a group can carry two
// redundant publishers interleaved on the same ports under different
// channel_ids. Reset Count is per publisher, so a reset on one says nothing
// about the other, and wiping both would throw away books that never reset.
func (s *Shard) resetChannel(ch uint8) {
	for k := range s.instruments {
		if k.ch == ch {
			delete(s.instruments, k)
		}
	}
	for k := range s.refdata {
		if k.ch == ch {
			delete(s.refdata, k)
		}
	}
	for k := range s.deltaBuf {
		if k.ch == ch {
			delete(s.deltaBuf, k)
		}
	}
	for k := range s.snapCtx {
		if k.ch == ch {
			delete(s.snapCtx, k)
		}
	}
	delete(s.open, ch)
}

// clearShadows abandons every in-flight snapshot group after a socket drop: the
// open group on each channel, the half-built shadows those groups were filling,
// and the snapshot contexts stamping their wire_snapshots rows.
//
// The break spans a group, so what the open group names is no longer what the
// next snapshot_order belongs to: that record carries no instrument_id, and
// filing it by a pre-drop pointer puts one instrument's orders into another
// instrument's shadow. Every channel goes, not one: all of them arrive over the
// parser socket that dropped.
//
// Status and the live book are deliberately untouched. A shadow is never the
// live book, so abandoning a half-built one costs nothing a ready instrument is
// serving from, and the next snapshot cycle rebuilds it; demoting here would
// throw away books the deltas are keeping correct.
func (s *Shard) clearShadows() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.open = map[uint8]openGroup{}
	s.snapCtx = map[instKey]SnapshotContext{}
	for _, inst := range s.instruments {
		inst.OpenSnapshot = nil
	}
}

// apply mutates book state for one record and returns the resulting events.
// It holds s.mu so the SnapshotWriter's withInstrument callback is safe.
func (s *Shard) apply(rec Record) []ChannelEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := instKey{rec.ChannelID, rec.InstrumentID}

	switch rec.Type {
	case "instrument_definition":
		return s.applyInstrumentDefinition(k, rec)
	case "snapshot_begin":
		return s.applySnapshotBegin(k, rec)
	case "snapshot_order":
		return s.applySnapshotOrder(rec)
	case "snapshot_end":
		return s.applySnapshotEnd(k, rec)
	case "order_add", "order_cancel", "order_execute":
		return s.applyDelta(k, rec)
	case "instrument_reset":
		return s.applyInstrumentReset(k, rec)
	case "trade":
		// Behavior parity: the original channel.go set NO InstrumentID on the
		// trade event, so the dispatcher did not MarkDirty and resolved an
		// empty symbol. The events_writer "trade" row uses rec.InstrumentID
		// directly (not ev.InstrumentID), so the persisted instrument_id is
		// still correct. Do NOT set ev.InstrumentID here — it would change
		// MarkDirty / symbol-resolution behavior.
		return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
	}
	return nil
}

func (s *Shard) applyInstrumentDefinition(k instKey, rec Record) []ChannelEvent {
	symbol, _ := rec.Fields["symbol"].(string)
	priceExp := toInt8(rec.Fields["price_exponent"])
	qtyExp := toInt8(rec.Fields["qty_exponent"])
	s.refdata[k] = InstrumentDef{Symbol: symbol, PriceExponent: priceExp, QtyExponent: qtyExp}
	if inst, ok := s.instruments[k]; ok {
		inst.Symbol = symbol
		inst.PriceExponent = priceExp
		inst.QtyExponent = qtyExp
	} else {
		s.instruments[k] = NewInstrument(k.id, symbol, priceExp, qtyExp)
	}
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: k.id, Symbol: symbol, Record: rec}}
}

func (s *Shard) applySnapshotBegin(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		inst = NewInstrument(k.id, "", 0, 0)
		s.instruments[k] = inst
	}
	snapID := toUint32(rec.Fields["snapshot_id"])
	// Record the group identity before the accept/decline decision below.
	// Declining is the steady-state case and this group's orders still arrive:
	// they belong to this instrument and must not be offered to another.
	s.open[k.ch] = openGroup{inst: k, snapID: snapID}
	// A Ready instrument is maintained by contiguous deltas; snapshots are
	// gap-recovery only, so ignore them while Ready.
	if inst.Status == StatusReady {
		return nil
	}
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

// applySnapshotOrder files one order into the open group's shadow.
//
// The order goes to the instrument the open group names, never to whichever
// instrument happens to hold a shadow at a matching Snapshot ID. Ids are
// per-instrument, so a lost SnapshotEnd leaves a shadow lingering at the same id
// the next instrument's cycle uses, and choosing by id alone chooses arbitrarily
// under Go's randomized map order.
func (s *Shard) applySnapshotOrder(rec Record) []ChannelEvent {
	g, isOpen := s.open[rec.ChannelID]
	snapID := toUint32(rec.Fields["snapshot_id"])
	if !isOpen || g.snapID != snapID {
		// No group open on this channel — an order ahead of its own
		// SnapshotBegin or trailing its SnapshotEnd — or an id that disagrees
		// with the open group. Never guess an instrument.
		s.countSnapshotOrderDropped()
		return nil
	}
	inst, ok := s.instruments[g.inst]
	if !ok || inst.OpenSnapshot == nil {
		// The order belongs to this group and there is no shadow to build it
		// into: a Ready instrument declined the group at SnapshotBegin while the
		// publisher still sends every order in it, or an InstrumentReset has
		// since invalidated the shadow. The first is the steady state, so
		// counting these would swamp the drop signal with ordinary traffic.
		return nil
	}
	orderID := toUint64(rec.Fields["order_id"])
	side := sideFromString(toString(rec.Fields["side"]))
	flags := toUint8(rec.Fields["order_flags"])
	enter := toTime(rec.Fields["enter_ts"])
	price := toInt64(rec.Fields["price_raw"])
	qty := toUint64(rec.Fields["qty_raw"])
	if !inst.AddSnapshotOrder(snapID, orderID, side, flags, enter, price, qty) {
		// The shadow re-checks the id it was opened with. Reaching here means it
		// disagrees with the group the pointer names, so the order belongs to
		// neither.
		s.countSnapshotOrderDropped()
	}
	return nil
}

func (s *Shard) countSnapshotOrderDropped() {
	if s.metrics != nil {
		s.metrics.SnapshotOrderDroppedTotal.Inc()
	}
}

func (s *Shard) applySnapshotEnd(k instKey, rec Record) []ChannelEvent {
	snapID := toUint32(rec.Fields["snapshot_id"])
	// The end closes the group it names, whatever the outcome below and even for
	// an instrument this shard holds no state for. It has to name both the
	// instrument and the id: an end delayed behind the next SnapshotBegin — this
	// instrument's own next cycle, or another instrument's — would otherwise
	// close a group that is still live, and the rest of that group's orders
	// would be dropped as belonging to nothing.
	if g, isOpen := s.open[k.ch]; isOpen && g.inst == k && g.snapID == snapID {
		delete(s.open, k.ch)
	}
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	if inst.OpenSnapshot == nil {
		return nil // no shadow in progress; ignore (never demote)
	}
	if snapID < inst.OpenSnapshot.SnapshotID {
		// The delayed end, and the only direction this guard is for. The shadow
		// in progress is a later group's: this instrument's own next
		// SnapshotBegin replaced the one the end names. EndSnapshot discards the
		// shadow on an id it disagrees with, so offering this end to it would
		// throw away a group whose orders are still arriving and cost the
		// instrument the recovery cycle it is in the middle of. The end names a
		// group that is already finished, so there is nothing left to commit.
		//
		// Silent by design: an end reordered past the next begin is ordinary
		// datagram behavior, the shadow it protects goes on to commit or to be
		// counted on its own end, and a line per record would grow with the
		// reorder rate while naming nothing that is not already visible.
		return nil
	}
	// Ids are monotonic per (channel_id, instrument_id), so an end ahead of the
	// shadow is this instrument's own later group announcing itself: that
	// group's SnapshotBegin was lost, and the shadow belongs to a cycle that is
	// over and whose end will never arrive. It falls through to EndSnapshot,
	// which discards the dead shadow and counts
	// snapshot_discarded_total{reason="mismatch"}. Holding the shadow instead
	// would keep a snapshot no end can close and charge a log line to every
	// further end that names an id it disagrees with.
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		if s.metrics != nil {
			s.metrics.SnapshotDiscardedTotal.WithLabelValues(discardReason(err)).Inc()
		}
		log.Printf("shard %d instrument %d: snapshot discarded: %v", s.idx, k.id, err)
		return nil // discard shadow only; live book & status unchanged
	}
	s.replayBuffer(k, inst)
	return []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}

func (s *Shard) applyDelta(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		s.bufferDelta(k, rec)
		return nil
	}
	if inst.Status == StatusReady {
		return s.applyDeltaToReady(k, inst, rec)
	}
	s.bufferDelta(k, rec)
	return nil
}

func (s *Shard) applyOne(inst *Instrument, rec Record) ChannelEvent {
	switch rec.Type {
	case "order_add":
		inst.ApplyOrderAdd(toUint64(rec.Fields["order_id"]), sideFromString(toString(rec.Fields["side"])), toUint8(rec.Fields["order_flags"]), toTime(rec.Fields["enter_ts"]), toInt64(rec.Fields["price_raw"]), toUint64(rec.Fields["qty_raw"]))
	case "order_cancel":
		inst.ApplyOrderCancel(toUint64(rec.Fields["order_id"]))
	case "order_execute":
		inst.ApplyOrderExecute(toUint64(rec.Fields["order_id"]), toUint8(rec.Fields["exec_flags"]), toUint64(rec.Fields["exec_qty_raw"]))
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = toUint32(rec.Fields["per_instrument_seq"])
	return ChannelEvent{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
}

func (s *Shard) applyDeltaToReady(k instKey, inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1
	if piSeq < expected {
		return nil // old / duplicate
	}
	if piSeq > expected {
		if inst.Pending == nil {
			inst.Pending = map[uint32]Record{}
		}
		inst.Pending[piSeq] = rec
		if uint32(len(inst.Pending)) <= reorderWindow && piSeq-expected <= reorderWindow {
			return nil // within reorder window; wait for the hole to fill
		}
		// Window exceeded: genuine gap.
		log.Printf("shard %d instrument %d: per-instrument gap, expected %d got %d",
			s.idx, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		inst.Pending = nil
		s.bufferDelta(k, rec)
		if s.metrics != nil {
			s.metrics.BookDemotionsTotal.Inc()
		}
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}
	// piSeq == expected: apply, then drain contiguous reordered deltas.
	evs := []ChannelEvent{s.applyOne(inst, rec)}
	for inst.Pending != nil {
		next := inst.LastAppliedInstrumentSeq + 1
		pr, ok := inst.Pending[next]
		if !ok {
			break
		}
		delete(inst.Pending, next)
		evs = append(evs, s.applyOne(inst, pr))
		if len(inst.Pending) == 0 {
			inst.Pending = nil
		}
	}
	return evs
}

func (s *Shard) applyInstrumentReset(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	inst.Reset()
	newAnchor := toUint64(rec.Fields["new_anchor_seq"])
	s.deltaBuf[k] = filterBuffer(s.deltaBuf[k], func(b BufferedDelta) bool {
		return b.MktdataSeq > newAnchor
	})
	return []ChannelEvent{{Kind: "instrument_reset", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}

func (s *Shard) bufferDelta(k instKey, rec Record) {
	buf := s.deltaBuf[k]
	if len(buf) >= maxBufferedDeltasPerInstrument {
		buf = buf[1:]
	}
	buf = append(buf, BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec})
	sort.Slice(buf, func(i, j int) bool { return buf[i].MktdataSeq < buf[j].MktdataSeq })
	s.deltaBuf[k] = buf
}

func (s *Shard) replayBuffer(k instKey, inst *Instrument) {
	// Per-instrument buffer is fully consumed on snapshot end: every entry is
	// either covered by the snapshot anchor or re-applied. Drop the empty slot
	// so the map stays clean (it'll be recreated on demand by bufferDelta).
	for _, b := range s.deltaBuf[k] {
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue
		}
		s.applyDeltaToReady(k, inst, b.Record)
	}
	delete(s.deltaBuf, k)
}

func filterBuffer(buf []BufferedDelta, keep func(BufferedDelta) bool) []BufferedDelta {
	out := make([]BufferedDelta, 0, len(buf))
	for _, b := range buf {
		if keep(b) {
			out = append(out, b)
		}
	}
	return out
}

func discardReason(err error) string {
	switch {
	case errors.Is(err, errSnapshotShort):
		return "short"
	case errors.Is(err, errSnapshotMismatch):
		return "mismatch"
	default:
		// errNoOpenSnapshot cannot reach here: applySnapshotEnd's nil-guard returns before calling EndSnapshot.
		return "other"
	}
}

// --- type conversion helpers (JSON unmarshal yields float64 / string / bool by default) ---

func toUint8(v any) uint8 {
	switch x := v.(type) {
	case float64:
		return uint8(x)
	case uint8:
		return x
	}
	return 0
}

func toUint16(v any) uint16 {
	switch x := v.(type) {
	case float64:
		return uint16(x)
	case uint16:
		return x
	}
	return 0
}

func toUint32(v any) uint32 {
	switch x := v.(type) {
	case float64:
		return uint32(x)
	case uint32:
		return x
	}
	return 0
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case float64:
		return uint64(x)
	case uint64:
		return x
	}
	return 0
}

func toInt8(v any) int8 {
	switch x := v.(type) {
	case float64:
		return int8(x)
	case int8:
		return x
	}
	return 0
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case int64:
		return x
	}
	return 0
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toTime(v any) time.Time {
	if s, ok := v.(string); ok {
		t, _ := time.Parse(time.RFC3339Nano, s)
		return t
	}
	return time.Time{}
}

func sideFromString(s string) uint8 {
	if s == "ask" {
		return 1
	}
	return 0
}

// handle applies a record and performs persistence (events + snapshot dirty
// marking + metrics) for the shard's instruments. It is the shard goroutine's
// per-record entry point. Channel-scoped records never reach a shard.
func (s *Shard) handle(rec Record) {
	evs := s.apply(rec)

	k := instKey{rec.ChannelID, rec.InstrumentID}

	switch rec.Type {
	case "snapshot_begin":
		s.mu.Lock()
		def := s.refdata[k]
		s.snapCtx[k] = SnapshotContext{
			InstrumentID:      rec.InstrumentID,
			Symbol:            def.Symbol,
			SnapshotID:        getUint32(rec.Fields, "snapshot_id"),
			AnchorSeq:         getUint64(rec.Fields, "anchor_seq"),
			TotalOrders:       getUint32(rec.Fields, "total_orders"),
			LastInstrumentSeq: getUint32(rec.Fields, "last_instrument_seq"),
			PriceExponent:     def.PriceExponent,
			QtyExponent:       def.QtyExponent,
		}
		s.mu.Unlock()
	case "snapshot_order":
		// The same association as the book path, read from the same pointer, so
		// the wire_snapshots row and the shadow that received the order always
		// name one instrument. Keying the context by (channel, snapshot_id)
		// instead both grows the map once per lost SnapshotEnd — the id advances
		// every cycle, so the entry is never overwritten — and lets one
		// instrument's context answer for another's orders.
		//
		// Snapshot bookkeeping is read and written under s.mu throughout, as
		// resetChannel deletes from these same maps under it.
		s.mu.Lock()
		g, isOpen := s.open[rec.ChannelID]
		sctx, haveCtx := s.snapCtx[g.inst]
		s.mu.Unlock()
		if isOpen && haveCtx && g.snapID == getUint32(rec.Fields, "snapshot_id") {
			s.eventsW.WriteSnapshotOrder(rec, rec.ChannelID, sctx)
		}
	case "snapshot_end":
		// Only the named group's context goes, on the same grounds as
		// applySnapshotEnd: an end delayed behind this instrument's next
		// SnapshotBegin would otherwise take the live group's context with it,
		// and the rest of that group would persist no wire_snapshots rows.
		s.mu.Lock()
		if sctx, ok := s.snapCtx[k]; ok && sctx.SnapshotID == getUint32(rec.Fields, "snapshot_id") {
			delete(s.snapCtx, k)
		}
		s.mu.Unlock()
	}

	for _, ev := range evs {
		s.mu.Lock()
		def := s.refdata[instKey{rec.ChannelID, ev.InstrumentID}]
		s.mu.Unlock()
		s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)

		switch ev.Kind {
		case "applied_delta", "applied_snapshot":
			if ev.InstrumentID != 0 && s.sw != nil {
				s.sw.MarkDirty(ev.InstrumentID)
			}
		case "instrument_reset":
			if s.metrics != nil {
				s.metrics.InstrumentResetsTotal.WithLabelValues(getString(ev.Record.Fields, "reason")).Inc()
			}
			if s.sw != nil {
				s.sw.MarkDirty(ev.InstrumentID)
			}
		case "per_instrument_gap":
			if s.metrics != nil {
				s.metrics.PerInstrumentGapsTotal.Inc()
			}
		}
	}
}

// Run is the shard goroutine. It processes its FIFO inbox until ctx is done.
// Records mutate book state; a reset marker wipes state and quiesces the
// SnapshotWriter before acking; a clear-shadows marker abandons in-flight
// snapshot groups without acking, FIFO being enough to order it ahead of every
// record dispatched after a reconnect; a fence marker only acks (FIFO already
// guarantees preceding records' rows are enqueued).
func (s *Shard) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.inbox:
			switch msg.kind {
			case msgRecord:
				s.handle(*msg.rec)
			case msgClearShadows:
				s.clearShadows()
			case msgReset:
				s.mu.Lock()
				s.resetChannel(msg.ch)
				s.mu.Unlock()
				if s.sw != nil {
					s.sw.Reset(ctx) // ctx-aware: never wedges on shutdown
				}
				select {
				case msg.ack <- s.idx:
				case <-ctx.Done():
					return
				}
			case msgFence:
				select {
				case msg.ack <- s.idx:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// shardMsg is the inbox protocol. A record mutates book state; a reset wipes one
// channel's share of it and acks; a clear-shadows drops the snapshot groups a
// socket drop invalidated, leaving live books alone; a fence only acks, which is
// enough to order a channel-scoped write after every preceding instrument write
// because the inbox is FIFO.
type shardMsg struct {
	rec  *Record
	kind shardMsgKind
	ch   uint8 // channel to wipe, for msgReset
	ack  chan int
}

type shardMsgKind int

const (
	msgRecord shardMsgKind = iota
	msgReset
	msgFence
	msgClearShadows
)
