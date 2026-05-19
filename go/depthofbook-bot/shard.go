package main

import (
	"log"
	"sort"
	"sync"
	"time"
)

const maxBufferedDeltasPerInstrument = 10000

type instKey struct {
	ch uint8
	id uint32
}

// snapKey composite-keys snapshot context / routing by (channel, snapshot_id);
// snapshot IDs are only unique within a channel. Defined here (shard.go is the
// first new file); coordinator.go (Task 6) reuses this same type — do NOT
// redeclare it there.
type snapKey struct {
	ch   uint8
	snap uint32
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

// ChannelEvent is the small subset of bot-side state changes a shard reports
// outward (used by writers to enqueue persistence and by metrics to track resets).
type ChannelEvent struct {
	Kind         string // "applied_delta" | "applied_snapshot" | "instrument_reset" | "channel_reset" | "per_instrument_gap"
	InstrumentID uint32
	Symbol       string
	Record       Record
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
	snapCtx     map[snapKey]SnapshotContext // keyed by (channel, snapshot_id)

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
		snapCtx:     map[snapKey]SnapshotContext{},
		inbox:       make(chan shardMsg, 4096),
		sw:          sw,
		eventsW:     eventsW,
		metrics:     metrics,
	}
}

func (s *Shard) reset() {
	s.instruments = map[instKey]*Instrument{}
	s.refdata = map[instKey]InstrumentDef{}
	s.deltaBuf = map[instKey][]BufferedDelta{}
	s.snapCtx = map[snapKey]SnapshotContext{}
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
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	snapID := toUint32(rec.Fields["snapshot_id"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])
	if inst.Status == StatusReady && anchor <= inst.LastAppliedMktdataSeq {
		return nil
	}
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

func (s *Shard) applySnapshotOrder(rec Record) []ChannelEvent {
	snapID := toUint32(rec.Fields["snapshot_id"])
	for _, inst := range s.instruments {
		if inst.Status != StatusBuildingSnapshot || inst.OpenSnapshot == nil {
			continue
		}
		if inst.OpenSnapshot.SnapshotID != snapID {
			continue
		}
		orderID := toUint64(rec.Fields["order_id"])
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.AddSnapshotOrder(snapID, orderID, side, flags, enter, price, qty)
		return nil
	}
	return nil
}

func (s *Shard) applySnapshotEnd(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	snapID := toUint32(rec.Fields["snapshot_id"])
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		log.Printf("shard %d instrument %d: snapshot end failed: %v", s.idx, k.id, err)
		return nil
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

func (s *Shard) applyDeltaToReady(k instKey, inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1
	if piSeq < expected {
		return nil
	}
	if piSeq > expected {
		log.Printf("shard %d instrument %d: per-instrument gap, expected %d got %d",
			s.idx, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		s.bufferDelta(k, rec)
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}
	switch rec.Type {
	case "order_add":
		side := sideFromString(toString(rec.Fields["side"]))
		flags := toUint8(rec.Fields["order_flags"])
		orderID := toUint64(rec.Fields["order_id"])
		enter := toTime(rec.Fields["enter_ts"])
		price := toInt64(rec.Fields["price_raw"])
		qty := toUint64(rec.Fields["qty_raw"])
		inst.ApplyOrderAdd(orderID, side, flags, enter, price, qty)
	case "order_cancel":
		inst.ApplyOrderCancel(toUint64(rec.Fields["order_id"]))
	case "order_execute":
		inst.ApplyOrderExecute(toUint64(rec.Fields["order_id"]), toUint8(rec.Fields["exec_flags"]), toUint64(rec.Fields["exec_qty_raw"]))
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = piSeq
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
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
	buf := s.deltaBuf[k]
	remaining := make([]BufferedDelta, 0, len(buf))
	for _, b := range buf {
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue
		}
		s.applyDeltaToReady(k, inst, b.Record)
	}
	s.deltaBuf[k] = remaining
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

	sk := snapKey{rec.ChannelID, getUint32(rec.Fields, "snapshot_id")}
	switch rec.Type {
	case "snapshot_begin":
		s.mu.Lock()
		def := s.refdata[k]
		s.mu.Unlock()
		s.snapCtx[sk] = SnapshotContext{
			InstrumentID:      rec.InstrumentID,
			Symbol:            def.Symbol,
			SnapshotID:        getUint32(rec.Fields, "snapshot_id"),
			AnchorSeq:         getUint64(rec.Fields, "anchor_seq"),
			TotalOrders:       getUint32(rec.Fields, "total_orders"),
			LastInstrumentSeq: getUint32(rec.Fields, "last_instrument_seq"),
			PriceExponent:     def.PriceExponent,
			QtyExponent:       def.QtyExponent,
		}
	case "snapshot_order":
		if sctx, ok := s.snapCtx[sk]; ok {
			s.eventsW.WriteSnapshotOrder(rec, rec.ChannelID, sctx)
		}
	case "snapshot_end":
		delete(s.snapCtx, sk)
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

// shardMsg is the inbox protocol; populated in Task 5.
type shardMsg struct {
	rec  *Record
	kind shardMsgKind
	ack  chan int
}

type shardMsgKind int

const (
	msgRecord shardMsgKind = iota
	msgReset
	msgFence
)
