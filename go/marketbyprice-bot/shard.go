package main

import (
	"log"
	"sort"
	"sync"
	"time"
)

// maxBufferedDeltasPerShard bounds the delta buffer by record count across every
// instrument the shard owns. The spec requires a bounded buffer and a declared
// overflow policy, and sizes the cold-start worst case at ~1.4 GB for a 60 s
// snapshot cycle — the cycle-period knob and the subscriber-memory knob are the
// same knob.
const maxBufferedDeltasPerShard = 200000

// reorderWindow is how far ahead of last_applied a delta may arrive and still be
// treated as reordering rather than a gap. Carried over from the sibling bot,
// where the live path was observed reordering.
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
	ManifestSeq   uint16
}

// ChannelEvent is the subset of state changes a shard reports outward. The
// persistence layer (a follow-on plan) consumes these.
type ChannelEvent struct {
	// Kind is one of "applied_delta", "applied_snapshot", "instrument_reset",
	// "channel_reset", "per_instrument_gap", or "malformed_delta".
	//
	// Only "applied_delta" and "applied_snapshot" assert that book state changed.
	// "malformed_delta" reports a delta that arrived and was deliberately not
	// applied, so a consumer must not persist it as a mutation.
	Kind         string
	InstrumentID uint32
	Symbol       string
	Record       Record
}

// Shard owns a disjoint subset of instruments (by instrument_id % n) and all
// their state. Its goroutine is the only writer; mu guards book mutation so a
// future reader goroutine can read levels safely.
type Shard struct {
	idx int
	n   int

	mu          sync.Mutex
	instruments map[instKey]*Instrument
	refdata     map[instKey]InstrumentDef
	deltaBuf    map[instKey][]BufferedDelta
	bufferedN   int // running total across deltaBuf, so overflow is O(1) to detect

	// maxBuffered is the shard's record budget. A field rather than the bare
	// constant so tests can drive the overflow path without allocating 200k
	// records.
	maxBuffered int

	// Crossed-book monitoring state. sawBatchBoundary switches evaluation from
	// per-delta to per-boundary; touched is the set of instruments changed since
	// the previous boundary; crossed is the currently-crossed set behind the gauge.
	sawBatchBoundary bool
	touched          map[instKey]struct{}
	crossed          map[instKey]struct{}

	inbox   chan shardMsg
	metrics *Metrics
}

// shardMsg is the inbox protocol. A record mutates book state; a reset wipes it
// and acks; a fence only acks, which is enough to order a channel-scoped write
// after every preceding instrument write because the inbox is FIFO.
type shardMsg struct {
	rec  *Record
	kind shardMsgKind
	seq  uint16 // manifest seq, for msgManifestPrune
	ack  chan int
}

type shardMsgKind int

const (
	msgRecord shardMsgKind = iota
	msgReset
	msgFence
	msgManifestPrune
)

func NewShard(idx, n int, metrics *Metrics) *Shard {
	return &Shard{
		idx: idx, n: n,
		instruments: map[instKey]*Instrument{},
		refdata:     map[instKey]InstrumentDef{},
		deltaBuf:    map[instKey][]BufferedDelta{},
		maxBuffered: maxBufferedDeltasPerShard,
		touched:     map[instKey]struct{}{},
		crossed:     map[instKey]struct{}{},
		inbox:       make(chan shardMsg, 4096),
		metrics:     metrics,
	}
}

// applyDelta classifies one mktdata delta against the instrument's
// per-instrument sequence and applies, holds, discards, or buffers it.
func (s *Shard) applyDelta(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		// Unknown instrument: awaiting-refdata. Buffer until its definition lands.
		s.bufferDelta(k, rec)
		return nil
	}
	if inst.Status != StatusReady {
		s.bufferDelta(k, rec)
		return nil
	}
	return s.applyDeltaToReady(k, inst, rec)
}

func (s *Shard) applyDeltaToReady(k instKey, inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1

	if piSeq < expected {
		// Duplicate or late. Discard silently: a duplicated frame during
		// bootstrap must not cost a re-bootstrap.
		return nil
	}
	if piSeq > expected {
		if inst.Pending == nil {
			inst.Pending = map[uint32]Record{}
		}
		inst.Pending[piSeq] = rec
		if uint32(len(inst.Pending)) <= reorderWindow && piSeq-expected <= reorderWindow {
			return nil // within the reorder window; wait for the hole to fill
		}
		// Window exceeded: a genuine per-instrument gap.
		log.Printf("shard %d instrument %d: per-instrument gap, expected %d got %d",
			s.idx, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		inst.Pending = nil
		s.bufferDelta(k, rec)
		if s.metrics != nil {
			s.metrics.PerInstrumentGapsTotal.Inc()
		}
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}

	// Contiguous: apply, then drain any contiguous run held in Pending.
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

// applyOne mutates the book for one already-sequenced record.
func (s *Shard) applyOne(inst *Instrument, rec Record) ChannelEvent {
	switch rec.Type {
	case "level_update":
		div := inst.ApplyLevelUpdate(
			sideFromString(toString(rec.Fields["side"])),
			toInt64(rec.Fields["price_raw"]),
			toUint64(rec.Fields["qty_raw"]),
			orderCountFrom(rec.Fields),
			toUint8(rec.Fields["level_flags"]),
			actionFromString(toString(rec.Fields["action"])),
		)
		if s.metrics != nil {
			for _, d := range div {
				s.metrics.BookDivergenceTotal.WithLabelValues(string(d)).Inc()
			}
		}
	case "book_clear":
		err := inst.ApplyBookClear(
			clearSideFromString(toString(rec.Fields["clear_side"])),
			scopeFromString(toString(rec.Fields["scope"])),
			toInt64(rec.Fields["from_price_raw"]),
		)
		if err != nil {
			// Malformed: discard without advancing the trackers, because nothing
			// was applied. Returning early leaves last_applied where it was, so
			// the next delta is classified against the correct expected seq.
			//
			// The event Kind must NOT be "applied_delta" — no book change happened,
			// and a consumer that persists this as applied records a mutation the
			// book never saw.
			//
			// Defense in depth: unreachable from live traffic, because the parser
			// already rejects Scope=1 with ClearSide=2 at decode and never emits
			// such a record. This guards a hand-fed socket or a future parser that
			// relaxes that check.
			log.Printf("shard %d instrument %d: %v", s.idx, inst.ID, err)
			return ChannelEvent{Kind: "malformed_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
		}
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = toUint32(rec.Fields["per_instrument_seq"])
	return ChannelEvent{Kind: "applied_delta", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
}

// bufferDelta appends to the per-instrument buffer, keeping it ordered by
// mktdata seq, and enforces the shard budget.
func (s *Shard) bufferDelta(k instKey, rec Record) {
	buf := append(s.deltaBuf[k], BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec})
	sort.Slice(buf, func(i, j int) bool { return buf[i].MktdataSeq < buf[j].MktdataSeq })
	s.deltaBuf[k] = buf
	s.bufferedN++
	if s.bufferedN > s.maxBuffered {
		s.evictLargestBuffer()
	}
	if s.metrics != nil {
		s.metrics.DeltaBufferedRecords.Set(float64(s.bufferedN))
	}
}

// evictLargestBuffer implements the spec's recommended overflow policy: drop the
// buffered deltas for the instrument holding the most buffered data, mark that
// instrument gap, and continue. It recovers on its next snapshot exactly as any
// other gap instrument does. Sustained overflow means the snapshot cycle period
// is too long for the deployment's memory budget — a tuning signal an operator
// needs, which is why it is counted rather than silently absorbed.
func (s *Shard) evictLargestBuffer() {
	var victim instKey
	best := -1
	for k, buf := range s.deltaBuf {
		if len(buf) > best {
			victim, best = k, len(buf)
		}
	}
	if best <= 0 {
		return
	}
	s.bufferedN -= best
	delete(s.deltaBuf, victim)
	if inst, ok := s.instruments[victim]; ok {
		inst.Status = StatusGap
		inst.Pending = nil
	}
	if s.metrics != nil {
		s.metrics.DeltaBufferOverflowTotal.Inc()
	}
	log.Printf("shard %d: delta buffer overflow, evicted instrument %d (%d records)",
		s.idx, victim.id, best)
}

// replayBuffer drops buffered deltas covered by the snapshot anchor and replays
// the rest through the same classification as steady state.
func (s *Shard) replayBuffer(k instKey, inst *Instrument) {
	buf := s.deltaBuf[k]
	s.bufferedN -= len(buf)
	delete(s.deltaBuf, k)
	for _, b := range buf {
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue
		}
		// Re-check status every iteration, mirroring the guard in applyDelta. A
		// hole discovered mid-replay flips the instrument to gap, and without
		// this check every remaining entry would re-enter applyDeltaToReady and
		// declare the same gap again — inflating PerInstrumentGapsTotal by the
		// size of the trailing backlog and logging once per record.
		if inst.Status != StatusReady {
			s.bufferDelta(k, b.Record)
			continue
		}
		s.applyDeltaToReady(k, inst, b.Record)
	}
	if s.metrics != nil {
		s.metrics.DeltaBufferedRecords.Set(float64(s.bufferedN))
	}
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

// --- JSON coercion helpers: encoding/json yields float64 for every number ---

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

// orderCountFrom reads the optional order_count field. The parser OMITS the key
// when the wire carried the 0xFFFF sentinel, so an absent key means "not
// provided" and must map back to the sentinel — not to 0, which is a real count.
func orderCountFrom(fields map[string]any) uint16 {
	v, present := fields["order_count"]
	if !present {
		return u16Unavailable
	}
	return toUint16(v)
}

func sideFromString(s string) uint8 {
	if s == "ask" {
		return 1
	}
	return 0
}

func clearSideFromString(s string) uint8 {
	switch s {
	case "ask":
		return 1
	case "both":
		return 2
	default:
		return 0
	}
}

func scopeFromString(s string) uint8 {
	if s == "from_price" {
		return 1
	}
	return 0
}

func actionFromString(s string) uint8 {
	switch s {
	case "new":
		return 1
	case "change":
		return 2
	case "delete":
		return 3
	default:
		return 0
	}
}
