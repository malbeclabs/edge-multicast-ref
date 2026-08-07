package main

import (
	"encoding/json"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
	// Kind is one of the constants below.
	//
	// ONLY KindAppliedDelta and KindAppliedSnapshot assert that book state
	// changed. Every other kind reports a record that was seen and deliberately
	// not applied to the book, so a consumer must not persist it as a mutation.
	// noteConsistencyPoint depends on exactly this distinction to decide when to
	// evaluate crossed-book.
	//
	// Channel resets are deliberately absent: they are handled by draining every
	// shard through msgReset, which produces no per-instrument event, and are
	// observable as channel_resets_total.
	Kind         string
	InstrumentID uint32
	Symbol       string
	Record       Record
}

const (
	// Book state changed.
	KindAppliedDelta    = "applied_delta"
	KindAppliedSnapshot = "applied_snapshot"

	// Seen but not applied to the book.
	KindInstrumentReset      = "instrument_reset"
	KindInstrumentDefinition = "instrument_definition"
	KindBatchBoundary        = "batch_boundary"
	KindTrade                = "trade"
	KindPerInstrumentGap     = "per_instrument_gap"
	KindMalformedDelta       = "malformed_delta"
)

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

	eventsW *EventsWriter
	sw      *SnapshotWriter

	// symbols gates PERSISTENCE and read-out only. Nil means no filter. The book
	// engine always processes every instrument: sequencing, gap detection and the
	// delta buffer are only correct if every record is applied.
	symbols map[string]struct{}

	// Per-shard children of the shard-labelled gauges, resolved once. bufferDelta
	// is a hot path, so a WithLabelValues map lookup per append is worth avoiding.
	// Both are nil when metrics is nil, which tests rely on.
	crossedGauge  prometheus.Gauge
	bufferedGauge prometheus.Gauge
}

// publishBufferedGauge republishes this shard's buffered-record count.
func (s *Shard) publishBufferedGauge() {
	if s.bufferedGauge != nil {
		s.bufferedGauge.Set(float64(s.bufferedN))
	}
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
	msgClearShadows
)

func NewShard(idx, n int, eventsW *EventsWriter, metrics *Metrics) *Shard {
	s := &Shard{
		idx: idx, n: n,
		instruments: map[instKey]*Instrument{},
		refdata:     map[instKey]InstrumentDef{},
		deltaBuf:    map[instKey][]BufferedDelta{},
		maxBuffered: maxBufferedDeltasPerShard,
		touched:     map[instKey]struct{}{},
		crossed:     map[instKey]struct{}{},
		inbox:       make(chan shardMsg, 4096),
		metrics:     metrics,
		eventsW:     eventsW,
	}
	if metrics != nil {
		lbl := strconv.Itoa(idx)
		s.crossedGauge = metrics.CrossedInstruments.WithLabelValues(lbl)
		s.bufferedGauge = metrics.DeltaBufferedRecords.WithLabelValues(lbl)
	}
	return s
}

// parseSymbolFilter turns a comma-separated list into a lookup set. An empty
// string means no filter, represented as a nil map.
func parseSymbolFilter(csv string) map[string]struct{} {
	if strings.TrimSpace(csv) == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, s := range strings.Split(csv, ",") {
		if s = strings.TrimSpace(s); s != "" {
			out[s] = struct{}{}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// persists reports whether rows for this symbol should be written.
//
// With a filter active it fails CLOSED: an empty symbol is NOT persisted. In the
// shard path an empty symbol means the instrument's definition has not arrived
// yet, which is routine at cold start because the refdata cycle lags mktdata.
// Three paths reach here in that state — an instrument_reset (whose own comment
// notes it routinely precedes the definition), a snapshot_level captured for
// wire_levels, and the snapshot writer's read-out — and failing open leaked all
// three into ClickHouse under an empty symbol for instruments the operator explicitly
// filtered out.
//
// Channel-scoped records never reach this path. Coordinator.Dispatch writes
// heartbeat, manifest_summary, end_of_session and batch_boundary itself, so the
// "an empty symbol belongs to a channel-scoped record" justification the
// fail-open was built on no longer applies anywhere in the Shard path.
func (s *Shard) persists(symbol string) bool {
	if s.symbols == nil {
		return true
	}
	_, ok := s.symbols[symbol]
	return ok
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
		// Duplicate or late. Discarded without demoting: a duplicated frame during
		// bootstrap must not cost a re-bootstrap.
		//
		// Counted, though, because this path is also the only symptom of a wedged
		// instrument. A snapshot carrying a Last Instrument Seq far ahead of reality
		// commits and sets the tracker high; from then on every genuine delta lands
		// here while every later snapshot is declined as current, and the instrument
		// serves a frozen book indefinitely. Without this counter that state is
		// invisible — no log, no metric, and a book that still reads as ready.
		if s.metrics != nil {
			s.metrics.DeltasDiscardedTotal.WithLabelValues("stale_seq").Inc()
		}
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
		return []ChannelEvent{{Kind: KindPerInstrumentGap, InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
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
			return ChannelEvent{Kind: KindMalformedDelta, InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
		}
	}
	inst.LastAppliedMktdataSeq = rec.SequenceNumber
	inst.LastAppliedInstrumentSeq = toUint32(rec.Fields["per_instrument_seq"])
	// Recorded only past the malformed-BookClear early return above, so it tracks
	// records that genuinely changed the book — the same rule as the two sequence
	// trackers. level_snapshots.publisher_send_ts reads it.
	inst.LastAppliedSendTS = rec.sendTime()
	return ChannelEvent{Kind: KindAppliedDelta, InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}
}

// bufferDelta appends to the per-instrument buffer, keeping it ordered by
// mktdata seq, and enforces the shard budget.
//
// The buffer is kept sorted by insertion rather than by re-sorting on every
// append. Deltas arrive in mktdata-seq order, so the fast path is a bare append
// and the ordered insert is rare. Re-sorting the whole slice per append made
// this quadratic in the buffer's length — 2.0 us/record at 1k buffered but
// 36.7 us at 40k — and that cost lands in the shard goroutine, the only reader
// of its inbox. Once it exceeds the arrival rate it back-pressures through the
// inbox into Coordinator.send and then into the socket read loop, so the bot
// stops draining the parser exactly when one hot instrument is gapped waiting
// for a snapshot: the case the buffer exists to survive.
func (s *Shard) bufferDelta(k instKey, rec Record) {
	buf := s.deltaBuf[k]
	d := BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec}
	if n := len(buf); n > 0 && buf[n-1].MktdataSeq > d.MktdataSeq {
		// Out of order: splice it into place, preserving the sorted invariant.
		i := sort.Search(n, func(i int) bool { return buf[i].MktdataSeq > d.MktdataSeq })
		buf = append(buf, BufferedDelta{})
		copy(buf[i+1:], buf[i:])
		buf[i] = d
	} else {
		buf = append(buf, d)
	}
	s.deltaBuf[k] = buf
	s.bufferedN++
	if s.bufferedN > s.maxBuffered {
		s.evictLargestBuffer()
	}
	s.publishBufferedGauge()
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
	s.publishBufferedGauge()
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

// --- JSON coercion helpers ---
//
// The socket reader decodes with UseNumber, so numbers in Fields arrive as
// json.Number and are parsed from their literal text — exact for the full int64
// and uint64 ranges. The float64 cases remain because tests build Fields
// directly, and because a value written with a fraction or exponent still
// decodes through the float path.

// numInt64 parses a json.Number as an integer, falling back to its float value
// for anything carrying a fraction or exponent.
func numInt64(x json.Number) (int64, bool) {
	if n, err := strconv.ParseInt(x.String(), 10, 64); err == nil {
		return n, true
	}
	if f, err := x.Float64(); err == nil {
		return int64(f), true
	}
	return 0, false
}

func toUint8(v any) uint8 {
	switch x := v.(type) {
	case json.Number:
		if n, ok := numInt64(x); ok {
			return uint8(n)
		}
	case float64:
		return uint8(x)
	case uint8:
		return x
	}
	return 0
}

func toUint16(v any) uint16 {
	switch x := v.(type) {
	case json.Number:
		if n, ok := numInt64(x); ok {
			return uint16(n)
		}
	case float64:
		return uint16(x)
	case uint16:
		return x
	}
	return 0
}

func toUint32(v any) uint32 {
	switch x := v.(type) {
	case json.Number:
		if n, ok := numInt64(x); ok {
			return uint32(n)
		}
	case float64:
		return uint32(x)
	case uint32:
		return x
	}
	return 0
}

func toUint64(v any) uint64 {
	switch x := v.(type) {
	case json.Number:
		// ParseUint first: qty_raw is unsigned on the wire and its top half does
		// not survive a round trip through int64.
		if n, err := strconv.ParseUint(x.String(), 10, 64); err == nil {
			return n
		}
		if n, ok := numInt64(x); ok {
			return uint64(n)
		}
	case float64:
		return uint64(x)
	case uint64:
		return x
	}
	return 0
}

func toInt8(v any) int8 {
	switch x := v.(type) {
	case json.Number:
		if n, ok := numInt64(x); ok {
			return int8(n)
		}
	case float64:
		return int8(x)
	case int8:
		return x
	}
	return 0
}

func toInt64(v any) int64 {
	switch x := v.(type) {
	case json.Number:
		if n, ok := numInt64(x); ok {
			return n
		}
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
