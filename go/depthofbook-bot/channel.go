package main

import (
	"log"
	"sort"
	"time"
)

const maxBufferedDeltas = 10000 // bound on cold-start / gap-recovery buffer per channel

// BufferedDelta is one mktdata-port delta held while an instrument awaits a snapshot.
type BufferedDelta struct {
	MktdataSeq uint64
	Record     Record
}

// ChannelState holds all state for one channel_id.
type ChannelState struct {
	ChannelID   uint8
	ResetCount  uint8
	SeqLast     map[string]uint64 // port → last seq seen
	Refdata     map[uint32]InstrumentDef
	Manifest    ManifestState
	Instruments map[uint32]*Instrument
	DeltaBuffer []BufferedDelta // ordered by MktdataSeq
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

// ChannelEvent is the small subset of bot-side state changes the channel reports
// outward (used by writers to enqueue persistence and by metrics to track resets).
type ChannelEvent struct {
	Kind         string // "applied_delta" | "applied_snapshot" | "instrument_reset" | "channel_reset" | "per_instrument_gap"
	InstrumentID uint32
	Symbol       string
	Record       Record
}

// NewChannelState returns an empty channel.
func NewChannelState(id uint8) *ChannelState {
	return &ChannelState{
		ChannelID:   id,
		SeqLast:     map[string]uint64{},
		Refdata:     map[uint32]InstrumentDef{},
		Instruments: map[uint32]*Instrument{},
	}
}

// Apply dispatches a Record. Returns the events caused by applying it.
// The bot's main loop iterates these and forwards them to the writers.
func (c *ChannelState) Apply(rec Record) []ChannelEvent {
	// Detect channel reset on any port.
	if c.ResetCount != 0 && rec.ResetCount != c.ResetCount {
		log.Printf("channel %d: reset_count changed %d -> %d, discarding state", c.ChannelID, c.ResetCount, rec.ResetCount)
		evs := []ChannelEvent{{Kind: "channel_reset", Record: rec}}
		c.reset(rec.ResetCount)
		// Continue to apply this record below — it's the first frame of the new era.
		evs = append(evs, c.applyInner(rec)...)
		return evs
	}
	if c.ResetCount == 0 {
		c.ResetCount = rec.ResetCount
	}
	return c.applyInner(rec)
}

func (c *ChannelState) reset(newResetCount uint8) {
	c.ResetCount = newResetCount
	c.SeqLast = map[string]uint64{}
	c.Refdata = map[uint32]InstrumentDef{}
	c.Manifest = ManifestState{}
	c.Instruments = map[uint32]*Instrument{}
	c.DeltaBuffer = nil
}

func (c *ChannelState) applyInner(rec Record) []ChannelEvent {
	c.SeqLast[rec.Port] = rec.SequenceNumber

	switch rec.Type {
	case "instrument_definition":
		return c.applyInstrumentDefinition(rec)
	case "manifest_summary":
		return c.applyManifestSummary(rec)
	case "snapshot_begin":
		return c.applySnapshotBegin(rec)
	case "snapshot_order":
		return c.applySnapshotOrder(rec)
	case "snapshot_end":
		return c.applySnapshotEnd(rec)
	case "order_add", "order_cancel", "order_execute":
		return c.applyDelta(rec)
	case "instrument_reset":
		return c.applyInstrumentReset(rec)
	case "trade", "batch_boundary":
		// No book-state effect; bot writers will still persist these.
		return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
	case "heartbeat", "end_of_session":
		return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
	}
	return nil
}

func (c *ChannelState) applyInstrumentDefinition(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	symbol, _ := rec.Fields["symbol"].(string)
	priceExp := toInt8(rec.Fields["price_exponent"])
	qtyExp := toInt8(rec.Fields["qty_exponent"])
	c.Refdata[id] = InstrumentDef{Symbol: symbol, PriceExponent: priceExp, QtyExponent: qtyExp}
	if inst, ok := c.Instruments[id]; ok {
		inst.Symbol = symbol
		inst.PriceExponent = priceExp
		inst.QtyExponent = qtyExp
	} else {
		c.Instruments[id] = NewInstrument(id, symbol, priceExp, qtyExp)
	}
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: id, Symbol: symbol, Record: rec}}
}

func (c *ChannelState) applyManifestSummary(rec Record) []ChannelEvent {
	seq := toUint16(rec.Fields["manifest_seq"])
	valid := toUint8(rec.Fields["valid"]) != 0
	count := toUint32(rec.Fields["instrument_count"])
	c.Manifest = ManifestState{Seq: seq, Valid: valid, InstrumentCount: count}
	return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
}

func (c *ChannelState) applySnapshotBegin(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		// Snapshot for an instrument we have no refdata for; create a placeholder.
		inst = NewInstrument(id, "", 0, 0)
		c.Instruments[id] = inst
	}
	anchor := toUint64(rec.Fields["anchor_seq"])
	total := toUint32(rec.Fields["total_orders"])
	snapID := toUint32(rec.Fields["snapshot_id"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])

	// "Snapshot while ready" — re-bootstrap if anchor > last_applied.
	if inst.Status == StatusReady && anchor <= inst.LastAppliedMktdataSeq {
		// Snapshot is stale; ignore.
		return nil
	}
	inst.BeginSnapshot(snapID, anchor, total, lastInstr)
	return nil
}

func (c *ChannelState) applySnapshotOrder(rec Record) []ChannelEvent {
	// SnapshotOrder doesn't carry instrument_id (it's implied by the SnapshotBegin
	// that opened the group). The publisher MUST NOT interleave snapshot groups, so
	// we find the one currently in StatusBuildingSnapshot.
	snapID := toUint32(rec.Fields["snapshot_id"])
	for _, inst := range c.Instruments {
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

func (c *ChannelState) applySnapshotEnd(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		return nil
	}
	snapID := toUint32(rec.Fields["snapshot_id"])
	anchor := toUint64(rec.Fields["anchor_seq"])
	if _, _, err := inst.EndSnapshot(snapID, anchor); err != nil {
		log.Printf("channel %d instrument %d: snapshot end failed: %v", c.ChannelID, id, err)
		return nil
	}
	// Replay buffered deltas with mktdata_seq > anchor.
	c.replayBuffer(inst)
	return []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: id, Symbol: inst.Symbol, Record: rec}}
}

func (c *ChannelState) applyDelta(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		// Buffer until we know about the instrument.
		c.bufferDelta(rec)
		return nil
	}

	switch inst.Status {
	case StatusReady:
		return c.applyDeltaToReady(inst, rec)
	default:
		c.bufferDelta(rec)
		return nil
	}
}

func (c *ChannelState) applyDeltaToReady(inst *Instrument, rec Record) []ChannelEvent {
	piSeq := toUint32(rec.Fields["per_instrument_seq"])
	expected := inst.LastAppliedInstrumentSeq + 1

	if piSeq < expected {
		// Duplicate or late.
		return nil
	}
	if piSeq > expected {
		log.Printf("channel %d instrument %d: per-instrument gap, expected %d got %d",
			c.ChannelID, inst.ID, expected, piSeq)
		inst.Status = StatusGap
		c.bufferDelta(rec)
		return []ChannelEvent{{Kind: "per_instrument_gap", InstrumentID: inst.ID, Symbol: inst.Symbol, Record: rec}}
	}

	// Apply.
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

func (c *ChannelState) applyInstrumentReset(rec Record) []ChannelEvent {
	id := rec.InstrumentID
	inst, ok := c.Instruments[id]
	if !ok {
		return nil
	}
	inst.Reset()
	// Discard buffered deltas for this instrument with mktdata_seq <= new_anchor_seq.
	newAnchor := toUint64(rec.Fields["new_anchor_seq"])
	c.DeltaBuffer = filterBuffer(c.DeltaBuffer, func(b BufferedDelta) bool {
		if b.Record.InstrumentID != id {
			return true
		}
		return b.MktdataSeq > newAnchor
	})
	return []ChannelEvent{{Kind: "instrument_reset", InstrumentID: id, Symbol: inst.Symbol, Record: rec}}
}

func (c *ChannelState) bufferDelta(rec Record) {
	if len(c.DeltaBuffer) >= maxBufferedDeltas {
		// Drop oldest.
		c.DeltaBuffer = c.DeltaBuffer[1:]
	}
	c.DeltaBuffer = append(c.DeltaBuffer, BufferedDelta{MktdataSeq: rec.SequenceNumber, Record: rec})
	sort.Slice(c.DeltaBuffer, func(i, j int) bool {
		return c.DeltaBuffer[i].MktdataSeq < c.DeltaBuffer[j].MktdataSeq
	})
}

func (c *ChannelState) replayBuffer(inst *Instrument) {
	remaining := c.DeltaBuffer[:0]
	for _, b := range c.DeltaBuffer {
		if b.Record.InstrumentID != inst.ID {
			remaining = append(remaining, b)
			continue
		}
		if b.MktdataSeq <= inst.LastAppliedMktdataSeq {
			continue // already covered by snapshot
		}
		c.applyDeltaToReady(inst, b.Record)
	}
	c.DeltaBuffer = remaining
}

func filterBuffer(buf []BufferedDelta, keep func(BufferedDelta) bool) []BufferedDelta {
	out := buf[:0]
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
