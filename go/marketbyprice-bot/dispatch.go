package main

import (
	"context"
	"errors"
	"log"
)

// apply mutates book state for one record and returns the resulting events.
func (s *Shard) apply(rec Record) []ChannelEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := instKey{rec.ChannelID, rec.InstrumentID}

	switch rec.Type {
	case "instrument_definition":
		return s.applyInstrumentDefinition(k, rec)
	case "snapshot_begin":
		return s.applySnapshotBegin(k, rec)
	case "snapshot_level":
		return s.applySnapshotLevel(k, rec)
	case "snapshot_end":
		return s.applySnapshotEnd(k, rec)
	case "level_update", "book_clear":
		evs := s.applyDelta(k, rec)
		s.noteConsistencyPoint(k, evs)
		return evs
	case "instrument_reset":
		return s.applyInstrumentReset(k, rec)
	case "batch_boundary":
		return s.applyBatchBoundary(rec)
	case "trade", "liquidation":
		// No book effect. Surfaced for the persistence layer only.
		return []ChannelEvent{{Kind: "applied_delta", InstrumentID: rec.InstrumentID, Record: rec}}
	}
	return nil
}

func (s *Shard) applyInstrumentDefinition(k instKey, rec Record) []ChannelEvent {
	symbol := toString(rec.Fields["symbol"])
	priceExp := toInt8(rec.Fields["price_exponent"])
	qtyExp := toInt8(rec.Fields["qty_exponent"])
	s.refdata[k] = InstrumentDef{
		Symbol:        symbol,
		PriceExponent: priceExp,
		QtyExponent:   qtyExp,
		ManifestSeq:   toUint16(rec.Fields["manifest_seq"]),
	}
	inst, ok := s.instruments[k]
	if !ok {
		s.instruments[k] = NewInstrument(k.id, symbol, priceExp, qtyExp)
	} else {
		inst.Symbol = symbol
		inst.PriceExponent = priceExp
		inst.QtyExponent = qtyExp
	}
	return []ChannelEvent{{Kind: "applied_delta", InstrumentID: k.id, Symbol: symbol, Record: rec}}
}

func (s *Shard) instrumentFor(k instKey) *Instrument {
	inst, ok := s.instruments[k]
	if !ok {
		def := s.refdata[k]
		inst = NewInstrument(k.id, def.Symbol, def.PriceExponent, def.QtyExponent)
		s.instruments[k] = inst
	}
	return inst
}

func (s *Shard) applySnapshotBegin(k instKey, rec Record) []ChannelEvent {
	inst := s.instrumentFor(k)
	anchor := toUint64(rec.Fields["anchor_seq"])
	lastInstr := toUint32(rec.Fields["last_instrument_seq"])

	ok, err := inst.SnapshotAcceptable(anchor, lastInstr)
	if err != nil {
		// Stale anchor: a snapshot captured before an InstrumentReset but
		// delivered after it. Accepting it would leave the instrument ready
		// holding exactly the diverged book the reset existed to discard.
		if s.metrics != nil && errors.Is(err, errStaleAnchor) {
			s.metrics.SnapshotDiscardedTotal.WithLabelValues("stale_anchor").Inc()
		}
		return nil
	}
	if !ok {
		// Ready and current. Ignoring the snapshot is the ordinary case; deltas
		// have kept this book correct.
		return nil
	}
	inst.BeginSnapshot(
		toUint32(rec.Fields["snapshot_id"]),
		anchor,
		toUint32(rec.Fields["total_levels"]),
		lastInstr,
		toUint32(rec.Fields["depth_bound"]),
	)
	return nil
}

func (s *Shard) applySnapshotLevel(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		if s.metrics != nil {
			s.metrics.SnapshotLevelDroppedTotal.Inc()
		}
		return nil
	}
	added := inst.AddSnapshotLevel(
		toUint32(rec.Fields["snapshot_id"]),
		sideFromString(toString(rec.Fields["side"])),
		toInt64(rec.Fields["price_raw"]),
		toUint64(rec.Fields["qty_raw"]),
		orderCountFrom(rec.Fields),
		toUint8(rec.Fields["level_flags"]),
	)
	if !added && s.metrics != nil {
		s.metrics.SnapshotLevelDroppedTotal.Inc()
	}
	return nil
}

func (s *Shard) applySnapshotEnd(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	if inst.OpenSnapshot == nil {
		// No shadow in progress: the begin was ignored or discarded. Never demote.
		return nil
	}
	err := inst.EndSnapshot(toUint32(rec.Fields["snapshot_id"]), toUint64(rec.Fields["anchor_seq"]))
	if err != nil {
		if s.metrics != nil {
			s.metrics.SnapshotDiscardedTotal.WithLabelValues(discardReason(err)).Inc()
		}
		log.Printf("shard %d instrument %d: snapshot discarded: %v", s.idx, k.id, err)
		return nil // shadow only; live book and status untouched
	}
	s.replayBuffer(k, inst)
	evs := []ChannelEvent{{Kind: "applied_snapshot", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
	s.noteConsistencyPoint(k, evs)
	return evs
}

func discardReason(err error) string {
	switch {
	case errors.Is(err, errSnapshotShort):
		return "short"
	case errors.Is(err, errSnapshotMismatch):
		return "mismatch"
	case errors.Is(err, errNoOpenSnapshot):
		return "no_open_snapshot"
	default:
		return "other"
	}
}

func (s *Shard) applyInstrumentReset(k instKey, rec Record) []ChannelEvent {
	inst, ok := s.instruments[k]
	if !ok {
		return nil
	}
	anchor := toUint64(rec.Fields["new_anchor_seq"])
	inst.Reset(&anchor)

	// Drop buffered deltas the reset supersedes, keeping bufferedN in step. The
	// running total must be adjusted by exactly the number removed, or the shard
	// budget drifts.
	before := len(s.deltaBuf[k])
	kept := filterBuffer(s.deltaBuf[k], func(b BufferedDelta) bool { return b.MktdataSeq > anchor })
	s.bufferedN -= before - len(kept)
	if len(kept) == 0 {
		delete(s.deltaBuf, k)
	} else {
		s.deltaBuf[k] = kept
	}
	if s.metrics != nil {
		s.metrics.InstrumentResetsTotal.WithLabelValues(toString(rec.Fields["reason"])).Inc()
		s.metrics.DeltaBufferedRecords.Set(float64(s.bufferedN))
	}
	// A reset clears the book, so the instrument can no longer be crossed.
	delete(s.crossed, k)
	delete(s.touched, k)
	s.publishCrossedGauge()
	return []ChannelEvent{{Kind: "instrument_reset", InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
}

// applyBatchBoundary marks the channel as batching and evaluates crossed-book
// for every instrument touched since the previous boundary.
//
// Evaluating only at boundaries is what makes the counter meaningful on a
// batching channel: intermediate states within a batch are explicitly not
// consistency points, so a transient cross there is legal rather than a defect.
func (s *Shard) applyBatchBoundary(rec Record) []ChannelEvent {
	s.sawBatchBoundary = true
	for k := range s.touched {
		if inst, ok := s.instruments[k]; ok {
			s.evaluateCrossed(k, inst)
		}
		delete(s.touched, k)
	}
	return []ChannelEvent{{Kind: "applied_delta", Record: rec}}
}

// noteConsistencyPoint records or evaluates crossed-book after a book change.
// On a channel with no BatchBoundary observed, every applied delta is a
// consistency point; once boundaries are seen, evaluation defers to them.
func (s *Shard) noteConsistencyPoint(k instKey, evs []ChannelEvent) {
	applied := false
	for _, e := range evs {
		if e.Kind == "applied_delta" || e.Kind == "applied_snapshot" {
			applied = true
			break
		}
	}
	if !applied {
		return
	}
	inst, ok := s.instruments[k]
	if !ok {
		return
	}
	if s.sawBatchBoundary {
		s.touched[k] = struct{}{}
		return
	}
	s.evaluateCrossed(k, inst)
}

// evaluateCrossed compares the inside market and counts a crossed observation.
//
// The spec says to compare at each consistency point and increment when crossed,
// so this counts per observation rather than per transition — a persistently
// crossed book keeps incrementing, which is the intended defect-rate reading.
// The gauge answers "how many are crossed right now".
//
// Observability only: it never changes status, discards a book, or triggers a
// re-bootstrap.
func (s *Shard) evaluateCrossed(k instKey, inst *Instrument) {
	if inst.Crossed() {
		s.crossed[k] = struct{}{}
		if s.metrics != nil {
			s.metrics.CrossedBookEventsTotal.Inc()
		}
	} else {
		delete(s.crossed, k)
	}
	s.publishCrossedGauge()
}

func (s *Shard) publishCrossedGauge() {
	if s.metrics != nil {
		s.metrics.CrossedInstruments.Set(float64(len(s.crossed)))
	}
}

// pruneManifest drops instruments that have fallen out of the manifest.
//
// Definitions are retransmitted continuously across a definition cycle, so
// instruments are re-advertised under a new Manifest Seq gradually rather than
// all at once. Pruning everything below newSeq on the bump would evict
// instruments that are still in the manifest but have not been re-advertised
// yet. A one-generation grace window keeps anything at newSeq-1 or later.
func (s *Shard) pruneManifest(newSeq uint16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newSeq <= 1 {
		return // no generation old enough to be stale
	}
	cutoff := newSeq - 1
	for k, def := range s.refdata {
		if def.ManifestSeq >= cutoff {
			continue
		}
		delete(s.refdata, k)
		delete(s.instruments, k)
		s.bufferedN -= len(s.deltaBuf[k])
		delete(s.deltaBuf, k)
		delete(s.crossed, k)
		delete(s.touched, k)
	}
	s.publishCrossedGauge()
	if s.metrics != nil {
		s.metrics.DeltaBufferedRecords.Set(float64(s.bufferedN))
	}
}

func (s *Shard) reset() {
	s.instruments = map[instKey]*Instrument{}
	s.refdata = map[instKey]InstrumentDef{}
	s.deltaBuf = map[instKey][]BufferedDelta{}
	s.bufferedN = 0
	s.crossed = map[instKey]struct{}{}
	s.touched = map[instKey]struct{}{}
	s.sawBatchBoundary = false
}

// handle is the shard goroutine's per-record entry point.
func (s *Shard) handle(rec Record) {
	evs := s.apply(rec)
	_ = evs // the persistence layer consumes these in a follow-on plan
}

// Run processes the inbox until ctx is done.
func (s *Shard) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-s.inbox:
			switch msg.kind {
			case msgRecord:
				s.handle(*msg.rec)
			case msgManifestPrune:
				s.pruneManifest(msg.seq)
			case msgReset:
				s.mu.Lock()
				s.reset()
				s.mu.Unlock()
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
