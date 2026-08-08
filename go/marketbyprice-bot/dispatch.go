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
		// No book effect. Surfaced for the persistence layer only, so it must not
		// claim a mutation.
		return []ChannelEvent{{Kind: KindTrade, InstrumentID: rec.InstrumentID, Record: rec}}
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
	// Refdata only: the book is untouched, so this is not an applied delta.
	return []ChannelEvent{{Kind: KindInstrumentDefinition, InstrumentID: k.id, Symbol: symbol, Record: rec}}
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

	// Record the group identity before any accept/decline decision. Declining is
	// the steady-state case and its levels still arrive and still need capturing.
	inst.LastBegin = &SnapshotGroup{
		SnapshotID:        toUint32(rec.Fields["snapshot_id"]),
		AnchorSeq:         anchor,
		TotalLevels:       toUint32(rec.Fields["total_levels"]),
		LastInstrumentSeq: lastInstr,
		DepthBound:        toUint32(rec.Fields["depth_bound"]),
	}

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
	res := inst.AddSnapshotLevel(
		toUint32(rec.Fields["snapshot_id"]),
		sideFromString(toString(rec.Fields["side"])),
		toInt64(rec.Fields["price_raw"]),
		toUint64(rec.Fields["qty_raw"]),
		orderCountFrom(rec.Fields),
		toUint8(rec.Fields["level_flags"]),
	)
	// Capture for replay regardless of whether the level joined a shadow: a
	// declined snapshot is the steady-state case and its levels still describe
	// the publisher's book.
	if inst.LastBegin != nil {
		def := s.refdata[k]
		if s.persists(def.Symbol) {
			s.eventsW.WriteWireLevel(rec, k.ch, *inst.LastBegin, def.Symbol, def.PriceExponent, def.QtyExponent)
		}
	}
	// Only a Snapshot ID mismatch is a misroute. SnapshotLevelNoOpenShadow is the
	// healthy steady state — a ready, current instrument declined this periodic
	// snapshot at SnapshotBegin, but the publisher still sends every level of the
	// group. Counting those would swamp the misroute signal with normal traffic.
	if res == SnapshotLevelMismatch && s.metrics != nil {
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
	// The commit replaced the whole book, so the snapshot_end that carried it is
	// the newest wire record this book reflects. replayBuffer may push it further
	// forward through applyOne; both write the same field.
	inst.LastAppliedSendTS = rec.sendTime()
	replayed := s.replayBuffer(k, inst)
	// The commit precedes the deltas replayed on top of it, so the snapshot event
	// leads and the replayed deltas follow in mktdata_seq order.
	evs := append(
		[]ChannelEvent{{Kind: KindAppliedSnapshot, InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}},
		replayed...,
	)
	s.noteConsistencyPoint(k, evs)
	return evs
}

// discardReason labels a snapshot discard.
//
// There is deliberately no "no_open_snapshot" reason. applySnapshotEnd returns
// before calling EndSnapshot when no shadow is open, so errNoOpenSnapshot cannot
// reach here — and that case is the healthy declined-snapshot path anyway (a
// ready, current instrument ignored the begin, and the end still arrives), not a
// discard worth counting. EndSnapshot keeps returning the error as an API guard
// for direct callers.
func discardReason(err error) string {
	switch {
	case errors.Is(err, errSnapshotShort):
		return "short"
	case errors.Is(err, errSnapshotMismatch):
		return "mismatch"
	default:
		return "other"
	}
}

func (s *Shard) applyInstrumentReset(k instKey, rec Record) []ChannelEvent {
	// instrumentFor, not an early return on absence, matching applySnapshotBegin.
	// At cold start the refdata cycle lags mktdata, so a reset routinely arrives
	// before the instrument's own definition. Dropping it there would discard the
	// RequiredAnchorSeq it carries, and the pre-reset snapshot it exists to
	// invalidate would then commit — leaving the instrument ready and serving the
	// diverged book, with no discard counted anywhere.
	inst := s.instrumentFor(k)
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
	}
	s.publishBufferedGauge()
	// A reset clears the book, so the instrument can no longer be crossed.
	delete(s.crossed, k)
	delete(s.touched, k)
	s.publishCrossedGauge()
	return []ChannelEvent{{Kind: KindInstrumentReset, InstrumentID: k.id, Symbol: inst.Symbol, Record: rec}}
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
	// A boundary is a consistency point, not a book mutation, and it carries no
	// instrument_id — reporting it as an applied delta on instrument 0 was doubly
	// wrong.
	return []ChannelEvent{{Kind: KindBatchBoundary, Record: rec}}
}

// noteConsistencyPoint records or evaluates crossed-book after a book change.
// On a channel with no BatchBoundary observed, every applied delta is a
// consistency point; once boundaries are seen, evaluation defers to them.
func (s *Shard) noteConsistencyPoint(k instKey, evs []ChannelEvent) {
	applied := false
	for _, e := range evs {
		if e.Kind == KindAppliedDelta || e.Kind == KindAppliedSnapshot {
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
	if s.crossedGauge != nil {
		s.crossedGauge.Set(float64(len(s.crossed)))
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
	s.publishBufferedGauge()
}

func (s *Shard) reset() {
	s.instruments = map[instKey]*Instrument{}
	s.refdata = map[instKey]InstrumentDef{}
	s.deltaBuf = map[instKey][]BufferedDelta{}
	s.bufferedN = 0
	s.crossed = map[instKey]struct{}{}
	s.touched = map[instKey]struct{}{}
	s.sawBatchBoundary = false
	// Republish both gauges. Zeroing the state without re-exporting leaves each
	// series holding its pre-reset value indefinitely on a shard that then goes
	// quiet — nothing else writes them until the next crossed book or buffered
	// delta, which may never come.
	s.publishCrossedGauge()
	s.publishBufferedGauge()
}

// clearShadows discards every in-flight snapshot shadow after a socket drop.
//
// Status and the live book are deliberately untouched: a shadow is never the
// live book, so abandoning a half-built one costs nothing a ready instrument was
// relying on, and the next snapshot cycle rebuilds it. Demoting here would throw
// away books the deltas are keeping correct.
func (s *Shard) clearShadows() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range s.instruments {
		inst.OpenSnapshot = nil
	}
}

// handle is the shard goroutine's per-record entry point.
func (s *Shard) handle(rec Record) {
	evs := s.apply(rec)
	if len(evs) == 0 {
		return
	}
	k := instKey{rec.ChannelID, rec.InstrumentID}
	def := s.refdataFor(k)
	persist := s.persists(def.Symbol)
	for _, ev := range evs {
		if persist && persistableFromShard(ev.Kind) {
			s.eventsW.Write(ev, rec.ChannelID, def.Symbol, def.PriceExponent, def.QtyExponent)
		}
		// MarkDirty stays outside the gate; the snapshot writer applies the filter
		// itself at flush time, so a filtered instrument's dirty entry is dropped
		// there rather than leaving stale gauge series behind.
		//
		// ONLY a real book mutation dirties an instrument. Dirtying on a
		// non-mutating kind would rewrite an unchanged book on every batch
		// boundary — which is why PR 3's review gave those paths their own kinds.
		if ev.Kind == KindAppliedDelta || ev.Kind == KindAppliedSnapshot {
			s.sw.MarkDirty(instKey{rec.ChannelID, ev.InstrumentID})
		}
	}
}

// persistableFromShard reports whether an event Kind may be written to `events`
// from the shard path.
//
// The engine already computes the right Kind for every path; the writer used to
// throw it away and switch on Record.Type alone, which is what made these three
// indistinguishable from real applied deltas.
//
//   - batch_boundary is channel-scoped and BROADCAST to every shard, so a
//     per-shard write turns one wire message into N rows. The Coordinator writes
//     the single row (see coordinator.go). The event itself is still returned,
//     because applyBatchBoundary's crossed-book evaluation depends on it.
//   - per_instrument_gap carries Record.Type "level_update" but the record was
//     BUFFERED, not applied.
//   - malformed_delta carries Record.Type "book_clear" but nothing was applied.
//
// `events` is defined as an applied-delta log, so the latter two do not belong
// in it at all — they are already observable as per_instrument_gaps_total and in
// the log line applyOne emits.
func persistableFromShard(kind string) bool {
	switch kind {
	case KindBatchBoundary, KindPerInstrumentGap, KindMalformedDelta:
		return false
	}
	return true
}

// refdataFor returns the instrument's definition, or a zero value when the
// definition has not arrived yet.
//
// It ACQUIRES s.mu itself, so it must never be called from code already holding
// the shard lock — that is the self-deadlock this split exists to avoid. handle
// calls it after s.apply has returned and released the lock.
func (s *Shard) refdataFor(k instKey) InstrumentDef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refdata[k]
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
			case msgClearShadows:
				s.clearShadows()
			case msgReset:
				s.mu.Lock()
				s.reset()
				s.mu.Unlock()
				// Drop queued snapshot work for books that no longer exist, and
				// bump the generation so a batch already extracted is abandoned
				// rather than written against post-reset state.
				s.sw.Reset(ctx)
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
