package main

import (
	"errors"
	"fmt"
	"time"
)

// InstrumentStatus is the serving status of one instrument's book.
//
// The spec's five-state machine collapses to three here, because two of its
// states are represented orthogonally: "awaiting-refdata" is absence from the
// shard's instrument map, and "building-snapshot" is OpenSnapshot != nil, which
// is deliberately independent of serving status so that building a snapshot can
// never affect whether the current book is usable.
type InstrumentStatus int

const (
	StatusAwaitingSnapshot InstrumentStatus = iota
	StatusReady
	StatusGap
)

func (s InstrumentStatus) String() string {
	switch s {
	case StatusAwaitingSnapshot:
		return "awaiting-snapshot"
	case StatusReady:
		return "ready"
	case StatusGap:
		return "gap"
	default:
		return "unknown"
	}
}

// LevelState is one aggregated price level. Quantity is absolute.
type LevelState struct {
	QtyRaw     uint64
	OrderCount uint16 // u16Unavailable (0xFFFF) means the venue did not supply it
	Flags      uint8
}

// u16Unavailable mirrors the parser's sentinel: absent, or too large to express.
const u16Unavailable uint16 = 0xFFFF

// PendingSnapshot is the shadow built between SnapshotBegin and SnapshotEnd.
// It is never the live book: on any validation failure only the shadow is
// discarded, so a short snapshot cannot evict a book the deltas are keeping
// correct.
type PendingSnapshot struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	LastInstrumentSeq uint32
	DepthBound        uint32
	ReceivedLevels    uint32
	Bids, Asks        map[int64]*LevelState
}

// SnapshotGroup is the identity a SnapshotBegin establishes for the group of
// SnapshotLevel records that follow it.
//
// It is recorded whether or not the snapshot is accepted. SnapshotLevel records
// carry only snapshot_id, so the remaining four fields exist nowhere else, and a
// ready, current instrument DECLINES its periodic snapshot without opening a
// shadow while the publisher still sends every level of that group. Sourcing
// these from OpenSnapshot would leave the replay capture empty exactly when the
// feed is healthy.
type SnapshotGroup struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	LastInstrumentSeq uint32
	DepthBound        uint32
}

// Instrument holds the book and state-machine position for one
// (channel_id, instrument_id).
type Instrument struct {
	ID            uint32
	Symbol        string
	PriceExponent int8
	QtyExponent   int8
	Status        InstrumentStatus

	// Books keyed by RAW price. Rank is derived by sorting keys at read time;
	// the spec forbids keying book state on rank.
	Bids, Asks map[int64]*LevelState

	// DepthBound: nil = unknown, 0 = publisher claims a complete book,
	// N = bounded at N levels per side. Defaults to unknown and MUST NOT
	// default to 0 — a never-snapshotted instrument must not assert
	// completeness through the subscriber's own initialisation.
	DepthBound *uint32

	LastAppliedMktdataSeq    uint64
	LastAppliedInstrumentSeq uint32

	// LastAppliedSendTS is the publisher's send timestamp on the last record that
	// actually changed this book — an applied delta, or the snapshot_end that
	// committed a shadow. It is maintained alongside LastAppliedMktdataSeq and is
	// the only honest input for level_snapshots.publisher_send_ts: the read-out
	// is computed by this process, so it has no send timestamp of its own, and
	// stamping recv_ts into both columns made the schema's MATERIALIZED
	// wire_latency_ms structurally 0.0 for every row that could ever exist.
	LastAppliedSendTS time.Time

	// RequiredAnchorSeq is set by InstrumentReset. While non-nil, any
	// SnapshotBegin with an older Anchor Seq MUST be discarded.
	RequiredAnchorSeq *uint64

	// RequiredInstrumentSeq is the Per-Instrument Seq a recovery snapshot MUST
	// cover, set on every demotion to gap. While non-nil, a snapshot whose Last
	// Instrument Seq is below it is discarded.
	//
	// It is the Last Instrument Seq counterpart of RequiredAnchorSeq, and it is a
	// separate field rather than a reuse of it because the two answer different
	// questions from different series. A reset invalidates everything captured
	// before a channel-wide Anchor Seq; a demotion invalidates everything captured
	// before one instrument's own hole, and the hole is only expressible in that
	// instrument's series.
	//
	// nil is what distinguishes "never had a book" from "lost a known mutation".
	// A cold-start instrument must accept the first snapshot it is offered,
	// whatever its Last Instrument Seq — including 0, for an instrument with no
	// deltas yet — so the requirement cannot be derived from the trackers, which
	// read 0 in both cases.
	RequiredInstrumentSeq *uint32

	OpenSnapshot *PendingSnapshot
	Pending      map[uint32]Record // out-of-order deltas keyed by per_instrument_seq

	// LostInstrumentSeq holds the Per-Instrument Seq of every book-affecting
	// message this process classified as malformed and therefore never applied.
	//
	// The publisher consumed the sequence for it, so nothing will ever fill the
	// hole: without this set, the recovery that follows re-walks the reorder
	// window over a number that cannot arrive. That costs a whole window of
	// valid buffered deltas and a per_instrument_gaps_total the book engine
	// caused itself — the counter this demotion exists to keep clean. It is
	// consulted when the next expected sequence is computed and pruned whenever
	// the tracker moves past an entry.
	LostInstrumentSeq map[uint32]bool

	// LastBegin is the identity of the most recent SnapshotBegin, accepted or
	// declined. Used only to denormalize group identity onto wire_levels rows.
	LastBegin *SnapshotGroup
}

func NewInstrument(id uint32, symbol string, priceExp, qtyExp int8) *Instrument {
	return &Instrument{
		ID:            id,
		Symbol:        symbol,
		PriceExponent: priceExp,
		QtyExponent:   qtyExp,
		Status:        StatusAwaitingSnapshot,
		Bids:          map[int64]*LevelState{},
		Asks:          map[int64]*LevelState{},
	}
}

func (i *Instrument) side(s uint8) map[int64]*LevelState {
	if s == 1 {
		return i.Asks
	}
	return i.Bids
}

// DivergenceKind classifies a publisher/subscriber disagreement that the spec
// asks a subscriber to count without altering the applied result.
type DivergenceKind string

const (
	DivergenceNewOnPresent     DivergenceKind = "new_on_present"
	DivergenceChangeOnAbsent   DivergenceKind = "change_on_absent"
	DivergenceDeleteNonzeroQty DivergenceKind = "delete_nonzero_qty"
	DivergenceZeroQtyBadAction DivergenceKind = "zero_qty_wrong_action"
)

// ApplyLevelUpdate applies the spec's absolute-quantity rule and returns any
// divergence observed. Action NEVER gates the apply: every LevelUpdate states
// the complete resulting state of one level, so applying by quantity alone
// always produces the correct level regardless of what Action claims.
func (i *Instrument) ApplyLevelUpdate(sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags, action uint8) []DivergenceKind {
	book := i.side(sideByte)
	_, present := book[priceRaw]

	// Independent checks, deliberately NOT a switch. The spec's four divergence
	// conditions are not mutually exclusive — Quantity=0 with Action=New on an
	// already-present level violates two of them at once — and the spec asks a
	// subscriber to surface each. A switch fires at most one case and would
	// silently drop the rest, under-reporting exactly the doubly-malformed
	// messages that most deserve attention.
	var div []DivergenceKind
	if qtyRaw == 0 && action != 3 {
		// Publisher rule: Quantity 0 is only legal with Action=Delete.
		div = append(div, DivergenceZeroQtyBadAction)
	}
	if qtyRaw != 0 && action == 3 {
		div = append(div, DivergenceDeleteNonzeroQty)
	}
	if action == 1 && present {
		div = append(div, DivergenceNewOnPresent)
	}
	if action == 2 && !present {
		div = append(div, DivergenceChangeOnAbsent)
	}

	if qtyRaw == 0 {
		delete(book, priceRaw)
		return div
	}
	book[priceRaw] = &LevelState{QtyRaw: qtyRaw, OrderCount: orderCount, Flags: flags}
	return div
}

var errBookClearScopeSide = errors.New("book_clear scope=1 with clear_side=both")

var errBookClearReserved = errors.New("book_clear carries a reserved Clear Side or Scope value")

// The values clearSideFromString and scopeFromString return for a spelling the
// parser does not define. They are outside both wire enumerations — Clear Side
// is 0..2 and Scope is 0..1 — so they cannot collide with a real value.
const (
	clearSideReserved uint8 = 0xff
	scopeReserved     uint8 = 0xff
)

// Reason labels for malformed_deltas_total. The values match the parser's
// dz_mbp_parser_malformed_total{reason} for the same wire conditions, so the
// decode-side and book-side counters line up on one dashboard.
const (
	reasonBookClearScopeSide = "bookclear_scope_side"
	// reasonMalformedOther labels a book-affecting message rejected by a rule
	// other than BookClear's scope/side rule. A BookClear carrying a reserved
	// Clear Side or Scope value lands here.
	reasonMalformedOther = "other"
)

// malformedReason names the rule a book-affecting message broke, for the metric
// label and the log line.
func malformedReason(err error) string {
	if errors.Is(err, errBookClearScopeSide) {
		return reasonBookClearScopeSide
	}
	return reasonMalformedOther
}

// bookClearMalformed reports the rule a BookClear breaks, or nil when it is
// well-formed.
//
// It reads the message's own fields and nothing else, which is what lets a record
// be classified the moment it arrives — before its Per-Instrument Seq says
// whether it is ready to apply, and therefore before the reorder window can hold
// it. Shard.applyDeltaToReady classifies on receipt for exactly that reason.
func bookClearMalformed(clearSide, scope uint8) error {
	// A reserved value first, because the rule below reads both as if they were
	// defined. The spec states Clear Side 0..2 and Scope 0..1; anything else is
	// a publisher defect, and applying it would delete levels on a side the
	// message never named.
	if clearSide > 2 || scope > 1 {
		return fmt.Errorf("%w: clear_side=%d scope=%d", errBookClearReserved, clearSide, scope)
	}
	if scope == 1 && clearSide == 2 {
		// One price cannot bound both sides.
		return fmt.Errorf("%w", errBookClearScopeSide)
	}
	return nil
}

// ApplyBookClear removes levels in bulk. clearSide 0=bid, 1=ask, 2=both.
// scope 0 clears the whole side(s); scope 1 clears from fromPriceRaw outward —
// for bids every level at or below it, for asks every level at or above it.
//
// A BookClear is not a resynchronisation signal: an instrument that applies one
// stays ready.
func (i *Instrument) ApplyBookClear(clearSide, scope uint8, fromPriceRaw int64) error {
	if err := bookClearMalformed(clearSide, scope); err != nil {
		return err
	}
	clear := func(book map[int64]*LevelState, isBid bool) {
		if scope == 0 {
			for p := range book {
				delete(book, p)
			}
			return
		}
		for p := range book {
			if (isBid && p <= fromPriceRaw) || (!isBid && p >= fromPriceRaw) {
				delete(book, p)
			}
		}
	}
	if clearSide == 0 || clearSide == 2 {
		clear(i.Bids, true)
	}
	if clearSide == 1 || clearSide == 2 {
		clear(i.Asks, false)
	}
	return nil
}

// BeginSnapshot opens a shadow. Status and the live book are untouched.
func (i *Instrument) BeginSnapshot(snapID uint32, anchorSeq uint64, totalLevels, lastInstrSeq, depthBound uint32) {
	i.OpenSnapshot = &PendingSnapshot{
		SnapshotID:        snapID,
		AnchorSeq:         anchorSeq,
		TotalLevels:       totalLevels,
		LastInstrumentSeq: lastInstrSeq,
		DepthBound:        depthBound,
		Bids:              map[int64]*LevelState{},
		Asks:              map[int64]*LevelState{},
	}
}

// SnapshotLevelResult is why AddSnapshotLevel accepted or refused a level.
//
// The two refusals are deliberately distinct. "No open shadow" is the healthy
// steady state: a ready, current instrument declines its periodic snapshot at
// SnapshotBegin, yet the publisher still sends every level of that group and the
// coordinator still forwards them. Counting those as dropped would bury the
// misroute signal the drop counter exists to expose under ordinary traffic.
type SnapshotLevelResult int

const (
	SnapshotLevelAdded SnapshotLevelResult = iota
	// SnapshotLevelNoOpenShadow: the begin was declined or discarded by design.
	// Expected, and NOT a defect.
	SnapshotLevelNoOpenShadow
	// SnapshotLevelMismatch: a level whose Snapshot ID does not match the open
	// group. A real misroute, which is what the drop counter is for.
	SnapshotLevelMismatch
)

// AddSnapshotLevel inserts into the shadow, reporting whether it was accepted
// and, if not, which of the two refusals applied.
func (i *Instrument) AddSnapshotLevel(snapID uint32, sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags uint8) SnapshotLevelResult {
	if i.OpenSnapshot == nil {
		return SnapshotLevelNoOpenShadow
	}
	if i.OpenSnapshot.SnapshotID != snapID {
		return SnapshotLevelMismatch
	}
	book := i.OpenSnapshot.Bids
	if sideByte == 1 {
		book = i.OpenSnapshot.Asks
	}
	book[priceRaw] = &LevelState{QtyRaw: qtyRaw, OrderCount: orderCount, Flags: flags}
	i.OpenSnapshot.ReceivedLevels++
	return SnapshotLevelAdded
}

var (
	errSnapshotMismatch = errors.New("snapshot end mismatch")
	errSnapshotShort    = errors.New("snapshot level count mismatch")
	errNoOpenSnapshot   = errors.New("snapshot end with no open snapshot")
	errStaleAnchor      = errors.New("snapshot anchor older than required anchor")
	// errStaleInstrumentSeq: the snapshot was captured before the hole that
	// demoted this instrument, so committing it would restore ready over a book
	// still missing the mutation the demotion was declared for.
	errStaleInstrumentSeq = errors.New("snapshot last instrument seq older than the hole")
)

// EndSnapshot validates and commits the shadow. On ANY failure only the shadow
// is discarded: Status, Bids, and Asks are never touched. For an instrument that
// was already Ready this deliberately departs from the spec's literal "discard
// the partial book and revert to awaiting-snapshot", because dropping a book the
// deltas are keeping correct costs a full round-robin cycle of availability and
// buys nothing — the spec's own gap-recovery schedule repairs a bad book on the
// next snapshot either way.
func (i *Instrument) EndSnapshot(snapID uint32, anchorSeq uint64) error {
	if i.OpenSnapshot == nil {
		return errNoOpenSnapshot
	}
	if i.OpenSnapshot.SnapshotID != snapID || i.OpenSnapshot.AnchorSeq != anchorSeq {
		i.OpenSnapshot = nil
		return fmt.Errorf("%w: snapshot_id=%d anchor=%d", errSnapshotMismatch, snapID, anchorSeq)
	}
	if i.OpenSnapshot.ReceivedLevels != i.OpenSnapshot.TotalLevels {
		got, want := i.OpenSnapshot.ReceivedLevels, i.OpenSnapshot.TotalLevels
		i.OpenSnapshot = nil
		return fmt.Errorf("%w: got %d expected %d", errSnapshotShort, got, want)
	}
	// Re-checked here and not only at the begin, because the requirement can be
	// set while the shadow is already open: a ready instrument that was behind
	// opens a shadow, a delta then gaps it, and the shadow it opened while healthy
	// was captured before that hole. SnapshotAcceptable never saw the hole, so
	// only this check stands between the commit and a ready status over a book
	// missing the mutation the demotion was declared for.
	if i.RequiredInstrumentSeq != nil && i.OpenSnapshot.LastInstrumentSeq < *i.RequiredInstrumentSeq {
		got, want := i.OpenSnapshot.LastInstrumentSeq, *i.RequiredInstrumentSeq
		i.OpenSnapshot = nil
		return fmt.Errorf("%w: last_instrument_seq=%d required=%d", errStaleInstrumentSeq, got, want)
	}

	depth := i.OpenSnapshot.DepthBound
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	i.LastAppliedMktdataSeq = i.OpenSnapshot.AnchorSeq
	i.LastAppliedInstrumentSeq = i.OpenSnapshot.LastInstrumentSeq
	// A snapshot at or past a lost sequence already accounts for it: its book
	// is the state after that message would have applied. One captured BEFORE
	// it does not, and that entry has to survive — it is the whole reason this
	// set exists.
	i.pruneLostInstrumentSeq()
	i.DepthBound = &depth
	// Clear the required anchor on ANY accepted snapshot at or after it, not
	// only an exact match: the publisher's mandated snapshot at S' can itself be
	// lost, and the next round-robin snapshot carries a newer anchor and is a
	// perfectly good recovery. Clearing only on exact match would leave the
	// required anchor set permanently.
	if i.RequiredAnchorSeq != nil && i.OpenSnapshot.AnchorSeq >= *i.RequiredAnchorSeq {
		i.RequiredAnchorSeq = nil
	}
	// The guard above returned for every shadow below the requirement, so
	// reaching here means this snapshot covers the hole: the demotion is repaired
	// and the requirement is spent.
	i.RequiredInstrumentSeq = nil
	i.OpenSnapshot = nil
	// Free Pending too. Its entries are keyed to the pre-snapshot per-instrument
	// sequence, and LastAppliedInstrumentSeq has just jumped to the snapshot's
	// Last Instrument Seq, so none of them can ever match LastApplied+1 and drain.
	// Left behind they still count toward the reorder-window bound in
	// applyDeltaToReady, so ordinary reordering is eventually misclassified as a
	// gap and inflates per_instrument_gaps_total — the counter an operator reads
	// to judge feed loss.
	i.Pending = nil
	i.Status = StatusReady
	return nil
}

// SnapshotAcceptable decides whether a SnapshotBegin should be processed.
//
// The discriminator is Last Instrument Seq, NOT Anchor Seq. Anchor Seq is a
// channel-wide mktdata sequence that advances on every other instrument's
// deltas and on every heartbeat, so comparing it against this instrument's
// tracker would be true for nearly every instrument on nearly every cycle and
// would rebuild every good book on every rotation.
func (i *Instrument) SnapshotAcceptable(anchorSeq uint64, lastInstrSeq uint32) (bool, error) {
	if i.RequiredAnchorSeq != nil && anchorSeq < *i.RequiredAnchorSeq {
		return false, errStaleAnchor
	}
	// A gap instrument is NOT indiscriminately hungry for a book. It reaches gap
	// by losing a specific Per-Instrument Seq, and a snapshot captured before that
	// seq is a copy of the publisher's book from before the loss: committing it
	// restores ready over a book this book engine already knows is missing a
	// mutation, and every delta behind the hole then piles up in the reorder
	// window until it declares a second gap — landing a publisher defect, or an
	// eviction this process chose, in per_instrument_gaps_total.
	if i.RequiredInstrumentSeq != nil && lastInstrSeq < *i.RequiredInstrumentSeq {
		return false, errStaleInstrumentSeq
	}
	if i.Status != StatusReady {
		return true, nil
	}
	// Ready: only re-bootstrap when the snapshot was captured after deltas this
	// subscriber never applied.
	return lastInstrSeq > i.LastAppliedInstrumentSeq, nil
}

// RequireSnapshotAtLeast records that recovery needs a snapshot captured at or
// after Per-Instrument Seq instrSeq. Every demotion to gap calls it with the seq
// whose mutation was lost, so the three demotion paths — sequence gap, delta
// buffer eviction, malformed record — all state the same requirement in the same
// series.
//
// It never lowers the requirement. A gapped instrument can be demoted again
// before it recovers — an eviction on a book a malformed record already gapped —
// and a snapshot has to cover every seq that was lost, so what it must reach is
// the newest of them. Taking the latest demotion's seq unconditionally would let
// a second demotion behind the first waive the first one's hole.
func (i *Instrument) RequireSnapshotAtLeast(instrSeq uint32) {
	if i.RequiredInstrumentSeq != nil && *i.RequiredInstrumentSeq >= instrSeq {
		return
	}
	seq := instrSeq
	i.RequiredInstrumentSeq = &seq
}

// Crossed reports whether the inside market is crossed. Strict >, so a locked
// book (best bid == best ask), which is routine on some venues, is not counted.
func (i *Instrument) Crossed() bool {
	if len(i.Bids) == 0 || len(i.Asks) == 0 {
		return false
	}
	bestBid, bestAsk := int64(0), int64(0)
	first := true
	for p := range i.Bids {
		if first || p > bestBid {
			bestBid, first = p, false
		}
	}
	first = true
	for p := range i.Asks {
		if first || p < bestAsk {
			bestAsk, first = p, false
		}
	}
	return bestBid > bestAsk
}

// Reset discards all level state and returns to awaiting-snapshot, recording
// the required anchor from an InstrumentReset.
func (i *Instrument) Reset(requiredAnchor *uint64) {
	i.Bids = map[int64]*LevelState{}
	i.Asks = map[int64]*LevelState{}
	i.OpenSnapshot = nil
	i.Pending = nil
	i.Status = StatusAwaitingSnapshot
	i.LastAppliedMktdataSeq = 0
	i.LastAppliedInstrumentSeq = 0
	i.LastAppliedSendTS = time.Time{}
	// A reset starts a new Per-Instrument Seq series, so every number in the
	// old one is meaningless rather than merely applied.
	i.LostInstrumentSeq = nil
	i.DepthBound = nil // back to unknown, never 0
	i.RequiredAnchorSeq = requiredAnchor
	// The reset restarts the Per-Instrument Seq series, so a requirement recorded
	// against the old one names a seq that will not come round again for a whole
	// wrap. Left set it would refuse every post-reset snapshot and wedge the
	// instrument in awaiting-snapshot; RequiredAnchorSeq is what guards recovery
	// from here.
	i.RequiredInstrumentSeq = nil
}

// NextExpectedInstrumentSeq is the Per-Instrument Seq this instrument is
// waiting for: one past the last applied, advanced over any sequence recorded
// as permanently lost.
//
// A malformed book-affecting message consumes its sequence at the publisher and
// is never applied here, so the number it took can never arrive. Treating it as
// a hole sends the instrument through the reorder window to a gap it declares
// against itself.
func (i *Instrument) NextExpectedInstrumentSeq() uint32 {
	expected := i.LastAppliedInstrumentSeq + 1
	for i.LostInstrumentSeq[expected] {
		expected++
	}
	return expected
}

// MarkInstrumentSeqLost records that a Per-Instrument Seq was consumed by a
// message this process will never apply.
func (i *Instrument) MarkInstrumentSeqLost(seq uint32) {
	if i.LostInstrumentSeq == nil {
		i.LostInstrumentSeq = map[uint32]bool{}
	}
	i.LostInstrumentSeq[seq] = true
}

// pruneLostInstrumentSeq drops the entries the tracker has moved past, so the
// set cannot grow for the life of the process.
func (i *Instrument) pruneLostInstrumentSeq() {
	for seq := range i.LostInstrumentSeq {
		if seq <= i.LastAppliedInstrumentSeq {
			delete(i.LostInstrumentSeq, seq)
		}
	}
	if len(i.LostInstrumentSeq) == 0 {
		i.LostInstrumentSeq = nil
	}
}
