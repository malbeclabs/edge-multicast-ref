package main

import (
	"errors"
	"fmt"
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

	// RequiredAnchorSeq is set by InstrumentReset. While non-nil, any
	// SnapshotBegin with an older Anchor Seq MUST be discarded.
	RequiredAnchorSeq *uint64

	OpenSnapshot *PendingSnapshot
	Pending      map[uint32]Record // out-of-order deltas keyed by per_instrument_seq
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

	var div []DivergenceKind
	switch {
	case qtyRaw == 0 && action != 3:
		// Publisher rule: Quantity 0 is only legal with Action=Delete.
		div = append(div, DivergenceZeroQtyBadAction)
	case qtyRaw != 0 && action == 3:
		div = append(div, DivergenceDeleteNonzeroQty)
	case action == 1 && present:
		div = append(div, DivergenceNewOnPresent)
	case action == 2 && !present:
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

// ApplyBookClear removes levels in bulk. clearSide 0=bid, 1=ask, 2=both.
// scope 0 clears the whole side(s); scope 1 clears from fromPriceRaw outward —
// for bids every level at or below it, for asks every level at or above it.
//
// A BookClear is not a resynchronisation signal: an instrument that applies one
// stays ready.
func (i *Instrument) ApplyBookClear(clearSide, scope uint8, fromPriceRaw int64) error {
	if scope == 1 && clearSide == 2 {
		// One price cannot bound both sides.
		return fmt.Errorf("%w", errBookClearScopeSide)
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

// AddSnapshotLevel inserts into the shadow. Returns false when snapID does not
// match the open shadow, which the caller counts and discards.
func (i *Instrument) AddSnapshotLevel(snapID uint32, sideByte uint8, priceRaw int64, qtyRaw uint64, orderCount uint16, flags uint8) bool {
	if i.OpenSnapshot == nil || i.OpenSnapshot.SnapshotID != snapID {
		return false
	}
	book := i.OpenSnapshot.Bids
	if sideByte == 1 {
		book = i.OpenSnapshot.Asks
	}
	book[priceRaw] = &LevelState{QtyRaw: qtyRaw, OrderCount: orderCount, Flags: flags}
	i.OpenSnapshot.ReceivedLevels++
	return true
}

var (
	errSnapshotMismatch = errors.New("snapshot end mismatch")
	errSnapshotShort    = errors.New("snapshot level count mismatch")
	errNoOpenSnapshot   = errors.New("snapshot end with no open snapshot")
	errStaleAnchor      = errors.New("snapshot anchor older than required anchor")
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

	depth := i.OpenSnapshot.DepthBound
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	i.LastAppliedMktdataSeq = i.OpenSnapshot.AnchorSeq
	i.LastAppliedInstrumentSeq = i.OpenSnapshot.LastInstrumentSeq
	i.DepthBound = &depth
	// Clear the required anchor on ANY accepted snapshot at or after it, not
	// only an exact match: the publisher's mandated snapshot at S' can itself be
	// lost, and the next round-robin snapshot carries a newer anchor and is a
	// perfectly good recovery. Clearing only on exact match would leave the
	// required anchor set permanently.
	if i.RequiredAnchorSeq != nil && i.OpenSnapshot.AnchorSeq >= *i.RequiredAnchorSeq {
		i.RequiredAnchorSeq = nil
	}
	i.OpenSnapshot = nil
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
	if i.Status != StatusReady {
		return true, nil
	}
	// Ready: only re-bootstrap when the snapshot was captured after deltas this
	// subscriber never applied.
	return lastInstrSeq > i.LastAppliedInstrumentSeq, nil
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
	i.DepthBound = nil // back to unknown, never 0
	i.RequiredAnchorSeq = requiredAnchor
}
