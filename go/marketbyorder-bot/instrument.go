package main

import (
	"errors"
	"fmt"
	"time"
)

// InstrumentStatus tracks the state-machine position from the spec.
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

// RestingOrder is one entry in the bid or ask map.
type RestingOrder struct {
	OrderID  uint64
	Side     uint8
	Flags    uint8
	EnterTS  time.Time
	Price    int64  // raw, scale via Instrument.PriceExponent for display
	Quantity uint64 // raw, decremented on partial fills
}

// PendingSnapshot is the state held during SnapshotBegin..SnapshotEnd.
type PendingSnapshot struct {
	SnapshotID        uint32
	AnchorSeq         uint64
	TotalOrders       uint32
	LastInstrumentSeq uint32
	ReceivedOrders    uint32
	Bids              map[uint64]*RestingOrder
	Asks              map[uint64]*RestingOrder
}

// Instrument holds the live book and state-machine position for one (channel_id, instrument_id).
type Instrument struct {
	ID                       uint32
	Symbol                   string
	PriceExponent            int8
	QtyExponent              int8
	Status                   InstrumentStatus
	Bids                     map[uint64]*RestingOrder
	Asks                     map[uint64]*RestingOrder
	LastAppliedMktdataSeq    uint64
	LastAppliedInstrumentSeq uint32
	OpenSnapshot             *PendingSnapshot
}

// NewInstrument returns an Instrument awaiting its first snapshot.
func NewInstrument(id uint32, symbol string, priceExp, qtyExp int8) *Instrument {
	return &Instrument{
		ID:            id,
		Symbol:        symbol,
		PriceExponent: priceExp,
		QtyExponent:   qtyExp,
		Status:        StatusAwaitingSnapshot,
		Bids:          map[uint64]*RestingOrder{},
		Asks:          map[uint64]*RestingOrder{},
	}
}

// ApplyOrderAdd inserts a new resting order. Caller is responsible for seq checks
// and for status==Ready precondition.
func (i *Instrument) ApplyOrderAdd(orderID uint64, side, flags uint8, enterTS time.Time, price int64, qty uint64) {
	o := &RestingOrder{
		OrderID:  orderID,
		Side:     side,
		Flags:    flags,
		EnterTS:  enterTS,
		Price:    price,
		Quantity: qty,
	}
	if side == 0 {
		i.Bids[orderID] = o
	} else {
		i.Asks[orderID] = o
	}
}

// ApplyOrderCancel removes an order. Cancels for unknown order_ids are silently dropped
// per the spec ("MAY receive OrderCancel for an Order ID it does not have").
func (i *Instrument) ApplyOrderCancel(orderID uint64) {
	delete(i.Bids, orderID)
	delete(i.Asks, orderID)
}

// ApplyOrderExecute decrements the resting order's quantity. If exec_flags bit 0
// (full-fill) is set, or remaining quantity reaches 0, the order is removed.
// Cancels for unknown order_ids are silently dropped.
func (i *Instrument) ApplyOrderExecute(orderID uint64, execFlags uint8, execQty uint64) {
	if o, ok := i.Bids[orderID]; ok {
		i.applyExecToOrder(i.Bids, o, execFlags, execQty)
		return
	}
	if o, ok := i.Asks[orderID]; ok {
		i.applyExecToOrder(i.Asks, o, execFlags, execQty)
	}
}

func (i *Instrument) applyExecToOrder(book map[uint64]*RestingOrder, o *RestingOrder, execFlags uint8, execQty uint64) {
	if execFlags&0x01 != 0 {
		delete(book, o.OrderID)
		return
	}
	if execQty >= o.Quantity {
		o.Quantity = 0
	} else {
		o.Quantity -= execQty
	}
	if o.Quantity == 0 {
		delete(book, o.OrderID)
	}
}

// BeginSnapshot opens a new shadow PendingSnapshot without changing Status or
// the live Bids/Asks. If a snapshot is already open, it is silently replaced.
func (i *Instrument) BeginSnapshot(snapID uint32, anchorSeq uint64, totalOrders, lastInstrSeq uint32) {
	i.OpenSnapshot = &PendingSnapshot{
		SnapshotID:        snapID,
		AnchorSeq:         anchorSeq,
		TotalOrders:       totalOrders,
		LastInstrumentSeq: lastInstrSeq,
		Bids:              map[uint64]*RestingOrder{},
		Asks:              map[uint64]*RestingOrder{},
	}
}

// AddSnapshotOrder appends an order to the pending snapshot. snapID must match the open one.
// Returns true if the order was added; false if snapID mismatched.
func (i *Instrument) AddSnapshotOrder(snapID uint32, orderID uint64, side, flags uint8, enterTS time.Time, price int64, qty uint64) bool {
	if i.OpenSnapshot == nil || i.OpenSnapshot.SnapshotID != snapID {
		return false
	}
	o := &RestingOrder{OrderID: orderID, Side: side, Flags: flags, EnterTS: enterTS, Price: price, Quantity: qty}
	if side == 0 {
		i.OpenSnapshot.Bids[orderID] = o
	} else {
		i.OpenSnapshot.Asks[orderID] = o
	}
	i.OpenSnapshot.ReceivedOrders++
	return true
}

var (
	errSnapshotMismatch = errors.New("snapshot end mismatch")
	errSnapshotShort    = errors.New("snapshot order count short")
	errNoOpenSnapshot   = errors.New("snapshot end with no open snapshot")
)

// EndSnapshot validates and commits the pending snapshot. Returns the AnchorSeq
// and LastInstrumentSeq on success, or an error if validation fails.
// On any failure, only the shadow (OpenSnapshot) is discarded — Status, Bids,
// and Asks are never touched.
func (i *Instrument) EndSnapshot(snapID uint32, anchorSeq uint64) (uint64, uint32, error) {
	if i.OpenSnapshot == nil {
		return 0, 0, errNoOpenSnapshot
	}
	if i.OpenSnapshot.SnapshotID != snapID || i.OpenSnapshot.AnchorSeq != anchorSeq {
		i.OpenSnapshot = nil // discard shadow only; live book & Status untouched
		return 0, 0, fmt.Errorf("%w: snapshot_id=%d anchor=%d", errSnapshotMismatch, snapID, anchorSeq)
	}
	if i.OpenSnapshot.ReceivedOrders != i.OpenSnapshot.TotalOrders {
		got, want := i.OpenSnapshot.ReceivedOrders, i.OpenSnapshot.TotalOrders
		i.OpenSnapshot = nil // discard shadow only; live book & Status untouched
		return 0, 0, fmt.Errorf("%w: got %d expected %d", errSnapshotShort, got, want)
	}

	// Commit atomically.
	i.Bids = i.OpenSnapshot.Bids
	i.Asks = i.OpenSnapshot.Asks
	anchor := i.OpenSnapshot.AnchorSeq
	lastInstr := i.OpenSnapshot.LastInstrumentSeq
	i.OpenSnapshot = nil
	i.Status = StatusReady
	i.LastAppliedMktdataSeq = anchor
	i.LastAppliedInstrumentSeq = lastInstr
	return anchor, lastInstr, nil
}

// Reset discards the entire book and pending snapshot, returning to awaiting-snapshot.
func (i *Instrument) Reset() {
	i.Bids = map[uint64]*RestingOrder{}
	i.Asks = map[uint64]*RestingOrder{}
	i.OpenSnapshot = nil
	i.Status = StatusAwaitingSnapshot
	i.LastAppliedMktdataSeq = 0
	i.LastAppliedInstrumentSeq = 0
}
