package main

import (
	"math"
	"testing"
)

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// readyInstrument builds a ready instrument with the given exponents.
func readyInstrument(priceExp, qtyExp int8) *Instrument {
	inst := NewInstrument(7, "SYM", priceExp, qtyExp)
	inst.Status = StatusReady
	return inst
}

func TestComputeLevels_SortsBidsDescendingAsksAscending(t *testing.T) {
	inst := readyInstrument(0, 0)
	// Insert out of order; the read-out establishes rank, never the wire.
	for _, p := range []int64{1000, 1200, 1100} {
		inst.Bids[p] = &LevelState{QtyRaw: 1}
	}
	for _, p := range []int64{1500, 1300, 1400} {
		inst.Asks[p] = &LevelState{QtyRaw: 1}
	}

	snap := ComputeLevels(inst, 10)

	wantBids := []float64{1200, 1100, 1000} // best bid is the highest
	for i, want := range wantBids {
		if !closeTo(snap.Bids[i].Price, want) {
			t.Errorf("bid %d: got %v want %v", i, snap.Bids[i].Price, want)
		}
	}
	wantAsks := []float64{1300, 1400, 1500} // best ask is the lowest
	for i, want := range wantAsks {
		if !closeTo(snap.Asks[i].Price, want) {
			t.Errorf("ask %d: got %v want %v", i, snap.Asks[i].Price, want)
		}
	}
}

func TestComputeLevels_ScalesByExponents(t *testing.T) {
	inst := readyInstrument(-2, -8)
	inst.Bids[123456] = &LevelState{QtyRaw: 100000000, OrderCount: 3}

	snap := ComputeLevels(inst, 10)

	if len(snap.Bids) != 1 {
		t.Fatalf("bids: %+v", snap.Bids)
	}
	if !closeTo(snap.Bids[0].Price, 1234.56) {
		t.Errorf("price: got %v want 1234.56", snap.Bids[0].Price)
	}
	if !closeTo(snap.Bids[0].Qty, 1.0) {
		t.Errorf("qty: got %v want 1.0", snap.Bids[0].Qty)
	}
	if snap.Bids[0].OrderCount != 3 {
		t.Errorf("order count: got %d want 3", snap.Bids[0].OrderCount)
	}
}

func TestComputeLevels_TopNTruncates(t *testing.T) {
	inst := readyInstrument(0, 0)
	// Five bids; only the best three may be returned.
	for i, p := range []int64{1000, 1100, 1200, 1300, 1400} {
		inst.Bids[p] = &LevelState{QtyRaw: uint64(i + 1)}
	}

	snap := ComputeLevels(inst, 3)

	if len(snap.Bids) != 3 {
		t.Fatalf("top-n must truncate: got %d want 3", len(snap.Bids))
	}
	// Ranked best-first: 1400 (qty 5), 1300 (qty 4), 1200 (qty 3).
	wantQty := []float64{5, 4, 3}
	wantCum := []float64{5, 9, 12}
	for i := range wantQty {
		if !closeTo(snap.Bids[i].Qty, wantQty[i]) {
			t.Errorf("bid %d qty: got %v want %v", i, snap.Bids[i].Qty, wantQty[i])
		}
		if !closeTo(snap.Bids[i].CumulativeQty, wantCum[i]) {
			t.Errorf("bid %d cumulative: got %v want %v", i, snap.Bids[i].CumulativeQty, wantCum[i])
		}
	}
}

// The 0xFFFF sentinel means "not provided", so it must not read out as 65535.
func TestComputeLevels_OrderCountSentinelBecomesZero(t *testing.T) {
	inst := readyInstrument(0, 0)
	inst.Bids[1000] = &LevelState{QtyRaw: 1, OrderCount: u16Unavailable}
	inst.Asks[1100] = &LevelState{QtyRaw: 1, OrderCount: 0} // a real zero

	snap := ComputeLevels(inst, 10)

	if snap.Bids[0].OrderCount != 0 {
		t.Errorf("sentinel must read out as 0, not 65535: got %d", snap.Bids[0].OrderCount)
	}
	if snap.Asks[0].OrderCount != 0 {
		t.Errorf("a real zero count stays 0: got %d", snap.Asks[0].OrderCount)
	}
}

func TestComputeLevels_CarriesDepthBoundAndCrossed(t *testing.T) {
	inst := readyInstrument(0, 0)
	inst.Bids[1200] = &LevelState{QtyRaw: 1}
	inst.Asks[1000] = &LevelState{QtyRaw: 1} // bid above ask: crossed

	snap := ComputeLevels(inst, 10)
	if snap.DepthBound != nil {
		t.Errorf("depth bound must stay unknown until a snapshot sets it: %v", snap.DepthBound)
	}
	if !snap.Crossed {
		t.Error("a crossed inside market must be reported")
	}
	if snap.InstrumentID != 7 || snap.Symbol != "SYM" {
		t.Errorf("identity: %d %q", snap.InstrumentID, snap.Symbol)
	}

	// A publisher claim of completeness is carried through as a non-nil 0.
	var complete uint32
	inst.DepthBound = &complete
	if snap := ComputeLevels(inst, 10); snap.DepthBound == nil || *snap.DepthBound != 0 {
		t.Errorf("depth bound 0 is a positive claim of completeness: %v", snap.DepthBound)
	}
}
