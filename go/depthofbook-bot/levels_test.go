package main

import (
	"math"
	"testing"
	"time"
)

func approxEq(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestLevels_BidsDescendingAsksAscending(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0) // exponents 0 → no scaling
	inst.Status = StatusReady

	// Bids at three prices.
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 102, 3)
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 101, 7)

	// Asks at two prices.
	inst.ApplyOrderAdd(10, 1, 0, time.Now(), 105, 4)
	inst.ApplyOrderAdd(11, 1, 0, time.Now(), 104, 2)

	snap := ComputeLevels(inst, 5)
	if len(snap.Bids) != 3 || len(snap.Asks) != 2 {
		t.Fatalf("counts: bids=%d asks=%d", len(snap.Bids), len(snap.Asks))
	}
	if !approxEq(snap.Bids[0].Price, 102) || !approxEq(snap.Bids[1].Price, 101) || !approxEq(snap.Bids[2].Price, 100) {
		t.Errorf("bids order: %+v", snap.Bids)
	}
	if !approxEq(snap.Asks[0].Price, 104) || !approxEq(snap.Asks[1].Price, 105) {
		t.Errorf("asks order: %+v", snap.Asks)
	}
}

func TestLevels_TiesAggregate(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 100, 3) // tie with order 1
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 99, 7)

	snap := ComputeLevels(inst, 5)
	if len(snap.Bids) != 2 {
		t.Fatalf("expected 2 levels, got %d", len(snap.Bids))
	}
	if !approxEq(snap.Bids[0].Qty, 8) || snap.Bids[0].OrderCount != 2 {
		t.Errorf("level 0: qty=%v count=%d", snap.Bids[0].Qty, snap.Bids[0].OrderCount)
	}
}

func TestLevels_DepthCap(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	for i := int64(1); i <= 30; i++ {
		inst.ApplyOrderAdd(uint64(i), 0, 0, time.Now(), int64(100-i), 1) // 30 distinct prices
	}
	snap := ComputeLevels(inst, 10)
	if len(snap.Bids) != 10 {
		t.Errorf("expected 10 levels, got %d", len(snap.Bids))
	}
}

func TestLevels_CumulativeQty(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 100, 5)
	inst.ApplyOrderAdd(2, 0, 0, time.Now(), 99, 3)
	inst.ApplyOrderAdd(3, 0, 0, time.Now(), 98, 7)

	snap := ComputeLevels(inst, 5)
	if !approxEq(snap.Bids[0].CumulativeQty, 5) ||
		!approxEq(snap.Bids[1].CumulativeQty, 8) ||
		!approxEq(snap.Bids[2].CumulativeQty, 15) {
		t.Errorf("cumulative: %+v", snap.Bids)
	}
}

func TestLevels_PriceExponentScaling(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 8244600, 300000000) // raw values
	snap := ComputeLevels(inst, 5)
	if !approxEq(snap.Bids[0].Price, 82446) {
		t.Errorf("scaled price: %v", snap.Bids[0].Price)
	}
	if !approxEq(snap.Bids[0].Qty, 3.0) {
		t.Errorf("scaled qty: %v", snap.Bids[0].Qty)
	}
}

func TestLevels_EmptySide(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", 0, 0)
	inst.Status = StatusReady
	snap := ComputeLevels(inst, 5)
	if snap.Bids != nil || snap.Asks != nil {
		t.Errorf("expected nil for empty: bids=%v asks=%v", snap.Bids, snap.Asks)
	}
}
