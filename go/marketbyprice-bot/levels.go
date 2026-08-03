package main

import (
	"math"
	"sort"
)

// Level is one price level scaled into human units for read-out.
type Level struct {
	Price      float64 `json:"price"`
	Qty        float64 `json:"qty"`
	OrderCount uint32  `json:"order_count"`

	// CumulativeQty is the running sum of Qty from the inside market outward,
	// over the levels actually returned.
	//
	// It is exhaustive depth ONLY when the snapshot's DepthBound is a non-nil 0,
	// which is the publisher's positive claim that it carries the complete book.
	// Under a non-nil, non-zero bound, levels beyond the bound are unknown rather
	// than empty, so summing this understates available liquidity — the exact
	// failure Depth Bound exists to prevent. Under a nil bound nothing is known
	// at all.
	CumulativeQty float64 `json:"cumulative_qty"`
}

// LevelSnapshot is a point-in-time read of one instrument's book.
type LevelSnapshot struct {
	InstrumentID uint32  `json:"instrument_id"`
	Symbol       string  `json:"symbol"`
	Bids         []Level `json:"bids"`
	Asks         []Level `json:"asks"`

	// DepthBound: nil = unknown (no snapshot has established one), 0 = the
	// publisher claims a complete book, N = bounded at N levels per side.
	DepthBound *uint32 `json:"depth_bound,omitempty"`

	// Crossed is observability only; a crossed book is still served.
	Crossed bool `json:"crossed"`
}

// ComputeLevels reads the best n levels per side, scaled by the instrument's
// exponents.
//
// Rank is derived here by sorting price keys, never stored: the spec forbids
// keying book state on rank, because a positional key is invalidated by every
// insertion at a better price. Unlike the sibling market-by-order bot there is
// no aggregation step — this feed is already price-aggregated on the wire, so a
// level is a direct read of the map.
func ComputeLevels(inst *Instrument, n int) LevelSnapshot {
	priceScale := math.Pow10(int(inst.PriceExponent))
	qtyScale := math.Pow10(int(inst.QtyExponent))

	snap := LevelSnapshot{
		InstrumentID: inst.ID,
		Symbol:       inst.Symbol,
		DepthBound:   inst.DepthBound,
		Crossed:      inst.Crossed(),
	}

	// Bids rank best-first descending, asks best-first ascending.
	snap.Bids = takeSide(inst.Bids, n, priceScale, qtyScale, func(a, b int64) bool { return a > b })
	snap.Asks = takeSide(inst.Asks, n, priceScale, qtyScale, func(a, b int64) bool { return a < b })
	return snap
}

// takeSide sorts one side's raw price keys with better and returns the best n,
// scaled, with a running cumulative quantity.
func takeSide(book map[int64]*LevelState, n int, priceScale, qtyScale float64, better func(a, b int64) bool) []Level {
	if n <= 0 || len(book) == 0 {
		return nil
	}
	prices := make([]int64, 0, len(book))
	for p := range book {
		prices = append(prices, p)
	}
	sort.Slice(prices, func(i, j int) bool { return better(prices[i], prices[j]) })
	if len(prices) > n {
		prices = prices[:n]
	}

	out := make([]Level, 0, len(prices))
	var cum float64
	for _, p := range prices {
		st := book[p]
		qty := float64(st.QtyRaw) * qtyScale
		cum += qty
		out = append(out, Level{
			Price:         float64(p) * priceScale,
			Qty:           qty,
			OrderCount:    readOrderCount(st.OrderCount),
			CumulativeQty: cum,
		})
	}
	return out
}

// readOrderCount maps the absent sentinel to 0. 0xFFFF means the venue did not
// supply a count (or the true count exceeds 0xFFFE), so surfacing it verbatim
// would read as a real count of 65535. A genuine 0 passes through unchanged.
func readOrderCount(c uint16) uint32 {
	if c == u16Unavailable {
		return 0
	}
	return uint32(c)
}
