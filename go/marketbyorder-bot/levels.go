package main

import (
	"math"
	"sort"
)

// Level is one aggregated price level.
type Level struct {
	Price         float64
	Qty           float64
	OrderCount    uint32
	CumulativeQty float64
}

// LevelSnapshot is the result of aggregating an Instrument's order maps.
type LevelSnapshot struct {
	InstrumentID uint32
	Symbol       string
	Bids         []Level
	Asks         []Level
}

// ComputeLevels aggregates orders by price into top-N levels per side.
// Bids descending (best = highest price); asks ascending (best = lowest).
// Prices are scaled via inst.PriceExponent; quantities via inst.QtyExponent.
func ComputeLevels(inst *Instrument, n int) LevelSnapshot {
	return LevelSnapshot{
		InstrumentID: inst.ID,
		Symbol:       inst.Symbol,
		Bids:         aggregate(inst.Bids, inst.PriceExponent, inst.QtyExponent, n, true),
		Asks:         aggregate(inst.Asks, inst.PriceExponent, inst.QtyExponent, n, false),
	}
}

func aggregate(orders map[uint64]*RestingOrder, priceExp, qtyExp int8, n int, descending bool) []Level {
	if len(orders) == 0 {
		return nil
	}
	type bucket struct {
		PriceRaw int64
		QtyRaw   uint64
		Count    uint32
	}
	byPrice := map[int64]*bucket{}
	for _, o := range orders {
		b, ok := byPrice[o.Price]
		if !ok {
			b = &bucket{PriceRaw: o.Price}
			byPrice[o.Price] = b
		}
		b.QtyRaw += o.Quantity
		b.Count++
	}
	prices := make([]int64, 0, len(byPrice))
	for p := range byPrice {
		prices = append(prices, p)
	}
	if descending {
		sort.Slice(prices, func(i, j int) bool { return prices[i] > prices[j] })
	} else {
		sort.Slice(prices, func(i, j int) bool { return prices[i] < prices[j] })
	}
	if len(prices) > n {
		prices = prices[:n]
	}

	priceScale := math.Pow10(int(priceExp))
	qtyScale := math.Pow10(int(qtyExp))

	out := make([]Level, len(prices))
	var cum float64
	for i, p := range prices {
		b := byPrice[p]
		qty := float64(b.QtyRaw) * qtyScale
		cum += qty
		out[i] = Level{
			Price:         float64(b.PriceRaw) * priceScale,
			Qty:           qty,
			OrderCount:    b.Count,
			CumulativeQty: cum,
		}
	}
	return out
}
