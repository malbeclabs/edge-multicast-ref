package main

import (
	"testing"
	"time"
)

// helper to build records concisely for tests
func r(rt string, port string, seq uint64, instID uint32, fields map[string]any) Record {
	return Record{
		Type:           rt,
		Timestamp:      time.Unix(1700000000, 0),
		ChannelID:      0,
		Port:           port,
		SequenceNumber: seq,
		ResetCount:     1,
		InstrumentID:   instID,
		Fields:         fields,
	}
}

func TestChannel_ColdStart(t *testing.T) {
	c := NewChannelState(0)

	// 1. InstrumentDefinition
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol":         "BTC-USDT",
		"price_exponent": float64(-2),
		"qty_exponent":   float64(-8),
	}))
	if _, ok := c.Refdata[100]; !ok {
		t.Fatal("refdata not stored")
	}

	// 2. Mktdata delta arrives before snapshot — should buffer.
	c.Apply(r("order_add", "mktdata", 50, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(101),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if len(c.DeltaBuffer) != 1 {
		t.Fatalf("expected 1 buffered delta, got %d", len(c.DeltaBuffer))
	}

	// 3. SnapshotBegin/Order/End with anchor=49 (so the buffered delta is post-anchor).
	c.Apply(r("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(49), "total_orders": float64(0),
		"snapshot_id": float64(7), "last_instrument_seq": float64(100),
	}))
	c.Apply(r("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(49), "snapshot_id": float64(7),
	}))

	inst := c.Instruments[100]
	if inst.Status != StatusReady {
		t.Fatalf("status: %v", inst.Status)
	}
	if len(inst.Bids) != 1 {
		t.Errorf("expected buffered delta replayed: bids=%d", len(inst.Bids))
	}
	if inst.LastAppliedInstrumentSeq != 101 {
		t.Errorf("last applied instrument seq: %d", inst.LastAppliedInstrumentSeq)
	}
}

func TestChannel_PerInstrumentGap(t *testing.T) {
	c := NewChannelState(0)
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	c.Apply(r("snapshot_begin", "snapshot", 1, 100, map[string]any{
		"anchor_seq": float64(0), "total_orders": float64(0),
		"snapshot_id": float64(1), "last_instrument_seq": float64(0),
	}))
	c.Apply(r("snapshot_end", "snapshot", 2, 100, map[string]any{
		"anchor_seq": float64(0), "snapshot_id": float64(1),
	}))

	inst := c.Instruments[100]
	inst.LastAppliedInstrumentSeq = 0

	// Apply seq=1 — should succeed.
	c.Apply(r("order_add", "mktdata", 100, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(1),
		"order_id": float64(1), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82446), "qty_raw": float64(3000),
	}))
	if inst.Status != StatusReady {
		t.Fatalf("after seq=1 status: %v", inst.Status)
	}

	// Apply seq=3 — gap.
	evs := c.Apply(r("order_add", "mktdata", 102, 100, map[string]any{
		"side": "bid", "order_flags": float64(0), "per_instrument_seq": float64(3),
		"order_id": float64(2), "enter_ts": time.Unix(1700000000, 0).Format(time.RFC3339Nano),
		"price_raw": float64(82440), "qty_raw": float64(2000),
	}))
	if inst.Status != StatusGap {
		t.Errorf("expected status gap, got %v", inst.Status)
	}
	if len(evs) != 1 || evs[0].Kind != "per_instrument_gap" {
		t.Errorf("expected per_instrument_gap event, got %+v", evs)
	}
}

func TestChannel_ChannelReset(t *testing.T) {
	c := NewChannelState(0)
	c.Apply(r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	}))
	if c.ResetCount != 1 {
		t.Fatalf("reset_count: %d", c.ResetCount)
	}

	// Now a record arrives with reset_count=2.
	rec := r("instrument_definition", "refdata", 1, 100, map[string]any{
		"symbol": "BTC-USDT", "price_exponent": float64(-2), "qty_exponent": float64(-8),
	})
	rec.ResetCount = 2
	evs := c.Apply(rec)

	found := false
	for _, e := range evs {
		if e.Kind == "channel_reset" {
			found = true
		}
	}
	if !found {
		t.Error("expected channel_reset event")
	}
	if c.ResetCount != 2 {
		t.Errorf("post-reset: %d", c.ResetCount)
	}
}
