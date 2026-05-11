package main

import "testing"

func TestSymbolFilterAllowsOnlyConfiguredInstrumentFlow(t *testing.T) {
	f := NewSymbolFilter("2z")

	if !f.Allow(r("heartbeat", "mktdata", 1, 0, nil)) {
		t.Fatal("heartbeat should pass through")
	}
	if f.Allow(r("order_add", "mktdata", 2, 7, map[string]any{"per_instrument_seq": float64(1)})) {
		t.Fatal("delta before matching refdata should be filtered")
	}
	if f.Allow(r("instrument_definition", "refdata", 3, 7, map[string]any{"symbol": "BTC"})) {
		t.Fatal("non-matching refdata should be filtered")
	}
	if !f.Allow(r("instrument_definition", "refdata", 4, 9, map[string]any{"symbol": "2Z"})) {
		t.Fatal("matching refdata should pass")
	}
	if !f.Allow(r("order_add", "mktdata", 5, 9, map[string]any{"per_instrument_seq": float64(1)})) {
		t.Fatal("matching instrument delta should pass")
	}
	if f.Allow(r("order_add", "mktdata", 6, 7, map[string]any{"per_instrument_seq": float64(1)})) {
		t.Fatal("non-matching instrument delta should be filtered")
	}
}

func TestSymbolFilterKeepsOnlyMatchingSnapshotOrders(t *testing.T) {
	f := NewSymbolFilter("2Z")
	f.Allow(r("instrument_definition", "refdata", 1, 9, map[string]any{"symbol": "2Z"}))

	if f.Allow(r("snapshot_order", "snapshot", 2, 0, map[string]any{"snapshot_id": float64(1)})) {
		t.Fatal("snapshot order without matching begin should be filtered")
	}
	if !f.Allow(r("snapshot_begin", "snapshot", 3, 9, map[string]any{"snapshot_id": float64(1)})) {
		t.Fatal("matching snapshot begin should pass")
	}
	if !f.Allow(r("snapshot_order", "snapshot", 4, 0, map[string]any{"snapshot_id": float64(1)})) {
		t.Fatal("snapshot order for active matching snapshot should pass")
	}
	if !f.Allow(r("snapshot_end", "snapshot", 5, 9, map[string]any{"snapshot_id": float64(1)})) {
		t.Fatal("matching snapshot end should pass")
	}
	if f.Allow(r("snapshot_order", "snapshot", 6, 0, map[string]any{"snapshot_id": float64(1)})) {
		t.Fatal("snapshot order after end should be filtered")
	}
}
