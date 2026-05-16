package main

import (
	"errors"
	"testing"
	"time"
)

func TestInstrument_OrderAddAndCancel(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)

	inst.ApplyOrderAdd(1, 0, 0, enter, 82446, 3000)
	inst.ApplyOrderAdd(2, 0, 0, enter, 82420, 1500)
	inst.ApplyOrderAdd(3, 1, 0, enter, 82480, 2000)

	if len(inst.Bids) != 2 || len(inst.Asks) != 1 {
		t.Errorf("counts: bids=%d asks=%d", len(inst.Bids), len(inst.Asks))
	}

	inst.ApplyOrderCancel(2)
	if _, ok := inst.Bids[2]; ok {
		t.Error("expected order 2 cancelled")
	}

	// Cancelling unknown id is a silent no-op.
	inst.ApplyOrderCancel(999)
}

func TestInstrument_OrderExecutePartialAndFull(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)
	inst.ApplyOrderAdd(1, 0, 0, enter, 82446, 1000)

	inst.ApplyOrderExecute(1, 0, 300)
	if inst.Bids[1].Quantity != 700 {
		t.Errorf("partial: got %d want 700", inst.Bids[1].Quantity)
	}

	// Full-fill flag removes regardless of remaining qty.
	inst.ApplyOrderExecute(1, 0x01, 100)
	if _, ok := inst.Bids[1]; ok {
		t.Error("expected order removed after full-fill")
	}
}

func TestInstrument_OrderExecuteToZeroRemoves(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	enter := time.Unix(1700000000, 0)
	inst.ApplyOrderAdd(1, 1, 0, enter, 82480, 500)
	inst.ApplyOrderExecute(1, 0, 500)
	if _, ok := inst.Asks[1]; ok {
		t.Error("expected order removed when qty reaches 0")
	}
}

func TestInstrument_SnapshotReassembly(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	enter := time.Unix(1700000000, 0)

	inst.BeginSnapshot(7, 5000, 3, 100)
	if inst.Status != StatusBuildingSnapshot {
		t.Fatalf("status: got %v", inst.Status)
	}

	inst.AddSnapshotOrder(7, 10, 0, 0, enter, 82446, 3000)
	inst.AddSnapshotOrder(7, 11, 0, 0, enter, 82420, 1500)
	inst.AddSnapshotOrder(7, 12, 1, 0, enter, 82480, 2000)

	anchor, lastInstr, err := inst.EndSnapshot(7, 5000)
	if err != nil {
		t.Fatal(err)
	}
	if anchor != 5000 || lastInstr != 100 {
		t.Errorf("anchor/lastInstr: %d %d", anchor, lastInstr)
	}
	if inst.Status != StatusReady {
		t.Errorf("status: %v", inst.Status)
	}
	if len(inst.Bids) != 2 || len(inst.Asks) != 1 {
		t.Errorf("committed book: bids=%d asks=%d", len(inst.Bids), len(inst.Asks))
	}
	if inst.LastAppliedMktdataSeq != 5000 || inst.LastAppliedInstrumentSeq != 100 {
		t.Errorf("last applied: %d %d", inst.LastAppliedMktdataSeq, inst.LastAppliedInstrumentSeq)
	}
}

func TestInstrument_SnapshotEndMismatchedID(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 2, 100)
	inst.AddSnapshotOrder(7, 10, 0, 0, time.Now(), 82446, 3000)
	_, _, err := inst.EndSnapshot(8, 5000) // wrong snapshot_id
	if !errors.Is(err, errSnapshotMismatch) {
		t.Fatalf("expected errSnapshotMismatch, got %v", err)
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
}

func TestInstrument_SnapshotEndShortCount(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 3, 100)
	inst.AddSnapshotOrder(7, 10, 0, 0, time.Now(), 82446, 3000) // only 1 of 3
	_, _, err := inst.EndSnapshot(7, 5000)
	if !errors.Is(err, errSnapshotShort) {
		t.Fatalf("expected errSnapshotShort, got %v", err)
	}
	if inst.Status != StatusAwaitingSnapshot {
		t.Errorf("status: %v", inst.Status)
	}
}

func TestInstrument_AddSnapshotOrderWrongID(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.BeginSnapshot(7, 5000, 1, 100)
	if inst.AddSnapshotOrder(99, 10, 0, 0, time.Now(), 82446, 3000) {
		t.Error("expected false for mismatched snapshot_id")
	}
}

func TestInstrument_Reset(t *testing.T) {
	inst := NewInstrument(100, "BTC-USDT", -2, -8)
	inst.Status = StatusReady
	inst.ApplyOrderAdd(1, 0, 0, time.Now(), 82446, 3000)
	inst.LastAppliedMktdataSeq = 5000
	inst.LastAppliedInstrumentSeq = 100

	inst.Reset()
	if inst.Status != StatusAwaitingSnapshot || len(inst.Bids) != 0 || len(inst.Asks) != 0 {
		t.Errorf("post-reset: %+v", inst)
	}
	if inst.LastAppliedMktdataSeq != 0 || inst.LastAppliedInstrumentSeq != 0 {
		t.Error("seq trackers not reset")
	}
	if inst.OpenSnapshot != nil {
		t.Error("OpenSnapshot not cleared by Reset")
	}
}
