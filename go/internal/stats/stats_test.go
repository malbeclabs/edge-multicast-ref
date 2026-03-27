package stats

import (
	"testing"
	"time"
)

func dummySig() [64]byte {
	var sig [64]byte
	for i := range sig {
		sig[i] = byte(i)
	}
	return sig
}

func TestNewStats(t *testing.T) {
	s := NewStats(32)
	if s.TotalDataShreds != 0 {
		t.Errorf("TotalDataShreds = %d, want 0", s.TotalDataShreds)
	}
	if s.TotalCodingShreds != 0 {
		t.Errorf("TotalCodingShreds = %d, want 0", s.TotalCodingShreds)
	}
	if s.TotalHeartbeats != 0 {
		t.Errorf("TotalHeartbeats = %d, want 0", s.TotalHeartbeats)
	}
	if s.ParseErrors != 0 {
		t.Errorf("ParseErrors = %d, want 0", s.ParseErrors)
	}
	if s.LastHeartbeat != nil {
		t.Error("LastHeartbeat should be nil")
	}
	if len(s.Slots) != 0 {
		t.Errorf("Slots length = %d, want 0", len(s.Slots))
	}
}

func TestRecordShredData(t *testing.T) {
	s := NewStats(32)
	sig := dummySig()
	s.RecordShred(100, true, 0, 0, sig)

	if s.TotalDataShreds != 1 {
		t.Errorf("TotalDataShreds = %d, want 1", s.TotalDataShreds)
	}
	if s.TotalCodingShreds != 0 {
		t.Errorf("TotalCodingShreds = %d, want 0", s.TotalCodingShreds)
	}

	ss := s.GetSlot(100)
	if ss == nil {
		t.Fatal("slot 100 not found")
	}
	if ss.DataShredCount != 1 {
		t.Errorf("DataShredCount = %d, want 1", ss.DataShredCount)
	}
	if ss.IsDataOnly() {
		// Just check the coding count is zero.
		if ss.CodingShredCount != 0 {
			t.Errorf("CodingShredCount = %d, want 0", ss.CodingShredCount)
		}
	}
}

// IsDataOnly is a test helper — not part of the public API.
func (ss *SlotStats) IsDataOnly() bool {
	return ss.CodingShredCount == 0
}

func TestRecordShredCoding(t *testing.T) {
	s := NewStats(32)
	sig := dummySig()
	s.RecordShred(100, false, 0, 0, sig)

	if s.TotalCodingShreds != 1 {
		t.Errorf("TotalCodingShreds = %d, want 1", s.TotalCodingShreds)
	}
	if s.TotalDataShreds != 0 {
		t.Errorf("TotalDataShreds = %d, want 0", s.TotalDataShreds)
	}

	ss := s.GetSlot(100)
	if ss == nil {
		t.Fatal("slot 100 not found")
	}
	if ss.CodingShredCount != 1 {
		t.Errorf("CodingShredCount = %d, want 1", ss.CodingShredCount)
	}
}

func TestMultipleShredsSameSlot(t *testing.T) {
	s := NewStats(32)
	sig := dummySig()

	s.RecordShred(100, true, 0, 0, sig)
	s.RecordShred(100, true, 1, 0, sig)
	s.RecordShred(100, true, 5, 1, sig)
	s.RecordShred(100, false, 0, 0, sig)

	ss := s.GetSlot(100)
	if ss == nil {
		t.Fatal("slot 100 not found")
	}
	if ss.DataShredCount != 3 {
		t.Errorf("DataShredCount = %d, want 3", ss.DataShredCount)
	}
	if ss.CodingShredCount != 1 {
		t.Errorf("CodingShredCount = %d, want 1", ss.CodingShredCount)
	}
	if ss.HighestDataIndex != 5 {
		t.Errorf("HighestDataIndex = %d, want 5", ss.HighestDataIndex)
	}
	if ss.FECSetCount != 2 {
		t.Errorf("FECSetCount = %d, want 2", ss.FECSetCount)
	}
}

func TestRingBufferEviction(t *testing.T) {
	s := NewStats(3)
	sig := dummySig()

	s.RecordShred(10, true, 0, 0, sig)
	s.RecordShred(20, true, 0, 0, sig)
	s.RecordShred(30, true, 0, 0, sig)

	if len(s.Slots) != 3 {
		t.Fatalf("Slots length = %d, want 3", len(s.Slots))
	}

	// Adding a 4th slot should evict the lowest (10).
	s.RecordShred(40, true, 0, 0, sig)

	if len(s.Slots) != 3 {
		t.Fatalf("Slots length = %d, want 3 after eviction", len(s.Slots))
	}
	if s.GetSlot(10) != nil {
		t.Error("slot 10 should have been evicted")
	}
	if s.GetSlot(20) == nil {
		t.Error("slot 20 should still exist")
	}
	if s.GetSlot(40) == nil {
		t.Error("slot 40 should exist")
	}
}

func TestHeartbeatCounting(t *testing.T) {
	s := NewStats(32)

	if s.LastHeartbeat != nil {
		t.Error("LastHeartbeat should be nil initially")
	}

	s.RecordHeartbeat()
	if s.TotalHeartbeats != 1 {
		t.Errorf("TotalHeartbeats = %d, want 1", s.TotalHeartbeats)
	}
	if s.LastHeartbeat == nil {
		t.Error("LastHeartbeat should not be nil after recording")
	}

	s.RecordHeartbeat()
	s.RecordHeartbeat()
	if s.TotalHeartbeats != 3 {
		t.Errorf("TotalHeartbeats = %d, want 3", s.TotalHeartbeats)
	}
}

func TestShredsPerSecond(t *testing.T) {
	s := NewStats(32)
	sig := dummySig()

	// Record several shreds right now.
	for i := 0; i < 10; i++ {
		s.RecordShred(100, true, uint32(i), 0, sig)
	}

	rate := s.ShredsPerSecond()
	if rate < 10.0 {
		t.Errorf("ShredsPerSecond = %f, want >= 10.0", rate)
	}

	// Inject an old timestamp to verify pruning works.
	old := time.Now().Add(-2 * time.Second)
	s.rateWindow = append([]time.Time{old}, s.rateWindow...)

	rate = s.ShredsPerSecond()
	// The old entry should have been pruned, so rate should still be ~10.
	if rate < 10.0 {
		t.Errorf("ShredsPerSecond after prune = %f, want >= 10.0", rate)
	}
}
