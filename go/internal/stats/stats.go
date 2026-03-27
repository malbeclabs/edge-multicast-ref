// Package stats tracks shred reception statistics.
package stats

import (
	"sort"
	"time"
)

// SignaturePrefix holds the first 8 bytes of an Ed25519 signature.
type SignaturePrefix [8]byte

// SlotStats tracks statistics for a single slot.
type SlotStats struct {
	Slot              uint64
	DataShredCount    uint64
	CodingShredCount  uint64
	HighestDataIndex  uint32
	FECSetCount       int
	SignaturePrefix   SignaturePrefix
	FirstSeen         time.Time
	LastSeen          time.Time
	fecSetIndices     map[uint32]struct{}
}

// Stats aggregates shred reception statistics across all slots.
// This struct is NOT thread-safe; callers must wrap access with sync.RWMutex.
type Stats struct {
	TotalDataShreds   uint64
	TotalCodingShreds uint64
	TotalHeartbeats   uint64
	ParseErrors       uint64
	LastHeartbeat     *time.Time
	StartTime         time.Time
	Slots             map[uint64]*SlotStats
	maxSlots          int
	rateWindow        []time.Time
}

// NewStats creates a new Stats tracker with the given maximum slot capacity.
func NewStats(maxSlots int) *Stats {
	return &Stats{
		StartTime: time.Now(),
		Slots:     make(map[uint64]*SlotStats),
		maxSlots:  maxSlots,
	}
}

// RecordShred records a received shred into the stats.
func (s *Stats) RecordShred(slot uint64, isData bool, index uint32, fecSetIndex uint32, signature [64]byte) {
	now := time.Now()
	s.rateWindow = append(s.rateWindow, now)

	if isData {
		s.TotalDataShreds++
	} else {
		s.TotalCodingShreds++
	}

	ss, ok := s.Slots[slot]
	if !ok {
		ss = &SlotStats{
			Slot:          slot,
			FirstSeen:     now,
			fecSetIndices: make(map[uint32]struct{}),
		}
		copy(ss.SignaturePrefix[:], signature[:8])
		s.Slots[slot] = ss
	}

	ss.LastSeen = now

	if isData {
		ss.DataShredCount++
		if index > ss.HighestDataIndex {
			ss.HighestDataIndex = index
		}
	} else {
		ss.CodingShredCount++
	}

	if _, exists := ss.fecSetIndices[fecSetIndex]; !exists {
		ss.fecSetIndices[fecSetIndex] = struct{}{}
		ss.FECSetCount = len(ss.fecSetIndices)
	}

	// Evict lowest slot if over capacity.
	if len(s.Slots) > s.maxSlots {
		var minSlot uint64
		first := true
		for k := range s.Slots {
			if first || k < minSlot {
				minSlot = k
				first = false
			}
		}
		delete(s.Slots, minSlot)
	}
}

// RecordHeartbeat records a heartbeat event.
func (s *Stats) RecordHeartbeat() {
	s.TotalHeartbeats++
	now := time.Now()
	s.LastHeartbeat = &now
}

// RecordParseError increments the parse error counter.
func (s *Stats) RecordParseError() {
	s.ParseErrors++
}

// ShredsPerSecond returns the shred reception rate over the last 1 second.
func (s *Stats) ShredsPerSecond() float64 {
	now := time.Now()
	cutoff := now.Add(-1 * time.Second)

	// Prune old entries.
	start := 0
	for start < len(s.rateWindow) && s.rateWindow[start].Before(cutoff) {
		start++
	}
	s.rateWindow = s.rateWindow[start:]

	return float64(len(s.rateWindow))
}

// GetSlot returns the SlotStats for a given slot, or nil if not tracked.
func (s *Stats) GetSlot(slot uint64) *SlotStats {
	return s.Slots[slot]
}

// RecentSlots returns all tracked slots sorted descending by slot number.
func (s *Stats) RecentSlots() []*SlotStats {
	slots := make([]*SlotStats, 0, len(s.Slots))
	for _, ss := range s.Slots {
		slots = append(slots, ss)
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i].Slot > slots[j].Slot
	})
	return slots
}
