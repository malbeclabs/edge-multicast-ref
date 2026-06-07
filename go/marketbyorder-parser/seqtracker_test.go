package main

import "testing"

func TestSeqTracker(t *testing.T) {
	tests := []struct {
		name        string
		seqs        []uint64
		wantGaps    uint64 // total gap events
		wantMissing uint64 // total missing frames
	}{
		{
			name:        "no gaps",
			seqs:        []uint64{10, 11, 12, 13},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "one gap",
			seqs:        []uint64{10, 11, 15}, // gap of 3 between 11 and 15
			wantGaps:    1,
			wantMissing: 3,
		},
		{
			name:        "two gaps",
			seqs:        []uint64{1, 2, 5, 6, 10}, // gaps: 5-2=2 missing, 10-6=3 missing
			wantGaps:    2,
			wantMissing: 5,
		},
		{
			name:        "dup/reorder ignored",
			seqs:        []uint64{10, 11, 10, 11, 12}, // 10 and 11 repeated
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "gap then dup",
			seqs:        []uint64{1, 3, 2, 4}, // gap at 3 (+1 missing), then 2 is dup/reorder (ignored)
			wantGaps:    1,
			wantMissing: 1,
		},
		{
			name:        "first frame sets baseline",
			seqs:        []uint64{100},
			wantGaps:    0,
			wantMissing: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker seqTracker
			var totalGaps, totalMissing uint64
			for _, seq := range tc.seqs {
				g, m := tracker.observe(seq)
				totalGaps += g
				totalMissing += m
			}
			if totalGaps != tc.wantGaps {
				t.Errorf("gaps: got %d, want %d", totalGaps, tc.wantGaps)
			}
			if totalMissing != tc.wantMissing {
				t.Errorf("missing: got %d, want %d", totalMissing, tc.wantMissing)
			}
		})
	}
}
