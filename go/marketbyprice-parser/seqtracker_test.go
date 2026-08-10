package main

import (
	"net/netip"
	"testing"
)

// obs is one observation in a test case: which publisher sent it, and its seq.
type obs struct {
	src string
	ch  uint8
	seq uint64
}

func TestSeqTracker(t *testing.T) {
	const a = "10.0.0.1"
	const b = "10.0.0.2"

	tests := []struct {
		name        string
		obs         []obs
		wantGaps    uint64
		wantMissing uint64
	}{
		{
			name:        "no gaps",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 12}, {a, 1, 13}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "one gap",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 15}},
			wantGaps:    1,
			wantMissing: 3,
		},
		{
			name:        "two gaps",
			obs:         []obs{{a, 1, 1}, {a, 1, 2}, {a, 1, 5}, {a, 1, 6}, {a, 1, 10}},
			wantGaps:    2,
			wantMissing: 5,
		},
		{
			name:        "dup/reorder ignored",
			obs:         []obs{{a, 1, 10}, {a, 1, 11}, {a, 1, 10}, {a, 1, 11}, {a, 1, 12}},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name:        "gap then dup",
			obs:         []obs{{a, 1, 1}, {a, 1, 3}, {a, 1, 2}, {a, 1, 4}},
			wantGaps:    1,
			wantMissing: 1,
		},
		{
			name:        "first frame sets baseline",
			obs:         []obs{{a, 1, 100}},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The regression this change exists for. Two publishers interleave on one
		// port with unrelated sequence spaces; a single tracker read that as a
		// storm of gaps.
		{
			name: "interleaved channels are independent",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1001}, {b, 110, 5001},
				{a, 10, 1002}, {b, 110, 5002},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// The case a channel-only key cannot reach: same channel id, two sources.
		{
			name: "same channel id from two sources stays separate",
			obs: []obs{
				{a, 1, 1000}, {b, 1, 7000},
				{a, 1, 1001}, {b, 1, 7001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		{
			name: "same source on two channels stays separate",
			obs: []obs{
				{a, 1, 1000}, {a, 2, 9000},
				{a, 1, 1001}, {a, 2, 9001},
			},
			wantGaps:    0,
			wantMissing: 0,
		},
		// A real gap must still be caught when publishers interleave.
		{
			name: "gap in one publisher counted while the other is clean",
			obs: []obs{
				{a, 10, 1000}, {b, 110, 5000},
				{a, 10, 1004}, {b, 110, 5001},
			},
			wantGaps:    1,
			wantMissing: 3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tracker seqTracker
			var totalGaps, totalMissing uint64
			for _, o := range tc.obs {
				g, m := tracker.observe(netip.MustParseAddr(o.src), o.ch, o.seq)
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
