package main

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// genStream produces a deterministic multi-instrument stream: define, snapshot
// (empty), then ordered deltas per instrument.
func genStream(instruments int, deltasPer int, seed int64) []Record {
	rng := rand.New(rand.NewSource(seed))
	var recs []Record
	seq := uint64(1)
	for id := uint32(1); id <= uint32(instruments); id++ {
		recs = append(recs, Record{Type: "instrument_definition", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"symbol": fmt.Sprintf("S%d", id), "price_exponent": float64(-2), "qty_exponent": float64(-8)}})
		seq++
		recs = append(recs, Record{Type: "snapshot_begin", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"anchor_seq": float64(0), "total_orders": float64(0), "snapshot_id": float64(id), "last_instrument_seq": float64(0)}})
		seq++
		recs = append(recs, Record{Type: "snapshot_end", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"anchor_seq": float64(0), "snapshot_id": float64(id)}})
		seq++
	}
	piSeq := map[uint32]uint32{}
	order := make([]uint32, 0, instruments*deltasPer)
	for id := uint32(1); id <= uint32(instruments); id++ {
		for k := 0; k < deltasPer; k++ {
			order = append(order, id)
		}
	}
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	oid := uint64(1)
	for _, id := range order {
		piSeq[id]++
		recs = append(recs, Record{Type: "order_add", ChannelID: 0, InstrumentID: id, ResetCount: 1,
			SequenceNumber: seq, Timestamp: time.Unix(1700000000, 0),
			Fields: map[string]any{"side": "bid", "order_flags": float64(0),
				"per_instrument_seq": float64(piSeq[id]), "order_id": float64(oid),
				"enter_ts":  time.Unix(1700000000, 0).Format(time.RFC3339Nano),
				"price_raw": float64(100 + oid), "qty_raw": float64(10)}})
		seq++
		oid++
	}
	return recs
}

// instFP is the per-instrument terminal-state fingerprint compared across
// shard counts. Divergence here means per-instrument ordering was not preserved.
type instFP struct {
	Status     InstrumentStatus
	Bids, Asks int
	LastPiSeq  uint32
	LastMktSeq uint64
}

// runSharded feeds recs through a Coordinator over n shards, drains via a
// fence, and returns each instrument's terminal fingerprint plus the metrics
// (so callers can assert e.g. no spurious per-instrument gaps).
func runSharded(t *testing.T, n int, recs []Record) (map[uint32]instFP, *Metrics) {
	t.Helper()
	metrics := stubMetrics()
	shards := make([]*Shard, n)
	for i := 0; i < n; i++ {
		s := NewShard(i, n, NewEventsWriter(nil), nil, metrics)
		s.sw = NewSnapshotWriter(nil, 5, 50, metrics, 0, func(s *Shard) func(uint32, func(*Instrument)) {
			return func(id uint32, fn func(*Instrument)) {
				s.mu.Lock()
				defer s.mu.Unlock()
				fn(s.instruments[instKey{0, id}])
			}
		}(s))
		shards[i] = s
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := NewCoordinator(ctx, shards, NewEventsWriter(nil), metrics)
	defer cancel()
	for _, s := range shards {
		go s.sw.Run(ctx)
		go s.Run(ctx)
	}
	for _, r := range recs {
		c.Dispatch(r)
	}
	// Drain via a fence so all shard inboxes are empty before we read state.
	c.runFence(Record{Type: "end_of_session", ChannelID: 0, ResetCount: 1, Timestamp: time.Unix(1700000000, 0), Fields: map[string]any{}})

	out := map[uint32]instFP{}
	for _, s := range shards {
		s.mu.Lock()
		for k, inst := range s.instruments {
			out[k.id] = instFP{
				Status:     inst.Status,
				Bids:       len(inst.Bids),
				Asks:       len(inst.Asks),
				LastPiSeq:  inst.LastAppliedInstrumentSeq,
				LastMktSeq: inst.LastAppliedMktdataSeq,
			}
		}
		s.mu.Unlock()
	}
	return out, metrics
}

func TestParity_ShardedMatchesSingleShard(t *testing.T) {
	recs := genStream(50, 20, 12345)
	base, _ := runSharded(t, 1, recs)
	for _, n := range []int{2, 4, 8} {
		got, _ := runSharded(t, n, recs)
		if reflect.DeepEqual(base, got) {
			continue
		}
		ids := make([]int, 0, len(base))
		for id := range base {
			ids = append(ids, int(id))
		}
		sort.Ints(ids)
		for _, id := range ids {
			if base[uint32(id)] != got[uint32(id)] {
				t.Fatalf("n=%d instrument %d: baseline=%+v sharded=%+v",
					n, id, base[uint32(id)], got[uint32(id)])
			}
		}
		t.Fatalf("n=%d: fingerprint maps differ in key set", n)
	}
}

func counterVal(t *testing.T, c interface{ Write(*dto.Metric) error }) float64 {
	t.Helper()
	var m dto.Metric
	if err := c.Write(&m); err != nil {
		t.Fatalf("counter write: %v", err)
	}
	return m.GetCounter().GetValue()
}

func TestAcceptance_ThroughputSoakAndParity(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}
	recs := genStream(330, 50, 99)

	base, _ := runSharded(t, 1, recs)

	for _, n := range []int{1, 4} {
		start := time.Now()
		got, m := runSharded(t, n, recs)
		elapsed := time.Since(start)

		// (1) No spurious per-instrument gaps — the stream is contiguous.
		if g := counterVal(t, m.PerInstrumentGapsTotal); g != 0 {
			t.Fatalf("n=%d: per_instrument_gaps_total=%v (want 0) — sharding reordered an instrument", n, g)
		}
		// (2) Per-instrument terminal parity vs single-shard baseline.
		if !reflect.DeepEqual(base, got) {
			t.Fatalf("n=%d: parity mismatch vs single-shard baseline", n)
		}
		// (3) Completion without deadlock/backlog. ~330*50 deltas + setup
		// (~18k records); 30s is intentionally generous for CI.
		if elapsed > 30*time.Second {
			t.Fatalf("n=%d: processing took %v (>30s) — backlog/deadlock suspected", n, elapsed)
		}
		t.Logf("n=%d processed %d records in %v, gaps=0, parity ok", n, len(recs), elapsed)
	}
}
