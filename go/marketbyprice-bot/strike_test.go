package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// The strike tests feed the book-builder what the parser's socket does: JSON
// lines, decoded with UseNumber as the read loop decodes them. A Record built by
// hand would carry Go integers in Fields, and what reaches production is
// json.Number and an RFC 3339 string for the fixing time.

func recordFromJSON(t *testing.T, line string) Record {
	t.Helper()
	var rec Record
	dec := json.NewDecoder(bytes.NewReader([]byte(line)))
	dec.UseNumber()
	if err := dec.Decode(&rec); err != nil {
		t.Fatalf("decode %s: %v", line, err)
	}
	return rec
}

// strikeDefRec is an instrument_definition as the parser emits it, from datagram
// seq on the refdata port of channel 0.
func strikeDefRec(t *testing.T, seq uint64, instID uint32, sourceID uint16) Record {
	t.Helper()
	return strikeDefRecAt(t, seq, 0, instID, sourceID)
}

// strikeDefRecAt is strikeDefRec for a definition that was message msgIndex of
// its datagram.
func strikeDefRecAt(t *testing.T, seq uint64, msgIndex uint8, instID uint32, sourceID uint16) Record {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"type": "instrument_definition", "channel_id": 0, "port": "refdata", "seq": seq,
		"msg_index": msgIndex, "instrument_id": instID,
		"fields": map[string]any{
			"symbol": "BTC-ABOVE-92799.99", "source_id": sourceID,
			"price_exponent": -2, "qty_exponent": 0, "manifest_seq": 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return recordFromJSON(t, string(b))
}

// A strike bounded below, inclusive, at 92,700.00, and above, exclusive, at
// 92,799.99: Bound Flags 0b0111, the golden vector's shape.
const fixedStrikeFields = `"strike_exponent":-2,"bound_flags":7,"pending":false,` +
	`"lower_present":true,"lower_inclusive":true,"upper_present":true,"upper_inclusive":false,` +
	`"lower_bound_raw":9270000,"upper_bound_raw":9279999,"fixing_time":"2023-11-14T22:13:20.000000003Z"`

// What the parser emits for a pending strike: no bound and no fixing time.
const pendingStrikeFields = `"strike_exponent":-2,"bound_flags":0,"pending":true,` +
	`"lower_present":false,"lower_inclusive":false,"upper_present":false,"upper_inclusive":false`

// strikeRec is a strike_interval that was message 1 of its datagram: directly
// behind a definition that opened it.
func strikeRec(t *testing.T, seq uint64, instID uint32, sourceID uint16, fields string) Record {
	t.Helper()
	return strikeRecAt(t, seq, 1, instID, sourceID, fields)
}

func strikeRecAt(t *testing.T, seq uint64, msgIndex uint8, instID uint32, sourceID uint16, fields string) Record {
	t.Helper()
	b, err := json.Marshal(map[string]any{
		"type": "strike_interval", "channel_id": 0, "port": "refdata", "seq": seq,
		"msg_index": msgIndex, "instrument_id": instID,
	})
	if err != nil {
		t.Fatal(err)
	}
	line := fmt.Sprintf(`%s,"fields":{"source_id":%d,%s}}`, b[:len(b)-1], sourceID, fields)
	return recordFromJSON(t, line)
}

// strikeHarness runs records through the Coordinator and then through the shard
// each was routed to, in order, on the test's own goroutine.
type strikeHarness struct {
	t      *testing.T
	c      *Coordinator
	shards []*Shard
	m      *Metrics
}

func newStrikeHarness(t *testing.T) *strikeHarness {
	t.Helper()
	m := NewMetrics("test", "test")
	shards := make([]*Shard, 2)
	for i := range shards {
		shards[i] = NewShard(i, len(shards), NewEventsWriter(nil), m)
	}
	return &strikeHarness{
		t: t, shards: shards, m: m,
		c: NewCoordinator(context.Background(), shards, NewEventsWriter(nil), m),
	}
}

func (h *strikeHarness) dispatch(recs ...Record) {
	h.t.Helper()
	for _, rec := range recs {
		h.c.Dispatch(rec)
		for _, s := range h.shards {
			for _, routed := range drain(s) {
				s.apply(routed)
			}
		}
	}
}

func (h *strikeHarness) def(instID uint32) (InstrumentDef, bool) {
	s := h.shards[h.c.shardFor(instID)]
	def, ok := s.refdata[instKey{0, instID}]
	return def, ok
}

func (h *strikeHarness) strike(instID uint32) *StrikeInterval {
	h.t.Helper()
	def, ok := h.def(instID)
	if !ok {
		h.t.Fatalf("no definition held for instrument %d", instID)
	}
	return def.Strike
}

func (h *strikeHarness) ignored(reason string) float64 {
	return counterValue(h.m.StrikeIntervalsIgnoredTotal.WithLabelValues(reason))
}

func (h *strikeHarness) requireNothingIgnored() {
	h.t.Helper()
	for _, reason := range []string{strikeIgnoredNotBehindDefinition, strikeIgnoredSourceID, strikeIgnoredNoDefinition} {
		if got := h.ignored(reason); got != 0 {
			h.t.Errorf("strike_intervals_ignored_total{reason=%q} = %v, want 0", reason, got)
		}
	}
}

var wantFixedStrike = StrikeInterval{
	Exponent: -2, BoundFlags: 0x07,
	LowerBoundRaw: 9270000, UpperBoundRaw: 9279999,
	FixingTime: time.Unix(0, 1700000000000000003).UTC(),
}

func TestStrike_HeldBehindItsDefinition(t *testing.T) {
	h := newStrikeHarness(t)
	h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, fixedStrikeFields))

	got := h.strike(5)
	if got == nil {
		t.Fatal("the strike behind the definition was not held")
	}
	if *got != wantFixedStrike {
		t.Errorf("strike = %+v, want %+v", *got, wantFixedStrike)
	}
	if got.Pending() {
		t.Error("a strike with both bounds present reads as pending")
	}
	h.requireNothingIgnored()
}

// A pending strike is a strike: the instrument's contract pays on one, and the
// venue has not fixed it. It is held, and told apart from no strike at all.
func TestStrike_PendingIsHeldAndReplacedWhenFixed(t *testing.T) {
	h := newStrikeHarness(t)
	h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, pendingStrikeFields))

	got := h.strike(5)
	if got == nil {
		t.Fatal("a pending strike was not held")
	}
	if !got.Pending() || got.LowerBoundRaw != 0 || got.UpperBoundRaw != 0 || !got.FixingTime.IsZero() {
		t.Errorf("pending strike = %+v", *got)
	}

	// The venue fixes it; the publisher sends the pair again, early.
	h.dispatch(strikeDefRec(t, 11, 5, 2), strikeRec(t, 11, 5, 2, fixedStrikeFields))
	if got := h.strike(5); got == nil || *got != wantFixedStrike {
		t.Errorf("after the fix, strike = %+v, want %+v", got, wantFixedStrike)
	}
	h.requireNothingIgnored()
}

// A definition with no StrikeInterval behind it means the contract does not pay
// on a strike, so the one held for the previous transmission goes.
func TestStrike_DefinitionWithoutOneDeletesIt(t *testing.T) {
	h := newStrikeHarness(t)
	h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, fixedStrikeFields))
	if h.strike(5) == nil {
		t.Fatal("setup: strike not held")
	}

	h.dispatch(strikeDefRec(t, 11, 5, 2), strikeDefRec(t, 11, 6, 2))
	if got := h.strike(5); got != nil {
		t.Errorf("strike survived a definition that arrived without one: %+v", *got)
	}
	h.requireNothingIgnored()
}

// Strike State rule 5. Each case leaves instrument 5 holding the strike it had,
// a pending one, and counts the fixed strike it was sent as ignored.
func TestStrike_IgnoredUnlessDirectlyBehindItsOwnDefinition(t *testing.T) {
	heartbeat := func(t *testing.T, seq uint64) Record {
		return recordFromJSON(t, fmt.Sprintf(`{"type":"heartbeat","channel_id":0,"port":"refdata","seq":%d,"msg_index":1}`, seq))
	}
	onPort := func(rec Record, port string) Record { rec.Port = port; return rec }
	onChannel := func(rec Record, ch uint8) Record { rec.ChannelID = ch; return rec }

	cases := []struct {
		name   string
		reason string
		recs   func(t *testing.T) []Record
	}{
		{
			name: "first in its datagram", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeRecAt(t, 20, 0, 5, 2, fixedStrikeFields)}
			},
		},
		{
			// The parser emits no record for a message it skips or drops, so
			// these two records are neighbours on the socket. The strike was
			// message 2 and the definition message 0.
			name: "a message the parser skipped between it and its definition", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), strikeRecAt(t, 20, 2, 5, 2, fixedStrikeFields)}
			},
		},
		{
			// A parser that states no message index states 0 for every record.
			name: "records with no message index", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), strikeRecAt(t, 20, 0, 5, 2, fixedStrikeFields)}
			},
		},
		{
			name: "behind another instrument's definition", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 7, 2), strikeRec(t, 20, 5, 2, fixedStrikeFields)}
			},
		},
		{
			name: "a message between it and its definition", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), heartbeat(t, 20), strikeRecAt(t, 20, 2, 5, 2, fixedStrikeFields)}
			},
		},
		{
			name: "behind another strike", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{
					strikeDefRec(t, 20, 5, 2), strikeRec(t, 20, 5, 2, pendingStrikeFields),
					strikeRecAt(t, 20, 2, 5, 2, fixedStrikeFields),
				}
			},
		},
		{
			name: "its definition ended the datagram before", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), strikeRec(t, 21, 5, 2, fixedStrikeFields)}
			},
		},
		{
			name: "same sequence number on another port", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), onPort(strikeRec(t, 20, 5, 2, fixedStrikeFields), "mktdata")}
			},
		},
		{
			name: "same sequence number on another channel", reason: strikeIgnoredNotBehindDefinition,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), onChannel(strikeRec(t, 20, 5, 2, fixedStrikeFields), 1)}
			},
		},
		{
			name: "a Source ID its definition does not carry", reason: strikeIgnoredSourceID,
			recs: func(t *testing.T) []Record {
				return []Record{strikeDefRec(t, 20, 5, 2), strikeRec(t, 20, 5, 3, fixedStrikeFields)}
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newStrikeHarness(t)
			h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, pendingStrikeFields))
			if got := h.strike(5); got == nil || !got.Pending() {
				t.Fatalf("setup: instrument 5 holds %+v, want a pending strike", got)
			}

			recs := c.recs(t)
			h.dispatch(recs...)

			// Some cases re-advertise instrument 5's definition on the way, which
			// by itself replaces the held strike with whatever follows it. What
			// every case has in common is that the fixed strike is never held.
			if got := h.strike(5); got != nil && !got.Pending() {
				t.Errorf("an ignored strike was held: %+v", *got)
			}
			if got := h.ignored(c.reason); got != 1 {
				t.Errorf("strike_intervals_ignored_total{reason=%q} = %v, want 1", c.reason, got)
			}
			// And it reached no shard on another channel's or instrument's key.
			for i, s := range h.shards {
				for k, def := range s.refdata {
					if def.Strike != nil && !def.Strike.Pending() {
						t.Errorf("shard %d holds a fixed strike for %+v", i, k)
					}
				}
			}
		})
	}
}

// The strike has the definition's lifetime, because it is held inside it.
func TestStrike_GoesWithItsDefinition(t *testing.T) {
	t.Run("manifest prune", func(t *testing.T) {
		h := newStrikeHarness(t)
		h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, fixedStrikeFields))
		// The definition is tagged Manifest Seq 4; 6 is two generations on.
		h.shards[h.c.shardFor(5)].pruneManifest(6)
		if def, ok := h.def(5); ok {
			t.Errorf("definition survived the prune, strike %+v", def.Strike)
		}
	})
	t.Run("channel reset", func(t *testing.T) {
		h := newStrikeHarness(t)
		h.dispatch(strikeDefRec(t, 10, 5, 2), strikeRec(t, 10, 5, 2, fixedStrikeFields))
		h.shards[h.c.shardFor(5)].resetChannel(0)
		if def, ok := h.def(5); ok {
			t.Errorf("definition survived the reset, strike %+v", def.Strike)
		}
	})
}

// A socket drop loses whatever lay between the last record read and the first
// of the new connection, so the definition before it is not the message before
// the strike after it.
func TestStrike_IgnoredAcrossADisconnect(t *testing.T) {
	h := newStrikeHarness(t)
	h.dispatch(strikeDefRec(t, 10, 5, 2))
	h.c.OnDisconnect()
	for _, s := range h.shards {
		drain(s) // the clear-shadows message
	}
	h.dispatch(strikeRec(t, 10, 5, 2, fixedStrikeFields))

	if got := h.strike(5); got != nil {
		t.Errorf("a strike read after a reconnect was held: %+v", *got)
	}
	if got := h.ignored(strikeIgnoredNotBehindDefinition); got != 1 {
		t.Errorf("ignored = %v, want 1", got)
	}
}

// A shard handed a strike for an instrument it holds no definition of has
// nowhere to hold it. The Coordinator's check makes this unreachable from the
// socket; it is counted rather than assumed.
func TestStrike_ShardIgnoresOneWithNoDefinition(t *testing.T) {
	m := NewMetrics("test", "test")
	s := NewShard(0, 1, NewEventsWriter(nil), m)
	if evs := s.apply(strikeRec(t, 10, 5, 2, fixedStrikeFields)); len(evs) != 0 {
		t.Errorf("events: %+v", evs)
	}
	if len(s.refdata) != 0 || len(s.instruments) != 0 {
		t.Errorf("a strike created state: refdata %+v, instruments %+v", s.refdata, s.instruments)
	}
	if got := counterValue(m.StrikeIntervalsIgnoredTotal.WithLabelValues(strikeIgnoredNoDefinition)); got != 1 {
		t.Errorf("ignored = %v, want 1", got)
	}
}

// A strike changes no book and has no table, so it produces no event, and it
// leaves the definition it is held on otherwise as it was.
func TestStrike_ProducesNoEventAndKeepsTheDefinition(t *testing.T) {
	s := NewShard(0, 1, NewEventsWriter(nil), nil)
	s.apply(strikeDefRec(t, 10, 5, 2))
	before := s.refdata[instKey{0, 5}]

	if evs := s.apply(strikeRec(t, 10, 5, 2, fixedStrikeFields)); len(evs) != 0 {
		t.Errorf("events: %+v", evs)
	}
	after := s.refdata[instKey{0, 5}]
	if after.Strike == nil {
		t.Fatal("strike not held")
	}
	after.Strike = nil
	if after != before {
		t.Errorf("definition changed: %+v, was %+v", after, before)
	}
	if before.SourceID != 2 || before.Symbol != "BTC-ABOVE-92799.99" {
		t.Errorf("definition = %+v", before)
	}
}
