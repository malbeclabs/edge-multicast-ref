package tob

import (
	"encoding/binary"
	"testing"
)

// buildStrikeIntervalMsg constructs a full 40-byte StrikeInterval message. The
// four reserved bytes carry a non-zero pattern: a publisher zeroes them and a
// reader does not look.
func buildStrikeIntervalMsg(instID uint32, sourceID uint16, exp int8, flags uint8, lower, upper int64, fixingNS uint64) []byte {
	buf := make([]byte, 40)
	buf[0] = msgStrikeInterval
	buf[1] = 40
	binary.LittleEndian.PutUint32(buf[4:8], instID)
	binary.LittleEndian.PutUint16(buf[8:10], sourceID)
	buf[10] = byte(exp)
	buf[11] = flags
	binary.LittleEndian.PutUint64(buf[12:20], uint64(lower))
	binary.LittleEndian.PutUint64(buf[20:28], uint64(upper))
	binary.LittleEndian.PutUint64(buf[28:36], fixingNS)
	copy(buf[36:40], []byte{0xDE, 0xAD, 0xBE, 0xEF})
	return buf
}

// buildInstDefBodyV3 gives every definition this Source ID.
const strikeTestSourceID = 77

func strikeTestTS() uint64 { return 1700000000000000000 }

func parseOne(t *testing.T, p *TopOfBookParser, datagram []byte) []Record {
	t.Helper()
	recs, err := p.Parse(datagram, PacketMeta{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	return recs
}

func recordTypes(recs []Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.Type
	}
	return out
}

func TestDecodeStrikeInterval(t *testing.T) {
	// A negative lower bound and a negative exponent, so a decoder reading
	// either as unsigned fails; distinct bounds, so one exchanging them does.
	msg := buildStrikeIntervalMsg(77, 5, -3, 0x0D, -1500, 2500, 1700000000000000009)
	b, err := decodeTopOfBookBody(msgStrikeInterval, msg[messageHeaderSize:], schemaVersionV3)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := b.(*topOfBookStrikeInterval)
	if !ok {
		t.Fatalf("decoded %T", b)
	}
	want := topOfBookStrikeInterval{
		InstrumentID: 77, SourceID: 5, StrikeExponent: -3, BoundFlags: 0x0D,
		LowerBound: -1500, UpperBound: 2500, FixingTime: 1700000000000000009,
	}
	if *got != want {
		t.Errorf("got %+v, want %+v", *got, want)
	}
}

func TestParse_StrikeIntervalRecord(t *testing.T) {
	p := NewTopOfBookParser()
	recs := parseOne(t, p, buildDatagramWithVersion(3, 1, 100, strikeTestTS(),
		buildInstrumentDefMsgV3(42, "BTC-ABOVE-92700", "BTC", "USD"),
		buildStrikeIntervalMsg(42, strikeTestSourceID, -2, 0x07, 9270000, 9279999, 1700000000000000003)))

	if got := recordTypes(recs); len(got) != 2 || got[0] != "instrument_definition" || got[1] != "strike_interval" {
		t.Fatalf("records: %v", got)
	}
	rec := recs[1]
	if rec.InstrumentID != 42 || rec.Symbol != "BTC-ABOVE-92700" {
		t.Errorf("instrument %d %q", rec.InstrumentID, rec.Symbol)
	}
	if rec.ChannelID != 1 || rec.SequenceNumber != 100 {
		t.Errorf("channel %d seq %d", rec.ChannelID, rec.SequenceNumber)
	}
	want := map[string]any{
		"source_id": uint16(strikeTestSourceID), "strike_exponent": int8(-2), "bound_flags": uint8(0x07),
		"pending": false, "lower_present": true, "lower_inclusive": true,
		"upper_present": true, "upper_inclusive": false,
		"lower_bound": int64(9270000), "upper_bound": int64(9279999),
		"fixing_time_ns": uint64(1700000000000000003),
	}
	if len(rec.Fields) != len(want) {
		t.Errorf("fields: got %v", rec.Fields)
	}
	for k, w := range want {
		if got := rec.Fields[k]; got != w {
			t.Errorf("%s: got %v (%T) want %v (%T)", k, got, got, w, w)
		}
	}
}

// A pending strike states no bound and no time, so its record carries none: a 0
// there would read as a strike of zero fixed at the Unix epoch. The same holds for
// the one absent side of a strike bounded on the other. A subscriber ignores
// Bound Flags bits 4-7, so they change none of this and refuse nothing.
func TestParse_StrikeIntervalOmitsWhatIsNotStated(t *testing.T) {
	cases := []struct {
		name        string
		flags       uint8
		fixingNS    uint64
		wantPending bool
		wantKeys    []string // of lower_bound, upper_bound, fixing_time_ns
	}{
		{"pending", 0x00, 0, true, nil},
		{"pending with reserved bits set", 0xF0, 0, true, nil},
		{"lower only, fixed at listing", 0x03, 0, false, []string{"lower_bound"}},
		{"upper only", 0x04, 1700000000000000009, false, []string{"upper_bound", "fixing_time_ns"}},
		{"both, with reserved bits set", 0xFF, 0, false, []string{"lower_bound", "upper_bound"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var lower, upper int64
			if c.flags&boundLowerPresent != 0 {
				lower = 100
			}
			if c.flags&boundUpperPresent != 0 {
				upper = 200
			}
			p := NewTopOfBookParser()
			recs := parseOne(t, p, buildDatagramWithVersion(3, 1, 100, strikeTestTS(),
				buildInstrumentDefMsgV3(42, "X", "BTC", "USD"),
				buildStrikeIntervalMsg(42, strikeTestSourceID, 0, c.flags, lower, upper, c.fixingNS)))
			if len(recs) != 2 || recs[1].Type != "strike_interval" {
				t.Fatalf("records: %v", recordTypes(recs))
			}
			rec := recs[1]
			if got := rec.Fields["pending"]; got != c.wantPending {
				t.Errorf("pending: got %v want %v", got, c.wantPending)
			}
			if got := rec.Fields["bound_flags"]; got != c.flags {
				t.Errorf("bound_flags: got %v, want the byte as it arrived, 0x%02x", got, c.flags)
			}
			want := map[string]bool{}
			for _, k := range c.wantKeys {
				want[k] = true
			}
			for _, k := range []string{"lower_bound", "upper_bound", "fixing_time_ns"} {
				if _, present := rec.Fields[k]; present != want[k] {
					t.Errorf("%s present = %v, want %v", k, present, want[k])
				}
			}
			if held := p.instruments[42].Strike; held == nil || held.pending() != c.wantPending {
				t.Errorf("held strike = %+v", held)
			}
		})
	}
}

// Strike State rule 5. Each case first gives instrument 42 a pending strike in
// one datagram, then sends a fixed one in a position the rule excludes: it
// yields no record, it is counted, and the fixed strike is never held.
func TestParse_StrikeIntervalIgnoredUnlessDirectlyBehindItsOwnDefinition(t *testing.T) {
	def := func(id uint32) []byte { return buildInstrumentDefMsgV3(id, "X", "BTC", "USD") }
	pending := func(id uint32) []byte { return buildStrikeIntervalMsg(id, strikeTestSourceID, -2, 0, 0, 0, 0) }
	fixed := func(id uint32, sourceID uint16) []byte {
		return buildStrikeIntervalMsg(id, sourceID, -2, 0x07, 9270000, 9279999, 0)
	}
	unknown := []byte{0x7E, 8, 0, 0, 1, 2, 3, 4} // a type this decoder skips by its length

	cases := []struct {
		name      string
		datagrams [][][]byte
		wantTypes []string // of the records the case's datagrams yield
	}{
		{
			name:      "first in its datagram",
			datagrams: [][][]byte{{fixed(42, strikeTestSourceID)}},
		},
		{
			name:      "behind another instrument's definition",
			datagrams: [][][]byte{{def(43), fixed(42, strikeTestSourceID)}},
			wantTypes: []string{"instrument_definition"},
		},
		{
			name:      "a heartbeat between it and its definition",
			datagrams: [][][]byte{{def(42), buildHeartbeat(1, strikeTestTS()), fixed(42, strikeTestSourceID)}},
			wantTypes: []string{"instrument_definition", "heartbeat"},
		},
		{
			name:      "a skipped type between it and its definition",
			datagrams: [][][]byte{{def(42), unknown, fixed(42, strikeTestSourceID)}},
			wantTypes: []string{"instrument_definition"},
		},
		{
			name:      "behind another strike",
			datagrams: [][][]byte{{def(42), pending(42), fixed(42, strikeTestSourceID)}},
			wantTypes: []string{"instrument_definition", "strike_interval"},
		},
		{
			name:      "its definition ended the datagram before",
			datagrams: [][][]byte{{def(42)}, {fixed(42, strikeTestSourceID)}},
			wantTypes: []string{"instrument_definition"},
		},
		{
			name:      "a Source ID its definition does not carry",
			datagrams: [][][]byte{{def(42), fixed(42, strikeTestSourceID+1)}},
			wantTypes: []string{"instrument_definition"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := NewTopOfBookParser()
			parseOne(t, p, buildDatagramWithVersion(3, 1, 100, strikeTestTS(), def(42), pending(42)))
			if held := p.instruments[42].Strike; held == nil || !held.pending() {
				t.Fatalf("setup: instrument 42 holds %+v, want a pending strike", held)
			}

			var got []string
			for i, msgs := range c.datagrams {
				got = append(got, recordTypes(parseOne(t, p, buildDatagramWithVersion(3, 1, uint64(101+i), strikeTestTS(), msgs...)))...)
			}
			if len(got) != len(c.wantTypes) {
				t.Fatalf("records: got %v want %v", got, c.wantTypes)
			}
			for i := range got {
				if got[i] != c.wantTypes[i] {
					t.Fatalf("records: got %v want %v", got, c.wantTypes)
				}
			}
			if p.strikesIgnored != 1 {
				t.Errorf("strikesIgnored = %d, want 1", p.strikesIgnored)
			}
			// Some cases re-advertise instrument 42's definition on the way,
			// which by itself replaces the held strike with whatever follows
			// it. What every case has in common is that the fixed strike is
			// held for no instrument.
			for id, info := range p.instruments {
				if info.Strike != nil && !info.Strike.pending() {
					t.Errorf("instrument %d holds the ignored strike: %+v", id, *info.Strike)
				}
			}
		})
	}
}

// A schema 1 InstrumentDefinition carries no Source ID, and the rule compares
// it only where the definition carries one.
func TestParse_StrikeIntervalBehindASchema1DefinitionIsHeldWhateverItsSourceID(t *testing.T) {
	p := NewTopOfBookParser()
	recs := parseOne(t, p, buildDatagramWithVersion(1, 1, 100, strikeTestTS(),
		buildInstrumentDefMsgV1(42, "X", "BTC", "USD"),
		buildStrikeIntervalMsg(42, 9, -2, 0x01, 500, 0, 0)))
	if got := recordTypes(recs); len(got) != 2 || got[1] != "strike_interval" {
		t.Fatalf("records: %v", got)
	}
	if p.strikesIgnored != 0 || p.instruments[42].Strike == nil {
		t.Errorf("strikesIgnored = %d, held = %+v", p.strikesIgnored, p.instruments[42].Strike)
	}
}

// The strike has the definition's lifetime: the last one sent is current, a
// definition that arrives without one deletes it, and a channel reset clears
// it with the definitions.
func TestParse_StrikeIntervalLifetime(t *testing.T) {
	def := buildInstrumentDefMsgV3(42, "X", "BTC", "USD")
	p := NewTopOfBookParser()

	parseOne(t, p, buildDatagramWithVersion(3, 1, 100, strikeTestTS(), def,
		buildStrikeIntervalMsg(42, strikeTestSourceID, -2, 0, 0, 0, 0)))
	if held := p.instruments[42].Strike; held == nil || !held.pending() {
		t.Fatalf("pending strike not held: %+v", held)
	}

	// The venue fixes it; the publisher sends the pair again.
	parseOne(t, p, buildDatagramWithVersion(3, 1, 101, strikeTestTS(), def,
		buildStrikeIntervalMsg(42, strikeTestSourceID, -2, 0x01, 9270000, 0, 5)))
	if held := p.instruments[42].Strike; held == nil || held.pending() || held.LowerBound != 9270000 {
		t.Fatalf("fixed strike did not replace the pending one: %+v", held)
	}

	// A definition with no strike behind it: the contract does not pay on one.
	parseOne(t, p, buildDatagramWithVersion(3, 1, 102, strikeTestTS(), def, buildInstrumentDefMsgV3(43, "Y", "BTC", "USD")))
	if held := p.instruments[42].Strike; held != nil {
		t.Errorf("strike survived a definition that arrived without one: %+v", *held)
	}

	parseOne(t, p, buildDatagramWithVersion(3, 1, 103, strikeTestTS(), def,
		buildStrikeIntervalMsg(42, strikeTestSourceID, -2, 0x01, 9270000, 0, 5)))
	parseOne(t, p, buildDatagramWithVersion(3, 1, 104, strikeTestTS(), buildChannelReset(strikeTestTS())))
	if len(p.instruments) != 0 {
		t.Errorf("channel reset left %d definitions, and their strikes", len(p.instruments))
	}
	if p.strikesIgnored != 0 {
		t.Errorf("strikesIgnored = %d, want 0", p.strikesIgnored)
	}
}

// The refdata and mktdata ports are read by two goroutines sharing one parser,
// the lock is taken per message, and a ChannelReset travels either port. So a
// reset can clear a definition after it was stored and before the strike
// directly behind it is handled. The strike is then held nowhere, and it yields
// no record: a record would state a strike for an instrument the parser has
// just told its consumer to forget.
//
// The interleaving is driven by hand, at the one point it can happen: between
// the two messages of the datagram.
func TestStrikeInterval_NoRecordWhenItsDefinitionIsNoLongerHeld(t *testing.T) {
	strike := &topOfBookStrikeInterval{InstrumentID: 42, SourceID: strikeTestSourceID, BoundFlags: 0x01, LowerBound: 500}
	storeDef := func(p *TopOfBookParser) *topOfBookInstrumentDef {
		def := &topOfBookInstrumentDef{InstrumentID: 42, SourceID: strikeTestSourceID, Symbol: "X"}
		p.handleInstrumentDef(1, 100, 0, strikeTestTS(), PacketMeta{}, nil, def)
		return def
	}
	handle := func(p *TopOfBookParser, prevDef *topOfBookInstrumentDef) []Record {
		return p.handleStrikeInterval(1, 100, 0, strikeTestTS(), PacketMeta{}, strike, prevDef, schemaVersionV3)
	}

	t.Run("held and emitted when nothing intervenes", func(t *testing.T) {
		p := NewTopOfBookParser()
		def := storeDef(p)
		if recs := handle(p, def); len(recs) != 1 || recs[0].Type != "strike_interval" {
			t.Fatalf("records: %+v", recs)
		}
		if p.instruments[42].Strike == nil || p.strikesIgnored != 0 {
			t.Errorf("held = %+v, strikesIgnored = %d", p.instruments[42].Strike, p.strikesIgnored)
		}
	})

	t.Run("a channel reset cleared the definition", func(t *testing.T) {
		p := NewTopOfBookParser()
		def := storeDef(p)
		p.handleChannelReset(1, 7, 1, &topOfBookChannelReset{})
		if recs := handle(p, def); len(recs) != 0 {
			t.Errorf("a strike held nowhere was emitted: %+v", recs)
		}
		if len(p.instruments) != 0 {
			t.Errorf("the strike brought a definition back: %+v", p.instruments)
		}
		if p.strikesIgnored != 1 {
			t.Errorf("strikesIgnored = %d, want 1", p.strikesIgnored)
		}
	})

	// The entry is there, for the same instrument, and it is not the one the
	// strike arrived behind: a reset and a later definition both happened.
	t.Run("the definition held is not the one it arrived behind", func(t *testing.T) {
		p := NewTopOfBookParser()
		def := storeDef(p)
		p.handleChannelReset(1, 7, 1, &topOfBookChannelReset{})
		storeDef(p)
		if recs := handle(p, def); len(recs) != 0 {
			t.Errorf("a strike was emitted for a definition it did not arrive behind: %+v", recs)
		}
		if held := p.instruments[42].Strike; held != nil {
			t.Errorf("a strike was held on a definition it did not arrive behind: %+v", *held)
		}
		if p.strikesIgnored != 1 {
			t.Errorf("strikesIgnored = %d, want 1", p.strikesIgnored)
		}
	})
}

// The same race, run as one: a goroutine parsing definition-and-strike
// datagrams against one parsing ChannelResets. What -race checks is the
// locking. What the loop checks is the accounting: each datagram yields the
// pair or the definition alone, and the ones that yield the definition alone
// are exactly the strikes counted as ignored.
func TestStrikeInterval_ConcurrentChannelResetSuppressesTheStrikesItCounts(t *testing.T) {
	p := NewTopOfBookParser()
	pair := buildDatagramWithVersion(3, 1, 100, strikeTestTS(),
		buildInstrumentDefMsgV3(42, "X", "BTC", "USD"),
		buildStrikeIntervalMsg(42, strikeTestSourceID, -2, 0x01, 500, 0, 0))
	resetDatagram := buildDatagramWithVersion(3, 1, 7, strikeTestTS(), buildChannelReset(strikeTestTS()))

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 2000; i++ {
			if _, err := p.Parse(resetDatagram, PacketMeta{}); err != nil {
				t.Errorf("reset: %v", err)
				return
			}
		}
	}()
	var emitted, ignored int
	for i := 0; i < 2000; i++ {
		recs, err := p.Parse(pair, PacketMeta{})
		if err != nil {
			t.Fatalf("pair: %v", err)
		}
		switch len(recs) {
		case 2:
			emitted++
		case 1:
			ignored++
		default:
			t.Fatalf("records: %v", recordTypes(recs))
		}
	}
	<-done
	p.mu.RLock()
	got := p.strikesIgnored
	p.mu.RUnlock()
	if int(got) != ignored {
		t.Errorf("strikesIgnored = %d, but %d datagrams yielded the definition alone", got, ignored)
	}
	t.Logf("%d strikes emitted, %d suppressed by a reset in between", emitted, ignored)
}
