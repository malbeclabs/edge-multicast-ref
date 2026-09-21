package main

import (
	"testing"
)

// stubEnqueuer records rows by table so tests can assert the mapping.
type stubEnqueuer struct {
	rows map[string][]map[string]any
}

func newStubEnqueuer() *stubEnqueuer {
	return &stubEnqueuer{rows: map[string][]map[string]any{}}
}

func (s *stubEnqueuer) Enqueue(table string, row map[string]any) bool {
	s.rows[table] = append(s.rows[table], row)
	return true
}

func (s *stubEnqueuer) only(t *testing.T, table string) map[string]any {
	t.Helper()
	got := s.rows[table]
	if len(got) != 1 {
		t.Fatalf("expected exactly one %s row, got %d", table, len(got))
	}
	return got[0]
}

func TestEventsWriter_InstrumentDefinition(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       instDefRec(11, "BTC-USDT", 5),
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["symbol"] != "BTC-USDT" {
		t.Errorf("symbol: %v", row["symbol"])
	}
	if row["price_exponent"] != int8(-2) || row["qty_exponent"] != int8(-8) {
		t.Errorf("exponents: %v %v", row["price_exponent"], row["qty_exponent"])
	}
	if row["manifest_seq"] != uint16(5) {
		t.Errorf("manifest_seq: %v", row["manifest_seq"])
	}
}

func TestEventsWriter_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       instDefRec(11, "BTC-USDT", 5),
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["source_id"] != uint16(77) {
		t.Errorf("source_id: got %v (%T) want uint16(77)", row["source_id"], row["source_id"])
	}
}

// A v1 definition carries no Source ID at all. The row must still be written,
// with the registry's Unknown value rather than being dropped.
func TestEventsWriter_InstrumentDefinitionWithoutSourceID(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := instDefRec(11, "BTC-USDT", 5)
	delete(rec.Fields, "source_id")
	w.Write(ChannelEvent{
		Kind:         KindInstrumentDefinition,
		InstrumentID: 11,
		Record:       rec,
	}, 0, "BTC-USDT", -2, -8)

	row := st.only(t, "instruments")
	if row["source_id"] != uint16(0) {
		t.Errorf("source_id: got %v want uint16(0)", row["source_id"])
	}
}

// asset_class, market_model, settle_type and price_bound arrive as raw uint8.
// The parser stringifies side, action and the reason fields inline but NOT these,
// and the schema declares them LowCardinality(String), so reading them as strings
// would write empty values for every instrument.
func TestEventsWriter_InstrumentEnumsAreStringified(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := instDefRec(11, "SYM", 1)
	rec.Fields["asset_class"] = float64(1)  // crypto_spot
	rec.Fields["market_model"] = float64(1) // clob
	rec.Fields["settle_type"] = float64(1)  // cash
	rec.Fields["price_bound"] = float64(2)  // non_negative

	w.Write(ChannelEvent{Kind: KindInstrumentDefinition, InstrumentID: 11, Record: rec}, 0, "SYM", -2, -8)

	row := st.only(t, "instruments")
	for col, want := range map[string]string{
		"asset_class":  "crypto_spot",
		"market_model": "clob",
		"settle_type":  "cash",
		"price_bound":  "non_negative",
	} {
		if got := row[col]; got != want {
			t.Errorf("%s: got %v want %q", col, got, want)
		}
	}
}

func TestEventsWriter_LevelUpdateScalesAndKeepsSentinelsNull(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := levelUpdateRec(11, 900, 6, "bid", 123456, 500)
	// levelUpdateRec sets order_count; drop it to model the wire sentinel, which
	// the parser signals by OMITTING the key.
	delete(rec.Fields, "order_count")

	w.Write(ChannelEvent{Kind: KindAppliedDelta, InstrumentID: 11, Record: rec}, 0, "SYM", -2, -8)

	row := st.only(t, "events")
	if row["kind"] != "level_update" {
		t.Errorf("kind: %v", row["kind"])
	}
	if got := row["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled by 10^-2: got %v want ~1234.56", got)
	}
	if row["order_count"] != nil {
		t.Errorf("an omitted order_count must be nil, not %v — zero is a real count", row["order_count"])
	}
	if row["level_index"] != nil {
		t.Errorf("an omitted level_index must be nil, got %v", row["level_index"])
	}
	if row["per_instrument_seq"] != uint32(6) {
		t.Errorf("per_instrument_seq: %v", row["per_instrument_seq"])
	}
}

func TestEventsWriter_LevelUpdateKeepsRealZeroOrderCount(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := levelUpdateRec(11, 900, 6, "bid", 1000, 5)
	rec.Fields["order_count"] = float64(0) // a real count of zero, not the sentinel

	w.Write(ChannelEvent{Kind: KindAppliedDelta, InstrumentID: 11, Record: rec}, 0, "SYM", 0, 0)

	row := st.only(t, "events")
	if row["order_count"] == nil {
		t.Fatal("a present order_count of 0 must persist as 0, not NULL")
	}
	if got := row["order_count"].(uint32); got != 0 {
		t.Errorf("order_count: got %v want 0", got)
	}
}

func TestEventsWriter_BookClear(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindAppliedDelta,
		InstrumentID: 11,
		Record:       bookClearRec(11, 900, 6, "bid", "from_price", 5000),
	}, 0, "SYM", -2, 0)

	row := st.only(t, "events")
	if row["kind"] != "book_clear" {
		t.Errorf("kind: %v", row["kind"])
	}
	if row["clear_side"] != "bid" || row["clear_scope"] != "from_price" {
		t.Errorf("clear cols: %v %v", row["clear_side"], row["clear_scope"])
	}
	if got := row["from_price"].(float64); got < 49.99 || got > 50.01 {
		t.Errorf("from_price must be scaled: got %v want ~50", got)
	}
}

func TestEventsWriter_ChannelHealth(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{Record: Record{
		Type: "manifest_summary", Port: "refdata", ChannelID: 0,
		Fields: map[string]any{
			"manifest_seq": float64(7), "valid": float64(1), "instrument_count": float64(42),
		},
	}}, 0, "", 0, 0)

	row := st.only(t, "channel_health")
	if row["kind"] != "manifest_summary" {
		t.Errorf("kind: %v", row["kind"])
	}
	if row["manifest_seq"] != uint16(7) || row["instrument_count"] != uint32(42) {
		t.Errorf("manifest cols: %v %v", row["manifest_seq"], row["instrument_count"])
	}
}

// A nil client must be a safe no-op so the book-builder runs with persistence disabled.
func TestEventsWriter_NilClientIsNoOp(t *testing.T) {
	w := NewEventsWriter(nil)
	w.Write(ChannelEvent{Kind: KindAppliedDelta, Record: levelUpdateRec(11, 900, 6, "bid", 1000, 5)}, 0, "SYM", 0, 0)
	// No panic is the assertion.
}

func TestEventsWriter_WireLevelDenormalizesGroupIdentity(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	g := SnapshotGroup{
		SnapshotID: 4, AnchorSeq: 9999, TotalLevels: 3,
		LastInstrumentSeq: 100, DepthBound: 25,
	}
	rec := snapLevelRec(11, 4, "bid", 123456, 500)

	w.WriteWireLevel(rec, 0, g, "SYM", -2, -8)

	row := st.only(t, "wire_levels")
	if row["snapshot_id"] != uint32(4) || row["anchor_seq"] != uint64(9999) {
		t.Errorf("group identity: %v %v", row["snapshot_id"], row["anchor_seq"])
	}
	if row["total_levels"] != uint32(3) || row["last_instrument_seq"] != uint32(100) {
		t.Errorf("group counts: %v %v", row["total_levels"], row["last_instrument_seq"])
	}
	if row["depth_bound"] != uint32(25) {
		t.Errorf("depth_bound: %v", row["depth_bound"])
	}
	if got := row["price"].(float64); got < 1234.55 || got > 1234.57 {
		t.Errorf("price must be scaled: got %v", got)
	}
	if row["side"] != "bid" {
		t.Errorf("side: %v", row["side"])
	}
}

func TestEventsWriter_WireLevelOmittedOrderCountIsNull(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	rec := snapLevelRec(11, 4, "bid", 1000, 5)
	delete(rec.Fields, "order_count") // the wire sentinel

	w.WriteWireLevel(rec, 0, SnapshotGroup{SnapshotID: 4}, "SYM", 0, 0)

	if row := st.only(t, "wire_levels"); row["order_count"] != nil {
		t.Errorf("an omitted order_count must be nil, got %v", row["order_count"])
	}
}

// A batch_boundary belongs to the channel, not to an instrument: the wire
// carries no Instrument ID, so rec.InstrumentID is 0 and no symbol answers to
// it. The row must therefore carry NEITHER column, whatever the caller passes
// for symbol — stamping them makes every boundary row claim instrument 0 and
// whichever symbol the refdata map holds at key 0.
//
// The symbol argument here is deliberately non-empty. Today's two call sites
// both pass "", so the wrong symbol is not reachable through them; that is the
// call sites being careful, not the writer being correct, and this asserts the
// writer.
func TestEventsWriter_BatchBoundaryCarriesNoInstrumentIdentity(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind: KindBatchBoundary,
		Record: Record{
			Type: "batch_boundary", Port: "mktdata", ChannelID: 2,
			SequenceNumber: 91, ResetCount: 3,
			Fields: map[string]any{"batch_id": float64(77), "batch_ts": "2026-08-02T00:00:00Z"},
		},
	}, 2, "BTC-USDT", -2, -8)

	if n := len(st.rows["channel_health"]); n != 0 {
		t.Errorf("a boundary carries batch_id and batch_ts, which channel_health has no columns for; got %d rows there", n)
	}
	row := st.only(t, "events")
	if got, ok := row["symbol"]; ok {
		t.Errorf("symbol must be omitted for a batch_boundary, got %#v", got)
	}
	if got, ok := row["instrument_id"]; ok {
		t.Errorf("instrument_id must be omitted for a batch_boundary, got %#v", got)
	}
	// The channel-scoped columns and the boundary payload still have to land.
	if row["kind"] != "batch_boundary" {
		t.Errorf("kind: %v", row["kind"])
	}
	if row["channel_id"] != uint8(2) {
		t.Errorf("channel_id: got %v want 2", row["channel_id"])
	}
	if row["mktdata_seq"] != uint64(91) {
		t.Errorf("mktdata_seq: got %v want 91", row["mktdata_seq"])
	}
	if row["reset_count"] != uint8(3) {
		t.Errorf("reset_count: got %v want 3", row["reset_count"])
	}
	if row["batch_id"] != uint32(77) {
		t.Errorf("batch_id: got %v want 77", row["batch_id"])
	}
	if row["batch_ts"] == nil || row["batch_ts"] == "" {
		t.Errorf("batch_ts: got %#v", row["batch_ts"])
	}
}

// The counterpart: an instrument-tied kind must still be stamped with the
// identity it does have. Dropping the identity columns wholesale would be the
// opposite defect.
func TestEventsWriter_InstrumentTiedKindKeepsIdentity(t *testing.T) {
	st := newStubEnqueuer()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         KindAppliedDelta,
		InstrumentID: 11,
		Record:       levelUpdateRec(11, 900, 6, "bid", 1000, 5),
	}, 0, "BTC-USDT", 0, 0)

	row := st.only(t, "events")
	if row["symbol"] != "BTC-USDT" {
		t.Errorf("symbol: got %#v want BTC-USDT", row["symbol"])
	}
	if row["instrument_id"] != uint32(11) {
		t.Errorf("instrument_id: got %#v want 11", row["instrument_id"])
	}
}
