package main

import (
	"sync"
	"testing"
	"time"
)

func TestBuildEventRow_HasSourceSendRecvColumns(t *testing.T) {
	sourceTS := time.Unix(1717689600, 0).UTC()
	send := sourceTS.Add(150 * time.Millisecond)
	recv := sourceTS.Add(230 * time.Millisecond)
	rec := Record{
		Type:       "order_add",
		SourceTSNS: uint64(sourceTS.UnixNano()),
		SendTSNS:   uint64(send.UnixNano()),
		RecvTSNS:   uint64(recv.UnixNano()),
		RecvTSKind: "kernel_udp_software",
	}
	row := buildEventRow(rec, 1, "TEST", time.Now().UTC())
	if row["publisher_send_ts"] != chTime(send) {
		t.Errorf("publisher_send_ts = %v, want %v", row["publisher_send_ts"], chTime(send))
	}
	if row["source_ts"] != chTime(sourceTS) {
		t.Errorf("source_ts = %v, want %v", row["source_ts"], chTime(sourceTS))
	}
	if row["recv_ts"] != chTime(recv) {
		t.Errorf("recv_ts = %v, want %v", row["recv_ts"], chTime(recv))
	}
	if row["recv_ts_kind"] != "kernel_udp_software" {
		t.Errorf("recv_ts_kind = %v", row["recv_ts_kind"])
	}
}

func TestBuildEventRow_OmitsSourceTsWhenAbsent(t *testing.T) {
	rec := Record{Type: "heartbeat", SendTSNS: uint64(time.Unix(1717689600, 0).UnixNano()), RecvTSNS: uint64(time.Unix(1717689600, 0).UnixNano())}
	row := buildEventRow(rec, 1, "", time.Now().UTC())
	if _, ok := row["source_ts"]; ok {
		t.Errorf("source_ts should be omitted when SourceTSNS==0")
	}
}

// A record with no RecvTSNS falls back to the clock read Write already took,
// not to a fresh one taken inside the row builder. One Write call can produce
// two rows from one record, and a second read would let them disagree on
// recv_ts by however long the call took — and pin nothing in a test.
func TestBuildEventRow_RecvTsFallsBackToTheSuppliedNow(t *testing.T) {
	now := time.Unix(1717689600, 0).UTC()
	rec := Record{Type: "order_add", SendTSNS: uint64(now.UnixNano())}
	row := buildEventRow(rec, 1, "TEST", now)
	if row["recv_ts"] != chTime(now) {
		t.Errorf("recv_ts = %v, want the supplied now %v", row["recv_ts"], chTime(now))
	}
}

func TestEventsWriter_InstrumentDefinitionCarriesSourceID(t *testing.T) {
	cw := &captureWriter{}
	w := NewEventsWriter(cw)

	w.Write(ChannelEvent{
		InstrumentID: 4242,
		Record: Record{
			Type:         "instrument_definition",
			InstrumentID: 4242,
			Fields: map[string]any{
				"symbol": "BTC-USDT",
				// float64, not uint16: records reach the book-builder as decoded JSON.
				"source_id": float64(77),
			},
		},
	}, 0, "BTC-USDT", -2, -8)

	rows := cw.captured()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0]["source_id"] != uint16(77) {
		t.Errorf("source_id: got %v (%T) want uint16(77)", rows[0]["source_id"], rows[0]["source_id"])
	}
}

// tableStub records rows per table, which captureWriter cannot do — it is
// table-blind, and asserting which table a kind lands in is the point here.
// Locked like captureWriter, so a shard test that writes from the shard
// goroutine can reuse it.
type tableStub struct {
	mu   sync.Mutex
	rows map[string][]map[string]any
}

func newTableStub() *tableStub {
	return &tableStub{rows: map[string][]map[string]any{}}
}

func (s *tableStub) Enqueue(table string, row map[string]any) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows[table] = append(s.rows[table], row)
	return true
}

func (s *tableStub) count(table string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.rows[table])
}

func (s *tableStub) only(t *testing.T, table string) map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	got := s.rows[table]
	if len(got) != 1 {
		t.Fatalf("expected exactly one %s row, got %d", table, len(got))
	}
	return got[0]
}

// A batch_boundary belongs to the channel, not to an instrument: the wire
// carries no Instrument ID, so rec.InstrumentID is 0 and no symbol answers to
// it. The row must therefore carry NEITHER column, whatever the caller passes
// for symbol — writing them made every boundary row claim instrument 0 and
// whichever symbol the refdata map happened to hold at key 0.
//
// The symbol argument here is deliberately non-empty. runFence, the only call
// site a boundary reaches, passes ""; that is the call site being careful, not
// the writer being correct, and this asserts the writer.
func TestEventsWriter_BatchBoundaryCarriesNoInstrumentIdentity(t *testing.T) {
	st := newTableStub()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind: "applied_delta",
		Record: Record{
			Type: "batch_boundary", Port: "mktdata", ChannelID: 2,
			SequenceNumber: 91, ResetCount: 3,
			Fields: map[string]any{"batch_id": float64(77), "batch_ts": "2026-08-02T00:00:00Z"},
		},
	}, 2, "BTC-USDT", -2, -8)

	if n := st.count("channel_health"); n != 0 {
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
	st := newTableStub()
	w := NewEventsWriter(st)

	w.Write(ChannelEvent{
		Kind:         "applied_delta",
		InstrumentID: 11,
		Record: Record{
			Type: "order_add", Port: "mktdata", InstrumentID: 11,
			Fields: map[string]any{
				"order_id": float64(5), "side": "bid",
				"price_raw": float64(1000), "qty_raw": float64(5),
			},
		},
	}, 0, "BTC-USDT", 0, 0)

	row := st.only(t, "events")
	if row["symbol"] != "BTC-USDT" {
		t.Errorf("symbol: got %#v want BTC-USDT", row["symbol"])
	}
	if row["instrument_id"] != uint32(11) {
		t.Errorf("instrument_id: got %#v want 11", row["instrument_id"])
	}
}
