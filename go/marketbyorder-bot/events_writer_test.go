package main

import (
	"testing"
	"time"
)

func TestBuildEventRow_HasSourceSendRecvColumns(t *testing.T) {
	source := time.Unix(1717689600, 0).UTC()
	send := source.Add(150 * time.Millisecond)
	recv := source.Add(230 * time.Millisecond)
	rec := Record{
		Type:       "order_add",
		SourceTSNS: uint64(source.UnixNano()),
		SendTSNS:   uint64(send.UnixNano()),
		RecvTSNS:   uint64(recv.UnixNano()),
		RecvTSKind: "kernel_udp_software",
	}
	row := buildEventRow(rec, 1, "TEST")
	if row["publisher_send_ts"] != chTime(send) {
		t.Errorf("publisher_send_ts = %v, want %v", row["publisher_send_ts"], chTime(send))
	}
	if row["source_ts"] != chTime(source) {
		t.Errorf("source_ts = %v, want %v", row["source_ts"], chTime(source))
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
	row := buildEventRow(rec, 1, "")
	if _, ok := row["source_ts"]; ok {
		t.Errorf("source_ts should be omitted when SourceTSNS==0")
	}
}
