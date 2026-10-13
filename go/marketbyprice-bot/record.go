package main

import "time"

// Record is the parser's envelope. MessageIndex is the position of the record's
// message among the application messages of its datagram, from 0, counting the
// ones the parser emitted no record for.
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	SourceTSNS     uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS       uint64         `json:"send_ts_ns,omitempty"`
	RecvTSNS       uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind     string         `json:"recv_ts_kind,omitempty"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	MessageIndex   uint8          `json:"msg_index"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

func (r Record) recvTime(fallback time.Time) time.Time {
	if r.RecvTSNS != 0 {
		return time.Unix(0, int64(r.RecvTSNS)).UTC()
	}
	return fallback
}

func (r Record) sourceTime() (time.Time, bool) {
	if r.SourceTSNS == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(r.SourceTSNS)).UTC(), true
}

func (r Record) sendTime() time.Time {
	return time.Unix(0, int64(r.SendTSNS)).UTC()
}
