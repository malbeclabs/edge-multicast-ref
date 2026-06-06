package main

import "time"

// Record mirrors the topofbook-parser's JSON Lines output format.
// We decode just the fields the bot uses — unknown fields are ignored.
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	SourceTSNS     uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS       uint64         `json:"send_ts_ns,omitempty"`
	RecvTSNS       uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind     string         `json:"recv_ts_kind,omitempty"`
	ChannelID      uint8          `json:"channel_id"`
	SequenceNumber uint64         `json:"seq"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Symbol         string         `json:"symbol,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

// recvTime returns the kernel NIC receive time; falls back when absent.
func (r *Record) recvTime(fallback time.Time) time.Time {
	if r.RecvTSNS != 0 {
		return time.Unix(0, int64(r.RecvTSNS)).UTC()
	}
	return fallback
}

func (r *Record) sourceTime() (time.Time, bool) {
	if r.SourceTSNS == 0 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(r.SourceTSNS)).UTC(), true
}

func (r *Record) sendTime() time.Time {
	return time.Unix(0, int64(r.SendTSNS)).UTC()
}

// Convenience views over Record.Fields. Field names match the parser's
// JSONL emission in topofbook.go (see handleQuote/handleTrade).
// Missing keys return (zero, false) — callers must check and skip.

func (r *Record) bidPrice() (float64, bool) { return floatField(r, "bid_price") }
func (r *Record) askPrice() (float64, bool) { return floatField(r, "ask_price") }
func (r *Record) bidQty() (float64, bool)   { return floatField(r, "bid_qty") }
func (r *Record) askQty() (float64, bool)   { return floatField(r, "ask_qty") }

func (r *Record) tradePrice() (float64, bool)      { return floatField(r, "trade_price") }
func (r *Record) tradeQty() (float64, bool)        { return floatField(r, "trade_qty") }
func (r *Record) cumulativeVolume() (float64, bool) { return floatField(r, "cumulative_volume") }

func (r *Record) aggressorSide() (string, bool) { return stringField(r, "aggressor_side") }
func (r *Record) tradeID() (uint64, bool) {
	// JSON numbers decode as float64; trade_id fits fine for demo-scale sequences.
	f, ok := floatField(r, "trade_id")
	if !ok {
		return 0, false
	}
	return uint64(f), true
}

func floatField(r *Record, key string) (float64, bool) {
	v, ok := r.Fields[key]
	if !ok {
		return 0, false
	}
	f, ok := v.(float64)
	return f, ok
}

func stringField(r *Record, key string) (string, bool) {
	v, ok := r.Fields[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
