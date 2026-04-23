package main

import "time"

// Record mirrors the topofbook-parser's JSON Lines output format.
// We decode just the fields the bot uses — unknown fields are ignored.
type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	ChannelID      uint8          `json:"channel_id"`
	SequenceNumber uint64         `json:"seq"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Symbol         string         `json:"symbol,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
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
