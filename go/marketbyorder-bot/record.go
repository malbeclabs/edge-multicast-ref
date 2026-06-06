package main

import "time"

type Record struct {
	Type           string         `json:"type"`
	Timestamp      time.Time      `json:"ts"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}
