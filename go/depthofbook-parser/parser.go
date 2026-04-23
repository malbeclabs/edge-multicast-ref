package main

import (
	"fmt"
	"time"
)

// Record is the JSON-serialised envelope emitted by the parser for every
// wire message. Bot consumes these one per line on the parser socket.
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

// Parser decodes a wire frame received on a given port and returns zero or
// more Records. A return of (nil, nil) means the frame was valid but produced
// no records (e.g., padding-only). A non-nil error indicates the frame should
// be dropped and an error counter incremented.
type Parser interface {
	Name() string
	ParseFrame(port string, frame []byte) ([]Record, error)
}

var parserRegistry = map[string]func() Parser{}

func registerParser(name string, ctor func() Parser) {
	parserRegistry[name] = ctor
}

func newParser(name string) (Parser, error) {
	ctor, ok := parserRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown parser: %q", name)
	}
	return ctor(), nil
}
