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
	SourceTSNS     uint64         `json:"source_ts_ns,omitempty"`
	SendTSNS       uint64         `json:"send_ts_ns,omitempty"`
	RecvTSNS       uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind     string         `json:"recv_ts_kind,omitempty"`
	ChannelID      uint8          `json:"channel_id"`
	Port           string         `json:"port"`
	SequenceNumber uint64         `json:"seq"`
	ResetCount     uint8          `json:"reset_count"`
	InstrumentID   uint32         `json:"instrument_id,omitempty"`
	Fields         map[string]any `json:"fields,omitempty"`
}

// Parser decodes a wire frame received on a given port and returns zero or
// more Records, plus any publisher defects observed in that frame.
//
// Defects are returned per frame rather than accumulated on the Parser because
// the Runner shares one Parser across all three port goroutines; a counter
// field on the Parser would be a data race. This is why the signature differs
// from the topofbook and marketbyorder parsers, which surface no defect counts.
//
// On success the returned slice is non-nil but may be empty, meaning the frame
// was valid and every message in it was skipped or dropped; callers must test
// len, not nil. A non-nil error indicates the frame should be dropped and a
// counter incremented, and the record slice is then nil.
type Parser interface {
	Name() string
	ParseFrame(port string, frame []byte) ([]Record, Defects, error)
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
