package main

import "time"

// Record represents a single decoded feed message ready for output.
type Record struct {
	Type            string         `json:"type"`
	Timestamp       time.Time      `json:"ts"`
	RecvTimestamp   time.Time      `json:"recv_ts,omitempty"`
	RecvTimestampNS uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind      string         `json:"recv_ts_kind,omitempty"`
	ChannelID       uint8          `json:"channel_id"`
	SequenceNumber  uint64         `json:"seq"`
	ResetCount      uint8          `json:"reset_count"`
	InstrumentID    uint32         `json:"instrument_id,omitempty"`
	Symbol          string         `json:"symbol,omitempty"`
	Fields          map[string]any `json:"fields,omitempty"`
}

// Parser decodes raw UDP datagrams into structured records.
// Implementations are stateful: they maintain instrument definitions
// to enrich marketdata messages with refdata (e.g. symbol, exponents).
type Parser interface {
	// Name returns the parser identifier (e.g. "topofbook").
	Name() string

	// Parse decodes a single UDP datagram and returns zero or more records.
	// Messages for instruments whose definitions have not yet been received
	// are buffered internally until the definition arrives.
	Parse(data []byte) ([]Record, error)

	// Buffered returns the number of messages currently buffered
	// waiting for instrument definitions.
	Buffered() int

	// InstrumentCount returns the number of instrument definitions
	// the parser has learned so far.
	InstrumentCount() int
}

// metricsAware is implemented by parsers that can emit their own metrics.
// main.go calls setMetrics at startup if the parser opts in.
type metricsAware interface {
	setMetrics(m *metrics)
}

// ParserFactory creates a new Parser instance.
type ParserFactory func() Parser

var parserRegistry = map[string]ParserFactory{}

// RegisterParser registers a parser factory under the given name.
func RegisterParser(name string, factory ParserFactory) {
	parserRegistry[name] = factory
}

// NewParser creates a parser by name from the registry.
func NewParser(name string) (Parser, bool) {
	factory, ok := parserRegistry[name]
	if !ok {
		return nil, false
	}
	return factory(), true
}

// RegisteredParsers returns the names of all registered parsers.
func RegisteredParsers() []string {
	names := make([]string, 0, len(parserRegistry))
	for name := range parserRegistry {
		names = append(names, name)
	}
	return names
}
