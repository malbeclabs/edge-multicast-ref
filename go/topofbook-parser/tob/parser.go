package tob

import "time"

// Record represents a single decoded feed message ready for output.
type Record struct {
	Type            string         `json:"type"`
	Timestamp       time.Time      `json:"ts"`
	RecvTimestamp   time.Time      `json:"recv_ts,omitempty"`
	RecvTimestampNS uint64         `json:"parser_kernel_recv_ts_ns,omitempty"`
	RecvTSKind      string         `json:"recv_ts_kind,omitempty"`
	PublisherSource string         `json:"publisher_source_ip,omitempty"`
	MulticastGroup  string         `json:"multicast_group,omitempty"`
	Port            int            `json:"port,omitempty"`
	Channel         string         `json:"channel,omitempty"`
	ChannelID       uint8          `json:"channel_id"`
	SequenceNumber  uint64         `json:"seq"`
	ResetCount      uint8          `json:"reset_count"`
	InstrumentID    uint32         `json:"instrument_id,omitempty"`
	Symbol          string         `json:"symbol,omitempty"`
	Fields          map[string]any `json:"fields,omitempty"`
}

// PacketMeta carries transport receive metadata for the UDP datagram being parsed.
type PacketMeta struct {
	RecvTimestamp   time.Time
	RecvTimestampNS uint64
	RecvTSKind      string
	PublisherSource string
	MulticastGroup  string
	Port            int
	Channel         string
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
	Parse(data []byte, meta PacketMeta) ([]Record, error)

	// Buffered returns the number of messages currently buffered
	// waiting for instrument definitions.
	Buffered() int

	// InstrumentCount returns the number of instrument definitions
	// the parser has learned so far.
	InstrumentCount() int
}
