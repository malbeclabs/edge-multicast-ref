package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	dobMagic           uint16 = 0x4444
	dobSchemaVersion   uint8  = 1
	frameHeaderSize           = 24
	messageHeaderSize         = 4
	maxFrameSize              = 1232
)

// Message type IDs.
const (
	msgTypeHeartbeat            uint8 = 0x01
	msgTypeInstrumentDefinition uint8 = 0x02
	msgTypeTrade                uint8 = 0x04
	msgTypeEndOfSession         uint8 = 0x06
	msgTypeManifestSummary      uint8 = 0x07
	msgTypeOrderAdd             uint8 = 0x10
	msgTypeOrderCancel          uint8 = 0x11
	msgTypeOrderExecute         uint8 = 0x12
	msgTypeBatchBoundary        uint8 = 0x13
	msgTypeInstrumentReset      uint8 = 0x14
	msgTypeSnapshotBegin        uint8 = 0x20
	msgTypeSnapshotOrder        uint8 = 0x21
	msgTypeSnapshotEnd          uint8 = 0x22
)

// Wire decoding errors.
var (
	errBadMagic        = errors.New("bad magic")
	errSchemaVersion   = errors.New("unsupported schema version")
	errFrameTooShort   = errors.New("frame too short for header")
	errFrameLength     = errors.New("frame length mismatch")
	errMessageTooShort = errors.New("message too short for header")
	errMessageLength   = errors.New("message length out of range")
	errTruncated       = errors.New("truncated message body")
)

// FrameHeader is the 24-byte frame header common to all three ports.
type FrameHeader struct {
	Magic         uint16
	SchemaVersion uint8
	ChannelID     uint8
	Sequence      uint64
	SendTimestamp time.Time
	MessageCount  uint8
	ResetCount    uint8
	FrameLength   uint16
}

// MessageHeader is the 4-byte header preceding each application message.
type MessageHeader struct {
	Type   uint8
	Length uint8
	Flags  uint16
}

const flagSnapshot uint16 = 0x0001

// ParseFrameHeader decodes the 24-byte frame header from buf.
// Returns the header, the number of bytes consumed (always 24), and any error.
// Caller is responsible for verifying buf length is at least frameHeaderSize.
func ParseFrameHeader(buf []byte) (FrameHeader, error) {
	if len(buf) < frameHeaderSize {
		return FrameHeader{}, errFrameTooShort
	}
	h := FrameHeader{
		Magic:         binary.LittleEndian.Uint16(buf[0:2]),
		SchemaVersion: buf[2],
		ChannelID:     buf[3],
		Sequence:      binary.LittleEndian.Uint64(buf[4:12]),
		MessageCount:  buf[20],
		ResetCount:    buf[21],
		FrameLength:   binary.LittleEndian.Uint16(buf[22:24]),
	}
	if h.Magic != dobMagic {
		return h, errBadMagic
	}
	if h.SchemaVersion != dobSchemaVersion {
		return h, errSchemaVersion
	}
	tsNs := binary.LittleEndian.Uint64(buf[12:20])
	h.SendTimestamp = time.Unix(0, int64(tsNs)).UTC()
	if int(h.FrameLength) != len(buf) {
		return h, errFrameLength
	}
	return h, nil
}

// ParseMessageHeader decodes a 4-byte application message header.
func ParseMessageHeader(buf []byte) (MessageHeader, error) {
	if len(buf) < messageHeaderSize {
		return MessageHeader{}, errMessageTooShort
	}
	return MessageHeader{
		Type:   buf[0],
		Length: buf[1],
		Flags:  binary.LittleEndian.Uint16(buf[2:4]),
	}, nil
}

// fixedString decodes a fixed-length null-padded ASCII field.
func fixedString(buf []byte) string {
	for i, b := range buf {
		if b == 0 {
			return string(buf[:i])
		}
	}
	return string(buf)
}

// readTSNs reads an 8-byte little-endian nanoseconds-since-epoch timestamp.
func readTSNs(buf []byte) time.Time {
	ns := binary.LittleEndian.Uint64(buf)
	return time.Unix(0, int64(ns)).UTC()
}

// HeartbeatBody is the 12-byte body of a Heartbeat message (after the 4-byte header).
type HeartbeatBody struct {
	ChannelID uint8
	Timestamp time.Time
}

// ParseHeartbeat decodes a Heartbeat body. buf must be exactly 12 bytes.
func ParseHeartbeat(buf []byte) (HeartbeatBody, error) {
	if len(buf) != 12 {
		return HeartbeatBody{}, fmt.Errorf("%w: expected 12 bytes for heartbeat body, got %d", errTruncated, len(buf))
	}
	return HeartbeatBody{
		ChannelID: buf[0],
		Timestamp: readTSNs(buf[4:12]),
	}, nil
}

// EndOfSessionBody is the 8-byte body of an EndOfSession message.
type EndOfSessionBody struct {
	Timestamp time.Time
}

// ParseEndOfSession decodes an EndOfSession body. buf must be exactly 8 bytes.
func ParseEndOfSession(buf []byte) (EndOfSessionBody, error) {
	if len(buf) != 8 {
		return EndOfSessionBody{}, fmt.Errorf("%w: expected 8 bytes for end_of_session body, got %d", errTruncated, len(buf))
	}
	return EndOfSessionBody{Timestamp: readTSNs(buf[0:8])}, nil
}

// ManifestSummaryBody is the 20-byte body of a ManifestSummary message.
type ManifestSummaryBody struct {
	ChannelID       uint8
	Valid           uint8
	ManifestSeq     uint16
	InstrumentCount uint32
	Timestamp       time.Time
}

// ParseManifestSummary decodes a ManifestSummary body. buf must be exactly 20 bytes.
func ParseManifestSummary(buf []byte) (ManifestSummaryBody, error) {
	if len(buf) != 20 {
		return ManifestSummaryBody{}, fmt.Errorf("%w: expected 20 bytes for manifest_summary body, got %d", errTruncated, len(buf))
	}
	return ManifestSummaryBody{
		ChannelID:       buf[0],
		Valid:           buf[1],
		ManifestSeq:     binary.LittleEndian.Uint16(buf[4:6]),
		InstrumentCount: binary.LittleEndian.Uint32(buf[8:12]),
		Timestamp:       readTSNs(buf[12:20]),
	}, nil
}

// InstrumentDefinitionBody is the 76-byte body of an InstrumentDefinition.
type InstrumentDefinitionBody struct {
	InstrumentID  uint32
	Symbol        string
	Leg1          string
	Leg2          string
	AssetClass    uint8
	PriceExponent int8
	QtyExponent   int8
	MarketModel   uint8
	TickSizeRaw   int64
	LotSizeRaw    uint64
	ContractValue uint64
	Expiry        time.Time
	SettleType    uint8
	PriceBound    uint8
	ManifestSeq   uint16
}

// ParseInstrumentDefinition decodes an InstrumentDefinition body. buf must be exactly 76 bytes.
func ParseInstrumentDefinition(buf []byte) (InstrumentDefinitionBody, error) {
	if len(buf) != 76 {
		return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected 76 bytes for instrument_definition body, got %d", errTruncated, len(buf))
	}
	return InstrumentDefinitionBody{
		InstrumentID:  binary.LittleEndian.Uint32(buf[0:4]),
		Symbol:        fixedString(buf[4:20]),
		Leg1:          fixedString(buf[20:28]),
		Leg2:          fixedString(buf[28:36]),
		AssetClass:    buf[36],
		PriceExponent: int8(buf[37]),
		QtyExponent:   int8(buf[38]),
		MarketModel:   buf[39],
		TickSizeRaw:   int64(binary.LittleEndian.Uint64(buf[40:48])),
		LotSizeRaw:    binary.LittleEndian.Uint64(buf[48:56]),
		ContractValue: binary.LittleEndian.Uint64(buf[56:64]),
		Expiry:        readTSNs(buf[64:72]),
		SettleType:    buf[72],
		PriceBound:    buf[73],
		ManifestSeq:   binary.LittleEndian.Uint16(buf[74:76]),
	}, nil
}

// TradeBody is the 48-byte body of a Trade message.
type TradeBody struct {
	InstrumentID        uint32
	SourceID            uint16
	AggressorSide       uint8
	TradeFlags          uint8
	SourceTimestamp     time.Time
	TradePriceRaw       int64
	TradeQtyRaw         uint64
	TradeID             uint64
	CumulativeVolumeRaw uint64
}

// ParseTrade decodes a Trade body. buf must be exactly 48 bytes.
func ParseTrade(buf []byte) (TradeBody, error) {
	if len(buf) != 48 {
		return TradeBody{}, fmt.Errorf("%w: expected 48 bytes for trade body, got %d", errTruncated, len(buf))
	}
	return TradeBody{
		InstrumentID:        binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:            binary.LittleEndian.Uint16(buf[4:6]),
		AggressorSide:       buf[6],
		TradeFlags:          buf[7],
		SourceTimestamp:     readTSNs(buf[8:16]),
		TradePriceRaw:       int64(binary.LittleEndian.Uint64(buf[16:24])),
		TradeQtyRaw:         binary.LittleEndian.Uint64(buf[24:32]),
		TradeID:             binary.LittleEndian.Uint64(buf[32:40]),
		CumulativeVolumeRaw: binary.LittleEndian.Uint64(buf[40:48]),
	}, nil
}
