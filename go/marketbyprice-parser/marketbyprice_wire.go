// Wire format authoritatively defined by the Market-by-Price Feed spec:
// https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md
// Keep the byte layout below in sync with that document.
//
// Body sizes in this file are message size minus the 4-byte application
// message header, because that is what each Parse* function receives.
//
// Length checks are exact equality, not >=. The spec's forward-compatibility
// rule that a decoder ignores trailing bytes only applies across a Schema
// Version bump, and ParseFrameHeader rejects unimplemented versions before any
// body is parsed. Within v1, an unexpected body length is malformed.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	mbpMagic          uint16 = 0x4442
	mbpSchemaVersion  uint8  = 1
	frameHeaderSize          = 24
	messageHeaderSize        = 4
	maxFrameSize             = 1232
)

// Message type IDs. Types 0x03 and 0x05 are reserved and intentionally unused
// (Quote in top-of-book, and a reserved slot) so a misrouted sibling frame
// cannot cross-decode. 0x13, 0x14, 0x22, and the 0x02/0x04 bodies are
// byte-for-byte identical to the market-by-order feed.
const (
	msgTypeHeartbeat            uint8 = 0x01
	msgTypeInstrumentDefinition uint8 = 0x02
	msgTypeTrade                uint8 = 0x04
	msgTypeEndOfSession         uint8 = 0x06
	msgTypeManifestSummary      uint8 = 0x07
	msgTypeBatchBoundary        uint8 = 0x13
	msgTypeInstrumentReset      uint8 = 0x14
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
	errMalformedBody   = errors.New("malformed message body")
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

// flagSnapshot is application-header Flags bit 0. The publisher sets it on the
// snapshot port and clears it on mktdata and refdata. It MUST NOT be used to
// route a message — Type ID and port already determine that — but disagreement
// with the arrival port is a publisher defect worth counting.
const flagSnapshot uint16 = 0x0001

// ParseFrameHeader decodes the 24-byte frame header from buf.
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
	if h.Magic != mbpMagic {
		return h, errBadMagic
	}
	if h.SchemaVersion != mbpSchemaVersion {
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

// BatchBoundaryBody is the 12-byte body of a BatchBoundary message.
type BatchBoundaryBody struct {
	BatchID   uint32
	BatchTime time.Time
}

// ParseBatchBoundary decodes a BatchBoundary body. buf must be exactly 12 bytes.
func ParseBatchBoundary(buf []byte) (BatchBoundaryBody, error) {
	if len(buf) != 12 {
		return BatchBoundaryBody{}, fmt.Errorf("%w: expected 12 bytes for batch_boundary body, got %d", errTruncated, len(buf))
	}
	return BatchBoundaryBody{
		BatchID:   binary.LittleEndian.Uint32(buf[0:4]),
		BatchTime: readTSNs(buf[4:12]),
	}, nil
}

// InstrumentResetBody is the 24-byte body of an InstrumentReset message.
type InstrumentResetBody struct {
	InstrumentID uint32
	Reason       uint8
	NewAnchorSeq uint64
	Timestamp    time.Time
}

// ParseInstrumentReset decodes an InstrumentReset body. buf must be exactly 24 bytes.
func ParseInstrumentReset(buf []byte) (InstrumentResetBody, error) {
	if len(buf) != 24 {
		return InstrumentResetBody{}, fmt.Errorf("%w: expected 24 bytes for instrument_reset body, got %d", errTruncated, len(buf))
	}
	return InstrumentResetBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		Reason:       buf[4],
		// bytes 5-7 are reserved padding
		NewAnchorSeq: binary.LittleEndian.Uint64(buf[8:16]),
		Timestamp:    readTSNs(buf[16:24]),
	}, nil
}

// SnapshotEndBody is the 16-byte body of a SnapshotEnd message.
type SnapshotEndBody struct {
	InstrumentID uint32
	AnchorSeq    uint64
	SnapshotID   uint32
}

// ParseSnapshotEnd decodes a SnapshotEnd body. buf must be exactly 16 bytes.
func ParseSnapshotEnd(buf []byte) (SnapshotEndBody, error) {
	if len(buf) != 16 {
		return SnapshotEndBody{}, fmt.Errorf("%w: expected 16 bytes for snapshot_end body, got %d", errTruncated, len(buf))
	}
	return SnapshotEndBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		AnchorSeq:    binary.LittleEndian.Uint64(buf[4:12]),
		SnapshotID:   binary.LittleEndian.Uint32(buf[12:16]),
	}, nil
}
