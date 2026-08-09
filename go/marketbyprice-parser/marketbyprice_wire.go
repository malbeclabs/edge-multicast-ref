// Wire format authoritatively defined by the Market-by-Price Feed spec:
// https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-price/spec.md
// Keep the byte layout below in sync with that document.
//
// Body sizes in this file are message size minus the 4-byte application
// message header, because that is what each Parse* function receives.
//
// Length checks are exact equality, not >=. The spec's forward-compatibility
// rule that a decoder ignores trailing bytes only applies across a Schema
// Version bump, and ParseFrameHeader rejects unimplemented versions (anything
// other than v1 or v3) before any body is parsed. Within a given version, an
// unexpected body length is malformed.

package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	mbpMagic           uint16 = 0x4442
	mbpSchemaVersionV1 uint8  = 1 // v1: InstrumentDefinition, 76-byte body (80-byte message)
	mbpSchemaVersionV3 uint8  = 3 // v3: InstrumentDefinition, 126-byte body (130-byte message)
	frameHeaderSize           = 24
	messageHeaderSize         = 4
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
	msgTypeLiquidation          uint8 = 0x08
	msgTypeSnapshotBegin        uint8 = 0x20
	msgTypeLevelUpdate          uint8 = 0x40
	msgTypeBookClear            uint8 = 0x41
	msgTypeSnapshotLevel        uint8 = 0x42
)

// Wire decoding errors.
var (
	errBadMagic        = errors.New("bad magic")
	errSchemaVersion   = errors.New("unsupported schema version")
	errFrameTooShort   = errors.New("frame too short for header")
	errFrameLength     = errors.New("frame length mismatch")
	errMessageTooShort = errors.New("message too short for header")
	errMessageLength   = errors.New("message length out of range")
	errMessageCount    = errors.New("message count out of range")
	errTruncated       = errors.New("truncated message body")
	errMalformedBody   = errors.New("malformed message body")
	errUnknownType     = errors.New("unknown message type")
)

// FrameHeader is the 24-byte frame header common to all three ports.
type FrameHeader struct {
	Magic         uint16
	SchemaVersion uint8
	ChannelID     uint8
	Sequence      uint64
	SendTimestamp time.Time
	MessageCount  uint8 // 1-255; ParseFrameHeader rejects 0
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
	if h.SchemaVersion != mbpSchemaVersionV1 && h.SchemaVersion != mbpSchemaVersionV3 {
		return h, errSchemaVersion
	}
	tsNs := binary.LittleEndian.Uint64(buf[12:20])
	h.SendTimestamp = time.Unix(0, int64(tsNs)).UTC()
	if int(h.FrameLength) != len(buf) {
		return h, errFrameLength
	}
	// The spec gives Message Count a range of 1-255: a frame carries at least one
	// application message. Checked here rather than left to the walk, because a
	// zero count makes the loop body unreachable, so the frame would otherwise be
	// accepted as valid-but-empty and the violation would never be counted. This
	// is the last surviving silently-tolerated empty frame now that leftover body
	// bytes fail the frame.
	if h.MessageCount == 0 {
		return h, fmt.Errorf("%w: message count 0, frame carries no messages", errMessageCount)
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

// InstrumentDefinitionBody is the body of an InstrumentDefinition. Its wire
// length depends on schema version: 76 bytes at v1, 126 bytes at v3.
type InstrumentDefinitionBody struct {
	InstrumentID  uint32
	SourceID      uint16 // 0 at schema v1, which carries no Source ID
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

// InstrumentDefinition body lengths, excluding the 4-byte message header.
//
// v3 inserts Source ID (u16) after Instrument ID and widens Symbol from
// char[16] to char[64], shifting every field after Instrument ID by 50 bytes.
// Nothing else in this feed changed between the two schema versions.
//
// There is no version 2. A 128-byte layout carrying the widened Symbol without
// Source ID was specified upstream and superseded before any publisher emitted
// it, so version 2 is rejected here rather than decoded.
const (
	instDefBodyLenV1 = 76
	instDefBodyLenV3 = 126
)

// ParseInstrumentDefinition decodes an InstrumentDefinition body using the
// layout for the frame's schema version.
//
// The body length cross-checks the declared version. They can only disagree if a
// publisher bumped the header without the payload or the reverse, and the
// mismatch must be caught here: decoding a v1 body under the v3 layout would
// read Source ID and Symbol across 66 bytes of adjacent fields and yield a
// plausible-looking instrument rather than an error.
func ParseInstrumentDefinition(buf []byte, schemaVersion uint8) (InstrumentDefinitionBody, error) {
	var symStart, symEnd int
	var sourceID uint16
	switch schemaVersion {
	case mbpSchemaVersionV1:
		if len(buf) != instDefBodyLenV1 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 1 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV1, len(buf))
		}
		// v1 carries no Source ID; sourceID stays 0 (registry Unknown).
		symStart, symEnd = 4, 20
	case mbpSchemaVersionV3:
		if len(buf) != instDefBodyLenV3 {
			return InstrumentDefinitionBody{}, fmt.Errorf("%w: expected %d bytes for schema version 3 instrument_definition body, got %d",
				errTruncated, instDefBodyLenV3, len(buf))
		}
		sourceID = binary.LittleEndian.Uint16(buf[4:6])
		symStart, symEnd = 6, 70
	default:
		return InstrumentDefinitionBody{}, fmt.Errorf("%w: %d", errSchemaVersion, schemaVersion)
	}

	// Every field after Symbol is at a fixed offset from the end of Symbol, which
	// is what makes one body of code serve both layouts.
	o := symEnd
	return InstrumentDefinitionBody{
		InstrumentID:  binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:      sourceID,
		Symbol:        fixedString(buf[symStart:symEnd]),
		Leg1:          fixedString(buf[o : o+8]),
		Leg2:          fixedString(buf[o+8 : o+16]),
		AssetClass:    buf[o+16],
		PriceExponent: int8(buf[o+17]),
		QtyExponent:   int8(buf[o+18]),
		MarketModel:   buf[o+19],
		TickSizeRaw:   int64(binary.LittleEndian.Uint64(buf[o+20 : o+28])),
		LotSizeRaw:    binary.LittleEndian.Uint64(buf[o+28 : o+36]),
		ContractValue: binary.LittleEndian.Uint64(buf[o+36 : o+44]),
		Expiry:        readTSNs(buf[o+44 : o+52]),
		SettleType:    buf[o+52],
		PriceBound:    buf[o+53],
		ManifestSeq:   binary.LittleEndian.Uint16(buf[o+54 : o+56]),
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

// u16Unavailable is the shared sentinel for Order Count and Level Index. It
// means "not provided, or beyond what this field can express", and saturates
// rather than wrapping. It MUST NOT be read as a magnitude: it is neither a
// count nor a rank of 65535.
const u16Unavailable uint16 = 0xFFFF

// LiquidationBody is the 44-byte body of a Liquidation message. Byte-identical
// to the top-of-book feed's 0x08, though no other parser in this repo decodes it.
// Annotates a forced Trade, keyed on Trade ID, in the same frame as that Trade.
type LiquidationBody struct {
	InstrumentID   uint32
	SourceID       uint16
	Flags          uint8 // bit 0: liquidated side (0=long, 1=short); bit 1: ADL
	Method         uint8 // 0=market, 1=backstop, 0xFF=unknown
	TradeID        uint64
	MarkPriceRaw   int64
	LiquidatedUser [20]byte
}

// ParseLiquidation decodes a Liquidation body. buf must be exactly 44 bytes.
func ParseLiquidation(buf []byte) (LiquidationBody, error) {
	if len(buf) != 44 {
		return LiquidationBody{}, fmt.Errorf("%w: expected 44 bytes for liquidation body, got %d", errTruncated, len(buf))
	}
	b := LiquidationBody{
		InstrumentID: binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:     binary.LittleEndian.Uint16(buf[4:6]),
		Flags:        buf[6],
		Method:       buf[7],
		TradeID:      binary.LittleEndian.Uint64(buf[8:16]),
		MarkPriceRaw: int64(binary.LittleEndian.Uint64(buf[16:24])),
	}
	copy(b.LiquidatedUser[:], buf[24:44])
	return b, nil
}

// SnapshotBeginBody is the 36-byte body of a SnapshotBegin message.
//
// Bytes 0-31 are byte-for-byte the market-by-order feed's 32-byte body, with
// Total Orders reading as Total Levels. Depth Bound is appended at offset 32.
// That prefix-superset rule exists so a market-by-order decoder can read a
// market-by-price frame; it does not license this decoder to accept a 32-byte
// body, so the length check is exact.
type SnapshotBeginBody struct {
	InstrumentID      uint32
	AnchorSeq         uint64
	TotalLevels       uint32
	SnapshotID        uint32
	LastInstrumentSeq uint32
	Timestamp         time.Time
	DepthBound        uint32 // 0 = complete book; N = bounded at N levels per side
}

// ParseSnapshotBegin decodes a SnapshotBegin body. buf must be exactly 36 bytes.
func ParseSnapshotBegin(buf []byte) (SnapshotBeginBody, error) {
	if len(buf) != 36 {
		return SnapshotBeginBody{}, fmt.Errorf("%w: expected 36 bytes for snapshot_begin body, got %d", errTruncated, len(buf))
	}
	return SnapshotBeginBody{
		InstrumentID:      binary.LittleEndian.Uint32(buf[0:4]),
		AnchorSeq:         binary.LittleEndian.Uint64(buf[4:12]),
		TotalLevels:       binary.LittleEndian.Uint32(buf[12:16]),
		SnapshotID:        binary.LittleEndian.Uint32(buf[16:20]),
		LastInstrumentSeq: binary.LittleEndian.Uint32(buf[20:24]),
		Timestamp:         readTSNs(buf[24:32]),
		DepthBound:        binary.LittleEndian.Uint32(buf[32:36]),
	}, nil
}

// LevelUpdateBody is the 44-byte body of a LevelUpdate message — the core
// message of this feed. Quantity is the ABSOLUTE aggregate resting quantity at
// the price after the change, never a delta; 0 removes the level.
type LevelUpdateBody struct {
	InstrumentID     uint32
	SourceID         uint16
	Side             uint8 // 0=bid, 1=ask
	Action           uint8 // informational only; MUST NOT gate the apply
	PerInstrumentSeq uint32
	PriceRaw         int64  // the level's key
	QtyRaw           uint64 // absolute; 0 = delete
	Timestamp        time.Time
	OrderCount       uint16 // u16Unavailable = absent
	LevelIndex       uint16 // informational only; u16Unavailable = absent
	UpdateReason     uint8
	LevelFlags       uint8
}

// ParseLevelUpdate decodes a LevelUpdate body. buf must be exactly 44 bytes.
func ParseLevelUpdate(buf []byte) (LevelUpdateBody, error) {
	if len(buf) != 44 {
		return LevelUpdateBody{}, fmt.Errorf("%w: expected 44 bytes for level_update body, got %d", errTruncated, len(buf))
	}
	return LevelUpdateBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		Side:             buf[6],
		Action:           buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		PriceRaw:         int64(binary.LittleEndian.Uint64(buf[12:20])),
		QtyRaw:           binary.LittleEndian.Uint64(buf[20:28]),
		Timestamp:        readTSNs(buf[28:36]),
		OrderCount:       binary.LittleEndian.Uint16(buf[36:38]),
		LevelIndex:       binary.LittleEndian.Uint16(buf[38:40]),
		UpdateReason:     buf[40],
		LevelFlags:       buf[41],
		// bytes 42-43 are reserved padding
	}, nil
}

// BookClearBody is the 32-byte body of a BookClear message. Bulk removal of
// levels. Not a resynchronization signal: a subscriber that applies one stays
// ready.
type BookClearBody struct {
	InstrumentID     uint32
	SourceID         uint16
	ClearSide        uint8 // 0=bid, 1=ask, 2=both
	Scope            uint8 // 0=entire side, 1=from FromPrice outward
	PerInstrumentSeq uint32
	FromPriceRaw     int64 // inclusive bound when Scope=1
	Timestamp        time.Time
	ClearReason      uint8
}

// ParseBookClear decodes a BookClear body. buf must be exactly 32 bytes.
//
// Scope=1 with ClearSide=2 is malformed — one price cannot bound both sides —
// and is rejected so the caller discards and counts it.
func ParseBookClear(buf []byte) (BookClearBody, error) {
	if len(buf) != 32 {
		return BookClearBody{}, fmt.Errorf("%w: expected 32 bytes for book_clear body, got %d", errTruncated, len(buf))
	}
	b := BookClearBody{
		InstrumentID:     binary.LittleEndian.Uint32(buf[0:4]),
		SourceID:         binary.LittleEndian.Uint16(buf[4:6]),
		ClearSide:        buf[6],
		Scope:            buf[7],
		PerInstrumentSeq: binary.LittleEndian.Uint32(buf[8:12]),
		FromPriceRaw:     int64(binary.LittleEndian.Uint64(buf[12:20])),
		Timestamp:        readTSNs(buf[20:28]),
		ClearReason:      buf[28],
		// bytes 29-31 are reserved padding
	}
	if b.Scope == 1 && b.ClearSide == 2 {
		return b, fmt.Errorf("%w: book_clear scope=1 with clear_side=both", errMalformedBody)
	}
	return b, nil
}

// SnapshotLevelBody is the 28-byte body of a SnapshotLevel message. The
// Instrument ID is implied by the containing SnapshotBegin and is not repeated.
// Quantity is non-zero by rule; an empty level is represented by its absence.
type SnapshotLevelBody struct {
	SnapshotID uint32
	PriceRaw   int64
	QtyRaw     uint64
	OrderCount uint16 // u16Unavailable = absent
	Side       uint8  // 0=bid, 1=ask
	LevelFlags uint8
}

// ParseSnapshotLevel decodes a SnapshotLevel body. buf must be exactly 28 bytes.
func ParseSnapshotLevel(buf []byte) (SnapshotLevelBody, error) {
	if len(buf) != 28 {
		return SnapshotLevelBody{}, fmt.Errorf("%w: expected 28 bytes for snapshot_level body, got %d", errTruncated, len(buf))
	}
	return SnapshotLevelBody{
		SnapshotID: binary.LittleEndian.Uint32(buf[0:4]),
		PriceRaw:   int64(binary.LittleEndian.Uint64(buf[4:12])),
		QtyRaw:     binary.LittleEndian.Uint64(buf[12:20]),
		OrderCount: binary.LittleEndian.Uint16(buf[20:22]),
		Side:       buf[22],
		LevelFlags: buf[23],
		// bytes 24-27 are reserved padding
	}, nil
}
