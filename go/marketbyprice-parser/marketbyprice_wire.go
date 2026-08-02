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
	"time"
)

const (
	mbpMagic          uint16 = 0x4442
	mbpSchemaVersion  uint8  = 1
	frameHeaderSize          = 24
	messageHeaderSize        = 4
	maxFrameSize             = 1232
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
