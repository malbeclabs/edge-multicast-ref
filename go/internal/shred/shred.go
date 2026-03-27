// Package shred implements a native Go parser for Solana shred binary payloads.
package shred

import (
	"encoding/binary"
	"fmt"
)

const (
	// SignatureSize is the length of an Ed25519 signature.
	SignatureSize = 64
	// MinShredSize is the minimum valid shred payload size (common header).
	MinShredSize = 83

	shredVariantOffset = 64
	slotOffset         = 65
	indexOffset         = 73
	versionOffset      = 77
	fecSetIndexOffset  = 79
)

// ParsedShred holds the decoded fields from a Solana shred common header.
type ParsedShred struct {
	Slot        uint64
	Index       uint32
	IsData      bool
	FECSetIndex uint32
	Version     uint16
	Signature   [64]byte
}

// ParseShred decodes the common header from a raw shred payload.
// Returns an error if the payload is too short or the shred variant is unknown.
func ParseShred(payload []byte) (*ParsedShred, error) {
	if len(payload) < MinShredSize {
		return nil, fmt.Errorf("shred payload too short: %d bytes (minimum %d)", len(payload), MinShredSize)
	}

	// Decode shred variant byte. Solana encoding:
	//   LegacyCode:  0x5A (0b0101_1010)
	//   LegacyData:  0xA5 (0b1010_0101)
	//   MerkleCode:  top nibble 0x40, 0x60, or 0x70 (proof_size in low 4 bits)
	//   MerkleData:  top nibble 0x80, 0x90, or 0xB0 (proof_size in low 4 bits)
	variant := payload[shredVariantOffset]
	var isData bool
	switch {
	case variant == 0xA5: // LegacyData
		isData = true
	case variant == 0x5A: // LegacyCode
		isData = false
	case variant&0xF0 == 0x80 || variant&0xF0 == 0x90 || variant&0xF0 == 0xB0: // MerkleData
		isData = true
	case variant&0xF0 == 0x40 || variant&0xF0 == 0x60 || variant&0xF0 == 0x70: // MerkleCode
		isData = false
	default:
		return nil, fmt.Errorf("unknown shred variant byte: 0x%02x", variant)
	}

	s := &ParsedShred{
		Slot:        binary.LittleEndian.Uint64(payload[slotOffset : slotOffset+8]),
		Index:       binary.LittleEndian.Uint32(payload[indexOffset : indexOffset+4]),
		Version:     binary.LittleEndian.Uint16(payload[versionOffset : versionOffset+2]),
		FECSetIndex: binary.LittleEndian.Uint32(payload[fecSetIndexOffset : fecSetIndexOffset+4]),
		IsData:      isData,
	}
	copy(s.Signature[:], payload[:SignatureSize])

	return s, nil
}
