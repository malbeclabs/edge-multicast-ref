package shred

import (
	"encoding/binary"
	"testing"
)

func TestParseShredEmpty(t *testing.T) {
	_, err := ParseShred([]byte{})
	if err == nil {
		t.Fatal("expected error for empty payload")
	}
}

func TestParseShredTooShort(t *testing.T) {
	payload := make([]byte, 82)
	_, err := ParseShred(payload)
	if err == nil {
		t.Fatal("expected error for 82-byte payload")
	}
}

func TestParseShredDataShred(t *testing.T) {
	payload := make([]byte, 128)

	// Fill signature with known pattern.
	for i := 0; i < 64; i++ {
		payload[i] = byte(i)
	}

	// Variant byte: data shred = type 1 => (1 << 5) = 0x20.
	payload[64] = 0xA5 // LegacyData

	// Slot = 42.
	binary.LittleEndian.PutUint64(payload[65:], 42)
	// Index = 7.
	binary.LittleEndian.PutUint32(payload[73:], 7)
	// Version = 100.
	binary.LittleEndian.PutUint16(payload[77:], 100)
	// FEC set index = 3.
	binary.LittleEndian.PutUint32(payload[79:], 3)

	s, err := ParseShred(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Slot != 42 {
		t.Errorf("slot = %d, want 42", s.Slot)
	}
	if s.Index != 7 {
		t.Errorf("index = %d, want 7", s.Index)
	}
	if !s.IsData {
		t.Error("expected IsData=true")
	}
	if s.FECSetIndex != 3 {
		t.Errorf("fec set index = %d, want 3", s.FECSetIndex)
	}
	if s.Version != 100 {
		t.Errorf("version = %d, want 100", s.Version)
	}
}

func TestParseShredCodingShred(t *testing.T) {
	payload := make([]byte, 128)

	// Variant byte: coding shred = type 2 => (2 << 5) = 0x40.
	payload[64] = 0x40

	binary.LittleEndian.PutUint64(payload[65:], 99)
	binary.LittleEndian.PutUint32(payload[73:], 12)
	binary.LittleEndian.PutUint16(payload[77:], 5)
	binary.LittleEndian.PutUint32(payload[79:], 8)

	s, err := ParseShred(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.IsData {
		t.Error("expected IsData=false for coding shred")
	}
	if s.Slot != 99 {
		t.Errorf("slot = %d, want 99", s.Slot)
	}
	if s.Index != 12 {
		t.Errorf("index = %d, want 12", s.Index)
	}
}

func TestParseShredMerkleData(t *testing.T) {
	payload := make([]byte, 128)
	// MerkleData chained, proof_size=6: 0x90 | 0x06 = 0x96
	payload[64] = 0x96
	binary.LittleEndian.PutUint64(payload[65:], 409310471)
	binary.LittleEndian.PutUint32(payload[73:], 320)

	s, err := ParseShred(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !s.IsData {
		t.Error("expected IsData=true for MerkleData")
	}
	if s.Slot != 409310471 {
		t.Errorf("slot = %d, want 409310471", s.Slot)
	}
}

func TestParseShredVariousSizes(t *testing.T) {
	sizes := []int{64, 82, 83, 128, 1228, 1272}
	for _, size := range sizes {
		payload := make([]byte, size)
		if size >= MinShredSize {
			// Set a valid variant so we don't get variant error.
			payload[64] = 0xA5 // LegacyData
		}
		// Should not panic regardless of outcome.
		_, _ = ParseShred(payload)
	}
}

func TestParseShredSignatureExtraction(t *testing.T) {
	payload := make([]byte, 128)

	// Fill signature with distinct bytes.
	var expectedSig [64]byte
	for i := 0; i < 64; i++ {
		payload[i] = byte(0xAA ^ byte(i))
		expectedSig[i] = payload[i]
	}

	// Valid data variant.
	payload[64] = 0xA5 // LegacyData

	s, err := ParseShred(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s.Signature != expectedSig {
		t.Error("signature does not match input bytes")
	}
}
