package main

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJSONFileSink_Write(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.jsonl")

	sink, err := NewJSONFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{
		{
			Type:           "level_update",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 100,
			InstrumentID:   42,
			Fields: map[string]any{
				"source_id":          uint16(1),
				"side":               "bid",
				"action":             "new",
				"per_instrument_seq": uint32(1000),
				"price_raw":          int64(6743250),
				"qty_raw":            uint32(100),
				"timestamp":          ts,
				"update_reason":      "new_order",
				"level_flags":        uint8(0),
				"implied":            false,
				"amm_synthetic":      false,
			},
		},
		{
			Type:           "book_clear",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 101,
			InstrumentID:   42,
			Fields: map[string]any{
				"source_id":          uint16(1),
				"clear_side":         "bid",
				"scope":              uint8(0),
				"per_instrument_seq": uint32(1001),
				"timestamp":          ts,
				"clear_reason":       "halt",
			},
		},
	}

	if err := sink.Write(records); err != nil {
		t.Fatalf("error writing records: %v", err)
	}

	if err := sink.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	// Read back and verify.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("error opening output file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var decoded []Record
	for scanner.Scan() {
		var r Record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("error decoding line: %v", err)
		}
		decoded = append(decoded, r)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(decoded))
	}
	if decoded[0].Type != "level_update" {
		t.Errorf("expected first record type level_update, got %s", decoded[0].Type)
	}
	if decoded[0].InstrumentID != 42 {
		t.Errorf("expected instrument_id 42, got %d", decoded[0].InstrumentID)
	}
	if decoded[1].Type != "book_clear" {
		t.Errorf("expected second record type book_clear, got %s", decoded[1].Type)
	}
}

func TestJSONFileSink_Append(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.jsonl")

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)

	// Write first batch.
	sink1, err := NewJSONFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	sink1.Write([]Record{{Type: "heartbeat", Timestamp: ts, ChannelID: 1, SequenceNumber: 1}})
	sink1.Close()

	// Write second batch (should append).
	sink2, err := NewJSONFileSink(path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	sink2.Write([]Record{{Type: "heartbeat", Timestamp: ts, ChannelID: 1, SequenceNumber: 2}})
	sink2.Close()

	// Count lines.
	f, _ := os.Open(path)
	defer f.Close()
	scanner := bufio.NewScanner(f)
	count := 0
	for scanner.Scan() {
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 lines after append, got %d", count)
	}
}
