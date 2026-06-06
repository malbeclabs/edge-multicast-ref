package main

import (
	"bufio"
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestSocketSink_JSONBroadcast(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")

	sink, err := NewSocketSink("json", sockPath, nil)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer sink.Close()

	// Connect two clients.
	conn1, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting client 1: %v", err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting client 2: %v", err)
	}
	defer conn2.Close()

	// Give the accept loop time to register clients.
	time.Sleep(50 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{
		{
			Type:           "quote",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 100,
			InstrumentID:   42,
			Fields: map[string]any{
				"bid_price": 67432.5,
			},
		},
	}

	if err := sink.Write(records); err != nil {
		t.Fatalf("error writing to socket sink: %v", err)
	}

	// Both clients should receive the same JSONL record.
	for i, conn := range []net.Conn{conn1, conn2} {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			t.Fatalf("client %d: no data received", i+1)
		}
		var r Record
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("client %d: error decoding JSON: %v", i+1, err)
		}
		if r.InstrumentID != 42 {
			t.Errorf("client %d: expected instrument_id 42, got %d", i+1, r.InstrumentID)
		}
	}
}


func TestSocketSink_DropsDisconnectedClient(t *testing.T) {
	sockPath := filepath.Join(t.TempDir(), "test.sock")

	sink, err := NewSocketSink("json", sockPath, nil)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer sink.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Close the client before writing.
	conn.Close()

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{
		{Type: "heartbeat", Timestamp: ts, ChannelID: 1, SequenceNumber: 1},
	}

	// Write should succeed (disconnected client is dropped, not an error).
	if err := sink.Write(records); err != nil {
		t.Fatalf("expected no error after client disconnect, got: %v", err)
	}

	// Verify client was removed.
	sink.mu.Lock()
	count := len(sink.clients)
	sink.mu.Unlock()
	if count != 0 {
		t.Errorf("expected 0 clients after disconnect, got %d", count)
	}
}
