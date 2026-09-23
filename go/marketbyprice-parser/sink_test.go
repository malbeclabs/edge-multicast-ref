package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// shortTempSock returns a Unix-domain socket path short enough to fit within the
// macOS sockaddr_un.sun_path limit (104 bytes). t.TempDir() embeds the test's
// function name, which for longer names pushes the socket path over that limit
// and makes bind fail with EINVAL ("invalid argument"). A minimal-prefix temp
// dir keeps the path short regardless of the test name.
func shortTempSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "s")
	if err != nil {
		t.Fatalf("creating temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func levelUpdateRecord(ts time.Time) Record {
	return Record{
		Type:           "level_update",
		Timestamp:      ts,
		ChannelID:      1,
		SequenceNumber: 100,
		InstrumentID:   42,
		Fields: map[string]any{
			"source_id":     uint16(1),
			"side":          "bid",
			"action":        "new",
			"price_raw":     int64(6743250),
			"qty_raw":       uint64(100),
			"update_reason": "new_order",
		},
	}
}

// TestNewSink_JSONOverSocket covers the route a running parser takes: a
// "unix://" path must produce a listening socket that broadcasts JSONL, not a
// file of that name. Without this, mistaking the socket branch for the file
// branch leaves every book-builder unable to connect and nothing but silence
// to show it.
func TestNewSink_JSONOverSocket(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSink(SinkConfig{Format: "json", Path: "unix://" + sockPath})
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	info, err := os.Stat(sockPath)
	if err != nil {
		t.Fatalf("no socket at %s: %v", sockPath, err)
	}
	if info.Mode().Type() != os.ModeSocket {
		t.Fatalf("%s is %v, not a socket", sockPath, info.Mode().Type())
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	// Give the accept loop time to register the client.
	time.Sleep(50 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	if err := s.Write([]Record{levelUpdateRecord(ts)}); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatal("no data received")
	}
	var r Record
	if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
		t.Fatalf("error decoding JSON: %v", err)
	}
	if r.Type != "level_update" || r.SequenceNumber != 100 {
		t.Errorf("received %+v", r)
	}
}

// TestNewSink_JSONToFile is the other half of the same branch: a plain path
// must produce a file and no socket.
func TestNewSink_JSONToFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "out.jsonl")

	s, err := NewSink(SinkConfig{Format: "json", Path: path})
	if err != nil {
		t.Fatalf("error creating file sink: %v", err)
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	if err := s.Write([]Record{levelUpdateRecord(ts)}); err != nil {
		t.Fatalf("error writing: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("no file at %s: %v", path, err)
	}
	if info.Mode().Type() != 0 {
		t.Errorf("%s is %v, not a regular file", path, info.Mode().Type())
	}
}

// TestNewSink_UnsupportedFormat keeps an unrecognised format an error rather
// than a sink that silently serves nothing.
func TestNewSink_UnsupportedFormat(t *testing.T) {
	for _, format := range []string{"csv", "parquet", ""} {
		if s, err := NewSink(SinkConfig{Format: format, Path: filepath.Join(t.TempDir(), "out")}); err == nil {
			s.Close()
			t.Errorf("format %q was accepted", format)
		}
	}
}
