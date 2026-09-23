package sink

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJSONFile_Write(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.jsonl")

	s, err := NewJSONFile[testRecord](path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}

	records := []testRecord{
		{Type: "quote", Seq: 100},
		{Type: "trade", Seq: 101},
	}

	if err := s.Write(records); err != nil {
		t.Fatalf("error writing records: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("error opening output file: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	var decoded []testRecord
	for scanner.Scan() {
		var r testRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("error decoding line: %v", err)
		}
		decoded = append(decoded, r)
	}

	if len(decoded) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(decoded))
	}
	if decoded[0].Type != "quote" || decoded[0].Seq != 100 {
		t.Errorf("first record round-tripped as %+v", decoded[0])
	}
	if decoded[1].Type != "trade" || decoded[1].Seq != 101 {
		t.Errorf("second record round-tripped as %+v", decoded[1])
	}
}

func TestJSONFile_Append(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "output.jsonl")

	// Write first batch.
	s1, err := NewJSONFile[testRecord](path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	if err := s1.Write([]testRecord{{Type: "heartbeat", Seq: 1}}); err != nil {
		t.Fatalf("error writing first batch: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("error closing first sink: %v", err)
	}

	// Write second batch (should append).
	s2, err := NewJSONFile[testRecord](path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	if err := s2.Write([]testRecord{{Type: "heartbeat", Seq: 2}}); err != nil {
		t.Fatalf("error writing second batch: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("error closing second sink: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("error opening output file: %v", err)
	}
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

// TestJSONFile_DoesNotEscapeHTML pins SetEscapeHTML(false): a symbol or a
// string field carrying &, < or > reaches a downstream consumer as itself
// rather than as a & escape.
func TestJSONFile_DoesNotEscapeHTML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.jsonl")

	s, err := NewJSONFile[testRecord](path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	if err := s.Write([]testRecord{{Type: "a&b<c>d", Seq: 1}}); err != nil {
		t.Fatalf("error writing record: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("error reading output file: %v", err)
	}
	if !strings.Contains(string(raw), `"a&b<c>d"`) {
		t.Errorf("expected the unescaped value in the output, got %q", string(raw))
	}
}

// TestJSONFile_WriteErrorIsReported closes the underlying file behind the
// sink's back, so Write reaches the encoder's error path.
func TestJSONFile_WriteErrorIsReported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output.jsonl")

	s, err := NewJSONFile[testRecord](path)
	if err != nil {
		t.Fatalf("error creating sink: %v", err)
	}
	if err := s.file.Close(); err != nil {
		t.Fatalf("error closing the file: %v", err)
	}
	if err := s.Write([]testRecord{{Type: "quote", Seq: 1}}); err == nil {
		t.Error("expected an error writing to a closed file, got nil")
	}
}
