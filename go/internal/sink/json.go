package sink

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// JSONFile writes records as newline-delimited JSON (JSONL) to a file.
type JSONFile[R any] struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
}

// NewJSONFile opens (or creates) the file at path for JSONL output.
func NewJSONFile[R any](path string) (*JSONFile[R], error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("opening output file: %w", err)
	}
	enc := json.NewEncoder(f)
	enc.SetEscapeHTML(false)
	return &JSONFile[R]{file: f, enc: enc}, nil
}

func (s *JSONFile[R]) Write(records []R) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range records {
		if err := s.enc.Encode(&records[i]); err != nil {
			return fmt.Errorf("encoding record: %w", err)
		}
	}
	return nil
}

func (s *JSONFile[R]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file.Close()
}
