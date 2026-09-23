package main

import (
	"fmt"
	"strings"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/sink"
)

// OutputSink writes decoded records to a destination.
//
// The transport lives in go/internal/sink and is shared with the other
// parsers; Record is this module's own, and the sink is generic over it.
type OutputSink = sink.Sink[Record]

// JSONFileSink writes records as newline-delimited JSON (JSONL) to a file.
type JSONFileSink = sink.JSONFile[Record]

// NewJSONFileSink opens (or creates) the file at path for JSONL output.
func NewJSONFileSink(path string) (*JSONFileSink, error) {
	return sink.NewJSONFile[Record](path)
}

// SinkConfig describes the desired output format and destination.
type SinkConfig struct {
	// Format is the output encoding: "json".
	Format string

	// Path is the output destination. A file path for file output,
	// or "unix:///path/to/sock" for a Unix domain socket.
	Path string

	// Metrics is optional; when non-nil, the socket sink tracks
	// connected-client and drop counters.
	Metrics *Metrics
}

// NewSink creates an OutputSink from the given configuration.
//
// Path formats:
//   - "/path/to/file"          → file output
//   - "unix:///path/to/sock"   → Unix domain socket (broadcast to all connected clients)
func NewSink(cfg SinkConfig) (OutputSink, error) {
	isSocket := strings.HasPrefix(cfg.Path, "unix://")

	switch cfg.Format {
	case "json":
		if isSocket {
			return sink.NewSocket(strings.TrimPrefix(cfg.Path, "unix://"), sink.NewJSONConnWriter[Record], cfg.Metrics.socketMetrics())
		}
		return NewJSONFileSink(cfg.Path)
	default:
		return nil, fmt.Errorf("unsupported format: %q (marketbyorder supports json only)", cfg.Format)
	}
}
