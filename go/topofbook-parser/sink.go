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
	// Format is the output encoding: "json" or "csv".
	Format string

	// Path is the output destination. A file path for file output,
	// or "unix:///path/to/sock" for a Unix domain socket.
	Path string

	// Metrics is optional; when non-nil, the socket sink tracks
	// connected-client and drop counters.
	Metrics *metrics
}

// NewSink creates an OutputSink from the given configuration.
//
// The format decides the per-client encoder a socket sink serves with: JSONL
// comes from the shared package, and the CSV quote/trade layout is this feed's
// own, so it is supplied from here.
//
// Path formats:
//   - "/path/to/file"          → file output
//   - "unix:///path/to/sock"   → Unix domain socket (broadcast to all connected clients)
func NewSink(cfg SinkConfig) (OutputSink, error) {
	isSocket := strings.HasPrefix(cfg.Path, "unix://")
	sockPath := strings.TrimPrefix(cfg.Path, "unix://")

	switch cfg.Format {
	case "json":
		if isSocket {
			return sink.NewSocket(sockPath, sink.NewJSONConnWriter[Record], cfg.Metrics.socketMetrics())
		}
		return NewJSONFileSink(cfg.Path)
	case "csv":
		if isSocket {
			return sink.NewSocket(sockPath, newCSVConnWriter, cfg.Metrics.socketMetrics())
		}
		return NewCSVFileSink(cfg.Path)
	default:
		return nil, fmt.Errorf("unknown output format: %q", cfg.Format)
	}
}
