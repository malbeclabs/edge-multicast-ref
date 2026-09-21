package sink

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// testRecord stands in for a feed's Record. No sink in this package reads a
// field of the record type, so any JSON-encodable struct exercises the same
// code a parser's Record does.
type testRecord struct {
	Type string `json:"type"`
	Seq  uint64 `json:"seq"`
}

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

// countingMetrics records what the socket sink reports, standing in for a
// parser's prometheus counters. A nil receiver is tolerated the same way a
// parser's optional *Metrics is, so the same double covers both the
// nil-interface and typed-nil cases.
type countingMetrics struct {
	mu      sync.Mutex
	clients int
	drops   map[string]int
	sent    int
}

func newCountingMetrics() *countingMetrics {
	return &countingMetrics{drops: make(map[string]int)}
}

func (m *countingMetrics) SetSocketClients(n int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients = n
}

func (m *countingMetrics) AddSocketClientDrops(reason string, n int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops[reason] += n
}

func (m *countingMetrics) AddSocketRecordsSent(n int) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent += n
}

func (m *countingMetrics) snapshot() (clients int, drops map[string]int, sent int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make(map[string]int, len(m.drops))
	for k, v := range m.drops {
		copied[k] = v
	}
	return m.clients, copied, m.sent
}

// TestDropReasonLabelsAreStable pins the two strings, not the constants: they
// reach /metrics as the reason label on every feed's
// socket_client_drops_total, so an operator's alert keys on the text. Changing
// one is a dashboard break rather than a rename.
func TestDropReasonLabelsAreStable(t *testing.T) {
	if DropReasonQueueFull != "queue_full" {
		t.Errorf("queue-full label is %q", DropReasonQueueFull)
	}
	if DropReasonWriteError != "write_error" {
		t.Errorf("write-error label is %q", DropReasonWriteError)
	}
}
