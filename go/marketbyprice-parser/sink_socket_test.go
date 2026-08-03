package main

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"
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

// readCounterVec reads the current value of a CounterVec label combination.
func readCounterVec(t *testing.T, cv *prometheus.CounterVec, lvs ...string) float64 {
	t.Helper()
	metric, err := cv.GetMetricWithLabelValues(lvs...)
	if err != nil {
		t.Fatalf("metric lookup failed: %v", err)
	}
	m := &dto.Metric{}
	if err := metric.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Counter.GetValue()
}

func TestSocketSink_JSONBroadcast(t *testing.T) {
	sockPath := shortTempSock(t)

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
				"qty_raw":            uint64(100),
				"timestamp":          ts,
				"update_reason":      "new_order",
				"level_flags":        uint8(0),
				"implied":            false,
				"amm_synthetic":      false,
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
	sockPath := shortTempSock(t)

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

	// Verify client was removed. Removal is async (per-client writer
	// goroutine detects the closed conn on its next flush), so poll.
	deadline := time.Now().Add(time.Second)
	var count int
	for time.Now().Before(deadline) {
		sink.mu.Lock()
		count = len(sink.clients)
		sink.mu.Unlock()
		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count != 0 {
		t.Errorf("expected 0 clients after disconnect, got %d", count)
	}
}

// TestSocketSink_BackPressure verifies that Write never blocks the caller when
// a client's queue is full, that the queue_full drop counter is incremented,
// and that a healthy second client continues to receive records.
//
// We inject a clientWriter whose channel is pre-filled (capacity 0 — i.e.
// unbuffered and already consumed by no one) alongside a real connected client.
// This isolates the non-blocking select in Write() from OS socket buffer sizes.
func TestSocketSink_BackPressure(t *testing.T) {
	sockPath := shortTempSock(t)

	m := NewMetrics("test", "abc123")
	sink, err := NewSocketSink("json", sockPath, m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer sink.Close()

	// goodConn: a real client that reads normally.
	goodConn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting good client: %v", err)
	}
	defer goodConn.Close()

	// Give the accept loop time to register the good client.
	time.Sleep(50 * time.Millisecond)

	// Inject a fake clientWriter with a zero-capacity (unbuffered) channel.
	// Write's non-blocking select will immediately take the default branch,
	// recording a queue_full drop — without involving any OS socket buffer.
	fakeConn, fakeServer := net.Pipe()
	defer fakeConn.Close()
	defer fakeServer.Close()

	fakeCW := &clientWriter{
		conn: fakeConn,
		ch:   make(chan []Record), // unbuffered: always full from Write's perspective
		done: make(chan struct{}),
	}
	// Mark done closed so Close() doesn't hang waiting for this goroutine.
	close(fakeCW.done)

	sink.mu.Lock()
	sink.clients[fakeConn] = fakeCW
	sink.mu.Unlock()

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	batch := []Record{
		{
			Type:           "level_update",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 1,
			InstrumentID:   7,
			Fields: map[string]any{
				"source_id":          uint16(1),
				"side":               "ask",
				"action":             "change",
				"per_instrument_seq": uint32(100),
				"price_raw":          int64(6743300),
				"qty_raw":            uint64(50),
				"timestamp":          ts,
				"update_reason":      "amend",
				"level_flags":        uint8(0),
				"implied":            false,
				"amm_synthetic":      false,
			},
		},
	}

	// A single Write is enough: the fake client's unbuffered channel causes an
	// immediate queue_full drop. Multiple writes confirm Write never blocks.
	const writes = 5
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < writes; i++ {
			if err := sink.Write(batch); err != nil {
				t.Errorf("Write(%d) returned error: %v", i, err)
				return
			}
		}
	}()

	select {
	case <-done:
		// good — all writes returned promptly
	case <-time.After(2 * time.Second):
		t.Fatal("Write blocked: did not return within 2s — back-pressure fix missing")
	}

	// At least one queue_full drop must have been recorded.
	drops := readCounterVec(t, m.SocketClientDrops, "queue_full")
	if drops == 0 {
		t.Error("expected at least one queue_full drop, got 0")
	}

	// Good client should still receive records — drain what it has.
	goodConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	scanner := bufio.NewScanner(goodConn)
	received := 0
	for scanner.Scan() {
		received++
		if received >= writes {
			break
		}
	}
	if received == 0 {
		t.Error("good client received no records despite not blocking")
	}
}

// TestSocketSink_ConcurrentWriteClose verifies that concurrent Write() calls
// racing against Close() never panic (e.g. send-on-closed-channel) and that
// the sink shuts down cleanly with no goroutine leaks. Run under -race to
// detect data races.
func TestSocketSink_ConcurrentWriteClose(t *testing.T) {
	sockPath := shortTempSock(t)

	sink, err := NewSocketSink("json", sockPath, nil)
	if err != nil {
		t.Fatalf("creating socket sink: %v", err)
	}

	// Connect a client so Write has real channels to send on.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("connecting client: %v", err)
	}
	defer conn.Close()

	// Give the accept loop time to register the client.
	time.Sleep(20 * time.Millisecond)

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	batch := []Record{
		{
			Type:           "level_update",
			Timestamp:      ts,
			ChannelID:      1,
			SequenceNumber: 1,
			InstrumentID:   99,
			Fields: map[string]any{
				"source_id":          uint16(2),
				"side":               "bid",
				"action":             "delete",
				"per_instrument_seq": uint32(500),
				"price_raw":          int64(6744000),
				"qty_raw":            uint64(0),
				"timestamp":          ts,
				"update_reason":      "cancel",
				"level_flags":        uint8(0),
				"implied":            false,
				"amm_synthetic":      false,
			},
		},
	}

	const writers = 8
	ready := make(chan struct{})
	var wg sync.WaitGroup

	// Launch writers that all start at the same time as Close.
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			for j := 0; j < 200; j++ {
				sink.Write(batch) //nolint:errcheck
			}
		}()
	}

	// Close races directly against the writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ready
		sink.Close() //nolint:errcheck
	}()

	close(ready) // start all goroutines simultaneously
	wg.Wait()

	// Double-Close must also be safe (idempotent).
	sink.Close() //nolint:errcheck
}
