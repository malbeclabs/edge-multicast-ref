package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func encodeRecord(r Record) string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%s\n", b)
}

// capturingDispatcher records what the reader dispatched and how many times the
// socket dropped.
type capturingDispatcher struct {
	mu          sync.Mutex
	records     []Record
	disconnects int
}

func (d *capturingDispatcher) Dispatch(r Record) {
	d.mu.Lock()
	d.records = append(d.records, r)
	d.mu.Unlock()
}

func (d *capturingDispatcher) OnDisconnect() {
	d.mu.Lock()
	d.disconnects++
	d.mu.Unlock()
}

func (d *capturingDispatcher) snapshot() []Record {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Record, len(d.records))
	copy(out, d.records)
	return out
}

func (d *capturingDispatcher) disconnectCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.disconnects
}

// The socket reader was copied verbatim from marketbyorder-bot but its
// behavioural tests were not, leaving the read and reconnect paths uncovered
// here. These two are ported from that sibling.
func TestBot_ReadsRecordsFromSocket(t *testing.T) {
	sockPath := tempSock(t)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	var serverWG sync.WaitGroup
	serverWG.Add(1)
	go func() {
		defer serverWG.Done()
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for i := 0; i < 3; i++ {
			conn.Write([]byte(encodeRecord(Record{
				Type:           "heartbeat",
				Timestamp:      time.Unix(1700000000, 0),
				Port:           "mktdata",
				SequenceNumber: uint64(i),
			})))
		}
		time.Sleep(200 * time.Millisecond)
	}()

	disp := &capturingDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewBot(sockPath, disp, NewMetrics("test", "test")).Run(ctx)

	waitFor(t, 3*time.Second, func() bool { return len(disp.snapshot()) == 3 })
	cancel()
	serverWG.Wait()
}

func TestBot_ReconnectsOnDisconnect(t *testing.T) {
	sockPath := tempSock(t)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	// Accept twice: serve one record, hang up, then serve another on the
	// reconnect. The bot's first backoff is 250ms, so this stays quick.
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			conn.Write([]byte(encodeRecord(Record{
				Type: "heartbeat", Port: "mktdata", SequenceNumber: uint64(i),
			})))
			conn.Close()
		}
	}()

	disp := &capturingDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewBot(sockPath, disp, NewMetrics("test", "test")).Run(ctx)

	waitFor(t, 5*time.Second, func() bool { return len(disp.snapshot()) >= 2 })
	if got := disp.disconnectCount(); got < 1 {
		t.Errorf("the dispatcher must be told the socket dropped: got %d", got)
	}
}

// A drop must reach the dispatcher even when nothing is read, so state that
// spans records is never carried across a break.
func TestBot_NotifiesDispatcherOnDisconnect(t *testing.T) {
	sockPath := tempSock(t)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.Close() // immediate hang-up, no records
	}()

	disp := &capturingDispatcher{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go NewBot(sockPath, disp, NewMetrics("test", "test")).Run(ctx)

	waitFor(t, 3*time.Second, func() bool { return disp.disconnectCount() >= 1 })
}

// tempSock returns a short Unix socket path. t.TempDir() embeds the test name,
// and the sun_path limit is 104 bytes on darwin, so longer test names fail to
// bind with a bare "invalid argument".
func tempSock(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "mbp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

func waitFor(t *testing.T, limit time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", limit)
}

func gatheredNames(t *testing.T, m *Metrics) []string {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}

func TestMetricsNamespace(t *testing.T) {
	m := NewMetrics("test", "abc123")
	// build_info is set inside NewMetrics and uptime_seconds is a GaugeFunc, so
	// both are gathered without any observation. Every *Vec reports no metric
	// family until a label set is observed, which is why they are not probed here.
	names := gatheredNames(t, m)
	var sawBuildInfo bool
	for _, n := range names {
		if n == "dz_mbp_bot_build_info" {
			sawBuildInfo = true
		}
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under a sibling feed namespace", n)
		}
	}
	if !sawBuildInfo {
		t.Errorf("missing dz_mbp_bot_build_info in %v", names)
	}
}
