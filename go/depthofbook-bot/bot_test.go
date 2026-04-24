package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func encodeRecord(r Record) string {
	b, _ := json.Marshal(r)
	return fmt.Sprintf("%s\n", b)
}

// stubMetrics returns a real Metrics instance for use in tests.
func stubMetrics() *Metrics {
	return NewMetrics("test", "test")
}

type capturingDispatcher struct {
	mu      sync.Mutex
	records []Record
}

func (d *capturingDispatcher) Dispatch(r Record) {
	d.mu.Lock()
	d.records = append(d.records, r)
	d.mu.Unlock()
}

func (d *capturingDispatcher) snapshot() []Record {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Record, len(d.records))
	copy(out, d.records)
	return out
}

func TestBot_ReadsRecordsFromSocket(t *testing.T) {
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")

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
		// Send 3 records.
		for i := 0; i < 3; i++ {
			rec := Record{
				Type:           "heartbeat",
				Timestamp:      time.Unix(1700000000, 0),
				ChannelID:      0,
				Port:           "mktdata",
				SequenceNumber: uint64(i),
			}
			conn.Write([]byte(encodeRecord(rec)))
		}
		// Wait briefly for the bot to consume them, then close.
		time.Sleep(200 * time.Millisecond)
	}()

	disp := &capturingDispatcher{}
	bot := NewBot(sockPath, disp, stubMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bot.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if got := len(disp.snapshot()); got != 3 {
		t.Fatalf("expected 3 records dispatched, got %d", got)
	}
	cancel()
	serverWG.Wait()
}

func TestBot_ReconnectsOnDisconnect(t *testing.T) {
	if testing.Short() {
		t.Skip("reconnect test takes ~6s")
	}
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "test.sock")
	defer os.Remove(sockPath)

	disp := &capturingDispatcher{}
	bot := NewBot(sockPath, disp, stubMetrics())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go bot.Run(ctx)
	time.Sleep(300 * time.Millisecond) // bot tries to dial, fails (no listener)

	// Now create the listener.
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
		defer conn.Close()
		conn.Write([]byte(encodeRecord(Record{Type: "heartbeat", Timestamp: time.Now(), Port: "mktdata"})))
		time.Sleep(200 * time.Millisecond)
	}()

	// Bot's backoff is at most 5s; give it room to reconnect and read.
	time.Sleep(6 * time.Second)

	if got := len(disp.snapshot()); got < 1 {
		t.Fatalf("expected >=1 record after reconnect, got %d", got)
	}
}
