package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// runTimeout bounds how long a test waits for run to return. Nothing in the
// test path blocks for anywhere near this; a run still going at this point is
// one that is never coming back.
const runTimeout = 10 * time.Second

// fatalReadRunner stands in for the Runner. It writes the records a receive
// loop had already parsed and handed to the sink, then fails the way a read
// that is not a deadline expiry does — the failure that takes every port down,
// and so the way the process ends.
//
// It waits for connected first, so the records are written to a sink that has
// a client registered, as they would be on a live feed.
type fatalReadRunner struct {
	sink      OutputSink
	records   []Record
	connected <-chan struct{}
	err       error
}

func (f *fatalReadRunner) Run(ctx context.Context) error {
	<-f.connected
	if err := f.sink.Write(f.records); err != nil {
		return err
	}
	return f.err
}

// dialWhenReady connects to the sink's socket, retrying until it exists: run
// creates it on its own goroutine, so it is not there the moment run starts.
func dialWhenReady(t *testing.T, sockPath string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("unix", sockPath)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("connecting to the sink socket %s: %v", sockPath, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// waitForClient blocks until the sink's accept loop has registered the
// connection, which it reports on the connected-client gauge. Write only
// reaches clients that are registered by the time it runs, so a record written
// before that would never have been queued at all and the test would prove
// nothing about what Close carries out.
func waitForClient(t *testing.T, m *Metrics) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := readGauge(t, m.SocketClients)
		if n == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the sink reports %v connected clients, want 1", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestRun_FatalReadFlushesTheSinkBeforeReportingIt pins what a fatal read owes
// the client reading the sink's socket. One port failing winds down the other
// two and ends the process, so this is a designed exit path rather than an
// unreachable branch — and a body that exits from inside it, as log.Fatalf
// does, skips the deferred sink.Close and abandons every batch still queued
// for a connected client.
//
// So run returns the failure instead, main turns it into the non-zero exit
// once the defers have unwound, and the test holds run to both halves: the
// records reach the client, and the sink is closed — the socket file gone and
// the client at EOF — by the time run returns.
func TestRun_FatalReadFlushesTheSinkBeforeReportingIt(t *testing.T) {
	sockPath := shortTempSock(t)

	// The ports are never opened: the runner below stands in for the one
	// newPortRunner would have built, so nothing here joins a group.
	cfg := config{
		group:        testMulticastGroup,
		refdataPort:  20001,
		mktdataPort:  20002,
		snapshotPort: 20003,
		output:       "unix://" + sockPath,
		format:       "json",
		parserName:   "marketbyorder",
	}

	ts := time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC)
	records := []Record{{
		Type:           "level_update",
		Timestamp:      ts,
		ChannelID:      1,
		SequenceNumber: 100,
		InstrumentID:   42,
	}}

	fatal := errors.New("read snapshot: use of closed network connection")
	connected := make(chan struct{})
	built := make(chan *Metrics, 1)
	done := make(chan error, 1)

	go func() {
		done <- run(cfg, func(_ Parser, sink OutputSink, metrics *Metrics, _ config) (portRunner, error) {
			built <- metrics
			return &fatalReadRunner{sink: sink, records: records, connected: connected, err: fatal}, nil
		})
	}()

	metrics := <-built
	conn := dialWhenReady(t, sockPath)
	defer conn.Close()
	waitForClient(t, metrics)
	close(connected) // the runner writes its records, then fails

	select {
	case err := <-done:
		if !errors.Is(err, fatal) {
			t.Fatalf("run returned %v, want the fatal read error %v; run has to report it rather than exit on it", err, fatal)
		}
	case <-time.After(runTimeout):
		t.Fatalf("run still going %s after the fatal read", runTimeout)
	}

	// run has returned, so its defers have all unwound. Everything below is
	// what the deferred Close had to have done on the way out.
	if _, err := os.Stat(sockPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the sink socket %s after run returned: got %v, want it removed by Close", sockPath, err)
	}

	conn.SetReadDeadline(time.Now().Add(runTimeout)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatalf("the client received none of the records queued before the fatal read (scan error %v); they were dropped on the way out", scanner.Err())
	}
	var got Record
	if err := json.Unmarshal(scanner.Bytes(), &got); err != nil {
		t.Fatalf("decoding what the client received: %v", err)
	}
	if got.InstrumentID != records[0].InstrumentID || got.SequenceNumber != records[0].SequenceNumber {
		t.Errorf("the client received instrument %d seq %d, want instrument %d seq %d",
			got.InstrumentID, got.SequenceNumber, records[0].InstrumentID, records[0].SequenceNumber)
	}
	if scanner.Scan() {
		t.Errorf("the client read %q after the queued records, want EOF: Close leaves no connection open behind it", scanner.Bytes())
	}
}
