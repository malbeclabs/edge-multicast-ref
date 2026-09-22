package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestSocket_JSONBroadcast(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], nil)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

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

	records := []testRecord{{Type: "quote", Seq: 100}}

	if err := s.Write(records); err != nil {
		t.Fatalf("error writing to socket sink: %v", err)
	}

	// Both clients should receive the same JSONL record.
	for i, conn := range []net.Conn{conn1, conn2} {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
		scanner := bufio.NewScanner(conn)
		if !scanner.Scan() {
			t.Fatalf("client %d: no data received", i+1)
		}
		var r testRecord
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			t.Fatalf("client %d: error decoding JSON: %v", i+1, err)
		}
		if r.Type != "quote" || r.Seq != 100 {
			t.Errorf("client %d: received %+v", i+1, r)
		}
	}
}

// lineConnWriter is a ConnWriter that is not the JSON one, standing in for a
// feed whose socket output has its own record layout.
type lineConnWriter struct {
	w *bufio.Writer
}

func newLineConnWriter(w io.Writer) ConnWriter[testRecord] {
	return &lineConnWriter{w: bufio.NewWriter(w)}
}

func (l *lineConnWriter) WriteRecords(records []testRecord) error {
	for _, r := range records {
		if _, err := fmt.Fprintf(l.w, "%s,%d\n", r.Type, r.Seq); err != nil {
			return err
		}
	}
	return l.w.Flush()
}

// TestSocket_UsesTheSuppliedConnWriter pins the seam that keeps a feed's
// output format out of this package: the writer the caller supplies is the one
// every client is served through.
func TestSocket_UsesTheSuppliedConnWriter(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSocket(sockPath, newLineConnWriter, nil)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if err := s.Write([]testRecord{{Type: "quote", Seq: 7}}); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	scanner := bufio.NewScanner(conn)
	if !scanner.Scan() {
		t.Fatal("no data received")
	}
	if got := scanner.Text(); got != "quote,7" {
		t.Errorf("expected the supplied writer's output, got %q", got)
	}
}

// TestSocket_RequiresConnWriter checks the guard runs before the socket path is
// touched: a caller passing no writer gets an error, and no socket file is left
// behind.
func TestSocket_RequiresConnWriter(t *testing.T) {
	sockPath := shortTempSock(t)

	if err := os.WriteFile(sockPath, []byte("not a socket"), 0600); err != nil {
		t.Fatalf("writing placeholder file: %v", err)
	}

	s, err := NewSocket[testRecord](sockPath, nil, nil)
	if err == nil {
		s.Close()
		t.Fatal("expected an error with no connection writer, got nil")
	}
	if _, statErr := os.Stat(sockPath); statErr != nil {
		t.Errorf("the placeholder file at the socket path was removed: %v", statErr)
	}
}

func TestSocket_DropsDisconnectedClient(t *testing.T) {
	sockPath := shortTempSock(t)

	m := newCountingMetrics()
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	// Close the client before writing.
	conn.Close()

	records := []testRecord{{Type: "heartbeat", Seq: 1}}

	// Write should succeed (disconnected client is dropped, not an error).
	if err := s.Write(records); err != nil {
		t.Fatalf("expected no error after client disconnect, got: %v", err)
	}

	// Verify client was removed. Removal is async (per-client writer
	// goroutine detects the closed conn on its next flush), so poll.
	deadline := time.Now().Add(time.Second)
	var count int
	for time.Now().Before(deadline) {
		s.mu.Lock()
		count = len(s.clients)
		s.mu.Unlock()
		if count == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count != 0 {
		t.Errorf("expected 0 clients after disconnect, got %d", count)
	}

	clients, drops, _ := m.snapshot()
	if drops[DropReasonWriteError] != 1 {
		t.Errorf("expected 1 %s drop, got %d", DropReasonWriteError, drops[DropReasonWriteError])
	}
	if clients != 0 {
		t.Errorf("expected the client gauge back at 0, got %d", clients)
	}
}

// TestSocket_BackPressure verifies that Write never blocks the caller when
// a client's queue is full, that the queue_full drop counter is incremented,
// and that a healthy second client continues to receive records.
//
// We inject a clientWriter whose outbound queue is pre-filled (capacity 0 —
// i.e. unbuffered and already consumed by no one) alongside a real connected
// client.
// This isolates the non-blocking select in Write() from OS socket buffer sizes.
func TestSocket_BackPressure(t *testing.T) {
	sockPath := shortTempSock(t)

	m := newCountingMetrics()
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	// goodConn: a real client that reads normally.
	goodConn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting good client: %v", err)
	}
	defer goodConn.Close()

	// Give the accept loop time to register the good client.
	time.Sleep(50 * time.Millisecond)

	// Inject a fake clientWriter with a zero-capacity (unbuffered) outbound
	// queue. Write's non-blocking select will immediately take the default branch,
	// recording a queue_full drop — without involving any OS socket buffer.
	fakeConn, fakeServer := net.Pipe()
	defer fakeConn.Close()
	defer fakeServer.Close()

	fakeCW := &clientWriter[testRecord]{
		conn: fakeConn,
		ch:   make(chan []testRecord), // unbuffered: always full from Write's perspective
		done: make(chan struct{}),
		w:    NewJSONConnWriter[testRecord](fakeConn),
	}
	// Mark done closed so Close() doesn't hang waiting for this goroutine.
	close(fakeCW.done)

	s.mu.Lock()
	s.clients[fakeConn] = fakeCW
	s.mu.Unlock()

	batch := []testRecord{{Type: "quote", Seq: 1}}

	// A single Write is enough: the fake client's unbuffered outbound queue
	// causes an immediate queue_full drop. Multiple writes confirm Write never
	// blocks.
	const writes = 5
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < writes; i++ {
			if err := s.Write(batch); err != nil {
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
	_, drops, sent := m.snapshot()
	if drops[DropReasonQueueFull] == 0 {
		t.Errorf("expected at least one %s drop, got 0", DropReasonQueueFull)
	}
	if sent == 0 {
		t.Error("expected records sent to the healthy client to be counted, got 0")
	}

	// Good client should still receive records — drain what it has.
	goodConn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
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

// TestSocket_ConcurrentWriteClose verifies that concurrent Write() calls
// racing against Close() never panic (e.g. send on a closed outbound queue) and that
// the sink shuts down cleanly with no goroutine leaks. Run under -race to
// detect data races.
func TestSocket_ConcurrentWriteClose(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], nil)
	if err != nil {
		t.Fatalf("creating socket sink: %v", err)
	}

	// Connect a client so Write has a real outbound queue to send on.
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("connecting client: %v", err)
	}
	defer conn.Close()

	// Give the accept loop time to register the client.
	time.Sleep(20 * time.Millisecond)

	batch := []testRecord{{Type: "quote", Seq: 1}}

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
				s.Write(batch) //nolint:errcheck
			}
		}()
	}

	// Close races directly against the writers.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ready
		s.Close() //nolint:errcheck
	}()

	close(ready) // start all goroutines simultaneously
	wg.Wait()

	// Double-Close must also be safe (idempotent).
	s.Close() //nolint:errcheck
}

// TestSocket_NilMetricsTolerated covers both ways a parser can decline the
// counters: no SocketMetrics at all, and an optional counter set that is a nil
// pointer inside a non-nil interface.
func TestSocket_NilMetricsTolerated(t *testing.T) {
	for _, tc := range []struct {
		name    string
		metrics SocketMetrics
	}{
		{"nil interface", nil},
		{"typed nil pointer", (*countingMetrics)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sockPath := shortTempSock(t)

			s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], tc.metrics)
			if err != nil {
				t.Fatalf("error creating socket sink: %v", err)
			}
			defer s.Close()

			conn, err := net.Dial("unix", sockPath)
			if err != nil {
				t.Fatalf("error connecting: %v", err)
			}

			time.Sleep(50 * time.Millisecond)

			if err := s.Write([]testRecord{{Type: "quote", Seq: 1}}); err != nil {
				t.Fatalf("error writing: %v", err)
			}

			// Drop the client so the write-error path runs too.
			conn.Close()
			if err := s.Write([]testRecord{{Type: "quote", Seq: 2}}); err != nil {
				t.Fatalf("error writing after disconnect: %v", err)
			}

			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				s.mu.Lock()
				n := len(s.clients)
				s.mu.Unlock()
				if n == 0 {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Error("client was not dropped")
		})
	}
}

// TestSocket_RegisterAfterCloseIsRefused covers the accept-during-Close race
// from the registration side, which is the only side it can be driven
// deterministically from: a connection that arrives once Close has run must not
// join the client set Close has already taken away, because nothing would ever
// close its outbound queue, wait on its serve goroutine, or close its
// connection.
func TestSocket_RegisterAfterCloseIsRefused(t *testing.T) {
	sockPath := shortTempSock(t)

	m := newCountingMetrics()
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	client, accepted := net.Pipe()
	defer client.Close()

	// The deadline is what separates "closed" from "nobody is reading it" in
	// the assertion below, and net.Pipe only accepts one while it is open.
	if err := accepted.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("setting the deadline: %v", err)
	}

	cw, ok := s.register(accepted)
	if ok {
		t.Fatal("a connection was registered after Close")
	}
	if cw != nil {
		t.Error("a refused registration returned a client writer")
	}

	s.mu.Lock()
	n := len(s.clients)
	s.mu.Unlock()
	if n != 0 {
		t.Errorf("client set holds %d clients after Close", n)
	}

	if clients, _, _ := m.snapshot(); clients != 0 {
		t.Errorf("connected-client gauge left at %d after Close", clients)
	}

	// The refused connection is closed rather than left open with nothing
	// serving it: io.ErrClosedPipe says register closed it, while a deadline
	// error would say it is still open and simply unread.
	if _, err := accepted.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Errorf("expected the refused connection to be closed, writing to it gave %v", err)
	}
}

// TestSocket_WriteAfterCloseIsANoOp pins that a record arriving after shutdown
// is discarded rather than panicking on a closed outbound queue.
func TestSocket_WriteAfterCloseIsANoOp(t *testing.T) {
	sockPath := shortTempSock(t)

	m := newCountingMetrics()
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	time.Sleep(50 * time.Millisecond)

	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	_, _, sentBefore := m.snapshot()
	if err := s.Write([]testRecord{{Type: "quote", Seq: 1}}); err != nil {
		t.Fatalf("expected no error writing after Close, got: %v", err)
	}
	if _, _, sentAfter := m.snapshot(); sentAfter != sentBefore {
		t.Errorf("a write after Close counted %d records", sentAfter-sentBefore)
	}

	// The socket file is removed by Close.
	if _, statErr := os.Stat(sockPath); statErr == nil {
		t.Error("expected the socket file to be removed by Close")
	}
}

// clientWriterOf returns the one registered client's writer, waiting for the
// accept goroutine to put it in the client set.
func clientWriterOf(t *testing.T, s *Socket[testRecord]) *clientWriter[testRecord] {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var found *clientWriter[testRecord]
		s.mu.Lock()
		n := len(s.clients)
		for _, cw := range s.clients {
			found = cw
		}
		s.mu.Unlock()
		if n == 1 {
			return found
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no client was registered")
	return nil
}

// TestSocket_DroppedClientLeavesNoGoroutine pins what a client dropped for a
// write error leaves behind: nothing. Its serve goroutine has to return, and
// its done marker is the only signal that it did, because the outbound queue it
// was reading is never closed once the client is out of the set.
func TestSocket_DroppedClientLeavesNoGoroutine(t *testing.T) {
	sockPath := shortTempSock(t)

	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], nil)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	defer s.Close()

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	cw := clientWriterOf(t, s)

	// Close the client so the writer's flush fails and the client is dropped.
	conn.Close()
	if err := s.Write([]testRecord{{Type: "quote", Seq: 1}}); err != nil {
		t.Fatalf("error writing: %v", err)
	}

	select {
	case <-cw.done:
	case <-time.After(2 * time.Second):
		t.Fatal("the dropped client's serve goroutine did not return")
	}
}

// TestSocket_CloseReportsZeroClients pins the level the connected-client gauge
// reports after shutdown. A client connected at Close is disconnected by it, so
// the gauge has to read zero rather than the count the sink held while running.
func TestSocket_CloseReportsZeroClients(t *testing.T) {
	sockPath := shortTempSock(t)

	m := newCountingMetrics()
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], m)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	clientWriterOf(t, s)
	if clients, _, _ := m.snapshot(); clients != 1 {
		t.Fatalf("expected the gauge at 1 with one client connected, got %d", clients)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("error closing sink: %v", err)
	}

	if clients, _, _ := m.snapshot(); clients != 0 {
		t.Errorf("connected-client gauge left at %d after Close", clients)
	}
}

// lockProbeMetrics records, for every client-count report, whether the sink's
// mutex was free at the moment of the call. It stands in for a parser's
// counters only well enough to observe that one thing.
type lockProbeMetrics struct {
	probeMu sync.Mutex
	sinkMu  *sync.Mutex
	freeAt  []bool
}

// watch tells the probe which mutex to test. Called before any client can
// connect, and read back under the probe's own lock, so the accept goroutine
// races nothing for it.
func (p *lockProbeMetrics) watch(mu *sync.Mutex) {
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	p.sinkMu = mu
}

func (p *lockProbeMetrics) SetSocketClients(int) {
	p.probeMu.Lock()
	mu := p.sinkMu
	p.probeMu.Unlock()
	if mu == nil {
		return
	}

	// TryLock never blocks, so probing from inside the critical section is
	// safe: it simply reports the lock as taken.
	free := mu.TryLock()
	if free {
		mu.Unlock()
	}

	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	p.freeAt = append(p.freeAt, free)
}

func (p *lockProbeMetrics) AddSocketClientDrops(string, int) {}

func (p *lockProbeMetrics) AddSocketRecordsSent(int) {}

func (p *lockProbeMetrics) reports() []bool {
	p.probeMu.Lock()
	defer p.probeMu.Unlock()
	return append([]bool(nil), p.freeAt...)
}

// TestSocket_RegisterReportsTheClientCountUnderTheLock pins where register
// reports the connected-client gauge: inside the critical section, where
// dropClient reports it. The gauge is a level and the newest report wins, so a
// count reported after the unlock can land after a concurrent drop's newer
// count and leave socket_clients one client high until the next connect or
// drop. Nothing else takes mu while this test's single client connects, so the
// lock being free during the report means only that the report left the
// critical section.
func TestSocket_RegisterReportsTheClientCountUnderTheLock(t *testing.T) {
	sockPath := shortTempSock(t)

	p := &lockProbeMetrics{}
	s, err := NewSocket(sockPath, NewJSONConnWriter[testRecord], p)
	if err != nil {
		t.Fatalf("error creating socket sink: %v", err)
	}
	// Close's own report is deliberately outside mu — by then no client can
	// change the set — so this test reads register's report before shutdown.
	defer s.Close()
	p.watch(&s.mu)

	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		t.Fatalf("error connecting: %v", err)
	}
	defer conn.Close()

	// The report is made by the accept goroutine, so wait for it rather than
	// for the client: reaching into s.clients here would take mu and could
	// mask the very thing being asserted.
	var reports []bool
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		reports = p.reports()
		if len(reports) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(reports) == 0 {
		t.Fatal("register reported no client count")
	}

	if reports[0] {
		t.Error("register reported the client count with the sink mutex free; a concurrent drop's newer count can be overtaken by it")
	}
}
