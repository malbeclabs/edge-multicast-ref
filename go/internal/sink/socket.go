package sink

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
)

// Socket listens on a Unix domain socket and writes records to all
// connected clients. Each client has its own goroutine + bounded outbound
// queue so a slow consumer cannot back-pressure the UDP receive path:
// when the queue is full, the batch is dropped (and counted) instead of
// blocking the caller of Write.
type Socket[R any] struct {
	sockPath      string
	metrics       SocketMetrics // optional
	newConnWriter func(io.Writer) ConnWriter[R]

	mu       sync.Mutex
	listener net.Listener
	clients  map[net.Conn]*clientWriter[R]
	closed   bool
}

// outQueueLen bounds per-client buffered batches. Sized to absorb
// burst variance when the book-builder's single-goroutine dispatch falls behind;
// dropping here is preferred over dropping at the kernel UDP socket.
const outQueueLen = 16384

type clientWriter[R any] struct {
	conn net.Conn
	ch   chan []R
	done chan struct{}
	w    ConnWriter[R]
}

// ConnWriter writes records to a single connected client.
type ConnWriter[R any] interface {
	WriteRecords(records []R) error
}

// NewSocket creates a Unix domain socket at sockPath and begins accepting
// connections.
//
// newConnWriter builds the encoder for one client, and is what keeps a feed's
// output format out of this package: NewJSONConnWriter serves JSONL, and a
// parser whose format needs its own record layout passes its own writer.
// m may be nil.
func NewSocket[R any](sockPath string, newConnWriter func(io.Writer) ConnWriter[R], m SocketMetrics) (*Socket[R], error) {
	// Checked before the socket file is touched, so a caller that got this
	// wrong does not remove a file or leave a listener behind. Without it the
	// mistake surfaces as a nil call in the accept goroutine when the first
	// client connects.
	if newConnWriter == nil {
		return nil, errors.New("sink: NewSocket requires a connection writer")
	}

	// Remove any stale socket file.
	os.Remove(sockPath) //nolint:errcheck

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listening on unix socket %s: %w", sockPath, err)
	}

	if err := os.Chmod(sockPath, 0666); err != nil {
		lis.Close()
		return nil, fmt.Errorf("setting socket permissions: %w", err)
	}

	s := &Socket[R]{
		sockPath:      sockPath,
		listener:      lis,
		metrics:       m,
		newConnWriter: newConnWriter,
		clients:       make(map[net.Conn]*clientWriter[R]),
	}

	go s.acceptLoop()
	return s, nil
}

func (s *Socket[R]) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return
			}
			slog.Warn("edge: socket accept error", "path", s.sockPath, "error", err)
			continue
		}

		cw, ok := s.register(conn)
		if !ok {
			return
		}
		slog.Info("edge: socket client connected", "path", s.sockPath, "remote", conn.RemoteAddr())

		go s.serve(cw)
	}
}

// register adds conn to the client set, and reports false when the sink is
// already closed.
//
// The closed check is what keeps a connection accepted during Close from being
// registered into the client map Close has already taken away: that client
// would get a serve goroutine nobody ever closes an outbound queue for or
// waits on, leaving the goroutine and the connection behind and the
// connected-client gauge reading one after shutdown.
func (s *Socket[R]) register(conn net.Conn) (*clientWriter[R], bool) {
	cw := &clientWriter[R]{
		conn: conn,
		ch:   make(chan []R, outQueueLen),
		done: make(chan struct{}),
		w:    s.newConnWriter(conn),
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		conn.Close()
		return nil, false
	}
	s.clients[conn] = cw
	clientCount := len(s.clients)
	s.mu.Unlock()

	if s.metrics != nil {
		s.metrics.SetSocketClients(clientCount)
	}
	return cw, true
}

// serve drains cw.ch and writes records via the per-client ConnWriter.
// Exits on first write error; the client is then removed by dropClient.
func (s *Socket[R]) serve(cw *clientWriter[R]) {
	defer close(cw.done)

	for batch := range cw.ch {
		if err := cw.w.WriteRecords(batch); err != nil {
			s.dropClient(cw, err)
			return
		}
	}
}

func (s *Socket[R]) dropClient(cw *clientWriter[R], err error) {
	slog.Warn("edge: dropping socket client", "path", s.sockPath, "error", err)
	s.mu.Lock()
	if _, ok := s.clients[cw.conn]; ok {
		delete(s.clients, cw.conn)
		if s.metrics != nil {
			s.metrics.AddSocketClientDrops(DropReasonWriteError, 1)
			s.metrics.SetSocketClients(len(s.clients))
		}
	}
	s.mu.Unlock()
	cw.conn.Close()
	// Nothing is drained here, and that is what lets this goroutine return.
	// Write sends under mu with a non-blocking select, so no sender is ever
	// parked on cw.ch waiting for this receiver; ranging over the queue would
	// park the serve goroutine instead, because the only close of cw.ch is in
	// Close and Close does not hold a client this function has already removed
	// from the set.
}

func (s *Socket[R]) Write(records []R) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	// Hold mu across all non-blocking sends so that Close() cannot close
	// cw.ch between our snapshot and the send — which would cause a panic.
	// The select{...; default:} never blocks, so holding mu here is safe.
	sentTo := 0
	queueDrops := 0
	for _, cw := range s.clients {
		select {
		case cw.ch <- records:
			sentTo++
		default:
			queueDrops++
		}
	}
	s.mu.Unlock()

	if s.metrics != nil {
		if queueDrops > 0 {
			s.metrics.AddSocketClientDrops(DropReasonQueueFull, queueDrops)
		}
		if sentTo > 0 {
			s.metrics.AddSocketRecordsSent(len(records))
		}
	}
	return nil
}

func (s *Socket[R]) Close() error {
	s.mu.Lock()
	if s.closed {
		// Idempotent: second Close() is a no-op (the outbound queues are
		// already closed).
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	clients := s.clients
	s.clients = make(map[net.Conn]*clientWriter[R])
	// Close every client's outbound queue while holding mu. This is safe
	// because Write() also holds mu during its sends, so we can never close a
	// queue that Write is currently sending on.
	for _, cw := range clients {
		close(cw.ch)
		cw.conn.Close()
	}
	s.mu.Unlock()

	// Wait for serve() goroutines OUTSIDE mu. serve() calls dropClient() which
	// takes mu; waiting here while holding mu would deadlock.
	for _, cw := range clients {
		<-cw.done
	}

	// The connected-client gauge is a level, so shutdown has to state it.
	// dropClient skips a client Close has already taken out of the set, so
	// without this the gauge keeps publishing the last count the sink held
	// while it was running.
	if s.metrics != nil {
		s.metrics.SetSocketClients(0)
	}

	err := s.listener.Close()
	os.Remove(s.sockPath) //nolint:errcheck
	return err
}

// jsonConnWriter writes JSONL to a single connection via a 1 MiB bufio.Writer.
type jsonConnWriter[R any] struct {
	bw  *bufio.Writer
	enc *json.Encoder
}

// NewJSONConnWriter returns a ConnWriter that serves one client JSONL through a
// 1 MiB bufio.Writer, flushed once per batch.
//
// It returns the interface rather than the concrete type so that
// NewJSONConnWriter[Record] is directly assignable to NewSocket's
// newConnWriter parameter.
func NewJSONConnWriter[R any](w io.Writer) ConnWriter[R] {
	bw := bufio.NewWriterSize(w, 1<<20)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	return &jsonConnWriter[R]{bw: bw, enc: enc}
}

func (j *jsonConnWriter[R]) WriteRecords(records []R) error {
	for i := range records {
		if err := j.enc.Encode(&records[i]); err != nil {
			return err
		}
	}
	return j.bw.Flush()
}
