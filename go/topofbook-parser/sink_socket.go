package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
)

// SocketSink listens on a Unix domain socket and writes records to all
// connected clients. Each client has its own goroutine + bounded outbound
// queue so a slow consumer cannot back-pressure the UDP receive path:
// when the queue is full, the batch is dropped (and counted) instead of
// blocking the caller of Write.
type SocketSink struct {
	format   string
	sockPath string
	metrics  *metrics // optional

	mu       sync.Mutex
	listener net.Listener
	clients  map[net.Conn]*clientWriter
	closed   bool
}

// outQueueLen bounds per-client buffered batches. Sized to absorb
// burst variance when the bot's single-goroutine dispatch falls behind;
// dropping here is preferred over dropping at the kernel UDP socket.
const outQueueLen = 16384

type clientWriter struct {
	conn net.Conn
	ch   chan []Record
	done chan struct{}
	rw   recordWriter
}

// recordWriter writes records to a single connected client.
type recordWriter interface {
	writeRecords(records []Record) error
}

// NewSocketSink creates a Unix domain socket at sockPath and begins
// accepting connections. format must be "json" or "csv". m may be nil.
func NewSocketSink(format, sockPath string, m *metrics) (*SocketSink, error) {
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

	s := &SocketSink{
		format:   format,
		sockPath: sockPath,
		listener: lis,
		metrics:  m,
		clients:  make(map[net.Conn]*clientWriter),
	}

	go s.acceptLoop()
	return s, nil
}

func (s *SocketSink) acceptLoop() {
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

		// Choose encoder based on format; the per-client serve() goroutine
		// drives the encoder through a 1 MiB bufio.Writer.
		var rw recordWriter
		switch s.format {
		case "json":
			rw = newJSONConnWriter(conn)
		case "csv":
			rw = newCSVConnWriter(conn)
		}

		cw := &clientWriter{
			conn: conn,
			ch:   make(chan []Record, outQueueLen),
			done: make(chan struct{}),
			rw:   rw,
		}

		s.mu.Lock()
		s.clients[conn] = cw
		clientCount := len(s.clients)
		s.mu.Unlock()

		if s.metrics != nil {
			s.metrics.socketClients.Set(float64(clientCount))
		}
		slog.Info("edge: socket client connected", "path", s.sockPath, "remote", conn.RemoteAddr())

		go s.serve(cw)
	}
}

// serve drains cw.ch and writes records via the per-client recordWriter.
// For json clients the encoder is backed by a 1 MiB bufio.Writer; for csv
// the csv.Writer does its own internal buffering and is flushed each batch.
// Exits on first write error; the client is then removed by dropClient.
func (s *SocketSink) serve(cw *clientWriter) {
	defer close(cw.done)

	for batch := range cw.ch {
		if err := cw.rw.writeRecords(batch); err != nil {
			s.dropClient(cw, err)
			return
		}
	}
}

func (s *SocketSink) dropClient(cw *clientWriter, err error) {
	slog.Warn("edge: dropping socket client", "path", s.sockPath, "error", err)
	s.mu.Lock()
	if _, ok := s.clients[cw.conn]; ok {
		delete(s.clients, cw.conn)
		if s.metrics != nil {
			s.metrics.socketClientDrops.WithLabelValues("write_error").Inc()
			s.metrics.socketClients.Set(float64(len(s.clients)))
		}
	}
	s.mu.Unlock()
	cw.conn.Close()
	// Drain remaining queue so any pending Write senders don't block.
	for range cw.ch {
	}
}

func (s *SocketSink) Write(records []Record) error {
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
			s.metrics.socketClientDrops.WithLabelValues("queue_full").Add(float64(queueDrops))
		}
		if sentTo > 0 {
			s.metrics.socketRecordsSent.Add(float64(len(records)))
		}
	}
	return nil
}

func (s *SocketSink) Close() error {
	s.mu.Lock()
	if s.closed {
		// Idempotent: second Close() is a no-op (channels are already closed).
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	clients := s.clients
	s.clients = make(map[net.Conn]*clientWriter)
	// Close all channels while holding mu. This is safe because Write() also
	// holds mu during its sends, so we can never close a channel that Write
	// is currently sending on.
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

	err := s.listener.Close()
	os.Remove(s.sockPath) //nolint:errcheck
	return err
}

// jsonConnWriter writes JSONL to a single connection via a 1 MiB bufio.Writer.
type jsonConnWriter struct {
	bw  *bufio.Writer
	enc *json.Encoder
}

func newJSONConnWriter(w io.Writer) *jsonConnWriter {
	bw := bufio.NewWriterSize(w, 1<<20)
	enc := json.NewEncoder(bw)
	enc.SetEscapeHTML(false)
	return &jsonConnWriter{bw: bw, enc: enc}
}

func (j *jsonConnWriter) writeRecords(records []Record) error {
	for i := range records {
		if err := j.enc.Encode(&records[i]); err != nil {
			return err
		}
	}
	return j.bw.Flush()
}

// csvConnWriter writes CSV quote/trade rows to a single connection.
type csvConnWriter struct {
	w             *csv.Writer
	wroteQuoteHdr bool
	wroteTradeHdr bool
}

func newCSVConnWriter(w io.Writer) *csvConnWriter {
	return &csvConnWriter{w: csv.NewWriter(w)}
}

func (c *csvConnWriter) writeRecords(records []Record) error {
	for i := range records {
		r := &records[i]
		switch r.Type {
		case "quote":
			if !c.wroteQuoteHdr {
				if err := c.w.Write(quoteCSVHeader); err != nil {
					return err
				}
				c.wroteQuoteHdr = true
			}
			if err := c.w.Write(quoteToCSVRow(r)); err != nil {
				return err
			}
		case "trade":
			if !c.wroteTradeHdr {
				if err := c.w.Write(tradeCSVHeader); err != nil {
					return err
				}
				c.wroteTradeHdr = true
			}
			if err := c.w.Write(tradeToCSVRow(r)); err != nil {
				return err
			}
		default:
			continue
		}
	}
	c.w.Flush()
	return c.w.Error()
}
