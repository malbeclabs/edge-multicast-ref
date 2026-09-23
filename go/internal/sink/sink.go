// Package sink writes a parser's decoded records to a file or to a Unix domain
// socket.
//
// Every sink here is generic over the record type R, so each feed keeps its own
// standalone Record and only the transport is shared. Nothing in this package
// reads a field of R: records reach a destination either through
// encoding/json, which works on any type, or through a ConnWriter the feed
// supplies. Feed-specific encoding therefore stays in the parser that owns the
// wire shape.
package sink

// Sink writes records of type R to a destination.
type Sink[R any] interface {
	// Write outputs one or more records.
	Write(records []R) error

	// Close releases any resources held by the sink.
	Close() error
}

// Drop reasons reported through SocketMetrics.AddSocketClientDrops. The label
// vocabulary lives here so all feeds report a slow client and a failed write
// under the same two names. The two count different things, which is the whole
// reason they are separate labels:
const (
	// DropReasonQueueFull is one batch dropped for one client whose outbound
	// queue was full. The client stays connected, so a single slow reader
	// raises this once per batch it could not keep up with, not once.
	DropReasonQueueFull = "queue_full"

	// DropReasonWriteError is one client dropped and disconnected because a
	// write to it failed. Raised at most once per client.
	DropReasonWriteError = "write_error"
)

// SocketMetrics counts what the socket sink observes about its clients. A
// parser implements it over its own feed-prefixed counters, which is why this
// package holds no metrics backend of its own.
//
// Socket treats a nil SocketMetrics as "do not count", so a parser whose
// counters are optional passes an untyped nil and not a nil *Metrics — a nil
// pointer inside a non-nil interface still reaches these methods. An
// implementation is expected to tolerate a nil receiver even so.
type SocketMetrics interface {
	// SetSocketClients reports the number of currently connected clients.
	SetSocketClients(n int)

	// AddSocketClientDrops counts n drops for reason. What one drop is depends
	// on the reason: a dropped batch for DropReasonQueueFull, a disconnected
	// client for DropReasonWriteError.
	AddSocketClientDrops(reason string, n int)

	// AddSocketRecordsSent counts n records queued to at least one client.
	// Queued, not delivered: a batch that a client's queue accepted and its
	// writer then failed to write is counted here and again as a
	// DropReasonWriteError drop.
	AddSocketRecordsSent(n int)
}
