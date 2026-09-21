package main

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// serveTimeout bounds how long a test waits for serve to return. A port that is
// winding down notices the cancellation after its read deadline expires, so the
// bound is well above that 500ms; a serve that has to be waited out to this
// point is one that is not winding down at all.
const serveTimeout = 5 * time.Second

// localPortConn binds a UDP socket on the loopback interface, standing in for
// the multicast join that openMulticast performs.
//
// The socket is connected to a loopback port nothing runs on, so the kernel
// accepts datagrams from that peer alone and the test never sends any: the port
// stays idle whatever else the host is doing. That matters because a Runner
// built here carries no parser and no sink, so a datagram that did arrive would
// take receive into a nil parser. Nothing is sent to the peer either, so no
// ICMP error can come back and surface as a read failure.
func localPortConn(t *testing.T, label string) portConn {
	t.Helper()
	loopback := net.IPv4(127, 0, 0, 1)
	conn, err := net.DialUDP("udp4",
		&net.UDPAddr{IP: loopback, Port: 0},
		&net.UDPAddr{IP: loopback, Port: 9}) // discard, and unbound here
	if err != nil {
		t.Fatalf("listen %s: %v", label, err)
	}
	t.Cleanup(func() { conn.Close() })
	return portConn{label: label, conn: conn}
}

// TestServe_FatalReadErrorStopsEveryPort covers the defect in #32: one port's
// fatal read error has to take the whole runner down. A parser serving mktdata
// with snapshot gone leaves a subscriber that has detected a gap no way to
// recover the instrument, and reports no failure while doing it. With the
// sibling ports left running, serve blocks on them until the test's own timeout
// fires.
func TestServe_FatalReadErrorStopsEveryPort(t *testing.T) {
	refdata := localPortConn(t, "refdata")
	mktdata := localPortConn(t, "mktdata")

	// A closed socket fails its next read straight away, and not with a
	// timeout, which is exactly the fatal case receive reports.
	snapshot := localPortConn(t, "snapshot")
	if err := snapshot.conn.Close(); err != nil {
		t.Fatalf("close snapshot: %v", err)
	}

	r := &Runner{metrics: NewMetrics("test", "test")}
	done := make(chan error, 1)
	go func() { done <- r.serve(context.Background(), []portConn{refdata, mktdata, snapshot}) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("serve returned nil, want the snapshot read error")
		}
		if !strings.Contains(err.Error(), "read snapshot") {
			t.Errorf("serve returned %q, want an error naming the snapshot read", err)
		}
	case <-time.After(serveTimeout):
		t.Fatalf("serve still running %s after the snapshot read failed: the surviving ports keep it blocked and the error never reaches the caller", serveTimeout)
	}

	// serve returns only once every port goroutine has stopped, and each one
	// closes the socket it reads, so the idle ports are no longer being served.
	for _, pc := range []portConn{refdata, mktdata} {
		if err := pc.conn.SetReadDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
			t.Errorf("%s socket after serve returned: got %v, want it closed", pc.label, err)
		}
	}
}

// TestServe_IdlePortsRunUntilTheCallerCancels pins the other half of the
// contract. A port receiving nothing expires its read deadline over and over,
// which is the idle path and no reason to stop; and the caller cancelling ctx is
// a clean shutdown that reports no error, so the process still exits zero on a
// signal.
func TestServe_IdlePortsRunUntilTheCallerCancels(t *testing.T) {
	ports := []portConn{localPortConn(t, "refdata"), localPortConn(t, "mktdata"), localPortConn(t, "snapshot")}

	ctx, cancel := context.WithCancel(context.Background())
	r := &Runner{metrics: NewMetrics("test", "test")}
	done := make(chan error, 1)
	go func() { done <- r.serve(ctx, ports) }()

	// Nothing is ever sent to these sockets, so every read here expires on its
	// deadline. Two deadlines' worth of waiting puts each port through that at
	// least once, and serve has to be reading still.
	select {
	case err := <-done:
		t.Fatalf("serve returned %v while its ports were idle: a read deadline expiring is not a failure", err)
	case <-time.After(2 * readDeadline):
	}

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve after the caller cancelled: got %v, want nil", err)
		}
	case <-time.After(serveTimeout):
		t.Fatalf("serve still running %s after the caller cancelled", serveTimeout)
	}
}
