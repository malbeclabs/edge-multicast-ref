package main

import (
	"context"
	"errors"
	"fmt"
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

// testMulticastGroup is the group the Run test joins. Nothing is ever
// published to it: the only thing the test does with a socket Run opens is
// watch for it to be closed again.
const testMulticastGroup = "239.10.10.10"

// reserveUDPPort binds a UDP socket on a kernel-chosen port and returns both,
// so a test can name a port it knows is free.
//
// It also decides what holding a reservation means to openMulticast. A plain
// listen leaves SO_REUSEADDR unset, the multicast listen inside openMulticast
// sets it, and a bind is refused unless every socket already on the address
// has it too. So a reservation left in place makes openMulticast fail on that
// port, and a plain bind that succeeds afterwards proves no socket of
// openMulticast's is still holding the port. The test below uses it both ways
// round.
func reserveUDPPort(t *testing.T) (*net.UDPConn, int) {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: 0})
	if err != nil {
		t.Fatalf("reserve a UDP port: %v", err)
	}
	return conn, conn.LocalAddr().(*net.UDPAddr).Port
}

// TestRun_PartialOpenClosesTheSocketsAlreadyOpen pins what Run promises when
// opening the ports fails part-way through: the sockets it has already opened
// are closed before it returns, and closed while nothing is reading them,
// since Run opens every port before serve starts any receive loop. Leave them
// open and a process that is about to exit non-zero still holds two of the
// feed's three ports, so the restart behind it fails to open them.
func TestRun_PartialOpenClosesTheSocketsAlreadyOpen(t *testing.T) {
	// Three ports the kernel says are free. The reservation on the snapshot
	// port stays, which is what makes Run fail there; the refdata and mktdata
	// ports are handed straight back for Run to open.
	refdataRes, refdata := reserveUDPPort(t)
	mktdataRes, mktdata := reserveUDPPort(t)
	snapshotRes, snapshot := reserveUDPPort(t)
	t.Cleanup(func() { snapshotRes.Close() })
	refdataRes.Close()
	mktdataRes.Close()

	r, err := NewRunner(nil, nil, NewMetrics("test", "test"), testMulticastGroup, "", refdata, mktdata, snapshot)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	// Run returns from its open loop, so no receive loop ever starts and the
	// nil parser and sink are never reached.
	err = r.Run(context.Background())
	if err == nil {
		t.Fatalf("Run returned nil, want the failure to open the snapshot port %d", snapshot)
	}
	if want := fmt.Sprintf("open snapshot port %d", snapshot); !strings.Contains(err.Error(), want) {
		t.Fatalf("Run returned %q, want an error naming %q; one naming refdata or mktdata means the open failed before it reached the snapshot port, leaving the partial-open path untested", err, want)
	}

	for _, pc := range []struct {
		label string
		port  int
	}{{"refdata", refdata}, {"mktdata", mktdata}} {
		conn, err := net.ListenUDP("udp4", &net.UDPAddr{Port: pc.port})
		if err != nil {
			t.Errorf("binding the %s port %d after Run failed part-way: %v; want the port free, Run having closed the socket it opened there", pc.label, pc.port, err)
			continue
		}
		conn.Close()
	}
}
