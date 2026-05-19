package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

const maxUDPPacket = 65536

// portConfig binds a label to a UDP listening address.
type portConfig struct {
	Label string
	Port  int
}

// Runner is the per-port receive loop.
type Runner struct {
	parser  Parser
	sink    OutputSink
	metrics *Metrics
	group   net.IP
	iface   *net.Interface // may be nil for system default
	ports   []portConfig
}

func NewRunner(parser Parser, sink OutputSink, metrics *Metrics, group string, iface string, refdata, mktdata, snapshot int) (*Runner, error) {
	ip := net.ParseIP(group)
	if ip == nil || ip.To4() == nil {
		return nil, fmt.Errorf("invalid multicast group: %q", group)
	}

	var ifi *net.Interface
	if iface != "" {
		var err error
		ifi, err = net.InterfaceByName(iface)
		if err != nil {
			return nil, fmt.Errorf("interface %q: %w", iface, err)
		}
	}

	return &Runner{
		parser:  parser,
		sink:    sink,
		metrics: metrics,
		group:   ip.To4(),
		iface:   ifi,
		ports: []portConfig{
			{"refdata", refdata},
			{"mktdata", mktdata},
			{"snapshot", snapshot},
		},
	}, nil
}

// Run spawns one goroutine per port and blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(r.ports))

	var opened []*net.UDPConn
	for _, pc := range r.ports {
		conn, err := r.openMulticast(pc.Port)
		if err != nil {
			for _, c := range opened {
				c.Close()
			}
			return fmt.Errorf("open %s port %d: %w", pc.Label, pc.Port, err)
		}
		opened = append(opened, conn)
		wg.Add(1)
		go func(label string, conn *net.UDPConn) {
			defer wg.Done()
			defer conn.Close()
			r.receive(ctx, label, conn, errs)
		}(pc.Label, conn)
	}

	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			return e
		}
	}
	return nil
}

func (r *Runner) openMulticast(port int) (*net.UDPConn, error) {
	addr := &net.UDPAddr{IP: r.group, Port: port}
	conn, err := net.ListenMulticastUDP("udp4", r.iface, addr)
	if err != nil {
		return nil, err
	}
	if err := conn.SetReadBuffer(64 * 1024 * 1024); err != nil {
		log.Printf("warning: SetReadBuffer: %v", err)
	}
	return conn, nil
}

func (r *Runner) receive(ctx context.Context, port string, conn *net.UDPConn, errs chan<- error) {
	buf := make([]byte, maxUDPPacket)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			errs <- fmt.Errorf("read %s: %w", port, err)
			return
		}

		r.metrics.IngressPackets.WithLabelValues(port).Inc()
		r.metrics.IngressBytes.WithLabelValues(port).Add(float64(n))

		records, perr := r.parser.ParseFrame(port, buf[:n])
		if perr != nil {
			r.metrics.ParseErrors.WithLabelValues(port, classifyError(perr)).Inc()
			continue
		}

		for i := range records {
			r.metrics.RecordsTotal.WithLabelValues(records[i].Type).Inc()
			if lat := time.Since(records[i].Timestamp).Seconds(); lat >= 0 {
				r.metrics.WireLatency.WithLabelValues(port).Observe(lat)
			}
		}

		if err := r.sink.Write(records); err != nil {
			r.metrics.SinkWriteErrors.Inc()
		}
	}
}

func classifyError(err error) string {
	switch {
	case errors.Is(err, errBadMagic):
		return "bad_magic"
	case errors.Is(err, errSchemaVersion):
		return "schema_version"
	case errors.Is(err, errFrameLength), errors.Is(err, errMessageLength):
		return "frame_length"
	case errors.Is(err, errFrameTooShort), errors.Is(err, errMessageTooShort), errors.Is(err, errTruncated):
		return "truncated"
	default:
		return "other"
	}
}
