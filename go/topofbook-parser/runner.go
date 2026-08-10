package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/net/ipv4"
)

const (
	maxDatagramSize = 65535
	summaryInterval = 30 * time.Second
)

const frameHeaderSeqOffset = 4
const frameHeaderChannelOffset = 3
const frameHeaderMinLen = 12 // need at least bytes 0..11 to read the seq field

// pubKey identifies one publisher's sequence space.
//
// A group and port carries two redundant publishers interleaved packet by
// packet, distinguished by source address and by Channel ID in the frame
// header, and each numbers its frames independently. Keyed by port alone, the
// tracker read the alternation between them as continuous loss.
//
// netip.Addr rather than a string: comparable, usable directly as a map key,
// and no allocation per datagram.
type pubKey struct {
	src netip.Addr
	ch  uint8
}

// seqTracker tracks the frame header sequence number per publisher to detect
// real UDP datagram loss (gaps in the header seq).
type seqTracker struct {
	last map[pubKey]uint64
}

// observe records seq for one publisher and returns (gaps, missing) where gaps
// is 1 if a discontinuity was detected and missing is the number of missing
// frames. Reorders/dups (seq <= last) are ignored and return (0, 0).
//
// A key not seen before establishes its baseline silently and returns (0, 0);
// that is what keeps a newly-appearing publisher from producing a phantom gap
// the size of the sequence. It does not make a publisher that restarts in
// place under the same key safe: its sequence resets to a low value, last
// stays pinned high, and every later frame takes the seq >= last false
// branch, so loss for that publisher is never reported again for the life of
// the process. That in-place-restart case is a known limitation; Reset Count
// in the frame header is the intended future signal for it.
func (s *seqTracker) observe(src netip.Addr, ch uint8, seq uint64) (gaps, missing uint64) {
	if s.last == nil {
		s.last = make(map[pubKey]uint64)
	}
	k := pubKey{src: src, ch: ch}
	last, seen := s.last[k]
	if !seen {
		s.last[k] = seq
		return 0, 0
	}
	if seq > last+1 {
		gaps = 1
		missing = seq - (last + 1)
	}
	if seq >= last {
		s.last[k] = seq
	}
	return gaps, missing
}

// srcAddr normalises a datagram's sender address so one publisher always
// produces one map key. Shared by both build-tagged readDatagram variants.
func srcAddr(addr *net.UDPAddr) netip.Addr {
	if addr == nil {
		return netip.Addr{}
	}
	return addr.AddrPort().Addr().Unmap()
}

type RunnerConfig struct {
	GroupIP        net.IP
	MarketdataPort int
	RefdataPort    int
	Interface      string // network interface to join multicast on (optional)
	Parser         Parser
	Sink           OutputSink
	Metrics        *metrics // optional; may be nil
}

type Runner struct {
	cfg              RunnerConfig
	recordsWritten   atomic.Uint64
	firstFrameLogged atomic.Bool
}

func NewRunner(cfg RunnerConfig) *Runner {
	return &Runner{cfg: cfg}
}

// Run starts listening for multicast packets on both the marketdata and
// refdata ports. It blocks until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() {
		errCh <- r.listenPort(ctx, r.cfg.RefdataPort, "refdata")
	}()

	go func() {
		errCh <- r.listenPort(ctx, r.cfg.MarketdataPort, "marketdata")
	}()

	go r.logPeriodicSummary(ctx)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (r *Runner) logPeriodicSummary(ctx context.Context) {
	ticker := time.NewTicker(summaryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			slog.Info("runner summary",
				"records_written", r.recordsWritten.Load(),
				"buffered", r.cfg.Parser.Buffered(),
				"instruments_known", r.cfg.Parser.InstrumentCount())
		}
	}
}

func (r *Runner) listenPort(ctx context.Context, port int, label string) error {
	addr := &net.UDPAddr{
		IP:   r.cfg.GroupIP,
		Port: port,
	}

	var iface *net.Interface
	if r.cfg.Interface != "" {
		var err error
		iface, err = net.InterfaceByName(r.cfg.Interface)
		if err != nil {
			return fmt.Errorf("resolving interface %q: %w", r.cfg.Interface, err)
		}
	}

	conn, err := net.ListenMulticastUDP("udp4", iface, addr)
	if err != nil {
		return fmt.Errorf("joining multicast group %s port %d: %w", r.cfg.GroupIP, port, err)
	}
	defer conn.Close()

	if err := conn.SetReadBuffer(64 * 1024 * 1024); err != nil {
		slog.Warn("could not set UDP read buffer size", "error", err)
	}

	pc := ipv4.NewPacketConn(conn)
	if err := pc.SetControlMessage(ipv4.FlagDst, true); err != nil {
		slog.Warn("could not set control message flag", "error", err)
	}
	if err := enableTimestamping(conn); err != nil {
		slog.Warn("could not enable UDP receive timestamping", "error", err)
	}

	slog.Info("listening for multicast", "group", r.cfg.GroupIP, "port", port, "label", label,
		"interface", r.cfg.Interface)

	buf := make([]byte, maxDatagramSize)
	var tracker seqTracker
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		n, src, recvTime, recvKind, err := readDatagram(conn, buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("read error", "port", label, "error", err)
			continue
		}

		// Refdata is a low-rate periodic-retransmit stream; frame-seq gaps there
		// are not a meaningful loss signal, so it's excluded.
		if n >= frameHeaderMinLen && label != "refdata" {
			ch := buf[frameHeaderChannelOffset]
			seq := binary.LittleEndian.Uint64(buf[frameHeaderSeqOffset : frameHeaderSeqOffset+8])
			if gaps, missing := tracker.observe(src, ch, seq); gaps > 0 && r.cfg.Metrics != nil {
				srcLabel, chLabel := src.String(), strconv.Itoa(int(ch))
				r.cfg.Metrics.frameSeqGaps.WithLabelValues(label, srcLabel, chLabel).Add(float64(gaps))
				r.cfg.Metrics.framesMissing.WithLabelValues(label, srcLabel, chLabel).Add(float64(missing))
			}
		}

		if r.cfg.Metrics != nil {
			r.cfg.Metrics.ingressPackets.WithLabelValues(label).Inc()
			r.cfg.Metrics.ingressBytes.WithLabelValues(label).Add(float64(n))
		}

		records, err := r.cfg.Parser.Parse(buf[:n], PacketMeta{
			RecvTimestamp:   recvTime,
			RecvTimestampNS: uint64(recvTime.UnixNano()),
			RecvTSKind:      recvKind,
			MulticastGroup:  r.cfg.GroupIP.String(),
			Port:            port,
			Channel:         label,
		})
		if err != nil {
			if r.cfg.Metrics != nil {
				r.cfg.Metrics.parseErrors.WithLabelValues(label, classifyParseErr(err)).Inc()
			}
			slog.Warn("parse error", "port", label, "error", err)
			continue
		}

		// Schema Version is byte 2 of the frame header, already validated by
		// Parse. Read here rather than threaded through the parser, because the
		// three parsers return different shapes and this is observability.
		if r.cfg.Metrics != nil {
			r.cfg.Metrics.framesTotal.WithLabelValues(label, strconv.Itoa(int(buf[2]))).Inc()
		}

		if len(records) > 0 {
			if r.firstFrameLogged.CompareAndSwap(false, true) {
				slog.Info("parser producing records",
					"port", label,
					"first_batch_size", len(records))
			}
			if r.cfg.Metrics != nil {
				r.cfg.Metrics.buffered.Set(float64(r.cfg.Parser.Buffered()))
				r.cfg.Metrics.instrumentsTracked.Set(float64(r.cfg.Parser.InstrumentCount()))
				for i := range records {
					rec := &records[i]
					r.cfg.Metrics.records.WithLabelValues(rec.Type).Inc()
					if rec.SendTSNS != 0 {
						lat := recvTime.Sub(time.Unix(0, int64(rec.SendTSNS))).Seconds()
						if lat < 0 {
							lat = 0
						}
						r.cfg.Metrics.sendLatency.WithLabelValues(rec.Type).Observe(lat)
					}
					if rec.SourceTSNS != 0 {
						lat := recvTime.Sub(time.Unix(0, int64(rec.SourceTSNS))).Seconds()
						if lat < 0 {
							lat = 0
						}
						r.cfg.Metrics.sourceLatency.WithLabelValues(rec.Type).Observe(lat)
					}
				}
			}
			if err := r.cfg.Sink.Write(records); err != nil {
				if r.cfg.Metrics != nil {
					r.cfg.Metrics.sinkWriteErrors.Inc()
				}
				slog.Error("sink write error", "error", err)
				continue
			}
			r.recordsWritten.Add(uint64(len(records)))
		}
	}
}

func annotateReceiveTimestamp(records []Record, recvTime time.Time, recvKind string) {
	recvTime = recvTime.UTC()
	recvTSNS := uint64(recvTime.UnixNano())
	for i := range records {
		records[i].RecvTimestamp = recvTime
		records[i].RecvTimestampNS = recvTSNS
		records[i].RecvTSKind = recvKind
	}
}

// classifyParseErr maps a parse error into a low-cardinality bucket.
// Used as a Prometheus label value, so it must stay finite.
func classifyParseErr(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "magic"):
		return "bad_magic"
	case strings.Contains(msg, "schema"):
		return "schema_version"
	case strings.Contains(msg, "frame_length"):
		return "frame_length"
	case strings.Contains(msg, "truncat"):
		return "truncated"
	default:
		return "other"
	}
}
