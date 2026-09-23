package main

import (
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/sink"
)

// gatheredNames returns the metric family names the registry currently reports.
func gatheredNames(t *testing.T, m *Metrics) []string {
	t.Helper()
	families, err := m.registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	return names
}

func mustContain(t *testing.T, names []string, want string) {
	t.Helper()
	for _, n := range names {
		if n == want {
			return
		}
	}
	t.Errorf("missing %s in %v", want, names)
}

func TestMetricsNamespaceAndDefectCounters(t *testing.T) {
	m := NewMetrics("test", "abc123")

	// build_info is set and uptime_seconds is a GaugeFunc, so both carry values
	// as soon as NewMetrics runs. They are gathered without any observation,
	// which makes them the right probes for the namespace prefix.
	names := gatheredNames(t, m)
	mustContain(t, names, "dz_mbp_parser_build_info")
	mustContain(t, names, "dz_mbp_parser_uptime_seconds")

	// A CounterVec reports no metric family until a label set is observed, so
	// touch each vec before asserting on it.
	m.DatagramSeqGaps.WithLabelValues("mktdata", "10.0.0.1", "1").Inc()
	m.SnapshotFlagMismatch.WithLabelValues("mktdata").Inc()
	m.MalformedMessages.WithLabelValues("bookclear_scope_side").Inc()
	m.SkippedMessages.WithLabelValues("unknown_type").Inc()

	names = gatheredNames(t, m)
	for _, want := range []string{
		"dz_mbp_parser_datagram_seq_gaps_total",
		"dz_mbp_parser_snapshot_flag_mismatch_total",
		"dz_mbp_parser_malformed_total",
		"dz_mbp_parser_skipped_messages_total",
	} {
		mustContain(t, names, want)
	}

	// This module must not register anything under the namespace of another feed
	// in the DoubleZero Edge family. Copying metrics.go from marketbyorder-parser
	// and missing the namespace constant is the exact mistake this guards.
	for _, n := range names {
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under another family feed's namespace", n)
		}
	}
}

// TestDatagramsTotal_LabelsSchemaVersion proves datagrams_total is registered and
// counts per port and wire schema version independently, which is what makes
// a publisher's v1-to-v3 cutover observable.
func TestDatagramsTotal_LabelsSchemaVersion(t *testing.T) {
	m := NewMetrics("test", "test")

	m.DatagramsTotal.WithLabelValues("refdata", "1").Inc()
	m.DatagramsTotal.WithLabelValues("refdata", "3").Inc()
	m.DatagramsTotal.WithLabelValues("refdata", "3").Inc()

	// A CounterVec reports no metric family until a label set is observed
	// (see the comment above), so the registration check runs after the
	// increments rather than before.
	mustContain(t, gatheredNames(t, m), "dz_mbp_parser_datagrams_total")

	if got := readCounterVec(t, m.DatagramsTotal, "refdata", "1"); got != 1 {
		t.Errorf("v1 datagrams: got %v want 1", got)
	}
	if got := readCounterVec(t, m.DatagramsTotal, "refdata", "3"); got != 2 {
		t.Errorf("v3 datagrams: got %v want 2", got)
	}
}

// readCounterVec reads the current value of a CounterVec label combination.
func readCounterVec(t *testing.T, cv *prometheus.CounterVec, lvs ...string) float64 {
	t.Helper()
	metric, err := cv.GetMetricWithLabelValues(lvs...)
	if err != nil {
		t.Fatalf("metric lookup failed: %v", err)
	}
	m := &dto.Metric{}
	if err := metric.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Counter.GetValue()
}

// readCounter reads the current value of a Counter.
func readCounter(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Counter.GetValue()
}

// readGauge reads the current value of a Gauge.
func readGauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := g.Write(m); err != nil {
		t.Fatalf("metric write failed: %v", err)
	}
	return m.Gauge.GetValue()
}

// TestSocketMetrics_ReachTheFeedsCounters pins the wiring the shared socket
// sink reports through: each sink.SocketMetrics method must land on this feed's
// own socket counter, and a drop must carry its reason as the label value.
func TestSocketMetrics_ReachTheFeedsCounters(t *testing.T) {
	m := NewMetrics("test", "test")

	var sm sink.SocketMetrics = m
	sm.SetSocketClients(3)
	sm.AddSocketClientDrops(sink.DropReasonQueueFull, 2)
	sm.AddSocketClientDrops(sink.DropReasonWriteError, 1)
	sm.AddSocketRecordsSent(7)

	if got := readGauge(t, m.SocketClients); got != 3 {
		t.Errorf("socket_clients: got %v want 3", got)
	}
	if got := readCounterVec(t, m.SocketClientDrops, "queue_full"); got != 2 {
		t.Errorf("socket_client_drops_total{reason=queue_full}: got %v want 2", got)
	}
	if got := readCounterVec(t, m.SocketClientDrops, "write_error"); got != 1 {
		t.Errorf("socket_client_drops_total{reason=write_error}: got %v want 1", got)
	}
	if got := readCounter(t, m.SocketRecordsSent); got != 7 {
		t.Errorf("socket_records_sent_total: got %v want 7", got)
	}
}

// TestSocketMetrics_NilReceiverCountsNothing covers the optional case: an
// absent SinkConfig.Metrics reaches the sink as a nil *Metrics inside a
// non-nil interface, so every method has to tolerate it.
func TestSocketMetrics_NilReceiverCountsNothing(t *testing.T) {
	var sm sink.SocketMetrics = (*Metrics)(nil)

	sm.SetSocketClients(1)
	sm.AddSocketClientDrops(sink.DropReasonQueueFull, 1)
	sm.AddSocketRecordsSent(1)
}

// TestSocketMetrics_AbsentMetricsBecomeANilInterface pins the conversion that
// keeps the shared socket sink's "no metrics" path reachable: an optional
// *Metrics has to arrive as an untyped nil, because a nil pointer inside a
// non-nil interface would be called through instead of skipped.
func TestSocketMetrics_AbsentMetricsBecomeANilInterface(t *testing.T) {
	var absent *Metrics
	if sm := absent.socketMetrics(); sm != nil {
		t.Errorf("a nil *Metrics converted to a non-nil sink.SocketMetrics (%T)", sm)
	}
	if sm := NewMetrics("test", "test").socketMetrics(); sm == nil {
		t.Error("real counters converted to a nil sink.SocketMetrics")
	}
}
