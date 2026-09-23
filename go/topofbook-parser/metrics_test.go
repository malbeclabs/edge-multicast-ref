package main

import (
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/sink"
)

// TestDatagramsTotal_LabelsSchemaVersion proves datagrams_total is registered and
// counts per port and wire schema version independently, which is what makes
// a publisher's v1-to-v3 cutover observable.
func TestDatagramsTotal_LabelsSchemaVersion(t *testing.T) {
	m := newMetrics()

	m.datagramsTotal.WithLabelValues("refdata", "1").Inc()
	m.datagramsTotal.WithLabelValues("refdata", "3").Inc()
	m.datagramsTotal.WithLabelValues("refdata", "3").Inc()

	if got := readCounterVec(t, m.datagramsTotal, "refdata", "1"); got != 1 {
		t.Errorf("v1 datagrams: got %v want 1", got)
	}
	if got := readCounterVec(t, m.datagramsTotal, "refdata", "3"); got != 2 {
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
	m := newMetrics()

	var sm sink.SocketMetrics = m
	sm.SetSocketClients(3)
	sm.AddSocketClientDrops(sink.DropReasonQueueFull, 2)
	sm.AddSocketClientDrops(sink.DropReasonWriteError, 1)
	sm.AddSocketRecordsSent(7)

	if got := readGauge(t, m.socketClients); got != 3 {
		t.Errorf("socket_clients: got %v want 3", got)
	}
	if got := readCounterVec(t, m.socketClientDrops, "queue_full"); got != 2 {
		t.Errorf("socket_client_drops_total{reason=queue_full}: got %v want 2", got)
	}
	if got := readCounterVec(t, m.socketClientDrops, "write_error"); got != 1 {
		t.Errorf("socket_client_drops_total{reason=write_error}: got %v want 1", got)
	}
	if got := readCounter(t, m.socketRecordsSent); got != 7 {
		t.Errorf("socket_records_sent_total: got %v want 7", got)
	}
}

// TestSocketMetrics_NilReceiverCountsNothing covers the optional case: an
// absent SinkConfig.Metrics reaches the sink as a nil *metrics inside a
// non-nil interface, so every method has to tolerate it.
func TestSocketMetrics_NilReceiverCountsNothing(t *testing.T) {
	var sm sink.SocketMetrics = (*metrics)(nil)

	sm.SetSocketClients(1)
	sm.AddSocketClientDrops(sink.DropReasonQueueFull, 1)
	sm.AddSocketRecordsSent(1)
}

// TestSocketMetrics_AbsentMetricsBecomeANilInterface pins the conversion that
// keeps the shared socket sink's "no metrics" path reachable: an optional
// *metrics has to arrive as an untyped nil, because a nil pointer inside a
// non-nil interface would be called through instead of skipped.
func TestSocketMetrics_AbsentMetricsBecomeANilInterface(t *testing.T) {
	var absent *metrics
	if sm := absent.socketMetrics(); sm != nil {
		t.Errorf("a nil *metrics converted to a non-nil sink.SocketMetrics (%T)", sm)
	}
	if sm := newMetrics().socketMetrics(); sm == nil {
		t.Error("real counters converted to a nil sink.SocketMetrics")
	}
}
