package main

import (
	"strings"
	"testing"
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
	m.FrameSeqGaps.WithLabelValues("mktdata", "10.0.0.1", "1").Inc()
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

	// This module must not register anything under a sibling feed's namespace.
	// Copying metrics.go from marketbyorder-parser and missing the namespace
	// constant is the exact mistake this guards.
	for _, n := range names {
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under a sibling feed namespace", n)
		}
	}
}

// TestDatagramsTotal_LabelsSchemaVersion proves datagrams_total is registered and
// counts per port and wire schema version independently, which is what makes
// a publisher's v1-to-v3 cutover observable.
func TestDatagramsTotal_LabelsSchemaVersion(t *testing.T) {
	m := NewMetrics("test", "test")

	m.FramesTotal.WithLabelValues("refdata", "1").Inc()
	m.FramesTotal.WithLabelValues("refdata", "3").Inc()
	m.FramesTotal.WithLabelValues("refdata", "3").Inc()

	// A CounterVec reports no metric family until a label set is observed
	// (see the comment above), so the registration check runs after the
	// increments rather than before.
	mustContain(t, gatheredNames(t, m), "dz_mbp_parser_datagrams_total")

	if got := readCounterVec(t, m.FramesTotal, "refdata", "1"); got != 1 {
		t.Errorf("v1 frames: got %v want 1", got)
	}
	if got := readCounterVec(t, m.FramesTotal, "refdata", "3"); got != 2 {
		t.Errorf("v3 frames: got %v want 2", got)
	}
}
