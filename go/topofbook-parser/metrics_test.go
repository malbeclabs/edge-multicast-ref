package main

import "testing"

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
