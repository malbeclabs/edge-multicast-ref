package main

import "testing"

// TestFramesTotal_LabelsSchemaVersion proves frames_total is registered and
// counts per port and wire schema version independently, which is what makes
// a publisher's v1-to-v3 cutover observable.
func TestFramesTotal_LabelsSchemaVersion(t *testing.T) {
	m := NewMetrics("test", "test")

	m.FramesTotal.WithLabelValues("refdata", "1").Inc()
	m.FramesTotal.WithLabelValues("refdata", "3").Inc()
	m.FramesTotal.WithLabelValues("refdata", "3").Inc()

	if got := readCounterVec(t, m.FramesTotal, "refdata", "1"); got != 1 {
		t.Errorf("v1 frames: got %v want 1", got)
	}
	if got := readCounterVec(t, m.FramesTotal, "refdata", "3"); got != 2 {
		t.Errorf("v3 frames: got %v want 2", got)
	}
}
