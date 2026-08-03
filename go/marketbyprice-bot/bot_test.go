package main

import (
	"strings"
	"testing"
)

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

func TestMetricsNamespace(t *testing.T) {
	m := NewMetrics("test", "abc123")
	// build_info is set inside NewMetrics and uptime_seconds is a GaugeFunc, so
	// both are gathered without any observation. Every *Vec reports no metric
	// family until a label set is observed, which is why they are not probed here.
	names := gatheredNames(t, m)
	var sawBuildInfo bool
	for _, n := range names {
		if n == "dz_mbp_bot_build_info" {
			sawBuildInfo = true
		}
		if strings.HasPrefix(n, "dz_mbo_") || strings.HasPrefix(n, "dz_tob_") {
			t.Errorf("metric %s registered under a sibling feed namespace", n)
		}
	}
	if !sawBuildInfo {
		t.Errorf("missing dz_mbp_bot_build_info in %v", names)
	}
}
