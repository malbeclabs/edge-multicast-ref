// Package config provides shared configuration types for edge-multicast receivers.
package config

const (
	DisplayModeTUI = "tui"
	DisplayModeLog = "log"
)

// DisplayConfig controls how receiver output is presented.
type DisplayConfig struct {
	Mode            string `toml:"mode"`
	RefreshHz       int    `toml:"refresh_hz"`
	LogIntervalSecs int    `toml:"log_interval_secs"`
}

// DefaultDisplayConfig returns a DisplayConfig with sensible defaults.
func DefaultDisplayConfig() DisplayConfig {
	return DisplayConfig{
		Mode:            DisplayModeTUI,
		RefreshHz:       4,
		LogIntervalSecs: 5,
	}
}

// StatsConfig controls stats tracking behaviour.
type StatsConfig struct {
	MaxSlots int `toml:"max_slots"`
}

// DefaultStatsConfig returns a StatsConfig with sensible defaults.
func DefaultStatsConfig() StatsConfig {
	return StatsConfig{MaxSlots: 32}
}
