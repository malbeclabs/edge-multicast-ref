package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/config"
)

// NetworkConfig holds kernel-receiver specific network settings.
type NetworkConfig struct {
	Interface      string `toml:"interface"`
	MulticastGroup string `toml:"multicast_group"`
	ShredPort      uint16 `toml:"shred_port"`
	HeartbeatPort  uint16 `toml:"heartbeat_port"`
	RecvBufferSize int    `toml:"recv_buffer_size"`
}

// Config is the top-level configuration for the kernel receiver.
type Config struct {
	Network NetworkConfig        `toml:"network"`
	Display config.DisplayConfig `toml:"display"`
	Stats   config.StatsConfig   `toml:"stats"`
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Network: NetworkConfig{
			Interface:      "doublezero1",
			MulticastGroup: "233.84.178.1",
			ShredPort:      7733,
			HeartbeatPort:  5765,
			RecvBufferSize: 8388608,
		},
		Display: config.DefaultDisplayConfig(),
		Stats:   config.DefaultStatsConfig(),
	}
}

// CLIFlags holds parsed command-line arguments.
type CLIFlags struct {
	ConfigPath     string
	Interface      string
	MulticastGroup string
	ShredPort      int
	HeartbeatPort  int
	Mode           string
}

// ParseFlags parses command-line flags and returns the result.
func ParseFlags() *CLIFlags {
	cli := &CLIFlags{}
	flag.StringVar(&cli.ConfigPath, "config", "", "path to TOML config file")
	flag.StringVar(&cli.Interface, "interface", "", "network interface name")
	flag.StringVar(&cli.MulticastGroup, "multicast-group", "", "multicast group IP address")
	flag.IntVar(&cli.ShredPort, "shred-port", 0, "UDP port for shred data")
	flag.IntVar(&cli.HeartbeatPort, "heartbeat-port", 0, "UDP port for heartbeat data")
	flag.StringVar(&cli.Mode, "mode", "", "display mode (log or tui)")
	flag.Parse()
	return cli
}

// LoadConfig loads configuration from TOML file (if it exists) and applies CLI overrides.
func LoadConfig(configPath string, cli *CLIFlags) (*Config, error) {
	cfg := DefaultConfig()

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
		if err := toml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("parsing config file: %w", err)
		}
	}

	// Apply CLI overrides for explicitly set flags.
	if cli.Interface != "" {
		cfg.Network.Interface = cli.Interface
	}
	if cli.MulticastGroup != "" {
		cfg.Network.MulticastGroup = cli.MulticastGroup
	}
	if cli.ShredPort != 0 {
		cfg.Network.ShredPort = uint16(cli.ShredPort)
	}
	if cli.HeartbeatPort != 0 {
		cfg.Network.HeartbeatPort = uint16(cli.HeartbeatPort)
	}
	if cli.Mode != "" {
		cfg.Display.Mode = cli.Mode
	}

	return &cfg, nil
}
