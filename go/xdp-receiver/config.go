package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/malbeclabs/edge-multicast-ref/go/internal/config"
)

// XdpMode controls how the XDP program is attached to the NIC.
type XdpMode string

const (
	XdpModeAuto   XdpMode = "auto"
	XdpModeNative XdpMode = "native"
	XdpModeSkb    XdpMode = "skb"
)

// NetworkConfig holds XDP-receiver specific network settings.
type NetworkConfig struct {
	PhysicalInterface string `toml:"physical_interface"`
	MulticastGroup    string `toml:"multicast_group"`
	ShredPort         uint16 `toml:"shred_port"`
	HeartbeatPort     uint16 `toml:"heartbeat_port"`
}

// XdpConfig holds XDP and AF_XDP socket settings.
type XdpConfig struct {
	XdpMode   XdpMode `toml:"xdp_mode"`
	UmemSize  int     `toml:"umem_size"`
	FrameSize int     `toml:"frame_size"`
	RxQueue   uint32  `toml:"rx_queue"`
}

// Config is the top-level configuration for the XDP receiver.
type Config struct {
	Network NetworkConfig        `toml:"network"`
	Xdp     XdpConfig            `toml:"xdp"`
	Display config.DisplayConfig `toml:"display"`
	Stats   config.StatsConfig   `toml:"stats"`
}

// FrameCount returns the number of UMEM frames (umem_size / frame_size).
func (c *Config) FrameCount() int {
	return c.Xdp.UmemSize / c.Xdp.FrameSize
}

// DefaultConfig returns a Config populated with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Network: NetworkConfig{
			PhysicalInterface: "eth0",
			MulticastGroup:    "233.84.178.1",
			ShredPort:         7733,
			HeartbeatPort:     5765,
		},
		Xdp: XdpConfig{
			XdpMode:   XdpModeAuto,
			UmemSize:  4194304, // 4MB
			FrameSize: 2048,
			RxQueue:   0,
		},
		Display: config.DefaultDisplayConfig(),
		Stats:   config.DefaultStatsConfig(),
	}
}

// CLIFlags holds parsed command-line arguments.
type CLIFlags struct {
	ConfigPath        string
	PhysicalInterface string
	MulticastGroup    string
	ShredPort         int
	HeartbeatPort     int
	XdpMode           string
	RxQueue           int
	UmemSize          int
	Mode              string
}

// ParseFlags parses command-line flags and returns the result.
func ParseFlags() *CLIFlags {
	cli := &CLIFlags{}
	flag.StringVar(&cli.ConfigPath, "config", "", "path to TOML config file")
	flag.StringVar(&cli.PhysicalInterface, "physical-interface", "", "physical network interface (e.g. eth0, ens1f0)")
	flag.StringVar(&cli.MulticastGroup, "multicast-group", "", "multicast group IP address")
	flag.IntVar(&cli.ShredPort, "shred-port", 0, "UDP port for shred data")
	flag.IntVar(&cli.HeartbeatPort, "heartbeat-port", 0, "UDP port for heartbeat data")
	flag.StringVar(&cli.XdpMode, "xdp-mode", "", "XDP attach mode: auto, native, skb")
	flag.IntVar(&cli.RxQueue, "rx-queue", -1, "NIC RX queue to bind AF_XDP socket")
	flag.IntVar(&cli.UmemSize, "umem-size", 0, "UMEM size in bytes")
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
	if cli.PhysicalInterface != "" {
		cfg.Network.PhysicalInterface = cli.PhysicalInterface
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
	if cli.XdpMode != "" {
		switch cli.XdpMode {
		case "auto":
			cfg.Xdp.XdpMode = XdpModeAuto
		case "native":
			cfg.Xdp.XdpMode = XdpModeNative
		case "skb":
			cfg.Xdp.XdpMode = XdpModeSkb
		default:
			return nil, fmt.Errorf("unknown XDP mode: %s", cli.XdpMode)
		}
	}
	if cli.RxQueue >= 0 {
		cfg.Xdp.RxQueue = uint32(cli.RxQueue)
	}
	if cli.UmemSize > 0 {
		cfg.Xdp.UmemSize = cli.UmemSize
	}
	if cli.Mode != "" {
		cfg.Display.Mode = cli.Mode
	}

	return &cfg, nil
}
