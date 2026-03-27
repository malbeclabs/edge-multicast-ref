use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisplayMode {
    Tui,
    Log,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum XdpMode {
    Auto,
    Native,
    Skb,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    pub physical_interface: String,
    pub multicast_group: String,
    pub shred_port: u16,
    pub heartbeat_port: u16,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            physical_interface: "eth0".into(),
            multicast_group: "233.84.178.1".into(),
            shred_port: 7733,
            heartbeat_port: 5765,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct XdpConfig {
    pub xdp_mode: XdpMode,
    pub umem_size: usize,
    pub frame_size: usize,
    pub rx_queue: u32,
}

impl Default for XdpConfig {
    fn default() -> Self {
        Self {
            xdp_mode: XdpMode::Auto,
            umem_size: 4_194_304, // 4MB
            frame_size: 2048,
            rx_queue: 0,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DisplayConfig {
    pub mode: DisplayMode,
    pub refresh_hz: u32,
    pub log_interval_secs: u64,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        Self {
            mode: DisplayMode::Tui,
            refresh_hz: 4,
            log_interval_secs: 5,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StatsConfig {
    pub max_slots: usize,
}

impl Default for StatsConfig {
    fn default() -> Self {
        Self { max_slots: 32 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
#[derive(Default)]
pub struct Config {
    pub network: NetworkConfig,
    pub xdp: XdpConfig,
    pub display: DisplayConfig,
    pub stats: StatsConfig,
}

#[derive(Parser, Debug)]
#[command(name = "edge-multicast-xdp-receiver")]
#[command(about = "XDP-based receiver for DoubleZero edge multicast shred feeds")]
pub struct Cli {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Physical network interface (e.g. eth0, ens1f0)
    #[arg(long)]
    pub physical_interface: Option<String>,

    /// Multicast group address
    #[arg(long)]
    pub multicast_group: Option<String>,

    /// Shred UDP port
    #[arg(long)]
    pub shred_port: Option<u16>,

    /// Heartbeat UDP port
    #[arg(long)]
    pub heartbeat_port: Option<u16>,

    /// XDP attach mode: auto, native, skb
    #[arg(long)]
    pub xdp_mode: Option<String>,

    /// NIC RX queue to bind AF_XDP socket
    #[arg(long)]
    pub rx_queue: Option<u32>,

    /// Display mode: tui or log
    #[arg(long)]
    pub mode: Option<String>,
}

impl Config {
    pub fn load(cli: &Cli) -> anyhow::Result<Self> {
        let mut config = if cli.config.exists() {
            let contents = std::fs::read_to_string(&cli.config)?;
            toml::from_str(&contents)?
        } else {
            Config::default()
        };

        if let Some(ref iface) = cli.physical_interface {
            config.network.physical_interface = iface.clone();
        }
        if let Some(ref group) = cli.multicast_group {
            config.network.multicast_group = group.clone();
        }
        if let Some(port) = cli.shred_port {
            config.network.shred_port = port;
        }
        if let Some(port) = cli.heartbeat_port {
            config.network.heartbeat_port = port;
        }
        if let Some(ref mode) = cli.xdp_mode {
            config.xdp.xdp_mode = match mode.as_str() {
                "auto" => XdpMode::Auto,
                "native" => XdpMode::Native,
                "skb" => XdpMode::Skb,
                other => anyhow::bail!("unknown XDP mode: {other}"),
            };
        }
        if let Some(queue) = cli.rx_queue {
            config.xdp.rx_queue = queue;
        }
        if let Some(ref mode) = cli.mode {
            config.display.mode = match mode.as_str() {
                "log" => DisplayMode::Log,
                "tui" => DisplayMode::Tui,
                other => anyhow::bail!("unknown display mode: {other}"),
            };
        }

        Ok(config)
    }

    /// Number of UMEM frames, derived from umem_size / frame_size.
    pub fn frame_count(&self) -> usize {
        self.xdp.umem_size / self.xdp.frame_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.network.physical_interface, "eth0");
        assert_eq!(config.network.multicast_group, "233.84.178.1");
        assert_eq!(config.network.shred_port, 7733);
        assert_eq!(config.network.heartbeat_port, 5765);
        assert_eq!(config.xdp.xdp_mode, XdpMode::Auto);
        assert_eq!(config.xdp.umem_size, 4_194_304);
        assert_eq!(config.xdp.frame_size, 2048);
        assert_eq!(config.xdp.rx_queue, 0);
        assert_eq!(config.display.mode, DisplayMode::Tui);
        assert_eq!(config.display.refresh_hz, 4);
        assert_eq!(config.display.log_interval_secs, 5);
        assert_eq!(config.stats.max_slots, 32);
    }

    #[test]
    fn test_frame_count() {
        let config = Config::default();
        assert_eq!(config.frame_count(), 2048); // 4MB / 2048
    }

    #[test]
    fn test_parse_full_toml() {
        let toml_str = r#"
[network]
physical_interface = "ens1f0"
multicast_group = "239.0.0.1"
shred_port = 8000
heartbeat_port = 8001

[xdp]
xdp_mode = "native"
umem_size = 8388608
frame_size = 4096
rx_queue = 2

[display]
mode = "log"
refresh_hz = 2
log_interval_secs = 10

[stats]
max_slots = 64
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.network.physical_interface, "ens1f0");
        assert_eq!(config.network.shred_port, 8000);
        assert_eq!(config.xdp.xdp_mode, XdpMode::Native);
        assert_eq!(config.xdp.umem_size, 8_388_608);
        assert_eq!(config.xdp.frame_size, 4096);
        assert_eq!(config.xdp.rx_queue, 2);
        assert_eq!(config.display.mode, DisplayMode::Log);
        assert_eq!(config.stats.max_slots, 64);
    }

    #[test]
    fn test_partial_toml_uses_defaults() {
        let toml_str = r#"
[network]
physical_interface = "mlx5_0"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.network.physical_interface, "mlx5_0");
        assert_eq!(config.network.shred_port, 7733);
        assert_eq!(config.xdp.xdp_mode, XdpMode::Auto);
        assert_eq!(config.display.mode, DisplayMode::Tui);
    }
}
