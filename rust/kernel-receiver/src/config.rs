use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DisplayMode {
    Tui,
    Log,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct NetworkConfig {
    pub interface: String,
    pub multicast_group: String,
    pub shred_port: u16,
    pub heartbeat_port: u16,
    pub recv_buffer_size: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            interface: "doublezero1".into(),
            multicast_group: "233.84.178.1".into(),
            shred_port: 7733,
            heartbeat_port: 5765,
            recv_buffer_size: 8_388_608,
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
pub struct Config {
    pub network: NetworkConfig,
    pub display: DisplayConfig,
    pub stats: StatsConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network: NetworkConfig::default(),
            display: DisplayConfig::default(),
            stats: StatsConfig::default(),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "edge-multicast-receiver")]
#[command(about = "Receive and monitor DoubleZero edge multicast shred feeds")]
pub struct Cli {
    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Network interface to bind to
    #[arg(long)]
    pub interface: Option<String>,

    /// Multicast group address
    #[arg(long)]
    pub multicast_group: Option<String>,

    /// Shred UDP port
    #[arg(long)]
    pub shred_port: Option<u16>,

    /// Heartbeat UDP port
    #[arg(long)]
    pub heartbeat_port: Option<u16>,

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

        if let Some(ref iface) = cli.interface {
            config.network.interface = iface.clone();
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
        if let Some(ref mode) = cli.mode {
            config.display.mode = match mode.as_str() {
                "log" => DisplayMode::Log,
                "tui" => DisplayMode::Tui,
                other => anyhow::bail!("unknown display mode: {}", other),
            };
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.network.interface, "doublezero1");
        assert_eq!(config.network.multicast_group, "233.84.178.1");
        assert_eq!(config.network.shred_port, 7733);
        assert_eq!(config.network.heartbeat_port, 5765);
        assert_eq!(config.network.recv_buffer_size, 8_388_608);
        assert_eq!(config.display.mode, DisplayMode::Tui);
        assert_eq!(config.display.refresh_hz, 4);
        assert_eq!(config.display.log_interval_secs, 5);
        assert_eq!(config.stats.max_slots, 32);
    }

    #[test]
    fn test_parse_example_toml() {
        let toml_str = r#"
[network]
interface = "eth0"
multicast_group = "239.0.0.1"
shred_port = 8000
heartbeat_port = 8001
recv_buffer_size = 4194304

[display]
mode = "log"
refresh_hz = 2
log_interval_secs = 10

[stats]
max_slots = 64
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.network.interface, "eth0");
        assert_eq!(config.network.shred_port, 8000);
        assert_eq!(config.display.mode, DisplayMode::Log);
        assert_eq!(config.stats.max_slots, 64);
    }

    #[test]
    fn test_partial_toml_uses_defaults() {
        let toml_str = r#"
[network]
interface = "mynic"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.network.interface, "mynic");
        assert_eq!(config.network.shred_port, 7733);
        assert_eq!(config.display.mode, DisplayMode::Tui);
    }
}
