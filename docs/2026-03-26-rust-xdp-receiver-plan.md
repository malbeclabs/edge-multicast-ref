# Rust XDP Multicast Shred Receiver — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an XDP-based Rust receiver that attaches an eBPF filter to a physical NIC, redirects GRE-encapsulated Solana shred packets to an AF_XDP socket, and displays live statistics via TUI or log — same output interface as the kernel-socket receiver.

**Architecture:** An eBPF XDP program (aya-ebpf) parses Eth→outerIP→GRE→innerIP→UDP headers and redirects matching packets to an AF_XDP socket. Rust userspace (aya + xsk-rs) reads packets from the AF_XDP RX ring, strips encapsulation headers, parses shred payloads with `solana-ledger`, and updates shared stats. Display thread runs ratatui TUI or streaming log with XDP-specific counters.

**Tech Stack:** Rust, aya/aya-ebpf (eBPF), xsk-rs (AF_XDP), solana-ledger (shred parsing), ratatui/crossterm (TUI), clap/serde/toml (config)

**Spec:** `docs/2026-03-26-rust-xdp-receiver-design.md`

**Platform:** Linux only. eBPF compilation requires nightly Rust + bpf-linker.

---

## File Map

| File | Responsibility |
|---|---|
| `rust/xdp-receiver/Cargo.toml` | Userspace binary crate manifest |
| `rust/xdp-receiver/build.rs` | Compiles eBPF crate via aya-build, outputs ELF to `$OUT_DIR` |
| `rust/xdp-receiver/config.example.toml` | Example configuration file with `[xdp]` section |
| `rust/xdp-receiver/src/main.rs` | CLI, config loading, XDP attach, AF_XDP setup, thread spawning, shutdown |
| `rust/xdp-receiver/src/config.rs` | Config with `physical_interface` and `[xdp]` section (adapted from kernel-receiver) |
| `rust/xdp-receiver/src/stats.rs` | Stats with XDP-specific counters (adapted from kernel-receiver) |
| `rust/xdp-receiver/src/shred_parser.rs` | Shred parsing (verbatim copy from kernel-receiver) |
| `rust/xdp-receiver/src/xdp.rs` | eBPF program loading, XDP attach, BPF map configuration |
| `rust/xdp-receiver/src/receiver.rs` | AF_XDP socket setup, UMEM, RX ring polling, GRE header stripping |
| `rust/xdp-receiver/src/display/mod.rs` | Display mode dispatch (verbatim from kernel-receiver) |
| `rust/xdp-receiver/src/display/tui.rs` | ratatui dashboard with XDP stats panel (adapted) |
| `rust/xdp-receiver/src/display/log.rs` | Streaming logger with XDP stats (adapted) |
| `rust/xdp-receiver/ebpf/Cargo.toml` | eBPF crate manifest (bpfel-unknown-none target) |
| `rust/xdp-receiver/ebpf/.cargo/config.toml` | eBPF build target + build-std config |
| `rust/xdp-receiver/ebpf/rust-toolchain.toml` | Nightly toolchain for eBPF crate |
| `rust/xdp-receiver/ebpf/src/main.rs` | XDP eBPF program: GRE parse, filter, redirect to AF_XDP |
| `rust/xdp-receiver/common/Cargo.toml` | Shared types crate (no_std compatible) |
| `rust/xdp-receiver/common/src/lib.rs` | `FilterConfig` and `XdpStats` repr(C) structs |

---

### Task 1: Project Scaffold + Common Crate

**Files:**
- Create: `rust/xdp-receiver/Cargo.toml`
- Create: `rust/xdp-receiver/common/Cargo.toml`
- Create: `rust/xdp-receiver/common/src/lib.rs`
- Create: `rust/xdp-receiver/ebpf/Cargo.toml`
- Create: `rust/xdp-receiver/ebpf/.cargo/config.toml`
- Create: `rust/xdp-receiver/ebpf/rust-toolchain.toml`
- Create: `rust/xdp-receiver/ebpf/src/main.rs` (stub)
- Create: `rust/xdp-receiver/src/main.rs` (stub)
- Create: stub files for all modules

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p rust/xdp-receiver/src/display
mkdir -p rust/xdp-receiver/ebpf/src
mkdir -p rust/xdp-receiver/ebpf/.cargo
mkdir -p rust/xdp-receiver/common/src
```

- [ ] **Step 2: Write common/Cargo.toml**

```toml
[package]
name = "xdp-filter-common"
version = "0.1.0"
edition = "2021"

[features]
default = []
userspace = ["aya"]

[dependencies]
aya = { version = "0.13", optional = true, default-features = false }
```

- [ ] **Step 3: Write common/src/lib.rs**

```rust
#![no_std]

/// Filter configuration written to BPF Array map by userspace, read by eBPF program.
/// All values are stored in host byte order. The eBPF program converts packet bytes
/// from network order via from_be() before comparing.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FilterConfig {
    pub multicast_ip: u32,
    pub shred_port: u16,
    pub heartbeat_port: u16,
}

/// Per-CPU statistics counters updated by eBPF program, read by userspace.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XdpStats {
    pub redirected: u64,
    pub passed: u64,
    pub errors: u64,
}

#[cfg(feature = "userspace")]
unsafe impl aya::Pod for FilterConfig {}

#[cfg(feature = "userspace")]
unsafe impl aya::Pod for XdpStats {}
```

- [ ] **Step 4: Write ebpf/Cargo.toml**

```toml
[package]
name = "xdp-filter"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "xdp-filter"
path = "src/main.rs"

[dependencies]
aya-ebpf = "0.1"
aya-log-ebpf = "0.1"
xdp-filter-common = { path = "../common" }

[profile.release]
panic = "abort"
```

- [ ] **Step 5: Write ebpf/.cargo/config.toml**

```toml
[build]
target = "bpfel-unknown-none"

[unstable]
build-std = ["core"]
```

- [ ] **Step 6: Write ebpf/rust-toolchain.toml**

```toml
[toolchain]
channel = "nightly"
components = ["rust-src"]
```

- [ ] **Step 7: Write ebpf/src/main.rs (stub)**

```rust
#![no_std]
#![no_main]

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
```

- [ ] **Step 8: Write userspace Cargo.toml**

```toml
[package]
name = "edge-multicast-xdp-receiver"
version = "0.1.0"
edition = "2021"
description = "XDP-based receiver for DoubleZero edge multicast shred feeds"

[dependencies]
# Solana - upstream Agave
solana-ledger = "2.2"
solana-sdk = "2.2"

# eBPF / XDP
aya = "0.13"
aya-log = "0.2"

# AF_XDP
xsk-rs = "0.6"

# CLI + Config
clap = { version = "4", features = ["derive"] }
toml = "0.8"
serde = { version = "1", features = ["derive"] }

# TUI
ratatui = "0.29"
crossterm = "0.28"

# System
libc = "0.2"

# Signal handling
ctrlc = { version = "3", features = ["termination"] }

# Misc
anyhow = "1"

# Shared types
xdp-filter-common = { path = "common", features = ["userspace"] }

[build-dependencies]
aya-build = "0.1"
cargo_metadata = "0.19"
```

**Note:** `xsk-rs` version should be verified against latest crates.io at build time. The API patterns in this plan are based on xsk-rs 0.6.x. If the API has changed, adapt accordingly.

- [ ] **Step 9: Write build.rs**

```rust
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=ebpf/src");
    println!("cargo:rerun-if-changed=ebpf/Cargo.toml");

    // Only build eBPF on Linux (requires nightly + bpf-linker)
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap() == "linux" {
        let ebpf_dir = PathBuf::from("ebpf");

        let metadata = cargo_metadata::MetadataCommand::new()
            .manifest_path(ebpf_dir.join("Cargo.toml"))
            .no_deps()
            .exec()
            .expect("failed to get ebpf crate metadata");

        let ebpf_package = metadata
            .packages
            .iter()
            .find(|p| p.name == "xdp-filter")
            .expect("xdp-filter package not found in ebpf/Cargo.toml");

        let pkg = aya_build::Package {
            name: ebpf_package.name.clone(),
            root_dir: ebpf_package
                .manifest_path
                .parent()
                .unwrap()
                .as_std_path()
                .to_path_buf(),
            ..Default::default()
        };

        aya_build::build_ebpf(vec![pkg], aya_build::Toolchain::default())
            .expect("failed to build eBPF program");
    } else {
        eprintln!("cargo:warning=Skipping eBPF build on non-Linux platform. XDP features will not be available.");
    }
}
```

- [ ] **Step 10: Write src/main.rs (stub)**

```rust
use anyhow::Result;

mod config;
mod display;
mod receiver;
mod shred_parser;
mod stats;
mod xdp;

fn main() -> Result<()> {
    println!("edge-multicast-xdp-receiver starting...");
    Ok(())
}
```

- [ ] **Step 11: Create module stubs**

```bash
touch rust/xdp-receiver/src/config.rs
touch rust/xdp-receiver/src/stats.rs
touch rust/xdp-receiver/src/shred_parser.rs
touch rust/xdp-receiver/src/receiver.rs
touch rust/xdp-receiver/src/xdp.rs
```

`src/display/mod.rs`:
```rust
pub mod log;
pub mod tui;
```

```bash
touch rust/xdp-receiver/src/display/tui.rs
touch rust/xdp-receiver/src/display/log.rs
```

- [ ] **Step 12: Verify common crate compiles**

```bash
cd rust/xdp-receiver/common && cargo check
```

Expected: compiles clean.

- [ ] **Step 13: Verify userspace crate compiles (on any platform)**

```bash
cd rust/xdp-receiver && cargo check
```

Expected: compiles with warnings about unused modules. On non-Linux, prints warning about skipping eBPF build.

- [ ] **Step 14: Commit**

```bash
git add rust/xdp-receiver/
git commit -m "scaffold: xdp-receiver project with ebpf and common crates"
```

---

### Task 2: Config Module (TDD)

**Files:**
- Create: `rust/xdp-receiver/src/config.rs`

Adapted from `rust/kernel-receiver/src/config.rs`. Changes: replaces `interface` with `physical_interface`, removes `recv_buffer_size`, adds `[xdp]` section with `xdp_mode`, `umem_size`, `frame_size`, `rx_queue`.

- [ ] **Step 1: Write config module with tests**

Write to `rust/xdp-receiver/src/config.rs`:

```rust
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
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cd rust/xdp-receiver && cargo test --lib config
```

Expected: 4 tests pass.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/src/config.rs
git commit -m "feat(xdp): config module with XDP section, TOML parsing, CLI overrides"
```

---

### Task 3: Stats Module (TDD)

**Files:**
- Create: `rust/xdp-receiver/src/stats.rs`

Adapted from `rust/kernel-receiver/src/stats.rs`. Adds XDP-specific counters: `xdp_attach_mode`, `xdp_redirected`, `xdp_passed`, `xdp_errors`, `afxdp_rx_fill_level`, `afxdp_fill_starvation`.

- [ ] **Step 1: Write stats module with tests**

Write to `rust/xdp-receiver/src/stats.rs`:

```rust
use std::collections::{BTreeMap, HashSet};
use std::time::Instant;

/// First 8 bytes of the shred signature, used as a proxy leader identifier.
pub type SignaturePrefix = [u8; 8];

#[derive(Debug, Clone)]
pub struct SlotStats {
    pub slot: u64,
    pub data_shred_count: u64,
    pub coding_shred_count: u64,
    pub highest_data_index: u32,
    pub fec_set_count: usize,
    pub signature_prefix: SignaturePrefix,
    pub first_seen: Instant,
    pub last_seen: Instant,
    fec_set_indices: HashSet<u32>,
}

impl SlotStats {
    fn new(slot: u64, signature: [u8; 64]) -> Self {
        let now = Instant::now();
        let mut sig_prefix = [0u8; 8];
        sig_prefix.copy_from_slice(&signature[..8]);
        Self {
            slot,
            data_shred_count: 0,
            coding_shred_count: 0,
            highest_data_index: 0,
            fec_set_count: 0,
            signature_prefix: sig_prefix,
            first_seen: now,
            last_seen: now,
            fec_set_indices: HashSet::new(),
        }
    }

    fn record(&mut self, is_data: bool, index: u32, fec_set_index: u32) {
        self.last_seen = Instant::now();
        if is_data {
            self.data_shred_count += 1;
            if index > self.highest_data_index {
                self.highest_data_index = index;
            }
        } else {
            self.coding_shred_count += 1;
        }
        self.fec_set_indices.insert(fec_set_index);
        self.fec_set_count = self.fec_set_indices.len();
    }
}

#[derive(Debug)]
pub struct Stats {
    pub total_data_shreds: u64,
    pub total_coding_shreds: u64,
    pub total_heartbeats: u64,
    pub parse_errors: u64,
    pub last_heartbeat: Option<Instant>,
    pub start_time: Instant,

    /// Recent slots ordered by slot number. Bounded by `max_slots`.
    pub slots: BTreeMap<u64, SlotStats>,
    max_slots: usize,

    /// Timestamps of recent shreds for rate calculation.
    rate_window: Vec<Instant>,

    // --- XDP-specific counters ---
    /// XDP attach mode string ("native", "skb", or "unknown").
    pub xdp_attach_mode: String,
    /// Packets matched filter and redirected to AF_XDP (from BPF stats map).
    pub xdp_redirected: u64,
    /// Packets that didn't match and went to kernel (from BPF stats map).
    pub xdp_passed: u64,
    /// Packets that failed eBPF parsing (from BPF stats map).
    pub xdp_errors: u64,
    /// Current AF_XDP fill ring occupancy.
    pub afxdp_rx_fill_level: usize,
    /// Times the fill ring was empty when kernel needed a frame.
    pub afxdp_fill_starvation: u64,
}

impl Stats {
    pub fn new(max_slots: usize) -> Self {
        Self {
            total_data_shreds: 0,
            total_coding_shreds: 0,
            total_heartbeats: 0,
            parse_errors: 0,
            last_heartbeat: None,
            start_time: Instant::now(),
            slots: BTreeMap::new(),
            max_slots,
            rate_window: Vec::new(),
            xdp_attach_mode: "unknown".into(),
            xdp_redirected: 0,
            xdp_passed: 0,
            xdp_errors: 0,
            afxdp_rx_fill_level: 0,
            afxdp_fill_starvation: 0,
        }
    }

    pub fn record_shred(
        &mut self,
        slot: u64,
        is_data: bool,
        index: u32,
        fec_set_index: u32,
        signature: [u8; 64],
    ) {
        if is_data {
            self.total_data_shreds += 1;
        } else {
            self.total_coding_shreds += 1;
        }

        let slot_stats = self
            .slots
            .entry(slot)
            .or_insert_with(|| SlotStats::new(slot, signature));
        slot_stats.record(is_data, index, fec_set_index);

        while self.slots.len() > self.max_slots {
            if let Some((&oldest, _)) = self.slots.iter().next() {
                self.slots.remove(&oldest);
            }
        }

        self.rate_window.push(Instant::now());
    }

    pub fn record_heartbeat(&mut self) {
        self.total_heartbeats += 1;
        self.last_heartbeat = Some(Instant::now());
    }

    pub fn record_parse_error(&mut self) {
        self.parse_errors += 1;
    }

    pub fn shreds_per_second(&mut self) -> f64 {
        let now = Instant::now();
        let one_sec_ago = now - std::time::Duration::from_secs(1);
        self.rate_window.retain(|t| *t >= one_sec_ago);
        self.rate_window.len() as f64
    }

    pub fn get_slot(&self, slot: u64) -> Option<&SlotStats> {
        self.slots.get(&slot)
    }

    /// Returns recent slots in descending order (newest first).
    pub fn recent_slots(&self) -> Vec<&SlotStats> {
        self.slots.values().rev().collect()
    }

    /// Update XDP program counters from BPF stats map values.
    pub fn update_xdp_counters(&mut self, redirected: u64, passed: u64, errors: u64) {
        self.xdp_redirected = redirected;
        self.xdp_passed = passed;
        self.xdp_errors = errors;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats_has_xdp_fields() {
        let stats = Stats::new(4);
        assert_eq!(stats.xdp_attach_mode, "unknown");
        assert_eq!(stats.xdp_redirected, 0);
        assert_eq!(stats.xdp_passed, 0);
        assert_eq!(stats.xdp_errors, 0);
        assert_eq!(stats.afxdp_rx_fill_level, 0);
        assert_eq!(stats.afxdp_fill_starvation, 0);
    }

    #[test]
    fn test_update_xdp_counters() {
        let mut stats = Stats::new(4);
        stats.update_xdp_counters(100, 50, 3);
        assert_eq!(stats.xdp_redirected, 100);
        assert_eq!(stats.xdp_passed, 50);
        assert_eq!(stats.xdp_errors, 3);
    }

    #[test]
    fn test_record_shred_data() {
        let mut stats = Stats::new(4);
        stats.record_shred(100, true, 0, 0, [0xAB; 64]);
        assert_eq!(stats.total_data_shreds, 1);
        assert_eq!(stats.total_coding_shreds, 0);
        assert_eq!(stats.slots.len(), 1);

        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.slot, 100);
        assert_eq!(slot.data_shred_count, 1);
    }

    #[test]
    fn test_record_shred_coding() {
        let mut stats = Stats::new(4);
        stats.record_shred(100, false, 5, 0, [0xAB; 64]);
        assert_eq!(stats.total_coding_shreds, 1);
        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.coding_shred_count, 1);
    }

    #[test]
    fn test_multiple_shreds_same_slot() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        stats.record_shred(100, true, 0, 0, sig);
        stats.record_shred(100, true, 1, 0, sig);
        stats.record_shred(100, true, 5, 1, sig);
        stats.record_shred(100, false, 0, 0, sig);

        let slot = stats.get_slot(100).unwrap();
        assert_eq!(slot.data_shred_count, 3);
        assert_eq!(slot.coding_shred_count, 1);
        assert_eq!(slot.highest_data_index, 5);
        assert_eq!(slot.fec_set_count, 2);
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        for slot in 0..6 {
            stats.record_shred(slot, true, 0, 0, sig);
        }
        assert_eq!(stats.slots.len(), 4);
        assert!(stats.get_slot(0).is_none());
        assert!(stats.get_slot(1).is_none());
        assert!(stats.get_slot(2).is_some());
        assert!(stats.get_slot(5).is_some());
    }

    #[test]
    fn test_heartbeat_counting() {
        let mut stats = Stats::new(4);
        stats.record_heartbeat();
        stats.record_heartbeat();
        assert_eq!(stats.total_heartbeats, 2);
        assert!(stats.last_heartbeat.is_some());
    }

    #[test]
    fn test_recent_slots_descending() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        stats.record_shred(100, true, 0, 0, sig);
        stats.record_shred(200, true, 0, 0, sig);
        stats.record_shred(150, true, 0, 0, sig);

        let recent = stats.recent_slots();
        assert_eq!(recent[0].slot, 200);
        assert_eq!(recent[1].slot, 150);
        assert_eq!(recent[2].slot, 100);
    }
}
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
cd rust/xdp-receiver && cargo test --lib stats
```

Expected: 8 tests pass.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/src/stats.rs
git commit -m "feat(xdp): stats module with XDP-specific counters"
```

---

### Task 4: Shred Parser (Copy)

**Files:**
- Create: `rust/xdp-receiver/src/shred_parser.rs`

Verbatim copy from `rust/kernel-receiver/src/shred_parser.rs`. No changes needed.

- [ ] **Step 1: Copy shred_parser.rs**

```bash
cp rust/kernel-receiver/src/shred_parser.rs rust/xdp-receiver/src/shred_parser.rs
```

- [ ] **Step 2: Run tests to verify**

```bash
cd rust/xdp-receiver && cargo test --lib shred_parser
```

Expected: 3 tests pass.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/src/shred_parser.rs
git commit -m "feat(xdp): shred parser (verbatim copy from kernel-receiver)"
```

---

### Task 5: eBPF XDP Filter Program

**Files:**
- Modify: `rust/xdp-receiver/ebpf/src/main.rs`

This is the XDP eBPF program that parses GRE-encapsulated packets and redirects matches to AF_XDP. Cannot be unit tested — requires Linux kernel with eBPF support.

- [ ] **Step 1: Write the eBPF program**

Write to `rust/xdp-receiver/ebpf/src/main.rs`:

```rust
#![no_std]
#![no_main]

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::{Array, PerCpuArray, XskMap},
    programs::XdpContext,
};
use xdp_filter_common::{FilterConfig, XdpStats};

#[map]
static CONFIG: Array<FilterConfig> = Array::with_max_entries(1, 0);

#[map]
static STATS: PerCpuArray<XdpStats> = PerCpuArray::with_max_entries(1, 0);

#[map]
static XSKMAP: XskMap = XskMap::with_max_entries(8, 0);

const ETH_P_IP: u16 = 0x0800;
const IPPROTO_GRE: u8 = 47;
const IPPROTO_UDP: u8 = 17;
const ETH_HDR_LEN: usize = 14;
const IPV4_HDR_MIN_LEN: usize = 20;
const GRE_HDR_MIN_LEN: usize = 4;

#[inline(always)]
fn inc_redirected() {
    if let Some(stats) = unsafe { STATS.get_ptr_mut(0) } {
        unsafe { (*stats).redirected += 1 };
    }
}

#[inline(always)]
fn inc_passed() {
    if let Some(stats) = unsafe { STATS.get_ptr_mut(0) } {
        unsafe { (*stats).passed += 1 };
    }
}

#[inline(always)]
fn inc_errors() {
    if let Some(stats) = unsafe { STATS.get_ptr_mut(0) } {
        unsafe { (*stats).errors += 1 };
    }
}

/// Read a u16 from packet data, converting from network byte order to host order.
#[inline(always)]
unsafe fn read_u16(data: usize, data_end: usize, offset: usize) -> Option<u16> {
    if data + offset + 2 > data_end {
        return None;
    }
    Some(u16::from_be(((data + offset) as *const u16).read_unaligned()))
}

/// Read a u8 from packet data.
#[inline(always)]
unsafe fn read_u8(data: usize, data_end: usize, offset: usize) -> Option<u8> {
    if data + offset + 1 > data_end {
        return None;
    }
    Some(*((data + offset) as *const u8))
}

/// Read a u32 from packet data, converting from network byte order to host order.
#[inline(always)]
unsafe fn read_u32(data: usize, data_end: usize, offset: usize) -> Option<u32> {
    if data + offset + 4 > data_end {
        return None;
    }
    Some(u32::from_be(((data + offset) as *const u32).read_unaligned()))
}

#[xdp]
pub fn xdp_filter(ctx: XdpContext) -> u32 {
    match try_xdp_filter(&ctx) {
        Ok(action) => action,
        Err(_) => {
            inc_errors();
            xdp_action::XDP_PASS
        }
    }
}

fn try_xdp_filter(ctx: &XdpContext) -> Result<u32, ()> {
    let data = ctx.data() as usize;
    let data_end = ctx.data_end() as usize;

    // Load filter config from BPF map
    let cfg = unsafe { CONFIG.get(0) }.ok_or(())?;

    // 1. Parse Ethernet header (14 bytes)
    let ethertype = unsafe { read_u16(data, data_end, 12) }.ok_or(())?;
    if ethertype != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    let mut offset = ETH_HDR_LEN;

    // 2. Parse outer IPv4 header
    let outer_ihl_byte = unsafe { read_u8(data, data_end, offset) }.ok_or(())?;
    let outer_ihl = ((outer_ihl_byte & 0x0F) as usize) * 4;
    if outer_ihl < IPV4_HDR_MIN_LEN {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let outer_proto = unsafe { read_u8(data, data_end, offset + 9) }.ok_or(())?;
    if outer_proto != IPPROTO_GRE {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    offset += outer_ihl;

    // 3. Parse GRE header (minimum 4 bytes)
    if data + offset + GRE_HDR_MIN_LEN > data_end {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let gre_flags = unsafe { read_u16(data, data_end, offset) }.ok_or(())?;
    let gre_protocol = unsafe { read_u16(data, data_end, offset + 2) }.ok_or(())?;
    if gre_protocol != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // Calculate GRE header length based on C/K/S flags
    let mut gre_len = GRE_HDR_MIN_LEN;
    if gre_flags & 0x8000 != 0 {
        gre_len += 4; // Checksum + Reserved1
    }
    if gre_flags & 0x2000 != 0 {
        gre_len += 4; // Key
    }
    if gre_flags & 0x1000 != 0 {
        gre_len += 4; // Sequence Number
    }
    offset += gre_len;

    // 4. Parse inner IPv4 header
    let inner_ihl_byte = unsafe { read_u8(data, data_end, offset) }.ok_or(())?;
    let inner_ihl = ((inner_ihl_byte & 0x0F) as usize) * 4;
    if inner_ihl < IPV4_HDR_MIN_LEN {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let inner_proto = unsafe { read_u8(data, data_end, offset + 9) }.ok_or(())?;
    if inner_proto != IPPROTO_UDP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    // Check destination IP matches configured multicast group
    let inner_dst_ip = unsafe { read_u32(data, data_end, offset + 16) }.ok_or(())?;
    if inner_dst_ip != cfg.multicast_ip {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    offset += inner_ihl;

    // 5. Parse UDP header — check destination port
    let udp_dst_port = unsafe { read_u16(data, data_end, offset + 2) }.ok_or(())?;
    if udp_dst_port != cfg.shred_port && udp_dst_port != cfg.heartbeat_port {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // 6. Match! Redirect to AF_XDP socket
    inc_redirected();
    let queue_id = unsafe { (*ctx.ctx).rx_queue_index };
    XSKMAP.redirect(queue_id, 0).map_err(|_| ())
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
```

- [ ] **Step 2: Verify eBPF compiles (Linux only)**

```bash
cd rust/xdp-receiver/ebpf && cargo +nightly build --release
```

Expected: compiles to `target/bpfel-unknown-none/release/xdp-filter`. If `bpf-linker` is not installed, install first: `cargo install bpf-linker`.

**On non-Linux:** This step cannot be run. Skip and verify during integration testing on Linux.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/ebpf/src/main.rs
git commit -m "feat(xdp): eBPF XDP filter program with GRE parsing and AF_XDP redirect"
```

---

### Task 6: XDP Loader Module

**Files:**
- Create: `rust/xdp-receiver/src/xdp.rs`

Handles loading the eBPF ELF, attaching the XDP program to the NIC, and configuring BPF maps.

- [ ] **Step 1: Write xdp.rs**

```rust
use std::net::Ipv4Addr;

use anyhow::{Context, Result};
use aya::maps::{Array, PerCpuArray, XskMap};
use aya::programs::xdp::{Xdp, XdpFlags};
use aya::Ebpf;

use crate::config::{Config, XdpMode};
use xdp_filter_common::{FilterConfig, XdpStats};

/// Loaded and attached XDP program state.
/// Detaches the XDP program on drop (aya handles this via owned links).
pub struct XdpHandle {
    pub ebpf: Ebpf,
    pub interface: String,
}

impl Drop for XdpHandle {
    fn drop(&mut self) {
        eprintln!("Detaching XDP program from {}...", self.interface);
    }
}

/// Load the eBPF program, attach XDP to the interface, and write filter config to BPF maps.
/// Returns the XdpHandle (owns the Ebpf instance) and the actual attach mode string.
pub fn attach_xdp(config: &Config) -> Result<(XdpHandle, String)> {
    #[cfg(target_os = "linux")]
    let mut ebpf = Ebpf::load(aya::include_bytes_aligned!(concat!(
        env!("OUT_DIR"),
        "/xdp-filter"
    )))
    .context("failed to load eBPF program")?;

    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("XDP is only supported on Linux");

    // Load and attach the XDP program
    let program: &mut Xdp = ebpf
        .program_mut("xdp_filter")
        .context("XDP program 'xdp_filter' not found in eBPF ELF")?
        .try_into()
        .context("program is not an XDP program")?;
    program.load().context("failed to load XDP program")?;

    let actual_mode = match config.xdp.xdp_mode {
        XdpMode::Auto => {
            // Try native first, fall back to SKB
            match program.attach(&config.network.physical_interface, XdpFlags::DRV_MODE) {
                Ok(_) => "native".to_string(),
                Err(_) => {
                    eprintln!("Native XDP attach failed, falling back to SKB mode");
                    program
                        .attach(&config.network.physical_interface, XdpFlags::SKB_MODE)
                        .context("failed to attach XDP program in SKB mode")?;
                    "skb".to_string()
                }
            }
        }
        XdpMode::Native => {
            program
                .attach(&config.network.physical_interface, XdpFlags::DRV_MODE)
                .context("failed to attach XDP program in native mode")?;
            "native".to_string()
        }
        XdpMode::Skb => {
            program
                .attach(&config.network.physical_interface, XdpFlags::SKB_MODE)
                .context("failed to attach XDP program in SKB mode")?;
            "skb".to_string()
        }
    };

    // Write filter config to BPF map
    let multicast_ip: Ipv4Addr = config
        .network
        .multicast_group
        .parse()
        .context("parsing multicast group IP")?;

    let filter_config = FilterConfig {
        multicast_ip: u32::from(multicast_ip),
        shred_port: config.network.shred_port,
        heartbeat_port: config.network.heartbeat_port,
    };

    let mut config_map: Array<_, FilterConfig> =
        Array::try_from(ebpf.map_mut("CONFIG").context("CONFIG map not found")?)
            .context("CONFIG map is not an Array")?;
    config_map
        .set(0, filter_config, 0)
        .context("failed to write filter config to BPF map")?;

    eprintln!(
        "XDP program attached to {} in {} mode",
        config.network.physical_interface, actual_mode
    );
    eprintln!(
        "Filter: multicast={}, shred_port={}, heartbeat_port={}",
        config.network.multicast_group, config.network.shred_port, config.network.heartbeat_port
    );

    let handle = XdpHandle {
        ebpf,
        interface: config.network.physical_interface.clone(),
    };

    Ok((handle, actual_mode))
}

/// Register an AF_XDP socket file descriptor in the XSK BPF map.
pub fn register_xsk_socket(
    ebpf: &mut Ebpf,
    queue_id: u32,
    socket_fd: std::os::fd::RawFd,
) -> Result<()> {
    let mut xsk_map = XskMap::try_from(ebpf.map_mut("XSKMAP").context("XSKMAP not found")?)
        .context("XSKMAP is not an XskMap")?;

    use std::os::fd::BorrowedFd;
    let fd = unsafe { BorrowedFd::borrow_raw(socket_fd) };
    xsk_map
        .set(queue_id, fd, 0)
        .with_context(|| format!("failed to register AF_XDP socket in XSKMAP[{queue_id}]"))?;

    eprintln!("Registered AF_XDP socket fd={socket_fd} at XSKMAP[{queue_id}]");
    Ok(())
}

/// Read XDP stats from the per-CPU BPF map. Returns aggregated (sum across CPUs) values.
pub fn read_xdp_stats(ebpf: &Ebpf) -> Result<(u64, u64, u64)> {
    let stats_map: PerCpuArray<_, XdpStats> =
        PerCpuArray::try_from(ebpf.map("STATS").context("STATS map not found")?)
            .context("STATS map is not a PerCpuArray")?;

    let per_cpu_values = stats_map
        .get(&0, 0)
        .context("failed to read STATS map")?;

    let mut total_redirected: u64 = 0;
    let mut total_passed: u64 = 0;
    let mut total_errors: u64 = 0;

    for cpu_stats in per_cpu_values.iter() {
        total_redirected += cpu_stats.redirected;
        total_passed += cpu_stats.passed;
        total_errors += cpu_stats.errors;
    }

    Ok((total_redirected, total_passed, total_errors))
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd rust/xdp-receiver && cargo check
```

Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/src/xdp.rs
git commit -m "feat(xdp): XDP loader module with attach, map config, and stats reading"
```

---

### Task 7: AF_XDP Receiver Module

**Files:**
- Create: `rust/xdp-receiver/src/receiver.rs`

Sets up AF_XDP socket via xsk-rs, manages UMEM, polls RX ring, strips GRE headers, and parses shred payloads.

- [ ] **Step 1: Write receiver.rs**

```rust
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

use crate::config::Config;
use crate::shred_parser;
use crate::stats::Stats;

const ETH_HDR_LEN: usize = 14;
const IPV4_HDR_MIN_LEN: usize = 20;
const GRE_HDR_MIN_LEN: usize = 4;
const UDP_HDR_LEN: usize = 8;

/// Represents the AF_XDP socket and UMEM resources.
pub struct AfXdpReceiver {
    fill_queue: xsk_rs::FillQueue,
    rx_queue: xsk_rs::RxQueue,
    umem: xsk_rs::Umem,
    frame_descs: Vec<xsk_rs::FrameDesc>,
    frame_count: usize,
}

impl AfXdpReceiver {
    /// Create and bind an AF_XDP socket to the specified interface and RX queue.
    /// Returns the receiver and the socket's raw fd (for registering in XSKMAP).
    pub fn new(config: &Config) -> Result<(Self, i32)> {
        let frame_count = config.frame_count();
        let frame_size = config.xdp.frame_size as u32;

        // Configure UMEM
        let umem_config = xsk_rs::UmemConfig::builder()
            .frame_count(frame_count as u32)
            .frame_size(frame_size)
            .fill_queue_size(frame_count as u32)
            .comp_queue_size(frame_count as u32)
            .build()
            .context("invalid UMEM config")?;

        let (umem, frame_descs) = xsk_rs::Umem::new(umem_config, frame_count as u32, false)
            .context("failed to create UMEM")?;

        // Configure socket
        let socket_config = xsk_rs::SocketConfig::builder()
            .rx_queue_size(frame_count as u32)
            .tx_queue_size(0) // RX only
            .build()
            .context("invalid socket config")?;

        let iface = xsk_rs::Interface::new(&config.network.physical_interface)
            .with_context(|| {
                format!(
                    "interface '{}' not found",
                    config.network.physical_interface
                )
            })?;

        let (_tx_queue, rx_queue, fq_cq) = xsk_rs::Socket::new(
            socket_config,
            &umem,
            &iface,
            config.xdp.rx_queue,
        )
        .context("failed to create AF_XDP socket")?;

        let (fill_queue, _comp_queue) = fq_cq
            .context("fill/completion queues not available (shared UMEM?)")?;

        let raw_fd = rx_queue.as_raw_fd();

        let receiver = Self {
            fill_queue,
            rx_queue,
            umem,
            frame_descs,
            frame_count,
        };

        Ok((receiver, raw_fd))
    }

    /// Populate the fill ring with all available frame addresses.
    pub fn fill_ring(&mut self) -> Result<()> {
        let submitted = unsafe { self.fill_queue.produce(&self.frame_descs) };
        eprintln!("Submitted {} frames to fill ring", submitted);
        Ok(())
    }

    /// Run the receive loop. Blocks until `shutdown` is set.
    pub fn run(
        &mut self,
        config: &Config,
        stats: Arc<RwLock<Stats>>,
        shutdown: Arc<AtomicBool>,
        ebpf: &aya::Ebpf,
    ) -> Result<()> {
        let poll_timeout_ms = 100;
        let mut xdp_stats_interval = Instant::now();
        let xdp_stats_period = Duration::from_secs(1);

        eprintln!(
            "AF_XDP receiver running on {} queue {}",
            config.network.physical_interface, config.xdp.rx_queue
        );

        while !shutdown.load(Ordering::Relaxed) {
            // Poll for received packets
            let frames_received = unsafe {
                self.rx_queue
                    .poll_and_consume(&mut self.frame_descs[..], poll_timeout_ms)
            };

            if frames_received == 0 {
                if xdp_stats_interval.elapsed() >= xdp_stats_period {
                    self.update_xdp_stats(&stats, ebpf);
                    xdp_stats_interval = Instant::now();
                }
                continue;
            }

            // Process received frames
            for i in 0..frames_received {
                let frame = &self.frame_descs[i];
                let pkt_data = unsafe { self.umem.data(frame) };
                self.process_packet(pkt_data, config, &stats);
            }

            // Return consumed frames to the fill ring
            let returned =
                unsafe { self.fill_queue.produce(&self.frame_descs[..frames_received]) };
            if returned < frames_received {
                stats.write().unwrap().afxdp_fill_starvation += 1;
            }
            stats.write().unwrap().afxdp_rx_fill_level =
                self.frame_count.saturating_sub(frames_received) + returned;

            // Periodically read BPF stats
            if xdp_stats_interval.elapsed() >= xdp_stats_period {
                self.update_xdp_stats(&stats, ebpf);
                xdp_stats_interval = Instant::now();
            }
        }

        eprintln!("AF_XDP receiver shutting down");
        Ok(())
    }

    /// Process a single received packet. Strips encapsulation headers and parses payload.
    fn process_packet(&self, pkt: &[u8], config: &Config, stats: &Arc<RwLock<Stats>>) {
        match Self::find_udp_payload(pkt) {
            Some((offset, port)) => {
                if port == config.network.heartbeat_port {
                    stats.write().unwrap().record_heartbeat();
                    return;
                }
                if port != config.network.shred_port {
                    return;
                }
                let payload = &pkt[offset..];
                match shred_parser::parse_shred(payload) {
                    Some(parsed) => {
                        let mut s = stats.write().unwrap();
                        s.record_shred(
                            parsed.slot,
                            parsed.is_data,
                            parsed.index,
                            parsed.fec_set_index,
                            parsed.signature,
                        );
                    }
                    None => {
                        stats.write().unwrap().record_parse_error();
                    }
                }
            }
            None => {
                stats.write().unwrap().record_parse_error();
            }
        }
    }

    /// Parse through Eth → outer IP → GRE → inner IP → UDP headers.
    /// Returns (payload_offset, udp_dst_port) if successful.
    fn find_udp_payload(pkt: &[u8]) -> Option<(usize, u16)> {
        if pkt.len() < ETH_HDR_LEN + IPV4_HDR_MIN_LEN {
            return None;
        }

        let mut offset = ETH_HDR_LEN;

        // Outer IPv4: read IHL
        let outer_ihl = ((pkt[offset] & 0x0F) as usize) * 4;
        if outer_ihl < IPV4_HDR_MIN_LEN || offset + outer_ihl > pkt.len() {
            return None;
        }
        // Check protocol == GRE (47)
        if pkt[offset + 9] != 47 {
            return None;
        }
        offset += outer_ihl;

        // GRE header
        if offset + GRE_HDR_MIN_LEN > pkt.len() {
            return None;
        }
        let gre_flags = u16::from_be_bytes([pkt[offset], pkt[offset + 1]]);
        let mut gre_len = GRE_HDR_MIN_LEN;
        if gre_flags & 0x8000 != 0 {
            gre_len += 4; // Checksum
        }
        if gre_flags & 0x2000 != 0 {
            gre_len += 4; // Key
        }
        if gre_flags & 0x1000 != 0 {
            gre_len += 4; // Sequence
        }
        offset += gre_len;

        // Inner IPv4
        if offset + IPV4_HDR_MIN_LEN > pkt.len() {
            return None;
        }
        let inner_ihl = ((pkt[offset] & 0x0F) as usize) * 4;
        if inner_ihl < IPV4_HDR_MIN_LEN || offset + inner_ihl > pkt.len() {
            return None;
        }
        if pkt[offset + 9] != 17 {
            return None; // Not UDP
        }
        offset += inner_ihl;

        // UDP header
        if offset + UDP_HDR_LEN > pkt.len() {
            return None;
        }
        let dst_port = u16::from_be_bytes([pkt[offset + 2], pkt[offset + 3]]);
        let payload_start = offset + UDP_HDR_LEN;

        Some((payload_start, dst_port))
    }

    fn update_xdp_stats(&self, stats: &Arc<RwLock<Stats>>, ebpf: &aya::Ebpf) {
        if let Ok((redirected, passed, errors)) = crate::xdp::read_xdp_stats(ebpf) {
            let mut s = stats.write().unwrap();
            s.update_xdp_counters(redirected, passed, errors);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal GRE-encapsulated UDP packet for testing header parsing.
    fn build_gre_udp_packet(dst_port: u16) -> Vec<u8> {
        let mut pkt = Vec::new();

        // Ethernet header (14 bytes)
        pkt.extend_from_slice(&[0u8; 12]); // dst + src MAC
        pkt.extend_from_slice(&[0x08, 0x00]); // EtherType: IPv4

        // Outer IPv4 (20 bytes) - protocol 47 (GRE)
        pkt.push(0x45); // version=4, IHL=5
        pkt.extend_from_slice(&[0u8; 8]); // ToS, TotalLen, ID, Flags, TTL
        pkt.push(47); // Protocol: GRE
        pkt.extend_from_slice(&[0u8; 2]); // Header checksum
        pkt.extend_from_slice(&[10, 0, 0, 1]); // Src IP
        pkt.extend_from_slice(&[10, 0, 0, 2]); // Dst IP

        // GRE header (4 bytes, no optional fields)
        pkt.extend_from_slice(&[0x00, 0x00]); // Flags: none
        pkt.extend_from_slice(&[0x08, 0x00]); // Protocol: IPv4

        // Inner IPv4 (20 bytes) - protocol 17 (UDP)
        pkt.push(0x45); // version=4, IHL=5
        pkt.extend_from_slice(&[0u8; 8]); // ToS, TotalLen, etc
        pkt.push(17); // Protocol: UDP
        pkt.extend_from_slice(&[0u8; 2]); // Header checksum
        pkt.extend_from_slice(&[148, 51, 0, 1]); // Src IP
        pkt.extend_from_slice(&[233, 84, 178, 1]); // Dst IP (multicast)

        // UDP header (8 bytes)
        pkt.extend_from_slice(&[0x00, 0x00]); // Src port
        pkt.extend_from_slice(&dst_port.to_be_bytes()); // Dst port
        pkt.extend_from_slice(&[0x00, 0x00]); // Length
        pkt.extend_from_slice(&[0x00, 0x00]); // Checksum

        // Payload (dummy data)
        pkt.extend_from_slice(&[0xAA; 100]);

        pkt
    }

    #[test]
    fn test_find_udp_payload_shred_port() {
        let pkt = build_gre_udp_packet(7733);
        let result = AfXdpReceiver::find_udp_payload(&pkt);
        assert!(result.is_some());
        let (offset, port) = result.unwrap();
        assert_eq!(port, 7733);
        // Eth(14) + outerIP(20) + GRE(4) + innerIP(20) + UDP(8) = 66
        assert_eq!(offset, 66);
    }

    #[test]
    fn test_find_udp_payload_heartbeat_port() {
        let pkt = build_gre_udp_packet(5765);
        let result = AfXdpReceiver::find_udp_payload(&pkt);
        assert!(result.is_some());
        let (_, port) = result.unwrap();
        assert_eq!(port, 5765);
    }

    #[test]
    fn test_find_udp_payload_truncated() {
        let pkt = vec![0u8; 30]; // Too short
        assert!(AfXdpReceiver::find_udp_payload(&pkt).is_none());
    }

    #[test]
    fn test_find_udp_payload_gre_with_key() {
        let mut pkt = Vec::new();

        // Ethernet (14)
        pkt.extend_from_slice(&[0u8; 12]);
        pkt.extend_from_slice(&[0x08, 0x00]);

        // Outer IPv4 (20) - GRE
        pkt.push(0x45);
        pkt.extend_from_slice(&[0u8; 8]);
        pkt.push(47);
        pkt.extend_from_slice(&[0u8; 2]);
        pkt.extend_from_slice(&[10, 0, 0, 1]);
        pkt.extend_from_slice(&[10, 0, 0, 2]);

        // GRE with Key flag set (8 bytes total)
        pkt.extend_from_slice(&[0x20, 0x00]); // Flags: Key bit (0x2000)
        pkt.extend_from_slice(&[0x08, 0x00]); // Protocol: IPv4
        pkt.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Key value

        // Inner IPv4 (20) - UDP
        pkt.push(0x45);
        pkt.extend_from_slice(&[0u8; 8]);
        pkt.push(17);
        pkt.extend_from_slice(&[0u8; 2]);
        pkt.extend_from_slice(&[148, 51, 0, 1]);
        pkt.extend_from_slice(&[233, 84, 178, 1]);

        // UDP (8)
        pkt.extend_from_slice(&[0x00, 0x00]);
        pkt.extend_from_slice(&7733u16.to_be_bytes());
        pkt.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

        pkt.extend_from_slice(&[0xBB; 50]);

        let result = AfXdpReceiver::find_udp_payload(&pkt);
        assert!(result.is_some());
        let (offset, port) = result.unwrap();
        assert_eq!(port, 7733);
        // Eth(14) + outerIP(20) + GRE(8) + innerIP(20) + UDP(8) = 70
        assert_eq!(offset, 70);
    }
}
```

**Note:** The xsk-rs API calls (`Umem::new`, `Socket::new`, `FillQueue::produce`, `RxQueue::poll_and_consume`, `Umem::data`) follow the general patterns of xsk-rs. At build time, verify exact method signatures against the resolved crate version and adapt if needed.

- [ ] **Step 2: Run the header parsing tests**

```bash
cd rust/xdp-receiver && cargo test --lib receiver
```

Expected: 4 tests pass (the `find_udp_payload` tests). The `AfXdpReceiver::new` and `run` methods can only be tested on Linux with appropriate capabilities.

- [ ] **Step 3: Commit**

```bash
git add rust/xdp-receiver/src/receiver.rs
git commit -m "feat(xdp): AF_XDP receiver with GRE header stripping and shred parsing"
```

---

### Task 8: Display Modules

**Files:**
- Create: `rust/xdp-receiver/src/display/mod.rs`
- Create: `rust/xdp-receiver/src/display/tui.rs`
- Create: `rust/xdp-receiver/src/display/log.rs`

Adapted from kernel-receiver. TUI adds an XDP stats panel. Log adds XDP stats to the summary line.

- [ ] **Step 1: Write display/mod.rs**

```rust
pub mod log;
pub mod tui;

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, RwLock};

use crate::config::{Config, DisplayMode};
use crate::stats::Stats;

/// Run the selected display mode. Blocks until shutdown.
pub fn run(
    config: &Config,
    stats: Arc<RwLock<Stats>>,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    match config.display.mode {
        DisplayMode::Log => log::run(config, stats, shutdown),
        DisplayMode::Tui => tui::run(config, stats, shutdown),
    }
}
```

- [ ] **Step 2: Write display/log.rs**

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::stats::Stats;

fn format_signature_prefix(sig: &[u8; 8]) -> String {
    let hex: String = sig.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}..{}", &hex[..4], &hex[12..])
}

pub fn run(
    config: &Config,
    stats: Arc<RwLock<Stats>>,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let interval = Duration::from_secs(config.display.log_interval_secs);
    let mut last_print = Instant::now();
    let mut last_reported_slots: Vec<u64> = Vec::new();

    eprintln!(
        "Log mode: printing stats every {}s. Press Ctrl+C to stop.",
        config.display.log_interval_secs
    );

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(Duration::from_millis(100));

        if last_print.elapsed() < interval {
            continue;
        }
        last_print = Instant::now();

        let mut s = stats.write().unwrap();

        // Print per-slot lines for newly seen slots
        let current_slots: Vec<u64> = s.slots.keys().copied().collect();
        for &slot_num in &current_slots {
            if last_reported_slots.contains(&slot_num) {
                continue;
            }
            if let Some(slot) = s.get_slot(slot_num) {
                let age_ms = slot.first_seen.elapsed().as_millis();
                println!(
                    "slot={} sig={} data={} coding={} fec_sets={} age_ms={}",
                    slot.slot,
                    format_signature_prefix(&slot.signature_prefix),
                    slot.data_shred_count,
                    slot.coding_shred_count,
                    slot.fec_set_count,
                    age_ms,
                );
            }
        }
        last_reported_slots = current_slots;

        // Print summary line with XDP stats
        let rate = s.shreds_per_second();
        let hb_ago = s
            .last_heartbeat
            .map(|t| format!("{}ms ago", t.elapsed().as_millis()))
            .unwrap_or_else(|| "never".into());

        println!(
            "[stats] shreds/sec={:.0} data={} coding={} errors={} heartbeats={} (last: {}) xdp_mode={} redirected={} passed={} ring_fill={}/{}",
            rate,
            s.total_data_shreds,
            s.total_coding_shreds,
            s.parse_errors,
            s.total_heartbeats,
            hb_ago,
            s.xdp_attach_mode,
            s.xdp_redirected,
            s.xdp_passed,
            s.afxdp_rx_fill_level,
            config.frame_count(),
        );
    }

    Ok(())
}
```

- [ ] **Step 3: Write display/tui.rs**

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Style, Stylize};
use ratatui::widgets::{Block, Borders, Paragraph, Row, Table};

use crate::config::Config;
use crate::stats::Stats;

fn format_signature_prefix(sig: &[u8; 8]) -> String {
    let hex: String = sig.iter().map(|b| format!("{b:02x}")).collect();
    format!("{}..{}", &hex[..4], &hex[12..])
}

fn format_duration_short(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

pub fn run(
    config: &Config,
    stats: Arc<RwLock<Stats>>,
    shutdown: Arc<AtomicBool>,
) -> anyhow::Result<()> {
    let tick_rate = Duration::from_millis(1000 / config.display.refresh_hz as u64);
    let mut terminal = ratatui::init();

    let result = run_loop(&mut terminal, &stats, &shutdown, tick_rate, config);

    ratatui::restore();
    result
}

fn run_loop(
    terminal: &mut ratatui::DefaultTerminal,
    stats: &Arc<RwLock<Stats>>,
    shutdown: &Arc<AtomicBool>,
    tick_rate: Duration,
    config: &Config,
) -> anyhow::Result<()> {
    while !shutdown.load(Ordering::Relaxed) {
        terminal.draw(|frame| {
            let chunks = Layout::vertical([
                Constraint::Length(3), // Top bar: status
                Constraint::Length(5), // XDP stats panel
                Constraint::Fill(1),  // Slot table
                Constraint::Length(3), // Bottom stats
            ])
            .split(frame.area());

            let mut s = stats.write().unwrap();

            // === Top bar ===
            let uptime = format_duration_short(s.start_time.elapsed());
            let hb_info = match s.last_heartbeat {
                Some(t) => format!(
                    "heartbeats: {} (last: {}ms ago)",
                    s.total_heartbeats,
                    t.elapsed().as_millis()
                ),
                None => format!("heartbeats: {} (none yet)", s.total_heartbeats),
            };
            let status_text = format!(
                " iface: {} | group: {} | xdp: {} | uptime: {} | {}",
                config.network.physical_interface,
                config.network.multicast_group,
                s.xdp_attach_mode,
                uptime,
                hb_info,
            );
            let status = Paragraph::new(status_text).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" XDP Multicast Receiver "),
            );
            frame.render_widget(status, chunks[0]);

            // === XDP Stats Panel (new) ===
            let xdp_text = format!(
                " redirected: {} | passed: {} | errors: {} | ring fill: {}/{} | starvation: {}",
                s.xdp_redirected,
                s.xdp_passed,
                s.xdp_errors,
                s.afxdp_rx_fill_level,
                config.frame_count(),
                s.afxdp_fill_starvation,
            );
            let xdp_panel = Paragraph::new(xdp_text).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" XDP Stats "),
            );
            frame.render_widget(xdp_panel, chunks[1]);

            // === Slot table ===
            let header = Row::new(vec![
                "Slot", "Signature", "Data", "Coding", "FEC Sets", "Age",
            ])
            .style(Style::new().bold());

            let rows: Vec<Row> = s
                .recent_slots()
                .iter()
                .map(|slot| {
                    let age = format_duration_short(slot.first_seen.elapsed());
                    Row::new(vec![
                        slot.slot.to_string(),
                        format_signature_prefix(&slot.signature_prefix),
                        slot.data_shred_count.to_string(),
                        slot.coding_shred_count.to_string(),
                        slot.fec_set_count.to_string(),
                        age,
                    ])
                })
                .collect();

            let table = Table::new(
                rows,
                [
                    Constraint::Length(12),
                    Constraint::Length(14),
                    Constraint::Length(8),
                    Constraint::Length(8),
                    Constraint::Length(10),
                    Constraint::Length(8),
                ],
            )
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Recent Slots "),
            );

            frame.render_widget(table, chunks[2]);

            // === Bottom stats ===
            let rate = s.shreds_per_second();
            let total = s.total_data_shreds + s.total_coding_shreds;
            let ratio = if s.total_coding_shreds > 0 {
                format!(
                    "{:.1}",
                    s.total_data_shreds as f64 / s.total_coding_shreds as f64
                )
            } else {
                "n/a".into()
            };
            let stats_text = format!(
                " shreds/sec: {:.0} | total: {} (data: {}, coding: {}) | data/coding: {} | errors: {}",
                rate, total, s.total_data_shreds, s.total_coding_shreds, ratio, s.parse_errors,
            );
            let stats_bar = Paragraph::new(stats_text)
                .block(Block::default().borders(Borders::ALL).title(" Stats "));
            frame.render_widget(stats_bar, chunks[3]);
        })?;

        if event::poll(tick_rate)? {
            if let Event::Key(key) = event::read()? {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => {
                        shutdown.store(true, Ordering::Relaxed);
                        return Ok(());
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
```

- [ ] **Step 4: Verify display modules compile**

```bash
cd rust/xdp-receiver && cargo check
```

Expected: compiles clean.

- [ ] **Step 5: Commit**

```bash
git add rust/xdp-receiver/src/display/
git commit -m "feat(xdp): display modules with XDP stats panel (TUI) and extended log format"
```

---

### Task 9: Main Integration

**Files:**
- Modify: `rust/xdp-receiver/src/main.rs`
- Create: `rust/xdp-receiver/config.example.toml`

Wire everything together: CLI → config → XDP attach → AF_XDP socket → receiver thread → display thread → shutdown.

- [ ] **Step 1: Write main.rs**

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use clap::Parser;

mod config;
mod display;
mod receiver;
mod shred_parser;
mod stats;
mod xdp;

use config::{Cli, Config};
use stats::Stats;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli)?;

    eprintln!(
        "edge-multicast-xdp-receiver v{}",
        env!("CARGO_PKG_VERSION")
    );
    eprintln!(
        "Interface: {}, Multicast: {}, Shred port: {}, Heartbeat port: {}",
        config.network.physical_interface,
        config.network.multicast_group,
        config.network.shred_port,
        config.network.heartbeat_port,
    );
    eprintln!(
        "XDP mode: {:?}, RX queue: {}, UMEM: {}MB ({} frames x {} bytes)",
        config.xdp.xdp_mode,
        config.xdp.rx_queue,
        config.xdp.umem_size / 1_048_576,
        config.frame_count(),
        config.xdp.frame_size,
    );
    eprintln!("Display mode: {:?}", config.display.mode);

    let stats = Arc::new(RwLock::new(Stats::new(config.stats.max_slots)));
    let shutdown = Arc::new(AtomicBool::new(false));

    // Set up Ctrl+C handler
    let shutdown_signal = shutdown.clone();
    ctrlc::set_handler(move || {
        shutdown_signal.store(true, Ordering::Relaxed);
    })?;

    // 1. Load and attach XDP program
    let (mut xdp_handle, attach_mode) = xdp::attach_xdp(&config)?;
    stats.write().unwrap().xdp_attach_mode = attach_mode;

    // 2. Create AF_XDP socket
    let (mut afxdp_receiver, socket_fd) = receiver::AfXdpReceiver::new(&config)?;

    // 3. Register socket in XSK BPF map
    xdp::register_xsk_socket(&mut xdp_handle.ebpf, config.xdp.rx_queue, socket_fd)?;

    // 4. Populate fill ring
    afxdp_receiver.fill_ring()?;

    // 5. Spawn receiver thread (takes ownership of ebpf handle for stats reading)
    let recv_config = config.clone();
    let recv_stats = stats.clone();
    let recv_shutdown = shutdown.clone();
    let recv_handle = std::thread::Builder::new()
        .name("receiver".into())
        .spawn(move || {
            if let Err(e) =
                afxdp_receiver.run(&recv_config, recv_stats, recv_shutdown, &xdp_handle.ebpf)
            {
                eprintln!("Receiver error: {e:#}");
            }
            // xdp_handle dropped here → detaches XDP program
            drop(xdp_handle);
        })?;

    // 6. Run display on main thread (blocks until shutdown)
    display::run(&config, stats, shutdown.clone())?;

    // 7. Wait for receiver to finish
    recv_handle.join().expect("receiver thread panicked");

    eprintln!("Shutdown complete.");
    Ok(())
}
```

- [ ] **Step 2: Write config.example.toml**

```toml
[network]
physical_interface = "eth0"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765

[xdp]
xdp_mode = "auto"          # "auto", "native", "skb"
umem_size = 4194304         # 4MB
frame_size = 2048
rx_queue = 0

[display]
mode = "tui"                # "tui" or "log"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32
```

- [ ] **Step 3: Verify it compiles**

```bash
cd rust/xdp-receiver && cargo check
```

Expected: compiles. Resolve any compilation errors from API mismatches — particularly `xsk-rs` and `aya` method signatures. Adjust as needed.

- [ ] **Step 4: Commit**

```bash
git add rust/xdp-receiver/src/main.rs rust/xdp-receiver/config.example.toml
git commit -m "feat(xdp): main integration - XDP attach, AF_XDP setup, thread spawning, shutdown"
```

---

### Task 10: Clippy, Formatting, Final Cleanup

**Files:**
- Modify: any files with warnings

- [ ] **Step 1: Run clippy**

```bash
cd rust/xdp-receiver && cargo clippy -- -D warnings
```

Fix any clippy warnings.

- [ ] **Step 2: Run rustfmt**

```bash
cd rust/xdp-receiver && cargo fmt
```

- [ ] **Step 3: Run all tests**

```bash
cd rust/xdp-receiver && cargo test
```

Expected: all unit tests pass (config, stats, shred_parser, receiver header parsing).

- [ ] **Step 4: Commit**

```bash
git add -A rust/xdp-receiver/
git commit -m "chore(xdp): clippy fixes, formatting, final cleanup"
```

---

## Build & Run Instructions

### Prerequisites (Linux only)

```bash
# Install nightly toolchain (for eBPF compilation)
rustup toolchain install nightly --component rust-src

# Install bpf-linker
cargo install bpf-linker

# Set capabilities (instead of running as root)
sudo setcap cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep ./target/release/edge-multicast-xdp-receiver
```

### Build

```bash
cd rust/xdp-receiver && cargo build --release
```

This compiles both the eBPF program (via build.rs) and the userspace binary.

### Run

```bash
./target/release/edge-multicast-xdp-receiver --physical-interface eth0 --config config.example.toml
```

### Manual XDP Detach (if process crashes)

```bash
ip link set dev eth0 xdp off
```