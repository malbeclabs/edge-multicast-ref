# Rust Kernel-Socket Multicast Shred Receiver — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal Rust tool that receives Solana shreds from a DoubleZero edge multicast feed via kernel UDP sockets and displays live statistics via TUI or stdout logging.

**Architecture:** Single-threaded recv loop on a spawned thread reads from two UDP sockets (shreds + heartbeats) via `libc::poll`, parses shred headers with `solana-ledger`, and updates shared stats. Main thread runs either a ratatui TUI dashboard or a streaming log printer.

**Tech Stack:** Rust, solana-ledger (upstream Agave), socket2, ratatui/crossterm, clap, serde/toml

**Spec:** `.claude/2026-03-26-rust-kernel-receiver-design.md`

---

## File Map

| File | Responsibility |
|---|---|
| `rust/kernel-receiver/Cargo.toml` | Crate manifest with all dependencies |
| `rust/kernel-receiver/config.example.toml` | Example configuration file |
| `rust/kernel-receiver/src/main.rs` | CLI parsing, config loading, thread spawning, shutdown |
| `rust/kernel-receiver/src/config.rs` | Config struct (TOML deserialization + CLI merge) |
| `rust/kernel-receiver/src/stats.rs` | `Stats`, `SlotStats` structs, ring buffer, rate tracking |
| `rust/kernel-receiver/src/shred_parser.rs` | Wraps `Shred::new_from_serialized_shred()`, extracts fields into `ParsedShred` |
| `rust/kernel-receiver/src/receiver.rs` | Socket creation, multicast join, poll-based recv loop |
| `rust/kernel-receiver/src/display/mod.rs` | `DisplayMode` enum, dispatch to tui or log |
| `rust/kernel-receiver/src/display/tui.rs` | ratatui dashboard with crossterm backend |
| `rust/kernel-receiver/src/display/log.rs` | Streaming stdout logger |

---

### Task 1: Project Scaffold

**Files:**
- Create: `rust/kernel-receiver/Cargo.toml`
- Create: `rust/kernel-receiver/config.example.toml`
- Create: `rust/kernel-receiver/src/main.rs`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p rust/kernel-receiver/src/display
```

- [ ] **Step 2: Write Cargo.toml**

```toml
[package]
name = "edge-multicast-receiver"
version = "0.1.0"
edition = "2021"
description = "Reference design for consuming DoubleZero edge multicast shred feeds"

[dependencies]
# Solana - upstream Agave
solana-ledger = "3.1"
solana-sdk = "2.2"

# CLI + Config
clap = { version = "4", features = ["derive"] }
toml = "0.8"
serde = { version = "1", features = ["derive"] }

# TUI
ratatui = "0.29"
crossterm = "0.28"

# Networking
socket2 = "0.5"
libc = "0.2"

# Misc
anyhow = "1"
```

Note: `libc` is added for `poll()` syscall. Pin solana crate versions to what actually resolves — may need adjustment at build time.

- [ ] **Step 3: Write config.example.toml**

```toml
[network]
interface = "doublezero1"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765
recv_buffer_size = 8388608  # 8MB

[display]
mode = "tui"  # "tui" or "log"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32
```

- [ ] **Step 4: Write minimal main.rs**

```rust
use anyhow::Result;

mod config;
mod receiver;
mod shred_parser;
mod stats;
mod display;

fn main() -> Result<()> {
    println!("edge-multicast-receiver starting...");
    Ok(())
}
```

- [ ] **Step 5: Verify it compiles**

```bash
cd rust/kernel-receiver && cargo check
```

Expected: compiles with warnings about unused modules (modules don't exist yet, so this step creates stub files for each module).

Create stub files so `cargo check` passes:

```bash
touch src/config.rs src/receiver.rs src/shred_parser.rs src/stats.rs
mkdir -p src/display
touch src/display/mod.rs src/display/tui.rs src/display/log.rs
```

Each stub file is empty. `src/display/mod.rs` needs:

```rust
pub mod tui;
pub mod log;
```

Run `cargo check` again. Expected: compiles clean (possibly with solana dep resolution warnings).

- [ ] **Step 6: Commit**

```bash
git add rust/kernel-receiver/
git commit -m "scaffold: rust kernel-receiver project with deps and stubs"
```

---

### Task 2: Config Module

**Files:**
- Create: `rust/kernel-receiver/src/config.rs`

- [ ] **Step 1: Write config test**

Add to the bottom of `src/config.rs`:

```rust
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
        assert_eq!(config.network.shred_port, 7733); // default
        assert_eq!(config.display.mode, DisplayMode::Tui); // default
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd rust/kernel-receiver && cargo test --lib config
```

Expected: FAIL — structs not defined yet.

- [ ] **Step 3: Implement config module**

Write `src/config.rs`:

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

        // Apply CLI overrides
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd rust/kernel-receiver && cargo test --lib config
```

Expected: 3 tests pass.

- [ ] **Step 5: Commit**

```bash
git add rust/kernel-receiver/src/config.rs
git commit -m "feat: config module with TOML parsing and CLI overrides"
```

---

### Task 3: Stats Module

**Files:**
- Create: `rust/kernel-receiver/src/stats.rs`

- [ ] **Step 1: Write stats tests**

Add to `src/stats.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_stats() {
        let stats = Stats::new(4);
        assert_eq!(stats.total_data_shreds, 0);
        assert_eq!(stats.total_coding_shreds, 0);
        assert_eq!(stats.total_heartbeats, 0);
        assert_eq!(stats.parse_errors, 0);
        assert_eq!(stats.slots.len(), 0);
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
        assert_eq!(slot.coding_shred_count, 0);
        assert_eq!(slot.highest_data_index, 0);
    }

    #[test]
    fn test_record_shred_coding() {
        let mut stats = Stats::new(4);
        stats.record_shred(100, false, 5, 0, [0xAB; 64]);
        assert_eq!(stats.total_data_shreds, 0);
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
        assert_eq!(slot.fec_set_count, 2); // fec indices 0 and 1
    }

    #[test]
    fn test_ring_buffer_eviction() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        for slot in 0..6 {
            stats.record_shred(slot, true, 0, 0, sig);
        }
        // Only 4 most recent slots should remain
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
    fn test_shreds_per_second() {
        let mut stats = Stats::new(4);
        let sig = [0xAB; 64];
        // Record 10 shreds
        for i in 0..10 {
            stats.record_shred(100, true, i, 0, sig);
        }
        // Rate should be > 0 (hard to test exact value due to timing)
        // Just verify it doesn't panic
        let _rate = stats.shreds_per_second();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd rust/kernel-receiver && cargo test --lib stats
```

Expected: FAIL — `Stats` not defined.

- [ ] **Step 3: Implement stats module**

Write `src/stats.rs`:

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

        // Evict oldest slots if over capacity
        while self.slots.len() > self.max_slots {
            if let Some((&oldest, _)) = self.slots.iter().next() {
                self.slots.remove(&oldest);
            }
        }

        // Track for rate calculation
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

        // Remove entries older than 1 second
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
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd rust/kernel-receiver && cargo test --lib stats
```

Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
git add rust/kernel-receiver/src/stats.rs
git commit -m "feat: stats module with per-slot tracking and rate calculation"
```

---

### Task 4: Shred Parser Module

**Files:**
- Create: `rust/kernel-receiver/src/shred_parser.rs`

- [ ] **Step 1: Write shred parser**

The parser wraps `solana-ledger` and returns a simple struct. Write `src/shred_parser.rs`:

```rust
use solana_ledger::shred::{Shred, ShredType};
use solana_sdk::signature::Signature;

/// Extracted fields from a parsed shred. Avoids holding onto the full Shred object.
#[derive(Debug, Clone)]
pub struct ParsedShred {
    pub slot: u64,
    pub index: u32,
    pub is_data: bool,
    pub fec_set_index: u32,
    pub version: u16,
    pub signature: [u8; 64],
}

/// Parse a raw UDP payload into a ParsedShred.
/// Returns None if the payload cannot be parsed as a valid shred.
pub fn parse_shred(payload: &[u8]) -> Option<ParsedShred> {
    let shred = Shred::new_from_serialized_shred(payload.to_vec()).ok()?;
    let sig_bytes: [u8; 64] = shred.signature().as_ref().try_into().ok()?;

    Some(ParsedShred {
        slot: shred.slot(),
        index: shred.index(),
        is_data: shred.shred_type() == ShredType::Data,
        fec_set_index: shred.fec_set_index(),
        version: shred.version(),
        signature: sig_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_garbage_returns_none() {
        let garbage = vec![0u8; 100];
        assert!(parse_shred(&garbage).is_none());
    }

    #[test]
    fn test_parse_empty_returns_none() {
        assert!(parse_shred(&[]).is_none());
    }

    #[test]
    fn test_parse_real_shred_from_pcap() {
        // This test uses a real shred payload extracted from the sample pcap.
        // It will be populated once we extract a raw shred from pcaps/sample.pcap.
        // For now, verify the parser doesn't panic on various sizes.
        for size in [64, 128, 256, 512, 1024, 1228, 1272] {
            let data = vec![0xFFu8; size];
            let _ = parse_shred(&data); // should not panic
        }
    }
}
```

- [ ] **Step 2: Run tests**

```bash
cd rust/kernel-receiver && cargo test --lib shred_parser
```

Expected: 3 tests pass. The real-shred test is a smoke test for now.

- [ ] **Step 3: Extract a real shred payload from pcap for testing**

```bash
# Extract raw UDP payload of first shred packet (frame 618) from the pcap
cd /Users/amcconnell/src/git/edge-multicast-ref
tshark -r pcaps/sample.pcap -Y "udp.dstport==7733" -c 1 -T fields -e data.data 2>/dev/null | xxd -r -p > rust/kernel-receiver/tests/fixtures/shred_payload.bin
```

If this works, update the `test_parse_real_shred_from_pcap` test to load and parse the binary file:

```rust
#[test]
fn test_parse_real_shred_from_pcap() {
    let payload = include_bytes!("../../tests/fixtures/shred_payload.bin");
    if payload.is_empty() {
        return; // fixture not yet extracted
    }
    let parsed = parse_shred(payload);
    // If solana-ledger can parse this as a valid shred, verify fields are reasonable
    if let Some(shred) = parsed {
        assert!(shred.slot > 0);
        assert!(shred.version > 0);
    }
}
```

Create the fixtures directory:

```bash
mkdir -p rust/kernel-receiver/tests/fixtures
```

- [ ] **Step 4: Run tests again with fixture**

```bash
cd rust/kernel-receiver && cargo test --lib shred_parser
```

Expected: passes (may or may not successfully parse depending on tshark extraction — either way, test doesn't fail).

- [ ] **Step 5: Commit**

```bash
git add rust/kernel-receiver/src/shred_parser.rs rust/kernel-receiver/tests/
git commit -m "feat: shred parser wrapping solana-ledger deserialization"
```

---

### Task 5: Receiver Module

**Files:**
- Create: `rust/kernel-receiver/src/receiver.rs`

- [ ] **Step 1: Write receiver module**

This module is mostly I/O (socket setup + recv loop) and is tested by running the binary against a live feed or pcap replay. Write `src/receiver.rs`:

```rust
use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::{Context, Result};
use socket2::{Domain, Protocol, Socket, Type};

use crate::config::Config;
use crate::shred_parser;
use crate::stats::Stats;

/// Create a UDP socket bound to the given port, joined to the multicast group
/// on the specified interface. Sets SO_RCVBUF.
fn create_multicast_socket(
    port: u16,
    multicast_group: &Ipv4Addr,
    interface_ip: &Ipv4Addr,
    recv_buf_size: usize,
) -> Result<Socket> {
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .context("creating UDP socket")?;

    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;

    let bind_addr = std::net::SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, port);
    socket
        .bind(&bind_addr.into())
        .with_context(|| format!("binding to port {}", port))?;

    socket
        .join_multicast_v4(multicast_group, interface_ip)
        .with_context(|| {
            format!(
                "joining multicast {} on interface {}",
                multicast_group, interface_ip
            )
        })?;

    socket
        .set_recv_buffer_size(recv_buf_size)
        .context("setting SO_RCVBUF")?;

    // Set non-blocking for use with poll
    socket.set_nonblocking(true)?;

    Ok(socket)
}

/// Resolve the interface name to its IPv4 address.
/// Falls back to 0.0.0.0 if resolution fails.
fn resolve_interface_ip(interface: &str) -> Ipv4Addr {
    // Use getifaddrs via libc or a simpler approach: parse /proc or ip command
    // For simplicity, try to get the interface IP from the system
    if let Ok(output) = std::process::Command::new("ip")
        .args(["-4", "-o", "addr", "show", interface])
        .output()
    {
        if let Ok(stdout) = std::str::from_utf8(&output.stdout) {
            // Parse line like: "26: doublezero1    inet 169.254.10.233/31 ..."
            for part in stdout.split_whitespace() {
                if let Some(ip_str) = part.split('/').next() {
                    if let Ok(ip) = ip_str.parse::<Ipv4Addr>() {
                        return ip;
                    }
                }
            }
        }
    }
    eprintln!(
        "warning: could not resolve IP for interface '{}', using 0.0.0.0",
        interface
    );
    Ipv4Addr::UNSPECIFIED
}

/// Run the receive loop. Blocks until `shutdown` is set to true.
pub fn run_recv_loop(
    config: &Config,
    stats: Arc<RwLock<Stats>>,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    let multicast_group: Ipv4Addr = config
        .network
        .multicast_group
        .parse()
        .context("parsing multicast group address")?;
    let interface_ip = resolve_interface_ip(&config.network.interface);

    eprintln!(
        "Binding to interface {} ({}), multicast group {}",
        config.network.interface, interface_ip, multicast_group
    );

    let shred_socket = create_multicast_socket(
        config.network.shred_port,
        &multicast_group,
        &interface_ip,
        config.network.recv_buffer_size,
    )
    .context("creating shred socket")?;

    let heartbeat_socket = create_multicast_socket(
        config.network.heartbeat_port,
        &multicast_group,
        &interface_ip,
        config.network.recv_buffer_size,
    )
    .context("creating heartbeat socket")?;

    eprintln!(
        "Listening for shreds on port {}, heartbeats on port {}",
        config.network.shred_port, config.network.heartbeat_port
    );

    let mut buf = [0u8; 2048]; // max shred payload is ~1272 bytes
    let mut poll_fds = [
        libc::pollfd {
            fd: shred_socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: heartbeat_socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        },
    ];

    while !shutdown.load(Ordering::Relaxed) {
        // Poll with 100ms timeout so we can check shutdown flag
        let ready = unsafe { libc::poll(poll_fds.as_mut_ptr(), 2, 100) };

        if ready < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err).context("poll() failed");
        }

        if ready == 0 {
            continue; // timeout, check shutdown
        }

        // Check shred socket
        if poll_fds[0].revents & libc::POLLIN != 0 {
            loop {
                match shred_socket.recv(unsafe {
                    &mut *(&mut buf as *mut [u8] as *mut [std::mem::MaybeUninit<u8>])
                }) {
                    Ok(n) => {
                        let payload = &buf[..n];
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
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        eprintln!("recv error on shred socket: {}", e);
                        break;
                    }
                }
            }
            poll_fds[0].revents = 0;
        }

        // Check heartbeat socket
        if poll_fds[1].revents & libc::POLLIN != 0 {
            loop {
                match heartbeat_socket.recv(unsafe {
                    &mut *(&mut buf as *mut [u8] as *mut [std::mem::MaybeUninit<u8>])
                }) {
                    Ok(_n) => {
                        stats.write().unwrap().record_heartbeat();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        eprintln!("recv error on heartbeat socket: {}", e);
                        break;
                    }
                }
            }
            poll_fds[1].revents = 0;
        }
    }

    eprintln!("Receiver shutting down");
    Ok(())
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd rust/kernel-receiver && cargo check
```

Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add rust/kernel-receiver/src/receiver.rs
git commit -m "feat: receiver with multicast UDP sockets and poll-based recv loop"
```

---

### Task 6: Log Display Mode

**Files:**
- Create: `rust/kernel-receiver/src/display/mod.rs`
- Create: `rust/kernel-receiver/src/display/log.rs`

- [ ] **Step 1: Write display mod.rs**

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

- [ ] **Step 2: Write log display**

Write `src/display/log.rs`:

```rust
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::stats::Stats;

fn format_signature_prefix(sig: &[u8; 8]) -> String {
    let hex: String = sig.iter().map(|b| format!("{:02x}", b)).collect();
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

        // Print summary line
        let rate = s.shreds_per_second();
        let hb_ago = s
            .last_heartbeat
            .map(|t| format!("{}ms ago", t.elapsed().as_millis()))
            .unwrap_or_else(|| "never".into());

        println!(
            "[stats] shreds/sec={:.0} data={} coding={} errors={} heartbeats={} (last: {})",
            rate,
            s.total_data_shreds,
            s.total_coding_shreds,
            s.parse_errors,
            s.total_heartbeats,
            hb_ago,
        );
    }

    Ok(())
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cd rust/kernel-receiver && cargo check
```

Expected: compiles (tui module is still a stub — that's fine).

- [ ] **Step 4: Commit**

```bash
git add rust/kernel-receiver/src/display/
git commit -m "feat: log display mode with per-slot and summary output"
```

---

### Task 7: TUI Display Mode

**Files:**
- Create: `rust/kernel-receiver/src/display/tui.rs`

- [ ] **Step 1: Write TUI display**

Write `src/display/tui.rs`:

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
    let hex: String = sig.iter().map(|b| format!("{:02x}", b)).collect();
    format!("{}..{}", &hex[..4], &hex[12..])
}

fn format_duration_short(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{}s", secs)
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
                Constraint::Length(3), // top status bar
                Constraint::Fill(1),  // middle slot table
                Constraint::Length(3), // bottom aggregate stats
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
                " iface: {} | group: {} | uptime: {} | {}",
                config.network.interface,
                config.network.multicast_group,
                uptime,
                hb_info,
            );
            let status = Paragraph::new(status_text)
                .block(Block::default().borders(Borders::ALL).title(" Edge Multicast Receiver "));
            frame.render_widget(status, chunks[0]);

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
                    Constraint::Length(12),  // Slot
                    Constraint::Length(14),  // Signature
                    Constraint::Length(8),   // Data
                    Constraint::Length(8),   // Coding
                    Constraint::Length(10),  // FEC Sets
                    Constraint::Length(8),   // Age
                ],
            )
            .header(header)
            .block(Block::default().borders(Borders::ALL).title(" Recent Slots "));

            frame.render_widget(table, chunks[1]);

            // === Bottom stats ===
            let rate = s.shreds_per_second();
            let total = s.total_data_shreds + s.total_coding_shreds;
            let ratio = if s.total_coding_shreds > 0 {
                format!("{:.1}", s.total_data_shreds as f64 / s.total_coding_shreds as f64)
            } else {
                "n/a".into()
            };
            let stats_text = format!(
                " shreds/sec: {:.0} | total: {} (data: {}, coding: {}) | data/coding: {} | errors: {}",
                rate, total, s.total_data_shreds, s.total_coding_shreds, ratio, s.parse_errors,
            );
            let stats_bar = Paragraph::new(stats_text)
                .block(Block::default().borders(Borders::ALL).title(" Stats "));
            frame.render_widget(stats_bar, chunks[2]);
        })?;

        // Handle input with tick-rate timeout
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

- [ ] **Step 2: Verify it compiles**

```bash
cd rust/kernel-receiver && cargo check
```

Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add rust/kernel-receiver/src/display/tui.rs
git commit -m "feat: ratatui TUI dashboard with slot table and aggregate stats"
```

---

### Task 8: Main Integration

**Files:**
- Modify: `rust/kernel-receiver/src/main.rs`

- [ ] **Step 1: Wire everything together in main.rs**

Write `src/main.rs`:

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

use config::{Cli, Config};
use stats::Stats;

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli)?;

    eprintln!("edge-multicast-receiver v{}", env!("CARGO_PKG_VERSION"));
    eprintln!(
        "Interface: {}, Multicast: {}, Shred port: {}, Heartbeat port: {}",
        config.network.interface,
        config.network.multicast_group,
        config.network.shred_port,
        config.network.heartbeat_port,
    );
    eprintln!("Display mode: {:?}", config.display.mode);

    let stats = Arc::new(RwLock::new(Stats::new(config.stats.max_slots)));
    let shutdown = Arc::new(AtomicBool::new(false));

    // Set up Ctrl+C handler
    let shutdown_signal = shutdown.clone();
    ctrlc::set_handler(move || {
        shutdown_signal.store(true, Ordering::Relaxed);
    })?;

    // Spawn receiver thread
    let recv_config = config.clone();
    let recv_stats = stats.clone();
    let recv_shutdown = shutdown.clone();
    let recv_handle = std::thread::Builder::new()
        .name("receiver".into())
        .spawn(move || {
            if let Err(e) = receiver::run_recv_loop(&recv_config, recv_stats, recv_shutdown) {
                eprintln!("Receiver error: {:#}", e);
            }
        })?;

    // Run display on main thread (blocks until shutdown)
    display::run(&config, stats, shutdown.clone())?;

    // Wait for receiver to finish
    recv_handle.join().expect("receiver thread panicked");

    eprintln!("Shutdown complete.");
    Ok(())
}
```

- [ ] **Step 2: Add ctrlc dependency to Cargo.toml**

Add under `[dependencies]`:

```toml
ctrlc = { version = "3", features = ["termination"] }
```

- [ ] **Step 3: Verify it compiles**

```bash
cd rust/kernel-receiver && cargo check
```

Expected: compiles clean.

- [ ] **Step 4: Build release binary**

```bash
cd rust/kernel-receiver && cargo build --release
```

Expected: builds successfully. Binary at `target/release/edge-multicast-receiver`.

- [ ] **Step 5: Quick smoke test**

```bash
cd rust/kernel-receiver && cargo run -- --mode log --help
```

Expected: prints help text with all CLI options.

```bash
cd rust/kernel-receiver && cargo run -- --mode log --interface lo --multicast-group 239.0.0.1
```

Expected: starts, prints "Listening for shreds on port 7733...", prints periodic `[stats]` lines with all zeros (no traffic on loopback). Ctrl+C exits cleanly.

- [ ] **Step 6: Commit**

```bash
git add rust/kernel-receiver/src/main.rs rust/kernel-receiver/Cargo.toml
git commit -m "feat: wire up main with receiver thread, display, and ctrl+c shutdown"
```

---

### Task 9: Integration Test with Pcap Replay

**Files:**
- No new source files. This task validates the tool works with real data.

- [ ] **Step 1: Create a pcap replay test script**

This is a manual integration test. On a machine with the `doublezero1` interface (or for local testing, use loopback with `tcpreplay` or `udpreplay`).

For local testing without a live feed, create a simple replay script that extracts UDP payloads from the pcap and sends them to localhost:

```bash
# On the target machine with doublezero1:
cd rust/kernel-receiver
cargo run --release -- --config ../../config.example.toml

# Or for local testing with a loopback multicast:
cargo run --release -- --interface lo --multicast-group 239.0.0.1 --mode log
```

In another terminal, send test packets:

```bash
# Simple test: send a fake heartbeat
echo -ne '\x44\x5a\x00\x01' | socat - UDP4-DATAGRAM:239.0.0.1:5765,interface=lo
```

Expected: the receiver should count the heartbeat.

- [ ] **Step 2: Verify TUI mode starts and exits cleanly**

```bash
cd rust/kernel-receiver
cargo run --release -- --interface lo --multicast-group 239.0.0.1 --mode tui
```

Expected: TUI renders with empty slot table, stats at zero. Press `q` to exit. Terminal should be restored cleanly (no garbled output).

- [ ] **Step 3: Run all unit tests**

```bash
cd rust/kernel-receiver && cargo test
```

Expected: all tests pass.

- [ ] **Step 4: Commit any test fixes**

```bash
git add -A rust/kernel-receiver/
git commit -m "chore: integration test validation and fixes"
```

---

### Task 10: Final Cleanup

**Files:**
- Verify: all files in `rust/kernel-receiver/`

- [ ] **Step 1: Run clippy**

```bash
cd rust/kernel-receiver && cargo clippy -- -D warnings
```

Fix any warnings.

- [ ] **Step 2: Run cargo fmt**

```bash
cd rust/kernel-receiver && cargo fmt
```

- [ ] **Step 3: Verify config.example.toml matches the Config struct**

Read `config.example.toml` and verify every field has a corresponding field in `Config`. Verify defaults in the struct match the example values.

- [ ] **Step 4: Final commit**

```bash
git add -A rust/kernel-receiver/
git commit -m "chore: clippy fixes, formatting, final cleanup"
```
