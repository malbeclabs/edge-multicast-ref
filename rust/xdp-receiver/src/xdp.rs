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
    let mut ebpf = Ebpf::load(aya::include_bytes_aligned!(concat!(
        env!("OUT_DIR"),
        "/xdp-filter"
    )))
    .context("failed to load eBPF program")?;

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

    let per_cpu_values = stats_map.get(&0, 0).context("failed to read STATS map")?;

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
