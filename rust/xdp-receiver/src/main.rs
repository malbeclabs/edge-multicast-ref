use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use anyhow::Result;
use clap::Parser;

mod config;
mod display;
#[cfg(target_os = "linux")]
mod receiver;
mod shred_parser;
mod stats;
#[cfg(target_os = "linux")]
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

    #[cfg(not(target_os = "linux"))]
    {
        anyhow::bail!(
            "XDP receiver requires Linux. This binary was compiled on a non-Linux platform \
             and cannot attach XDP programs or create AF_XDP sockets."
        );
    }

    #[cfg(target_os = "linux")]
    run_linux(config)
}

#[cfg(target_os = "linux")]
fn run_linux(config: Config) -> Result<()> {
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
