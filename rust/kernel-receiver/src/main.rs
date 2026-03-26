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
