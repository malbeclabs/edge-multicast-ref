use anyhow::Result;

mod config;
mod display;
#[cfg(target_os = "linux")]
mod receiver;
mod shred_parser;
mod stats;
#[cfg(target_os = "linux")]
mod xdp;

fn main() -> Result<()> {
    println!("edge-multicast-xdp-receiver starting...");
    Ok(())
}
