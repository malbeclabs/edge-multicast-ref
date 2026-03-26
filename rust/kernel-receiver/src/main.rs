use anyhow::Result;

mod config;
mod display;
mod receiver;
mod shred_parser;
mod stats;

fn main() -> Result<()> {
    println!("edge-multicast-receiver starting...");
    Ok(())
}
