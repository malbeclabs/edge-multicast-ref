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
