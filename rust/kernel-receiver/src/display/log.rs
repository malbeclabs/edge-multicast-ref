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
