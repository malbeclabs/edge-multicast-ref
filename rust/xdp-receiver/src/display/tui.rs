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
