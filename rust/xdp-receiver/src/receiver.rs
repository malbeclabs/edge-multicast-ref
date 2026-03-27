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
