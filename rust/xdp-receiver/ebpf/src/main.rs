#![no_std]
#![no_main]

use aya_ebpf::{
    bindings::xdp_action,
    macros::{map, xdp},
    maps::{Array, PerCpuArray, XskMap},
    programs::XdpContext,
};
use xdp_filter_common::{FilterConfig, XdpStats};

#[map]
static CONFIG: Array<FilterConfig> = Array::with_max_entries(1, 0);

#[map]
static STATS: PerCpuArray<XdpStats> = PerCpuArray::with_max_entries(1, 0);

#[map]
static XSKMAP: XskMap = XskMap::with_max_entries(8, 0);

const ETH_P_IP: u16 = 0x0800;
const IPPROTO_GRE: u8 = 47;
const IPPROTO_UDP: u8 = 17;
const ETH_HDR_LEN: usize = 14;
const IPV4_HDR_MIN_LEN: usize = 20;
const GRE_HDR_MIN_LEN: usize = 4;

#[inline(always)]
fn inc_redirected() {
    if let Some(stats) = STATS.get_ptr_mut(0) {
        unsafe { (*stats).redirected += 1 };
    }
}

#[inline(always)]
fn inc_passed() {
    if let Some(stats) = STATS.get_ptr_mut(0) {
        unsafe { (*stats).passed += 1 };
    }
}

#[inline(always)]
fn inc_errors() {
    if let Some(stats) = STATS.get_ptr_mut(0) {
        unsafe { (*stats).errors += 1 };
    }
}

/// Read a u16 from packet data, converting from network byte order to host order.
#[inline(always)]
unsafe fn read_u16(data: usize, data_end: usize, offset: usize) -> Option<u16> {
    if data + offset + 2 > data_end {
        return None;
    }
    Some(u16::from_be(((data + offset) as *const u16).read_unaligned()))
}

/// Read a u8 from packet data.
#[inline(always)]
unsafe fn read_u8(data: usize, data_end: usize, offset: usize) -> Option<u8> {
    if data + offset + 1 > data_end {
        return None;
    }
    Some(*((data + offset) as *const u8))
}

/// Read a u32 from packet data, converting from network byte order to host order.
#[inline(always)]
unsafe fn read_u32(data: usize, data_end: usize, offset: usize) -> Option<u32> {
    if data + offset + 4 > data_end {
        return None;
    }
    Some(u32::from_be(((data + offset) as *const u32).read_unaligned()))
}

#[xdp]
pub fn xdp_filter(ctx: XdpContext) -> u32 {
    match try_xdp_filter(&ctx) {
        Ok(action) => action,
        Err(_) => {
            inc_errors();
            xdp_action::XDP_PASS
        }
    }
}

fn try_xdp_filter(ctx: &XdpContext) -> Result<u32, ()> {
    let data = ctx.data() as usize;
    let data_end = ctx.data_end() as usize;

    // Load filter config from BPF map
    let cfg = CONFIG.get(0).ok_or(())?;

    // 1. Parse Ethernet header (14 bytes)
    let ethertype = unsafe { read_u16(data, data_end, 12) }.ok_or(())?;
    if ethertype != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    let mut offset = ETH_HDR_LEN;

    // 2. Parse outer IPv4 header
    let outer_ihl_byte = unsafe { read_u8(data, data_end, offset) }.ok_or(())?;
    let outer_ihl = ((outer_ihl_byte & 0x0F) as usize) * 4;
    if outer_ihl < IPV4_HDR_MIN_LEN {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let outer_proto = unsafe { read_u8(data, data_end, offset + 9) }.ok_or(())?;
    if outer_proto != IPPROTO_GRE {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    offset += outer_ihl;

    // 3. Parse GRE header (minimum 4 bytes)
    if data + offset + GRE_HDR_MIN_LEN > data_end {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let gre_flags = unsafe { read_u16(data, data_end, offset) }.ok_or(())?;
    let gre_protocol = unsafe { read_u16(data, data_end, offset + 2) }.ok_or(())?;
    if gre_protocol != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // Calculate GRE header length based on C/K/S flags
    let mut gre_len = GRE_HDR_MIN_LEN;
    if gre_flags & 0x8000 != 0 {
        gre_len += 4; // Checksum + Reserved1
    }
    if gre_flags & 0x2000 != 0 {
        gre_len += 4; // Key
    }
    if gre_flags & 0x1000 != 0 {
        gre_len += 4; // Sequence Number
    }
    offset += gre_len;

    // 4. Parse inner IPv4 header
    let inner_ihl_byte = unsafe { read_u8(data, data_end, offset) }.ok_or(())?;
    let inner_ihl = ((inner_ihl_byte & 0x0F) as usize) * 4;
    if inner_ihl < IPV4_HDR_MIN_LEN {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let inner_proto = unsafe { read_u8(data, data_end, offset + 9) }.ok_or(())?;
    if inner_proto != IPPROTO_UDP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    // Check destination IP matches configured multicast group
    let inner_dst_ip = unsafe { read_u32(data, data_end, offset + 16) }.ok_or(())?;
    if inner_dst_ip != cfg.multicast_ip {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    offset += inner_ihl;

    // 5. Parse UDP header — check destination port
    let udp_dst_port = unsafe { read_u16(data, data_end, offset + 2) }.ok_or(())?;
    if udp_dst_port != cfg.shred_port && udp_dst_port != cfg.heartbeat_port {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // 6. Match! Redirect to AF_XDP socket
    inc_redirected();
    let queue_id = unsafe { (*ctx.ctx).rx_queue_index };
    XSKMAP.redirect(queue_id, 0).map_err(|_| ())
}

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}
