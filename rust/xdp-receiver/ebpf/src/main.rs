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

/// Read a u8 from a cursor pointer. Caller must have already bounds-checked.
#[inline(always)]
unsafe fn read_u8_at(ptr: usize, off: usize) -> u8 {
    *((ptr + off) as *const u8)
}

/// Read a big-endian u16 from a cursor pointer using byte loads.
/// Caller must have already bounds-checked ptr + off + 2.
#[inline(always)]
unsafe fn read_u16_at(ptr: usize, off: usize) -> u16 {
    let b0 = *((ptr + off) as *const u8) as u16;
    let b1 = *((ptr + off + 1) as *const u8) as u16;
    b0 << 8 | b1
}

/// Read a big-endian u32 from a cursor pointer using byte loads.
/// Caller must have already bounds-checked ptr + off + 4.
#[inline(always)]
unsafe fn read_u32_at(ptr: usize, off: usize) -> u32 {
    let b0 = *((ptr + off) as *const u8) as u32;
    let b1 = *((ptr + off + 1) as *const u8) as u32;
    let b2 = *((ptr + off + 2) as *const u8) as u32;
    let b3 = *((ptr + off + 3) as *const u8) as u32;
    b0 << 24 | b1 << 16 | b2 << 8 | b3
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

    let cfg = CONFIG.get(0).ok_or(())?;

    // 1. Ethernet header: need 14 bytes (read ethertype at 12-13)
    if data + 14 > data_end {
        return Err(());
    }
    let ethertype = unsafe { read_u16_at(data, 12) };
    if ethertype != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // 2. Outer IPv4 at fixed offset 14: need at least 20 bytes (through proto at +9)
    if data + 34 > data_end {
        return Err(());
    }
    let outer_ihl_byte = unsafe { read_u8_at(data, 14) };
    let outer_ihl = ((outer_ihl_byte & 0x0F) as usize) * 4;
    if outer_ihl < 20 {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let outer_proto = unsafe { read_u8_at(data, 23) };
    if outer_proto != IPPROTO_GRE {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // --- From here, offset is variable (depends on outer IHL) ---
    // Use cursor pointer pattern: compute pointer, bounds-check, read from it.

    // 3. GRE header at data + 14 + outer_ihl
    let cursor = data + 14 + outer_ihl;
    if cursor + 4 > data_end {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let gre_flags = unsafe { read_u16_at(cursor, 0) };
    let gre_protocol = unsafe { read_u16_at(cursor, 2) };
    if gre_protocol != ETH_P_IP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // Variable GRE header length based on C/K/S flags
    let mut gre_len: usize = 4;
    if gre_flags & 0x8000 != 0 {
        gre_len += 4;
    }
    if gre_flags & 0x2000 != 0 {
        gre_len += 4;
    }
    if gre_flags & 0x1000 != 0 {
        gre_len += 4;
    }

    // 4. Inner IPv4 at cursor + gre_len: need 20 bytes (through dst_ip at +16..+19)
    let cursor = cursor + gre_len;
    if cursor + 20 > data_end {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let inner_ihl_byte = unsafe { read_u8_at(cursor, 0) };
    let inner_ihl = ((inner_ihl_byte & 0x0F) as usize) * 4;
    if inner_ihl < 20 {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let inner_proto = unsafe { read_u8_at(cursor, 9) };
    if inner_proto != IPPROTO_UDP {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }
    let inner_dst_ip = unsafe { read_u32_at(cursor, 16) };
    if inner_dst_ip != cfg.multicast_ip {
        inc_passed();
        return Ok(xdp_action::XDP_PASS);
    }

    // 5. UDP header at cursor + inner_ihl: need 4 bytes (dst port at +2..+3)
    let cursor = cursor + inner_ihl;
    if cursor + 4 > data_end {
        inc_errors();
        return Ok(xdp_action::XDP_PASS);
    }
    let udp_dst_port = unsafe { read_u16_at(cursor, 2) };
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
