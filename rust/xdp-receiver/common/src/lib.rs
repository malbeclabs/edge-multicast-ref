#![no_std]

/// Filter configuration written to BPF Array map by userspace, read by eBPF program.
/// All values are stored in host byte order. The eBPF program converts packet bytes
/// from network order via from_be() before comparing.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FilterConfig {
    pub multicast_ip: u32,
    pub shred_port: u16,
    pub heartbeat_port: u16,
}

/// Per-CPU statistics counters updated by eBPF program, read by userspace.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct XdpStats {
    pub redirected: u64,
    pub passed: u64,
    pub errors: u64,
}

#[cfg(feature = "userspace")]
unsafe impl aya::Pod for FilterConfig {}

#[cfg(feature = "userspace")]
unsafe impl aya::Pod for XdpStats {}
