//go:build ignore

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <linux/if_ether.h>
#include <linux/ip.h>

#define IPPROTO_GRE 47
#define IPPROTO_UDP 17

struct decap_config {
    __u32 multicast_ip;     /* big-endian numeric (read_be32 format), 0 = decap all */
    __u16 shred_port;       /* host byte order, 0 = don't filter */
    __u16 heartbeat_port;   /* host byte order, 0 = don't filter */
};

struct decap_stats {
    __u64 decapped;
    __u64 passed;
    __u64 errors;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct decap_config);
} config SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct decap_stats);
} stats SEC(".maps");

static __always_inline void inc_decapped(void) {
    __u32 key = 0;
    struct decap_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->decapped += 1;
}

static __always_inline void inc_passed(void) {
    __u32 key = 0;
    struct decap_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->passed += 1;
}

static __always_inline void inc_errors(void) {
    __u32 key = 0;
    struct decap_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->errors += 1;
}

/* Read a big-endian u16 from a pointer using byte loads (verifier-safe). */
static __always_inline __u16 read_be16(void *ptr) {
    __u8 *b = (__u8 *)ptr;
    return ((__u16)b[0] << 8) | (__u16)b[1];
}

/* Read a big-endian u32 from a pointer using byte loads (verifier-safe). */
static __always_inline __u32 read_be32(void *ptr) {
    __u8 *b = (__u8 *)ptr;
    return ((__u32)b[0] << 24) | ((__u32)b[1] << 16) | ((__u32)b[2] << 8) | (__u32)b[3];
}

SEC("xdp")
int xdp_decap(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    __u32 key = 0;
    struct decap_config *cfg = bpf_map_lookup_elem(&config, &key);
    if (!cfg) {
        inc_errors();
        return XDP_PASS;
    }

    /* 1. Ethernet header: need 14 bytes */
    if (data + 14 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u16 ethertype = read_be16(data + 12);
    if (ethertype != 0x0800) {
        inc_passed();
        return XDP_PASS;
    }

    /* Save source MAC before we modify anything. */
    __u8 src_mac[6];
    __u8 *eth_src = (__u8 *)(data + 6);
    src_mac[0] = eth_src[0];
    src_mac[1] = eth_src[1];
    src_mac[2] = eth_src[2];
    src_mac[3] = eth_src[3];
    src_mac[4] = eth_src[4];
    src_mac[5] = eth_src[5];

    /* 2. Outer IPv4 at offset 14: need at least 20 bytes */
    if (data + 34 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u8 outer_ihl_byte = *(__u8 *)(data + 14);
    __u32 outer_ihl = ((__u32)(outer_ihl_byte & 0x0F)) * 4;
    if (outer_ihl < 20) {
        inc_errors();
        return XDP_PASS;
    }
    __u8 outer_proto = *(__u8 *)(data + 23);
    if (outer_proto != IPPROTO_GRE) {
        inc_passed();
        return XDP_PASS;
    }

    /* 3. GRE header at data + 14 + outer_ihl */
    void *cursor = data + 14 + outer_ihl;
    if (cursor + 4 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u16 gre_flags = read_be16(cursor);
    __u16 gre_protocol = read_be16(cursor + 2);
    if (gre_protocol != 0x0800) {
        inc_passed();
        return XDP_PASS;
    }

    __u32 gre_len = 4;
    if (gre_flags & 0x8000) gre_len += 4; /* checksum */
    if (gre_flags & 0x2000) gre_len += 4; /* key */
    if (gre_flags & 0x1000) gre_len += 4; /* sequence */

    /* 4. Inner IPv4 at cursor + gre_len */
    cursor = cursor + gre_len;
    if (cursor + 20 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u8 inner_proto = *(__u8 *)(cursor + 9);
    __u32 inner_dst_ip = read_be32(cursor + 16);

    /* Filter by multicast IP if configured (0 = decap all GRE-over-IPv4). */
    if (cfg->multicast_ip != 0 && inner_dst_ip != cfg->multicast_ip) {
        inc_passed();
        return XDP_PASS;
    }

    /* Optional port filtering: only if both ports are configured and inner is UDP. */
    if (cfg->shred_port != 0 && cfg->heartbeat_port != 0) {
        if (inner_proto != IPPROTO_UDP) {
            inc_passed();
            return XDP_PASS;
        }
        __u8 inner_ihl_byte = *(__u8 *)cursor;
        __u32 inner_ihl = ((__u32)(inner_ihl_byte & 0x0F)) * 4;
        if (inner_ihl < 20) {
            inc_errors();
            return XDP_PASS;
        }
        void *udp = cursor + inner_ihl;
        if (udp + 4 > data_end) {
            inc_errors();
            return XDP_PASS;
        }
        __u16 udp_dst_port = read_be16(udp + 2);
        if (udp_dst_port != cfg->shred_port && udp_dst_port != cfg->heartbeat_port) {
            inc_passed();
            return XDP_PASS;
        }
    }

    /* 5. Compute multicast destination MAC from inner IP.
     *    IPv4 multicast MAC: 01:00:5e:<low 23 bits of IP>
     *    inner_dst_ip is in host-order numeric (from read_be32). */
    __u8 dst_mac[6];
    dst_mac[0] = 0x01;
    dst_mac[1] = 0x00;
    dst_mac[2] = 0x5e;
    dst_mac[3] = (inner_dst_ip >> 16) & 0x7f;
    dst_mac[4] = (inner_dst_ip >> 8) & 0xff;
    dst_mac[5] = inner_dst_ip & 0xff;

    /* 6. Strip outer IP + GRE by advancing the packet start.
     *    After adjust_head(strip_len), new data starts strip_len bytes in,
     *    leaving exactly 14 bytes of overwritable space before inner IP. */
    __u32 strip_len = outer_ihl + gre_len;
    if (bpf_xdp_adjust_head(ctx, (int)strip_len) != 0) {
        inc_errors();
        return XDP_PASS;
    }

    /* 7. Write new Ethernet header. Pointers are invalidated after adjust_head. */
    data = (void *)(long)ctx->data;
    data_end = (void *)(long)ctx->data_end;
    if (data + 14 > data_end) {
        inc_errors();
        return XDP_PASS;
    }

    __u8 *eth = (__u8 *)data;
    /* Destination MAC (multicast) */
    eth[0] = dst_mac[0]; eth[1] = dst_mac[1]; eth[2] = dst_mac[2];
    eth[3] = dst_mac[3]; eth[4] = dst_mac[4]; eth[5] = dst_mac[5];
    /* Source MAC (preserved from original frame) */
    eth[6]  = src_mac[0]; eth[7]  = src_mac[1]; eth[8]  = src_mac[2];
    eth[9]  = src_mac[3]; eth[10] = src_mac[4]; eth[11] = src_mac[5];
    /* EtherType: IPv4 */
    eth[12] = 0x08;
    eth[13] = 0x00;

    inc_decapped();
    return XDP_PASS;
}

char _license[] SEC("license") = "GPL";
