//go:build ignore

#include <linux/bpf.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>

#define ETH_P_IP_BE 0x0008  /* 0x0800 in network byte order (little-endian host) */
#define IPPROTO_GRE 47
#define IPPROTO_UDP 17

struct filter_config {
    __u32 multicast_ip;   /* network byte order */
    __u16 shred_port;     /* host byte order */
    __u16 heartbeat_port; /* host byte order */
};

struct xdp_stats {
    __u64 redirected;
    __u64 passed;
    __u64 errors;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct filter_config);
} config SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __uint(max_entries, 1);
    __type(key, __u32);
    __type(value, struct xdp_stats);
} stats SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __uint(max_entries, 8);
    __type(key, __u32);
    __type(value, __u32);
} xskmap SEC(".maps");

static __always_inline void inc_redirected(void) {
    __u32 key = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->redirected += 1;
}

static __always_inline void inc_passed(void) {
    __u32 key = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->passed += 1;
}

static __always_inline void inc_errors(void) {
    __u32 key = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats, &key);
    if (s)
        s->errors += 1;
}

/* Read a big-endian u16 from a cursor pointer using byte loads. */
static __always_inline __u16 read_be16(void *ptr) {
    __u8 *b = (__u8 *)ptr;
    return ((__u16)b[0] << 8) | (__u16)b[1];
}

/* Read a big-endian u32 from a cursor pointer using byte loads. */
static __always_inline __u32 read_be32(void *ptr) {
    __u8 *b = (__u8 *)ptr;
    return ((__u32)b[0] << 24) | ((__u32)b[1] << 16) | ((__u32)b[2] << 8) | (__u32)b[3];
}

SEC("xdp")
int xdp_filter(struct xdp_md *ctx) {
    void *data = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    __u32 key = 0;
    struct filter_config *cfg = bpf_map_lookup_elem(&config, &key);
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
    if (ethertype != 0x0800) { /* not IPv4 */
        inc_passed();
        return XDP_PASS;
    }

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
    __u8 outer_proto = *(__u8 *)(data + 23); /* offset 14 + 9 */
    if (outer_proto != IPPROTO_GRE) {
        inc_passed();
        return XDP_PASS;
    }

    /* --- Variable offset from here (depends on outer IHL) --- */
    /* Use cursor pointer pattern for verifier compatibility. */

    /* 3. GRE header at data + 14 + outer_ihl */
    void *cursor = data + 14 + outer_ihl;
    if (cursor + 4 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u16 gre_flags = read_be16(cursor);
    __u16 gre_protocol = read_be16(cursor + 2);
    if (gre_protocol != 0x0800) { /* GRE encapsulated protocol not IPv4 */
        inc_passed();
        return XDP_PASS;
    }

    /* Variable GRE header length based on C/K/S flags */
    __u32 gre_len = 4;
    if (gre_flags & 0x8000) gre_len += 4; /* checksum */
    if (gre_flags & 0x2000) gre_len += 4; /* key */
    if (gre_flags & 0x1000) gre_len += 4; /* sequence */

    /* 4. Inner IPv4 at cursor + gre_len: need 20 bytes */
    cursor = cursor + gre_len;
    if (cursor + 20 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u8 inner_ihl_byte = *(__u8 *)cursor;
    __u32 inner_ihl = ((__u32)(inner_ihl_byte & 0x0F)) * 4;
    if (inner_ihl < 20) {
        inc_errors();
        return XDP_PASS;
    }
    __u8 inner_proto = *(__u8 *)(cursor + 9);
    if (inner_proto != IPPROTO_UDP) {
        inc_passed();
        return XDP_PASS;
    }
    __u32 inner_dst_ip = read_be32(cursor + 16);
    if (inner_dst_ip != cfg->multicast_ip) {
        inc_passed();
        return XDP_PASS;
    }

    /* 5. UDP header at cursor + inner_ihl: need 4 bytes (dst port at +2) */
    cursor = cursor + inner_ihl;
    if (cursor + 4 > data_end) {
        inc_errors();
        return XDP_PASS;
    }
    __u16 udp_dst_port = read_be16(cursor + 2);
    if (udp_dst_port != cfg->shred_port && udp_dst_port != cfg->heartbeat_port) {
        inc_passed();
        return XDP_PASS;
    }

    /* 6. Match! Redirect to AF_XDP socket. */
    inc_redirected();
    return bpf_redirect_map(&xskmap, ctx->rx_queue_index, XDP_PASS);
}

char _license[] SEC("license") = "GPL";
