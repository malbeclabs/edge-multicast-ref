# C Multicast Shred Receivers — Design Spec

**Date:** 2026-04-08
**Status:** Draft
**Scope:** C implementations of both the kernel-socket and XDP receivers, matching the existing Rust and Go reference designs as closely as practical.

## Overview

Two C binaries that consume Solana shred multicast feeds from DoubleZero edge infrastructure and display live statistics:

- **`c/kernel-receiver`** — receives via standard kernel UDP sockets on a GRE tunnel interface (e.g. `doublezero1`). Matches `rust/kernel-receiver` and `go/kernel-receiver`.
- **`c/xdp-receiver`** — receives via an XDP eBPF program attached to a physical NIC, redirecting matched packets to an AF_XDP socket. Matches `rust/xdp-receiver` and `go/xdp-receiver`.

Both binaries share a `c/common/` directory containing shred header parsing, stats tracking, config loading, and display code.

**Target parity:** feature and output parity with the Rust implementations. Same CLI flags, same `config.toml` schema, same TUI layout, same log output format. A user running all three (Rust / Go / C) should see equivalent output.

**Non-goals for this iteration:**
- FEC recovery / deshredding
- Heartbeat sending
- Multi-queue AF_XDP binding
- Zero-copy AF_XDP mode
- Cross-platform support (Linux-only like the other implementations)

## Dependencies

**Build-time:**
- `gcc` or `clang` — userspace compilation
- `clang` (>= 11) — eBPF compilation to `bpfel` target (XDP receiver only)
- `make`
- `libbpf-dev` — loading eBPF programs, map access (XDP receiver only)
- `libxdp-dev` — AF_XDP socket, UMEM, rings (XDP receiver only)
- `libelf-dev`, `zlib1g-dev` — transitive deps of libbpf (XDP receiver only)
- `libncurses-dev` (or `libncursesw-dev`) — TUI

**Runtime:**
- Linux 5.4+ for XDP receiver
- Capabilities `cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep` on the XDP binary (or run as root)

**Vendored (checked into the repo):**
- `tomlc99` — single-file public-domain TOML parser (~2000 lines, one `.c` + one `.h`). Used by both receivers for config parsing.

No other external libraries. In particular: no libmnl, no libnl, no JSON/YAML parsers, no test frameworks.

## Project Structure

```
c/
├── README.md
├── common/
│   ├── shred.h            # Packed shred common header + parsed_shred_t + classification
│   ├── shred.c
│   ├── stats.h            # stats_t, slot_stats_t, ring buffer, XDP counter fields
│   ├── stats.c
│   ├── config.h           # config_t nested structs, TOML load, CLI merge
│   ├── config.c
│   ├── display.h          # display_run dispatcher
│   ├── display_tui.c      # ncurses dashboard
│   ├── display_log.c      # streaming logger
│   ├── toml.h             # vendored tomlc99
│   ├── toml.c             # vendored tomlc99
│   ├── shred_test.c       # unit tests for shred parsing
│   ├── stats_test.c       # unit tests for stats
│   └── NOTICE             # Apache 2.0 attribution (firedancer), tomlc99 license
├── kernel-receiver/
│   ├── Makefile
│   ├── config.example.toml
│   ├── main.c             # CLI + config + pthread setup + shutdown
│   └── receiver.c         # UDP socket creation + poll() loop
└── xdp-receiver/
    ├── Makefile
    ├── config.example.toml
    ├── main.c             # CLI + config + XDP attach + pthread setup + shutdown
    ├── receiver.c         # AF_XDP RX loop + GRE header stripping
    ├── xdp.c              # libbpf load/attach + map config + stats reading
    ├── find_udp_payload_test.c  # unit tests for header stripping
    └── bpf/
        └── xdp_filter.c   # eBPF source (clang -target bpf)
```

The Go implementation uses a `go/internal/` package for shared code. The C implementation mirrors this pattern with `c/common/`, but rather than a shared library each receiver's Makefile compiles `../common/*.c` directly into its own binary. This keeps each binary self-contained with no install step.

## Shared Code (`c/common/`)

### `shred.h` / `shred.c`

Shred header parsing. Derived from `firedancer/src/ballet/shred/fd_shred.h` (Apache 2.0). Cited in a file header comment; an Apache 2.0 attribution appears in `c/common/NOTICE`.

```c
// Packed common header — identical layout for data and coding shreds.
struct __attribute__((packed)) shred_common_hdr {
    uint8_t  signature[64];   // offset 0x00
    uint8_t  variant;         // offset 0x40
    uint64_t slot;            // offset 0x41
    uint32_t idx;             // offset 0x49
    uint16_t version;         // offset 0x4d
    uint32_t fec_set_idx;     // offset 0x4f
};  // sizeof == 83

typedef struct {
    uint64_t slot;
    uint32_t idx;
    uint32_t fec_set_idx;
    uint16_t version;
    uint8_t  signature[64];
    bool     is_data;
} parsed_shred_t;

// Returns true on success, false if payload is too short or variant is invalid.
bool shred_parse(const uint8_t *payload, size_t len, parsed_shred_t *out);
```

**Classification logic** (matches firedancer):

- `variant == 0xa5` → legacy data
- `variant == 0x5a` → legacy coding
- `(variant & 0xC0) == 0x80` → merkle data (all merkle data variants)
- `(variant & 0xC0) == 0x40` → merkle coding (all merkle coding variants)
- Anything else → parse failure

All multi-byte fields in the shred header are little-endian on the wire, which matches x86_64/ARM64 host byte order, so direct reads via the packed struct work without byteswapping.

### `stats.h` / `stats.c`

Mirrors `rust/kernel-receiver/src/stats.rs` and `rust/xdp-receiver/src/stats.rs`.

```c
#define STATS_MAX_FEC_SETS_PER_SLOT 16
#define STATS_RATE_WINDOW_MAX 16384

typedef uint8_t signature_prefix_t[8];

typedef struct {
    uint64_t slot;
    uint64_t data_shred_count;
    uint64_t coding_shred_count;
    uint32_t highest_data_index;
    size_t   fec_set_count;
    uint32_t fec_set_indices[STATS_MAX_FEC_SETS_PER_SLOT];
    signature_prefix_t signature_prefix;
    struct timespec first_seen;
    struct timespec last_seen;
} slot_stats_t;

typedef struct {
    // Globals
    uint64_t total_data_shreds;
    uint64_t total_coding_shreds;
    uint64_t total_heartbeats;
    uint64_t parse_errors;
    struct timespec last_heartbeat;    // tv_sec == 0 means "never"
    struct timespec start_time;

    // Bounded ring buffer of recent slots, kept sorted by slot number ascending.
    slot_stats_t *slots;               // malloc'd, capacity = max_slots
    size_t slots_len;
    size_t max_slots;

    // Rate window: circular buffer of recent shred timestamps.
    struct timespec rate_window[STATS_RATE_WINDOW_MAX];
    size_t rate_window_head;
    size_t rate_window_len;

    // XDP-specific fields (unused/zero in kernel-receiver).
    char     xdp_attach_mode[16];      // "", "native", "skb", "unknown"
    uint64_t xdp_redirected;
    uint64_t xdp_passed;
    uint64_t xdp_errors;
    size_t   afxdp_rx_fill_level;
    uint64_t afxdp_fill_starvation;
} stats_t;

void stats_init(stats_t *s, size_t max_slots);
void stats_free(stats_t *s);
void stats_record_shred(stats_t *s, uint64_t slot, bool is_data,
                        uint32_t index, uint32_t fec_set_index,
                        const uint8_t signature[64]);
void stats_record_heartbeat(stats_t *s);
void stats_record_parse_error(stats_t *s);
double stats_shreds_per_second(stats_t *s);
const slot_stats_t *stats_get_slot(const stats_t *s, uint64_t slot);
// Fills `out` with pointers to slots in descending slot order. Returns count.
size_t stats_recent_slots(const stats_t *s, const slot_stats_t **out, size_t out_cap);
void stats_update_xdp_counters(stats_t *s, uint64_t redirected,
                               uint64_t passed, uint64_t errors);
```

**Locking:** `stats.c` functions do not lock internally. The caller (main.c) owns a `pthread_mutex_t` and locks it around every stats access. This mirrors how the Go implementation uses a `sync.Mutex` in the caller rather than embedded in the Stats struct, and makes the locking boundary explicit.

**FEC set tracking:** Each `slot_stats_t` stores up to 16 unique FEC set indices in a sorted `uint32_t` array. `stats_record_shred` uses a linear insertion to maintain sort order and to deduplicate. 16 is more than sufficient in practice (typical slots have 1-4 FEC sets).

**Ring buffer eviction:** `slots[]` is kept sorted by slot number. When `stats_record_shred` would push len > max_slots, the oldest slot (index 0) is removed via `memmove`. O(max_slots) per insertion is fine at max_slots=32.

**Rate window:** A fixed-size circular buffer of 16384 `struct timespec` entries. `stats_shreds_per_second` walks backward from the head and counts entries within the last second. This avoids the dynamic allocation the Rust `Vec<Instant>` implicitly does.

### `config.h` / `config.c`

Uses vendored tomlc99 for parsing. Matches the TOML schema from `rust/xdp-receiver/config.example.toml` exactly.

```c
typedef enum { DISPLAY_MODE_TUI, DISPLAY_MODE_LOG } display_mode_t;
typedef enum { XDP_MODE_AUTO, XDP_MODE_NATIVE, XDP_MODE_SKB } xdp_mode_t;

typedef struct {
    char     interface[32];           // "doublezero1" for kernel, physical NIC name for xdp
    char     multicast_group[16];     // "233.84.178.1"
    uint16_t shred_port;
    uint16_t heartbeat_port;
    size_t   recv_buffer_size;        // only used by kernel-receiver
} network_config_t;

typedef struct {
    xdp_mode_t mode;
    size_t     umem_size;
    size_t     frame_size;
    uint32_t   rx_queue;
} xdp_config_t;

typedef struct {
    display_mode_t mode;
    uint32_t       refresh_hz;
    uint32_t       log_interval_secs;
} display_config_t;

typedef struct {
    size_t max_slots;
} stats_config_t;

typedef struct {
    network_config_t network;
    xdp_config_t     xdp;
    display_config_t display;
    stats_config_t   stats;
} config_t;

void config_init_defaults(config_t *cfg);
int  config_load_file(config_t *cfg, const char *path);   // returns 0 on success
int  config_parse_cli(config_t *cfg, int argc, char **argv);
size_t config_frame_count(const config_t *cfg);           // umem_size / frame_size
```

**Interface field:** the same `interface` field is used by both receivers. The kernel-receiver defaults it to `"doublezero1"` and expects a GRE tunnel interface. The XDP receiver defaults it to `"eth0"` and expects a physical NIC. This keeps one struct without a discriminated variant.

**CLI parsing:** `getopt_long` with the same long-option names as the Rust CLIs: `--interface`, `--multicast-group`, `--shred-port`, `--heartbeat-port`, `--mode` (display mode), `--config`, plus the XDP-only flags `--xdp-mode` and `--rx-queue`. CLI values override file values.

**File not found:** If `--config` points to a non-existent file, that's an error. If the default `config.toml` is absent, defaults are used silently (matches Rust behavior).

### `display.h` + `display_tui.c` + `display_log.c`

```c
// display.h
int display_run(const config_t *cfg,
                stats_t *stats,
                pthread_mutex_t *stats_lock,
                volatile sig_atomic_t *shutdown);
```

Dispatches to `display_tui_run` or `display_log_run` based on `cfg->display.mode`. Both functions block until `*shutdown` is non-zero.

**TUI layout (ncurses):** matches the Rust ratatui layout exactly:

```
┌─ XDP Multicast Receiver ────────────────────────────────────┐
│ iface: eth0 | group: 233.84.178.1 | xdp: native | uptime... │
└──────────────────────────────────────────────────────────────┘
┌─ XDP Stats ──────────────────────────────────────────────────┐   ← only shown if xdp_attach_mode != ""
│ redirected: N | passed: N | errors: N | ring: N/N | ...     │
└──────────────────────────────────────────────────────────────┘
┌─ Recent Slots ───────────────────────────────────────────────┐
│ Slot         Signature      Data  Coding  FEC Sets  Age     │
│ 312345678    d88e..202a      67      34         3   412ms   │
│ ...                                                          │
└──────────────────────────────────────────────────────────────┘
┌─ Stats ──────────────────────────────────────────────────────┐
│ shreds/sec: 8432 | total: 8432 | data/coding: 2.0 | ...     │
└──────────────────────────────────────────────────────────────┘
```

**Conditional XDP panel:** The kernel-receiver leaves `stats->xdp_attach_mode` as an empty string. `display_tui.c` checks for that and omits the XDP Stats panel entirely when it's empty. This is simpler than threading a "show XDP panel" boolean through the display API.

**Refresh:** `halfdelay(tick_10ms)` (or `timeout(ms)` on `getch`) gives us a non-blocking key read with a timeout. `q` or `ESC` sets the shutdown flag and returns.

**Log mode output:** byte-for-byte identical to `rust/xdp-receiver/src/display/log.rs`:

```
slot=312345678 sig=d88e..202a data=67 coding=34 fec_sets=3 age_ms=412
[stats] shreds/sec=8432 data=5621 coding=2811 errors=0 heartbeats=47 (last: 230ms ago) xdp_mode=native redirected=94521 passed=12034 ring_fill=2048/2048
```

In the kernel-receiver the XDP-specific suffix (` xdp_mode=... redirected=... passed=... ring_fill=...`) is omitted when `xdp_attach_mode` is empty.

### `toml.h` / `toml.c`

Verbatim copy of tomlc99 (https://github.com/cktan/tomlc99). Public domain. No modifications. Single source/header pair.

## `c/kernel-receiver`

### `main.c` (~80 lines)

```
1. config_init_defaults, config_load_file, config_parse_cli
2. stats_init
3. pthread_mutex_init
4. Install SIGINT/SIGTERM handler → sets volatile sig_atomic_t shutdown = 1
5. pthread_create receiver thread
6. display_run on main thread (blocks)
7. On return: pthread_join receiver
8. stats_free, pthread_mutex_destroy
9. exit 0
```

### `receiver.c` (~200 lines)

Mirrors `rust/kernel-receiver/src/receiver.rs`.

**Interface IP resolution:** Shell out to `ip -4 -o addr show <interface>`, read stdout via `popen`, and parse the first IP/netmask token (e.g. `169.254.10.233/31`). Falls back to `INADDR_ANY` with a warning on failure. Matches the Rust impl's approach — avoids a libmnl/netlink dependency.

**Socket creation** (called twice, once for shreds and once for heartbeats):
1. `socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)`
2. `setsockopt(SO_REUSEADDR)`, `setsockopt(SO_REUSEPORT)`
3. `bind` to `0.0.0.0:<port>`
4. `setsockopt(IP_ADD_MEMBERSHIP)` with `struct ip_mreq { imr_multiaddr = multicast_group, imr_interface = resolved_iface_ip }`
5. `setsockopt(SO_RCVBUF, recv_buffer_size)`
6. `fcntl(sockfd, F_SETFL, O_NONBLOCK)`

**Main loop:**
```
while (!shutdown) {
    poll(pollfds, 2, 100);  // 100ms timeout
    if (shred_pollfd ready) {
        while (recv != WOULDBLOCK) {
            if (shred_parse ok) {
                pthread_mutex_lock(&lock);
                stats_record_shred(...);
                pthread_mutex_unlock(&lock);
            } else {
                pthread_mutex_lock(&lock);
                stats_record_parse_error(&stats);
                pthread_mutex_unlock(&lock);
            }
        }
    }
    if (heartbeat_pollfd ready) {
        while (recv != WOULDBLOCK) {
            pthread_mutex_lock(&lock);
            stats_record_heartbeat(&stats);
            pthread_mutex_unlock(&lock);
        }
    }
}
```

**Linking:** `-lpthread -lncursesw`.

## `c/xdp-receiver`

### `bpf/xdp_filter.c`

The eBPF program. Compiled with:

```
clang -target bpf -O2 -g -c -I<libbpf include path> bpf/xdp_filter.c -o bpf/xdp_filter.o
```

Logic mirrors `rust/xdp-receiver/ebpf/src/main.rs` exactly:

1. Parse Ethernet header (14 bytes) → check EtherType == 0x0800 (IPv4), else `XDP_PASS`
2. Parse outer IPv4 header → extract IHL, check protocol == 47 (GRE), else `XDP_PASS`
3. Parse GRE header → check inner protocol == 0x0800, compute variable GRE length based on C/K/S flag bits (0x8000/0x2000/0x1000)
4. Parse inner IPv4 header → extract IHL, check protocol == 17 (UDP), check dst IP == configured multicast group, else `XDP_PASS`
5. Parse UDP header → check dst port == shred_port or heartbeat_port, else `XDP_PASS`
6. `bpf_redirect_map(&xsks_map, ctx->rx_queue_index, 0)` on match

**Maps (all SEC(".maps")):**

```c
struct filter_config {
    __u32 multicast_ip;     // host byte order
    __u16 shred_port;       // host byte order
    __u16 heartbeat_port;   // host byte order
};

struct xdp_stats {
    __u64 redirected;
    __u64 passed;
    __u64 errors;
};

struct {
    __uint(type, BPF_MAP_TYPE_ARRAY);
    __type(key, __u32);
    __type(value, struct filter_config);
    __uint(max_entries, 1);
} config_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
    __type(key, __u32);
    __type(value, struct xdp_stats);
    __uint(max_entries, 1);
} stats_map SEC(".maps");

struct {
    __uint(type, BPF_MAP_TYPE_XSKMAP);
    __type(key, __u32);
    __type(value, __u32);
    __uint(max_entries, 8);
} xsks_map SEC(".maps");
```

**Bounds checks:** Every packet byte read is preceded by `if (ptr + N > data_end) return XDP_PASS;`. No shortcuts — the BPF verifier requires this literally at every access.

**Byte order:** Packet bytes are big-endian. Read helpers convert via `bpf_ntohs`/`bpf_ntohl`. Config map fields are stored in host byte order; userspace converts before writing.

**License section:** `char __license[] SEC("license") = "Dual MIT/GPL";`

### `xdp.c` / `xdp.h` (~200 lines)

Mirrors `rust/xdp-receiver/src/xdp.rs`. Uses `libbpf` directly.

```c
typedef struct {
    struct bpf_object *obj;
    struct bpf_link   *link;
    int                ifindex;
    char               attach_mode[16];
} xdp_handle_t;

int  xdp_attach(const config_t *cfg, xdp_handle_t *out);
void xdp_detach(xdp_handle_t *h);
int  xdp_register_xsk(const xdp_handle_t *h, uint32_t queue_id, int xsk_fd);
int  xdp_read_stats(const xdp_handle_t *h,
                    uint64_t *redirected,
                    uint64_t *passed,
                    uint64_t *errors);
```

**`xdp_attach`:**

1. `bpf_object__open_file("bpf/xdp_filter.o", NULL)` — relative to binary cwd; fall back to an install path or `$XDP_FILTER_PATH` env var if not found.
2. `bpf_object__load(obj)`
3. `bpf_object__find_program_by_name(obj, "xdp_filter")`
4. Resolve ifindex via `if_nametoindex`
5. Attach: try `bpf_xdp_attach(ifindex, prog_fd, XDP_FLAGS_DRV_MODE, NULL)` first when mode is `auto`; on failure, retry with `XDP_FLAGS_SKB_MODE`. For explicit `native`/`skb` modes, use only the specified flag.
6. Populate `filter_config` and write to the config map via `bpf_map__update_elem`:
   - `multicast_ip = inet_addr(cfg->network.multicast_group)` (little-endian host order after parsing)
   - `shred_port = cfg->network.shred_port`
   - `heartbeat_port = cfg->network.heartbeat_port`

**`xdp_detach`:** `bpf_xdp_detach(ifindex, flags, NULL)` then `bpf_link__destroy(link)` then `bpf_object__close(obj)`.

**`xdp_register_xsk`:** `bpf_map__update_elem(xsks_map, &queue_id, &xsk_fd, BPF_ANY)`.

**`xdp_read_stats`:** PerCPU array — allocate a buffer of `libbpf_num_possible_cpus() * sizeof(struct xdp_stats)`, call `bpf_map__lookup_elem`, sum across all CPU entries.

### `receiver.c` (~250 lines)

Mirrors `rust/xdp-receiver/src/receiver.rs`. Uses `libxdp`'s `xsk.h` helpers.

```c
typedef struct {
    struct xsk_umem          *umem;
    struct xsk_socket        *xsk;
    struct xsk_ring_cons      rx;
    struct xsk_ring_prod      fill;
    struct xsk_ring_cons      comp;  // unused for RX-only but required by umem
    void                     *umem_area;
    size_t                    umem_size;
    size_t                    frame_size;
    size_t                    frame_count;
} afxdp_receiver_t;

int  afxdp_receiver_init(afxdp_receiver_t *r, const config_t *cfg);
int  afxdp_receiver_fill_ring(afxdp_receiver_t *r);
int  afxdp_receiver_socket_fd(const afxdp_receiver_t *r);
void afxdp_receiver_run(afxdp_receiver_t *r,
                        const config_t *cfg,
                        const xdp_handle_t *xdp,
                        stats_t *stats,
                        pthread_mutex_t *stats_lock,
                        volatile sig_atomic_t *shutdown);
void afxdp_receiver_destroy(afxdp_receiver_t *r);
```

**`afxdp_receiver_init`:**
1. `posix_memalign(&umem_area, getpagesize(), umem_size)` or `mmap(MAP_ANONYMOUS | MAP_PRIVATE)` for the UMEM region
2. `xsk_umem__create(&umem, umem_area, umem_size, &fill, &comp, &umem_cfg)` with fill/comp queue sizes == frame_count
3. `xsk_socket__create(&xsk, interface, rx_queue, umem, &rx, NULL, &xsk_cfg)` (RX-only, NULL TX ring). Set `XSK_LIBXDP_FLAGS__INHIBIT_PROG_LOAD` to prevent libxdp from attaching its own program — we've already attached ours via `xdp.c`.

**`afxdp_receiver_fill_ring`:** Reserve `frame_count` slots in the fill ring, set each addr to `i * frame_size`, submit.

**`afxdp_receiver_run`** main loop:
```
last_xdp_stats_read = now()
while (!shutdown) {
    poll(&pollfd, 1, 100);
    rcvd = xsk_ring_cons__peek(&rx, BATCH_SIZE, &idx_rx);
    if (rcvd > 0) {
        for (i = 0; i < rcvd; i++) {
            desc = xsk_ring_cons__rx_desc(&rx, idx_rx + i);
            pkt = xsk_umem__get_data(umem_area, desc->addr);
            process_packet(pkt, desc->len, cfg, stats, stats_lock);
        }
        xsk_ring_cons__release(&rx, rcvd);

        // Return frames to fill ring
        reserved = xsk_ring_prod__reserve(&fill, rcvd, &idx_fq);
        for (i = 0; i < reserved; i++) {
            *xsk_ring_prod__fill_addr(&fill, idx_fq + i) = /* reused addr */;
        }
        xsk_ring_prod__submit(&fill, reserved);

        if (reserved < rcvd) {
            lock; stats->afxdp_fill_starvation++; unlock;
        }
    }
    if (now() - last_xdp_stats_read >= 1sec) {
        xdp_read_stats(xdp, &r, &p, &e);
        lock; stats_update_xdp_counters(stats, r, p, e); unlock;
        last_xdp_stats_read = now();
    }
}
```

**`process_packet`** and **`find_udp_payload`**:

`find_udp_payload` is a pure function that walks the encapsulation chain and returns the UDP payload offset plus the destination port. Logic matches `rust/xdp-receiver/src/receiver.rs::find_udp_payload` exactly:

```c
// Returns -1 on parse failure.
int find_udp_payload(const uint8_t *pkt, size_t len,
                     size_t *out_payload_offset,
                     uint16_t *out_dst_port);
```

Steps: Ethernet (14) → outer IPv4 (read IHL, check proto==47) → GRE (read flags, compute length 4/8/12 bytes) → inner IPv4 (read IHL, check proto==17) → UDP (read dst port).

`process_packet` calls `find_udp_payload`, then:
- If port == heartbeat_port: `stats_record_heartbeat`
- If port == shred_port: `shred_parse` → `stats_record_shred`
- Otherwise: ignore

### `main.c` (~130 lines)

Startup sequence matches `rust/xdp-receiver/src/main.rs`:

```
1. config_init_defaults, config_load_file, config_parse_cli
2. stats_init
3. pthread_mutex_init
4. xdp_attach → populates stats->xdp_attach_mode
5. afxdp_receiver_init
6. xdp_register_xsk(handle, queue, xsk_fd)
7. afxdp_receiver_fill_ring
8. Install SIGINT/SIGTERM handler → volatile sig_atomic_t shutdown
9. pthread_create receiver thread (runs afxdp_receiver_run)
10. display_run on main thread (blocks)
11. On return: pthread_join receiver
12. xdp_detach (must run before afxdp_receiver_destroy — XDP program references xsks_map)
13. afxdp_receiver_destroy
14. stats_free, pthread_mutex_destroy
15. exit 0
```

**Signal safety:** the SIGINT/SIGTERM handler only writes to `volatile sig_atomic_t shutdown`. Nothing else. All cleanup happens in the main thread after `display_run` returns.

**Linking:** `-lpthread -lncursesw -lbpf -lxdp -lelf -lz`.

## Build System

### `c/kernel-receiver/Makefile`

```makefile
CC       ?= gcc
CFLAGS   ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE
CPPFLAGS += -I../common
LDLIBS    = -lpthread -lncursesw

COMMON_SRCS = $(wildcard ../common/*.c)
LOCAL_SRCS  = main.c receiver.c
SRCS        = $(COMMON_SRCS) $(LOCAL_SRCS)
OBJS        = $(SRCS:.c=.o)

all: edge-multicast-receiver

edge-multicast-receiver: $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $@ $(LDLIBS)

test: shred_test stats_test
	./shred_test && ./stats_test

shred_test: ../common/shred.o ../common/shred_test.o
	$(CC) $(CFLAGS) $^ -o $@

stats_test: ../common/stats.o ../common/stats_test.o
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -f $(OBJS) edge-multicast-receiver shred_test stats_test ../common/*_test.o
```

### `c/xdp-receiver/Makefile`

```makefile
CC          ?= gcc
CLANG       ?= clang
CFLAGS      ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE
CPPFLAGS    += -I../common
LDLIBS       = -lpthread -lncursesw -lbpf -lxdp -lelf -lz
BPF_CFLAGS   = -target bpf -O2 -g -Wall -I/usr/include/$(shell uname -m)-linux-gnu

COMMON_SRCS = $(wildcard ../common/*.c)
LOCAL_SRCS  = main.c receiver.c xdp.c
SRCS        = $(COMMON_SRCS) $(LOCAL_SRCS)
OBJS        = $(SRCS:.c=.o)

all: edge-multicast-xdp-receiver bpf/xdp_filter.o

edge-multicast-xdp-receiver: $(OBJS) bpf/xdp_filter.o
	$(CC) $(CFLAGS) $(OBJS) -o $@ $(LDLIBS)

bpf/xdp_filter.o: bpf/xdp_filter.c
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

test: shred_test stats_test find_udp_payload_test
	./shred_test && ./stats_test && ./find_udp_payload_test

shred_test: ../common/shred.o ../common/shred_test.o
	$(CC) $(CFLAGS) $^ -o $@

stats_test: ../common/stats.o ../common/stats_test.o
	$(CC) $(CFLAGS) $^ -o $@

find_udp_payload_test: receiver.o find_udp_payload_test.o
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -f $(OBJS) bpf/xdp_filter.o edge-multicast-xdp-receiver *_test \
	      ../common/*_test.o find_udp_payload_test.o
```

Note: `bpf/xdp_filter.o` is the eBPF ELF, opened at runtime from `./bpf/xdp_filter.o` relative to the binary's cwd (or via `$XDP_FILTER_PATH` env override).

## Testing

C has no bundled test framework. We hand-roll one — a `TEST(name)` macro plus `assert()` — to keep the dependency story clean.

```c
// common/test.h
#define TEST(name) static void test_##name(void)
#define RUN_TEST(name) do { \
    test_##name(); \
    printf("PASS: %s\n", #name); \
} while (0)
```

**Testable modules (OS-independent):**

- `common/shred_test.c`:
  - `test_parse_garbage_returns_false`
  - `test_parse_empty_returns_false`
  - `test_parse_too_short_returns_false`
  - `test_parse_merkle_data_variant`
  - `test_parse_merkle_coding_variant`
  - `test_parse_legacy_data_variant_0xa5`
  - `test_parse_legacy_coding_variant_0x5a`
  - `test_parse_invalid_variant_returns_false`

- `common/stats_test.c`:
  - `test_new_stats_zero`
  - `test_record_shred_data`
  - `test_record_shred_coding`
  - `test_multiple_shreds_same_slot`
  - `test_ring_buffer_eviction`
  - `test_heartbeat_counting`
  - `test_recent_slots_descending`
  - `test_fec_set_dedup`
  - `test_update_xdp_counters`

- `xdp-receiver/find_udp_payload_test.c`:
  - `test_find_udp_payload_shred_port`
  - `test_find_udp_payload_heartbeat_port`
  - `test_find_udp_payload_truncated`
  - `test_find_udp_payload_gre_with_key`
  - Test fixtures built from the same byte patterns as `rust/xdp-receiver/src/receiver.rs` tests

**Not unit tested (manual integration testing on Linux):**
- UDP socket creation, multicast join (requires network setup)
- eBPF loading, XDP attach (requires root + specific kernel)
- AF_XDP socket, UMEM, rings (requires root + specific driver support)
- ncurses rendering (requires terminal)

Run everything with `make test`. All unit tests must pass before commit.

## Error Handling

- **Syscall return checks:** every `socket`, `bind`, `setsockopt`, `poll`, `recv`, `malloc`, `pthread_*`, `bpf_*`, `xsk_*` return is checked. On failure: log via `fprintf(stderr, "context: %s\n", strerror(errno))`, set the shutdown flag, clean up, exit non-zero.
- **No `goto fail` chains** — each function does its own cleanup on its own error paths. Slightly more verbose but easier to read in isolation.
- **eBPF loading errors:** retrieve and print `bpf_program__log_buf` contents before exiting so the verifier log is visible.
- **XDP cleanup ordering:** detach must happen before `bpf_object__close`. Both run in `main.c` after `pthread_join`, never in signal handlers.
- **TUI cleanup:** `endwin()` is registered via `atexit()` so it runs even on abnormal exit. Without this, a crash during TUI mode leaves the terminal in an unusable state.
- **Signal handler contract:** the SIGINT/SIGTERM handler writes only to `volatile sig_atomic_t shutdown`. It does not call `printf`, does not touch mutexes, does not touch any non-atomic state. This is the only safe contract in C signal handlers.

## Threading Model

Two pthreads, matching the Rust and Go implementations:

1. **Receiver thread** (spawned) — reads from UDP sockets (kernel) or AF_XDP RX ring (XDP), parses shreds, updates `stats_t` under the mutex.
2. **Main thread** — runs `display_run` (ncurses TUI or log printer), reads from `stats_t` under the mutex, handles shutdown.

Shared state:
- `stats_t *` — protected by `pthread_mutex_t *` passed to both threads
- `volatile sig_atomic_t *shutdown` — set by signal handler, read by both threads' loops

No condition variables needed — both loops use their own `poll` timeouts and check the shutdown flag.

## Attribution & Licenses

- **Firedancer (Apache 2.0):** The shred header struct in `common/shred.h` is derived from `src/ballet/shred/fd_shred.h`. A citation appears in the file header comment and in `c/common/NOTICE`.
- **tomlc99 (Public Domain):** Full source vendored into `c/common/toml.[ch]`. License text appears in `c/common/NOTICE`.
- The rest of the C code is original and carries the same license as the top-level repo.

## Limitations

Same as the Rust and Go implementations:

- Receive-only (DoubleZero client handles heartbeat sending)
- No FEC recovery or deshredding
- XDP receiver binds a single RX queue
- "Leader identity" is a signature prefix (full leader-schedule lookup is out of scope)
- Linux only

## Future Work (Out of Scope)

- Multi-queue AF_XDP (bind multiple sockets across RX queues)
- Zero-copy AF_XDP mode (hugepage UMEM + driver support)
- FEC recovery via a vendored Reed-Solomon implementation
- Cross-comparison harness: run Rust / Go / C side-by-side on the same pcap and diff their log output
