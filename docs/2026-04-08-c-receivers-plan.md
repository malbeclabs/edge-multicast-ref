# C Multicast Shred Receivers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build C implementations of both the kernel-socket and XDP receivers that match the existing Rust and Go reference designs in behavior and output.

**Architecture:** Two C binaries (`c/kernel-receiver`, `c/xdp-receiver`) sharing common code in `c/common/` (shred parsing, stats, config, ncurses/log display). The kernel-receiver uses standard UDP sockets with `poll()`; the XDP receiver uses libbpf to load a clang-compiled eBPF program and libxdp for AF_XDP socket/UMEM management. Both use pthreads with a mutex-protected `stats_t`.

**Tech Stack:** C11, gcc/clang, GNU Make, ncurses, pthreads, libbpf, libxdp (XDP only), clang -target bpf (eBPF compilation), vendored tomlc99, hand-rolled unit tests with assert().

**Spec:** `docs/2026-04-08-c-receivers-design.md`

**Platform:** Linux only. The XDP receiver requires kernel 5.4+, capabilities (`cap_bpf`, `cap_net_admin`, `cap_net_raw`, `cap_perfmon`) or root.

---

## File Map

| File | Responsibility |
|---|---|
| `c/README.md` | Overview, build instructions |
| `c/common/NOTICE` | Apache 2.0 attribution (firedancer), tomlc99 license |
| `c/common/toml.h` | Vendored tomlc99 header |
| `c/common/toml.c` | Vendored tomlc99 source |
| `c/common/test.h` | TEST/RUN_TEST macros for hand-rolled unit tests |
| `c/common/shred.h` | Packed common header, parsed_shred_t, shred_parse() |
| `c/common/shred.c` | shred_parse implementation |
| `c/common/shred_test.c` | Unit tests for shred parsing |
| `c/common/stats.h` | stats_t, slot_stats_t, all record/query functions |
| `c/common/stats.c` | stats implementation (ring buffer, rate window, XDP counters) |
| `c/common/stats_test.c` | Unit tests for stats |
| `c/common/config.h` | config_t nested structs, config_init_defaults, config_load_file, config_parse_cli |
| `c/common/config.c` | TOML parsing via tomlc99 + getopt_long CLI parsing |
| `c/common/config_test.c` | Unit tests for config |
| `c/common/display.h` | display_run dispatcher signature |
| `c/common/display_log.c` | Log-mode display loop |
| `c/common/display_tui.c` | ncurses TUI display loop |
| `c/kernel-receiver/Makefile` | Build rules for kernel-receiver binary + tests |
| `c/kernel-receiver/config.example.toml` | Example config for kernel-receiver |
| `c/kernel-receiver/main.c` | CLI, config, pthread setup, shutdown |
| `c/kernel-receiver/receiver.c` | Multicast UDP sockets + poll() loop |
| `c/xdp-receiver/Makefile` | Build rules for xdp-receiver binary + eBPF program + tests |
| `c/xdp-receiver/config.example.toml` | Example config for xdp-receiver |
| `c/xdp-receiver/main.c` | CLI, XDP attach, AF_XDP setup, pthread setup, shutdown |
| `c/xdp-receiver/receiver.c` | AF_XDP RX loop + GRE header stripping (find_udp_payload) |
| `c/xdp-receiver/xdp.h` | xdp_handle_t, xdp_attach/detach/register_xsk/read_stats |
| `c/xdp-receiver/xdp.c` | libbpf load/attach + map config + stats reading |
| `c/xdp-receiver/find_udp_payload_test.c` | Unit tests for GRE header stripping |
| `c/xdp-receiver/bpf/xdp_filter.c` | eBPF program: parse headers, filter, redirect to AF_XDP |

---

### Task 1: Project Scaffold + Vendor tomlc99

**Files:**
- Create: `c/README.md`
- Create: `c/common/NOTICE`
- Create: `c/common/toml.h`
- Create: `c/common/toml.c`
- Create: `c/common/test.h`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p c/common
mkdir -p c/kernel-receiver
mkdir -p c/xdp-receiver/bpf
```

- [ ] **Step 2: Write c/README.md**

```markdown
# C Multicast Shred Receivers

Reference C implementations for consuming Solana shred multicast feeds from DoubleZero edge infrastructure. Two binaries:

- **kernel-receiver** — standard UDP sockets on a GRE tunnel interface
- **xdp-receiver** — libbpf-loaded XDP program + libxdp AF_XDP socket on a physical NIC

Both share parsing, stats, config, and display code in `c/common/`.

## Prerequisites

- Linux (kernel 5.4+ for XDP)
- gcc or clang
- clang (>=11) for eBPF compilation
- GNU Make
- libncurses-dev (or libncursesw-dev)
- libbpf-dev, libxdp-dev, libelf-dev, zlib1g-dev (XDP receiver only)

On Ubuntu/Debian:

```bash
apt install build-essential clang llvm libncurses-dev libbpf-dev libxdp-dev libelf-dev zlib1g-dev
```

## Build

```bash
cd c/kernel-receiver && make
cd c/xdp-receiver && make
```

## Test

```bash
cd c/kernel-receiver && make test
cd c/xdp-receiver && make test
```

## Run

```bash
./c/kernel-receiver/edge-multicast-receiver --interface doublezero1
sudo ./c/xdp-receiver/edge-multicast-xdp-receiver --interface eth0
```

See the design spec at [docs/2026-04-08-c-receivers-design.md](../docs/2026-04-08-c-receivers-design.md).
```

- [ ] **Step 3: Download tomlc99**

Fetch `toml.h` and `toml.c` from https://github.com/cktan/tomlc99 (public domain). Place them verbatim at `c/common/toml.h` and `c/common/toml.c`.

```bash
curl -fsSL https://raw.githubusercontent.com/cktan/tomlc99/master/toml.h -o c/common/toml.h
curl -fsSL https://raw.githubusercontent.com/cktan/tomlc99/master/toml.c -o c/common/toml.c
```

Do not modify these files. If the current upstream main branch has diverged, use the latest release tag instead.

- [ ] **Step 4: Write c/common/NOTICE**

```
edge-multicast-ref C implementation — attribution notices

This directory contains code derived from or vendored from third-party sources.

================================================================================
firedancer (Apache License 2.0)
================================================================================

The shred header struct definition and classification helpers in common/shred.h
are derived from:

    https://github.com/firedancer-io/firedancer/blob/main/src/ballet/shred/fd_shred.h

Copyright 2022-present Jump Trading and contributors.
Licensed under the Apache License, Version 2.0. A copy of the license is
available at: http://www.apache.org/licenses/LICENSE-2.0

================================================================================
tomlc99 (Public Domain)
================================================================================

common/toml.h and common/toml.c are vendored verbatim from:

    https://github.com/cktan/tomlc99

Released to the public domain. No modifications.
```

- [ ] **Step 5: Write c/common/test.h**

```c
#ifndef EDGE_MULTICAST_REF_C_TEST_H
#define EDGE_MULTICAST_REF_C_TEST_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define TEST(name) static void test_##name(void)

#define RUN_TEST(name) do { \
    test_##name(); \
    printf("PASS: %s\n", #name); \
} while (0)

#endif
```

- [ ] **Step 6: Verify tomlc99 compiles**

```bash
cd c/common && gcc -c -O2 -Wall toml.c -o /tmp/toml.o && rm /tmp/toml.o
```

Expected: compiles clean (possibly with unused-parameter warnings — acceptable for vendored code).

- [ ] **Step 7: Commit**

```bash
git add c/
git commit -m "scaffold: c project with vendored tomlc99 and NOTICE"
```

---

### Task 2: Shred Parsing Module (TDD)

**Files:**
- Create: `c/common/shred.h`
- Create: `c/common/shred.c`
- Create: `c/common/shred_test.c`

- [ ] **Step 1: Write shred.h**

```c
#ifndef EDGE_MULTICAST_REF_C_SHRED_H
#define EDGE_MULTICAST_REF_C_SHRED_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Derived from firedancer/src/ballet/shred/fd_shred.h (Apache 2.0).
// See c/common/NOTICE for attribution.

// Packed common header. Identical layout for data and coding shreds.
// All multi-byte fields are little-endian on the wire, matching x86_64/ARM64
// host byte order, so direct reads via the packed struct work without byteswap.
struct __attribute__((packed)) shred_common_hdr {
    uint8_t  signature[64];   // offset 0x00
    uint8_t  variant;         // offset 0x40
    uint64_t slot;            // offset 0x41
    uint32_t idx;             // offset 0x49
    uint16_t version;         // offset 0x4d
    uint32_t fec_set_idx;     // offset 0x4f
};  // sizeof == 83

#define SHRED_COMMON_HDR_SZ 83

typedef struct {
    uint64_t slot;
    uint32_t idx;
    uint32_t fec_set_idx;
    uint16_t version;
    uint8_t  signature[64];
    bool     is_data;
} parsed_shred_t;

// Parse a UDP payload as a shred common header.
// Returns true on success, false if the payload is too short or the variant
// byte does not identify a known data or coding shred.
bool shred_parse(const uint8_t *payload, size_t len, parsed_shred_t *out);

#endif
```

- [ ] **Step 2: Write shred_test.c**

```c
#include "shred.h"
#include "test.h"
#include <string.h>

// Build a minimal valid shred header with the given variant byte.
// Returns a heap buffer of SHRED_COMMON_HDR_SZ bytes; caller frees.
static uint8_t *build_shred(uint8_t variant, uint64_t slot, uint32_t idx,
                            uint16_t version, uint32_t fec_set_idx,
                            uint8_t sig_byte) {
    uint8_t *buf = calloc(1, SHRED_COMMON_HDR_SZ);
    memset(buf, sig_byte, 64);
    buf[64] = variant;
    memcpy(buf + 65, &slot, 8);
    memcpy(buf + 73, &idx, 4);
    memcpy(buf + 77, &version, 2);
    memcpy(buf + 79, &fec_set_idx, 4);
    return buf;
}

TEST(parse_empty_returns_false) {
    parsed_shred_t out;
    assert(shred_parse(NULL, 0, &out) == false);
}

TEST(parse_too_short_returns_false) {
    uint8_t buf[82] = {0};
    parsed_shred_t out;
    assert(shred_parse(buf, sizeof(buf), &out) == false);
}

TEST(parse_merkle_data_variant) {
    uint8_t *buf = build_shred(0x80, 100, 5, 42, 2, 0xAB);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.slot == 100);
    assert(out.idx == 5);
    assert(out.version == 42);
    assert(out.fec_set_idx == 2);
    assert(out.is_data == true);
    assert(out.signature[0] == 0xAB);
    assert(out.signature[63] == 0xAB);
    free(buf);
}

TEST(parse_merkle_coding_variant) {
    uint8_t *buf = build_shred(0x40, 200, 10, 42, 3, 0xCD);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.slot == 200);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_merkle_data_chained_variant) {
    uint8_t *buf = build_shred(0x90, 300, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == true);
    free(buf);
}

TEST(parse_merkle_code_chained_variant) {
    uint8_t *buf = build_shred(0x60, 301, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_legacy_data_variant_0xa5) {
    uint8_t *buf = build_shred(0xa5, 400, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == true);
    free(buf);
}

TEST(parse_legacy_coding_variant_0x5a) {
    uint8_t *buf = build_shred(0x5a, 401, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_garbage_returns_false) {
    uint8_t buf[SHRED_COMMON_HDR_SZ];
    memset(buf, 0xFF, sizeof(buf));  // variant 0xFF is not a valid type
    parsed_shred_t out;
    assert(shred_parse(buf, sizeof(buf), &out) == false);
}

int main(void) {
    RUN_TEST(parse_empty_returns_false);
    RUN_TEST(parse_too_short_returns_false);
    RUN_TEST(parse_merkle_data_variant);
    RUN_TEST(parse_merkle_coding_variant);
    RUN_TEST(parse_merkle_data_chained_variant);
    RUN_TEST(parse_merkle_code_chained_variant);
    RUN_TEST(parse_legacy_data_variant_0xa5);
    RUN_TEST(parse_legacy_coding_variant_0x5a);
    RUN_TEST(parse_garbage_returns_false);
    printf("All shred tests passed.\n");
    return 0;
}
```

- [ ] **Step 3: Write shred.c**

```c
#include "shred.h"
#include <string.h>

// Classify a variant byte. Returns 1 for data, 0 for coding, -1 for unknown.
static int classify_variant(uint8_t variant) {
    if (variant == 0xa5) return 1;           // legacy data
    if (variant == 0x5a) return 0;           // legacy coding
    if ((variant & 0xC0) == 0x80) return 1;  // any merkle data
    if ((variant & 0xC0) == 0x40) return 0;  // any merkle coding
    return -1;
}

bool shred_parse(const uint8_t *payload, size_t len, parsed_shred_t *out) {
    if (payload == NULL || len < SHRED_COMMON_HDR_SZ || out == NULL) {
        return false;
    }
    const struct shred_common_hdr *hdr = (const struct shred_common_hdr *)payload;
    int kind = classify_variant(hdr->variant);
    if (kind < 0) {
        return false;
    }
    memcpy(out->signature, hdr->signature, 64);
    out->slot = hdr->slot;
    out->idx = hdr->idx;
    out->version = hdr->version;
    out->fec_set_idx = hdr->fec_set_idx;
    out->is_data = (kind == 1);
    return true;
}
```

- [ ] **Step 4: Build and run the tests**

```bash
cd c/common && gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. shred.c shred_test.c -o /tmp/shred_test && /tmp/shred_test && rm /tmp/shred_test
```

Expected: 9 `PASS:` lines, then `All shred tests passed.`

- [ ] **Step 5: Commit**

```bash
git add c/common/shred.h c/common/shred.c c/common/shred_test.c
git commit -m "feat(c): shred parsing module with firedancer-derived common header"
```

---

### Task 3: Stats Module (TDD)

**Files:**
- Create: `c/common/stats.h`
- Create: `c/common/stats.c`
- Create: `c/common/stats_test.c`

- [ ] **Step 1: Write stats.h**

```c
#ifndef EDGE_MULTICAST_REF_C_STATS_H
#define EDGE_MULTICAST_REF_C_STATS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

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
    // Global counters
    uint64_t total_data_shreds;
    uint64_t total_coding_shreds;
    uint64_t total_heartbeats;
    uint64_t parse_errors;
    struct timespec last_heartbeat;    // tv_sec == 0 means "never"
    struct timespec start_time;

    // Ring buffer of recent slots, sorted ascending by slot number.
    slot_stats_t *slots;
    size_t slots_len;
    size_t max_slots;

    // Rate window: circular buffer of timestamps.
    struct timespec rate_window[STATS_RATE_WINDOW_MAX];
    size_t rate_window_head;
    size_t rate_window_len;

    // XDP-specific fields. Zero/empty in kernel-receiver.
    char     xdp_attach_mode[16];
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

// Fills `out` with up to `out_cap` pointers to slots in descending slot order.
// Returns the number filled.
size_t stats_recent_slots(const stats_t *s, const slot_stats_t **out, size_t out_cap);

void stats_update_xdp_counters(stats_t *s, uint64_t redirected,
                               uint64_t passed, uint64_t errors);

#endif
```

- [ ] **Step 2: Write stats_test.c**

```c
#include "stats.h"
#include "test.h"
#include <string.h>

static const uint8_t SIG_AB[64] = {
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
};

TEST(new_stats_zero) {
    stats_t s;
    stats_init(&s, 4);
    assert(s.total_data_shreds == 0);
    assert(s.total_coding_shreds == 0);
    assert(s.total_heartbeats == 0);
    assert(s.parse_errors == 0);
    assert(s.slots_len == 0);
    assert(s.xdp_redirected == 0);
    assert(s.xdp_attach_mode[0] == '\0');
    stats_free(&s);
}

TEST(record_shred_data) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    assert(s.total_data_shreds == 1);
    assert(s.total_coding_shreds == 0);
    assert(s.slots_len == 1);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot != NULL);
    assert(slot->slot == 100);
    assert(slot->data_shred_count == 1);
    assert(slot->signature_prefix[0] == 0xAB);
    stats_free(&s);
}

TEST(record_shred_coding) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, false, 5, 0, SIG_AB);
    assert(s.total_coding_shreds == 1);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->coding_shred_count == 1);
    stats_free(&s);
}

TEST(multiple_shreds_same_slot) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 1, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 5, 1, SIG_AB);
    stats_record_shred(&s, 100, false, 0, 0, SIG_AB);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->data_shred_count == 3);
    assert(slot->coding_shred_count == 1);
    assert(slot->highest_data_index == 5);
    assert(slot->fec_set_count == 2);
    stats_free(&s);
}

TEST(ring_buffer_eviction) {
    stats_t s;
    stats_init(&s, 4);
    for (uint64_t slot = 0; slot < 6; slot++) {
        stats_record_shred(&s, slot, true, 0, 0, SIG_AB);
    }
    assert(s.slots_len == 4);
    assert(stats_get_slot(&s, 0) == NULL);
    assert(stats_get_slot(&s, 1) == NULL);
    assert(stats_get_slot(&s, 2) != NULL);
    assert(stats_get_slot(&s, 5) != NULL);
    stats_free(&s);
}

TEST(heartbeat_counting) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_heartbeat(&s);
    stats_record_heartbeat(&s);
    assert(s.total_heartbeats == 2);
    assert(s.last_heartbeat.tv_sec != 0);
    stats_free(&s);
}

TEST(recent_slots_descending) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 200, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 150, true, 0, 0, SIG_AB);
    const slot_stats_t *recent[4];
    size_t n = stats_recent_slots(&s, recent, 4);
    assert(n == 3);
    assert(recent[0]->slot == 200);
    assert(recent[1]->slot == 150);
    assert(recent[2]->slot == 100);
    stats_free(&s);
}

TEST(fec_set_dedup) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 1, 0, SIG_AB);  // same fec_set_index
    stats_record_shred(&s, 100, true, 2, 1, SIG_AB);
    stats_record_shred(&s, 100, true, 3, 1, SIG_AB);  // same fec_set_index
    stats_record_shred(&s, 100, true, 4, 2, SIG_AB);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->fec_set_count == 3);
    stats_free(&s);
}

TEST(update_xdp_counters) {
    stats_t s;
    stats_init(&s, 4);
    stats_update_xdp_counters(&s, 100, 50, 3);
    assert(s.xdp_redirected == 100);
    assert(s.xdp_passed == 50);
    assert(s.xdp_errors == 3);
    stats_free(&s);
}

int main(void) {
    RUN_TEST(new_stats_zero);
    RUN_TEST(record_shred_data);
    RUN_TEST(record_shred_coding);
    RUN_TEST(multiple_shreds_same_slot);
    RUN_TEST(ring_buffer_eviction);
    RUN_TEST(heartbeat_counting);
    RUN_TEST(recent_slots_descending);
    RUN_TEST(fec_set_dedup);
    RUN_TEST(update_xdp_counters);
    printf("All stats tests passed.\n");
    return 0;
}
```

- [ ] **Step 3: Write stats.c**

```c
#include "stats.h"
#include <stdlib.h>
#include <string.h>

static struct timespec now_ts(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts;
}

static double ts_diff_secs(const struct timespec *a, const struct timespec *b) {
    return (a->tv_sec - b->tv_sec) + (a->tv_nsec - b->tv_nsec) / 1e9;
}

void stats_init(stats_t *s, size_t max_slots) {
    memset(s, 0, sizeof(*s));
    s->max_slots = max_slots;
    s->slots = calloc(max_slots, sizeof(slot_stats_t));
    s->start_time = now_ts();
}

void stats_free(stats_t *s) {
    free(s->slots);
    s->slots = NULL;
    s->slots_len = 0;
}

// Binary search: returns index of slot if found, or -1 if not.
// out_insert_pos is set to the position where `slot` would be inserted
// to maintain ascending order.
static int find_slot(const stats_t *s, uint64_t slot, size_t *out_insert_pos) {
    size_t lo = 0, hi = s->slots_len;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (s->slots[mid].slot == slot) {
            *out_insert_pos = mid;
            return (int)mid;
        } else if (s->slots[mid].slot < slot) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    *out_insert_pos = lo;
    return -1;
}

static void insert_fec_set(slot_stats_t *slot, uint32_t fec_set_index) {
    // Linear scan: dedup + keep sorted.
    for (size_t i = 0; i < slot->fec_set_count; i++) {
        if (slot->fec_set_indices[i] == fec_set_index) return;
        if (slot->fec_set_indices[i] > fec_set_index) {
            if (slot->fec_set_count >= STATS_MAX_FEC_SETS_PER_SLOT) return;
            memmove(&slot->fec_set_indices[i + 1], &slot->fec_set_indices[i],
                    (slot->fec_set_count - i) * sizeof(uint32_t));
            slot->fec_set_indices[i] = fec_set_index;
            slot->fec_set_count++;
            return;
        }
    }
    if (slot->fec_set_count >= STATS_MAX_FEC_SETS_PER_SLOT) return;
    slot->fec_set_indices[slot->fec_set_count++] = fec_set_index;
}

void stats_record_shred(stats_t *s, uint64_t slot, bool is_data,
                        uint32_t index, uint32_t fec_set_index,
                        const uint8_t signature[64]) {
    if (is_data) s->total_data_shreds++;
    else s->total_coding_shreds++;

    size_t pos;
    int existing = find_slot(s, slot, &pos);

    slot_stats_t *ss;
    if (existing < 0) {
        // Insert new slot at `pos`, shifting existing ones right.
        if (s->slots_len < s->max_slots) {
            memmove(&s->slots[pos + 1], &s->slots[pos],
                    (s->slots_len - pos) * sizeof(slot_stats_t));
            s->slots_len++;
        } else {
            // Full: evict oldest (index 0) by shifting left, then adjust pos.
            if (pos == 0) {
                // New slot is older than all existing — skip it entirely.
                return;
            }
            memmove(&s->slots[0], &s->slots[1], (pos - 1) * sizeof(slot_stats_t));
            pos--;
        }
        ss = &s->slots[pos];
        memset(ss, 0, sizeof(*ss));
        ss->slot = slot;
        memcpy(ss->signature_prefix, signature, 8);
        ss->first_seen = now_ts();
    } else {
        ss = &s->slots[existing];
    }

    ss->last_seen = now_ts();
    if (is_data) {
        ss->data_shred_count++;
        if (index > ss->highest_data_index) {
            ss->highest_data_index = index;
        }
    } else {
        ss->coding_shred_count++;
    }
    insert_fec_set(ss, fec_set_index);

    // Rate window push
    size_t idx = (s->rate_window_head + s->rate_window_len) % STATS_RATE_WINDOW_MAX;
    s->rate_window[idx] = now_ts();
    if (s->rate_window_len < STATS_RATE_WINDOW_MAX) {
        s->rate_window_len++;
    } else {
        s->rate_window_head = (s->rate_window_head + 1) % STATS_RATE_WINDOW_MAX;
    }
}

void stats_record_heartbeat(stats_t *s) {
    s->total_heartbeats++;
    s->last_heartbeat = now_ts();
}

void stats_record_parse_error(stats_t *s) {
    s->parse_errors++;
}

double stats_shreds_per_second(stats_t *s) {
    struct timespec now = now_ts();
    size_t count = 0;
    for (size_t i = 0; i < s->rate_window_len; i++) {
        size_t idx = (s->rate_window_head + i) % STATS_RATE_WINDOW_MAX;
        if (ts_diff_secs(&now, &s->rate_window[idx]) <= 1.0) {
            count++;
        }
    }
    return (double)count;
}

const slot_stats_t *stats_get_slot(const stats_t *s, uint64_t slot) {
    size_t pos;
    int idx = find_slot(s, slot, &pos);
    return (idx < 0) ? NULL : &s->slots[idx];
}

size_t stats_recent_slots(const stats_t *s, const slot_stats_t **out, size_t out_cap) {
    size_t n = (s->slots_len < out_cap) ? s->slots_len : out_cap;
    for (size_t i = 0; i < n; i++) {
        out[i] = &s->slots[s->slots_len - 1 - i];
    }
    return n;
}

void stats_update_xdp_counters(stats_t *s, uint64_t redirected,
                               uint64_t passed, uint64_t errors) {
    s->xdp_redirected = redirected;
    s->xdp_passed = passed;
    s->xdp_errors = errors;
}
```

- [ ] **Step 4: Build and run the tests**

```bash
cd c/common && gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. stats.c stats_test.c -o /tmp/stats_test && /tmp/stats_test && rm /tmp/stats_test
```

Expected: 9 `PASS:` lines, then `All stats tests passed.`

- [ ] **Step 5: Commit**

```bash
git add c/common/stats.h c/common/stats.c c/common/stats_test.c
git commit -m "feat(c): stats module with ring buffer, rate window, and XDP counters"
```

---

### Task 4: Config Module (TDD)

**Files:**
- Create: `c/common/config.h`
- Create: `c/common/config.c`
- Create: `c/common/config_test.c`

- [ ] **Step 1: Write config.h**

```c
#ifndef EDGE_MULTICAST_REF_C_CONFIG_H
#define EDGE_MULTICAST_REF_C_CONFIG_H

#include <stddef.h>
#include <stdint.h>

typedef enum { DISPLAY_MODE_TUI, DISPLAY_MODE_LOG } display_mode_t;
typedef enum { XDP_MODE_AUTO, XDP_MODE_NATIVE, XDP_MODE_SKB } xdp_mode_t;

typedef struct {
    char     interface[32];
    char     multicast_group[16];
    uint16_t shred_port;
    uint16_t heartbeat_port;
    size_t   recv_buffer_size;
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

// Populate with defaults (kernel-receiver oriented; override interface default
// in xdp-receiver's main.c after calling).
void config_init_defaults(config_t *cfg);

// Load config from a TOML file. Returns 0 on success, -1 on error.
// If the file does not exist, returns -2 (caller decides whether that's an error).
int config_load_file(config_t *cfg, const char *path);

// Parse CLI with getopt_long. Returns 0 on success, -1 on usage error.
// Sets *out_config_path if the user passed --config, else leaves it NULL.
int config_parse_cli(config_t *cfg, int argc, char **argv,
                     const char **out_config_path);

size_t config_frame_count(const config_t *cfg);

#endif
```

- [ ] **Step 2: Write config_test.c**

```c
#include "config.h"
#include "test.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>

TEST(init_defaults) {
    config_t cfg;
    config_init_defaults(&cfg);
    assert(strcmp(cfg.network.interface, "doublezero1") == 0);
    assert(strcmp(cfg.network.multicast_group, "233.84.178.1") == 0);
    assert(cfg.network.shred_port == 7733);
    assert(cfg.network.heartbeat_port == 5765);
    assert(cfg.network.recv_buffer_size == 8388608);
    assert(cfg.xdp.mode == XDP_MODE_AUTO);
    assert(cfg.xdp.umem_size == 4194304);
    assert(cfg.xdp.frame_size == 2048);
    assert(cfg.xdp.rx_queue == 0);
    assert(cfg.display.mode == DISPLAY_MODE_TUI);
    assert(cfg.display.refresh_hz == 4);
    assert(cfg.display.log_interval_secs == 5);
    assert(cfg.stats.max_slots == 32);
}

TEST(frame_count) {
    config_t cfg;
    config_init_defaults(&cfg);
    assert(config_frame_count(&cfg) == 2048);
}

TEST(load_full_toml) {
    const char *path = "/tmp/test_config_full.toml";
    FILE *f = fopen(path, "w");
    fprintf(f,
        "[network]\n"
        "interface = \"ens1f0\"\n"
        "multicast_group = \"239.0.0.1\"\n"
        "shred_port = 8000\n"
        "heartbeat_port = 8001\n"
        "recv_buffer_size = 4194304\n"
        "\n"
        "[xdp]\n"
        "xdp_mode = \"native\"\n"
        "umem_size = 8388608\n"
        "frame_size = 4096\n"
        "rx_queue = 2\n"
        "\n"
        "[display]\n"
        "mode = \"log\"\n"
        "refresh_hz = 2\n"
        "log_interval_secs = 10\n"
        "\n"
        "[stats]\n"
        "max_slots = 64\n");
    fclose(f);

    config_t cfg;
    config_init_defaults(&cfg);
    assert(config_load_file(&cfg, path) == 0);
    assert(strcmp(cfg.network.interface, "ens1f0") == 0);
    assert(cfg.network.shred_port == 8000);
    assert(cfg.xdp.mode == XDP_MODE_NATIVE);
    assert(cfg.xdp.umem_size == 8388608);
    assert(cfg.xdp.rx_queue == 2);
    assert(cfg.display.mode == DISPLAY_MODE_LOG);
    assert(cfg.stats.max_slots == 64);

    unlink(path);
}

TEST(load_partial_toml_uses_defaults) {
    const char *path = "/tmp/test_config_partial.toml";
    FILE *f = fopen(path, "w");
    fprintf(f, "[network]\ninterface = \"mlx5_0\"\n");
    fclose(f);

    config_t cfg;
    config_init_defaults(&cfg);
    assert(config_load_file(&cfg, path) == 0);
    assert(strcmp(cfg.network.interface, "mlx5_0") == 0);
    assert(cfg.network.shred_port == 7733);        // default
    assert(cfg.xdp.mode == XDP_MODE_AUTO);         // default
    assert(cfg.display.mode == DISPLAY_MODE_TUI);  // default

    unlink(path);
}

TEST(load_missing_file_returns_minus2) {
    config_t cfg;
    config_init_defaults(&cfg);
    assert(config_load_file(&cfg, "/tmp/nonexistent_config_xyz.toml") == -2);
}

int main(void) {
    RUN_TEST(init_defaults);
    RUN_TEST(frame_count);
    RUN_TEST(load_full_toml);
    RUN_TEST(load_partial_toml_uses_defaults);
    RUN_TEST(load_missing_file_returns_minus2);
    printf("All config tests passed.\n");
    return 0;
}
```

- [ ] **Step 3: Write config.c**

```c
#include "config.h"
#include "toml.h"
#include <errno.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void config_init_defaults(config_t *cfg) {
    memset(cfg, 0, sizeof(*cfg));
    strcpy(cfg->network.interface, "doublezero1");
    strcpy(cfg->network.multicast_group, "233.84.178.1");
    cfg->network.shred_port = 7733;
    cfg->network.heartbeat_port = 5765;
    cfg->network.recv_buffer_size = 8388608;
    cfg->xdp.mode = XDP_MODE_AUTO;
    cfg->xdp.umem_size = 4194304;
    cfg->xdp.frame_size = 2048;
    cfg->xdp.rx_queue = 0;
    cfg->display.mode = DISPLAY_MODE_TUI;
    cfg->display.refresh_hz = 4;
    cfg->display.log_interval_secs = 5;
    cfg->stats.max_slots = 32;
}

size_t config_frame_count(const config_t *cfg) {
    return cfg->xdp.umem_size / cfg->xdp.frame_size;
}

static void copy_str(char *dst, size_t dst_sz, const char *src) {
    strncpy(dst, src, dst_sz - 1);
    dst[dst_sz - 1] = '\0';
}

int config_load_file(config_t *cfg, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) {
        if (errno == ENOENT) return -2;
        return -1;
    }
    char errbuf[256];
    toml_table_t *root = toml_parse_file(f, errbuf, sizeof(errbuf));
    fclose(f);
    if (!root) {
        fprintf(stderr, "toml parse error: %s\n", errbuf);
        return -1;
    }

    toml_table_t *net = toml_table_in(root, "network");
    if (net) {
        toml_datum_t d;
        d = toml_string_in(net, "interface");
        if (d.ok) { copy_str(cfg->network.interface, sizeof(cfg->network.interface), d.u.s); free(d.u.s); }
        d = toml_string_in(net, "multicast_group");
        if (d.ok) { copy_str(cfg->network.multicast_group, sizeof(cfg->network.multicast_group), d.u.s); free(d.u.s); }
        d = toml_int_in(net, "shred_port");
        if (d.ok) cfg->network.shred_port = (uint16_t)d.u.i;
        d = toml_int_in(net, "heartbeat_port");
        if (d.ok) cfg->network.heartbeat_port = (uint16_t)d.u.i;
        d = toml_int_in(net, "recv_buffer_size");
        if (d.ok) cfg->network.recv_buffer_size = (size_t)d.u.i;
    }

    toml_table_t *xdp = toml_table_in(root, "xdp");
    if (xdp) {
        toml_datum_t d;
        d = toml_string_in(xdp, "xdp_mode");
        if (d.ok) {
            if (strcmp(d.u.s, "auto") == 0) cfg->xdp.mode = XDP_MODE_AUTO;
            else if (strcmp(d.u.s, "native") == 0) cfg->xdp.mode = XDP_MODE_NATIVE;
            else if (strcmp(d.u.s, "skb") == 0) cfg->xdp.mode = XDP_MODE_SKB;
            free(d.u.s);
        }
        d = toml_int_in(xdp, "umem_size");
        if (d.ok) cfg->xdp.umem_size = (size_t)d.u.i;
        d = toml_int_in(xdp, "frame_size");
        if (d.ok) cfg->xdp.frame_size = (size_t)d.u.i;
        d = toml_int_in(xdp, "rx_queue");
        if (d.ok) cfg->xdp.rx_queue = (uint32_t)d.u.i;
    }

    toml_table_t *disp = toml_table_in(root, "display");
    if (disp) {
        toml_datum_t d;
        d = toml_string_in(disp, "mode");
        if (d.ok) {
            if (strcmp(d.u.s, "tui") == 0) cfg->display.mode = DISPLAY_MODE_TUI;
            else if (strcmp(d.u.s, "log") == 0) cfg->display.mode = DISPLAY_MODE_LOG;
            free(d.u.s);
        }
        d = toml_int_in(disp, "refresh_hz");
        if (d.ok) cfg->display.refresh_hz = (uint32_t)d.u.i;
        d = toml_int_in(disp, "log_interval_secs");
        if (d.ok) cfg->display.log_interval_secs = (uint32_t)d.u.i;
    }

    toml_table_t *st = toml_table_in(root, "stats");
    if (st) {
        toml_datum_t d = toml_int_in(st, "max_slots");
        if (d.ok) cfg->stats.max_slots = (size_t)d.u.i;
    }

    toml_free(root);
    return 0;
}

int config_parse_cli(config_t *cfg, int argc, char **argv,
                     const char **out_config_path) {
    static struct option long_opts[] = {
        {"config",          required_argument, 0, 'c'},
        {"interface",       required_argument, 0, 'i'},
        {"multicast-group", required_argument, 0, 'g'},
        {"shred-port",      required_argument, 0, 's'},
        {"heartbeat-port",  required_argument, 0, 'b'},
        {"mode",            required_argument, 0, 'm'},
        {"xdp-mode",        required_argument, 0, 'x'},
        {"rx-queue",        required_argument, 0, 'q'},
        {"help",            no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };

    if (out_config_path) *out_config_path = NULL;

    int opt;
    optind = 1;
    while ((opt = getopt_long(argc, argv, "c:i:g:s:b:m:x:q:h", long_opts, NULL)) != -1) {
        switch (opt) {
            case 'c':
                if (out_config_path) *out_config_path = optarg;
                break;
            case 'i':
                copy_str(cfg->network.interface, sizeof(cfg->network.interface), optarg);
                break;
            case 'g':
                copy_str(cfg->network.multicast_group, sizeof(cfg->network.multicast_group), optarg);
                break;
            case 's':
                cfg->network.shred_port = (uint16_t)atoi(optarg);
                break;
            case 'b':
                cfg->network.heartbeat_port = (uint16_t)atoi(optarg);
                break;
            case 'm':
                if (strcmp(optarg, "tui") == 0) cfg->display.mode = DISPLAY_MODE_TUI;
                else if (strcmp(optarg, "log") == 0) cfg->display.mode = DISPLAY_MODE_LOG;
                else { fprintf(stderr, "unknown display mode: %s\n", optarg); return -1; }
                break;
            case 'x':
                if (strcmp(optarg, "auto") == 0) cfg->xdp.mode = XDP_MODE_AUTO;
                else if (strcmp(optarg, "native") == 0) cfg->xdp.mode = XDP_MODE_NATIVE;
                else if (strcmp(optarg, "skb") == 0) cfg->xdp.mode = XDP_MODE_SKB;
                else { fprintf(stderr, "unknown XDP mode: %s\n", optarg); return -1; }
                break;
            case 'q':
                cfg->xdp.rx_queue = (uint32_t)atoi(optarg);
                break;
            case 'h':
                fprintf(stderr,
                    "Usage: %s [options]\n"
                    "  --config <path>\n"
                    "  --interface <name>\n"
                    "  --multicast-group <ip>\n"
                    "  --shred-port <port>\n"
                    "  --heartbeat-port <port>\n"
                    "  --mode tui|log\n"
                    "  --xdp-mode auto|native|skb\n"
                    "  --rx-queue <n>\n",
                    argv[0]);
                return -1;
            default:
                return -1;
        }
    }
    return 0;
}
```

- [ ] **Step 4: Build and run the tests**

```bash
cd c/common && gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. config.c config_test.c toml.c -o /tmp/config_test && /tmp/config_test && rm /tmp/config_test
```

Expected: 5 `PASS:` lines, then `All config tests passed.`

- [ ] **Step 5: Commit**

```bash
git add c/common/config.h c/common/config.c c/common/config_test.c
git commit -m "feat(c): config module with TOML parsing and CLI overrides"
```

---

### Task 5: Display Log Module

**Files:**
- Create: `c/common/display.h`
- Create: `c/common/display_log.c`

Log mode is straightforward and can be tested by building (manual runtime check comes later).

- [ ] **Step 1: Write display.h**

```c
#ifndef EDGE_MULTICAST_REF_C_DISPLAY_H
#define EDGE_MULTICAST_REF_C_DISPLAY_H

#include "config.h"
#include "stats.h"
#include <pthread.h>
#include <signal.h>

// Run the configured display mode. Blocks until *shutdown is non-zero.
// Returns 0 on success, -1 on error.
int display_run(const config_t *cfg,
                stats_t *stats,
                pthread_mutex_t *stats_lock,
                volatile sig_atomic_t *shutdown);

// Dispatchers — exposed so Makefile can compile each mode independently.
int display_log_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown);
int display_tui_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown);

#endif
```

- [ ] **Step 2: Write display_log.c**

```c
#include "display.h"
#include <inttypes.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>

static double ts_diff_ms(const struct timespec *a, const struct timespec *b) {
    return (a->tv_sec - b->tv_sec) * 1000.0 + (a->tv_nsec - b->tv_nsec) / 1e6;
}

static void format_sig_prefix(char *out, size_t out_sz, const uint8_t sig[8]) {
    // Format as "xxxx..yyyy" using first 2 and last 2 bytes (hex).
    snprintf(out, out_sz, "%02x%02x..%02x%02x",
             sig[0], sig[1], sig[6], sig[7]);
}

int display_log_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown) {
    fprintf(stderr, "Log mode: printing stats every %us. Press Ctrl+C to stop.\n",
            cfg->display.log_interval_secs);

    struct timespec last_print;
    clock_gettime(CLOCK_MONOTONIC, &last_print);

    // Track slots already reported by slot number.
    uint64_t reported[256];
    size_t reported_len = 0;

    while (!*shutdown) {
        usleep(100000);  // 100ms

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (now.tv_sec - last_print.tv_sec)
                       + (now.tv_nsec - last_print.tv_nsec) / 1e9;
        if (elapsed < (double)cfg->display.log_interval_secs) continue;
        last_print = now;

        pthread_mutex_lock(stats_lock);

        // Per-slot lines for newly-seen slots.
        uint64_t current_slots[256];
        size_t current_len = stats->slots_len < 256 ? stats->slots_len : 256;
        for (size_t i = 0; i < current_len; i++) {
            current_slots[i] = stats->slots[i].slot;
        }
        for (size_t i = 0; i < current_len; i++) {
            uint64_t slot = current_slots[i];
            bool seen = false;
            for (size_t j = 0; j < reported_len; j++) {
                if (reported[j] == slot) { seen = true; break; }
            }
            if (seen) continue;
            const slot_stats_t *s = stats_get_slot(stats, slot);
            if (!s) continue;
            char sig_str[16];
            format_sig_prefix(sig_str, sizeof(sig_str), s->signature_prefix);
            double age_ms = ts_diff_ms(&now, &s->first_seen);
            printf("slot=%" PRIu64 " sig=%s data=%" PRIu64 " coding=%" PRIu64
                   " fec_sets=%zu age_ms=%.0f\n",
                   s->slot, sig_str, s->data_shred_count, s->coding_shred_count,
                   s->fec_set_count, age_ms);
        }
        if (current_len <= 256) {
            memcpy(reported, current_slots, current_len * sizeof(uint64_t));
            reported_len = current_len;
        }

        // Summary line.
        double rate = stats_shreds_per_second(stats);
        char hb_ago[32];
        if (stats->last_heartbeat.tv_sec == 0) {
            snprintf(hb_ago, sizeof(hb_ago), "never");
        } else {
            double hb_ms = ts_diff_ms(&now, &stats->last_heartbeat);
            snprintf(hb_ago, sizeof(hb_ago), "%.0fms ago", hb_ms);
        }
        if (stats->xdp_attach_mode[0] == '\0') {
            printf("[stats] shreds/sec=%.0f data=%" PRIu64 " coding=%" PRIu64
                   " errors=%" PRIu64 " heartbeats=%" PRIu64 " (last: %s)\n",
                   rate, stats->total_data_shreds, stats->total_coding_shreds,
                   stats->parse_errors, stats->total_heartbeats, hb_ago);
        } else {
            printf("[stats] shreds/sec=%.0f data=%" PRIu64 " coding=%" PRIu64
                   " errors=%" PRIu64 " heartbeats=%" PRIu64 " (last: %s)"
                   " xdp_mode=%s redirected=%" PRIu64 " passed=%" PRIu64
                   " ring_fill=%zu/%zu\n",
                   rate, stats->total_data_shreds, stats->total_coding_shreds,
                   stats->parse_errors, stats->total_heartbeats, hb_ago,
                   stats->xdp_attach_mode, stats->xdp_redirected, stats->xdp_passed,
                   stats->afxdp_rx_fill_level, config_frame_count(cfg));
        }
        fflush(stdout);

        pthread_mutex_unlock(stats_lock);
    }

    return 0;
}

int display_run(const config_t *cfg, stats_t *stats,
                pthread_mutex_t *stats_lock,
                volatile sig_atomic_t *shutdown) {
    if (cfg->display.mode == DISPLAY_MODE_LOG) {
        return display_log_run(cfg, stats, stats_lock, shutdown);
    } else {
        return display_tui_run(cfg, stats, stats_lock, shutdown);
    }
}
```

- [ ] **Step 3: Verify it compiles (display_tui not yet written, so stub it)**

Temporarily add a stub at the bottom of `display_log.c` — or better, add a stub file `c/common/display_tui.c`:

```c
#include "display.h"

int display_tui_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown) {
    (void)cfg; (void)stats; (void)stats_lock; (void)shutdown;
    return -1;  // stub — replaced in next task
}
```

```bash
cd c/common && gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. -c display_log.c display_tui.c && rm display_log.o display_tui.o
```

Expected: compiles clean.

- [ ] **Step 4: Commit**

```bash
git add c/common/display.h c/common/display_log.c c/common/display_tui.c
git commit -m "feat(c): display dispatcher and log mode"
```

---

### Task 6: Display TUI Module (ncurses)

**Files:**
- Modify: `c/common/display_tui.c`

- [ ] **Step 1: Replace the display_tui.c stub with the full ncurses implementation**

```c
#include "display.h"
#include <inttypes.h>
#include <ncurses.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static WINDOW *g_win = NULL;

static void cleanup_tui(void) {
    if (g_win != NULL) {
        endwin();
        g_win = NULL;
    }
}

static void format_sig_prefix(char *out, size_t out_sz, const uint8_t sig[8]) {
    snprintf(out, out_sz, "%02x%02x..%02x%02x", sig[0], sig[1], sig[6], sig[7]);
}

static void format_duration_short(char *out, size_t out_sz, double secs) {
    if (secs < 60) {
        snprintf(out, out_sz, "%.0fs", secs);
    } else if (secs < 3600) {
        snprintf(out, out_sz, "%dm%ds", (int)(secs / 60), (int)((long)secs % 60));
    } else {
        snprintf(out, out_sz, "%dh%dm", (int)(secs / 3600), (int)(((long)secs % 3600) / 60));
    }
}

int display_tui_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown) {
    g_win = initscr();
    if (!g_win) return -1;
    atexit(cleanup_tui);
    cbreak();
    noecho();
    curs_set(0);
    nodelay(stdscr, TRUE);
    keypad(stdscr, TRUE);

    int tick_ms = 1000 / (int)cfg->display.refresh_hz;
    if (tick_ms < 50) tick_ms = 50;
    timeout(tick_ms);

    while (!*shutdown) {
        int ch = getch();
        if (ch == 'q' || ch == 27 /* ESC */) {
            *shutdown = 1;
            break;
        }

        erase();

        pthread_mutex_lock(stats_lock);

        int rows, cols;
        getmaxyx(stdscr, rows, cols);
        (void)cols;

        int row = 0;

        // Top status bar
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double uptime = (now.tv_sec - stats->start_time.tv_sec)
                      + (now.tv_nsec - stats->start_time.tv_nsec) / 1e9;
        char uptime_s[32];
        format_duration_short(uptime_s, sizeof(uptime_s), uptime);
        char hb_info[64];
        if (stats->last_heartbeat.tv_sec == 0) {
            snprintf(hb_info, sizeof(hb_info), "heartbeats: %" PRIu64 " (none yet)",
                     stats->total_heartbeats);
        } else {
            double hb_ms = (now.tv_sec - stats->last_heartbeat.tv_sec) * 1000.0
                         + (now.tv_nsec - stats->last_heartbeat.tv_nsec) / 1e6;
            snprintf(hb_info, sizeof(hb_info), "heartbeats: %" PRIu64 " (last: %.0fms ago)",
                     stats->total_heartbeats, hb_ms);
        }

        if (stats->xdp_attach_mode[0] == '\0') {
            mvprintw(row++, 0, "=== Edge Multicast Receiver ===");
            mvprintw(row++, 0, " iface: %s | group: %s | uptime: %s | %s",
                     cfg->network.interface, cfg->network.multicast_group,
                     uptime_s, hb_info);
        } else {
            mvprintw(row++, 0, "=== XDP Multicast Receiver ===");
            mvprintw(row++, 0, " iface: %s | group: %s | xdp: %s | uptime: %s | %s",
                     cfg->network.interface, cfg->network.multicast_group,
                     stats->xdp_attach_mode, uptime_s, hb_info);
        }
        row++;

        // XDP stats panel (conditional)
        if (stats->xdp_attach_mode[0] != '\0') {
            mvprintw(row++, 0, "--- XDP Stats ---");
            mvprintw(row++, 0,
                     " redirected: %" PRIu64 " | passed: %" PRIu64 " | errors: %" PRIu64
                     " | ring: %zu/%zu | starvation: %" PRIu64,
                     stats->xdp_redirected, stats->xdp_passed, stats->xdp_errors,
                     stats->afxdp_rx_fill_level, config_frame_count(cfg),
                     stats->afxdp_fill_starvation);
            row++;
        }

        // Slot table
        mvprintw(row++, 0, "--- Recent Slots ---");
        mvprintw(row++, 0, " %-12s %-14s %-8s %-8s %-10s %-8s",
                 "Slot", "Signature", "Data", "Coding", "FEC Sets", "Age");
        const slot_stats_t *recent[32];
        size_t n = stats_recent_slots(stats, recent, 32);
        for (size_t i = 0; i < n && row < rows - 4; i++) {
            char sig_str[16], age_str[16];
            format_sig_prefix(sig_str, sizeof(sig_str), recent[i]->signature_prefix);
            double age_secs = (now.tv_sec - recent[i]->first_seen.tv_sec)
                            + (now.tv_nsec - recent[i]->first_seen.tv_nsec) / 1e9;
            format_duration_short(age_str, sizeof(age_str), age_secs);
            mvprintw(row++, 0, " %-12" PRIu64 " %-14s %-8" PRIu64 " %-8" PRIu64
                     " %-10zu %-8s",
                     recent[i]->slot, sig_str, recent[i]->data_shred_count,
                     recent[i]->coding_shred_count, recent[i]->fec_set_count, age_str);
        }
        row++;

        // Bottom aggregate stats
        double rate = stats_shreds_per_second(stats);
        uint64_t total = stats->total_data_shreds + stats->total_coding_shreds;
        char ratio_str[16];
        if (stats->total_coding_shreds > 0) {
            snprintf(ratio_str, sizeof(ratio_str), "%.1f",
                     (double)stats->total_data_shreds / (double)stats->total_coding_shreds);
        } else {
            strcpy(ratio_str, "n/a");
        }
        mvprintw(rows - 2, 0, "--- Stats ---");
        mvprintw(rows - 1, 0,
                 " shreds/sec: %.0f | total: %" PRIu64 " (data: %" PRIu64
                 ", coding: %" PRIu64 ") | data/coding: %s | errors: %" PRIu64,
                 rate, total, stats->total_data_shreds, stats->total_coding_shreds,
                 ratio_str, stats->parse_errors);

        pthread_mutex_unlock(stats_lock);

        refresh();
    }

    cleanup_tui();
    return 0;
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd c/common && gcc -O2 -Wall -Wextra -Wpedantic -std=c11 -I. -c display_tui.c -o /tmp/display_tui.o && rm /tmp/display_tui.o
```

Expected: compiles clean. If ncurses.h is missing, install `libncurses-dev`.

- [ ] **Step 3: Commit**

```bash
git add c/common/display_tui.c
git commit -m "feat(c): ncurses TUI display with conditional XDP panel"
```

---

### Task 7: Kernel Receiver — Makefile, Main, Receiver

**Files:**
- Create: `c/kernel-receiver/Makefile`
- Create: `c/kernel-receiver/config.example.toml`
- Create: `c/kernel-receiver/main.c`
- Create: `c/kernel-receiver/receiver.c`

- [ ] **Step 1: Write c/kernel-receiver/config.example.toml**

```toml
[network]
interface = "doublezero1"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765
recv_buffer_size = 8388608

[display]
mode = "tui"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32
```

- [ ] **Step 2: Write c/kernel-receiver/Makefile**

```makefile
CC       ?= gcc
CFLAGS   ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE
CPPFLAGS += -I../common
LDLIBS    = -lpthread -lncursesw

COMMON_SRCS := $(filter-out %_test.c, $(wildcard ../common/*.c))
LOCAL_SRCS  := main.c receiver.c
SRCS        := $(COMMON_SRCS) $(LOCAL_SRCS)
OBJS        := $(SRCS:.c=.o)

BIN = edge-multicast-receiver

all: $(BIN)

$(BIN): $(OBJS)
	$(CC) $(CFLAGS) $(OBJS) -o $@ $(LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

test: shred_test stats_test config_test
	./shred_test
	./stats_test
	./config_test

shred_test: ../common/shred.o ../common/shred_test.o
	$(CC) $(CFLAGS) $^ -o $@

stats_test: ../common/stats.o ../common/stats_test.o
	$(CC) $(CFLAGS) $^ -o $@

config_test: ../common/config.o ../common/config_test.o ../common/toml.o
	$(CC) $(CFLAGS) $^ -o $@

../common/shred_test.o: ../common/shred_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

../common/stats_test.o: ../common/stats_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

../common/config_test.o: ../common/config_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(BIN) shred_test stats_test config_test \
	      ../common/*_test.o

.PHONY: all test clean
```

- [ ] **Step 3: Write c/kernel-receiver/receiver.c**

```c
#include "config.h"
#include "shred.h"
#include "stats.h"
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    const config_t     *cfg;
    stats_t            *stats;
    pthread_mutex_t    *stats_lock;
    volatile sig_atomic_t *shutdown;
} kernel_receiver_ctx_t;

// Resolve the first IPv4 address of an interface by shelling out to
// `ip -4 -o addr show <interface>`. Returns INADDR_ANY on failure.
static uint32_t resolve_interface_ip(const char *interface) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "ip -4 -o addr show %s 2>/dev/null", interface);
    FILE *p = popen(cmd, "r");
    if (!p) return 0;
    char line[512];
    uint32_t ip_be = 0;
    while (fgets(line, sizeof(line), p)) {
        char *inet_str = strstr(line, " inet ");
        if (!inet_str) continue;
        inet_str += strlen(" inet ");
        char *slash = strchr(inet_str, '/');
        if (slash) *slash = '\0';
        struct in_addr a;
        if (inet_pton(AF_INET, inet_str, &a) == 1) {
            ip_be = a.s_addr;
            break;
        }
    }
    pclose(p);
    if (ip_be == 0) {
        fprintf(stderr, "warning: could not resolve IP for interface '%s', using 0.0.0.0\n",
                interface);
    }
    return ip_be;
}

static int create_multicast_socket(uint16_t port, uint32_t mcast_ip_be,
                                   uint32_t iface_ip_be, size_t rcvbuf) {
    int fd = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (fd < 0) { perror("socket"); return -1; }

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(fd);
        return -1;
    }

    struct ip_mreq mreq;
    mreq.imr_multiaddr.s_addr = mcast_ip_be;
    mreq.imr_interface.s_addr = iface_ip_be;
    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        perror("IP_ADD_MEMBERSHIP");
        close(fd);
        return -1;
    }

    int rcv = (int)rcvbuf;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcv, sizeof(rcv));

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    return fd;
}

void *kernel_receiver_thread(void *arg) {
    kernel_receiver_ctx_t *ctx = (kernel_receiver_ctx_t *)arg;
    const config_t *cfg = ctx->cfg;

    struct in_addr mcast_addr;
    if (inet_pton(AF_INET, cfg->network.multicast_group, &mcast_addr) != 1) {
        fprintf(stderr, "invalid multicast group: %s\n", cfg->network.multicast_group);
        return NULL;
    }
    uint32_t mcast_ip_be = mcast_addr.s_addr;
    uint32_t iface_ip_be = resolve_interface_ip(cfg->network.interface);

    fprintf(stderr, "Binding to interface %s (%s), multicast group %s\n",
            cfg->network.interface,
            iface_ip_be == 0 ? "0.0.0.0" : "resolved",
            cfg->network.multicast_group);

    int shred_fd = create_multicast_socket(cfg->network.shred_port,
                                           mcast_ip_be, iface_ip_be,
                                           cfg->network.recv_buffer_size);
    int hb_fd = create_multicast_socket(cfg->network.heartbeat_port,
                                        mcast_ip_be, iface_ip_be,
                                        cfg->network.recv_buffer_size);
    if (shred_fd < 0 || hb_fd < 0) {
        if (shred_fd >= 0) close(shred_fd);
        if (hb_fd >= 0) close(hb_fd);
        return NULL;
    }

    fprintf(stderr, "Listening for shreds on port %u, heartbeats on port %u\n",
            cfg->network.shred_port, cfg->network.heartbeat_port);

    struct pollfd pfds[2];
    pfds[0].fd = shred_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd = hb_fd;
    pfds[1].events = POLLIN;

    uint8_t buf[2048];

    while (!*ctx->shutdown) {
        int r = poll(pfds, 2, 100);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("poll");
            break;
        }
        if (r == 0) continue;

        if (pfds[0].revents & POLLIN) {
            for (;;) {
                ssize_t n = recv(shred_fd, buf, sizeof(buf), 0);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    perror("recv shred");
                    break;
                }
                parsed_shred_t parsed;
                if (shred_parse(buf, (size_t)n, &parsed)) {
                    pthread_mutex_lock(ctx->stats_lock);
                    stats_record_shred(ctx->stats, parsed.slot, parsed.is_data,
                                       parsed.idx, parsed.fec_set_idx, parsed.signature);
                    pthread_mutex_unlock(ctx->stats_lock);
                } else {
                    pthread_mutex_lock(ctx->stats_lock);
                    stats_record_parse_error(ctx->stats);
                    pthread_mutex_unlock(ctx->stats_lock);
                }
            }
            pfds[0].revents = 0;
        }

        if (pfds[1].revents & POLLIN) {
            for (;;) {
                ssize_t n = recv(hb_fd, buf, sizeof(buf), 0);
                if (n < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                    perror("recv heartbeat");
                    break;
                }
                pthread_mutex_lock(ctx->stats_lock);
                stats_record_heartbeat(ctx->stats);
                pthread_mutex_unlock(ctx->stats_lock);
            }
            pfds[1].revents = 0;
        }
    }

    close(shred_fd);
    close(hb_fd);
    fprintf(stderr, "Receiver shutting down\n");
    return NULL;
}
```

Add a small header declaring the context and entry point. Place it inline at the top of `main.c` instead to avoid an extra file.

- [ ] **Step 4: Write c/kernel-receiver/main.c**

```c
#include "config.h"
#include "display.h"
#include "stats.h"
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const config_t     *cfg;
    stats_t            *stats;
    pthread_mutex_t    *stats_lock;
    volatile sig_atomic_t *shutdown;
} kernel_receiver_ctx_t;

void *kernel_receiver_thread(void *arg);

static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

int main(int argc, char **argv) {
    config_t cfg;
    config_init_defaults(&cfg);

    const char *cfg_path = NULL;
    if (config_parse_cli(&cfg, argc, argv, &cfg_path) != 0) {
        return 1;
    }
    if (cfg_path == NULL) cfg_path = "config.toml";
    int load_rc = config_load_file(&cfg, cfg_path);
    if (load_rc == -1) {
        fprintf(stderr, "failed to load config from %s\n", cfg_path);
        return 1;
    }
    // Re-apply CLI overrides after the file load (file comes first, CLI wins).
    config_parse_cli(&cfg, argc, argv, &cfg_path);

    fprintf(stderr, "edge-multicast-receiver (c)\n");
    fprintf(stderr, "Interface: %s, Multicast: %s, Shred port: %u, Heartbeat port: %u\n",
            cfg.network.interface, cfg.network.multicast_group,
            cfg.network.shred_port, cfg.network.heartbeat_port);

    stats_t stats;
    stats_init(&stats, cfg.stats.max_slots);

    pthread_mutex_t lock;
    pthread_mutex_init(&lock, NULL);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    kernel_receiver_ctx_t ctx = {
        .cfg = &cfg,
        .stats = &stats,
        .stats_lock = &lock,
        .shutdown = &g_shutdown,
    };

    pthread_t tid;
    if (pthread_create(&tid, NULL, kernel_receiver_thread, &ctx) != 0) {
        perror("pthread_create");
        stats_free(&stats);
        return 1;
    }

    display_run(&cfg, &stats, &lock, &g_shutdown);

    pthread_join(tid, NULL);
    pthread_mutex_destroy(&lock);
    stats_free(&stats);
    fprintf(stderr, "Shutdown complete.\n");
    return 0;
}
```

- [ ] **Step 5: Build and run unit tests**

```bash
cd c/kernel-receiver && make && make test
```

Expected:
- Binary `edge-multicast-receiver` is produced
- `shred_test`, `stats_test`, `config_test` all report PASS lines

- [ ] **Step 6: Commit**

```bash
git add c/kernel-receiver/
git commit -m "feat(c): kernel-receiver binary with UDP poll loop"
```

---

### Task 8: XDP Receiver — eBPF Program

**Files:**
- Create: `c/xdp-receiver/bpf/xdp_filter.c`

The eBPF program is written in plain C and compiled by clang with `-target bpf`.

- [ ] **Step 1: Write bpf/xdp_filter.c**

```c
#include <linux/bpf.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/udp.h>
#include <linux/in.h>
#include <bpf/bpf_helpers.h>
#include <bpf/bpf_endian.h>

#define GRE_HDR_MIN_LEN 4
#define GRE_FLAG_CSUM  0x8000
#define GRE_FLAG_KEY   0x2000
#define GRE_FLAG_SEQ   0x1000

struct filter_config {
    __u32 multicast_ip;   // host byte order
    __u16 shred_port;     // host byte order
    __u16 heartbeat_port; // host byte order
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

static __always_inline void inc_redirected(void) {
    __u32 k = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats_map, &k);
    if (s) s->redirected++;
}

static __always_inline void inc_passed(void) {
    __u32 k = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats_map, &k);
    if (s) s->passed++;
}

static __always_inline void inc_errors(void) {
    __u32 k = 0;
    struct xdp_stats *s = bpf_map_lookup_elem(&stats_map, &k);
    if (s) s->errors++;
}

SEC("xdp")
int xdp_filter(struct xdp_md *ctx) {
    void *data     = (void *)(long)ctx->data;
    void *data_end = (void *)(long)ctx->data_end;

    __u32 k = 0;
    struct filter_config *cfg = bpf_map_lookup_elem(&config_map, &k);
    if (!cfg) { inc_errors(); return XDP_PASS; }

    // Ethernet
    struct ethhdr *eth = data;
    if ((void *)(eth + 1) > data_end) { inc_errors(); return XDP_PASS; }
    if (eth->h_proto != bpf_htons(ETH_P_IP)) { inc_passed(); return XDP_PASS; }

    // Outer IPv4
    struct iphdr *outer = (struct iphdr *)(eth + 1);
    if ((void *)(outer + 1) > data_end) { inc_errors(); return XDP_PASS; }
    if (outer->ihl < 5) { inc_errors(); return XDP_PASS; }
    if (outer->protocol != IPPROTO_GRE) { inc_passed(); return XDP_PASS; }
    __u32 outer_hdr_len = outer->ihl * 4;
    void *after_outer = (void *)outer + outer_hdr_len;
    if (after_outer + GRE_HDR_MIN_LEN > data_end) { inc_errors(); return XDP_PASS; }

    // GRE
    __u16 gre_flags = bpf_ntohs(*(__u16 *)after_outer);
    __u16 gre_proto = bpf_ntohs(*(__u16 *)(after_outer + 2));
    if (gre_proto != ETH_P_IP) { inc_passed(); return XDP_PASS; }
    __u32 gre_len = GRE_HDR_MIN_LEN;
    if (gre_flags & GRE_FLAG_CSUM) gre_len += 4;
    if (gre_flags & GRE_FLAG_KEY)  gre_len += 4;
    if (gre_flags & GRE_FLAG_SEQ)  gre_len += 4;
    void *after_gre = after_outer + gre_len;
    if (after_gre + sizeof(struct iphdr) > data_end) { inc_errors(); return XDP_PASS; }

    // Inner IPv4
    struct iphdr *inner = (struct iphdr *)after_gre;
    if (inner->ihl < 5) { inc_errors(); return XDP_PASS; }
    if (inner->protocol != IPPROTO_UDP) { inc_passed(); return XDP_PASS; }
    if (bpf_ntohl(inner->daddr) != cfg->multicast_ip) { inc_passed(); return XDP_PASS; }
    __u32 inner_hdr_len = inner->ihl * 4;
    void *after_inner = (void *)inner + inner_hdr_len;
    if (after_inner + sizeof(struct udphdr) > data_end) { inc_errors(); return XDP_PASS; }

    // UDP
    struct udphdr *udp = (struct udphdr *)after_inner;
    __u16 dport = bpf_ntohs(udp->dest);
    if (dport != cfg->shred_port && dport != cfg->heartbeat_port) {
        inc_passed();
        return XDP_PASS;
    }

    inc_redirected();
    __u32 queue = ctx->rx_queue_index;
    return bpf_redirect_map(&xsks_map, queue, 0);
}

char LICENSE[] SEC("license") = "Dual MIT/GPL";
```

- [ ] **Step 2: Verify eBPF compiles**

```bash
cd c/xdp-receiver && clang -target bpf -O2 -g -Wall -c bpf/xdp_filter.c -o bpf/xdp_filter.o
```

Expected: on Linux with libbpf-dev installed, produces `bpf/xdp_filter.o`. If `bpf_helpers.h` is missing, add `-I/usr/include/$(uname -m)-linux-gnu`.

On non-Linux (macOS development), skip this step — it can only compile on Linux with proper libbpf headers.

- [ ] **Step 3: Commit**

```bash
git add c/xdp-receiver/bpf/xdp_filter.c
git commit -m "feat(c): XDP eBPF filter program with GRE parsing and AF_XDP redirect"
```

---

### Task 9: XDP Receiver — Loader Module

**Files:**
- Create: `c/xdp-receiver/xdp.h`
- Create: `c/xdp-receiver/xdp.c`

- [ ] **Step 1: Write xdp.h**

```c
#ifndef EDGE_MULTICAST_REF_C_XDP_H
#define EDGE_MULTICAST_REF_C_XDP_H

#include "config.h"
#include <bpf/libbpf.h>
#include <stdint.h>

typedef struct {
    struct bpf_object *obj;
    int ifindex;
    unsigned int attach_flags;
    char attach_mode[16];
} xdp_handle_t;

// Load the eBPF object from bpf/xdp_filter.o (or $XDP_FILTER_PATH if set),
// attach it to the physical interface, and write filter config to the map.
// Returns 0 on success, -1 on error.
int xdp_attach(const config_t *cfg, xdp_handle_t *out);

// Detach XDP and close the bpf object.
void xdp_detach(xdp_handle_t *h);

// Register an AF_XDP socket fd in the XSKMAP at queue_id.
int xdp_register_xsk(const xdp_handle_t *h, uint32_t queue_id, int xsk_fd);

// Read per-CPU stats and sum across CPUs.
int xdp_read_stats(const xdp_handle_t *h,
                   uint64_t *redirected, uint64_t *passed, uint64_t *errors);

#endif
```

- [ ] **Step 2: Write xdp.c**

```c
#include "xdp.h"
#include <arpa/inet.h>
#include <bpf/bpf.h>
#include <bpf/libbpf.h>
#include <errno.h>
#include <net/if.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct filter_config_user {
    uint32_t multicast_ip;
    uint16_t shred_port;
    uint16_t heartbeat_port;
};

struct xdp_stats_user {
    uint64_t redirected;
    uint64_t passed;
    uint64_t errors;
};

int xdp_attach(const config_t *cfg, xdp_handle_t *out) {
    memset(out, 0, sizeof(*out));

    const char *path = getenv("XDP_FILTER_PATH");
    if (!path) path = "bpf/xdp_filter.o";

    struct bpf_object *obj = bpf_object__open_file(path, NULL);
    if (libbpf_get_error(obj)) {
        fprintf(stderr, "failed to open eBPF object at %s: %s\n",
                path, strerror(errno));
        return -1;
    }

    if (bpf_object__load(obj)) {
        fprintf(stderr, "failed to load eBPF object: %s\n", strerror(errno));
        bpf_object__close(obj);
        return -1;
    }

    struct bpf_program *prog = bpf_object__find_program_by_name(obj, "xdp_filter");
    if (!prog) {
        fprintf(stderr, "eBPF program 'xdp_filter' not found in object\n");
        bpf_object__close(obj);
        return -1;
    }
    int prog_fd = bpf_program__fd(prog);

    int ifindex = if_nametoindex(cfg->network.interface);
    if (ifindex == 0) {
        fprintf(stderr, "interface '%s' not found\n", cfg->network.interface);
        bpf_object__close(obj);
        return -1;
    }

    unsigned int flags;
    const char *mode_name;
    switch (cfg->xdp.mode) {
        case XDP_MODE_NATIVE: flags = XDP_FLAGS_DRV_MODE; mode_name = "native"; break;
        case XDP_MODE_SKB:    flags = XDP_FLAGS_SKB_MODE; mode_name = "skb"; break;
        case XDP_MODE_AUTO:
        default:              flags = XDP_FLAGS_DRV_MODE; mode_name = "native"; break;
    }

    int rc = bpf_xdp_attach(ifindex, prog_fd, flags, NULL);
    if (rc < 0 && cfg->xdp.mode == XDP_MODE_AUTO) {
        fprintf(stderr, "native XDP attach failed (%s), falling back to SKB mode\n",
                strerror(-rc));
        flags = XDP_FLAGS_SKB_MODE;
        mode_name = "skb";
        rc = bpf_xdp_attach(ifindex, prog_fd, flags, NULL);
    }
    if (rc < 0) {
        fprintf(stderr, "bpf_xdp_attach failed: %s\n", strerror(-rc));
        bpf_object__close(obj);
        return -1;
    }

    // Write filter config
    struct bpf_map *config_map = bpf_object__find_map_by_name(obj, "config_map");
    if (!config_map) {
        fprintf(stderr, "config_map not found\n");
        bpf_xdp_detach(ifindex, flags, NULL);
        bpf_object__close(obj);
        return -1;
    }
    struct in_addr mcast_addr;
    if (inet_pton(AF_INET, cfg->network.multicast_group, &mcast_addr) != 1) {
        fprintf(stderr, "invalid multicast group: %s\n", cfg->network.multicast_group);
        bpf_xdp_detach(ifindex, flags, NULL);
        bpf_object__close(obj);
        return -1;
    }
    struct filter_config_user fc = {
        .multicast_ip   = ntohl(mcast_addr.s_addr),
        .shred_port     = cfg->network.shred_port,
        .heartbeat_port = cfg->network.heartbeat_port,
    };
    uint32_t k = 0;
    if (bpf_map__update_elem(config_map, &k, sizeof(k), &fc, sizeof(fc), BPF_ANY) < 0) {
        fprintf(stderr, "failed to update config_map: %s\n", strerror(errno));
        bpf_xdp_detach(ifindex, flags, NULL);
        bpf_object__close(obj);
        return -1;
    }

    out->obj = obj;
    out->ifindex = ifindex;
    out->attach_flags = flags;
    strncpy(out->attach_mode, mode_name, sizeof(out->attach_mode) - 1);

    fprintf(stderr, "XDP program attached to %s in %s mode\n",
            cfg->network.interface, mode_name);
    return 0;
}

void xdp_detach(xdp_handle_t *h) {
    if (!h || !h->obj) return;
    fprintf(stderr, "Detaching XDP program from ifindex %d...\n", h->ifindex);
    bpf_xdp_detach(h->ifindex, h->attach_flags, NULL);
    bpf_object__close(h->obj);
    h->obj = NULL;
}

int xdp_register_xsk(const xdp_handle_t *h, uint32_t queue_id, int xsk_fd) {
    struct bpf_map *xsks_map = bpf_object__find_map_by_name(h->obj, "xsks_map");
    if (!xsks_map) {
        fprintf(stderr, "xsks_map not found\n");
        return -1;
    }
    if (bpf_map__update_elem(xsks_map, &queue_id, sizeof(queue_id),
                             &xsk_fd, sizeof(xsk_fd), BPF_ANY) < 0) {
        fprintf(stderr, "failed to update xsks_map: %s\n", strerror(errno));
        return -1;
    }
    fprintf(stderr, "Registered AF_XDP socket fd=%d at xsks_map[%u]\n", xsk_fd, queue_id);
    return 0;
}

int xdp_read_stats(const xdp_handle_t *h,
                   uint64_t *redirected, uint64_t *passed, uint64_t *errors) {
    struct bpf_map *stats_map = bpf_object__find_map_by_name(h->obj, "stats_map");
    if (!stats_map) return -1;

    int ncpu = libbpf_num_possible_cpus();
    if (ncpu <= 0) return -1;
    struct xdp_stats_user *per_cpu = calloc(ncpu, sizeof(struct xdp_stats_user));
    if (!per_cpu) return -1;

    uint32_t k = 0;
    if (bpf_map__lookup_elem(stats_map, &k, sizeof(k),
                             per_cpu, ncpu * sizeof(struct xdp_stats_user), 0) < 0) {
        free(per_cpu);
        return -1;
    }

    uint64_t r = 0, p = 0, e = 0;
    for (int i = 0; i < ncpu; i++) {
        r += per_cpu[i].redirected;
        p += per_cpu[i].passed;
        e += per_cpu[i].errors;
    }
    free(per_cpu);

    *redirected = r;
    *passed = p;
    *errors = e;
    return 0;
}
```

- [ ] **Step 3: Commit**

```bash
git add c/xdp-receiver/xdp.h c/xdp-receiver/xdp.c
git commit -m "feat(c): XDP loader module with libbpf attach and map config"
```

---

### Task 10: XDP Receiver — AF_XDP Receiver with find_udp_payload (TDD)

**Files:**
- Create: `c/xdp-receiver/receiver.h`
- Create: `c/xdp-receiver/receiver.c`
- Create: `c/xdp-receiver/find_udp_payload_test.c`

- [ ] **Step 1: Write receiver.h**

```c
#ifndef EDGE_MULTICAST_REF_C_XDP_RECEIVER_H
#define EDGE_MULTICAST_REF_C_XDP_RECEIVER_H

#include "config.h"
#include "stats.h"
#include "xdp.h"
#include <pthread.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <xdp/xsk.h>

typedef struct {
    struct xsk_umem     *umem;
    struct xsk_socket   *xsk;
    struct xsk_ring_cons rx;
    struct xsk_ring_prod fill;
    struct xsk_ring_cons comp;
    void  *umem_area;
    size_t umem_size;
    size_t frame_size;
    size_t frame_count;
} afxdp_receiver_t;

int  afxdp_receiver_init(afxdp_receiver_t *r, const config_t *cfg);
int  afxdp_receiver_fill_ring(afxdp_receiver_t *r);
int  afxdp_receiver_socket_fd(const afxdp_receiver_t *r);
void afxdp_receiver_destroy(afxdp_receiver_t *r);

typedef struct {
    afxdp_receiver_t       *r;
    const config_t         *cfg;
    const xdp_handle_t     *xdp;
    stats_t                *stats;
    pthread_mutex_t        *stats_lock;
    volatile sig_atomic_t  *shutdown;
} afxdp_thread_ctx_t;

void *afxdp_receiver_thread(void *arg);

// Exposed for unit testing.
// Returns 0 on success, -1 on parse failure.
int find_udp_payload(const uint8_t *pkt, size_t len,
                     size_t *out_payload_offset, uint16_t *out_dst_port);

#endif
```

- [ ] **Step 2: Write find_udp_payload_test.c**

```c
#include "receiver.h"
#include "../common/test.h"
#include <string.h>

static void append(uint8_t **pkt, size_t *len, const uint8_t *data, size_t n) {
    uint8_t *np = realloc(*pkt, *len + n);
    memcpy(np + *len, data, n);
    *pkt = np;
    *len += n;
}

// Build an Eth+GRE+IP+UDP packet with a 4-byte GRE header.
static uint8_t *build_basic(size_t *out_len, uint16_t dst_port) {
    uint8_t *pkt = NULL;
    size_t len = 0;
    uint8_t eth[14] = {0,0,0,0,0,0, 0,0,0,0,0,0, 0x08,0x00};
    append(&pkt, &len, eth, 14);
    uint8_t outer_ip[20] = {0x45,0,0,0, 0,0,0,0, 0,47,0,0, 10,0,0,1, 10,0,0,2};
    append(&pkt, &len, outer_ip, 20);
    uint8_t gre[4] = {0x00,0x00, 0x08,0x00};
    append(&pkt, &len, gre, 4);
    uint8_t inner_ip[20] = {0x45,0,0,0, 0,0,0,0, 0,17,0,0, 148,51,0,1, 233,84,178,1};
    append(&pkt, &len, inner_ip, 20);
    uint8_t udp[8] = {0x00,0x00, (uint8_t)(dst_port >> 8), (uint8_t)(dst_port & 0xff),
                      0x00,0x00, 0x00,0x00};
    append(&pkt, &len, udp, 8);
    uint8_t payload[100];
    memset(payload, 0xAA, 100);
    append(&pkt, &len, payload, 100);
    *out_len = len;
    return pkt;
}

// Build a packet with the GRE Key bit set (8-byte GRE header).
static uint8_t *build_with_key(size_t *out_len, uint16_t dst_port) {
    uint8_t *pkt = NULL;
    size_t len = 0;
    uint8_t eth[14] = {0,0,0,0,0,0, 0,0,0,0,0,0, 0x08,0x00};
    append(&pkt, &len, eth, 14);
    uint8_t outer_ip[20] = {0x45,0,0,0, 0,0,0,0, 0,47,0,0, 10,0,0,1, 10,0,0,2};
    append(&pkt, &len, outer_ip, 20);
    uint8_t gre[8] = {0x20,0x00, 0x08,0x00, 0,0,0,1};  // Key flag set
    append(&pkt, &len, gre, 8);
    uint8_t inner_ip[20] = {0x45,0,0,0, 0,0,0,0, 0,17,0,0, 148,51,0,1, 233,84,178,1};
    append(&pkt, &len, inner_ip, 20);
    uint8_t udp[8] = {0x00,0x00, (uint8_t)(dst_port >> 8), (uint8_t)(dst_port & 0xff),
                      0x00,0x00, 0x00,0x00};
    append(&pkt, &len, udp, 8);
    uint8_t payload[50];
    memset(payload, 0xBB, 50);
    append(&pkt, &len, payload, 50);
    *out_len = len;
    return pkt;
}

TEST(find_udp_payload_shred_port) {
    size_t len;
    uint8_t *pkt = build_basic(&len, 7733);
    size_t offset; uint16_t port;
    int rc = find_udp_payload(pkt, len, &offset, &port);
    assert(rc == 0);
    assert(port == 7733);
    // Eth(14) + outerIP(20) + GRE(4) + innerIP(20) + UDP(8) = 66
    assert(offset == 66);
    free(pkt);
}

TEST(find_udp_payload_heartbeat_port) {
    size_t len;
    uint8_t *pkt = build_basic(&len, 5765);
    size_t offset; uint16_t port;
    assert(find_udp_payload(pkt, len, &offset, &port) == 0);
    assert(port == 5765);
    free(pkt);
}

TEST(find_udp_payload_truncated) {
    uint8_t pkt[30] = {0};
    size_t offset; uint16_t port;
    assert(find_udp_payload(pkt, sizeof(pkt), &offset, &port) == -1);
}

TEST(find_udp_payload_gre_with_key) {
    size_t len;
    uint8_t *pkt = build_with_key(&len, 7733);
    size_t offset; uint16_t port;
    assert(find_udp_payload(pkt, len, &offset, &port) == 0);
    assert(port == 7733);
    // Eth(14) + outerIP(20) + GRE(8) + innerIP(20) + UDP(8) = 70
    assert(offset == 70);
    free(pkt);
}

int main(void) {
    RUN_TEST(find_udp_payload_shred_port);
    RUN_TEST(find_udp_payload_heartbeat_port);
    RUN_TEST(find_udp_payload_truncated);
    RUN_TEST(find_udp_payload_gre_with_key);
    printf("All find_udp_payload tests passed.\n");
    return 0;
}
```

- [ ] **Step 3: Write receiver.c**

```c
#include "receiver.h"
#include "shred.h"
#include <errno.h>
#include <net/if.h>
#include <poll.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <time.h>
#include <unistd.h>
#include <xdp/xsk.h>

#define ETH_HDR_LEN     14
#define IPV4_HDR_MIN    20
#define GRE_HDR_MIN     4
#define UDP_HDR_LEN     8
#define GRE_FLAG_CSUM   0x8000
#define GRE_FLAG_KEY    0x2000
#define GRE_FLAG_SEQ    0x1000
#define BATCH_SIZE      64

int find_udp_payload(const uint8_t *pkt, size_t len,
                     size_t *out_payload_offset, uint16_t *out_dst_port) {
    if (len < ETH_HDR_LEN + IPV4_HDR_MIN) return -1;
    size_t off = ETH_HDR_LEN;

    size_t outer_ihl = (pkt[off] & 0x0F) * 4;
    if (outer_ihl < IPV4_HDR_MIN || off + outer_ihl > len) return -1;
    if (pkt[off + 9] != 47) return -1;  // GRE
    off += outer_ihl;

    if (off + GRE_HDR_MIN > len) return -1;
    uint16_t gre_flags = ((uint16_t)pkt[off] << 8) | pkt[off + 1];
    size_t gre_len = GRE_HDR_MIN;
    if (gre_flags & GRE_FLAG_CSUM) gre_len += 4;
    if (gre_flags & GRE_FLAG_KEY)  gre_len += 4;
    if (gre_flags & GRE_FLAG_SEQ)  gre_len += 4;
    off += gre_len;

    if (off + IPV4_HDR_MIN > len) return -1;
    size_t inner_ihl = (pkt[off] & 0x0F) * 4;
    if (inner_ihl < IPV4_HDR_MIN || off + inner_ihl > len) return -1;
    if (pkt[off + 9] != 17) return -1;  // UDP
    off += inner_ihl;

    if (off + UDP_HDR_LEN > len) return -1;
    uint16_t dport = ((uint16_t)pkt[off + 2] << 8) | pkt[off + 3];

    *out_payload_offset = off + UDP_HDR_LEN;
    *out_dst_port = dport;
    return 0;
}

int afxdp_receiver_init(afxdp_receiver_t *r, const config_t *cfg) {
    memset(r, 0, sizeof(*r));
    r->umem_size = cfg->xdp.umem_size;
    r->frame_size = cfg->xdp.frame_size;
    r->frame_count = cfg->xdp.umem_size / cfg->xdp.frame_size;

    r->umem_area = mmap(NULL, r->umem_size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (r->umem_area == MAP_FAILED) {
        perror("mmap umem");
        return -1;
    }

    struct xsk_umem_config ucfg = {
        .fill_size      = r->frame_count,
        .comp_size      = r->frame_count,
        .frame_size     = (uint32_t)r->frame_size,
        .frame_headroom = 0,
        .flags          = 0,
    };
    if (xsk_umem__create(&r->umem, r->umem_area, r->umem_size,
                         &r->fill, &r->comp, &ucfg) != 0) {
        perror("xsk_umem__create");
        munmap(r->umem_area, r->umem_size);
        return -1;
    }

    struct xsk_socket_config scfg = {
        .rx_size     = (uint32_t)r->frame_count,
        .tx_size     = 0,
        .libbpf_flags = XSK_LIBBPF_FLAGS__INHIBIT_PROG_LOAD,
        .xdp_flags   = 0,
        .bind_flags  = XDP_COPY,
    };
    if (xsk_socket__create(&r->xsk, cfg->network.interface, cfg->xdp.rx_queue,
                           r->umem, &r->rx, NULL, &scfg) != 0) {
        perror("xsk_socket__create");
        xsk_umem__delete(r->umem);
        munmap(r->umem_area, r->umem_size);
        return -1;
    }

    return 0;
}

int afxdp_receiver_fill_ring(afxdp_receiver_t *r) {
    uint32_t idx = 0;
    size_t reserved = xsk_ring_prod__reserve(&r->fill, r->frame_count, &idx);
    if (reserved != r->frame_count) {
        fprintf(stderr, "failed to reserve %zu slots in fill ring (got %zu)\n",
                r->frame_count, reserved);
        return -1;
    }
    for (size_t i = 0; i < r->frame_count; i++) {
        *xsk_ring_prod__fill_addr(&r->fill, idx + i) = i * r->frame_size;
    }
    xsk_ring_prod__submit(&r->fill, r->frame_count);
    return 0;
}

int afxdp_receiver_socket_fd(const afxdp_receiver_t *r) {
    return xsk_socket__fd(r->xsk);
}

void afxdp_receiver_destroy(afxdp_receiver_t *r) {
    if (r->xsk) xsk_socket__delete(r->xsk);
    if (r->umem) xsk_umem__delete(r->umem);
    if (r->umem_area && r->umem_area != MAP_FAILED) {
        munmap(r->umem_area, r->umem_size);
    }
}

static void process_packet(const uint8_t *pkt, size_t len,
                           const config_t *cfg, stats_t *stats,
                           pthread_mutex_t *stats_lock) {
    size_t payload_off;
    uint16_t dport;
    if (find_udp_payload(pkt, len, &payload_off, &dport) != 0) {
        pthread_mutex_lock(stats_lock);
        stats_record_parse_error(stats);
        pthread_mutex_unlock(stats_lock);
        return;
    }
    if (dport == cfg->network.heartbeat_port) {
        pthread_mutex_lock(stats_lock);
        stats_record_heartbeat(stats);
        pthread_mutex_unlock(stats_lock);
        return;
    }
    if (dport != cfg->network.shred_port) return;

    parsed_shred_t parsed;
    if (shred_parse(pkt + payload_off, len - payload_off, &parsed)) {
        pthread_mutex_lock(stats_lock);
        stats_record_shred(stats, parsed.slot, parsed.is_data, parsed.idx,
                           parsed.fec_set_idx, parsed.signature);
        pthread_mutex_unlock(stats_lock);
    } else {
        pthread_mutex_lock(stats_lock);
        stats_record_parse_error(stats);
        pthread_mutex_unlock(stats_lock);
    }
}

void *afxdp_receiver_thread(void *arg) {
    afxdp_thread_ctx_t *ctx = (afxdp_thread_ctx_t *)arg;
    afxdp_receiver_t *r = ctx->r;

    struct pollfd pfd;
    pfd.fd = afxdp_receiver_socket_fd(r);
    pfd.events = POLLIN;

    struct timespec last_stats_read;
    clock_gettime(CLOCK_MONOTONIC, &last_stats_read);

    fprintf(stderr, "AF_XDP receiver running on %s queue %u\n",
            ctx->cfg->network.interface, ctx->cfg->xdp.rx_queue);

    while (!*ctx->shutdown) {
        int pr = poll(&pfd, 1, 100);
        if (pr < 0) {
            if (errno == EINTR) continue;
            perror("poll xsk");
            break;
        }

        uint32_t idx_rx = 0;
        size_t rcvd = xsk_ring_cons__peek(&r->rx, BATCH_SIZE, &idx_rx);
        if (rcvd > 0) {
            for (size_t i = 0; i < rcvd; i++) {
                const struct xdp_desc *desc = xsk_ring_cons__rx_desc(&r->rx, idx_rx + i);
                const uint8_t *pkt = xsk_umem__get_data(r->umem_area, desc->addr);
                process_packet(pkt, desc->len, ctx->cfg, ctx->stats, ctx->stats_lock);
            }
            xsk_ring_cons__release(&r->rx, rcvd);

            uint32_t idx_fq = 0;
            size_t reserved = xsk_ring_prod__reserve(&r->fill, rcvd, &idx_fq);
            for (size_t i = 0; i < reserved; i++) {
                const struct xdp_desc *desc = xsk_ring_cons__rx_desc(&r->rx, idx_rx + i);
                *xsk_ring_prod__fill_addr(&r->fill, idx_fq + i) = desc->addr;
            }
            xsk_ring_prod__submit(&r->fill, reserved);
            if (reserved < rcvd) {
                pthread_mutex_lock(ctx->stats_lock);
                ctx->stats->afxdp_fill_starvation++;
                pthread_mutex_unlock(ctx->stats_lock);
            }
        }

        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        double elapsed = (now.tv_sec - last_stats_read.tv_sec)
                       + (now.tv_nsec - last_stats_read.tv_nsec) / 1e9;
        if (elapsed >= 1.0) {
            uint64_t redir, passed, errs;
            if (xdp_read_stats(ctx->xdp, &redir, &passed, &errs) == 0) {
                pthread_mutex_lock(ctx->stats_lock);
                stats_update_xdp_counters(ctx->stats, redir, passed, errs);
                pthread_mutex_unlock(ctx->stats_lock);
            }
            last_stats_read = now;
        }
    }

    fprintf(stderr, "AF_XDP receiver shutting down\n");
    return NULL;
}
```

**Note on find_udp_payload_test compilation:** The test only exercises `find_udp_payload`, which doesn't depend on xsk/libbpf headers. But `receiver.c` includes `<xdp/xsk.h>` throughout. To keep the test buildable without linking libxdp, factor `find_udp_payload` into a function that's compilable independently — which it already is. The test Makefile target will link only against the object file section containing `find_udp_payload`. Simplest approach: build `find_udp_payload_test` by compiling a small shim that defines `find_udp_payload` separately, or build the full `receiver.o` and link against libxdp.

Use the latter (link against libxdp) since the test Makefile already has that dependency available.

- [ ] **Step 4: Commit**

```bash
git add c/xdp-receiver/receiver.h c/xdp-receiver/receiver.c c/xdp-receiver/find_udp_payload_test.c
git commit -m "feat(c): AF_XDP receiver with GRE header stripping and TDD tests"
```

---

### Task 11: XDP Receiver — Main + Makefile + Config

**Files:**
- Create: `c/xdp-receiver/main.c`
- Create: `c/xdp-receiver/config.example.toml`
- Create: `c/xdp-receiver/Makefile`

- [ ] **Step 1: Write c/xdp-receiver/config.example.toml**

```toml
[network]
interface = "eth0"
multicast_group = "233.84.178.1"
shred_port = 7733
heartbeat_port = 5765

[xdp]
xdp_mode = "auto"          # "auto", "native", "skb"
umem_size = 4194304         # 4MB
frame_size = 2048
rx_queue = 0

[display]
mode = "tui"                # "tui" or "log"
refresh_hz = 4
log_interval_secs = 5

[stats]
max_slots = 32
```

- [ ] **Step 2: Write c/xdp-receiver/main.c**

```c
#include "config.h"
#include "display.h"
#include "receiver.h"
#include "stats.h"
#include "xdp.h"
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static volatile sig_atomic_t g_shutdown = 0;

static void sig_handler(int sig) {
    (void)sig;
    g_shutdown = 1;
}

int main(int argc, char **argv) {
    config_t cfg;
    config_init_defaults(&cfg);
    // Override kernel-receiver default interface to something saner for XDP.
    strcpy(cfg.network.interface, "eth0");

    const char *cfg_path = NULL;
    if (config_parse_cli(&cfg, argc, argv, &cfg_path) != 0) return 1;
    if (cfg_path == NULL) cfg_path = "config.toml";
    int load_rc = config_load_file(&cfg, cfg_path);
    if (load_rc == -1) {
        fprintf(stderr, "failed to load config from %s\n", cfg_path);
        return 1;
    }
    config_parse_cli(&cfg, argc, argv, &cfg_path);

    fprintf(stderr, "edge-multicast-xdp-receiver (c)\n");
    fprintf(stderr, "Interface: %s, Multicast: %s, Shred port: %u, Heartbeat port: %u\n",
            cfg.network.interface, cfg.network.multicast_group,
            cfg.network.shred_port, cfg.network.heartbeat_port);
    fprintf(stderr, "XDP mode: %d, RX queue: %u, UMEM: %zuMB (%zu frames x %zu bytes)\n",
            cfg.xdp.mode, cfg.xdp.rx_queue,
            cfg.xdp.umem_size / 1048576,
            config_frame_count(&cfg), cfg.xdp.frame_size);

    stats_t stats;
    stats_init(&stats, cfg.stats.max_slots);

    pthread_mutex_t lock;
    pthread_mutex_init(&lock, NULL);

    // 1. Load and attach XDP program
    xdp_handle_t xdp;
    if (xdp_attach(&cfg, &xdp) != 0) {
        stats_free(&stats);
        pthread_mutex_destroy(&lock);
        return 1;
    }
    strncpy(stats.xdp_attach_mode, xdp.attach_mode, sizeof(stats.xdp_attach_mode) - 1);

    // 2. Create AF_XDP socket
    afxdp_receiver_t recv;
    if (afxdp_receiver_init(&recv, &cfg) != 0) {
        xdp_detach(&xdp);
        stats_free(&stats);
        pthread_mutex_destroy(&lock);
        return 1;
    }

    // 3. Register socket in XSK BPF map
    if (xdp_register_xsk(&xdp, cfg.xdp.rx_queue, afxdp_receiver_socket_fd(&recv)) != 0) {
        afxdp_receiver_destroy(&recv);
        xdp_detach(&xdp);
        stats_free(&stats);
        pthread_mutex_destroy(&lock);
        return 1;
    }

    // 4. Populate fill ring
    if (afxdp_receiver_fill_ring(&recv) != 0) {
        afxdp_receiver_destroy(&recv);
        xdp_detach(&xdp);
        stats_free(&stats);
        pthread_mutex_destroy(&lock);
        return 1;
    }

    // 5. Install signal handler
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sig_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // 6. Spawn receiver thread
    afxdp_thread_ctx_t ctx = {
        .r = &recv,
        .cfg = &cfg,
        .xdp = &xdp,
        .stats = &stats,
        .stats_lock = &lock,
        .shutdown = &g_shutdown,
    };
    pthread_t tid;
    if (pthread_create(&tid, NULL, afxdp_receiver_thread, &ctx) != 0) {
        perror("pthread_create");
        afxdp_receiver_destroy(&recv);
        xdp_detach(&xdp);
        stats_free(&stats);
        pthread_mutex_destroy(&lock);
        return 1;
    }

    // 7. Display on main thread
    display_run(&cfg, &stats, &lock, &g_shutdown);

    // 8. Wait for receiver
    pthread_join(tid, NULL);

    // 9. Cleanup
    xdp_detach(&xdp);
    afxdp_receiver_destroy(&recv);
    stats_free(&stats);
    pthread_mutex_destroy(&lock);
    fprintf(stderr, "Shutdown complete.\n");
    return 0;
}
```

- [ ] **Step 3: Write c/xdp-receiver/Makefile**

```makefile
CC          ?= gcc
CLANG       ?= clang
CFLAGS      ?= -O2 -g -Wall -Wextra -Wpedantic -std=c11 -D_GNU_SOURCE
CPPFLAGS    += -I../common -I.
LDLIBS       = -lpthread -lncursesw -lbpf -lxdp -lelf -lz
BPF_CFLAGS   = -target bpf -O2 -g -Wall

COMMON_SRCS := $(filter-out %_test.c, $(wildcard ../common/*.c))
LOCAL_SRCS  := main.c receiver.c xdp.c
SRCS        := $(COMMON_SRCS) $(LOCAL_SRCS)
OBJS        := $(SRCS:.c=.o)

BIN = edge-multicast-xdp-receiver

all: $(BIN) bpf/xdp_filter.o

$(BIN): $(OBJS) bpf/xdp_filter.o
	$(CC) $(CFLAGS) $(OBJS) -o $@ $(LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

bpf/xdp_filter.o: bpf/xdp_filter.c
	$(CLANG) $(BPF_CFLAGS) -c $< -o $@

test: shred_test stats_test config_test find_udp_payload_test
	./shred_test
	./stats_test
	./config_test
	./find_udp_payload_test

shred_test: ../common/shred.o ../common/shred_test.o
	$(CC) $(CFLAGS) $^ -o $@

stats_test: ../common/stats.o ../common/stats_test.o
	$(CC) $(CFLAGS) $^ -o $@

config_test: ../common/config.o ../common/config_test.o ../common/toml.o
	$(CC) $(CFLAGS) $^ -o $@

find_udp_payload_test: receiver.o find_udp_payload_test.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDLIBS)

../common/shred_test.o: ../common/shred_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

../common/stats_test.o: ../common/stats_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

../common/config_test.o: ../common/config_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

find_udp_payload_test.o: find_udp_payload_test.c
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(BIN) bpf/xdp_filter.o \
	      shred_test stats_test config_test find_udp_payload_test \
	      ../common/*_test.o find_udp_payload_test.o

.PHONY: all test clean
```

- [ ] **Step 4: Build and run tests (Linux only)**

```bash
cd c/xdp-receiver && make && make test
```

Expected:
- `bpf/xdp_filter.o` produced
- Binary `edge-multicast-xdp-receiver` produced
- All 4 test binaries (`shred_test`, `stats_test`, `config_test`, `find_udp_payload_test`) report PASS lines

If on non-Linux, skip the full build and only verify the code compiles up through individual file-level `-c` checks (libbpf/libxdp won't be present).

- [ ] **Step 5: Commit**

```bash
git add c/xdp-receiver/
git commit -m "feat(c): xdp-receiver main, config, and Makefile"
```

---

### Task 12: Final Cleanup + README Update

**Files:**
- Modify: `README.md`
- Modify: `c/README.md` if needed

- [ ] **Step 1: Run all tests from both binaries**

```bash
cd c/kernel-receiver && make clean && make && make test
cd ../xdp-receiver && make clean && make && make test
```

Expected: both binaries build, all unit tests pass.

- [ ] **Step 2: Update top-level README.md**

Find the implementations table in `README.md`:

```markdown
| **C** | planned | planned |
```

Replace with:

```markdown
| **C** | [c/kernel-receiver](c/kernel-receiver/) | [c/xdp-receiver](c/xdp-receiver/) |
```

- [ ] **Step 3: Run a final sanity compile from clean**

```bash
cd c/kernel-receiver && make clean && make
cd ../xdp-receiver && make clean && make
```

Expected: both binaries produced without warnings beyond those from vendored tomlc99.

- [ ] **Step 4: Commit**

```bash
git add README.md c/
git commit -m "chore(c): final cleanup, update top-level README with C implementations"
```

---

## Build & Run Instructions (Linux)

### Prerequisites

```bash
sudo apt install build-essential clang llvm libncurses-dev libbpf-dev libxdp-dev libelf-dev zlib1g-dev
```

### Build

```bash
cd c/kernel-receiver && make
cd ../xdp-receiver && make
```

### Test

```bash
cd c/kernel-receiver && make test
cd ../xdp-receiver && make test
```

### Set capabilities for the XDP receiver

```bash
sudo setcap cap_net_raw,cap_net_admin,cap_bpf,cap_perfmon=ep ./c/xdp-receiver/edge-multicast-xdp-receiver
```

### Run

```bash
./c/kernel-receiver/edge-multicast-receiver --interface doublezero1
./c/xdp-receiver/edge-multicast-xdp-receiver --interface eth0
```

### Manual XDP Detach

```bash
sudo ip link set dev eth0 xdp off
```
