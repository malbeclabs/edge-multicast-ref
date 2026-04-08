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
