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
