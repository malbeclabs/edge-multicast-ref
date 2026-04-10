#include "display.h"
#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
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

        // Print per-slot lines for newly-seen slots.
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
