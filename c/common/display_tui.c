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
