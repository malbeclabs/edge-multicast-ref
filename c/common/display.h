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
