#include "display.h"

int display_tui_run(const config_t *cfg, stats_t *stats,
                    pthread_mutex_t *stats_lock,
                    volatile sig_atomic_t *shutdown) {
    (void)cfg; (void)stats; (void)stats_lock; (void)shutdown;
    return -1;  // stub — replaced in Task 6
}
