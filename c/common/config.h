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
