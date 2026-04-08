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
