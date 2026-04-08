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
