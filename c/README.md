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
