# gre-decap

XDP program that strips GRE encapsulation from DoubleZero multicast packets inline on the physical NIC. After decapsulation, the kernel sees plain multicast UDP frames — no tunnel interface or application changes needed.

## How It Works

```
Before (wire):  Eth → Outer IP → GRE → Inner IP → UDP → Payload
After (kernel): Eth → Inner IP → UDP → Payload
```

The XDP program parses through the GRE encapsulation, calls `bpf_xdp_adjust_head()` to strip the outer IP and GRE headers, writes a new Ethernet header with the correct multicast destination MAC, and returns `XDP_PASS`.

## Build

Requires: Go 1.23+, clang, libbpf-dev

```bash
go generate ./...
go build -o gre-decap .
```

## Usage

```bash
# Decap all GRE traffic on eth0
sudo ./gre-decap -i eth0

# Decap only a specific multicast group
sudo ./gre-decap -i eth0 -g 233.84.178.1

# Force SKB mode (if native XDP not supported)
sudo ./gre-decap -i eth0 -g 233.84.178.1 -m skb

# Disable stats output
sudo ./gre-decap -i eth0 -g 233.84.178.1 -s 0
```

Applications receive packets by joining the multicast group on the physical interface:

```bash
# Example: receive shreds with socat
socat UDP4-RECVFROM:7733,ip-add-membership=233.84.178.1:eth0,fork -
```

## Kernel Setup

After decap, the source IP of the inner packet (148.51.x.x) may not be routable via the physical NIC. Disable reverse path filtering:

```bash
sysctl -w net.ipv4.conf.eth0.rp_filter=0
```

## Interaction with GRE Tunnel Interface

This program and the `doublezero1` tunnel interface are mutually exclusive for matched packets. Once the XDP program rewrites a GRE packet to plain multicast UDP, the kernel's GRE module never sees it — `doublezero1` receives nothing for that packet. Non-matching packets pass through to the tunnel normally.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-i` | (required) | Physical NIC to attach to |
| `-g` | all GRE | Multicast group IP to decap |
| `-m` | auto | XDP mode: `native`, `skb`, `auto` |
| `-s` | 1s | Stats interval (`0` to disable) |
