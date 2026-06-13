# Receiving the Hyperliquid Feed over DoubleZero Edge

This guide walks you from nothing to a live Hyperliquid market-data feed delivered
over [DoubleZero](https://doublezero.xyz) Edge.

The Hyperliquid feed is in early beta. It is published as UDP multicast on a DoubleZero
group named **`tiredsolid`**, on both **testnet** and **mainnet-beta**. This guide targets
the **`aws-tyo-hl-mainnet2`** publisher on mainnet-beta. See
[Channel details](#channel-details-mainnet-beta) below for its exact multicast address,
ports, and source ID.

There are two steps that are yours and one that we do for you:

1. Connect your host to DoubleZero (you).
2. Get an access pass for each receiving IP (we grant it during beta).
3. Subscribe to the `tiredsolid` group and read the feed (you).

## Before you start

- A **Linux host** with a stable **public IP** for each address you want to receive on.
- **GRE (IP protocol 47)** allowed inbound at your firewall or cloud security group.
  DoubleZero delivers data as GRE-encapsulated multicast. On AWS, also **disable the
  source/destination check** on the instance's ENI.
- A **DoubleZero identity keypair**. You create this during onboarding (see the connect
  guide linked below). Its public key is what we attach your access pass to.

## Step 1: Connect your host to DoubleZero

Follow the standard connect guide to install the `doublezero` client and onboard your
host:

➡️ **[How to connect to DoubleZero](https://docs.malbeclabs.com/setup/)**

Once the client is running, the tunnel appears as `doublezero1`:

```bash
ip a s doublezero1
```

The feed will arrive on this interface as plain UDP multicast once your access pass is in
place (Step 3) and you subscribe (Step 4). You then decode it yourself (Step 5).

> **Coming soon: a one-command container.** We also ship
> [`doublezero-edge-connect`](https://github.com/malbeclabs/doublezero-edge-connect), a
> container that bundles the client, decodes the feed, and re-serves it as normalized JSON
> over a WebSocket. It is not yet supported on mainnet-beta and is omitted from this guide
> for now. This page will be updated when it is ready.

## Step 2: Find the feed

List the multicast groups available on your network:

```bash
doublezero multicast group list
```

Look for the group with code **`tiredsolid`**. That is the Hyperliquid feed, on both
testnet and mainnet-beta. Note the code; you will use it in Steps 3 and 4.

### Channel details (mainnet-beta)

Several publisher hosts share the `tiredsolid` group. They use the same multicast address
but **distinct ports**, so you select a specific publisher by the ports you bind. This
guide targets **`aws-tyo-hl-mainnet2`**:

| Property | Value |
|----------|-------|
| Multicast group | `tiredsolid` → `233.84.178.15` |
| Publisher source address | `148.51.120.79` |
| Source ID | `3` |

It publishes two feeds, each on its own UDP ports:

| Feed | mktdata | refdata | snapshot | Spec |
|------|---------|---------|----------|------|
| Top-of-Book & Trades | `9201` | `9202` | — | [top-of-book](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) |
| Market-by-Order | `10201` | `10202` | `10203` | [market-by-order](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) |

The `source_id` field identifies the publisher host for audit, not the venue; every frame
from this host carries **`source_id=3`**. The venue is Hyperliquid. To receive
`aws-tyo-hl-mainnet2` specifically, bind to its ports above on `233.84.178.15`.

> On **testnet**, the `tiredsolid` group resolves to `233.84.178.6`. Ports and source ID
> are per-host; confirm the values for the publisher you target.

## Step 3: Get an access pass for each receiving IP

Multicast access is gated by an **access pass** tied to your identity and a specific
**client IP**, plus membership on the group's subscriber allow list. You need one access
pass per IP you intend to receive the feed on.

During the beta, the **DoubleZero Foundation creates the access pass and adds you to the
`tiredsolid` subscriber allow list manually**. Send us, for each receiving IP:

- the **public IP** you will receive on, and
- your **DoubleZero identity public key** (the pubkey of the keypair you onboarded with).

For transparency, these are the commands we run on your behalf for each IP:

```bash
# Grant an access pass for your identity on this IP
doublezero access-pass set \
  --accesspass-type prepaid \
  --client-ip <YOUR_IP> \
  --user-payer <YOUR_IDENTITY_PUBKEY>

# Add you to the tiredsolid subscriber allow list
doublezero multicast group allowlist subscriber add \
  --code tiredsolid \
  --user-payer <YOUR_IDENTITY_PUBKEY> \
  --client-ip <YOUR_IP>
```

Repeat for every IP. We will confirm once your passes are active.

## Step 4: Subscribe and receive

With your access pass in place, subscribe to the group:

```bash
doublezero connect multicast subscriber tiredsolid
```

Confirm the feed is arriving on the tunnel interface:

```bash
# Tunnel is up
ip a s doublezero1

# Packets are flowing from aws-tyo-hl-mainnet2 (Top-of-Book mktdata)
sudo tcpdump -ni doublezero1 host 233.84.178.15 and udp port 9201
```

## Step 5: Decode the feed

The feed is little-endian fixed-size binary frames on the multicast group. You decode it
yourself, in one of two ways.

### Use a reference parser from this repo

This repo ships multicast subscribers that decode the wire format and republish it as
JSON on a Unix socket. Use the one that matches the feed:

- [`go/topofbook-parser`](../go/topofbook-parser) for the
  [Top-of-Book & Trades](https://github.com/malbeclabs/edge-feed-spec/blob/main/top-of-book/spec.md) feed.
- [`go/marketbyorder-parser`](../go/marketbyorder-parser) for the
  [Market-by-Order](https://github.com/malbeclabs/edge-feed-spec/blob/main/market-by-order/spec.md) feed.

Pair a parser with its matching `*-bot` to build per-symbol state and persist to
ClickHouse, or read the parser's Unix socket directly from your own code. This is the
fastest way to usable data without writing a decoder. See the
[main README](../README.md#market-data-pipelines) for the full pipeline.

### Write your own decoder

To integrate directly into an existing trading system, decode against the
[edge-feed-spec](https://github.com/malbeclabs/edge-feed-spec). Start with the frame
header, then the message layouts for the feed you are receiving. Two things to keep in
mind:

- Each frame is at most **1,232 bytes** (one UDP datagram per frame), which leaves room
  for the GRE headers used in last-mile delivery.
- Bind to `aws-tyo-hl-mainnet2`'s ports on `233.84.178.15` (see
  [Channel details](#channel-details-mainnet-beta)). Frames from this host carry
  **`source_id=3`**.

## Getting help

For access passes, allow-list changes, or feed issues during the beta, contact the
DoubleZero Foundation with your identity pubkey and the IPs involved.
