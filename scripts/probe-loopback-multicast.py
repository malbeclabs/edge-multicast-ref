#!/usr/bin/env python3
"""Can this host deliver a multicast datagram to itself over loopback?

The socket end-to-end tests need that, and they need it to be a *precondition*
rather than a discovery: a runner that cannot do it should say so in one line,
not fail inside a capture with an errno a reader has to interpret. And it must
never be turned into a skip — a test that quietly does not run is a gate that
quietly does not gate.

Mirrors what the tests do: MCAST-TEST-NET group, membership joined on the
loopback address, IP_MULTICAST_LOOP on.
"""
import socket
import struct
import sys

GROUP = "233.252.0.10"
LOCAL = "127.0.0.1"
PORT = 40765
PAYLOAD = b"probe"


def main() -> int:
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        rx.bind(("", PORT))
        rx.setsockopt(
            socket.IPPROTO_IP,
            socket.IP_ADD_MEMBERSHIP,
            struct.pack("4s4s", socket.inet_aton(GROUP), socket.inet_aton(LOCAL)),
        )
        rx.settimeout(3.0)

        tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        tx.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        tx.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(LOCAL))
        tx.sendto(PAYLOAD, (GROUP, PORT))

        got, _ = rx.recvfrom(len(PAYLOAD) + 1)
    except OSError as e:
        print(f"loopback multicast unavailable: {e}", file=sys.stderr)
        return 1
    finally:
        rx.close()

    if got != PAYLOAD:
        print(f"loopback multicast delivered {got!r}, not {PAYLOAD!r}", file=sys.stderr)
        return 1
    print("loopback multicast works: a datagram sent to the group came back")
    return 0


if __name__ == "__main__":
    sys.exit(main())
