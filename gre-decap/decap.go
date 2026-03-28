package main

import (
	"fmt"
	"net"
	"os"

	"github.com/cilium/ebpf/link"
)

// DecapHandle holds the loaded eBPF objects and XDP link.
type DecapHandle struct {
	Objs      XdpDecapObjects
	Link      link.Link
	Interface string
}

// Close detaches the XDP program and releases eBPF resources.
func (h *DecapHandle) Close() {
	fmt.Fprintf(os.Stderr, "Detaching XDP program from %s...\n", h.Interface)
	if h.Link != nil {
		h.Link.Close()
	}
	h.Objs.Close()
}

// AttachDecap loads the eBPF program, attaches it to the given interface,
// and writes the decap configuration into the config map.
func AttachDecap(ifaceName, xdpMode string, multicastIP string) (*DecapHandle, string, error) {
	var objs XdpDecapObjects
	if err := LoadXdpDecapObjects(&objs, nil); err != nil {
		return nil, "", fmt.Errorf("load eBPF objects: %w", err)
	}

	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		objs.Close()
		return nil, "", fmt.Errorf("interface %q: %w", ifaceName, err)
	}

	var l link.Link
	var actualMode string

	switch xdpMode {
	case "native":
		l, err = link.AttachXDP(link.XDPOptions{
			Program:   objs.XdpDecap,
			Interface: iface.Index,
			Flags:     link.XDPDriverMode,
		})
		actualMode = "native"
	case "skb":
		l, err = link.AttachXDP(link.XDPOptions{
			Program:   objs.XdpDecap,
			Interface: iface.Index,
			Flags:     link.XDPGenericMode,
		})
		actualMode = "skb"
	default: // auto
		l, err = link.AttachXDP(link.XDPOptions{
			Program:   objs.XdpDecap,
			Interface: iface.Index,
			Flags:     link.XDPDriverMode,
		})
		actualMode = "native"
		if err != nil {
			fmt.Fprintf(os.Stderr, "Native XDP attach failed (%v), trying SKB mode\n", err)
			objs.Close()
			if err2 := LoadXdpDecapObjects(&objs, nil); err2 != nil {
				return nil, "", fmt.Errorf("reload eBPF objects: %w", err2)
			}
			l, err = link.AttachXDP(link.XDPOptions{
				Program:   objs.XdpDecap,
				Interface: iface.Index,
				Flags:     link.XDPGenericMode,
			})
			actualMode = "skb"
		}
	}
	if err != nil {
		objs.Close()
		return nil, "", fmt.Errorf("attach XDP (%s mode): %w", actualMode, err)
	}

	// Write decap config to map.
	var ipU32 uint32
	if multicastIP != "" {
		ip := net.ParseIP(multicastIP).To4()
		if ip == nil {
			l.Close()
			objs.Close()
			return nil, "", fmt.Errorf("invalid multicast IP: %s", multicastIP)
		}
		// Convert to big-endian numeric to match read_be32 in eBPF.
		ipU32 = uint32(ip[0])<<24 | uint32(ip[1])<<16 | uint32(ip[2])<<8 | uint32(ip[3])
	}

	cfg := XdpDecapDecapConfig{
		MulticastIp:   ipU32,
		ShredPort:     0, // no port filtering by default
		HeartbeatPort: 0,
	}
	if err := objs.Config.Put(uint32(0), cfg); err != nil {
		l.Close()
		objs.Close()
		return nil, "", fmt.Errorf("write decap config: %w", err)
	}

	return &DecapHandle{
		Objs:      objs,
		Link:      l,
		Interface: ifaceName,
	}, actualMode, nil
}

// ReadStats reads per-CPU stats and returns aggregated totals.
func (h *DecapHandle) ReadStats() (decapped, passed, errors uint64, err error) {
	var perCPU []XdpDecapDecapStats
	if err := h.Objs.Stats.Lookup(uint32(0), &perCPU); err != nil {
		return 0, 0, 0, fmt.Errorf("lookup stats: %w", err)
	}
	for _, v := range perCPU {
		decapped += v.Decapped
		passed += v.Passed
		errors += v.Errors
	}
	return decapped, passed, errors, nil
}
