package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/malbeclabs/edge-multicast-ref/go/internal/shred"
	"golang.org/x/sys/unix"
)

const (
	ethHdrLen      = 14
	ipv4HdrMinLen  = 20
	greHdrMinLen   = 4
	udpHdrLen      = 8
	pollTimeoutMs  = 100
	xdpStatsperiod = time.Second
)

// AfXdpSocket wraps a raw AF_XDP socket with its UMEM and ring buffers.
type AfXdpSocket struct {
	fd         int
	umem       []byte
	frameSize  int
	frameCount int

	// Ring buffer pointers (mmap'd)
	fillRing *xskRing
	rxRing   *xskRing
}

// xskRing represents an XSK ring buffer (fill or rx).
type xskRing struct {
	producer *uint32
	consumer *uint32
	descs    unsafe.Pointer // fill: *uint64 (addrs), rx: *unix.XDPDesc
	mask     uint32
	size     uint32
	cachedProd uint32
	cachedCons uint32
}

// xdpDesc matches the kernel's struct xdp_desc.
type xdpDesc struct {
	Addr    uint64
	Len     uint32
	Options uint32
}

// NewAfXdpSocket creates an AF_XDP socket, allocates UMEM, and sets up ring buffers.
func NewAfXdpSocket(cfg *Config) (*AfXdpSocket, error) {
	frameCount := cfg.FrameCount()
	frameSize := cfg.Xdp.FrameSize
	umemSize := frameCount * frameSize

	// Allocate UMEM region (page-aligned).
	umem, err := unix.Mmap(-1, 0, umemSize, unix.PROT_READ|unix.PROT_WRITE,
		unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_POPULATE)
	if err != nil {
		return nil, fmt.Errorf("mmap UMEM: %w", err)
	}

	// Create AF_XDP socket.
	fd, err := unix.Socket(unix.AF_XDP, unix.SOCK_RAW, 0)
	if err != nil {
		unix.Munmap(umem)
		return nil, fmt.Errorf("socket(AF_XDP): %w", err)
	}

	// Register UMEM.
	umemReg := unix.XDPUmemReg{
		Addr: uint64(uintptr(unsafe.Pointer(&umem[0]))),
		Len:  uint64(umemSize),
		Size: uint32(frameSize),
	}

	// Use power-of-2 ring sizes. Use frameCount if it's a power of 2,
	// otherwise find the largest power of 2 <= frameCount.
	ringSize := uint32(nearestPow2(frameCount))

	_, _, errno := unix.Syscall6(unix.SYS_SETSOCKOPT, uintptr(fd),
		unix.SOL_XDP, unix.XDP_UMEM_REG,
		uintptr(unsafe.Pointer(&umemReg)), unsafe.Sizeof(umemReg), 0)
	if errno != 0 {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("setsockopt XDP_UMEM_REG: %w", errno)
	}

	// Set ring sizes.
	if err := setsockoptInt(fd, unix.SOL_XDP, unix.XDP_UMEM_FILL_RING, int(ringSize)); err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("setsockopt FILL_RING: %w", err)
	}
	if err := setsockoptInt(fd, unix.SOL_XDP, unix.XDP_UMEM_COMPLETION_RING, int(ringSize)); err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("setsockopt COMPLETION_RING: %w", err)
	}
	if err := setsockoptInt(fd, unix.SOL_XDP, unix.XDP_RX_RING, int(ringSize)); err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("setsockopt RX_RING: %w", err)
	}

	// Get mmap offsets.
	offsets, err := getXdpMmapOffsets(fd)
	if err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("get mmap offsets: %w", err)
	}

	// mmap fill ring.
	fillMap, err := unix.Mmap(fd, unix.XDP_UMEM_PGOFF_FILL_RING,
		int(offsets.Fr.Desc)+int(ringSize)*8, // each fill desc is uint64
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		return nil, fmt.Errorf("mmap fill ring: %w", err)
	}

	fillRing := &xskRing{
		producer: (*uint32)(unsafe.Pointer(&fillMap[offsets.Fr.Producer])),
		consumer: (*uint32)(unsafe.Pointer(&fillMap[offsets.Fr.Consumer])),
		descs:    unsafe.Pointer(&fillMap[offsets.Fr.Desc]),
		mask:     ringSize - 1,
		size:     ringSize,
	}

	// mmap rx ring.
	rxMap, err := unix.Mmap(fd, unix.XDP_PGOFF_RX_RING,
		int(offsets.Rx.Desc)+int(ringSize)*int(unsafe.Sizeof(xdpDesc{})),
		unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED|unix.MAP_POPULATE)
	if err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		unix.Munmap(fillMap)
		return nil, fmt.Errorf("mmap rx ring: %w", err)
	}

	rxRing := &xskRing{
		producer: (*uint32)(unsafe.Pointer(&rxMap[offsets.Rx.Producer])),
		consumer: (*uint32)(unsafe.Pointer(&rxMap[offsets.Rx.Consumer])),
		descs:    unsafe.Pointer(&rxMap[offsets.Rx.Desc]),
		mask:     ringSize - 1,
		size:     ringSize,
	}

	// Bind socket.
	sa := unix.SockaddrXDP{
		Flags:   unix.XDP_USE_NEED_WAKEUP,
		Ifindex: uint32(mustIfaceIndex(cfg.Network.PhysicalInterface)),
		QueueID: cfg.Xdp.RxQueue,
	}
	if err := unix.Bind(fd, &sa); err != nil {
		unix.Close(fd)
		unix.Munmap(umem)
		unix.Munmap(fillMap)
		unix.Munmap(rxMap)
		return nil, fmt.Errorf("bind AF_XDP: %w", err)
	}

	sock := &AfXdpSocket{
		fd:         fd,
		umem:       umem,
		frameSize:  frameSize,
		frameCount: frameCount,
		fillRing:   fillRing,
		rxRing:     rxRing,
	}

	return sock, nil
}

// FD returns the socket file descriptor (for registering in XSKMAP).
func (s *AfXdpSocket) FD() int {
	return s.fd
}

// Close releases the AF_XDP socket and UMEM resources.
func (s *AfXdpSocket) Close() {
	unix.Close(s.fd)
	unix.Munmap(s.umem)
}

// FillRing submits all frames to the fill ring so the kernel can use them for RX.
func (s *AfXdpSocket) FillRing() {
	for i := 0; i < s.frameCount && i < int(s.fillRing.size); i++ {
		idx := *s.fillRing.producer & s.fillRing.mask
		fillDescs := (*[1 << 20]uint64)(s.fillRing.descs)
		fillDescs[idx] = uint64(i * s.frameSize)
		*s.fillRing.producer++
	}
	s.fillRing.cachedProd = *s.fillRing.producer
}

// ReceivePackets polls for received packets and returns the descriptors.
func (s *AfXdpSocket) ReceivePackets() []xdpDesc {
	cons := *s.rxRing.consumer
	prod := *s.rxRing.producer

	if cons == prod {
		return nil
	}

	descs := make([]xdpDesc, 0, prod-cons)
	rxDescs := (*[1 << 20]xdpDesc)(s.rxRing.descs)

	for cons != prod {
		idx := cons & s.rxRing.mask
		descs = append(descs, rxDescs[idx])
		cons++
	}

	*s.rxRing.consumer = cons
	return descs
}

// ReturnToFill returns consumed frame addresses to the fill ring.
func (s *AfXdpSocket) ReturnToFill(descs []xdpDesc) int {
	fillDescs := (*[1 << 20]uint64)(s.fillRing.descs)
	returned := 0
	for _, d := range descs {
		idx := *s.fillRing.producer & s.fillRing.mask
		fillDescs[idx] = d.Addr
		*s.fillRing.producer++
		returned++
	}
	return returned
}

// GetFrame returns the packet data from UMEM for a given descriptor.
func (s *AfXdpSocket) GetFrame(d xdpDesc) []byte {
	start := int(d.Addr)
	end := start + int(d.Len)
	if end > len(s.umem) {
		end = len(s.umem)
	}
	return s.umem[start:end]
}

// findUDPPayload parses through Eth -> outer IP -> GRE -> inner IP -> UDP headers.
// Returns (payload_offset, udp_dst_port, ok).
func findUDPPayload(pkt []byte) (int, uint16, bool) {
	if len(pkt) < ethHdrLen+ipv4HdrMinLen {
		return 0, 0, false
	}

	offset := ethHdrLen

	// Outer IPv4: read IHL
	outerIHL := int(pkt[offset]&0x0F) * 4
	if outerIHL < ipv4HdrMinLen || offset+outerIHL > len(pkt) {
		return 0, 0, false
	}
	// Check protocol == GRE (47)
	if pkt[offset+9] != 47 {
		return 0, 0, false
	}
	offset += outerIHL

	// GRE header
	if offset+greHdrMinLen > len(pkt) {
		return 0, 0, false
	}
	greFlags := uint16(pkt[offset])<<8 | uint16(pkt[offset+1])
	greLen := greHdrMinLen
	if greFlags&0x8000 != 0 {
		greLen += 4 // Checksum
	}
	if greFlags&0x2000 != 0 {
		greLen += 4 // Key
	}
	if greFlags&0x1000 != 0 {
		greLen += 4 // Sequence
	}
	offset += greLen

	// Inner IPv4
	if offset+ipv4HdrMinLen > len(pkt) {
		return 0, 0, false
	}
	innerIHL := int(pkt[offset]&0x0F) * 4
	if innerIHL < ipv4HdrMinLen || offset+innerIHL > len(pkt) {
		return 0, 0, false
	}
	if pkt[offset+9] != 17 { // Not UDP
		return 0, 0, false
	}
	offset += innerIHL

	// UDP header
	if offset+udpHdrLen > len(pkt) {
		return 0, 0, false
	}
	dstPort := uint16(pkt[offset+2])<<8 | uint16(pkt[offset+3])
	return offset + udpHdrLen, dstPort, true
}

// RunRecvLoop runs the AF_XDP receive loop, processing packets until ctx is done.
func RunRecvLoop(ctx context.Context, cfg *Config, xdpStats *XdpReceiverStats, mu *sync.RWMutex, handle *XdpHandle, sock *AfXdpSocket) error {
	// Fill the RX ring with all available frames.
	sock.FillRing()
	fmt.Fprintf(os.Stderr, "AF_XDP receiver running on %s queue %d\n",
		cfg.Network.PhysicalInterface, cfg.Xdp.RxQueue)

	pollFds := []unix.PollFd{
		{Fd: int32(sock.fd), Events: unix.POLLIN},
	}

	xdpStatsTimer := time.Now()

	for {
		select {
		case <-ctx.Done():
			fmt.Fprintln(os.Stderr, "AF_XDP receiver shutting down")
			return nil
		default:
		}

		// Poll for received packets.
		n, err := unix.Poll(pollFds, pollTimeoutMs)
		if err != nil {
			if err == unix.EINTR {
				continue
			}
			return fmt.Errorf("poll: %w", err)
		}
		if n == 0 {
			// Timeout: periodically read XDP stats.
			if time.Since(xdpStatsTimer) >= xdpStatsperiod {
				updateBpfStats(handle, xdpStats, mu)
				xdpStatsTimer = time.Now()
			}
			continue
		}

		// Receive packets from the RX ring.
		rxDescs := sock.ReceivePackets()
		if len(rxDescs) == 0 {
			continue
		}

		for _, desc := range rxDescs {
			pkt := sock.GetFrame(desc)
			processPacket(pkt, cfg, xdpStats, mu)
		}

		// Return consumed frames to fill ring.
		returned := sock.ReturnToFill(rxDescs)
		mu.Lock()
		if returned < len(rxDescs) {
			xdpStats.AfxdpFillStarvation++
		}
		xdpStats.AfxdpRxFillLevel = sock.frameCount - len(rxDescs) + returned
		mu.Unlock()

		// Periodically read BPF stats.
		if time.Since(xdpStatsTimer) >= xdpStatsperiod {
			updateBpfStats(handle, xdpStats, mu)
			xdpStatsTimer = time.Now()
		}
	}
}

// processPacket strips GRE encapsulation, parses the shred payload, and updates stats.
func processPacket(pkt []byte, cfg *Config, xdpStats *XdpReceiverStats, mu *sync.RWMutex) {
	offset, port, ok := findUDPPayload(pkt)
	if !ok {
		mu.Lock()
		xdpStats.RecordParseError()
		mu.Unlock()
		return
	}
	if port == cfg.Network.HeartbeatPort {
		mu.Lock()
		xdpStats.RecordHeartbeat()
		mu.Unlock()
		return
	}
	if port != cfg.Network.ShredPort {
		return
	}

	parsed, err := shred.ParseShred(pkt[offset:])
	if err != nil {
		mu.Lock()
		xdpStats.RecordParseError()
		mu.Unlock()
		return
	}
	mu.Lock()
	xdpStats.RecordShred(parsed.Slot, parsed.IsData, parsed.Index, parsed.FECSetIndex, parsed.Signature)
	mu.Unlock()
}

// updateBpfStats reads the XDP stats from the BPF map and updates the receiver stats.
func updateBpfStats(handle *XdpHandle, xdpStats *XdpReceiverStats, mu *sync.RWMutex) {
	redirected, passed, errors, err := ReadXdpStats(&handle.Objs)
	if err != nil {
		return
	}
	mu.Lock()
	xdpStats.UpdateXdpCounters(redirected, passed, errors)
	mu.Unlock()
}

// --- Helpers ---

func mustIfaceIndex(name string) int {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Interface %q not found: %v\n", name, err)
		os.Exit(1)
	}
	return iface.Index
}

func nearestPow2(n int) int {
	if n <= 0 {
		return 1
	}
	v := 1
	for v < n {
		v <<= 1
	}
	// If v > n, go back one step (largest pow2 <= n).
	if v > n {
		v >>= 1
	}
	if v == 0 {
		v = 1
	}
	return v
}

func setsockoptInt(fd, level, opt, value int) error {
	v := uint32(value)
	_, _, errno := unix.Syscall6(unix.SYS_SETSOCKOPT, uintptr(fd),
		uintptr(level), uintptr(opt),
		uintptr(unsafe.Pointer(&v)), unsafe.Sizeof(v), 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// xdpMmapOffsets and xdpRingOffset mirror the kernel structures.
type xdpRingOffset struct {
	Producer uint64
	Consumer uint64
	Desc     uint64
	Flags    uint64
}

type xdpMmapOffsets struct {
	Rx xdpRingOffset
	Tx xdpRingOffset
	Fr xdpRingOffset
	Cr xdpRingOffset
}

func getXdpMmapOffsets(fd int) (*xdpMmapOffsets, error) {
	var offsets xdpMmapOffsets
	size := uint32(unsafe.Sizeof(offsets))
	_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, uintptr(fd),
		unix.SOL_XDP, unix.XDP_MMAP_OFFSETS,
		uintptr(unsafe.Pointer(&offsets)), uintptr(unsafe.Pointer(&size)), 0)
	if errno != 0 {
		return nil, errno
	}
	return &offsets, nil
}
