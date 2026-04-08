#ifndef EDGE_MULTICAST_REF_C_SHRED_H
#define EDGE_MULTICAST_REF_C_SHRED_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Derived from firedancer/src/ballet/shred/fd_shred.h (Apache 2.0).
// See c/common/NOTICE for attribution.

// Packed common header. Identical layout for data and coding shreds.
// All multi-byte fields are little-endian on the wire, matching x86_64/ARM64
// host byte order, so direct reads via the packed struct work without byteswap.
struct __attribute__((packed)) shred_common_hdr {
    uint8_t  signature[64];   // offset 0x00
    uint8_t  variant;         // offset 0x40
    uint64_t slot;            // offset 0x41
    uint32_t idx;             // offset 0x49
    uint16_t version;         // offset 0x4d
    uint32_t fec_set_idx;     // offset 0x4f
};  // sizeof == 83

#define SHRED_COMMON_HDR_SZ 83

typedef struct {
    uint64_t slot;
    uint32_t idx;
    uint32_t fec_set_idx;
    uint16_t version;
    uint8_t  signature[64];
    bool     is_data;
} parsed_shred_t;

// Parse a UDP payload as a shred common header.
// Returns true on success, false if the payload is too short or the variant
// byte does not identify a known data or coding shred.
bool shred_parse(const uint8_t *payload, size_t len, parsed_shred_t *out);

#endif
