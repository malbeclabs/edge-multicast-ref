#include "shred.h"
#include <string.h>

_Static_assert(sizeof(struct shred_common_hdr) == SHRED_COMMON_HDR_SZ,
               "shred_common_hdr size mismatch: check packed attribute and field types");

// Classify a variant byte. Returns 1 for data, 0 for coding, -1 for unknown.
static int classify_variant(uint8_t variant) {
    if (variant == 0xa5) return 1;           // legacy data
    if (variant == 0x5a) return 0;           // legacy coding
    if ((variant & 0xC0) == 0x80) return 1;  // any merkle data
    if ((variant & 0xC0) == 0x40) return 0;  // any merkle coding
    return -1;
}

bool shred_parse(const uint8_t *payload, size_t len, parsed_shred_t *out) {
    if (payload == NULL || len < SHRED_COMMON_HDR_SZ || out == NULL) {
        return false;
    }
    const struct shred_common_hdr *hdr = (const struct shred_common_hdr *)payload;
    int kind = classify_variant(hdr->variant);
    if (kind < 0) {
        return false;
    }
    memcpy(out->signature, hdr->signature, 64);
    out->slot = hdr->slot;
    out->idx = hdr->idx;
    out->version = hdr->version;
    out->fec_set_idx = hdr->fec_set_idx;
    out->is_data = (kind == 1);
    return true;
}
