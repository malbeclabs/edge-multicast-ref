#include "shred.h"
#include "test.h"
#include <string.h>

// Build a minimal valid shred header with the given variant byte.
// Returns a heap buffer of SHRED_COMMON_HDR_SZ bytes; caller frees.
static uint8_t *build_shred(uint8_t variant, uint64_t slot, uint32_t idx,
                            uint16_t version, uint32_t fec_set_idx,
                            uint8_t sig_byte) {
    uint8_t *buf = calloc(1, SHRED_COMMON_HDR_SZ);
    memset(buf, sig_byte, 64);
    buf[64] = variant;
    memcpy(buf + 65, &slot, 8);
    memcpy(buf + 73, &idx, 4);
    memcpy(buf + 77, &version, 2);
    memcpy(buf + 79, &fec_set_idx, 4);
    return buf;
}

TEST(parse_empty_returns_false) {
    parsed_shred_t out;
    assert(shred_parse(NULL, 0, &out) == false);
}

TEST(parse_too_short_returns_false) {
    uint8_t buf[82] = {0};
    parsed_shred_t out;
    assert(shred_parse(buf, sizeof(buf), &out) == false);
}

TEST(parse_merkle_data_variant) {
    uint8_t *buf = build_shred(0x80, 100, 5, 42, 2, 0xAB);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.slot == 100);
    assert(out.idx == 5);
    assert(out.version == 42);
    assert(out.fec_set_idx == 2);
    assert(out.is_data == true);
    assert(out.signature[0] == 0xAB);
    assert(out.signature[63] == 0xAB);
    free(buf);
}

TEST(parse_merkle_coding_variant) {
    uint8_t *buf = build_shred(0x40, 200, 10, 42, 3, 0xCD);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.slot == 200);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_merkle_data_chained_variant) {
    uint8_t *buf = build_shred(0x90, 300, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == true);
    free(buf);
}

TEST(parse_merkle_code_chained_variant) {
    uint8_t *buf = build_shred(0x60, 301, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_merkle_data_chained_resigned_variant) {
    uint8_t *buf = build_shred(0xb0, 500, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == true);
    free(buf);
}

TEST(parse_merkle_code_chained_resigned_variant) {
    uint8_t *buf = build_shred(0x70, 501, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_legacy_data_variant_0xa5) {
    uint8_t *buf = build_shred(0xa5, 400, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == true);
    free(buf);
}

TEST(parse_legacy_coding_variant_0x5a) {
    uint8_t *buf = build_shred(0x5a, 401, 0, 0, 0, 0x00);
    parsed_shred_t out;
    assert(shred_parse(buf, SHRED_COMMON_HDR_SZ, &out) == true);
    assert(out.is_data == false);
    free(buf);
}

TEST(parse_garbage_returns_false) {
    uint8_t buf[SHRED_COMMON_HDR_SZ];
    memset(buf, 0xFF, sizeof(buf));  // variant 0xFF is not a valid type
    parsed_shred_t out;
    assert(shred_parse(buf, sizeof(buf), &out) == false);
}

int main(void) {
    RUN_TEST(parse_empty_returns_false);
    RUN_TEST(parse_too_short_returns_false);
    RUN_TEST(parse_merkle_data_variant);
    RUN_TEST(parse_merkle_coding_variant);
    RUN_TEST(parse_merkle_data_chained_variant);
    RUN_TEST(parse_merkle_code_chained_variant);
    RUN_TEST(parse_merkle_data_chained_resigned_variant);
    RUN_TEST(parse_merkle_code_chained_resigned_variant);
    RUN_TEST(parse_legacy_data_variant_0xa5);
    RUN_TEST(parse_legacy_coding_variant_0x5a);
    RUN_TEST(parse_garbage_returns_false);
    printf("All shred tests passed.\n");
    return 0;
}
