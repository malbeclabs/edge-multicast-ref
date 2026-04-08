#include "stats.h"
#include "test.h"
#include <string.h>

static const uint8_t SIG_AB[64] = {
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
    0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB, 0xAB,
};

TEST(new_stats_zero) {
    stats_t s;
    stats_init(&s, 4);
    assert(s.total_data_shreds == 0);
    assert(s.total_coding_shreds == 0);
    assert(s.total_heartbeats == 0);
    assert(s.parse_errors == 0);
    assert(s.slots_len == 0);
    assert(s.xdp_redirected == 0);
    assert(s.xdp_attach_mode[0] == '\0');
    stats_free(&s);
}

TEST(record_shred_data) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    assert(s.total_data_shreds == 1);
    assert(s.total_coding_shreds == 0);
    assert(s.slots_len == 1);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot != NULL);
    assert(slot->slot == 100);
    assert(slot->data_shred_count == 1);
    assert(slot->signature_prefix[0] == 0xAB);
    stats_free(&s);
}

TEST(record_shred_coding) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, false, 5, 0, SIG_AB);
    assert(s.total_coding_shreds == 1);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->coding_shred_count == 1);
    stats_free(&s);
}

TEST(multiple_shreds_same_slot) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 1, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 5, 1, SIG_AB);
    stats_record_shred(&s, 100, false, 0, 0, SIG_AB);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->data_shred_count == 3);
    assert(slot->coding_shred_count == 1);
    assert(slot->highest_data_index == 5);
    assert(slot->fec_set_count == 2);
    stats_free(&s);
}

TEST(ring_buffer_eviction) {
    stats_t s;
    stats_init(&s, 4);
    for (uint64_t slot = 0; slot < 6; slot++) {
        stats_record_shred(&s, slot, true, 0, 0, SIG_AB);
    }
    assert(s.slots_len == 4);
    assert(stats_get_slot(&s, 0) == NULL);
    assert(stats_get_slot(&s, 1) == NULL);
    assert(stats_get_slot(&s, 2) != NULL);
    assert(stats_get_slot(&s, 5) != NULL);
    stats_free(&s);
}

TEST(heartbeat_counting) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_heartbeat(&s);
    stats_record_heartbeat(&s);
    assert(s.total_heartbeats == 2);
    assert(s.last_heartbeat.tv_sec != 0);
    stats_free(&s);
}

TEST(recent_slots_descending) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 200, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 150, true, 0, 0, SIG_AB);
    const slot_stats_t *recent[4];
    size_t n = stats_recent_slots(&s, recent, 4);
    assert(n == 3);
    assert(recent[0]->slot == 200);
    assert(recent[1]->slot == 150);
    assert(recent[2]->slot == 100);
    stats_free(&s);
}

TEST(fec_set_dedup) {
    stats_t s;
    stats_init(&s, 4);
    stats_record_shred(&s, 100, true, 0, 0, SIG_AB);
    stats_record_shred(&s, 100, true, 1, 0, SIG_AB);  // same fec_set_index
    stats_record_shred(&s, 100, true, 2, 1, SIG_AB);
    stats_record_shred(&s, 100, true, 3, 1, SIG_AB);  // same fec_set_index
    stats_record_shred(&s, 100, true, 4, 2, SIG_AB);
    const slot_stats_t *slot = stats_get_slot(&s, 100);
    assert(slot->fec_set_count == 3);
    stats_free(&s);
}

TEST(update_xdp_counters) {
    stats_t s;
    stats_init(&s, 4);
    stats_update_xdp_counters(&s, 100, 50, 3);
    assert(s.xdp_redirected == 100);
    assert(s.xdp_passed == 50);
    assert(s.xdp_errors == 3);
    stats_free(&s);
}

int main(void) {
    RUN_TEST(new_stats_zero);
    RUN_TEST(record_shred_data);
    RUN_TEST(record_shred_coding);
    RUN_TEST(multiple_shreds_same_slot);
    RUN_TEST(ring_buffer_eviction);
    RUN_TEST(heartbeat_counting);
    RUN_TEST(recent_slots_descending);
    RUN_TEST(fec_set_dedup);
    RUN_TEST(update_xdp_counters);
    printf("All stats tests passed.\n");
    return 0;
}
