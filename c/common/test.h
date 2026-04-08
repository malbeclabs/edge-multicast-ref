#ifndef EDGE_MULTICAST_REF_C_TEST_H
#define EDGE_MULTICAST_REF_C_TEST_H

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#define TEST(name) static void test_##name(void)

#define RUN_TEST(name) do { \
    test_##name(); \
    printf("PASS: %s\n", #name); \
} while (0)

#endif
