/**
 * @file test_resend_ring.cpp
 * @brief Unit tests for ResendRing (the rows a handler can resend to TP).
 */

#include "tp_publisher.hpp"
#include "catch_amalgamated.hpp"

#include <vector>

namespace {
K mkRow(long long seq) { return knk(2, kj(seq), kj(seq * 10)); }
std::vector<long long> after(const t2s::ResendRing& r, long long s) {
    std::vector<long long> out;
    r.forEachAfter(s, [&](long long seq, K row) { REQUIRE(kK(row)[0]->j == seq); out.push_back(seq); return true; });
    return out;
}
} // namespace

TEST_CASE("ResendRing keeps the last N rows in order", "[ring]") {
    t2s::ResendRing ring(3);
    REQUIRE(ring.empty());
    for (long long s = 1; s <= 5; ++s) ring.push(s, mkRow(s));
    REQUIRE(ring.size() == 3);
    REQUIRE(ring.oldestSeq() == 3);
    REQUIRE(ring.newestSeq() == 5);
    REQUIRE(after(ring, 0) == std::vector<long long>{3, 4, 5});
    REQUIRE(after(ring, 3) == std::vector<long long>{4, 5});
    REQUIRE(after(ring, 5).empty());
    REQUIRE(after(ring, 9).empty());
}

TEST_CASE("ResendRing tells how many needed rows it no longer holds", "[ring]") {
    t2s::ResendRing ring(3);
    for (long long s = 10; s <= 14; ++s) ring.push(s, mkRow(s));      // holds 12, 13, 14
    REQUIRE(ring.missingAfter(11) == 0);     // TP logged up to 11: 12.. are all here
    REQUIRE(ring.missingAfter(13) == 0);
    REQUIRE(ring.missingAfter(9) == 2);      // 10 and 11 were needed and are gone
    REQUIRE(ring.missingAfter(0) == 11);
    t2s::ResendRing none(3);
    REQUIRE(none.missingAfter(7) == 0);
}

TEST_CASE("ResendRing owns one reference per row and releases it", "[ring]") {
    K row = mkRow(1);
    r1(row);                                  // our own reference, to observe the count
    {
        t2s::ResendRing ring(2);
        ring.push(1, row);                    // ring takes the original reference
        REQUIRE(row->r == 1);                 // r counts extra references: ours + the ring's = 1
        ring.push(2, mkRow(2));
        ring.push(3, mkRow(3));               // evicts row 1
        REQUIRE(row->r == 0);                 // only ours left
        ring.push(4, r1(row));
        REQUIRE(row->r == 1);
    }                                         // destructor releases
    REQUIRE(row->r == 0);
    r0(row);
}

TEST_CASE("forEachAfter stops when the callback says so", "[ring]") {
    t2s::ResendRing ring(10);
    for (long long s = 1; s <= 6; ++s) ring.push(s, mkRow(s));
    int n = 0;
    ring.forEachAfter(2, [&](long long, K) { return ++n < 2; });
    REQUIRE(n == 2);
}
