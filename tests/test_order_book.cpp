/**
 * @file test_order_book.cpp
 * @brief Unit tests for OrderBookManager.
 *
 * Tests the state machine (INIT -> SYNCING -> VALID -> INVALID),
 * snapshot ingestion, delta application semantics (insert/update/delete),
 * sequence-gap detection, and L5 extraction.
 *
 * Run via the standard test runner (./tests/run_tests.sh) or directly:
 *   ./build/test_order_book
 */

#include "order_book_manager.hpp"
#include "catch_amalgamated.hpp"

#include <vector>
#include <string>

namespace {

// Helpers to build PriceLevel vectors concisely.
PriceLevel pl(double price, double qty) {
    return PriceLevel{price, qty};
}

std::vector<PriceLevel> bids5(double basePrice) {
    // Decreasing bids starting from basePrice
    return {pl(basePrice,        1.0),
            pl(basePrice - 1.0,  2.0),
            pl(basePrice - 2.0,  3.0),
            pl(basePrice - 3.0,  4.0),
            pl(basePrice - 4.0,  5.0)};
}

std::vector<PriceLevel> asks5(double basePrice) {
    // Increasing asks starting from basePrice
    return {pl(basePrice,        1.0),
            pl(basePrice + 1.0,  2.0),
            pl(basePrice + 2.0,  3.0),
            pl(basePrice + 3.0,  4.0),
            pl(basePrice + 4.0,  5.0)};
}

} // namespace

// ============================================================================
// Construction & symbol mapping
// ============================================================================

TEST_CASE("OrderBookManager construction with multiple symbols", "[ordrbook]") {
    OrderBookManager mgr({"BTCUSDT", "ETHUSDT", "SOLUSDT"});

    REQUIRE(mgr.numSymbols() == 3);
    REQUIRE(mgr.getSymbolIndex("BTCUSDT") == 0);
    REQUIRE(mgr.getSymbolIndex("ETHUSDT") == 1);
    REQUIRE(mgr.getSymbolIndex("SOLUSDT") == 2);
    REQUIRE(mgr.getSymbolIndex("UNKNOWN") == -1);

    REQUIRE(mgr.getSymbol(0) == "BTCUSDT");
    REQUIRE(mgr.getSymbol(2) == "SOLUSDT");
}

// ============================================================================
// State machine
// ============================================================================

TEST_CASE("Fresh book starts in INIT state and needs snapshot", "[ordrbook][state]") {
    OrderBookManager mgr({"BTCUSDT"});

    REQUIRE(mgr.getState(0) == BookState::INIT);
    REQUIRE(mgr.needsSnapshot(0));
    REQUIRE_FALSE(mgr.isValid(0));
}

TEST_CASE("applySnapshot moves state to SYNCING", "[ordrbook][state]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));

    REQUIRE(mgr.getState(0) == BookState::SYNCING);
    REQUIRE_FALSE(mgr.isValid(0));
}

TEST_CASE("First delta after snapshot transitions to VALID", "[ordrbook][state]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));

    // First valid delta: U <= 101 <= u (101 = snapshotUpdateId + 1)
    bool ok = mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL);

    REQUIRE(ok);
    REQUIRE(mgr.getState(0) == BookState::VALID);
    REQUIRE(mgr.isValid(0));
}

TEST_CASE("reset() returns book to INIT", "[ordrbook][state]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);
    REQUIRE(mgr.isValid(0));

    mgr.reset(0);
    REQUIRE(mgr.getState(0) == BookState::INIT);
    REQUIRE(mgr.needsSnapshot(0));
}

// ============================================================================
// L5 extraction
// ============================================================================

TEST_CASE("getL5 returns correct prices and qtys after snapshot", "[ordrbook][l5]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);  // -> VALID

    L5Quote q = mgr.getL5(0, 1700000000123LL, 42);

    REQUIRE(q.sym == "BTCUSDT");
    REQUIRE(q.isValid);
    REQUIRE(q.fhRecvTimeUtcNs == 1700000000123LL);
    REQUIRE(q.fhSeqNo == 42);

    // Best bid, best ask
    REQUIRE(q.bidPrice1 == 50000.0);
    REQUIRE(q.bidQty1 == 1.0);
    REQUIRE(q.askPrice1 == 50001.0);
    REQUIRE(q.askQty1 == 1.0);

    // Deeper levels
    REQUIRE(q.bidPrice5 == 49996.0);
    REQUIRE(q.bidQty5 == 5.0);
    REQUIRE(q.askPrice5 == 50005.0);
    REQUIRE(q.askQty5 == 5.0);
}

TEST_CASE("getL5 reports isValid=false in non-VALID states", "[ordrbook][l5]") {
    OrderBookManager mgr({"BTCUSDT"});
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE_FALSE(q.isValid);  // INIT

    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    q = mgr.getL5(0, 0, 0);
    REQUIRE_FALSE(q.isValid);  // SYNCING
}

// ============================================================================
// Snapshot semantics
// ============================================================================

TEST_CASE("Snapshot truncates to BOOK_DEPTH levels", "[ordrbook][snapshot]") {
    OrderBookManager mgr({"BTCUSDT"});

    // Build 10 bids and 10 asks - manager should keep only top 5
    std::vector<PriceLevel> tenBids;
    std::vector<PriceLevel> tenAsks;
    for (int i = 0; i < 10; ++i) {
        tenBids.push_back(pl(50000.0 - i, 1.0 + i));
        tenAsks.push_back(pl(50001.0 + i, 1.0 + i));
    }
    mgr.applySnapshot(0, 100, tenBids, tenAsks);
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidPrice1 == 50000.0);
    REQUIRE(q.bidPrice5 == 49996.0);  // 50000 - 4, the 5th level
    // Levels 6-10 are gone; we keep only 5
}

TEST_CASE("Snapshot with fewer than 5 levels leaves rest empty", "[ordrbook][snapshot]") {
    OrderBookManager mgr({"BTCUSDT"});

    std::vector<PriceLevel> threeBids = {
        pl(50000.0, 1.0), pl(49999.0, 2.0), pl(49998.0, 3.0)
    };
    std::vector<PriceLevel> threeAsks = {
        pl(50001.0, 1.0), pl(50002.0, 2.0), pl(50003.0, 3.0)
    };
    mgr.applySnapshot(0, 100, threeBids, threeAsks);
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidPrice1 == 50000.0);
    REQUIRE(q.bidPrice3 == 49998.0);
    REQUIRE(q.bidPrice4 == 0.0);  // empty
    REQUIRE(q.bidQty4 == 0.0);
    REQUIRE(q.bidPrice5 == 0.0);
}

// ============================================================================
// Delta application semantics
// ============================================================================

TEST_CASE("Delta updates qty at existing price level", "[ordrbook][delta]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);

    // Update best bid qty from 1.0 to 7.5
    bool ok = mgr.applyDelta(0, 102, 102,
                             {pl(50000.0, 7.5)},  // bid update at existing price
                             {},
                             1700000001000LL);
    REQUIRE(ok);

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidPrice1 == 50000.0);
    REQUIRE(q.bidQty1 == 7.5);
    // Other levels untouched
    REQUIRE(q.bidPrice2 == 49999.0);
    REQUIRE(q.bidQty2 == 2.0);
}

TEST_CASE("Delta with qty=0 deletes a price level", "[ordrbook][delta]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);

    // Delete the best bid (50000.0)
    bool ok = mgr.applyDelta(0, 102, 102,
                             {pl(50000.0, 0.0)},  // qty=0 means delete
                             {},
                             1700000001000LL);
    REQUIRE(ok);

    L5Quote q = mgr.getL5(0, 0, 0);
    // Levels should shift up: old 49999 is now best
    REQUIRE(q.bidPrice1 == 49999.0);
    REQUIRE(q.bidQty1 == 2.0);
    REQUIRE(q.bidPrice4 == 49996.0);
    REQUIRE(q.bidQty4 == 5.0);
    // Last slot now empty
    REQUIRE(q.bidPrice5 == 0.0);
    REQUIRE(q.bidQty5 == 0.0);
}

TEST_CASE("Delta inserts new level at correct position", "[ordrbook][delta]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);

    // Insert a new bid better than current best
    bool ok = mgr.applyDelta(0, 102, 102,
                             {pl(50001.0, 9.0)},  // higher than 50000
                             {},
                             1700000001000LL);
    REQUIRE(ok);

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidPrice1 == 50001.0);  // new best
    REQUIRE(q.bidQty1 == 9.0);
    REQUIRE(q.bidPrice2 == 50000.0);  // shifted down
    REQUIRE(q.bidQty2 == 1.0);
    // The bottom level is shifted out (was 49996 with qty 5)
    REQUIRE(q.bidPrice5 == 49997.0);
    REQUIRE(q.bidQty5 == 4.0);
}

// ============================================================================
// Sequence gap detection
// ============================================================================

TEST_CASE("Sequence gap during VALID transitions to INVALID", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);  // VALID, lastUpdateId=101

    // Skip ahead - 105 instead of expected 102
    bool ok = mgr.applyDelta(0, 105, 110, {}, {}, 1700000001000LL);

    REQUIRE_FALSE(ok);
    REQUIRE(mgr.getState(0) == BookState::INVALID);
}

TEST_CASE("Snapshot too old triggers INVALID on first delta", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    // Snapshot has lastUpdateId=100, so first delta needs U <= 101 <= u.
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));

    // Delta starts at 200 - too far ahead
    bool ok = mgr.applyDelta(0, 200, 210, {}, {}, 1700000000000LL);

    REQUIRE_FALSE(ok);
    REQUIRE(mgr.getState(0) == BookState::INVALID);
}

TEST_CASE("Stale delta after snapshot is silently skipped", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));

    // Delta entirely before snapshot+1: u < 101. Should return true (skipped) but NOT transition.
    bool ok = mgr.applyDelta(0, 50, 80, {}, {}, 1700000000000LL);

    REQUIRE(ok);
    REQUIRE(mgr.getState(0) == BookState::SYNCING);  // still waiting for valid first delta
}

TEST_CASE("Cannot apply delta in INIT state", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});

    bool ok = mgr.applyDelta(0, 1, 5, {}, {}, 1700000000000LL);

    REQUIRE_FALSE(ok);
    REQUIRE(mgr.getState(0) == BookState::INIT);  // unchanged
}

// ============================================================================
// Multi-symbol independence
// ============================================================================

TEST_CASE("Symbols maintain independent state", "[ordrbook][multi]") {
    OrderBookManager mgr({"BTCUSDT", "ETHUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);
    // ETHUSDT untouched

    REQUIRE(mgr.isValid(0));
    REQUIRE_FALSE(mgr.isValid(1));
    REQUIRE(mgr.getState(1) == BookState::INIT);

    // Now snapshot ETHUSDT and confirm BTCUSDT still valid
    mgr.applySnapshot(1, 200, bids5(2000.0), asks5(2001.0));
    mgr.applyDelta(1, 201, 201, {}, {}, 1700000001000LL);

    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.isValid(1));

    L5Quote qBtc = mgr.getL5(0, 0, 0);
    L5Quote qEth = mgr.getL5(1, 0, 0);
    REQUIRE(qBtc.bidPrice1 == 50000.0);
    REQUIRE(qEth.bidPrice1 == 2000.0);
}

// ============================================================================
// Sequence overlap and stale-event handling (Binance spec compliance)
// ============================================================================
//
// Binance Spot Diff Depth Stream allows events that overlap with the last
// applied update id, or that are entirely stale (u < lastUpdateId). The
// spec only mandates re-sync when U > lastUpdateId + 1 (true gap). These
// tests pin down the VALID-state continuity check's behavior in those cases.

TEST_CASE("VALID accepts overlapping delta (U <= lastUpdateId)", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));  // -> VALID, lastUpdateId=105

    // Overlap: U=103 <= 105 (lastUpdateId), u=110 > 105.
    // Per spec this should apply, NOT invalidate.
    bool ok = mgr.applyDelta(0, 103, 110,
                             {pl(50000.0, 7.5)},  // change best bid qty
                             {},
                             1700000001000LL);

    REQUIRE(ok);
    REQUIRE(mgr.getState(0) == BookState::VALID);  // not invalidated

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidQty1 == 7.5);  // overwrite was applied
}

TEST_CASE("VALID accepts boundary overlap (U == lastUpdateId)", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));

    // Minimum overlap: U == lastUpdateId.
    bool ok = mgr.applyDelta(0, 105, 108, {}, {}, 1700000001000LL);

    REQUIRE(ok);
    REQUIRE(mgr.getState(0) == BookState::VALID);
}

TEST_CASE("VALID silently skips entirely-stale delta (u < lastUpdateId)", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));

    // Stale: u=90 < 105 (lastUpdateId). Spec: "If u < lastUpdateId, ignore."
    // The delta tries to change a price level - that change MUST NOT apply.
    bool ok = mgr.applyDelta(0, 80, 90,
                             {pl(50000.0, 99.0)},  // would-be poison if applied
                             {},
                             1700000001000LL);

    REQUIRE(ok);  // returns true (not a failure)
    REQUIRE(mgr.getState(0) == BookState::VALID);

    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidQty1 == 1.0);  // unchanged - poison correctly ignored
}

TEST_CASE("VALID applies boundary u == lastUpdateId", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));

    // Boundary: u == lastUpdateId. Spec uses strict < for the stale rule,
    // so this is NOT stale - it should apply (harmlessly overwriting
    // levels with their state as of update 105, which is what we have).
    bool ok = mgr.applyDelta(0, 80, 105, {}, {}, 1700000001000LL);

    REQUIRE(ok);
    REQUIRE(mgr.getState(0) == BookState::VALID);
}

TEST_CASE("VALID invalidates on minimal gap (U == lastUpdateId + 2)", "[ordrbook][gap]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));

    // Smallest gap: U=107 = lastUpdateId(105) + 2. Spec mandates invalidation.
    bool ok = mgr.applyDelta(0, 107, 110, {}, {}, 1700000001000LL);

    REQUIRE_FALSE(ok);
    REQUIRE(mgr.getState(0) == BookState::INVALID);
}

TEST_CASE("Buffered replay tolerates overlapping deltas after snapshot", "[ordrbook][gap]") {
    // Regression: after snapshot lands, the FH replays buffered deltas via
    // applyDelta in order. The first hits SYNCING (correct). The 2nd-Nth
    // hit VALID. If any of those overlap, the pre-fix strict-equality check
    // would abort replay halfway. This test simulates that flow.
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));

    // Delta 1 (SYNCING -> VALID, lastUpdateId 100 -> 105)
    REQUIRE(mgr.applyDelta(0, 101, 105, {}, {}, 1700000000000LL));
    REQUIRE(mgr.isValid(0));

    // Delta 2 - overlaps delta 1's final (would have aborted pre-fix)
    REQUIRE(mgr.applyDelta(0, 104, 110, {}, {}, 1700000000100LL));
    REQUIRE(mgr.isValid(0));

    // Delta 3 - overlaps delta 2's final
    REQUIRE(mgr.applyDelta(0, 108, 115, {}, {}, 1700000000200LL));
    REQUIRE(mgr.isValid(0));

    // Delta 4 - contiguous from delta 3
    REQUIRE(mgr.applyDelta(0, 116, 120, {}, {}, 1700000000300LL));
    REQUIRE(mgr.isValid(0));
}
