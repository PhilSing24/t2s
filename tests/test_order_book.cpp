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

#include <map>
#include <random>
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

TEST_CASE("getL5 shows the top BOOK_DEPTH levels of a deeper snapshot", "[ordrbook][snapshot]") {
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

// ============================================================================
// Delta buffer cap
// ============================================================================

TEST_CASE("bufferDelta enforces the cap: drops the oldest and counts", "[ordrbook][buffer]") {
    OrderBookManager mgr({"BTCUSDT"});
    for (long long i = 1; i <= static_cast<long long>(MAX_DELTA_BUFFER_SIZE); ++i) {
        REQUIRE(mgr.bufferDelta(0, BufferedDelta{i, i, 0, {}, {}}));
    }
    REQUIRE(mgr.getDeltaBuffer(0).size() == MAX_DELTA_BUFFER_SIZE);
    REQUIRE(mgr.bufferOverflows() == 0);

    // Three more: each drops the oldest
    for (long long i = 1001; i <= 1003; ++i) {
        REQUIRE_FALSE(mgr.bufferDelta(0, BufferedDelta{i, i, 0, {}, {}}));
    }
    REQUIRE(mgr.getDeltaBuffer(0).size() == MAX_DELTA_BUFFER_SIZE);
    REQUIRE(mgr.bufferOverflows() == 3);
    REQUIRE(mgr.getDeltaBuffer(0).front().firstUpdateId == 4);
    REQUIRE(mgr.getDeltaBuffer(0).back().finalUpdateId == 1003);
}

TEST_CASE("After an overflow a fresh snapshot still syncs", "[ordrbook][buffer]") {
    OrderBookManager mgr({"BTCUSDT"});
    for (long long i = 1; i <= 1500; ++i) mgr.bufferDelta(0, BufferedDelta{i, i, 0, {}, {}});
    REQUIRE(mgr.bufferOverflows() == 500);          // deltas 1..500 dropped

    // Snapshot taken at update id 1200: newer than everything dropped
    mgr.applySnapshot(0, 1200, bids5(50000.0), asks5(50001.0));
    for (const auto& d : mgr.getDeltaBuffer(0)) {
        REQUIRE(mgr.applyDelta(0, d.firstUpdateId, d.finalUpdateId, d.bids, d.asks, d.eventTimeMs));
    }
    REQUIRE(mgr.isValid(0));
}

TEST_CASE("After an overflow a snapshot older than the buffer is rejected", "[ordrbook][buffer]") {
    OrderBookManager mgr({"BTCUSDT"});
    for (long long i = 1; i <= 1500; ++i) mgr.bufferDelta(0, BufferedDelta{i, i, 0, {}, {}});

    // Snapshot at 300, but deltas 301..500 were dropped: there is a hole
    mgr.applySnapshot(0, 300, bids5(50000.0), asks5(50001.0));
    const auto& first = mgr.getDeltaBuffer(0).front();
    REQUIRE(first.firstUpdateId == 501);
    REQUIRE_FALSE(mgr.applyDelta(0, first.firstUpdateId, first.finalUpdateId, {}, {}, 0));
    REQUIRE(mgr.getState(0) == BookState::INVALID);  // caller resyncs
}

// ============================================================================
// Full-depth book: deletes are refilled by the next real level
// ============================================================================

namespace {

std::vector<PriceLevel> bidsN(double best, int n) {
    std::vector<PriceLevel> v;
    for (int i = 0; i < n; ++i) v.push_back(pl(best - i, 1.0 + i));
    return v;
}
std::vector<PriceLevel> asksN(double best, int n) {
    std::vector<PriceLevel> v;
    for (int i = 0; i < n; ++i) v.push_back(pl(best + i, 1.0 + i));
    return v;
}
BookConfig smallCfg() {
    BookConfig c; c.snapshotLimit = 20; c.refreshLowWater = 8; c.maxLevels = 60; return c;
}

} // namespace

TEST_CASE("Deleting a top-5 level promotes the sixth level", "[ordrbook][depth]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    REQUIRE(mgr.knownLevels(0, true) == 10);

    // Delete the best bid and the third ask
    REQUIRE(mgr.applyDelta(0, 102, 102, {pl(100.0, 0.0)}, {pl(103.0, 0.0)}, 0));
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrice1 == 99.0);
    REQUIRE(q.bidPrice5 == 95.0);      // the old sixth level, not an empty slot
    REQUIRE(q.bidQty5 == 6.0);
    REQUIRE(q.askPrice3 == 104.0);
    REQUIRE(q.askPrice5 == 106.0);
}

TEST_CASE("A level inserted below the top five is kept and surfaces later", "[ordrbook][depth]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bidsN(100.0, 5), asksN(101.0, 5));
    REQUIRE(mgr.applyDelta(0, 101, 101, {pl(90.0, 7.0)}, {pl(120.0, 9.0)}, 0));
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice5 == 96.0);

    REQUIRE(mgr.applyDelta(0, 102, 102, {pl(98.0, 0.0)}, {pl(101.0, 0.0)}, 0));
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.bidPrice5 == 90.0);
    REQUIRE(q.bidQty5 == 7.0);
    REQUIRE(q.askPrice5 == 120.0);
}

TEST_CASE("A genuinely thin book publishes empty slots and stays valid", "[ordrbook][depth]") {
    OrderBookManager mgr({"BTCUSDT"});                 // limit 1000: 5 levels = whole book
    mgr.applySnapshot(0, 100, bids5(100.0), asks5(101.0));
    REQUIRE(mgr.applyDelta(0, 101, 101, {pl(100.0, 0.0)}, {}, 0));
    REQUIRE_FALSE(mgr.hasHorizon(0, true));
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrice4 == 96.0);
    REQUIRE(q.bidPrice5 == 0.0);                       // the exchange has no fifth bid
    REQUIRE(mgr.depthExhaustedEvents() == 0);
}

TEST_CASE("A truncated snapshot sets a horizon; levels beyond it are not tracked", "[ordrbook][horizon]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));   // 20 = limit: truncated
    REQUIRE(mgr.hasHorizon(0, true));
    REQUIRE(mgr.hasHorizon(0, false));

    // bid 81 is the horizon; 80 and 50 are beyond it, 81.5 is inside
    REQUIRE(mgr.applyDelta(0, 101, 101, {pl(80.0, 1.0), pl(50.0, 1.0), pl(81.5, 1.0)},
                                        {pl(121.0, 1.0), pl(119.5, 1.0)}, 0));
    REQUIRE(mgr.knownLevels(0, true) == 21);
    REQUIRE(mgr.knownLevels(0, false) == 21);

    // A snapshot with fewer levels than the limit is the whole book
    OrderBookManager whole({"BTCUSDT"}, smallCfg());
    whole.applySnapshot(0, 100, bidsN(100.0, 19), asksN(101.0, 19));
    REQUIRE_FALSE(whole.hasHorizon(0, true));
    REQUIRE(whole.applyDelta(0, 101, 101, {pl(10.0, 1.0)}, {}, 0));
    REQUIRE(whole.knownLevels(0, true) == 20);
}

TEST_CASE("Known depth running low asks for a refresh; the refresh is seamless", "[ordrbook][refresh]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    REQUIRE_FALSE(mgr.wantsRefresh(0));

    // The market falls: the 13 best bids disappear, 7 known levels remain
    std::vector<PriceLevel> del;
    for (int i = 0; i < 13; ++i) del.push_back(pl(100.0 - i, 0.0));
    REQUIRE(mgr.applyDelta(0, 102, 102, del, {}, 0));
    REQUIRE(mgr.knownLevels(0, true) == 7);
    REQUIRE(mgr.wantsRefresh(0));
    REQUIRE(mgr.getL5(0, 0, 0).isValid);
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice1 == 87.0);

    mgr.beginRefresh(0);
    REQUIRE_FALSE(mgr.wantsRefresh(0));                 // one refresh at a time

    // Deltas keep flowing while the snapshot is fetched; the book keeps publishing
    REQUIRE(mgr.applyDelta(0, 103, 103, {pl(87.0, 9.0)}, {}, 0));
    REQUIRE(mgr.applyDelta(0, 104, 104, {pl(86.0, 0.0)}, {}, 0));
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.getL5(0, 0, 0).isValid);

    // Snapshot taken at update id 103: 20 bids from 87 down to 68 (87 has qty 9)
    auto snapBids = bidsN(87.0, 20); snapBids[0].qty = 9.0;
    REQUIRE(mgr.onSnapshot(0, 103, snapBids, asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.depthRefreshes() == 1);
    REQUIRE(mgr.knownLevels(0, true) == 19);            // 20 from the snapshot, 86 deleted by delta 104
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrice1 == 87.0);  REQUIRE(q.bidQty1 == 9.0);
    REQUIRE(q.bidPrice2 == 85.0);                       // delta 104 was replayed onto the snapshot
    REQUIRE(q.bidPrice5 == 82.0);

    // The stream continues without a hiccup
    REQUIRE(mgr.applyDelta(0, 105, 105, {pl(85.0, 0.0)}, {}, 0));
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice2 == 84.0);
}

TEST_CASE("A refresh snapshot ahead of the stream is bridged by the next delta", "[ordrbook][refresh]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    mgr.beginRefresh(0);
    REQUIRE(mgr.applyDelta(0, 102, 102, {pl(100.0, 5.0)}, {}, 0));

    // Snapshot at id 110: newer than every delta we have seen
    REQUIRE(mgr.onSnapshot(0, 110, bidsN(99.0, 20), asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice1 == 99.0);

    REQUIRE(mgr.applyDelta(0, 103, 108, {pl(99.0, 0.0)}, {}, 0));   // older than the snapshot: skipped
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice1 == 99.0);
    REQUIRE(mgr.applyDelta(0, 109, 112, {pl(99.0, 3.0)}, {}, 0));   // bridges 111
    REQUIRE(mgr.getL5(0, 0, 0).bidQty1 == 3.0);
    REQUIRE_FALSE(mgr.applyDelta(0, 120, 121, {}, {}, 0));          // a real gap is still a gap
    REQUIRE(mgr.getState(0) == BookState::INVALID);
}

TEST_CASE("A refresh that cannot bridge leaves the live book untouched", "[ordrbook][refresh]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    mgr.beginRefresh(0);
    // 1200 deltas while waiting: the buffer overflows and loses 102..301
    for (long long id = 102; id <= 1301; ++id) REQUIRE(mgr.applyDelta(0, id, id, {pl(100.0, double(id))}, {}, 0));
    REQUIRE(mgr.bufferOverflows() == 200);

    // Snapshot at 150 falls into the hole
    REQUIRE(mgr.onSnapshot(0, 150, bidsN(50.0, 20), asksN(51.0, 20)) == SnapshotOutcome::REFRESH_FAILED);
    REQUIRE(mgr.refreshFailures() == 1);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice1 == 100.0);
    REQUIRE(mgr.getL5(0, 0, 0).bidQty1 == 1301.0);
    REQUIRE_FALSE(mgr.refreshPending(0));

    // cancelRefresh (failed HTTP fetch) also counts and stops the buffering
    mgr.beginRefresh(0);
    mgr.cancelRefresh(0);
    REQUIRE(mgr.refreshFailures() == 2);
    REQUIRE(mgr.getDeltaBuffer(0).empty());
}

TEST_CASE("Fewer known levels than the depth: the quote is invalid, not wrong", "[ordrbook][horizon]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));

    std::vector<PriceLevel> del;
    for (int i = 0; i < 16; ++i) del.push_back(pl(100.0 - i, 0.0));   // 4 known bids left
    REQUIRE(mgr.applyDelta(0, 102, 102, del, {}, 0));
    REQUIRE(mgr.isValid(0));                      // the sequence is intact
    REQUIRE(mgr.depthExhausted(0));
    REQUIRE(mgr.depthExhaustedEvents() == 1);
    L5Quote q = mgr.getL5(0, 0, 0);
    REQUIRE_FALSE(q.isValid);
    REQUIRE(q.bidPrice1 == 0.0);                  // carries no levels, like any invalid row

    // A refresh restores it
    mgr.beginRefresh(0);
    REQUIRE(mgr.onSnapshot(0, 102, bidsN(84.0, 20), asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.getL5(0, 0, 0).isValid);
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice5 == 80.0);
    REQUIRE(mgr.depthExhaustedEvents() == 1);
}

TEST_CASE("The stored book is capped; trimming moves the horizon in", "[ordrbook][horizon]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());                    // maxLevels 60
    mgr.applySnapshot(0, 100, bidsN(100.0, 10), asksN(1000.0, 10));   // whole book, no horizon
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    std::vector<PriceLevel> ins;
    for (int i = 1; i <= 80; ++i) ins.push_back(pl(100.0 + i, 1.0));  // market rallies
    REQUIRE(mgr.applyDelta(0, 102, 102, ins, {}, 0));
    REQUIRE(mgr.knownLevels(0, true) == 60);
    REQUIRE(mgr.hasHorizon(0, true));
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice1 == 180.0);
    // 121 is the worst kept bid; an insert below it is now beyond the horizon
    REQUIRE(mgr.applyDelta(0, 103, 103, {pl(95.0, 1.0)}, {}, 0));
    REQUIRE(mgr.knownLevels(0, true) == 60);
}

TEST_CASE("onSnapshot performs the initial sync with the buffered deltas", "[ordrbook][sync]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.bufferDelta(0, BufferedDelta{95, 99, 0, {}, {}});                    // stale
    mgr.bufferDelta(0, BufferedDelta{100, 102, 0, {pl(100.0, 8.0)}, {}});    // bridges 101
    mgr.bufferDelta(0, BufferedDelta{103, 104, 0, {pl(99.0, 0.0)}, {}});
    REQUIRE(mgr.onSnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::SYNCED);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.getL5(0, 0, 0).bidQty1 == 8.0);
    REQUIRE(mgr.getL5(0, 0, 0).bidPrice2 == 98.0);
    REQUIRE(mgr.getDeltaBuffer(0).empty());

    OrderBookManager waiting({"BTCUSDT"});
    waiting.bufferDelta(0, BufferedDelta{90, 95, 0, {}, {}});
    REQUIRE(waiting.onSnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::AWAITING_BRIDGE);
    REQUIRE(waiting.getState(0) == BookState::SYNCING);

    OrderBookManager tooOld({"BTCUSDT"});
    tooOld.bufferDelta(0, BufferedDelta{200, 205, 0, {}, {}});
    REQUIRE(tooOld.onSnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::SYNC_FAILED);
    REQUIRE(tooOld.getState(0) == BookState::INVALID);

    // A snapshot nobody asked for is ignored
    REQUIRE(mgr.onSnapshot(0, 500, bidsN(1.0, 10), asksN(2.0, 10)) == SnapshotOutcome::IGNORED);
    REQUIRE(mgr.getL5(0, 0, 0).bidQty1 == 8.0);
}

// ----------------------------------------------------------------------------
// Property: whenever the quote is valid, its five levels are the exchange's.
// ----------------------------------------------------------------------------

TEST_CASE("Randomised market: a valid L5 always equals the exchange's top five", "[ordrbook][property]") {
    for (unsigned seed : {1u, 2u, 3u, 4u, 5u}) {
        std::mt19937 rng(seed);
        auto rnd = [&](int lo, int hi) { return std::uniform_int_distribution<int>(lo, hi)(rng); };

        // The "exchange": integer prices, bids below mid, asks above
        std::map<double, double> tb, ta;
        int mid = 10000;
        for (int i = 1; i <= 200; ++i) { tb[mid - i] = rnd(1, 9); ta[mid + i] = rnd(1, 9); }
        long long id = 1000;

        auto snapshot = [&](std::size_t limit, std::vector<PriceLevel>& b, std::vector<PriceLevel>& a) {
            b.clear(); a.clear();
            for (auto it = tb.rbegin(); it != tb.rend() && b.size() < limit; ++it) b.push_back(pl(it->first, it->second));
            for (auto it = ta.begin(); it != ta.end() && a.size() < limit; ++it) a.push_back(pl(it->first, it->second));
        };

        BookConfig cfg = smallCfg();
        OrderBookManager mgr({"X"}, cfg);
        std::vector<PriceLevel> sb, sa;
        snapshot(cfg.snapshotLimit, sb, sa);
        REQUIRE(mgr.onSnapshot(0, id, sb, sa) == SnapshotOutcome::AWAITING_BRIDGE);

        bool pending = false; int deliverIn = 0; long long snapId = 0;
        std::vector<PriceLevel> pb, pa;
        long long validRows = 0, invalidRows = 0, deletes = 0, refreshes = 0;

        for (int step = 0; step < 20000; ++step) {
            // One event: a handful of level changes near the touch, with drift
            std::vector<PriceLevel> eb, ea;
            int n = rnd(1, 6);
            if (step % 500 < 60) mid += (step / 500) % 2 ? 2 : -2;      // trending bursts
            for (int k = 0; k < n; ++k) {
                bool isBid = rnd(0, 1);
                double price = isBid ? mid - rnd(1, 40) : mid + rnd(1, 40);
                double qty = rnd(0, 3) == 0 ? 0.0 : rnd(1, 9);
                auto& side = isBid ? tb : ta;
                if (qty == 0.0) { deletes += side.erase(price); } else { side[price] = qty; }
                (isBid ? eb : ea).push_back(pl(price, qty));
            }
            // a trend also removes whatever the price crossed
            for (auto it = tb.upper_bound(mid - 1); it != tb.end();) { eb.push_back(pl(it->first, 0.0)); it = tb.erase(it); ++deletes; }
            for (auto it = ta.begin(); it != ta.end() && it->first <= mid;) { ea.push_back(pl(it->first, 0.0)); it = ta.erase(it); ++deletes; }

            long long U = id + 1; id += rnd(1, 3);
            REQUIRE(mgr.applyDelta(0, U, id, eb, ea, step));

            if (pending && --deliverIn <= 0) {
                REQUIRE(mgr.onSnapshot(0, snapId, pb, pa) == SnapshotOutcome::REFRESHED);
                pending = false; ++refreshes;
            }
            if (!pending && mgr.wantsRefresh(0)) {
                mgr.beginRefresh(0);
                pending = true; deliverIn = rnd(1, 6);
                snapshot(cfg.snapshotLimit, pb, pa); snapId = id;       // taken now, delivered later
            }

            L5Quote q = mgr.getL5(0, 0, 0);
            if (!q.isValid) { ++invalidRows; continue; }
            ++validRows;
            double bp[5] = {q.bidPrice1, q.bidPrice2, q.bidPrice3, q.bidPrice4, q.bidPrice5};
            double bq[5] = {q.bidQty1, q.bidQty2, q.bidQty3, q.bidQty4, q.bidQty5};
            double ap[5] = {q.askPrice1, q.askPrice2, q.askPrice3, q.askPrice4, q.askPrice5};
            double aq[5] = {q.askQty1, q.askQty2, q.askQty3, q.askQty4, q.askQty5};
            auto bi = tb.rbegin(); auto ai = ta.begin();
            for (int l = 0; l < 5; ++l) {
                double ebp = 0, ebq = 0, eap = 0, eaq = 0;
                if (bi != tb.rend()) { ebp = bi->first; ebq = bi->second; ++bi; }
                if (ai != ta.end())  { eap = ai->first; eaq = ai->second; ++ai; }
                if (bp[l] != ebp || bq[l] != ebq || ap[l] != eap || aq[l] != eaq) {
                    FAIL("seed " << seed << " step " << step << " level " << l + 1
                         << ": book " << bp[l] << "x" << bq[l] << " / " << ap[l] << "x" << aq[l]
                         << " exchange " << ebp << "x" << ebq << " / " << eap << "x" << eaq);
                }
            }
        }
        INFO("seed " << seed << " valid " << validRows << " invalid " << invalidRows
             << " deletes " << deletes << " refreshes " << refreshes);
        REQUIRE(deletes > 5000);
        REQUIRE(refreshes > 10);
        REQUIRE(validRows > 19000);
    }
}
