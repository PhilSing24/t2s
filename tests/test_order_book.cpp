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

TEST_CASE("getQuote returns correct prices and qtys after snapshot", "[ordrbook][l5]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    mgr.applyDelta(0, 101, 101, {}, {}, 1700000000000LL);  // -> VALID

    BookQuote q = mgr.getQuote(0, 1700000000123LL, 42);

    REQUIRE(q.sym == "BTCUSDT");
    REQUIRE(q.isValid);
    REQUIRE(q.fhRecvTimeUtcNs == 1700000000123LL);
    REQUIRE(q.fhSeqNo == 42);

    // Best bid, best ask
    REQUIRE(q.bidPrices[0] == 50000.0);
    REQUIRE(q.bidQtys[0] == 1.0);
    REQUIRE(q.askPrices[0] == 50001.0);
    REQUIRE(q.askQtys[0] == 1.0);

    // Deeper levels
    REQUIRE(q.bidPrices[4] == 49996.0);
    REQUIRE(q.bidQtys[4] == 5.0);
    REQUIRE(q.askPrices[4] == 50005.0);
    REQUIRE(q.askQtys[4] == 5.0);
}

TEST_CASE("getQuote reports isValid=false in non-VALID states", "[ordrbook][l5]") {
    OrderBookManager mgr({"BTCUSDT"});
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE_FALSE(q.isValid);  // INIT

    mgr.applySnapshot(0, 100, bids5(50000.0), asks5(50001.0));
    q = mgr.getQuote(0, 0, 0);
    REQUIRE_FALSE(q.isValid);  // SYNCING
}

// ============================================================================
// Snapshot semantics
// ============================================================================

TEST_CASE("getQuote shows the top five levels of a deeper snapshot", "[ordrbook][snapshot]") {
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[0] == 50000.0);
    REQUIRE(q.bidPrices[4] == 49996.0);  // 50000 - 4, the 5th level
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[0] == 50000.0);
    REQUIRE(q.bidPrices[2] == 49998.0);
    REQUIRE(q.bidPrices[3] == 0.0);  // empty
    REQUIRE(q.bidQtys[3] == 0.0);
    REQUIRE(q.bidPrices[4] == 0.0);
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[0] == 50000.0);
    REQUIRE(q.bidQtys[0] == 7.5);
    // Other levels untouched
    REQUIRE(q.bidPrices[1] == 49999.0);
    REQUIRE(q.bidQtys[1] == 2.0);
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    // Levels should shift up: old 49999 is now best
    REQUIRE(q.bidPrices[0] == 49999.0);
    REQUIRE(q.bidQtys[0] == 2.0);
    REQUIRE(q.bidPrices[3] == 49996.0);
    REQUIRE(q.bidQtys[3] == 5.0);
    // Last slot now empty
    REQUIRE(q.bidPrices[4] == 0.0);
    REQUIRE(q.bidQtys[4] == 0.0);
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[0] == 50001.0);  // new best
    REQUIRE(q.bidQtys[0] == 9.0);
    REQUIRE(q.bidPrices[1] == 50000.0);  // shifted down
    REQUIRE(q.bidQtys[1] == 1.0);
    // The bottom level is shifted out (was 49996 with qty 5)
    REQUIRE(q.bidPrices[4] == 49997.0);
    REQUIRE(q.bidQtys[4] == 4.0);
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

    BookQuote qBtc = mgr.getQuote(0, 0, 0);
    BookQuote qEth = mgr.getQuote(1, 0, 0);
    REQUIRE(qBtc.bidPrices[0] == 50000.0);
    REQUIRE(qEth.bidPrices[0] == 2000.0);
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidQtys[0] == 7.5);  // overwrite was applied
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

    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidQtys[0] == 1.0);  // unchanged - poison correctly ignored
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
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrices[0] == 99.0);
    REQUIRE(q.bidPrices[4] == 95.0);      // the old sixth level, not an empty slot
    REQUIRE(q.bidQtys[4] == 6.0);
    REQUIRE(q.askPrices[2] == 104.0);
    REQUIRE(q.askPrices[4] == 106.0);
}

TEST_CASE("A level inserted below the top five is kept and surfaces later", "[ordrbook][depth]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.applySnapshot(0, 100, bidsN(100.0, 5), asksN(101.0, 5));
    REQUIRE(mgr.applyDelta(0, 101, 101, {pl(90.0, 7.0)}, {pl(120.0, 9.0)}, 0));
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[4] == 96.0);

    REQUIRE(mgr.applyDelta(0, 102, 102, {pl(98.0, 0.0)}, {pl(101.0, 0.0)}, 0));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[4] == 90.0);
    REQUIRE(q.bidQtys[4] == 7.0);
    REQUIRE(q.askPrices[4] == 120.0);
}

TEST_CASE("A genuinely thin book publishes empty slots and stays valid", "[ordrbook][depth]") {
    OrderBookManager mgr({"BTCUSDT"});                 // limit 1000: 5 levels = whole book
    mgr.applySnapshot(0, 100, bids5(100.0), asks5(101.0));
    REQUIRE(mgr.applyDelta(0, 101, 101, {pl(100.0, 0.0)}, {}, 0));
    REQUIRE_FALSE(mgr.hasHorizon(0, true));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrices[3] == 96.0);
    REQUIRE(q.bidPrices[4] == 0.0);                       // the exchange has no fifth bid
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
    REQUIRE(mgr.getQuote(0, 0, 0).isValid);
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[0] == 87.0);

    mgr.beginRefresh(0);
    REQUIRE_FALSE(mgr.wantsRefresh(0));                 // one refresh at a time

    // Deltas keep flowing while the snapshot is fetched; the book keeps publishing
    REQUIRE(mgr.applyDelta(0, 103, 103, {pl(87.0, 9.0)}, {}, 0));
    REQUIRE(mgr.applyDelta(0, 104, 104, {pl(86.0, 0.0)}, {}, 0));
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.getQuote(0, 0, 0).isValid);

    // Snapshot taken at update id 103: 20 bids from 87 down to 68 (87 has qty 9)
    auto snapBids = bidsN(87.0, 20); snapBids[0].qty = 9.0;
    REQUIRE(mgr.onSnapshot(0, 103, snapBids, asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.depthRefreshes() == 1);
    REQUIRE(mgr.knownLevels(0, true) == 19);            // 20 from the snapshot, 86 deleted by delta 104
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrices[0] == 87.0);  REQUIRE(q.bidQtys[0] == 9.0);
    REQUIRE(q.bidPrices[1] == 85.0);                       // delta 104 was replayed onto the snapshot
    REQUIRE(q.bidPrices[4] == 82.0);

    // The stream continues without a hiccup
    REQUIRE(mgr.applyDelta(0, 105, 105, {pl(85.0, 0.0)}, {}, 0));
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[1] == 84.0);
}

TEST_CASE("Default low-water mark: the refresh starts at half the snapshot, early enough for a fast move", "[ordrbook][refresh]") {
    // Seen live on 2026-10-05 (BTC futures) with the mark at 100 levels: one
    // event took a side from above the mark to almost nothing, the refresh
    // was requested only then, and the next event exhausted the side before
    // the snapshot arrived. With the mark at 500 the refresh is already in
    // flight when such a move comes.
    OrderBookManager mgr({"BTCUSDT"});                       // defaults: limit 1000
    REQUIRE(mgr.refreshLowWater() == 500);
    mgr.applySnapshot(0, 100, bidsN(100000.0, 1000), asksN(100001.0, 1000));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    REQUIRE(mgr.hasHorizon(0, true));
    REQUIRE_FALSE(mgr.wantsRefresh(0));

    auto fall = [&](long long id, double fromBest, int levels) {
        std::vector<PriceLevel> del;
        for (int i = 0; i < levels; ++i) del.push_back(pl(fromBest - i, 0.0));
        REQUIRE(mgr.applyDelta(0, id, id, del, {}, 0));
    };
    fall(102, 100000.0, 500);                                // drift: 500 known bids left, at the mark
    REQUIRE(mgr.knownLevels(0, true) == 500);
    REQUIRE_FALSE(mgr.wantsRefresh(0));
    fall(103, 99500.0, 1);                                   // 499: below the mark
    REQUIRE(mgr.wantsRefresh(0));
    mgr.beginRefresh(0);

    // The fast move while the snapshot is being fetched: 200 levels in two events
    fall(104, 99499.0, 100);
    fall(105, 99399.0, 100);
    REQUIRE(mgr.knownLevels(0, true) == 299);
    REQUIRE(mgr.getQuote(0, 0, 0).isValid);
    REQUIRE(mgr.depthExhaustedEvents() == 0);                // with the mark at 100 this was an invalid row

    // Snapshot taken at update id 104 lands; event 105 is replayed onto it
    REQUIRE(mgr.onSnapshot(0, 104, bidsN(99399.0, 1000), asksN(100001.0, 1000)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.knownLevels(0, true) == 900);
    REQUIRE_FALSE(mgr.wantsRefresh(0));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrices[0] == 99299.0);
    REQUIRE(mgr.depthExhaustedEvents() == 0);
}

TEST_CASE("The low-water mark never exceeds half the snapshot limit", "[ordrbook][refresh]") {
    BookConfig c; c.snapshotLimit = 100;                     // default mark 500 would never be satisfied
    OrderBookManager mgr({"BTCUSDT"}, c);
    REQUIRE(mgr.refreshLowWater() == 50);
    mgr.applySnapshot(0, 100, bidsN(100.0, 100), asksN(101.0, 100));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    REQUIRE_FALSE(mgr.wantsRefresh(0));                      // a fresh snapshot is above the mark

    BookConfig small; small.snapshotLimit = 20; small.refreshLowWater = 8;
    REQUIRE(OrderBookManager({"BTCUSDT"}, small).refreshLowWater() == 8);   // an explicit lower mark is kept
}

TEST_CASE("A refresh snapshot ahead of the stream waits for its bridging delta", "[ordrbook][refresh]") {
    OrderBookManager mgr({"BTCUSDT"}, smallCfg());
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    mgr.beginRefresh(0);
    REQUIRE(mgr.applyDelta(0, 102, 102, {pl(100.0, 5.0)}, {}, 0));

    // Snapshot at id 110: newer than every delta we have seen. It is held
    // as a shadow; the live book stays on the stream.
    REQUIRE(mgr.onSnapshot(0, 110, bidsN(99.0, 20), asksN(101.0, 20)) == SnapshotOutcome::REFRESH_AWAITING_BRIDGE);
    REQUIRE(mgr.isValid(0));
    REQUIRE(mgr.depthRefreshes() == 0);
    REQUIRE(mgr.refreshPending(0));
    REQUIRE_FALSE(mgr.wantsRefresh(0));
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[0] == 100.0);
    REQUIRE(mgr.getQuote(0, 0, 0).exchUpdateId == 102);

    REQUIRE(mgr.applyDelta(0, 103, 108, {pl(100.0, 6.0)}, {}, 0));   // live applies it; older than the snapshot for the shadow
    REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 6.0);
    REQUIRE(mgr.depthRefreshes() == 0);

    REQUIRE(mgr.applyDelta(0, 109, 112, {pl(99.0, 3.0)}, {}, 0));    // bridges 111: shadow swapped in
    REQUIRE(mgr.depthRefreshes() == 1);
    REQUIRE_FALSE(mgr.refreshPending(0));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidPrices[0] == 99.0);                                 // the snapshot's book (100 was gone by id 110)
    REQUIRE(q.bidQtys[0] == 3.0);
    REQUIRE(q.exchUpdateId == 112);
    REQUIRE_FALSE(mgr.applyDelta(0, 120, 121, {}, {}, 0));           // a real gap is still a gap
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
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[0] == 100.0);
    REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 1301.0);
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
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE_FALSE(q.isValid);
    REQUIRE(q.bidPrices[0] == 0.0);                  // carries no levels, like any invalid row

    // A refresh restores it
    mgr.beginRefresh(0);
    REQUIRE(mgr.onSnapshot(0, 102, bidsN(84.0, 20), asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    REQUIRE(mgr.getQuote(0, 0, 0).isValid);
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[4] == 80.0);
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
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[0] == 180.0);
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
    REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 8.0);
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[1] == 98.0);
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
    REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 8.0);
}

// ----------------------------------------------------------------------------
// Property: whenever the quote is valid, its five levels are the exchange's.
// ----------------------------------------------------------------------------

TEST_CASE("Randomised market: a valid L5 always equals the exchange's top five", "[ordrbook][property]") {
    for (t2s::DepthSync sync : {t2s::DepthSync::Spot, t2s::DepthSync::Futures})
    for (unsigned seed : {1u, 2u, 3u, 4u, 5u}) {
        const bool fut = (sync == t2s::DepthSync::Futures);
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
        cfg.sync = sync;
        OrderBookManager mgr({"X"}, cfg);
        std::vector<PriceLevel> sb, sa;
        snapshot(cfg.snapshotLimit, sb, sa);
        REQUIRE(mgr.onSnapshot(0, id, sb, sa) == SnapshotOutcome::AWAITING_BRIDGE);

        bool pending = false; int deliverIn = 0; long long snapId = 0;
        std::vector<PriceLevel> pb, pa;
        long long validRows = 0, invalidRows = 0, deletes = 0, refreshes = 0;
        long long prevQuoteU = 0; bool havePrev = false;

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

            // Spot ids are consecutive; futures ids jump and each event names its predecessor
            long long prevU = id;
            long long U = fut ? id + rnd(1, 4) : id + 1;
            id = fut ? U + rnd(0, 3) : id + rnd(1, 3);
            BufferedDelta ev{U, id, step, eb, ea};
            if (fut) ev.prevFinalUpdateId = prevU;
            REQUIRE(mgr.applyDelta(0, ev));

            if (pending && --deliverIn <= 0) {
                SnapshotOutcome o = mgr.onSnapshot(0, snapId, pb, pa);
                REQUIRE((o == SnapshotOutcome::REFRESHED || o == SnapshotOutcome::REFRESH_AWAITING_BRIDGE));
                pending = false; ++refreshes;
            }
            if (!pending && mgr.wantsRefresh(0)) {
                mgr.beginRefresh(0);
                pending = true; deliverIn = rnd(1, 6);
                snapshot(cfg.snapshotLimit, pb, pa); snapId = id;       // taken now, delivered later
            }

            BookQuote q = mgr.getQuote(0, 0, 0);
            if (!q.isValid) { ++invalidRows; havePrev = false; continue; }
            ++validRows;
            // Every step publishes: the stored ids must chain exactly as the
            // continuity tool (kdb/utils/check_quote_seq.q) will require.
            REQUIRE(q.exchUpdateId == id);
            if (havePrev) {
                if (fut) {
                    REQUIRE((q.exchPrevUpdateId == prevQuoteU ||
                             (q.exchFirstUpdateId <= prevQuoteU && prevQuoteU <= q.exchUpdateId)));
                } else {
                    REQUIRE(q.exchFirstUpdateId <= prevQuoteU + 1);
                    REQUIRE(q.exchUpdateId >= prevQuoteU);
                }
            }
            mgr.recordPublish(0, q);
            prevQuoteU = q.exchUpdateId; havePrev = true;
            double bp[5] = {q.bidPrices[0], q.bidPrices[1], q.bidPrices[2], q.bidPrices[3], q.bidPrices[4]};
            double bq[5] = {q.bidQtys[0], q.bidQtys[1], q.bidQtys[2], q.bidQtys[3], q.bidQtys[4]};
            double ap[5] = {q.askPrices[0], q.askPrices[1], q.askPrices[2], q.askPrices[3], q.askPrices[4]};
            double aq[5] = {q.askQtys[0], q.askQtys[1], q.askQtys[2], q.askQtys[3], q.askQtys[4]};
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
        INFO((fut ? "futures" : "spot") << " seed " << seed << " valid " << validRows << " invalid " << invalidRows
             << " deletes " << deletes << " refreshes " << refreshes);
        REQUIRE(deletes > 5000);
        REQUIRE(refreshes > 10);
        REQUIRE(validRows > 19000);
    }
}

// ============================================================================
// USD-M futures sync rule (pu chain, first-event rule)
// https://developers.binance.com/docs/derivatives/usds-margined-futures/
//         websocket-market-streams/How-to-manage-a-local-order-book-correctly
// ============================================================================

namespace {

BookConfig futCfg() { BookConfig c; c.sync = t2s::DepthSync::Futures; return c; }
BookConfig futSmallCfg() { BookConfig c = smallCfg(); c.sync = t2s::DepthSync::Futures; return c; }

/// Futures depth event: U, u, pu and level updates
BufferedDelta fev(long long U, long long u, long long pu,
                  std::vector<PriceLevel> bids = {}, std::vector<PriceLevel> asks = {}) {
    BufferedDelta d{U, u, 1700000000000LL, std::move(bids), std::move(asks)};
    d.prevFinalUpdateId = pu;
    d.transactTimeMs = 1699999999990LL;
    return d;
}

} // namespace

TEST_CASE("Futures: events older than the snapshot are dropped (step 4)", "[ordrbook][futures]") {
    OrderBookManager mgr({"BTCUSDT"}, futCfg());
    mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, fev(900, 950, 890, {pl(100.0, 0.0)})));     // u < lastUpdateId
    REQUIRE(mgr.applyDelta(0, fev(960, 999, 950, {pl(99.0, 0.0)})));      // u = lastUpdateId - 1
    REQUIRE(mgr.getState(0) == BookState::SYNCING);
    REQUIRE(mgr.getQuote(0, 0, 0).bidPrices[0] == 100.0);                       // nothing applied
}

TEST_CASE("Futures: the first processed event has U <= lastUpdateId <= u (step 5)", "[ordrbook][futures]") {
    SECTION("straddling event") {
        OrderBookManager mgr({"BTCUSDT"}, futCfg());
        mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
        REQUIRE(mgr.applyDelta(0, fev(990, 1010, 985, {pl(100.0, 7.0)})));
        REQUIRE(mgr.isValid(0));
        REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 7.0);
    }
    SECTION("boundary u == lastUpdateId") {
        OrderBookManager mgr({"BTCUSDT"}, futCfg());
        mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
        REQUIRE(mgr.applyDelta(0, fev(995, 1000, 990)));
        REQUIRE(mgr.isValid(0));
    }
    SECTION("boundary U == lastUpdateId") {
        OrderBookManager mgr({"BTCUSDT"}, futCfg());
        mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
        REQUIRE(mgr.applyDelta(0, fev(1000, 1004, 998)));
        REQUIRE(mgr.isValid(0));
    }
    SECTION("first event entirely after the snapshot: snapshot too old") {
        OrderBookManager mgr({"BTCUSDT"}, futCfg());
        mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
        REQUIRE_FALSE(mgr.applyDelta(0, fev(1005, 1010, 1003)));
        REQUIRE(mgr.getState(0) == BookState::INVALID);
    }
    SECTION("event whose pu is the snapshot id follows it directly") {
        OrderBookManager mgr({"BTCUSDT"}, futCfg());
        mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
        REQUIRE(mgr.applyDelta(0, fev(1003, 1007, 1000, {pl(100.0, 2.0)})));
        REQUIRE(mgr.isValid(0));
        REQUIRE(mgr.getQuote(0, 0, 0).bidQtys[0] == 2.0);
    }
}

TEST_CASE("Futures: pu must equal the previous u; ids need not be consecutive (step 6)", "[ordrbook][futures]") {
    OrderBookManager mgr({"BTCUSDT"}, futCfg());
    mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, fev(990, 1010, 985)));
    // ids jump by hundreds (other symbols share the counter): fine, pu chains
    REQUIRE(mgr.applyDelta(0, fev(1500, 1520, 1010, {pl(100.0, 3.0)})));
    REQUIRE(mgr.applyDelta(0, fev(2900, 2901, 1520, {}, {pl(101.0, 4.0)})));
    REQUIRE(mgr.isValid(0));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidQtys[0] == 3.0);
    REQUIRE(q.askQtys[0] == 4.0);
    REQUIRE(q.exchTransactTimeMs == 1699999999990LL);

    // The same stream under the SPOT rule would be a false gap
    OrderBookManager spot({"BTCUSDT"});
    spot.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(spot.applyDelta(0, 990, 1010, {}, {}, 0));
    REQUIRE_FALSE(spot.applyDelta(0, 1500, 1520, {}, {}, 0));
}

TEST_CASE("Futures: a broken pu chain is a gap; the book resyncs from a new snapshot", "[ordrbook][futures]") {
    OrderBookManager mgr({"BTCUSDT"}, futCfg());
    mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, fev(990, 1010, 985)));
    REQUIRE(mgr.applyDelta(0, fev(1011, 1020, 1010)));

    // One event lost: pu (1030) is not our last u (1020)
    REQUIRE_FALSE(mgr.applyDelta(0, fev(1031, 1040, 1030, {pl(100.0, 0.0)})));
    REQUIRE(mgr.getState(0) == BookState::INVALID);
    REQUIRE_FALSE(mgr.getQuote(0, 0, 0).isValid);

    // Even consecutive-looking ids are a gap if pu does not match
    OrderBookManager m2({"BTCUSDT"}, futCfg());
    m2.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(m2.applyDelta(0, fev(990, 1010, 985)));
    REQUIRE_FALSE(m2.applyDelta(0, fev(1011, 1015, 1009)));

    // Resync: reset, buffer the stream, new snapshot, replay
    mgr.reset(0);
    REQUIRE(mgr.needsSnapshot(0));
    mgr.bufferDelta(0, fev(1041, 1050, 1040));
    mgr.bufferDelta(0, fev(1051, 1060, 1050, {pl(100.0, 6.0)}));
    mgr.bufferDelta(0, fev(1061, 1070, 1060, {pl(99.0, 0.0)}));
    REQUIRE(mgr.onSnapshot(0, 1055, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::SYNCED);
    REQUIRE(mgr.isValid(0));
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.bidQtys[0] == 6.0);          // 1051-1060 straddles 1055: applied
    REQUIRE(q.bidPrices[1] == 98.0);       // 1061-1070 applied
    REQUIRE(mgr.applyDelta(0, fev(1071, 1080, 1070)));
}

TEST_CASE("Futures: a gap inside the buffered deltas fails the initial sync", "[ordrbook][futures]") {
    OrderBookManager mgr({"BTCUSDT"}, futCfg());
    mgr.bufferDelta(0, fev(1041, 1050, 1040));
    mgr.bufferDelta(0, fev(1051, 1060, 1050));
    mgr.bufferDelta(0, fev(1071, 1080, 1070));     // 1061-1070 missing
    REQUIRE(mgr.onSnapshot(0, 1055, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::SYNC_FAILED);
    REQUIRE(mgr.getState(0) == BookState::INVALID);
}

TEST_CASE("Futures: background refresh bridges with the futures rule", "[ordrbook][futures][refresh]") {
    OrderBookManager mgr({"BTCUSDT"}, futSmallCfg());
    mgr.applySnapshot(0, 1000, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, fev(990, 1010, 985)));
    std::vector<PriceLevel> del;
    for (int i = 0; i < 13; ++i) del.push_back(pl(100.0 - i, 0.0));
    REQUIRE(mgr.applyDelta(0, fev(1200, 1210, 1010, del)));
    REQUIRE(mgr.wantsRefresh(0));
    mgr.beginRefresh(0);
    REQUIRE(mgr.applyDelta(0, fev(1400, 1410, 1210, {pl(87.0, 9.0)})));
    REQUIRE(mgr.applyDelta(0, fev(1600, 1610, 1410, {pl(86.0, 0.0)})));

    auto snapBids = bidsN(87.0, 20); snapBids[0].qty = 9.0;
    REQUIRE(mgr.onSnapshot(0, 1405, snapBids, asksN(101.0, 20)) == SnapshotOutcome::REFRESHED);
    BookQuote q = mgr.getQuote(0, 0, 0);
    REQUIRE(q.isValid);
    REQUIRE(q.bidPrices[0] == 87.0);
    REQUIRE(q.bidPrices[1] == 85.0);
    REQUIRE(mgr.applyDelta(0, fev(1800, 1801, 1610)));            // chain continues from the live stream
    REQUIRE_FALSE(mgr.applyDelta(0, fev(2000, 2001, 1900)));      // and still detects a gap

    // Refresh snapshot ahead of the stream: held until an event bridges it
    OrderBookManager m2({"BTCUSDT"}, futSmallCfg());
    m2.applySnapshot(0, 1000, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(m2.applyDelta(0, fev(990, 1010, 985)));
    m2.beginRefresh(0);
    REQUIRE(m2.applyDelta(0, fev(1100, 1110, 1010)));
    REQUIRE(m2.onSnapshot(0, 1300, bidsN(99.0, 20), asksN(101.0, 20)) == SnapshotOutcome::REFRESH_AWAITING_BRIDGE);
    REQUIRE(m2.getQuote(0, 0, 0).bidPrices[0] == 100.0);                 // live book untouched
    REQUIRE(m2.applyDelta(0, fev(1200, 1250, 1110, {pl(100.0, 8.0)})));  // before the snapshot: live only
    REQUIRE(m2.getQuote(0, 0, 0).bidQtys[0] == 8.0);
    REQUIRE(m2.depthRefreshes() == 0);
    REQUIRE(m2.applyDelta(0, fev(1290, 1310, 1250, {pl(99.0, 5.0)})));   // straddles 1300: swap
    REQUIRE(m2.depthRefreshes() == 1);
    REQUIRE(m2.getQuote(0, 0, 0).bidPrices[0] == 99.0);
    REQUIRE(m2.getQuote(0, 0, 0).bidQtys[0] == 5.0);
    REQUIRE(m2.applyDelta(0, fev(1400, 1401, 1310)));
}

// ============================================================================
// Configurable published depth
// ============================================================================

TEST_CASE("The published depth comes from BookConfig", "[ordrbook][depthcfg]") {
    for (int depth : {1, 3, 5, 10}) {
        BookConfig cfg; cfg.depth = depth;
        OrderBookManager mgr({"BTCUSDT"}, cfg);
        REQUIRE(mgr.depth() == depth);
        mgr.applySnapshot(0, 100, bidsN(100.0, 12), asksN(101.0, 12));
        REQUIRE(mgr.applyDelta(0, 101, 101, {pl(100.0, 0.0)}, {}, 0));
        BookQuote q = mgr.getQuote(0, 0, 0);
        REQUIRE(q.isValid);
        REQUIRE(q.depth() == depth);
        REQUIRE(q.bidPrices.size() == static_cast<std::size_t>(depth));
        REQUIRE(q.askQtys.size() == static_cast<std::size_t>(depth));
        REQUIRE(q.bidPrices[0] == 99.0);
        REQUIRE(q.bidPrices[depth - 1] == 99.0 - (depth - 1));   // refilled from below
        REQUIRE(q.askPrices[depth - 1] == 101.0 + (depth - 1));
    }
}

TEST_CASE("Depth exhaustion is judged against the configured depth", "[ordrbook][depthcfg]") {
    BookConfig cfg = smallCfg(); cfg.depth = 3;
    OrderBookManager mgr({"BTCUSDT"}, cfg);
    mgr.applySnapshot(0, 100, bidsN(100.0, 20), asksN(101.0, 20));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    std::vector<PriceLevel> del;
    for (int i = 0; i < 16; ++i) del.push_back(pl(100.0 - i, 0.0));   // 4 known bids left
    REQUIRE(mgr.applyDelta(0, 102, 102, del, {}, 0));
    REQUIRE_FALSE(mgr.depthExhausted(0));                             // 4 >= 3: still fine at depth 3
    REQUIRE(mgr.getQuote(0, 0, 0).isValid);
    REQUIRE(mgr.applyDelta(0, 103, 103, {pl(84.0, 0.0), pl(83.0, 0.0)}, {}, 0));   // 2 left
    REQUIRE(mgr.depthExhausted(0));
    REQUIRE_FALSE(mgr.getQuote(0, 0, 0).isValid);
}

// ============================================================================
// Exchange update ids carried by the published quote
// ============================================================================

TEST_CASE("Spot quote carries the id range of the events since the last publish", "[ordrbook][ids]") {
    OrderBookManager mgr({"BTCUSDT"});
    mgr.bufferDelta(0, BufferedDelta{95, 99, 0, {}, {}});          // stale
    mgr.bufferDelta(0, BufferedDelta{100, 102, 0, {}, {}});        // bridges 101
    mgr.bufferDelta(0, BufferedDelta{103, 104, 0, {}, {}});
    REQUIRE(mgr.onSnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10)) == SnapshotOutcome::SYNCED);

    BookQuote q1 = mgr.getQuote(0, 0, 1);
    REQUIRE(q1.exchFirstUpdateId == 100);       // U of the bridging event, not of the stale one
    REQUIRE(q1.exchUpdateId == 104);
    REQUIRE(q1.exchPrevUpdateId == 0);          // spot has no pu
    mgr.recordPublish(0, q1);

    // Heartbeat: nothing applied since the publish, ids repeat
    BookQuote hb = mgr.getQuote(0, 0, 2);
    REQUIRE(hb.exchFirstUpdateId == 100);
    REQUIRE(hb.exchUpdateId == 104);
    mgr.recordPublish(0, hb);

    // Three events before the next publish: the range covers all three
    REQUIRE(mgr.applyDelta(0, 105, 106, {}, {}, 0));
    REQUIRE(mgr.applyDelta(0, 107, 107, {}, {}, 0));
    REQUIRE(mgr.applyDelta(0, 108, 111, {pl(100.0, 9.0)}, {}, 0));
    BookQuote q2 = mgr.getQuote(0, 0, 3);
    REQUIRE(q2.exchFirstUpdateId == 105);       // == previous exchUpdateId + 1
    REQUIRE(q2.exchUpdateId == 111);
    mgr.recordPublish(0, q2);

    // A stale event (skipped) does not open a range
    REQUIRE(mgr.applyDelta(0, 104, 110, {}, {}, 0));
    BookQuote q3 = mgr.getQuote(0, 0, 4);
    REQUIRE(q3.exchFirstUpdateId == 105);
    REQUIRE(q3.exchUpdateId == 111);

    // An invalid quote carries no ids
    REQUIRE_FALSE(mgr.applyDelta(0, 200, 201, {}, {}, 0));
    BookQuote bad = mgr.getQuote(0, 0, 5);
    REQUIRE_FALSE(bad.isValid);
    REQUIRE(bad.exchFirstUpdateId == 0);
    REQUIRE(bad.exchUpdateId == 0);
}

TEST_CASE("Futures quote carries U, u and the pu of the first event of the range", "[ordrbook][ids][futures]") {
    OrderBookManager mgr({"BTCUSDT"}, futCfg());
    mgr.applySnapshot(0, 1000, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, fev(990, 1010, 985)));
    BookQuote q1 = mgr.getQuote(0, 0, 1);
    REQUIRE(q1.exchFirstUpdateId == 990);
    REQUIRE(q1.exchUpdateId == 1010);
    REQUIRE(q1.exchPrevUpdateId == 985);
    mgr.recordPublish(0, q1);

    REQUIRE(mgr.applyDelta(0, fev(1500, 1520, 1010)));
    REQUIRE(mgr.applyDelta(0, fev(2900, 2901, 1520)));
    BookQuote q2 = mgr.getQuote(0, 0, 2);
    REQUIRE(q2.exchPrevUpdateId == 1010);       // == previous exchUpdateId: the chain a reader can verify
    REQUIRE(q2.exchFirstUpdateId == 1500);
    REQUIRE(q2.exchUpdateId == 2901);
}

TEST_CASE("lastPublishedValid tells whether a hole needs marking", "[ordrbook][ids]") {
    OrderBookManager mgr({"BTCUSDT", "ETHUSDT"});
    REQUIRE_FALSE(mgr.lastPublishedValid(0));
    mgr.applySnapshot(0, 100, bidsN(100.0, 10), asksN(101.0, 10));
    REQUIRE(mgr.applyDelta(0, 101, 101, {}, {}, 0));
    mgr.recordPublish(0, mgr.getQuote(0, 0, 1));
    REQUIRE(mgr.lastPublishedValid(0));
    REQUIRE_FALSE(mgr.lastPublishedValid(1));
    BookQuote inv(5); inv.sym = "BTCUSDT";
    mgr.recordPublish(0, inv);
    REQUIRE_FALSE(mgr.lastPublishedValid(0));
}
