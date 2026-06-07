/**
 * @file test_trade_fh_row_construction.cpp
 * @brief Regression fixture for the trade_binance and trade_binance_fut K-row schemas.
 *
 * Locks the structure of rows produced by t2s::buildTradeRow (spot) and
 * t2s::buildAggTradeRow (futures) against silent drift. This is the
 * safety net for the MarketConfig refactor (step 3) and the futures
 * schema branch (step 5) in ADR-013: if either builder's output
 * changes, this test fails and points at the specific field.
 *
 * Both row builders are pure (no clocks, no I/O, no global state), so
 * the tests pass deterministic inputs and assert structural and value
 * properties of the resulting K object. Failures are readable.
 *
 * Run directly: ./build/test_trade_fh_row_construction
 */

#include "trade_row.hpp"
#include "catch_amalgamated.hpp"

#include <string>

namespace {

// Same value used in production (TradeFeedHandler::KDB_EPOCH_OFFSET_NS).
// Defined here independently to avoid coupling the test to the FH header.
constexpr long long KDB_EPOCH_OFFSET_NS = 946684800000000000LL;

constexpr bool approxEq(double a, double b, double eps = 1e-9) {
    return (a > b ? a - b : b - a) < eps;
}

// A canonical set of inputs reused across the spot tests. Hand-picked
// so each value is unique and recognisable in failure output.
struct CanonicalSpotInputs {
    long long   fhRecvTimeUtcNs = 1717459200000000000LL;  // 2024-06-04 00:00:00 UTC
    std::string sym             = "BTCUSDT";
    long long   tradeId         = 12345678LL;
    double      price           = 50000.5;
    double      qty             = 0.123456;
    bool        buyerIsMaker    = true;
    long long   exchEventTimeMs = 1717459199950LL;
    long long   exchTradeTimeMs = 1717459199900LL;
    long long   fhParseUs       = 42LL;
    long long   fhSendUs        = 7LL;
    long long   fhSeqNo         = 1001LL;
};

t2s::KOwned buildCanonicalSpot(const CanonicalSpotInputs& in = {}) {
    return t2s::buildTradeRow(
        in.fhRecvTimeUtcNs, in.sym, in.tradeId,
        in.price, in.qty, in.buyerIsMaker,
        in.exchEventTimeMs, in.exchTradeTimeMs,
        in.fhParseUs, in.fhSendUs, in.fhSeqNo,
        KDB_EPOCH_OFFSET_NS);
}

// Canonical inputs for the futures aggTrade row. Distinct values from
// the spot set so cross-test mixups would be visible.
struct CanonicalAggInputs {
    long long   fhRecvTimeUtcNs = 1717459260000000000LL;  // +60 sec vs spot canonical
    std::string sym             = "BTCUSDT";
    long long   aggTradeId      = 99999000LL;
    long long   firstTradeId    = 99999001LL;
    long long   lastTradeId     = 99999004LL;  // 4-fill aggregation
    double      price           = 50100.25;
    double      qty             = 0.5;
    bool        buyerIsMaker    = false;
    long long   exchEventTimeMs = 1717459259850LL;
    long long   exchTradeTimeMs = 1717459259800LL;
    long long   fhParseUs       = 31LL;
    long long   fhSendUs        = 9LL;
    long long   fhSeqNo         = 2002LL;
};

t2s::KOwned buildCanonicalAgg(const CanonicalAggInputs& in = {}) {
    return t2s::buildAggTradeRow(
        in.fhRecvTimeUtcNs, in.sym, in.aggTradeId,
        in.firstTradeId, in.lastTradeId,
        in.price, in.qty, in.buyerIsMaker,
        in.exchEventTimeMs, in.exchTradeTimeMs,
        in.fhParseUs, in.fhSendUs, in.fhSeqNo,
        KDB_EPOCH_OFFSET_NS);
}

} // namespace

// ============================================================================
// SPOT: buildTradeRow
// ============================================================================

TEST_CASE("buildTradeRow produces 12-field mixed list", "[trade_fh][row][regression][spot]") {
    t2s::KOwned row = buildCanonicalSpot();

    REQUIRE(row.get() != nullptr);
    REQUIRE(row.get()->t == 0);     // mixed list
    REQUIRE(row.get()->n == 12);    // exactly 12 fields
}

TEST_CASE("buildTradeRow field types match the trade_binance schema",
          "[trade_fh][row][regression][spot]") {
    t2s::KOwned row = buildCanonicalSpot();
    K r = row.get();

    REQUIRE(kK(r)[0]->t  == -KP);   // time
    REQUIRE(kK(r)[1]->t  == -KS);   // sym
    REQUIRE(kK(r)[2]->t  == -KJ);   // tradeId
    REQUIRE(kK(r)[3]->t  == -KF);   // price
    REQUIRE(kK(r)[4]->t  == -KF);   // qty
    REQUIRE(kK(r)[5]->t  == -KB);   // buyerIsMaker
    REQUIRE(kK(r)[6]->t  == -KJ);   // exchEventTimeMs
    REQUIRE(kK(r)[7]->t  == -KJ);   // exchTradeTimeMs
    REQUIRE(kK(r)[8]->t  == -KJ);   // fhRecvTimeUtcNs
    REQUIRE(kK(r)[9]->t  == -KJ);   // fhParseUs
    REQUIRE(kK(r)[10]->t == -KJ);   // fhSendUs
    REQUIRE(kK(r)[11]->t == -KJ);   // fhSeqNo
}

TEST_CASE("buildTradeRow field values match canonical inputs",
          "[trade_fh][row][regression][spot]") {
    CanonicalSpotInputs in;
    t2s::KOwned row = buildCanonicalSpot(in);
    K r = row.get();

    REQUIRE(kK(r)[0]->j == in.fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS);
    REQUIRE(std::string(kK(r)[1]->s) == in.sym);
    REQUIRE(kK(r)[2]->j == in.tradeId);
    REQUIRE(approxEq(kK(r)[3]->f, in.price));
    REQUIRE(approxEq(kK(r)[4]->f, in.qty));
    REQUIRE(kK(r)[5]->g == 1);
    REQUIRE(kK(r)[6]->j == in.exchEventTimeMs);
    REQUIRE(kK(r)[7]->j == in.exchTradeTimeMs);
    REQUIRE(kK(r)[8]->j == in.fhRecvTimeUtcNs);
    REQUIRE(kK(r)[9]->j == in.fhParseUs);
    REQUIRE(kK(r)[10]->j == in.fhSendUs);
    REQUIRE(kK(r)[11]->j == in.fhSeqNo);
}

TEST_CASE("buildTradeRow encodes buyerIsMaker=false as 0", "[trade_fh][row][spot]") {
    CanonicalSpotInputs in;
    in.buyerIsMaker = false;
    t2s::KOwned row = buildCanonicalSpot(in);
    REQUIRE(kK(row.get())[5]->g == 0);
}

TEST_CASE("buildTradeRow preserves arbitrary symbols", "[trade_fh][row][spot]") {
    for (const auto& sym : { "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT" }) {
        CanonicalSpotInputs in;
        in.sym = sym;
        t2s::KOwned row = buildCanonicalSpot(in);
        REQUIRE(std::string(kK(row.get())[1]->s) == sym);
    }
}

TEST_CASE("buildTradeRow time field uses the supplied epoch offset",
          "[trade_fh][row][regression][spot]") {
    const long long altOffset = 1000000000LL;
    t2s::KOwned row = t2s::buildTradeRow(
        5000000000LL, "BTCUSDT", 1LL, 1.0, 1.0, true,
        0LL, 0LL, 0LL, 0LL, 1LL, altOffset);

    REQUIRE(kK(row.get())[0]->j == 5000000000LL - altOffset);
}

TEST_CASE("buildTradeRow fhSendUs slot is mutable in-place via KBorrowed",
          "[trade_fh][row][regression][spot]") {
    CanonicalSpotInputs in;
    in.fhSendUs = 0LL;
    t2s::KOwned row = buildCanonicalSpot(in);

    REQUIRE(kK(row.get())[10]->j == 0);
    t2s::KBorrowed slot(kK(row.get())[10]);
    slot.get()->j = 12345LL;
    REQUIRE(kK(row.get())[10]->j == 12345LL);
}

TEST_CASE("buildTradeRow handles zero-valued numeric fields", "[trade_fh][row][spot]") {
    t2s::KOwned row = t2s::buildTradeRow(
        0LL, "BTCUSDT", 0LL, 0.0, 0.0, false,
        0LL, 0LL, 0LL, 0LL, 0LL, KDB_EPOCH_OFFSET_NS);

    K r = row.get();
    REQUIRE(r->n == 12);
    REQUIRE(kK(r)[0]->j == -KDB_EPOCH_OFFSET_NS);
    REQUIRE(kK(r)[2]->j == 0);
    REQUIRE(approxEq(kK(r)[3]->f, 0.0));
    REQUIRE(kK(r)[5]->g == 0);
    REQUIRE(kK(r)[11]->j == 0);
}

// ============================================================================
// FUTURES: buildAggTradeRow
// ============================================================================

TEST_CASE("buildAggTradeRow produces 14-field mixed list",
          "[trade_fh][row][regression][futures]") {
    t2s::KOwned row = buildCanonicalAgg();

    REQUIRE(row.get() != nullptr);
    REQUIRE(row.get()->t == 0);     // mixed list
    REQUIRE(row.get()->n == 14);    // exactly 14 fields (2 more than spot)
}

TEST_CASE("buildAggTradeRow field types match the trade_binance_fut schema",
          "[trade_fh][row][regression][futures]") {
    t2s::KOwned row = buildCanonicalAgg();
    K r = row.get();

    REQUIRE(kK(r)[0]->t  == -KP);   // time
    REQUIRE(kK(r)[1]->t  == -KS);   // sym
    REQUIRE(kK(r)[2]->t  == -KJ);   // aggTradeId
    REQUIRE(kK(r)[3]->t  == -KJ);   // firstTradeId
    REQUIRE(kK(r)[4]->t  == -KJ);   // lastTradeId
    REQUIRE(kK(r)[5]->t  == -KF);   // price
    REQUIRE(kK(r)[6]->t  == -KF);   // qty
    REQUIRE(kK(r)[7]->t  == -KB);   // buyerIsMaker
    REQUIRE(kK(r)[8]->t  == -KJ);   // exchEventTimeMs
    REQUIRE(kK(r)[9]->t  == -KJ);   // exchTradeTimeMs
    REQUIRE(kK(r)[10]->t == -KJ);   // fhRecvTimeUtcNs
    REQUIRE(kK(r)[11]->t == -KJ);   // fhParseUs
    REQUIRE(kK(r)[12]->t == -KJ);   // fhSendUs
    REQUIRE(kK(r)[13]->t == -KJ);   // fhSeqNo
}

TEST_CASE("buildAggTradeRow field values match canonical inputs",
          "[trade_fh][row][regression][futures]") {
    CanonicalAggInputs in;
    t2s::KOwned row = buildCanonicalAgg(in);
    K r = row.get();

    REQUIRE(kK(r)[0]->j == in.fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS);
    REQUIRE(std::string(kK(r)[1]->s) == in.sym);
    REQUIRE(kK(r)[2]->j == in.aggTradeId);
    REQUIRE(kK(r)[3]->j == in.firstTradeId);
    REQUIRE(kK(r)[4]->j == in.lastTradeId);
    REQUIRE(approxEq(kK(r)[5]->f, in.price));
    REQUIRE(approxEq(kK(r)[6]->f, in.qty));
    REQUIRE(kK(r)[7]->g == 0);      // buyerIsMaker=false in canonical
    REQUIRE(kK(r)[8]->j == in.exchEventTimeMs);
    REQUIRE(kK(r)[9]->j == in.exchTradeTimeMs);
    REQUIRE(kK(r)[10]->j == in.fhRecvTimeUtcNs);
    REQUIRE(kK(r)[11]->j == in.fhParseUs);
    REQUIRE(kK(r)[12]->j == in.fhSendUs);
    REQUIRE(kK(r)[13]->j == in.fhSeqNo);
}

TEST_CASE("buildAggTradeRow fhSendUs slot is at index 12 (not 10 as in spot)",
          "[trade_fh][row][regression][futures]") {
    // This is the most likely source of a silent bug if anyone refactors
    // processMessage to use a shared slot index - they'll patch the wrong
    // field. The test makes the index difference between schemas explicit.
    CanonicalAggInputs in;
    in.fhSendUs = 0LL;
    t2s::KOwned row = buildCanonicalAgg(in);

    REQUIRE(kK(row.get())[12]->j == 0);
    t2s::KBorrowed slot(kK(row.get())[12]);
    slot.get()->j = 54321LL;
    REQUIRE(kK(row.get())[12]->j == 54321LL);

    // And slot 10 in the futures layout is fhRecvTimeUtcNs, NOT fhSendUs.
    // Confirm that, so a regression that swaps the slot index would fail
    // both this assertion and the assertion above.
    REQUIRE(kK(row.get())[10]->j == in.fhRecvTimeUtcNs);
}

TEST_CASE("buildAggTradeRow encodes buyerIsMaker=true as 1", "[trade_fh][row][futures]") {
    CanonicalAggInputs in;
    in.buyerIsMaker = true;
    t2s::KOwned row = buildCanonicalAgg(in);
    REQUIRE(kK(row.get())[7]->g == 1);
}

TEST_CASE("buildAggTradeRow firstTradeId == lastTradeId for single-fill aggregations",
          "[trade_fh][row][futures]") {
    // Many aggTrades are single fills (one taker hit one resting order).
    // In that case f == l == a-something. Test the function handles equal
    // f/l correctly (just stores them - no special-casing).
    CanonicalAggInputs in;
    in.aggTradeId   = 5000LL;
    in.firstTradeId = 7000LL;
    in.lastTradeId  = 7000LL;
    t2s::KOwned row = buildCanonicalAgg(in);

    REQUIRE(kK(row.get())[2]->j == 5000LL);
    REQUIRE(kK(row.get())[3]->j == 7000LL);
    REQUIRE(kK(row.get())[4]->j == 7000LL);
}

TEST_CASE("buildAggTradeRow preserves arbitrary symbols", "[trade_fh][row][futures]") {
    for (const auto& sym : { "BTCUSDT", "ETHUSDT", "SOLUSDT" }) {
        CanonicalAggInputs in;
        in.sym = sym;
        t2s::KOwned row = buildCanonicalAgg(in);
        REQUIRE(std::string(kK(row.get())[1]->s) == sym);
    }
}

TEST_CASE("buildAggTradeRow handles zero-valued numeric fields",
          "[trade_fh][row][futures]") {
    t2s::KOwned row = t2s::buildAggTradeRow(
        0LL, "BTCUSDT", 0LL, 0LL, 0LL, 0.0, 0.0, false,
        0LL, 0LL, 0LL, 0LL, 0LL, KDB_EPOCH_OFFSET_NS);

    K r = row.get();
    REQUIRE(r->n == 14);
    REQUIRE(kK(r)[0]->j == -KDB_EPOCH_OFFSET_NS);
    REQUIRE(kK(r)[2]->j == 0);    // aggTradeId
    REQUIRE(kK(r)[3]->j == 0);    // firstTradeId
    REQUIRE(kK(r)[4]->j == 0);    // lastTradeId
    REQUIRE(approxEq(kK(r)[5]->f, 0.0));
    REQUIRE(kK(r)[7]->g == 0);
    REQUIRE(kK(r)[13]->j == 0);
}

// ============================================================================
// CROSS-SCHEMA INVARIANTS
// ============================================================================

TEST_CASE("Spot and futures rows are different sizes", "[trade_fh][row][regression]") {
    // A silent regression that made both builders return identical row
    // shapes (e.g. via a copy-paste mistake) would silently break futures.
    // Lock the count difference explicitly.
    auto spot = buildCanonicalSpot();
    auto agg  = buildCanonicalAgg();

    REQUIRE(spot.get()->n == 12);
    REQUIRE(agg.get()->n  == 14);
    REQUIRE(agg.get()->n - spot.get()->n == 2);  // futures has f+l extra
}
