/**
 * @file test_trade_fh_row_construction.cpp
 * @brief Regression fixture for the trade_binance K-row schema.
 *
 * Locks the structure of rows produced by t2s::buildTradeRow against
 * silent changes (field reorder, type drift, off-by-one in column count,
 * accidental epoch-offset changes). This is the safety net for the
 * MarketConfig refactor in step 3 of ADR-013: if buildTradeRow's output
 * changes during that refactor, this test fails and points at the
 * specific field that drifted.
 *
 * The row builder is pure (no clocks, no I/O, no global state), so the
 * test passes deterministic inputs and asserts structural and value
 * properties of the resulting K object — no recorded snapshot bytes, no
 * opaque hex comparisons. Failures are readable.
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

// Float-tolerance comparator. Matches the project's existing test style
// (see test_json_reader.cpp) of using hand-rolled approxEq rather than
// Catch::Approx, for consistency across Catch2 versions.
constexpr bool approxEq(double a, double b, double eps = 1e-9) {
    return (a > b ? a - b : b - a) < eps;
}

// A canonical set of inputs reused across the structural and value
// tests. Hand-picked so each value is unique and recognisable in
// failure output: a real BTC-like price, a non-round qty, distinct
// timestamps, distinct latencies, distinct sequence number.
struct CanonicalInputs {
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

t2s::KOwned buildCanonical(const CanonicalInputs& in = {}) {
    return t2s::buildTradeRow(
        in.fhRecvTimeUtcNs, in.sym, in.tradeId,
        in.price, in.qty, in.buyerIsMaker,
        in.exchEventTimeMs, in.exchTradeTimeMs,
        in.fhParseUs, in.fhSendUs, in.fhSeqNo,
        KDB_EPOCH_OFFSET_NS);
}

} // namespace

// ============================================================================
// Structural invariants — type and shape of the row
// ============================================================================

TEST_CASE("buildTradeRow produces 12-field mixed list", "[trade_fh][row][regression]") {
    t2s::KOwned row = buildCanonical();

    REQUIRE(row.get() != nullptr);
    REQUIRE(row.get()->t == 0);     // mixed list
    REQUIRE(row.get()->n == 12);    // exactly 12 fields
}

TEST_CASE("buildTradeRow field types match the trade_binance schema", "[trade_fh][row][regression]") {
    t2s::KOwned row = buildCanonical();
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

// ============================================================================
// Value invariants — every field carries the input value through unchanged
// ============================================================================

TEST_CASE("buildTradeRow field values match canonical inputs", "[trade_fh][row][regression]") {
    CanonicalInputs in;
    t2s::KOwned row = buildCanonical(in);
    K r = row.get();

    // [0] time = recvUtcNs - epochOffset
    REQUIRE(kK(r)[0]->j == in.fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS);

    // [1] sym
    REQUIRE(std::string(kK(r)[1]->s) == in.sym);

    // [2] tradeId
    REQUIRE(kK(r)[2]->j == in.tradeId);

    // [3] price
    REQUIRE(approxEq(kK(r)[3]->f, in.price));

    // [4] qty
    REQUIRE(approxEq(kK(r)[4]->f, in.qty));

    // [5] buyerIsMaker (KB stored as G byte: 1 for true)
    REQUIRE(kK(r)[5]->g == 1);

    // [6] exchEventTimeMs
    REQUIRE(kK(r)[6]->j == in.exchEventTimeMs);

    // [7] exchTradeTimeMs
    REQUIRE(kK(r)[7]->j == in.exchTradeTimeMs);

    // [8] fhRecvTimeUtcNs
    REQUIRE(kK(r)[8]->j == in.fhRecvTimeUtcNs);

    // [9] fhParseUs
    REQUIRE(kK(r)[9]->j == in.fhParseUs);

    // [10] fhSendUs
    REQUIRE(kK(r)[10]->j == in.fhSendUs);

    // [11] fhSeqNo
    REQUIRE(kK(r)[11]->j == in.fhSeqNo);
}

// ============================================================================
// Edge cases and parametric checks
// ============================================================================

TEST_CASE("buildTradeRow encodes buyerIsMaker=false as 0", "[trade_fh][row]") {
    CanonicalInputs in;
    in.buyerIsMaker = false;
    t2s::KOwned row = buildCanonical(in);

    REQUIRE(kK(row.get())[5]->g == 0);
}

TEST_CASE("buildTradeRow preserves arbitrary symbols", "[trade_fh][row]") {
    for (const auto& sym : { "BTCUSDT", "ETHUSDT", "SOLUSDT", "DOGEUSDT" }) {
        CanonicalInputs in;
        in.sym = sym;
        t2s::KOwned row = buildCanonical(in);
        REQUIRE(std::string(kK(row.get())[1]->s) == sym);
    }
}

TEST_CASE("buildTradeRow time field uses the supplied epoch offset", "[trade_fh][row][regression]") {
    // Custom epoch offset (not the production constant) — confirms the
    // function honours its parameter rather than reading a global.
    const long long altOffset = 1000000000LL;
    t2s::KOwned row = t2s::buildTradeRow(
        /*fhRecvTimeUtcNs*/  5000000000LL,
        /*sym*/              "BTCUSDT",
        /*tradeId*/          1LL,
        /*price*/            1.0,
        /*qty*/              1.0,
        /*buyerIsMaker*/     true,
        /*exchEventTimeMs*/  0LL,
        /*exchTradeTimeMs*/  0LL,
        /*fhParseUs*/        0LL,
        /*fhSendUs*/         0LL,
        /*fhSeqNo*/          1LL,
        /*kdbEpochOffsetNs*/ altOffset);

    REQUIRE(kK(row.get())[0]->j == 5000000000LL - altOffset);
}

TEST_CASE("buildTradeRow fhSendUs slot is mutable in-place via KBorrowed", "[trade_fh][row][regression]") {
    // Production code in processMessage initialises fhSendUs to 0, then
    // patches the slot after measuring send latency. This test locks the
    // contract: slot [10] is reachable as -KJ and can be mutated via the
    // KBorrowed pattern without breaking the parent list.
    CanonicalInputs in;
    in.fhSendUs = 0LL;
    t2s::KOwned row = buildCanonical(in);

    REQUIRE(kK(row.get())[10]->j == 0);

    t2s::KBorrowed slot(kK(row.get())[10]);
    slot.get()->j = 12345LL;

    REQUIRE(kK(row.get())[10]->j == 12345LL);
}

TEST_CASE("buildTradeRow handles zero-valued numeric fields", "[trade_fh][row]") {
    // All-zero inputs (except sym which must be a valid C-string) — checks
    // that the function does not have any "use defaults if zero" surprises.
    t2s::KOwned row = t2s::buildTradeRow(
        0LL, "BTCUSDT", 0LL, 0.0, 0.0, false,
        0LL, 0LL, 0LL, 0LL, 0LL, KDB_EPOCH_OFFSET_NS);

    K r = row.get();
    REQUIRE(r->n == 12);
    REQUIRE(kK(r)[0]->j == -KDB_EPOCH_OFFSET_NS);  // 0 - offset
    REQUIRE(kK(r)[2]->j == 0);
    REQUIRE(approxEq(kK(r)[3]->f, 0.0));
    REQUIRE(kK(r)[5]->g == 0);
    REQUIRE(kK(r)[11]->j == 0);
}
