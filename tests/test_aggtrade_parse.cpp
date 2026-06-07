/**
 * @file test_aggtrade_parse.cpp
 * @brief Unit tests for parsing the Binance USDT-M futures @aggTrade
 *        WebSocket payload via the JsonReader wrapper.
 *
 * Mirrors the schema-extraction portion of TradeFeedHandler::processMessage
 * for the FuturesAggTrade branch. Covers:
 *
 *   - well-formed aggTrade payloads (combined-stream wrapper shape)
 *   - missing aggregate-id fields (a/f/l)
 *   - type errors on the futures-specific fields
 *   - field name distinction from spot (a vs t)
 *
 * Does NOT exercise the K-row construction - that's covered by
 * test_trade_fh_row_construction's futures cases. Together these two
 * tests bracket the futures path from JSON in to K row out.
 */

#include "json_reader.hpp"
#include "catch_amalgamated.hpp"

#include <rapidjson/document.h>

#include <string>

namespace {

rapidjson::Document parse(const char* json) {
    rapidjson::Document d;
    d.Parse(json);
    return d;
}

constexpr bool approxEq(double a, double b, double eps = 1e-9) {
    return (a > b ? a - b : b - a) < eps;
}

// A canonical Binance USDT-M futures aggTrade payload wrapped in the
// combined-stream envelope. The "stream" field tells the receiver which
// per-symbol stream this came from; the "data" object holds the event.
// Fields (per Binance docs):
//   e: event type ("aggTrade")
//   E: event time (ms since epoch)
//   s: symbol
//   a: aggregated trade id
//   p: price (string)
//   q: quantity (string)
//   f: first constituent trade id
//   l: last constituent trade id
//   T: trade time (ms since epoch)
//   m: was the buyer the maker?
const char* CANONICAL_AGGTRADE = R"({
    "stream": "btcusdt@aggTrade",
    "data": {
        "e": "aggTrade",
        "E": 1717459199850,
        "s": "BTCUSDT",
        "a": 99999000,
        "p": "50100.25",
        "q": "0.5",
        "f": 99999001,
        "l": 99999004,
        "T": 1717459199800,
        "m": false
    }
})";

} // namespace

// ============================================================================
// Happy path
// ============================================================================

TEST_CASE("aggTrade payload: all expected fields extract cleanly",
          "[parse][futures]") {
    auto doc = parse(CANONICAL_AGGTRADE);
    REQUIRE_FALSE(doc.HasParseError());

    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    auto sym = d.string("s");
    auto a   = d.int64("a");
    auto f   = d.int64("f");
    auto l   = d.int64("l");
    auto p   = d.priceString("p");
    auto q   = d.priceString("q");
    auto m   = d.boolean("m");
    auto E   = d.int64("E");
    auto T   = d.int64("T");

    REQUIRE_FALSE(d.hasError());

    REQUIRE(sym.value() == "BTCUSDT");
    REQUIRE(a.value() == 99999000LL);
    REQUIRE(f.value() == 99999001LL);
    REQUIRE(l.value() == 99999004LL);
    REQUIRE(approxEq(p.value(), 50100.25));
    REQUIRE(approxEq(q.value(), 0.5));
    REQUIRE(m.value() == false);
    REQUIRE(E.value() == 1717459199850LL);
    REQUIRE(T.value() == 1717459199800LL);
}

TEST_CASE("aggTrade payload: lastTradeId == firstTradeId is valid (single-fill agg)",
          "[parse][futures]") {
    // Single-fill aggregations (one taker hit one resting order at one
    // price) are the most common case on liquid markets. f and l are
    // typically equal in that case. Confirm the reader handles it.
    const char* singleFill = R"({
        "data": {
            "s": "BTCUSDT", "a": 100, "f": 200, "l": 200,
            "p": "50000", "q": "0.01", "T": 1, "E": 1, "m": true
        }
    })";
    auto doc = parse(singleFill);
    t2s::JsonReader d = t2s::JsonReader(doc).obj("data");

    REQUIRE(d.int64("a").value() == 100);
    REQUIRE(d.int64("f").value() == 200);
    REQUIRE(d.int64("l").value() == 200);
    REQUIRE_FALSE(d.hasError());
}

// ============================================================================
// Field-name distinction from spot
// ============================================================================

TEST_CASE("aggTrade payload: requesting spot field 't' on a futures payload errors",
          "[parse][futures][schema]") {
    // The futures payload uses 'a' for the aggregate id. The spot field
    // name 't' (tradeId) is NOT present. Code that mistakenly used the
    // wrong field name would see this error - which is the desired
    // failure mode (loud and immediate, not silent zero).
    auto doc = parse(CANONICAL_AGGTRADE);
    t2s::JsonReader d = t2s::JsonReader(doc).obj("data");

    auto t = d.int64("t");

    REQUIRE_FALSE(t.has_value());
    REQUIRE(d.hasError());
    REQUIRE(d.lastError().find("missing") != std::string::npos);
    REQUIRE(d.lastError().find("t") != std::string::npos);
}

TEST_CASE("aggTrade payload: requesting futures field 'a' on a spot payload errors",
          "[parse][futures][schema]") {
    // Mirror image of the previous test: spot payloads have 't', not 'a'.
    const char* spotPayload = R"({
        "data": {
            "s": "BTCUSDT", "t": 12345678, "p": "50000", "q": "0.01",
            "T": 1, "E": 1, "m": true
        }
    })";
    auto doc = parse(spotPayload);
    t2s::JsonReader d = t2s::JsonReader(doc).obj("data");

    auto a = d.int64("a");

    REQUIRE_FALSE(a.has_value());
    REQUIRE(d.hasError());
}

// ============================================================================
// Failure modes specific to aggTrade
// ============================================================================

TEST_CASE("aggTrade payload: missing 'a' field is reported",
          "[parse][futures]") {
    const char* missingA = R"({
        "data": {
            "s": "BTCUSDT", "f": 1, "l": 1, "p": "50000", "q": "0.01",
            "T": 1, "E": 1, "m": true
        }
    })";
    auto doc = parse(missingA);
    t2s::JsonReader d = t2s::JsonReader(doc).obj("data");

    REQUIRE_FALSE(d.int64("a").has_value());
    REQUIRE(d.hasError());
}

TEST_CASE("aggTrade payload: missing 'f' or 'l' is reported",
          "[parse][futures]") {
    const char* missingF = R"({
        "data": {
            "s": "BTCUSDT", "a": 100, "l": 1, "p": "50000", "q": "0.01",
            "T": 1, "E": 1, "m": true
        }
    })";
    auto docF = parse(missingF);
    t2s::JsonReader dF = t2s::JsonReader(docF).obj("data");
    REQUIRE(dF.int64("a").has_value());
    REQUIRE_FALSE(dF.int64("f").has_value());
    REQUIRE(dF.hasError());

    const char* missingL = R"({
        "data": {
            "s": "BTCUSDT", "a": 100, "f": 1, "p": "50000", "q": "0.01",
            "T": 1, "E": 1, "m": true
        }
    })";
    auto docL = parse(missingL);
    t2s::JsonReader dL = t2s::JsonReader(docL).obj("data");
    REQUIRE(dL.int64("a").has_value());
    REQUIRE(dL.int64("f").has_value());
    REQUIRE_FALSE(dL.int64("l").has_value());
    REQUIRE(dL.hasError());
}

TEST_CASE("aggTrade payload: type errors on a/f/l are reported",
          "[parse][futures]") {
    const char* wrongType = R"({
        "data": {
            "s": "BTCUSDT", "a": "not_a_number", "f": 1, "l": 1,
            "p": "50000", "q": "0.01", "T": 1, "E": 1, "m": true
        }
    })";
    auto doc = parse(wrongType);
    t2s::JsonReader d = t2s::JsonReader(doc).obj("data");

    REQUIRE_FALSE(d.int64("a").has_value());
    REQUIRE(d.hasError());
}

TEST_CASE("aggTrade payload: 'data' object missing yields poisoned inner reader",
          "[parse][futures]") {
    const char* noData = R"({
        "stream": "btcusdt@aggTrade"
    })";
    auto doc = parse(noData);
    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    REQUIRE(d.hasError());
    REQUIRE_FALSE(d.int64("a").has_value());
}
