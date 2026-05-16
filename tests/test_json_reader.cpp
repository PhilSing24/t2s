/**
 * @file test_json_reader.cpp
 * @brief Unit tests for the JsonReader safe-accessor wrapper.
 *
 * Covers: happy path, missing keys, wrong types, malformed numeric strings,
 * error propagation through nested obj() calls, first-error-wins semantics,
 * poisoned construction, and the parseLevelPair helper.
 *
 * Run directly: ./build/test_json_reader
 */

#include "json_reader.hpp"
#include "catch_amalgamated.hpp"

#include <rapidjson/document.h>

namespace {

// Helper to parse a JSON literal into a Document.
rapidjson::Document parse(const char* json) {
    rapidjson::Document d;
    d.Parse(json);
    return d;
}

// Float-tolerance comparator. Avoids depending on Catch::Approx vs Approx
// across Catch2 versions; matches the project's existing test style of
// using plain == with hand-rolled helpers where needed.
constexpr bool approxEq(double a, double b, double eps = 1e-9) {
    return (a > b ? a - b : b - a) < eps;
}

} // namespace

// ============================================================================
// Happy path / basic accessors
// ============================================================================

TEST_CASE("JsonReader: basic accessors return values for well-formed input", "[json]") {
    auto doc = parse(R"({"sym":"BTC","tid":123,"price":"50000.5","active":true})");
    t2s::JsonReader r(doc);

    REQUIRE(r.string("sym").value() == "BTC");
    REQUIRE(r.int64("tid").value() == 123);
    REQUIRE(approxEq(r.priceString("price").value(), 50000.5));
    REQUIRE(r.boolean("active").value() == true);
    REQUIRE_FALSE(r.hasError());
}

TEST_CASE("JsonReader: drilling into nested objects", "[json]") {
    auto doc = parse(R"({"data":{"s":"BTC","t":42}})");
    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    REQUIRE(d.string("s").value() == "BTC");
    REQUIRE(d.int64("t").value() == 42);
    REQUIRE_FALSE(d.hasError());
    REQUIRE_FALSE(root.hasError());  // root unchanged
}

TEST_CASE("JsonReader: array accessor returns borrowed pointer", "[json]") {
    auto doc = parse(R"({"b":[["1.0","2.0"],["3.0","4.0"]]})");
    t2s::JsonReader r(doc);

    const auto* arr = r.array("b");
    REQUIRE(arr != nullptr);
    REQUIRE(arr->Size() == 2);
    REQUIRE_FALSE(r.hasError());
}

// ============================================================================
// Failure modes: missing keys, wrong types
// ============================================================================

TEST_CASE("JsonReader: missing key sets error and returns nullopt", "[json]") {
    auto doc = parse(R"({"s":"BTC"})");
    t2s::JsonReader r(doc);

    REQUIRE_FALSE(r.int64("missing").has_value());
    REQUIRE(r.hasError());
    REQUIRE(r.lastError().find("missing") != std::string::npos);
}

TEST_CASE("JsonReader: wrong type sets error", "[json]") {
    auto doc = parse(R"({"t":"not_a_number","n":42,"b":"not_bool"})");
    t2s::JsonReader r(doc);

    REQUIRE_FALSE(r.int64("t").has_value());      // string -> int64 fails
    REQUIRE_FALSE(r.string("n").has_value());     // number -> string fails
    REQUIRE_FALSE(r.boolean("b").has_value());    // string -> bool fails
    REQUIRE(r.hasError());
}

TEST_CASE("JsonReader: priceString rejects malformed numeric string", "[json]") {
    auto doc = parse(R"({"p":"not_a_price"})");
    t2s::JsonReader r(doc);

    REQUIRE_FALSE(r.priceString("p").has_value());
    REQUIRE(r.hasError());
    REQUIRE(r.lastError().find("strtod") != std::string::npos);
}

TEST_CASE("JsonReader: priceString accepts valid numeric strings", "[json]") {
    auto doc = parse(R"({"p1":"0.001234","p2":"50000","p3":"1e6"})");
    t2s::JsonReader r(doc);

    REQUIRE(approxEq(r.priceString("p1").value(), 0.001234));
    REQUIRE(approxEq(r.priceString("p2").value(), 50000.0));
    REQUIRE(approxEq(r.priceString("p3").value(), 1e6));
    REQUIRE_FALSE(r.hasError());
}

TEST_CASE("JsonReader: priceString rejects numeric (non-string) JSON value", "[json]") {
    // Binance sends prices as strings; if a number is sent, treat as schema error.
    auto doc = parse(R"({"p":50000.5})");
    t2s::JsonReader r(doc);

    REQUIRE_FALSE(r.priceString("p").has_value());
    REQUIRE(r.hasError());
    REQUIRE(r.lastError().find("not a string") != std::string::npos);
}

// ============================================================================
// Error semantics: first-wins, propagation, poisoning
// ============================================================================

TEST_CASE("JsonReader: first error wins, later failures do not overwrite", "[json]") {
    auto doc = parse(R"({})");
    t2s::JsonReader r(doc);

    (void)r.int64("first_missing");
    auto firstErr = r.lastError();
    REQUIRE_FALSE(firstErr.empty());

    (void)r.string("second_missing");
    REQUIRE(r.lastError() == firstErr);  // unchanged
}

TEST_CASE("JsonReader: nested obj propagates error from inner failure", "[json]") {
    auto doc = parse(R"({"data":"not_object"})");
    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    REQUIRE(d.hasError());
    REQUIRE_FALSE(d.int64("anything").has_value());
}

TEST_CASE("JsonReader: missing nested obj poisons returned reader", "[json]") {
    auto doc = parse(R"({})");
    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    REQUIRE(d.hasError());
    REQUIRE_FALSE(d.string("anything").has_value());
}

TEST_CASE("JsonReader: non-object root is poisoned at construction", "[json]") {
    auto doc = parse("[]");  // array, not object
    t2s::JsonReader r(doc);

    REQUIRE(r.hasError());
    REQUIRE_FALSE(r.int64("anything").has_value());
}

// ============================================================================
// parseLevelPair (free function)
// ============================================================================

TEST_CASE("parseLevelPair: parses well-formed [price, qty]", "[json][level]") {
    auto doc = parse(R"(["50000.5","1.234"])");
    auto p = t2s::parseLevelPair(doc);

    REQUIRE(p.has_value());
    REQUIRE(approxEq(p->first, 50000.5));
    REQUIRE(approxEq(p->second, 1.234));
}

TEST_CASE("parseLevelPair: rejects wrong shape", "[json][level]") {
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"("not_array")")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"([])")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"(["only_one"])")).has_value());
}

TEST_CASE("parseLevelPair: rejects non-string elements", "[json][level]") {
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"([50000.5,1.234])")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"(["50000.5",1.234])")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"([50000.5,"1.234"])")).has_value());
}

TEST_CASE("parseLevelPair: rejects unparseable numeric strings", "[json][level]") {
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"(["abc","1.0"])")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"(["1.0","xyz"])")).has_value());
    REQUIRE_FALSE(t2s::parseLevelPair(parse(R"(["",""])")).has_value());
}

TEST_CASE("parseLevelPair: tolerates extra elements (Binance future-compat)", "[json][level]") {
    // If Binance ever extends levels with extra fields, we should still
    // accept the first two as [price, qty].
    auto p = t2s::parseLevelPair(parse(R"(["50000.5","1.234","extra"])"));
    REQUIRE(p.has_value());
    REQUIRE(approxEq(p->first, 50000.5));
}
