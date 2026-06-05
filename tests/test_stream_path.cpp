/**
 * @file test_stream_path.cpp
 * @brief Unit tests for t2s::buildStreamPath.
 *
 * Locks the WebSocket stream-path string-building against silent drift
 * during the MarketConfig refactor (ADR-013 step 3) and beyond. The
 * function is pure — symbol list + suffix in, path string out — so the
 * tests are deterministic and require no Catch2/kdb/Boost link beyond
 * the amalgamation itself.
 *
 * Run directly: ./build/test_stream_path
 */

#include "market_config.hpp"
#include "catch_amalgamated.hpp"

#include <string>
#include <vector>

TEST_CASE("buildStreamPath produces single-symbol spot path", "[stream_path]") {
    REQUIRE(t2s::buildStreamPath({"btcusdt"}, "@trade")
            == "/stream?streams=btcusdt@trade");
}

TEST_CASE("buildStreamPath produces multi-symbol spot path with slash separators",
          "[stream_path]") {
    REQUIRE(t2s::buildStreamPath({"btcusdt", "ethusdt", "solusdt"}, "@trade")
            == "/stream?streams=btcusdt@trade/ethusdt@trade/solusdt@trade");
}

TEST_CASE("buildStreamPath produces single-symbol futures aggTrade path",
          "[stream_path]") {
    REQUIRE(t2s::buildStreamPath({"btcusdt"}, "@aggTrade")
            == "/stream?streams=btcusdt@aggTrade");
}

TEST_CASE("buildStreamPath produces multi-symbol futures aggTrade path",
          "[stream_path]") {
    REQUIRE(t2s::buildStreamPath({"btcusdt", "ethusdt"}, "@aggTrade")
            == "/stream?streams=btcusdt@aggTrade/ethusdt@aggTrade");
}

TEST_CASE("buildStreamPath with empty symbols list yields prefix only",
          "[stream_path]") {
    // Caller is expected to refuse to start with no symbols; we still
    // check the function's output is well-formed.
    REQUIRE(t2s::buildStreamPath({}, "@trade") == "/stream?streams=");
}

TEST_CASE("buildStreamPath preserves symbol case verbatim", "[stream_path]") {
    // Binance expects lowercase; this is a contract for the caller, not
    // the function. The function does no normalisation.
    REQUIRE(t2s::buildStreamPath({"BTCUSDT"}, "@trade")
            == "/stream?streams=BTCUSDT@trade");
}

TEST_CASE("buildStreamPath does not add a leading @ to the suffix", "[stream_path]") {
    // The suffix is included as-is; missing @ is a config bug we want
    // to be visible in the resulting path rather than silently fixed.
    REQUIRE(t2s::buildStreamPath({"btcusdt"}, "trade")
            == "/stream?streams=btcusdttrade");
}

TEST_CASE("MarketConfig defaults select spot Binance", "[market_config]") {
    t2s::MarketConfig m;
    REQUIRE(m.host         == "stream.binance.com");
    REQUIRE(m.port         == "9443");
    REQUIRE(m.streamSuffix == "@trade");
    REQUIRE(m.tpTable      == "trade_binance");
    REQUIRE(m.schema       == t2s::TradeSchema::SpotTrade);
}
