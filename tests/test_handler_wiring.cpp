/**
 * @file test_handler_wiring.cpp
 * @brief The real handler objects are wired from their market config.
 *
 * The sandbox tests drive the backfill through a fake exchange, so nothing
 * there notices if the handler hands its REST client the wrong endpoint.
 * (The first live check did: the constructor read the host from a config
 * object it had just moved from, and every backfill request failed.)
 */

#include "trade_feed_handler.hpp"
#include "quote_feed_handler.hpp"
#include "catch_amalgamated.hpp"

TEST_CASE("Trade handler: backfill REST endpoint comes from the market config", "[wiring]") {
    t2s::MarketConfig spot;
    spot.backfillEnabled = true;
    spot.backfillRestHost = "api.binance.com";
    spot.backfillRestPath = "/api/v3/historicalTrades";
    TradeFeedHandler h({"btcusdt"}, spot, "localhost", 15999);
    REQUIRE(h.backfillRestHost() == "api.binance.com");
    REQUIRE(h.backfillRestPath() == "/api/v3/historicalTrades");

    t2s::MarketConfig fut;
    fut.schema = t2s::TradeSchema::FuturesAggTrade;
    fut.tpTable = "trade_binance_fut";
    fut.backfillEnabled = true;
    fut.backfillRestHost = "fapi.binance.com";
    fut.backfillRestPath = "/fapi/v1/aggTrades";
    TradeFeedHandler f({"btcusdt"}, fut, "localhost", 15999);
    REQUIRE(f.backfillRestHost() == "fapi.binance.com");
    REQUIRE(f.backfillRestPath() == "/fapi/v1/aggTrades");
}

TEST_CASE("A REST client without a host refuses to send", "[wiring]") {
    RestClient c("", "443", "/x");
    auto r = c.get("/x?y=1");
    REQUIRE(r.status == 0);
    REQUIRE(r.error == "REST client has no host configured");
}
