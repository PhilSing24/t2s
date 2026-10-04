/**
 * @file test_trade_gap.cpp
 * @brief Unit tests for trade-id gap detection and the trade_gap event row.
 */

#include "trade_gap.hpp"
#include "catch_amalgamated.hpp"

#include <string>

using t2s::TradeIdTracker;
using Kind = t2s::TradeIdTracker::Kind;

TEST_CASE("TradeIdTracker: consecutive ids are in order; the first has no predecessor", "[gap][tracker]") {
    TradeIdTracker t;
    REQUIRE(t.onId("BTCUSDT", 100).kind == Kind::First);
    REQUIRE(t.onId("BTCUSDT", 101).kind == Kind::InOrder);
    REQUIRE(t.onId("BTCUSDT", 102).kind == Kind::InOrder);
    REQUIRE(t.last("BTCUSDT") == 102);
    REQUIRE_FALSE(t.known("ETHUSDT"));
}

TEST_CASE("TradeIdTracker: a jump is a gap with its exact missing range", "[gap][tracker]") {
    TradeIdTracker t;
    t.onId("BTCUSDT", 100);
    auto r = t.onId("BTCUSDT", 105);
    REQUIRE(r.kind == Kind::Gap);
    REQUIRE(r.firstMissing == 101);
    REQUIRE(r.lastMissing == 104);
    REQUIRE(r.previous == 100);
    REQUIRE(t.onId("BTCUSDT", 106).kind == Kind::InOrder);     // the gap is reported once

    auto one = t.onId("BTCUSDT", 108);                          // a single missing id
    REQUIRE(one.kind == Kind::Gap);
    REQUIRE(one.firstMissing == 107);
    REQUIRE(one.lastMissing == 107);
}

TEST_CASE("TradeIdTracker: duplicates and late ids are not gaps and do not move the mark back", "[gap][tracker]") {
    TradeIdTracker t;
    t.onId("BTCUSDT", 100); t.onId("BTCUSDT", 101);
    REQUIRE(t.onId("BTCUSDT", 101).kind == Kind::Duplicate);
    REQUIRE(t.onId("BTCUSDT", 99).kind == Kind::OutOfOrder);
    REQUIRE(t.last("BTCUSDT") == 101);
    REQUIRE(t.onId("BTCUSDT", 102).kind == Kind::InOrder);      // no false gap after the late id
}

TEST_CASE("TradeIdTracker: symbols are independent", "[gap][tracker]") {
    TradeIdTracker t;
    t.onId("BTCUSDT", 100); t.onId("ETHUSDT", 5000);
    REQUIRE(t.onId("ETHUSDT", 5001).kind == Kind::InOrder);
    REQUIRE(t.onId("BTCUSDT", 103).kind == Kind::Gap);
}

TEST_CASE("TradeIdTracker: a seeded last id makes the first live id comparable", "[gap][tracker]") {
    // What a restarted handler does with the last id TP has logged
    TradeIdTracker t;
    t.seed("BTCUSDT", 500);
    auto r = t.onId("BTCUSDT", 531);
    REQUIRE(r.kind == Kind::Gap);
    REQUIRE(r.firstMissing == 501);
    REQUIRE(r.lastMissing == 530);

    TradeIdTracker u;
    u.seed("BTCUSDT", 500);
    REQUIRE(u.onId("BTCUSDT", 501).kind == Kind::InOrder);      // nothing was missed
}

TEST_CASE("TradeGap bookkeeping", "[gap]") {
    t2s::TradeGap g; g.sym = "BTCUSDT"; g.firstId = 101; g.lastId = 104;
    REQUIRE(g.missing() == 4);
    REQUIRE(g.nextNeededId() == 101);
    g.recovered = 2; g.recoveredThroughId = 102;
    REQUIRE(g.nextNeededId() == 103);
}

TEST_CASE("trade_gap row is in schema order", "[gap][row]") {
    t2s::TradeGap g; g.sym = "ETHUSDT"; g.firstId = 101; g.lastId = 104;
    const long long now = 1700000000000000000LL;

    K d = t2s::buildGapRow(now, g, "trade_binance", t2s::GapStatus::Detected, "");
    REQUIRE(d->t == 0);
    REQUIRE(d->n == t2s::GAP_ROW_WIDTH);
    K* f = kK(d);
    REQUIRE(f[0]->t == -KP);  REQUIRE(f[0]->j == now - t2s::GAP_KDB_EPOCH_OFFSET_NS);
    REQUIRE(std::string(f[1]->s) == "ETHUSDT");
    REQUIRE(std::string(f[2]->s) == "trade_binance");
    REQUIRE(f[3]->j == 101);
    REQUIRE(f[4]->j == 104);
    REQUIRE(f[5]->j == 4);
    REQUIRE(std::string(f[6]->s) == "detected");
    REQUIRE(f[7]->j == 0);
    REQUIRE(f[8]->j == t2s::GAP_NULL_LONG);          // nothing recovered yet: null
    REQUIRE(std::string(f[9]->s) == "");
    r0(d);

    g.recovered = 4; g.recoveredThroughId = 104;
    K r = t2s::buildGapRow(now, g, "trade_binance_fut", t2s::GapStatus::Recovered, "");
    REQUIRE(std::string(kK(r)[2]->s) == "trade_binance_fut");
    REQUIRE(std::string(kK(r)[6]->s) == "recovered");
    REQUIRE(kK(r)[7]->j == 4);
    REQUIRE(kK(r)[8]->j == 104);
    r0(r);

    K u = t2s::buildGapRow(now, g, "trade_binance", t2s::GapStatus::Unrecoverable, "tooLarge");
    REQUIRE(std::string(kK(u)[6]->s) == "unrecoverable");
    REQUIRE(std::string(kK(u)[9]->s) == "tooLarge");
    r0(u);
    REQUIRE(std::string(t2s::gapStatusName(t2s::GapStatus::Partial)) == "partial");
}

TEST_CASE("GapEventQueue keeps events while TP is unreachable", "[gap][queue]") {
    std::atomic<bool> running{true};
    t2s::TpPublisherConfig cfg; cfg.table = "trade_binance"; cfg.width = 12;
    t2s::TpPublisher tp(cfg, running);              // never connected
    t2s::GapEventQueue q;
    t2s::TradeGap g; g.sym = "BTCUSDT"; g.firstId = 1; g.lastId = 2;
    q.push(t2s::buildGapRow(0, g, "trade_binance", t2s::GapStatus::Detected, ""));
    q.push(t2s::buildGapRow(0, g, "trade_binance", t2s::GapStatus::Recovered, ""));
    REQUIRE_FALSE(q.flush(tp));
    REQUIRE(q.size() == 2);                         // nothing dropped
    REQUIRE(q.acked() == 0);
}
