/**
 * @file test_trade_backfill.cpp
 * @brief Unit tests for TradeBackfill with a fake fetcher (no network):
 *        paging, id checking, failures, resume, and the REST reply parser.
 */

#include "trade_backfill.hpp"
#include "catch_amalgamated.hpp"

#include <map>
#include <string>
#include <vector>

using t2s::BackfillPage;
using t2s::BackfillTrade;
using t2s::GapStatus;
using t2s::TradeGap;

namespace {

/// Serves ids [servedFrom, servedTo]; can fail the first N calls, etc.
struct FakeFetcher {
    long long servedFrom = 1, servedTo = 1000000000;
    int failFirst = 0;             // transport failures before working
    int status429First = 0;        // HTTP 429 replies before working
    bool tooOld = false;
    int calls = 0;
    std::vector<std::pair<long long, int>> requests;

    BackfillPage fetch(const std::string&, long long fromId, int limit) {
        ++calls;
        requests.emplace_back(fromId, limit);
        BackfillPage p;
        p.recvTimeUtcNs = 1700000000000000000LL + calls;
        if (tooOld) { p.httpStatus = 400; p.tooOld = true; p.error = "HTTP 400 -4166"; return p; }
        if (status429First > 0) { --status429First; p.httpStatus = 429; p.retryAfterSec = 7; p.error = "HTTP 429"; return p; }
        if (failFirst > 0) { --failFirst; p.error = "connection refused"; return p; }
        p.ok = true; p.httpStatus = 200;
        long long start = std::max(fromId, servedFrom);     // spot: an old id returns the oldest available
        for (long long id = start; id <= servedTo && static_cast<int>(p.trades.size()) < limit; ++id) {
            BackfillTrade t; t.id = id; t.price = 100.0 + id; t.qty = 1.0; t.tradeTimeMs = 1000 + id;
            p.trades.push_back(t);
        }
        return p;
    }
};

struct Sink {
    std::vector<long long> ids;
    std::vector<std::string> events;           // "status:recovered:through:reason"
    auto publish() { return [this](const std::string&, const BackfillTrade& t, long long) { ids.push_back(t.id); }; }
    auto record() {
        return [this](const TradeGap& g, GapStatus s, const std::string& reason) {
            events.push_back(std::string(t2s::gapStatusName(s)) + ":" + std::to_string(g.recovered) + ":" +
                             std::to_string(g.recoveredThroughId) + ":" + reason);
        };
    }
};

t2s::TradeBackfillConfig cfg(int pageLimit = 1000) {
    t2s::TradeBackfillConfig c;
    c.inlineFetch = true;
    c.pageLimit = pageLimit;
    c.sched.weightPerRequest = 25; c.sched.weightLimitPerMin = 6000;
    c.sched.jitterFraction = 0.0;
    return c;
}
TradeGap gap(long long first, long long last, const char* sym = "BTCUSDT") {
    TradeGap g; g.sym = sym; g.firstId = first; g.lastId = last; return g;
}
std::vector<long long> range(long long a, long long b) { std::vector<long long> v; for (long long i = a; i <= b; ++i) v.push_back(i); return v; }

} // namespace

TEST_CASE("A small gap is fetched in one page and reported recovered", "[backfill]") {
    FakeFetcher f; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
    bf.addGap(gap(101, 140));
    bf.pump(0, s.publish(), s.record());
    REQUIRE(s.ids == range(101, 140));
    REQUIRE(s.events == std::vector<std::string>{"recovered:40:140:"});
    REQUIRE(f.requests == std::vector<std::pair<long long, int>>{{101, 40}});   // asks only for what is missing
    REQUIRE(bf.openGaps() == 0);
    REQUIRE(bf.tradesBackfilled() == 40);
    REQUIRE(bf.gapsRecovered() == 1);
}

TEST_CASE("A larger gap is paged, with a partial event after every page", "[backfill]") {
    FakeFetcher f; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg(100));
    bf.addGap(gap(1000, 1249));
    bf.pump(0, s.publish(), s.record());
    REQUIRE(s.ids == range(1000, 1249));
    REQUIRE(s.events == std::vector<std::string>{"partial:100:1099:", "partial:200:1199:", "recovered:250:1249:"});
    REQUIRE(f.requests == std::vector<std::pair<long long, int>>{{1000, 100}, {1100, 100}, {1200, 50}});
}

TEST_CASE("Rows past the gap's end are not published", "[backfill]") {
    struct Greedy : FakeFetcher {
        BackfillPage fetch(const std::string& s, long long fromId, int) { return FakeFetcher::fetch(s, fromId, 1000); }
    } f;
    Sink s;
    t2s::TradeBackfill<Greedy> bf(f, cfg());
    bf.addGap(gap(10, 12));
    bf.pump(0, s.publish(), s.record());
    REQUIRE(s.ids == range(10, 12));
}

TEST_CASE("The weight budget spaces the requests", "[backfill][ratelimit]") {
    FakeFetcher f; Sink s;
    auto c = cfg(10);                       // budget 600 weight = 24 requests at once, then one per 2.5 s
    t2s::TradeBackfill<FakeFetcher> bf(f, c);
    bf.addGap(gap(1, 1000));                // 100 pages
    bf.pump(0, s.publish(), s.record());
    REQUIRE(f.calls == 24);
    REQUIRE(bf.openGaps() == 1);
    bf.pump(2499, s.publish(), s.record());
    REQUIRE(f.calls == 24);
    bf.pump(2500, s.publish(), s.record());
    REQUIRE(f.calls == 25);
    std::int64_t now = 2500;
    while (bf.openGaps() > 0 && now < 600000) { now += 100; bf.pump(now, s.publish(), s.record()); }
    REQUIRE(s.ids == range(1, 1000));
    REQUIRE(now >= 2500 + 75 * 2500 - 100);  // never faster than the budget
}

TEST_CASE("A failed request is retried with backoff and loses nothing", "[backfill][failure]") {
    FakeFetcher f; f.failFirst = 2; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
    bf.addGap(gap(1, 5));
    bf.pump(0, s.publish(), s.record());
    REQUIRE(f.calls == 1);
    REQUIRE(s.ids.empty());
    bf.pump(999, s.publish(), s.record());   REQUIRE(f.calls == 1);    // 1 s backoff
    bf.pump(1000, s.publish(), s.record());  REQUIRE(f.calls == 2);
    bf.pump(2999, s.publish(), s.record());  REQUIRE(f.calls == 2);    // 2 s backoff
    bf.pump(3000, s.publish(), s.record());  REQUIRE(f.calls == 3);
    REQUIRE(s.ids == range(1, 5));
    REQUIRE(s.events == std::vector<std::string>{"recovered:5:5:"});
    REQUIRE(bf.pagesFailed() == 2);
}

TEST_CASE("HTTP 429 pauses for Retry-After (at least 60 s)", "[backfill][ratelimit]") {
    FakeFetcher f; f.status429First = 1; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
    bf.addGap(gap(1, 5));
    bf.pump(0, s.publish(), s.record());
    bf.pump(59999, s.publish(), s.record());
    REQUIRE(f.calls == 1);
    bf.pump(60000, s.publish(), s.record());
    REQUIRE(s.ids == range(1, 5));
    REQUIRE(bf.scheduler().rateLimitPauses() == 1);
}

TEST_CASE("A gap that keeps failing becomes unrecoverable: restFailed", "[backfill][failure]") {
    FakeFetcher f; f.failFirst = 1000; Sink s;
    auto c = cfg(); c.maxFailures = 3;
    t2s::TradeBackfill<FakeFetcher> bf(f, c);
    bf.addGap(gap(1, 5));
    for (std::int64_t now = 0; now < 600000 && bf.openGaps() > 0; now += 500) bf.pump(now, s.publish(), s.record());
    REQUIRE(s.ids.empty());
    REQUIRE(s.events == std::vector<std::string>{"unrecoverable:0:0:restFailed"});
    REQUIRE(f.calls == 3);
    REQUIRE(bf.gapsUnrecoverable() == 1);
}

TEST_CASE("Unrecoverable reasons: tooLarge, tooOld, notServed, backfillDisabled", "[backfill][unrecoverable]") {
    SECTION("tooLarge: no request is made") {
        FakeFetcher f; Sink s; auto c = cfg(); c.maxGapIds = 100;
        t2s::TradeBackfill<FakeFetcher> bf(f, c);
        bf.addGap(gap(1, 101));
        bf.addGap(gap(500, 599));               // exactly at the cap: fine
        bf.pump(0, s.publish(), s.record());
        REQUIRE(s.events == std::vector<std::string>{"unrecoverable:0:0:tooLarge", "recovered:100:599:"});
        REQUIRE(f.requests.front().first == 500);
    }
    SECTION("tooOld: the futures endpoint refuses the range") {
        FakeFetcher f; f.tooOld = true; Sink s;
        t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
        bf.addGap(gap(1, 5));
        bf.pump(0, s.publish(), s.record());
        REQUIRE(s.events == std::vector<std::string>{"unrecoverable:0:0:tooOld"});
        REQUIRE(f.calls == 1);                  // no retry storm
    }
    SECTION("notServed: the exchange answers with later ids than asked") {
        FakeFetcher f; f.servedFrom = 50; Sink s;
        t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
        bf.addGap(gap(10, 20));
        bf.pump(0, s.publish(), s.record());
        REQUIRE(s.ids.empty());                 // ids 50.. are NOT published as if they were 10..
        REQUIRE(s.events == std::vector<std::string>{"unrecoverable:0:0:notServed"});
    }
    SECTION("notServed after a partial recovery keeps what was recovered") {
        FakeFetcher f; f.servedTo = 14; Sink s;
        auto c = cfg(); c.maxFailures = 2;
        t2s::TradeBackfill<FakeFetcher> bf(f, c);
        bf.addGap(gap(10, 20));
        for (std::int64_t now = 0; now < 600000 && bf.openGaps() > 0; now += 500) bf.pump(now, s.publish(), s.record());
        REQUIRE(s.ids == range(10, 14));
        REQUIRE(s.events == std::vector<std::string>{"partial:5:14:", "unrecoverable:5:14:notServed"});
    }
    SECTION("backfill disabled") {
        FakeFetcher f; Sink s; auto c = cfg(); c.enabled = false;
        t2s::TradeBackfill<FakeFetcher> bf(f, c);
        bf.addGap(gap(1, 5));
        bf.pump(0, s.publish(), s.record());
        REQUIRE(s.events == std::vector<std::string>{"unrecoverable:0:0:backfillDisabled"});
        REQUIRE(f.calls == 0);
    }
}

TEST_CASE("A too-large gap behind another gap gets no request at all", "[backfill][unrecoverable]") {
    // Regression (live check 2026-10-04): the size check ran only at the top
    // of the loop, so a gap that became the head when its predecessor
    // finished had pages requested before the cap was applied.
    FakeFetcher f; Sink s; auto c = cfg(10); c.maxGapIds = 100;
    t2s::TradeBackfill<FakeFetcher> bf(f, c);
    bf.addGap(gap(1, 30, "SOLUSDT"));          // fine: 3 pages
    bf.addGap(gap(1000, 1500, "BTCUSDT"));     // 501 ids: above the cap
    bf.addGap(gap(5000, 5009, "ETHUSDT"));     // fine: 1 page
    bf.pump(0, s.publish(), s.record());
    std::vector<long long> want = range(1, 30); for (long long i = 5000; i <= 5009; ++i) want.push_back(i);
    REQUIRE(s.ids == want);                                                   // no id of the large gap
    REQUIRE(f.requests == std::vector<std::pair<long long, int>>{{1, 10}, {11, 10}, {21, 10}, {5000, 10}});
    REQUIRE(s.events == std::vector<std::string>{"partial:10:10:", "partial:20:20:", "recovered:30:30:",
                                                 "unrecoverable:0:0:tooLarge", "recovered:10:5009:"});
}

TEST_CASE("A resumed gap continues after what was already recovered", "[backfill][resume]") {
    FakeFetcher f; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
    TradeGap g = gap(100, 199); g.recovered = 60; g.recoveredThroughId = 159;     // from TP's trade state
    bf.addGap(g);
    bf.pump(0, s.publish(), s.record());
    REQUIRE(s.ids == range(160, 199));          // 100..159 are not fetched or published again
    REQUIRE(s.events == std::vector<std::string>{"recovered:100:199:"});

    Sink s2;
    TradeGap done = gap(100, 199); done.recovered = 100; done.recoveredThroughId = 199;
    bf.addGap(done);                            // rows all logged, only the final event was lost
    bf.pump(0, s2.publish(), s2.record());
    REQUIRE(s2.ids.empty());
    REQUIRE(s2.events == std::vector<std::string>{"recovered:100:199:"});
}

TEST_CASE("Several gaps are handled in order", "[backfill]") {
    FakeFetcher f; Sink s;
    t2s::TradeBackfill<FakeFetcher> bf(f, cfg());
    bf.addGap(gap(10, 12, "BTCUSDT"));
    bf.addGap(gap(500, 501, "ETHUSDT"));
    bf.pump(0, s.publish(), s.record());
    REQUIRE(s.ids == std::vector<long long>{10, 11, 12, 500, 501});
    REQUIRE(s.events.size() == 2);
}

TEST_CASE("The worker thread fetches off the publishing thread", "[backfill][thread]") {
    FakeFetcher f; Sink s;
    auto c = cfg(50); c.inlineFetch = false;
    t2s::TradeBackfill<FakeFetcher> bf(f, c);
    bf.start();
    bf.addGap(gap(1, 120));
    auto t0 = std::chrono::steady_clock::now();
    while (bf.openGaps() > 0 && std::chrono::steady_clock::now() - t0 < std::chrono::seconds(5)) {
        bf.pump(0, s.publish(), s.record());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    bf.stop();
    REQUIRE(s.ids == range(1, 120));
    REQUIRE(s.events.back() == "recovered:120:120:");
}

// ----------------------------------------------------------------------------
// REST reply parsing (bodies captured from Binance on 2026-10-04)
// ----------------------------------------------------------------------------

TEST_CASE("Spot historicalTrades body parses", "[backfill][parse]") {
    const std::string body = R"([{"id":6734512354,"price":"85245.16000000","qty":"0.00217000","quoteQty":"184.98199720","time":1791122729085,"isBuyerMaker":true,"isBestMatch":true},)"
                             R"({"id":6734512355,"price":"85245.17000000","qty":"0.00063000","quoteQty":"53.70445080","time":1791122731476,"isBuyerMaker":false,"isBestMatch":true}])";
    std::vector<BackfillTrade> out; std::string err;
    REQUIRE(t2s::parseBackfillBody(body, t2s::TradeSchema::SpotTrade, out, err));
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].id == 6734512354LL);
    REQUIRE(out[0].price == Catch::Approx(85245.16));
    REQUIRE(out[0].qty == Catch::Approx(0.00217));
    REQUIRE(out[0].tradeTimeMs == 1791122729085LL);
    REQUIRE(out[0].buyerIsMaker);
    REQUIRE_FALSE(out[1].buyerIsMaker);
    REQUIRE(std::isnan(out[0].qtyExRpi));
}

TEST_CASE("Futures aggTrades body parses, with and without nq", "[backfill][parse]") {
    const std::string body = R"([{"a":3474562343,"p":"85282.40","q":"0.014","nq":"0.010","f":8144200001,"l":8144200003,"T":1791122000123,"m":true},)"
                             R"({"a":3474562344,"p":"85282.50","q":"0.002","f":8144200004,"l":8144200004,"T":1791122000456,"m":false}])";
    std::vector<BackfillTrade> out; std::string err;
    REQUIRE(t2s::parseBackfillBody(body, t2s::TradeSchema::FuturesAggTrade, out, err));
    REQUIRE(out.size() == 2);
    REQUIRE(out[0].id == 3474562343LL);
    REQUIRE(out[0].qtyExRpi == Catch::Approx(0.010));
    REQUIRE(out[0].firstTradeId == 8144200001LL);
    REQUIRE(out[0].lastTradeId == 8144200003LL);
    REQUIRE(std::isnan(out[1].qtyExRpi));
}

TEST_CASE("An empty array, an error object and garbage", "[backfill][parse]") {
    std::vector<BackfillTrade> out; std::string err;
    REQUIRE(t2s::parseBackfillBody("[]", t2s::TradeSchema::SpotTrade, out, err));
    REQUIRE(out.empty());
    REQUIRE_FALSE(t2s::parseBackfillBody(R"({"code":-4166,"msg":"Search window is restricted to recent 2 days only."})", t2s::TradeSchema::FuturesAggTrade, out, err));
    REQUIRE_FALSE(t2s::parseBackfillBody("<html>", t2s::TradeSchema::SpotTrade, out, err));
    REQUIRE_FALSE(t2s::parseBackfillBody(R"([{"id":1,"price":"x","qty":"1","time":2,"isBuyerMaker":true}])", t2s::TradeSchema::SpotTrade, out, err));
}

TEST_CASE("RestTradeFetcher builds the request and classifies the reply", "[backfill][fetcher]") {
    struct FakeHttp {
        struct R { int status = 200; std::string body = "[]"; int retryAfterSec = 0; int usedWeight1m = -1; std::string error; };
        R next; std::string lastTarget;
        R get(const std::string& target) { lastTarget = target; return next; }
    } http;
    t2s::RestTradeFetcher<FakeHttp> spot(http, "/api/v3/historicalTrades", t2s::TradeSchema::SpotTrade);
    BackfillPage p = spot.fetch("BTCUSDT", 1234, 500);
    REQUIRE(http.lastTarget == "/api/v3/historicalTrades?symbol=BTCUSDT&fromId=1234&limit=500");
    REQUIRE(p.ok);
    REQUIRE(p.trades.empty());
    REQUIRE(p.recvTimeUtcNs > 0);

    http.next.status = 400; http.next.body = R"({"code":-4166,"msg":"Search window is restricted to recent 2 days only."})";
    t2s::RestTradeFetcher<FakeHttp> fut(http, "/fapi/v1/aggTrades", t2s::TradeSchema::FuturesAggTrade);
    p = fut.fetch("BTCUSDT", 1, 1000);
    REQUIRE_FALSE(p.ok);
    REQUIRE(p.tooOld);

    http.next.status = 429; http.next.retryAfterSec = 30; http.next.body = "{}";
    p = fut.fetch("BTCUSDT", 1, 1000);
    REQUIRE_FALSE(p.ok);
    REQUIRE_FALSE(p.tooOld);
    REQUIRE(p.httpStatus == 429);
    REQUIRE(p.retryAfterSec == 30);

    http.next = FakeHttp::R{}; http.next.status = 0; http.next.error = "timeout";
    p = spot.fetch("BTCUSDT", 1, 1);
    REQUIRE_FALSE(p.ok);
    REQUIRE(p.error == "timeout");
}
