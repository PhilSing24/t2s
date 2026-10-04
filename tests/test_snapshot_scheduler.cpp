/**
 * @file test_snapshot_scheduler.cpp
 * @brief Unit tests for SnapshotScheduler: backoff, weight budget, pauses.
 *
 * The scheduler is pure (time is an argument), so an hour-long REST outage
 * is simulated in microseconds.
 */

#include "snapshot_scheduler.hpp"
#include "catch_amalgamated.hpp"

#include <deque>

using t2s::SnapshotScheduler;
using t2s::SnapshotSchedulerConfig;

namespace {
auto noJitter = [] { return 0.0; };
SnapshotSchedulerConfig spotCfg() { return SnapshotSchedulerConfig{}; }   // weight 50, limit 6000
SnapshotSchedulerConfig futCfg() {
    SnapshotSchedulerConfig c; c.weightPerRequest = 20; c.weightLimitPerMin = 2400; return c;
}
} // namespace

TEST_CASE("First request is allowed and counted", "[sched]") {
    SnapshotScheduler s(3, spotCfg(), noJitter);
    REQUIRE(s.tryAcquire(0, 0));
    REQUIRE(s.requests() == 1);
    REQUIRE(s.failures() == 0);
}

TEST_CASE("A failure delays the next request for that symbol only", "[sched][backoff]") {
    SnapshotScheduler s(2, spotCfg(), noJitter);
    REQUIRE(s.tryAcquire(0, 0));
    s.onFailure(0, 100);
    REQUIRE_FALSE(s.tryAcquire(0, 100));
    REQUIRE_FALSE(s.tryAcquire(0, 1099));
    REQUIRE(s.tryAcquire(1, 200));          // other symbol unaffected
    REQUIRE(s.tryAcquire(0, 1100));         // 1 s after the failure
}

TEST_CASE("Backoff doubles and is capped at the maximum", "[sched][backoff]") {
    SnapshotScheduler s(1, spotCfg(), noJitter);
    std::int64_t now = 0;
    std::vector<std::int64_t> delays;
    for (int i = 0; i < 9; ++i) {
        s.onFailure(0, now);
        delays.push_back(s.nextAllowedMs(0) - now);
        now = s.nextAllowedMs(0);
    }
    REQUIRE(delays == std::vector<std::int64_t>{1000, 2000, 4000, 8000, 16000, 32000, 60000, 60000, 60000});
}

TEST_CASE("onSynced forgets the failures", "[sched][backoff]") {
    SnapshotScheduler s(1, spotCfg(), noJitter);
    for (int i = 0; i < 5; ++i) s.onFailure(0, 0);
    REQUIRE(s.consecutiveFailures(0) == 5);
    s.onSynced(0);
    REQUIRE(s.consecutiveFailures(0) == 0);
    REQUIRE(s.tryAcquire(0, 1));
    s.onFailure(0, 1);
    REQUIRE(s.nextAllowedMs(0) == 1001);    // back to the initial delay
}

TEST_CASE("Jitter stretches the delay by at most 25%", "[sched][backoff]") {
    SnapshotScheduler lo(1, spotCfg(), [] { return 0.0; });
    SnapshotScheduler hi(1, spotCfg(), [] { return 0.999; });
    lo.onFailure(0, 0); hi.onFailure(0, 0);
    REQUIRE(lo.nextAllowedMs(0) == 1000);
    REQUIRE(hi.nextAllowedMs(0) > 1200);
    REQUIRE(hi.nextAllowedMs(0) <= 1250);
}

TEST_CASE("Weight budget: a tenth of the limit, then a steady refill", "[sched][budget]") {
    SECTION("spot: 600 weight = 12 snapshots, then one every 5 s") {
        SnapshotScheduler s(100, spotCfg(), noJitter);
        REQUIRE(s.budgetWeightPerMin() == Catch::Approx(600.0));
        int granted = 0;
        for (int i = 0; i < 100; ++i) granted += s.tryAcquire(i, 0);
        REQUIRE(granted == 12);
        REQUIRE_FALSE(s.tryAcquire(50, 4999));
        REQUIRE(s.tryAcquire(50, 5000));
        REQUIRE_FALSE(s.tryAcquire(51, 5001));
    }
    SECTION("futures: 240 weight = 12 snapshots") {
        SnapshotScheduler s(100, futCfg(), noJitter);
        int granted = 0;
        for (int i = 0; i < 100; ++i) granted += s.tryAcquire(i, 0);
        REQUIRE(granted == 12);
    }
}

TEST_CASE("HTTP 429 and 418 pause every symbol", "[sched][pause]") {
    SECTION("429 with Retry-After") {
        SnapshotScheduler s(3, spotCfg(), noJitter);
        REQUIRE(s.tryAcquire(0, 0));
        s.onFailure(0, 0, 429, 120);
        REQUIRE(s.rateLimitPauses() == 1);
        REQUIRE_FALSE(s.tryAcquire(1, 119999));
        REQUIRE_FALSE(s.tryAcquire(2, 119999));
        REQUIRE(s.tryAcquire(1, 120000));
    }
    SECTION("429 without Retry-After waits at least 60 s") {
        SnapshotScheduler s(3, spotCfg(), noJitter);
        s.onFailure(0, 0, 429, 0);
        REQUIRE_FALSE(s.tryAcquire(1, 59999));
        REQUIRE(s.tryAcquire(1, 60000));
    }
    SECTION("418 (ban) waits at least 5 minutes") {
        SnapshotScheduler s(3, spotCfg(), noJitter);
        s.onFailure(0, 0, 418, 10);
        REQUIRE_FALSE(s.tryAcquire(1, 299999));
        REQUIRE(s.tryAcquire(1, 300000));
    }
    SECTION("an ordinary HTTP 500 does not pause the other symbols") {
        SnapshotScheduler s(3, spotCfg(), noJitter);
        s.onFailure(0, 0, 500, 0);
        REQUIRE(s.rateLimitPauses() == 0);
        REQUIRE(s.tryAcquire(1, 1));
    }
}

TEST_CASE("A high used-weight header pauses all requests for a minute", "[sched][pause]") {
    SnapshotScheduler s(2, spotCfg(), noJitter);
    REQUIRE(s.tryAcquire(0, 0));
    s.onFetchOk(0, 10, 2999);               // just under half of 6000
    REQUIRE(s.tryAcquire(1, 20));
    s.onFetchOk(1, 30, 3000);               // half the limit: someone else is using this IP
    REQUIRE(s.rateLimitPauses() == 1);
    REQUIRE_FALSE(s.tryAcquire(0, 60029));
    REQUIRE(s.tryAcquire(0, 60030));
}

TEST_CASE("An hour-long REST outage cannot cause a request storm", "[sched][storm]") {
    // 3 symbols, a delta every 100 ms each, every snapshot fails at once.
    // Before the scheduler this was up to 10 requests/s per symbol.
    for (auto cfg : {spotCfg(), futCfg()}) {
        SnapshotScheduler s(3, cfg);        // real jitter
        std::deque<std::int64_t> window;    // request times in the last 60 s
        std::size_t worstPerMinute = 0;
        long long total = 0;
        for (std::int64_t now = 0; now < 3600000; now += 100) {
            for (int sym = 0; sym < 3; ++sym) {
                if (s.tryAcquire(sym, now)) {
                    ++total;
                    window.push_back(now);
                    s.onFailure(sym, now + 50);
                }
            }
            while (!window.empty() && window.front() <= now - 60000) window.pop_front();
            worstPerMinute = std::max(worstPerMinute, window.size());
        }
        // Worst minute is the first one (1,2,4,8,16,32 s backoffs): 7 per symbol.
        REQUIRE(worstPerMinute <= 21);
        REQUIRE(worstPerMinute * cfg.weightPerRequest <= cfg.weightLimitPerMin * 0.2);
        // Steady state: one request per symbol per 60-75 s.
        REQUIRE(total <= 3 * (7 + 60));
        REQUIRE(total >= 3 * 45);
    }
}
