/**
 * @file test_row_clock.cpp
 * @brief Unit tests for the rule that decides a row's `time` when the
 *        local clock lags the exchange (row_clock.hpp).
 */

#include "row_clock.hpp"
#include "catch_amalgamated.hpp"

namespace {
constexpr long long MS = 1000000LL;                    // ns per ms
constexpr long long T0 = 1791157149000LL;              // an exchange time, ms
} // namespace

TEST_CASE("A row received after its event keeps the receive time", "[rowclock]") {
    // normal running: received 270 ms after the event
    auto s = t2s::stampRowTime((T0 + 270) * MS + 123, T0, 2000);
    REQUIRE_FALSE(s.corrected);
    REQUIRE(s.timeUtcNs == (T0 + 270) * MS + 123);
    REQUIRE(s.lagMs == 0 - 270);
}

TEST_CASE("A clock behind by more than the threshold takes the event time", "[rowclock]") {
    // the wake of 2026-10-04: clock 6 minutes behind
    const long long recv = (T0 - 371000) * MS + 456;
    auto s = t2s::stampRowTime(recv, T0, 2000);
    REQUIRE(s.corrected);
    REQUIRE(s.timeUtcNs == T0 * MS);
    REQUIRE(s.timeUtcNs != recv);                      // the marker: time differs from the receive time
    REQUIRE(s.lagMs == 370999);
}

TEST_CASE("The threshold is exclusive and small lags are left alone", "[rowclock]") {
    REQUIRE_FALSE(t2s::stampRowTime((T0 - 2000) * MS, T0, 2000).corrected);     // exactly 2 s
    REQUIRE(t2s::stampRowTime((T0 - 2000) * MS - 1, T0, 2000).corrected);       // 1 ns more
    REQUIRE_FALSE(t2s::stampRowTime((T0 - 500) * MS, T0, 2000).corrected);
    REQUIRE(t2s::stampRowTime((T0 - 500) * MS, T0, 100).corrected);             // configurable
}

TEST_CASE("A clock ahead of the exchange is never corrected", "[rowclock]") {
    // late delivery and a fast clock look the same: receive time after the event
    auto s = t2s::stampRowTime((T0 + 600000) * MS, T0, 2000);
    REQUIRE_FALSE(s.corrected);
    REQUIRE(s.timeUtcNs == (T0 + 600000) * MS);
}

TEST_CASE("Without an event time the receive time is kept", "[rowclock]") {
    const long long nullLong = static_cast<long long>(0x8000000000000000ULL);
    for (long long ev : {0LL, nullLong, -5LL}) {
        auto s = t2s::stampRowTime(123456789, ev, 2000);
        REQUIRE_FALSE(s.corrected);
        REQUIRE(s.timeUtcNs == 123456789);
        REQUIRE(s.lagMs == 0);
    }
}

TEST_CASE("RowClock: a row without an event time uses the most recent one", "[rowclock]") {
    t2s::RowClock c(2000);
    const long long nullLong = static_cast<long long>(0x8000000000000000ULL);
    const long long stale = (T0 - 300000) * MS;

    // nothing seen yet: cannot judge, receive time kept
    REQUIRE_FALSE(c.stamp(stale, 0).corrected);

    c.noteEvent(T0);                                   // an event that published no row
    auto inv = c.stamp(stale, 0);                      // invalid quote row
    REQUIRE(inv.corrected);
    REQUIRE(inv.timeUtcNs == T0 * MS);
    auto bf = c.stamp(stale + 5, nullLong);            // backfilled trade
    REQUIRE(bf.corrected);
    REQUIRE(bf.timeUtcNs == T0 * MS);
    REQUIRE(c.correctedRows() == 2);

    // clock fine: the most recent event is slightly in the past, no correction
    REQUIRE_FALSE(c.stamp((T0 + 40) * MS, 0).corrected);
}

TEST_CASE("RowClock: a row with its own event time uses it, not an older or newer one", "[rowclock]") {
    t2s::RowClock c(2000);
    c.noteEvent(T0 + 900000);                          // some other, later event
    auto s = c.stamp((T0 + 100) * MS, T0);             // own event time: clock is fine for this row
    REQUIRE_FALSE(s.corrected);
    REQUIRE(c.lastEventMs() == T0);                    // most recent, not the maximum
}

TEST_CASE("RowClock: one bad event time does not affect the rows after it", "[rowclock]") {
    t2s::RowClock c(2000);
    const long long nullLong = static_cast<long long>(0x8000000000000000ULL);
    REQUIRE_FALSE(c.stamp((T0 + 100) * MS, T0).corrected);
    auto bad = c.stamp((T0 + 200) * MS, T0 + 86400000);            // exchange sends a time a day ahead
    REQUIRE(bad.corrected);
    auto next = c.stamp((T0 + 300) * MS, T0 + 150);                // next event is sane
    REQUIRE_FALSE(next.corrected);
    REQUIRE(next.timeUtcNs == (T0 + 300) * MS);
    REQUIRE_FALSE(c.stamp((T0 + 310) * MS, nullLong).corrected);   // and so is a row without one
    REQUIRE(c.correctedRows() == 1);
}

TEST_CASE("RowClock: counts corrected rows and reports the start and end of a lag", "[rowclock]") {
    t2s::RowClock c(2000);
    REQUIRE(c.thresholdMs() == 2000);
    REQUIRE(c.stamp((T0 + 100) * MS, T0).edge == t2s::ClockEdge::None);
    REQUIRE_FALSE(c.lagging());

    // clock falls 5 minutes behind: 3 rows
    const long long behind = 300000;
    auto a = c.stamp((T0 + 1000 - behind) * MS, T0 + 900);
    REQUIRE(a.edge == t2s::ClockEdge::LagStarted);
    REQUIRE(c.lagging());
    REQUIRE(c.stamp((T0 + 1100 - behind) * MS, T0 + 1000).edge == t2s::ClockEdge::None);
    REQUIRE(c.stamp((T0 + 1200 - behind) * MS, T0 + 1100).corrected);
    REQUIRE(c.correctedRows() == 3);

    // clock stepped back in line
    auto e = c.stamp((T0 + 1400) * MS, T0 + 1200);
    REQUIRE_FALSE(e.corrected);
    REQUIRE(e.edge == t2s::ClockEdge::LagEnded);
    REQUIRE_FALSE(c.lagging());
    REQUIRE(c.lastEpisodeRows() == 3);
    REQUIRE(c.lastEpisodeMaxLagMs() == behind - 100);
    REQUIRE(c.stamp((T0 + 1500) * MS, T0 + 1300).edge == t2s::ClockEdge::None);
    REQUIRE(c.correctedRows() == 3);
}

TEST_CASE("RowClock: a lag hovering at the threshold is one episode", "[rowclock]") {
    t2s::RowClock c(2000);
    int started = 0, ended = 0;
    for (int i = 0; i < 100; ++i) {
        // lag alternates 2.1 s (corrected) and 1.9 s (not corrected)
        const long long lag = (i % 2 == 0) ? 2100 : 1900;
        auto s = c.stamp((T0 + i * 100 - lag) * MS, T0 + i * 100);
        REQUIRE(s.corrected == (i % 2 == 0));
        if (s.edge == t2s::ClockEdge::LagStarted) ++started;
        if (s.edge == t2s::ClockEdge::LagEnded) ++ended;
    }
    REQUIRE(started == 1);
    REQUIRE(ended == 0);
    REQUIRE(c.stamp((T0 + 20000) * MS, T0 + 19900).edge == t2s::ClockEdge::LagEnded);
}

TEST_CASE("RowClock: a threshold of zero or less never corrects", "[rowclock]") {
    t2s::RowClock c(0);
    REQUIRE_FALSE(c.stamp((T0 - 300000) * MS, T0).corrected);
}
