/**
 * @file sim_trade_publisher.cpp
 * @brief Test publisher: sends synthetic spot trade rows to a tickerplant
 *        through the real TpPublisher (registration, resend ring).
 *
 * Used by the sandbox tests to exercise, without the exchange, the code the
 * feed handlers publish with. Not started by start.sh.
 *
 *   sim_trade_publisher --port P --rows N [--rate R] [--ring K]
 *                       [--session S] [--first-id I] [--sym SYM]
 *                       [--gap-at K --gap-size G] [--pending-gap F:L]
 *                       [--seed-from-tp 1]
 *                       [--backfill 1] [--backfill-page P] [--backfill-fail N]
 *                       [--backfill-served-from ID] [--backfill-max-gap M]
 *                       [--die-after-backfilled N]
 *
 * Publishes N rows at R rows/second (default 1000) with tradeId I, I+1, ...
 * and fhSeqNo 1..N. With --gap-at K, the ids jump by G before row K (0-based),
 * as if G trades had not been received: the gap goes through the handlers'
 * TradeIdTracker and is recorded as a trade_gap event. --pending-gap queues
 * a gap event BEFORE the first connect (TP may still be down).
 * --seed-from-tp 1 asks TP for the last logged id per symbol first, as a
 * starting trade handler does, so a gap left by "downtime" is detected, and
 * open gaps are resumed.
 * --backfill 1 runs the real TradeBackfill against a fake exchange that
 * serves any trade id (from --backfill-served-from on), failing its first N
 * requests if asked; backfilled rows are published like the handler does
 * (null exchEventTimeMs). --die-after-backfilled N kills the process, without
 * any clean-up, right after the Nth backfilled row: a crash mid-backfill.
 * Prints:
 *   SIM done rows=N reconnects=A resent=B unresendable=C gaps=D gapEventsAcked=E gapEventsPending=F
 * Exit code 0, or 2 if TP rejected the session.
 */

// trade_backfill.hpp first: it brings in Boost, which must be seen before
// k.h (whose short macros such as `wi` collide with Boost's own names).
#include "trade_backfill.hpp"
#include "tp_publisher.hpp"
#include "trade_row.hpp"
#include "trade_gap.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

namespace {
std::atomic<bool> g_running{true};
void onSignal(int) { g_running = false; }
constexpr long long KDB_EPOCH_OFFSET_NS = 946684800000000000LL;

long long nowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}
} // namespace

int main(int argc, char* argv[]) {
    int port = 0; long long rows = 0, rate = 1000, firstId = 1, session = nowNs();
    std::size_t ring = 4096; std::string sym = "BTCUSDT";
    long long gapAt = -1, gapSize = 0, pendFirst = 0, pendLast = 0;
    bool seedFromTp = false, doBackfill = false;
    long long bfPage = 1000, bfFail = 0, bfServedFrom = 1, bfMaxGap = 500000, dieAfter = -1;
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i]; const char* v = argv[i + 1];
        if (a == "--port") port = std::atoi(v);
        else if (a == "--rows") rows = std::atoll(v);
        else if (a == "--rate") rate = std::atoll(v);
        else if (a == "--ring") ring = static_cast<std::size_t>(std::atoll(v));
        else if (a == "--session") session = std::atoll(v);
        else if (a == "--first-id") firstId = std::atoll(v);
        else if (a == "--sym") sym = v;
        else if (a == "--seed-from-tp") seedFromTp = std::atoll(v) != 0;
        else if (a == "--backfill") doBackfill = std::atoll(v) != 0;
        else if (a == "--backfill-page") bfPage = std::atoll(v);
        else if (a == "--backfill-fail") bfFail = std::atoll(v);
        else if (a == "--backfill-served-from") bfServedFrom = std::atoll(v);
        else if (a == "--backfill-max-gap") bfMaxGap = std::atoll(v);
        else if (a == "--die-after-backfilled") dieAfter = std::atoll(v);
        else if (a == "--gap-at") gapAt = std::atoll(v);
        else if (a == "--gap-size") gapSize = std::atoll(v);
        else if (a == "--pending-gap") {
            const char* c = std::strchr(v, ':');
            if (!c) { std::cerr << "--pending-gap FIRST:LAST\n"; return 1; }
            pendFirst = std::atoll(v); pendLast = std::atoll(c + 1);
        }
        else { std::cerr << "unknown option " << a << "\n"; return 1; }
    }
    if (port <= 0 || rows <= 0 || rate <= 0) {
        std::cerr << "usage: sim_trade_publisher --port P --rows N [--rate R] [--ring K] [--session S] [--first-id I] [--sym SYM]\n";
        return 1;
    }
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    spdlog::set_pattern("[%H:%M:%S.%e] [sim] [%l] %v");

    t2s::TpPublisherConfig cfg;
    cfg.port = port;
    cfg.table = "trade_binance";
    cfg.width = t2s::TRADE_ROW_WIDTH;
    cfg.sessionId = session;
    cfg.ringSize = ring;
    cfg.initialBackoffMs = 200;      // tests restart TP within a second or two
    cfg.maxBackoffMs = 1000;
    t2s::TpPublisher tp(cfg, g_running);
    t2s::TradeIdTracker tracker;
    t2s::GapEventQueue gapEvents;
    long long gaps = 0;
    auto record = [&](const t2s::TradeGap& g) {
        ++gaps;
        gapEvents.push(t2s::buildGapRow(nowNs(), g, cfg.table, t2s::GapStatus::Detected, ""));
        gapEvents.flush(tp);
    };
    if (pendLast >= pendFirst && pendFirst > 0) {
        t2s::TradeGap g; g.sym = sym; g.firstId = pendFirst; g.lastId = pendLast;
        record(g);                       // not connected yet: stays queued
    }

    if (!tp.connect(1)) {
        std::cout << "SIM failed: " << (tp.fatalError().empty() ? "shutdown" : tp.fatalError()) << std::endl;
        return tp.fatalError().empty() ? 1 : 2;
    }
    // The exchange, for the backfill: serves every id from bfServedFrom on
    struct FakeExchange {
        long long servedFrom; long long failFirst;
        t2s::BackfillPage fetch(const std::string&, long long fromId, int limit) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            t2s::BackfillPage p;
            p.recvTimeUtcNs = nowNs();
            if (failFirst > 0) { --failFirst; p.error = "simulated REST failure"; return p; }
            p.ok = true; p.httpStatus = 200;
            for (long long id = std::max(fromId, servedFrom); static_cast<int>(p.trades.size()) < limit; ++id) {
                t2s::BackfillTrade t; t.id = id; t.price = 100.0 + 0.01 * (id % 100); t.qty = 0.25;
                t.tradeTimeMs = p.recvTimeUtcNs / 1000000 - 5000; t.buyerIsMaker = (id % 2) == 0;
                p.trades.push_back(t);
            }
            return p;
        }
    } exchange{bfServedFrom, bfFail};
    t2s::TradeBackfillConfig bfCfg;
    bfCfg.enabled = true;
    bfCfg.pageLimit = static_cast<int>(bfPage);
    bfCfg.maxGapIds = bfMaxGap;
    bfCfg.maxFailures = 4;
    bfCfg.sched.weightPerRequest = 1; bfCfg.sched.weightLimitPerMin = 600000;   // no waiting in tests
    bfCfg.sched.initialBackoffMs = 50; bfCfg.sched.maxBackoffMs = 200;
    t2s::TradeBackfill<FakeExchange> backfill(exchange, bfCfg);
    backfill.start();
    long long seq = 0, backfilled = 0;
    auto steadyMs = [] { return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count(); };
    auto pump = [&] {
        backfill.pump(steadyMs(),
            [&](const std::string& s2, const t2s::BackfillTrade& t, long long recvNs) {
                ++seq;
                t2s::KOwned row = t2s::buildTradeRow(recvNs, s2, t.id, t.price, t.qty, t.buyerIsMaker,
                                                     t2s::GAP_NULL_LONG, t.tradeTimeMs, 0, 0, seq, KDB_EPOCH_OFFSET_NS);
                tp.publish(row.release(), seq);
                if (++backfilled == dieAfter) {
                    // let TP read the rows, then die without reporting progress
                    if (tp.connected()) { K r = k(tp.handle(), const_cast<S>("1"), (K)0); if (r) r0(r); }
                    std::_Exit(9);
                }
            },
            [&](const t2s::TradeGap& g, t2s::GapStatus st, const std::string& reason) {
                gapEvents.push(t2s::buildGapRow(nowNs(), g, cfg.table, st, reason));
                gapEvents.flush(tp);
            });
    };

    // Like a starting trade handler: take the last logged id per symbol from
    // TP, and pick up the gaps a previous run left open
    std::size_t seeded = 0, openGaps = 0; bool firstAfterSeed = false;
    if (seedFromTp) {
        auto st = tp.tradeState();
        for (const auto& kv : st.lastIds) { tracker.seed(kv.first, kv.second); ++seeded; if (kv.first == sym) firstAfterSeed = true; }
        openGaps = st.openGaps.size();
        for (const auto& og : st.openGaps) {
            t2s::TradeGap g; g.sym = og.sym; g.firstId = og.firstId; g.lastId = og.lastId;
            g.recovered = og.recovered; g.recoveredThroughId = og.recoveredThroughId;
            if (doBackfill) backfill.addGap(g);
        }
    }
    const auto gap = std::chrono::nanoseconds(1000000000LL / rate);
    long long sent = 0;
    long long idShift = 0;
    for (long long i = 0; i < rows && g_running; ++i) {
        long long recv = nowNs();
        if (i == gapAt) idShift = gapSize;
        const long long id = firstId + i + idShift;
        auto res = tracker.onId(sym, id);
        if (res.kind == t2s::TradeIdTracker::Kind::Gap) {
            t2s::TradeGap g; g.sym = sym; g.firstId = res.firstMissing; g.lastId = res.lastMissing;
            ++gaps;
            gapEvents.push(t2s::buildGapRow(nowNs(), g, cfg.table, t2s::GapStatus::Detected, firstAfterSeed ? "handlerRestart" : ""));
            gapEvents.flush(tp);
            if (doBackfill) backfill.addGap(g);     // without --backfill the gap is only recorded
        }
        firstAfterSeed = false;
        ++seq;
        t2s::KOwned row = t2s::buildTradeRow(recv, sym, id, 100.0 + 0.01 * (i % 100), 0.5, (i % 2) == 0,
                                             recv / 1000000, recv / 1000000, 1, 1, seq, KDB_EPOCH_OFFSET_NS);
        if (!tp.publish(row.release(), seq)) break;
        ++sent;
        if (gapEvents.size() > 0) gapEvents.flush(tp);
        pump();
        std::this_thread::sleep_for(gap);
    }
    // Finish the backfill that is still running (bounded wait)
    for (auto t0 = steadyMs(); backfill.openGaps() > 0 && steadyMs() - t0 < 15000 && g_running;) {
        pump();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    backfill.stop();
    if (gapEvents.size() > 0) gapEvents.flush(tp);
    // Let TP read what is still in the socket before we close it
    if (tp.connected()) {
        K r = k(tp.handle(), const_cast<S>("1"), (K)0);
        if (r) r0(r);
    }
    std::cout << "SIM done rows=" << sent << " reconnects=" << tp.reconnects()
              << " resent=" << tp.rowsResent() << " unresendable=" << tp.rowsUnresendable()
              << " gaps=" << gaps << " gapEventsAcked=" << gapEvents.acked()
              << " gapEventsPending=" << gapEvents.size()
              << " seeded=" << seeded << " openGaps=" << openGaps
              << " backfilled=" << backfilled << " gapsRecovered=" << backfill.gapsRecovered()
              << " gapsUnrecoverable=" << backfill.gapsUnrecoverable()
              << " gapsStillOpen=" << backfill.openGaps() << std::endl;
    if (!tp.fatalError().empty()) return 2;
    return sent == rows ? 0 : 1;
}
