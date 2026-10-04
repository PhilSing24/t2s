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
 *
 * Publishes N rows at R rows/second (default 1000) with tradeId I, I+1, ...
 * and fhSeqNo 1..N, then prints one line:
 *   SIM done rows=N reconnects=A resent=B unresendable=C
 * Exit code 0, or 2 if TP rejected the session.
 */

#include "tp_publisher.hpp"
#include "trade_row.hpp"

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
    for (int i = 1; i + 1 < argc; i += 2) {
        std::string a = argv[i]; const char* v = argv[i + 1];
        if (a == "--port") port = std::atoi(v);
        else if (a == "--rows") rows = std::atoll(v);
        else if (a == "--rate") rate = std::atoll(v);
        else if (a == "--ring") ring = static_cast<std::size_t>(std::atoll(v));
        else if (a == "--session") session = std::atoll(v);
        else if (a == "--first-id") firstId = std::atoll(v);
        else if (a == "--sym") sym = v;
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

    if (!tp.connect(1)) {
        std::cout << "SIM failed: " << (tp.fatalError().empty() ? "shutdown" : tp.fatalError()) << std::endl;
        return tp.fatalError().empty() ? 1 : 2;
    }
    const auto gap = std::chrono::nanoseconds(1000000000LL / rate);
    long long sent = 0;
    for (long long i = 0; i < rows && g_running; ++i) {
        long long recv = nowNs();
        t2s::KOwned row = t2s::buildTradeRow(recv, sym, firstId + i, 100.0 + 0.01 * (i % 100), 0.5, (i % 2) == 0,
                                             recv / 1000000, recv / 1000000, 1, 1, i + 1, KDB_EPOCH_OFFSET_NS);
        if (!tp.publish(row.release(), i + 1)) break;
        ++sent;
        std::this_thread::sleep_for(gap);
    }
    // Let TP read what is still in the socket before we close it
    if (tp.connected()) {
        K r = k(tp.handle(), const_cast<S>("1"), (K)0);
        if (r) r0(r);
    }
    std::cout << "SIM done rows=" << sent << " reconnects=" << tp.reconnects()
              << " resent=" << tp.rowsResent() << " unresendable=" << tp.rowsUnresendable() << std::endl;
    if (!tp.fatalError().empty()) return 2;
    return sent == rows ? 0 : 1;
}
