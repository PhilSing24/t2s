/**
 * @file tp_publisher.hpp
 * @brief A feed handler's connection to the tickerplant: connect, register
 *        the session, publish rows, and resend after a lost connection.
 *
 * Shared by the trade and quote handlers (they used to carry a copy each).
 *
 * No row is lost across a TP restart or a dropped connection:
 *   - every published row is also kept in a ring (the last `ringSize`)
 *   - on (re)connect the handler registers its session; TP replies with the
 *     last fhSeqNo it has LOGGED for that session (or -1 if it holds
 *     nothing for it, e.g. a restarted handler)
 *   - the handler resends every ring row after that number, in order, and
 *     then carries on
 * A row written into a socket whose peer had just died looks sent to the
 * handler; TP's reply is what tells the truth. If the ring no longer
 * reaches back far enough, the first row TP receives jumps ahead and TP
 * counts the difference as missed - never silently.
 *
 * See .tp.registerSession in kdb/tick/tp.q for TP's side.
 */

#ifndef T2S_TP_PUBLISHER_HPP
#define T2S_TP_PUBLISHER_HPP

#include "k_object.hpp"   // k.h with its one-letter macros undefined

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace t2s {

/**
 * The last N published rows, by fhSeqNo. Holds one reference to each K row.
 */
class ResendRing {
public:
    explicit ResendRing(std::size_t capacity) : capacity_(capacity ? capacity : 1) {}
    ~ResendRing() { clear(); }
    ResendRing(const ResendRing&) = delete;
    ResendRing& operator=(const ResendRing&) = delete;

    /// Takes over one reference to `row` (the caller r1()s it if it also sends it).
    void push(long long fhSeqNo, K row) {
        if (rows_.size() >= capacity_) {
            r0(rows_.front().second);
            rows_.pop_front();
        }
        rows_.emplace_back(fhSeqNo, row);
    }

    void clear() {
        for (auto& e : rows_) r0(e.second);
        rows_.clear();
    }

    bool empty() const { return rows_.empty(); }
    std::size_t size() const { return rows_.size(); }
    std::size_t capacity() const { return capacity_; }
    long long oldestSeq() const { return rows_.empty() ? 0 : rows_.front().first; }
    long long newestSeq() const { return rows_.empty() ? 0 : rows_.back().first; }

    /// Calls fn(fhSeqNo, row) for every row with fhSeqNo > afterSeq, oldest
    /// first. fn returns false to stop. `row` stays owned by the ring.
    template <typename Fn>
    void forEachAfter(long long afterSeq, Fn fn) const {
        for (const auto& e : rows_) {
            if (e.first > afterSeq && !fn(e.first, e.second)) return;
        }
    }

    /// How many rows after afterSeq are no longer in the ring (0 = all there).
    long long missingAfter(long long afterSeq) const {
        if (rows_.empty()) return 0;
        long long firstNeeded = afterSeq + 1;
        return rows_.front().first > firstNeeded ? rows_.front().first - firstNeeded : 0;
    }

private:
    std::size_t capacity_;
    std::deque<std::pair<long long, K>> rows_;
};

struct TpPublisherConfig {
    std::string host = "localhost";
    int         port = 5010;
    std::string table;                 ///< TP table this handler owns
    long long   width = 0;             ///< feed-handler columns it announces
    long long   sessionId = 0;         ///< process start time, ns
    std::size_t ringSize = 4096;       ///< rows kept for resending
    int initialBackoffMs = 1000;
    int maxBackoffMs = 8000;
};

class TpPublisher {
public:
    /// `running` is the handler's shutdown flag: connect() gives up when it
    /// goes false, and a rejected registration sets it false.
    TpPublisher(TpPublisherConfig cfg, std::atomic<bool>& running)
        : cfg_(std::move(cfg)), running_(running), ring_(cfg_.ringSize) {}

    ~TpPublisher() { close(); }
    TpPublisher(const TpPublisher&) = delete;
    TpPublisher& operator=(const TpPublisher&) = delete;

    /**
     * Connect and register, retrying with backoff until it works.
     * @param nextFhSeqNo fhSeqNo of the next NEW row (rows up to
     *        nextFhSeqNo-1 have been published and may need resending)
     * @return false if shutdown was requested or TP rejected the handler
     *         (fatalError() says why)
     */
    bool connect(long long nextFhSeqNo) {
        int attempt = 0;
        while (running_) {
            spdlog::info("Connecting to TP on {}:{}...", cfg_.host, cfg_.port);
            int h = khpu(const_cast<S>(cfg_.host.c_str()), cfg_.port, const_cast<S>(""));
            if (h > 0) {
                long long lastLogged = -1;
                int reg = registerSession(h, nextFhSeqNo, lastLogged);
                if (reg < 0) {              // rejected: fatal, do not retry
                    kclose(h);
                    running_ = false;
                    return false;
                }
                if (reg == 1 && resend(h, lastLogged)) {
                    handle_ = h;
                    spdlog::info("Connected to TP (handle {})", h);
                    return true;
                }
                kclose(h);                  // lost again during registration or resend
            } else {
                spdlog::error("Failed to connect to TP");
            }
            if (!sleepWithBackoff(attempt++)) return false;
        }
        return false;
    }

    /**
     * Publish one row (async). Takes ownership of `row`. If the connection
     * is found dead, reconnects and resends from the ring, which includes
     * this row.
     * @return false only if shutdown was requested or TP rejected the
     *         handler while reconnecting
     */
    bool publish(K row, long long fhSeqNo) {
        ring_.push(fhSeqNo, r1(row));
        if (handle_ > 0) {
            K result = k(-handle_, const_cast<S>(".u.upd"), ks(const_cast<S>(cfg_.table.c_str())), row, (K)0);
            if (result != nullptr) return true;
            spdlog::error("TP connection lost, reconnecting...");
            kclose(handle_);
            handle_ = -1;
            ++reconnects_;
        } else {
            r0(row);
        }
        return connect(fhSeqNo + 1);
    }

    enum class EventResult { Acked, ConnectionLost, Rejected };

    /**
     * Send one event row synchronously: .tp.event[table; row] replies with
     * the row's tpSeqNo once it is in TP's log. `row` stays owned by the
     * caller, who keeps it until the result is Acked (or Rejected).
     * A lost connection is left for the next publish()/connect() to repair.
     */
    EventResult sendEvent(const std::string& table, K row) {
        if (handle_ <= 0) return EventResult::ConnectionLost;
        K r = k(handle_, const_cast<S>(".tp.event"), ks(const_cast<S>(table.c_str())), r1(row), (K)0);
        if (r == nullptr) {
            spdlog::error("TP connection lost while sending a {} event", table);
            kclose(handle_);
            handle_ = -1;
            ++reconnects_;
            return EventResult::ConnectionLost;
        }
        if (r->t == -128) {
            spdlog::error("TP rejected a {} event: {}", table, r->s);
            r0(r);
            return EventResult::Rejected;
        }
        r0(r);
        return EventResult::Acked;
    }

    /// What TP has logged for a trade table: last exchange trade id per
    /// symbol, and the gaps still open. See .tp.tradeState in tp.q.
    struct TradeState {
        bool ok = false;
        std::vector<std::pair<std::string, long long>> lastIds;
        struct OpenGap { std::string sym; long long firstId, lastId, recovered, recoveredThroughId; };
        std::vector<OpenGap> openGaps;
    };

    TradeState tradeState() {
        TradeState st;
        if (handle_ <= 0) return st;
        K r = k(handle_, const_cast<S>(".tp.tradeState"), ks(const_cast<S>(cfg_.table.c_str())), (K)0);
        if (r == nullptr) {
            spdlog::error("TP connection lost while asking for the trade state");
            kclose(handle_); handle_ = -1; ++reconnects_;
            return st;
        }
        if (r->t == -128) {
            spdlog::error("TP could not give the trade state for {}: {}", cfg_.table, r->s);
            r0(r);
            return st;
        }
        // (syms; ids; gapSyms; gapFirst; gapLast; gapRecovered; gapThrough)
        if (r->t == 0 && r->n == 7) {
            K syms = kK(r)[0], ids = kK(r)[1];
            if (syms->t == KS && ids->t == KJ && syms->n == ids->n) {
                for (J i = 0; i < syms->n; ++i) st.lastIds.emplace_back(kS(syms)[i], kJ(ids)[i]);
            }
            K gs = kK(r)[2], gf = kK(r)[3], gl = kK(r)[4], gr = kK(r)[5], gt = kK(r)[6];
            if (gs->t == KS && gf->t == KJ && gl->t == KJ && gr->t == KJ && gt->t == KJ) {
                for (J i = 0; i < gs->n; ++i) {
                    long long through = kJ(gt)[i];
                    st.openGaps.push_back({kS(gs)[i], kJ(gf)[i], kJ(gl)[i], kJ(gr)[i],
                                           through == static_cast<long long>(0x8000000000000000ULL) ? 0 : through});
                }
            }
            st.ok = true;
        }
        r0(r);
        return st;
    }

    void close() {
        if (handle_ > 0) {
            kclose(handle_);
            handle_ = -1;
        }
    }

    int handle() const { return handle_; }
    bool connected() const { return handle_ > 0; }
    const std::string& fatalError() const { return fatalError_; }
    const std::string& table() const { return cfg_.table; }

    // -- counters -----------------------------------------------------------
    long long reconnects() const { return reconnects_; }          ///< connections found dead
    long long rowsResent() const { return rowsResent_; }          ///< rows sent again after a reconnect
    long long rowsUnresendable() const { return rowsUnresendable_; } ///< rows TP lacked that the ring no longer held
    long long lastReply() const { return lastReply_; }            ///< TP's reply at the last registration
    const ResendRing& ring() const { return ring_; }

private:
    /// 1 registered (lastLogged set), 0 connection lost, -1 rejected
    int registerSession(int h, long long nextFhSeqNo, long long& lastLogged) {
        K r = k(h, const_cast<S>(".tp.registerSession"),
                ks(const_cast<S>(cfg_.table.c_str())), kj(cfg_.sessionId), kj(nextFhSeqNo), kj(cfg_.width), (K)0);
        if (r == nullptr) {
            spdlog::error("TP connection lost during session registration");
            return 0;
        }
        if (r->t == -128) {
            fatalError_ = "registration rejected for " + cfg_.table + ": " + r->s;
            spdlog::critical("TP REJECTED session registration for {} (sessionId={}, nextFhSeqNo={}, width={}): {}",
                             cfg_.table, cfg_.sessionId, nextFhSeqNo, cfg_.width, r->s);
            r0(r);
            return -1;
        }
        lastLogged = (r->t == -KJ) ? r->j : -1;
        r0(r);
        lastReply_ = lastLogged;
        spdlog::info("Session registered with TP: table={} sessionId={} nextFhSeqNo={} width={}; TP has logged up to fhSeqNo {}",
                     cfg_.table, cfg_.sessionId, nextFhSeqNo, cfg_.width,
                     lastLogged < 0 ? std::string("(nothing for this session)") : std::to_string(lastLogged));
        return 1;
    }

    /// Resend the ring rows TP has not logged. false if the connection died again.
    bool resend(int h, long long lastLogged) {
        if (lastLogged < 0 || ring_.empty() || ring_.newestSeq() <= lastLogged) return true;
        long long gone = ring_.missingAfter(lastLogged);
        if (gone > 0) {
            rowsUnresendable_ += gone;
            spdlog::error("Cannot resend fhSeqNo {}..{} to TP: no longer in the ring ({} rows). TP will count them as missed.",
                          lastLogged + 1, lastLogged + gone, gone);
        }
        long long n = 0;
        bool ok = true;
        ring_.forEachAfter(lastLogged, [&](long long, K row) {
            K res = k(-h, const_cast<S>(".u.upd"), ks(const_cast<S>(cfg_.table.c_str())), r1(row), (K)0);
            if (res == nullptr) { ok = false; return false; }
            ++n;
            return true;
        });
        if (!ok) {
            spdlog::error("TP connection lost while resending ({} rows sent)", n);
            return false;
        }
        rowsResent_ += n;
        spdlog::warn("Resent {} row(s) to TP: fhSeqNo {}..{}", n,
                     std::max(lastLogged + 1, ring_.oldestSeq()), ring_.newestSeq());
        return true;
    }

    bool sleepWithBackoff(int attempt) {
        int delay = cfg_.initialBackoffMs;
        for (int i = 0; i < attempt && delay < cfg_.maxBackoffMs; ++i) delay *= 2;
        delay = std::min(delay, cfg_.maxBackoffMs);
        spdlog::info("Waiting {}ms before reconnecting to TP...", delay);
        for (int slept = 0; slept < delay && running_; slept += 100) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        return running_;
    }

    TpPublisherConfig cfg_;
    std::atomic<bool>& running_;
    ResendRing ring_;
    int handle_ = -1;
    std::string fatalError_;
    long long reconnects_ = 0;
    long long rowsResent_ = 0;
    long long rowsUnresendable_ = 0;
    long long lastReply_ = -1;
};

} // namespace t2s

#endif // T2S_TP_PUBLISHER_HPP
