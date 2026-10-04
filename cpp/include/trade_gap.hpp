/**
 * @file trade_gap.hpp
 * @brief Gaps in the exchange's own trade ids: detection, the trade_gap
 *        event row, and the queue that holds events until TP acknowledges.
 *
 * Binance numbers trades per symbol without holes (spot: trade id `t`;
 * USD-M futures: aggregate trade id `a`). A jump in that id means trades
 * the handler did not receive. Each gap is recorded in the trade_gap table
 * (kdb/schemas.q: .schema.tradeGap) as one row per status change:
 * detected -> partial* -> recovered | unrecoverable.
 */

#ifndef T2S_TRADE_GAP_HPP
#define T2S_TRADE_GAP_HPP

#include "k_object.hpp"
#include "tp_publisher.hpp"

#include <deque>
#include <string>
#include <unordered_map>

namespace t2s {

enum class GapStatus { Detected, Partial, Recovered, Unrecoverable };

inline const char* gapStatusName(GapStatus s) {
    switch (s) {
        case GapStatus::Detected:      return "detected";
        case GapStatus::Partial:       return "partial";
        case GapStatus::Recovered:     return "recovered";
        case GapStatus::Unrecoverable: return "unrecoverable";
    }
    return "detected";
}

/// One gap: ids firstId..lastId (inclusive) of `sym` are missing.
struct TradeGap {
    std::string sym;
    long long firstId = 0;
    long long lastId = 0;
    long long recovered = 0;            ///< ids backfilled so far
    long long recoveredThroughId = 0;   ///< highest id backfilled so far (0 = none)
    long long missing() const { return lastId - firstId + 1; }
    long long nextNeededId() const { return recoveredThroughId > 0 ? recoveredThroughId + 1 : firstId; }
};

constexpr int GAP_ROW_WIDTH = 10;
constexpr long long GAP_KDB_EPOCH_OFFSET_NS = 946684800000000000LL;
constexpr long long GAP_NULL_LONG = static_cast<long long>(0x8000000000000000ULL);

/**
 * trade_gap row, in .schema.tradeGap order:
 *   time, sym, srcTable, firstMissingId, lastMissingId, missing, status,
 *   recovered, recoveredThroughId, reason
 */
inline K buildGapRow(long long timeUtcNs, const TradeGap& g, const std::string& srcTable,
                     GapStatus status, const std::string& reason) {
    return knk(GAP_ROW_WIDTH,
        ktj(-KP, timeUtcNs - GAP_KDB_EPOCH_OFFSET_NS),
        ks(const_cast<S>(g.sym.c_str())),
        ks(const_cast<S>(srcTable.c_str())),
        kj(g.firstId),
        kj(g.lastId),
        kj(g.missing()),
        ks(const_cast<S>(gapStatusName(status))),
        kj(g.recovered),
        kj(g.recoveredThroughId > 0 ? g.recoveredThroughId : GAP_NULL_LONG),
        ks(const_cast<S>(reason.c_str())));
}

/**
 * Last id seen per symbol; classifies each new id.
 */
class TradeIdTracker {
public:
    enum class Kind { First, InOrder, Gap, Duplicate, OutOfOrder };
    struct Result {
        Kind kind = Kind::First;
        long long firstMissing = 0;   ///< set for Gap
        long long lastMissing = 0;
        long long previous = 0;       ///< last id before this one (0 if none)
    };

    /// Classify `id` and remember the highest id seen for `sym`.
    Result onId(const std::string& sym, long long id) {
        Result r;
        auto it = last_.find(sym);
        if (it == last_.end()) {
            last_[sym] = id;
            return r;
        }
        r.previous = it->second;
        if (id == it->second + 1)      r.kind = Kind::InOrder;
        else if (id == it->second)     r.kind = Kind::Duplicate;
        else if (id < it->second)      r.kind = Kind::OutOfOrder;
        else { r.kind = Kind::Gap; r.firstMissing = it->second + 1; r.lastMissing = id - 1; }
        if (id > it->second) it->second = id;
        return r;
    }

    /// Start from a known last id (what TP has logged for the symbol), so a
    /// restarted handler still sees the gap its downtime left.
    void seed(const std::string& sym, long long lastId) { last_[sym] = lastId; }

    bool known(const std::string& sym) const { return last_.count(sym) > 0; }
    long long last(const std::string& sym) const {
        auto it = last_.find(sym);
        return it == last_.end() ? 0 : it->second;
    }

private:
    std::unordered_map<std::string, long long> last_;
};

/**
 * trade_gap events waiting for TP's acknowledgement. Events are rare and
 * must not be lost, so they are sent synchronously (.tp.event replies with
 * the row's tpSeqNo once it is logged) and stay queued until that reply.
 */
class GapEventQueue {
public:
    explicit GapEventQueue(std::string table = "trade_gap") : table_(std::move(table)) {}
    ~GapEventQueue() { for (K r : pending_) r0(r); }
    GapEventQueue(const GapEventQueue&) = delete;
    GapEventQueue& operator=(const GapEventQueue&) = delete;

    /// Takes ownership of `row`.
    void push(K row) { pending_.push_back(row); }

    /// Send what is queued, oldest first. Stops at a lost connection (the
    /// rest stays queued). A row TP rejects outright is dropped and counted.
    /// @return true if the queue is empty afterwards
    bool flush(TpPublisher& tp) {
        while (!pending_.empty()) {
            K row = pending_.front();
            TpPublisher::EventResult res = tp.sendEvent(table_, row);
            if (res == TpPublisher::EventResult::ConnectionLost) return false;
            if (res == TpPublisher::EventResult::Rejected) ++rejected_; else ++acked_;
            r0(row);
            pending_.pop_front();
        }
        return true;
    }

    std::size_t size() const { return pending_.size(); }
    long long acked() const { return acked_; }
    long long rejected() const { return rejected_; }

private:
    std::string table_;
    std::deque<K> pending_;
    long long acked_ = 0;
    long long rejected_ = 0;
};

} // namespace t2s

#endif // T2S_TRADE_GAP_HPP
