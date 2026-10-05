/**
 * @file trade_backfill.hpp
 * @brief Fetch the trades of a trade-id gap from Binance REST and hand them
 *        back for publication.
 *
 * Endpoints (checked 2026-10-04; neither needs an API key):
 *   spot     GET /api/v3/historicalTrades?symbol=&fromId=&limit=   weight 25, max 1000 rows
 *            https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
 *            returns raw trades by id: id, price, qty, time, isBuyerMaker.
 *            (/api/v3/aggTrades returns aggregates and cannot rebuild the
 *            individual trades trade_binance stores.)
 *   futures  GET /fapi/v1/aggTrades?symbol=&fromId=&limit=         weight 20, max 1000 rows
 *            https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Compressed-Aggregate-Trades-List
 *            returns a, p, q, nq, f, l, T, m. Only the past two days can be
 *            queried: an older fromId gets HTTP 400, code -4166.
 * An id that does not exist yet returns an empty array (retry). On spot, an
 * id older than what is served returns the oldest trades available instead,
 * so every returned id is checked against the range that was asked for.
 *
 * Neither endpoint returns the event time `E`. A backfilled row therefore
 * has a null exchEventTimeMs, which is how it is told apart from a live
 * row. Its `time` is the moment the REST reply arrived (the handler's
 * receive time, as for every row); the trade's own time is exchTradeTimeMs.
 *
 * Rate limits: every request goes through SnapshotScheduler (10% of the
 * weight limit, exponential backoff after a failure, pause on 429/418).
 *
 * Progress is reported after every page as a trade_gap event (`partial`,
 * then `recovered`), or `unrecoverable` with a reason:
 *   tooLarge    more missing ids than the configured cap
 *   tooOld      futures: older than the two days the endpoint serves
 *   notServed   the exchange no longer returns these ids
 *   restFailed  the request kept failing
 */

#ifndef T2S_TRADE_BACKFILL_HPP
#define T2S_TRADE_BACKFILL_HPP

#include "json_reader.hpp"
#include "market_config.hpp"
#include "snapshot_scheduler.hpp"
#include "snapshot_worker.hpp"     // BoundedQueue
#include "trade_gap.hpp"

#include <rapidjson/document.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <deque>
#include <limits>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace t2s {

struct BackfillTrade {
    long long id = 0;                 ///< spot trade id / futures aggTrade id
    double price = 0.0;
    double qty = 0.0;
    double qtyExRpi = std::numeric_limits<double>::quiet_NaN();   ///< futures `nq`
    long long firstTradeId = 0;       ///< futures `f`
    long long lastTradeId = 0;        ///< futures `l`
    long long tradeTimeMs = 0;
    bool buyerIsMaker = false;
};

struct BackfillPage {
    bool ok = false;                  ///< HTTP 200 and a parsable array
    bool tooOld = false;              ///< the exchange says the range is out of reach
    int httpStatus = 0;
    int retryAfterSec = 0;
    int usedWeight1m = -1;
    long long recvTimeUtcNs = 0;      ///< when the reply arrived
    std::string error;
    std::vector<BackfillTrade> trades;
};

/// Parse a REST reply body into trades. Pure; false + error on a bad body.
inline bool parseBackfillBody(const std::string& body, TradeSchema schema,
                              std::vector<BackfillTrade>& out, std::string& error) {
    rapidjson::Document doc;
    doc.Parse(body.c_str());
    if (doc.HasParseError() || !doc.IsArray()) { error = "reply is not a JSON array"; return false; }
    auto num = [](const rapidjson::Value& v, const char* k, double& d) {
        if (!v.HasMember(k) || !v[k].IsString()) return false;
        char* end = nullptr; const char* s = v[k].GetString();
        d = std::strtod(s, &end);
        return end != s && *end == '\0';
    };
    auto i64 = [](const rapidjson::Value& v, const char* k, long long& x) {
        if (!v.HasMember(k) || !v[k].IsInt64()) return false;
        x = v[k].GetInt64(); return true;
    };
    auto flag = [](const rapidjson::Value& v, const char* k, bool& b) {
        if (!v.HasMember(k) || !v[k].IsBool()) return false;
        b = v[k].GetBool(); return true;
    };
    out.reserve(doc.Size());
    for (const auto& v : doc.GetArray()) {
        if (!v.IsObject()) { error = "array element is not an object"; return false; }
        BackfillTrade t;
        bool good;
        if (schema == TradeSchema::SpotTrade) {
            good = i64(v, "id", t.id) && num(v, "price", t.price) && num(v, "qty", t.qty) &&
                   i64(v, "time", t.tradeTimeMs) && flag(v, "isBuyerMaker", t.buyerIsMaker);
        } else {
            good = i64(v, "a", t.id) && num(v, "p", t.price) && num(v, "q", t.qty) &&
                   i64(v, "f", t.firstTradeId) && i64(v, "l", t.lastTradeId) &&
                   i64(v, "T", t.tradeTimeMs) && flag(v, "m", t.buyerIsMaker);
            double nq;
            if (good && num(v, "nq", nq)) t.qtyExRpi = nq;      // optional, like on the stream
        }
        if (!good) { error = "trade object lacks an expected field"; return false; }
        out.push_back(t);
    }
    return true;
}

/// Real fetcher: one HTTPS GET per page. HttpGetT is RestClient (or a fake).
template <typename HttpGetT>
class RestTradeFetcher {
public:
    RestTradeFetcher(HttpGetT& http, std::string path, TradeSchema schema)
        : http_(http), path_(std::move(path)), schema_(schema) {}

    BackfillPage fetch(const std::string& sym, long long fromId, int limit) {
        BackfillPage page;
        auto r = http_.get(path_ + "?symbol=" + sym + "&fromId=" + std::to_string(fromId) +
                           "&limit=" + std::to_string(limit));
        page.recvTimeUtcNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        page.httpStatus = r.status;
        page.retryAfterSec = r.retryAfterSec;
        page.usedWeight1m = r.usedWeight1m;
        if (!r.error.empty()) { page.error = r.error; return page; }
        if (r.status != 200) {
            page.error = "HTTP " + std::to_string(r.status) + " " + r.body.substr(0, 160);
            // Futures: {"code":-4166,"msg":"Search window is restricted to recent 2 days only."}
            page.tooOld = (r.status == 400 && r.body.find("-4166") != std::string::npos);
            return page;
        }
        page.ok = parseBackfillBody(r.body, schema_, page.trades, page.error);
        return page;
    }

private:
    HttpGetT& http_;
    std::string path_;
    TradeSchema schema_;
};

struct TradeBackfillConfig {
    bool enabled = true;
    long long maxGapIds = 500000;      ///< a larger gap is unrecoverable (tooLarge)
    int pageLimit = 1000;              ///< rows per request (the endpoints' maximum)
    int maxFailures = 10;              ///< consecutive failed requests before giving up on a gap
    bool inlineFetch = false;          ///< tests: fetch on the calling thread instead of the worker
    SnapshotSchedulerConfig sched;     ///< request weight and the exchange's limit
};

/**
 * Backfills gaps one page at a time. FetcherT provides
 *   BackfillPage fetch(const std::string& sym, long long fromId, int limit);
 *
 * Single consumer: addGap() and pump() are called from the publishing
 * thread; only fetch() runs on the worker thread.
 */
template <typename FetcherT>
class TradeBackfill {
public:
    TradeBackfill(FetcherT& fetcher, TradeBackfillConfig cfg)
        : fetcher_(fetcher), cfg_(cfg), sched_(1, cfg.sched), requests_(4), results_(4) {}
    ~TradeBackfill() { stop(); }
    TradeBackfill(const TradeBackfill&) = delete;
    TradeBackfill& operator=(const TradeBackfill&) = delete;

    void start() {
        if (cfg_.inlineFetch || running_.exchange(true)) return;
        thread_ = std::thread([this] { runLoop(); });
    }
    void stop() {
        if (!running_.exchange(false)) return;
        requests_.shutdown();
        results_.shutdown();
        if (thread_.joinable()) thread_.join();
    }

    /// Queue a gap. A resumed gap (from TP's trade state) keeps its progress.
    void addGap(const TradeGap& gap) { queue_.push_back(Job{gap, 0}); }

    /**
     * Drive the backfill: take a finished page if there is one, publish its
     * trades, report progress, and ask for the next page when the rate
     * limits allow.
     *   publishRow(const std::string& sym, const BackfillTrade&, long long recvTimeUtcNs)
     *   recordEvent(const TradeGap&, GapStatus, const std::string& reason)
     */
    template <typename PublishRow, typename RecordEvent>
    void pump(std::int64_t nowMs, PublishRow publishRow, RecordEvent recordEvent) {
        for (int guard = 0; guard < 100000; ++guard) {
            bool progressed = false;

            // 1. A finished page, if any
            if (inFlight_) {
                std::optional<BackfillPage> page = cfg_.inlineFetch ? std::move(inlinePage_) : results_.try_pop();
                inlinePage_.reset();
                if (page.has_value()) {
                    inFlight_ = false;
                    onPage(*page, nowMs, publishRow, recordEvent);
                    progressed = true;
                }
            }

            // 2. Gaps that need no request: disabled, too large, already
            //    complete. This must run BEFORE step 3 for whatever gap is now
            //    at the head, including one that became the head in step 1 -
            //    otherwise a gap above the cap gets pages fetched until the
            //    rate budget happens to pause the requests (seen live: 16,000
            //    trades of a 750,848-id gap were fetched before it was
            //    declared tooLarge).
            while (!inFlight_ && !queue_.empty()) {
                Job& job = queue_.front();
                if (!cfg_.enabled) { finish(recordEvent, GapStatus::Unrecoverable, "backfillDisabled"); progressed = true; continue; }
                if (job.gap.missing() > cfg_.maxGapIds) { finish(recordEvent, GapStatus::Unrecoverable, "tooLarge"); progressed = true; continue; }
                if (job.gap.nextNeededId() > job.gap.lastId) { finish(recordEvent, GapStatus::Recovered, ""); progressed = true; continue; }
                break;
            }

            // 3. The next request, if the rate limits allow
            if (!inFlight_ && !queue_.empty() && sched_.tryAcquire(0, nowMs)) {
                const TradeGap& g = queue_.front().gap;
                long long from = g.nextNeededId();
                int limit = static_cast<int>(std::min<long long>(cfg_.pageLimit, g.lastId - from + 1));
                ++pagesRequested_;
                inFlight_ = true;
                if (cfg_.inlineFetch) inlinePage_ = fetcher_.fetch(g.sym, from, limit);
                else requests_.push(Request{g.sym, from, limit});
                progressed = cfg_.inlineFetch;
            }
            if (!progressed) return;
        }
    }

    // -- observers ----------------------------------------------------------
    std::size_t openGaps() const { return queue_.size(); }
    long long pagesRequested() const { return pagesRequested_; }
    long long pagesFailed() const { return pagesFailed_; }
    long long tradesBackfilled() const { return tradesBackfilled_; }
    long long gapsRecovered() const { return gapsRecovered_; }
    long long gapsUnrecoverable() const { return gapsUnrecoverable_; }
    const SnapshotScheduler& scheduler() const { return sched_; }

private:
    struct Job { TradeGap gap; int failures; };
    struct Request { std::string sym; long long fromId; int limit; };

    template <typename RecordEvent>
    void finish(RecordEvent& recordEvent, GapStatus status, const std::string& reason) {
        recordEvent(queue_.front().gap, status, reason);
        if (status == GapStatus::Recovered) ++gapsRecovered_; else ++gapsUnrecoverable_;
        queue_.pop_front();
    }

    template <typename RecordEvent>
    void fail(BackfillPage& page, std::int64_t nowMs, RecordEvent& recordEvent, const char* giveUpReason) {
        ++pagesFailed_;
        {
            const Job& j = queue_.front();
            spdlog::warn("Backfill request failed for {} from id {} (attempt {} of {}): {}",
                         j.gap.sym, j.gap.nextNeededId(), j.failures + 1, cfg_.maxFailures,
                         page.ok ? (page.trades.empty() ? std::string("empty reply (ids not available yet?)")
                                                        : "reply starts at id " + std::to_string(page.trades.front().id))
                                 : page.error);
        }
        sched_.onFailure(0, nowMs, page.httpStatus, page.retryAfterSec, page.usedWeight1m);
        Job& job = queue_.front();
        if (++job.failures >= cfg_.maxFailures) finish(recordEvent, GapStatus::Unrecoverable, giveUpReason);
    }

    template <typename PublishRow, typename RecordEvent>
    void onPage(BackfillPage& page, std::int64_t nowMs, PublishRow& publishRow, RecordEvent& recordEvent) {
        Job& job = queue_.front();
        TradeGap& g = job.gap;
        if (!page.ok) {
            if (page.tooOld) {
                ++pagesFailed_;
                sched_.onFetchOk(0, nowMs, page.usedWeight1m);
                finish(recordEvent, GapStatus::Unrecoverable, "tooOld");
                return;
            }
            fail(page, nowMs, recordEvent, "restFailed");
            return;
        }
        sched_.onFetchOk(0, nowMs, page.usedWeight1m);
        long long expected = g.nextNeededId();
        if (page.trades.empty()) {              // not available (yet): retry with backoff
            fail(page, nowMs, recordEvent, "notServed");
            return;
        }
        if (page.trades.front().id > expected) { // the exchange starts later than we asked
            ++pagesFailed_;
            finish(recordEvent, GapStatus::Unrecoverable, "notServed");
            return;
        }
        long long n = 0;
        for (const BackfillTrade& t : page.trades) {
            if (t.id < expected) continue;
            if (t.id != expected || t.id > g.lastId) break;
            publishRow(g.sym, t, page.recvTimeUtcNs);
            ++expected; ++n;
        }
        if (n == 0) { fail(page, nowMs, recordEvent, "notServed"); return; }
        tradesBackfilled_ += n;
        g.recovered += n;
        g.recoveredThroughId = expected - 1;
        job.failures = 0;
        sched_.onSynced(0);
        if (g.recoveredThroughId >= g.lastId) finish(recordEvent, GapStatus::Recovered, "");
        else recordEvent(g, GapStatus::Partial, "");
    }

    void runLoop() {
        while (running_.load()) {
            auto req = requests_.pop_blocking();
            if (!req.has_value()) break;
            results_.push(fetcher_.fetch(req->sym, req->fromId, req->limit));
        }
    }

    FetcherT& fetcher_;
    TradeBackfillConfig cfg_;
    SnapshotScheduler sched_;
    std::deque<Job> queue_;
    bool inFlight_ = false;
    std::optional<BackfillPage> inlinePage_;
    BoundedQueue<Request> requests_;
    BoundedQueue<BackfillPage> results_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    long long pagesRequested_ = 0, pagesFailed_ = 0, tradesBackfilled_ = 0;
    long long gapsRecovered_ = 0, gapsUnrecoverable_ = 0;
};

} // namespace t2s

#endif // T2S_TRADE_BACKFILL_HPP
