/**
 * @file snapshot_scheduler.hpp
 * @brief Decides WHEN a REST depth snapshot may be requested.
 *
 * The quote handler asks tryAcquire() before every snapshot request. The
 * scheduler says no when any of these holds:
 *
 *   1. Backoff. After a failure for a symbol (HTTP error, timeout, or a
 *      snapshot that did not lead to a synced book) the next request for
 *      that symbol waits initialBackoffMs * 2^(n-1), capped at
 *      maxBackoffMs, stretched by up to +25% jitter. Reset by onSynced().
 *   2. Weight budget. A token bucket holding budgetFraction of the
 *      exchange's per-minute IP weight limit, shared by all symbols and
 *      refilled continuously. Every request costs weightPerRequest. With
 *      the defaults (10%) the handler can never use more than a tenth of
 *      the limit in any minute, plus one bucket of initial burst.
 *   3. Rate-limit pause. HTTP 429 or 418 stops ALL requests for the
 *      server's Retry-After (at least 60 s for 429, 300 s for 418). A
 *      used-weight header at or above half the limit (other programs on
 *      the same IP count too) stops all requests for 60 s.
 *
 * Binance limits (checked 2026-10-04 against /exchangeInfo and the docs):
 *   spot    GET /api/v3/depth  limit 1000 = weight 50, IP limit 6000/min
 *           https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
 *   futures GET /fapi/v1/depth limit 1000 = weight 20, IP limit 2400/min
 *           https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book
 *
 * Pure: no clock, no I/O. The caller passes a monotonic time in
 * milliseconds, which is what makes it testable.
 */

#ifndef T2S_SNAPSHOT_SCHEDULER_HPP
#define T2S_SNAPSHOT_SCHEDULER_HPP

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <vector>

namespace t2s {

struct SnapshotSchedulerConfig {
    int          weightPerRequest       = 50;     ///< cost of one snapshot
    int          weightLimitPerMin      = 6000;   ///< exchange IP limit
    double       budgetFraction         = 0.10;   ///< share of the limit we allow ourselves
    std::int64_t initialBackoffMs       = 1000;
    std::int64_t maxBackoffMs           = 60000;
    double       jitterFraction         = 0.25;   ///< delay *= 1 + jitterFraction * U[0,1)
    std::int64_t minPause429Ms          = 60000;
    std::int64_t minPause418Ms          = 300000;
    double       usedWeightPauseFraction = 0.5;   ///< of weightLimitPerMin
    std::int64_t usedWeightPauseMs      = 60000;
};

class SnapshotScheduler {
public:
    using JitterFn = std::function<double()>;   ///< returns a value in [0,1)

    SnapshotScheduler(int numSymbols, SnapshotSchedulerConfig cfg, JitterFn jitter = {})
        : cfg_(cfg),
          jitter_(std::move(jitter)),
          failures_(numSymbols, 0),
          nextAllowedMs_(numSymbols, 0) {
        budget_ = std::max<double>(cfg_.weightPerRequest,
                                   cfg_.weightLimitPerMin * cfg_.budgetFraction);
        tokens_ = budget_;
        if (!jitter_) {
            auto rng = std::make_shared<std::mt19937>(std::random_device{}());
            jitter_ = [rng] { return std::uniform_real_distribution<double>(0.0, 1.0)(*rng); };
        }
    }

    /** May a snapshot for symIdx be requested now? Consumes budget on yes. */
    bool tryAcquire(int symIdx, std::int64_t nowMs) {
        refill(nowMs);
        if (nowMs < pauseUntilMs_) return false;
        if (nowMs < nextAllowedMs_[symIdx]) return false;
        if (tokens_ < cfg_.weightPerRequest) return false;
        tokens_ -= cfg_.weightPerRequest;
        ++requests_;
        return true;
    }

    /** The HTTP fetch worked. usedWeight1m is the X-MBX-USED-WEIGHT-1M header, -1 if absent. */
    void onFetchOk(int /*symIdx*/, std::int64_t nowMs, int usedWeight1m = -1) {
        checkUsedWeight(nowMs, usedWeight1m);
    }

    /**
     * A request failed, or its snapshot did not produce a synced book.
     * httpStatus 0 = transport error / timeout / sync failure.
     */
    void onFailure(int symIdx, std::int64_t nowMs, int httpStatus = 0,
                   int retryAfterSec = 0, int usedWeight1m = -1) {
        ++failuresTotal_;
        int n = ++failures_[symIdx];
        std::int64_t delay = cfg_.initialBackoffMs;
        for (int i = 1; i < n && delay < cfg_.maxBackoffMs; ++i) delay *= 2;
        delay = std::min(delay, cfg_.maxBackoffMs);
        delay += static_cast<std::int64_t>(delay * cfg_.jitterFraction * jitter_());
        nextAllowedMs_[symIdx] = nowMs + delay;

        if (httpStatus == 429 || httpStatus == 418) {
            std::int64_t floorMs = (httpStatus == 418) ? cfg_.minPause418Ms : cfg_.minPause429Ms;
            pauseAll(nowMs, std::max<std::int64_t>(floorMs, retryAfterSec * 1000LL));
        }
        checkUsedWeight(nowMs, usedWeight1m);
    }

    /** The book for symIdx reached VALID: forget its failures. */
    void onSynced(int symIdx) {
        failures_[symIdx] = 0;
        nextAllowedMs_[symIdx] = 0;
    }

    // -- observers ----------------------------------------------------------
    long long requests() const { return requests_; }
    long long failures() const { return failuresTotal_; }
    long long rateLimitPauses() const { return rateLimitPauses_; }
    int consecutiveFailures(int symIdx) const { return failures_[symIdx]; }
    std::int64_t nextAllowedMs(int symIdx) const {
        return std::max(nextAllowedMs_[symIdx], pauseUntilMs_);
    }
    std::int64_t pausedUntilMs() const { return pauseUntilMs_; }
    double budgetWeightPerMin() const { return budget_; }

private:
    void refill(std::int64_t nowMs) {
        if (lastRefillMs_ < 0) { lastRefillMs_ = nowMs; return; }
        if (nowMs <= lastRefillMs_) return;
        tokens_ = std::min(budget_, tokens_ + budget_ * (nowMs - lastRefillMs_) / 60000.0);
        lastRefillMs_ = nowMs;
    }
    void pauseAll(std::int64_t nowMs, std::int64_t ms) {
        if (nowMs + ms > pauseUntilMs_) pauseUntilMs_ = nowMs + ms;
        ++rateLimitPauses_;
    }
    void checkUsedWeight(std::int64_t nowMs, int usedWeight1m) {
        if (usedWeight1m < 0) return;
        if (usedWeight1m >= cfg_.weightLimitPerMin * cfg_.usedWeightPauseFraction)
            pauseAll(nowMs, cfg_.usedWeightPauseMs);
    }

    SnapshotSchedulerConfig cfg_;
    JitterFn jitter_;
    std::vector<int> failures_;
    std::vector<std::int64_t> nextAllowedMs_;
    double budget_ = 0.0;
    double tokens_ = 0.0;
    std::int64_t lastRefillMs_ = -1;
    std::int64_t pauseUntilMs_ = 0;
    long long requests_ = 0;
    long long failuresTotal_ = 0;
    long long rateLimitPauses_ = 0;
};

} // namespace t2s

#endif // T2S_SNAPSHOT_SCHEDULER_HPP
