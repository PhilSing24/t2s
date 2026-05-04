/**
 * @file snapshot_worker.hpp
 * @brief Async snapshot fetcher for the quote feed handler.
 *
 * Moves REST snapshot fetches off the WebSocket read thread. The WebSocket
 * loop enqueues requests and polls results at the start of each iteration;
 * a single worker thread does the actual HTTP round trips.
 *
 * Without this, snapshot fetches block ws.read() for 100-500ms each. When
 * many symbols need snapshots at once (post-reconnect), Binance's send
 * buffer fills, deltas pile up, and Binance closes the connection.
 *
 * Design:
 * - Single worker thread (snapshot fetches are rate-limited by Binance
 *   anyway, no benefit from parallelism).
 * - Bounded input queue (drop-oldest on full to prevent unbounded growth).
 * - Each request carries a monotonic requestId; the WebSocket loop tracks
 *   the latest request per symbol so stale results can be discarded.
 * - Worker is templated on the fetcher type for testability - in tests we
 *   substitute a mock that returns canned data without hitting the network.
 */

#ifndef SNAPSHOT_WORKER_HPP
#define SNAPSHOT_WORKER_HPP

#include "rest_client.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace t2s {

// Request queued by the WebSocket loop.
struct SnapshotRequest {
    int symIdx;
    std::string sym;
    std::uint64_t requestId;  // monotonic, unique per (handler, run)
};

// Result returned by the worker.
struct SnapshotResult {
    int symIdx;
    std::string sym;
    std::uint64_t requestId;
    SnapshotData data;
};

/**
 * Thread-safe bounded FIFO queue with shutdown semantics.
 *
 * Used for both request and result queues. push() drops oldest on full;
 * pop_blocking() waits until an item is available or shutdown is signalled.
 */
template <typename T>
class BoundedQueue {
public:
    explicit BoundedQueue(std::size_t capacity) : capacity_(capacity) {}

    /** Push an item. Returns false and drops the oldest if full. */
    bool push(T item) {
        std::lock_guard<std::mutex> lk(mu_);
        if (shutdown_) return false;
        bool dropped = false;
        if (q_.size() >= capacity_) {
            q_.pop_front();
            dropped = true;
        }
        q_.push_back(std::move(item));
        cv_.notify_one();
        return !dropped;
    }

    /** Block until an item is available or shutdown. */
    std::optional<T> pop_blocking() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait(lk, [&] { return shutdown_ || !q_.empty(); });
        if (q_.empty()) return std::nullopt;
        T item = std::move(q_.front());
        q_.pop_front();
        return item;
    }

    /** Non-blocking pop. Returns nullopt if empty. */
    std::optional<T> try_pop() {
        std::lock_guard<std::mutex> lk(mu_);
        if (q_.empty()) return std::nullopt;
        T item = std::move(q_.front());
        q_.pop_front();
        return item;
    }

    /** Drain all items (used by WebSocket loop to apply all pending results). */
    std::deque<T> drain() {
        std::lock_guard<std::mutex> lk(mu_);
        std::deque<T> out;
        std::swap(out, q_);
        return out;
    }

    /** Signal shutdown. Wakes blocked consumers. They observe nullopt. */
    void shutdown() {
        std::lock_guard<std::mutex> lk(mu_);
        shutdown_ = true;
        cv_.notify_all();
    }

    std::size_t size() const {
        std::lock_guard<std::mutex> lk(mu_);
        return q_.size();
    }

    bool is_shutdown() const {
        std::lock_guard<std::mutex> lk(mu_);
        return shutdown_;
    }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::deque<T> q_;
    std::size_t capacity_;
    bool shutdown_ = false;
};

/**
 * Async snapshot worker.
 *
 * Templated on FetcherT for testability: production code uses RestClient;
 * tests inject a fake. The fetcher must expose:
 *   SnapshotData fetchSnapshot(const std::string& sym, int depth);
 */
template <typename FetcherT>
class SnapshotWorker {
public:
    static constexpr std::size_t QUEUE_CAPACITY = 10;
    static constexpr int SNAPSHOT_DEPTH = 1000;

    explicit SnapshotWorker(FetcherT& fetcher)
        : fetcher_(fetcher),
          requests_(QUEUE_CAPACITY),
          results_(QUEUE_CAPACITY) {}

    ~SnapshotWorker() { stop(); }

    SnapshotWorker(const SnapshotWorker&) = delete;
    SnapshotWorker& operator=(const SnapshotWorker&) = delete;

    /** Start the worker thread. Idempotent. */
    void start() {
        if (running_.exchange(true)) return;
        thread_ = std::thread([this] { runLoop(); });
    }

    /** Signal stop and join the worker thread. Idempotent. */
    void stop() {
        if (!running_.exchange(false)) return;
        requests_.shutdown();
        results_.shutdown();
        if (thread_.joinable()) thread_.join();
    }

    /**
     * Enqueue a snapshot request. Returns the assigned request id on success,
     * or 0 if the queue rejected the push (capacity reached + drop-oldest
     * already applied means the worker is overloaded; caller should treat
     * as failure and reset the symbol).
     */
    std::uint64_t enqueueRequest(int symIdx, const std::string& sym) {
        std::uint64_t id = nextRequestId_.fetch_add(1) + 1;
        SnapshotRequest req{symIdx, sym, id};
        bool accepted = requests_.push(std::move(req));
        if (!accepted) {
            spdlog::warn("SnapshotWorker: queue full, dropped oldest request");
            // We still allocated the id and pushed; the dropped one was an
            // older request. Return the id - caller treats this as success
            // because their request did make it onto the queue.
        }
        return id;
    }

    /** Drain all pending results. Called by the WebSocket loop. */
    std::deque<SnapshotResult> drainResults() { return results_.drain(); }

    std::size_t pendingRequests() const { return requests_.size(); }
    std::size_t pendingResults() const { return results_.size(); }

private:
    void runLoop() {
        while (running_.load()) {
            auto reqOpt = requests_.pop_blocking();
            if (!reqOpt.has_value()) break;  // shutdown
            const auto& req = *reqOpt;

            spdlog::debug("SnapshotWorker: fetching {}", req.sym);
            SnapshotData data = fetcher_.fetchSnapshot(req.sym, SNAPSHOT_DEPTH);
            if (!data.success) {
                spdlog::warn("SnapshotWorker: fetch failed for {}: {}",
                             req.sym, data.error);
            }

            SnapshotResult result{req.symIdx, req.sym, req.requestId, std::move(data)};
            results_.push(std::move(result));
        }
    }

    FetcherT& fetcher_;
    BoundedQueue<SnapshotRequest> requests_;
    BoundedQueue<SnapshotResult> results_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> nextRequestId_{0};
};

} // namespace t2s

#endif // SNAPSHOT_WORKER_HPP
