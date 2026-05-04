/**
 * @file test_snapshot_worker.cpp
 * @brief Unit tests for SnapshotWorker and BoundedQueue.
 *
 * Covers:
 *   - BoundedQueue: push/pop, bounded drop-oldest, blocking pop, shutdown
 *   - SnapshotWorker: start/stop, enqueue, result delivery, request id
 *     monotonicity, integration with a fake fetcher
 *
 * The fetcher type is templated, so we substitute a controllable fake
 * here that records calls and returns canned data without hitting the
 * network. Production code uses RestClient.
 */

#include "snapshot_worker.hpp"
#include "catch_amalgamated.hpp"

#include <chrono>
#include <thread>
#include <atomic>
#include <vector>

using namespace t2s;
using namespace std::chrono_literals;

// ============================================================================
// BoundedQueue
// ============================================================================

TEST_CASE("BoundedQueue accepts up to capacity", "[queue]") {
    BoundedQueue<int> q(3);
    REQUIRE(q.push(1));
    REQUIRE(q.push(2));
    REQUIRE(q.push(3));
    REQUIRE(q.size() == 3);
}

TEST_CASE("BoundedQueue drops oldest when full", "[queue]") {
    BoundedQueue<int> q(2);
    q.push(1);
    q.push(2);
    bool ok = q.push(3);   // full -> drops oldest, returns false
    REQUIRE_FALSE(ok);
    REQUIRE(q.size() == 2);

    // Drained order: 2, 3 (1 was dropped)
    auto a = q.try_pop();
    auto b = q.try_pop();
    auto c = q.try_pop();
    REQUIRE(a.has_value());
    REQUIRE(*a == 2);
    REQUIRE(b.has_value());
    REQUIRE(*b == 3);
    REQUIRE_FALSE(c.has_value());
}

TEST_CASE("BoundedQueue try_pop returns nullopt on empty", "[queue]") {
    BoundedQueue<int> q(2);
    REQUIRE_FALSE(q.try_pop().has_value());
}

TEST_CASE("BoundedQueue drain returns all items in FIFO order", "[queue]") {
    BoundedQueue<int> q(10);
    q.push(1);
    q.push(2);
    q.push(3);
    auto items = q.drain();
    REQUIRE(items.size() == 3);
    REQUIRE(items[0] == 1);
    REQUIRE(items[1] == 2);
    REQUIRE(items[2] == 3);
    REQUIRE(q.size() == 0);
}

TEST_CASE("BoundedQueue shutdown wakes blocked consumer", "[queue]") {
    BoundedQueue<int> q(5);
    std::atomic<bool> got_nullopt{false};

    std::thread consumer([&] {
        auto v = q.pop_blocking();
        if (!v.has_value()) got_nullopt = true;
    });

    // Give consumer a moment to start blocking
    std::this_thread::sleep_for(20ms);
    q.shutdown();
    consumer.join();
    REQUIRE(got_nullopt.load());
}

TEST_CASE("BoundedQueue rejects push after shutdown", "[queue]") {
    BoundedQueue<int> q(5);
    q.shutdown();
    REQUIRE_FALSE(q.push(1));
    REQUIRE(q.size() == 0);
}

// ============================================================================
// SnapshotWorker - fake fetcher
// ============================================================================

namespace {

/// Fake snapshot fetcher for tests. Records calls and returns canned data.
struct FakeFetcher {
    std::atomic<int> callCount{0};
    bool shouldSucceed{true};
    std::chrono::milliseconds latency{0ms};

    SnapshotData fetchSnapshot(const std::string& sym, int /*depth*/) {
        callCount.fetch_add(1);
        std::this_thread::sleep_for(latency);

        SnapshotData d;
        d.success = shouldSucceed;
        if (shouldSucceed) {
            d.lastUpdateId = 1000 + callCount.load();
            d.bids = {{50000.0, 1.0}, {49999.0, 2.0}};
            d.asks = {{50001.0, 1.0}, {50002.0, 2.0}};
        } else {
            d.error = "fake failure";
        }
        return d;
    }
};

// Helper to wait until predicate is true or timeout.
template <typename Pred>
bool waitFor(Pred p, std::chrono::milliseconds timeout = 1s) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (p()) return true;
        std::this_thread::sleep_for(2ms);
    }
    return p();
}

} // namespace

// ============================================================================
// SnapshotWorker
// ============================================================================

TEST_CASE("SnapshotWorker start/stop without work", "[worker]") {
    FakeFetcher f;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();
    std::this_thread::sleep_for(10ms);
    w.stop();
    REQUIRE(f.callCount == 0);
}

TEST_CASE("SnapshotWorker fetches a single request", "[worker]") {
    FakeFetcher f;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();

    auto id = w.enqueueRequest(0, "BTCUSDT");
    REQUIRE(id == 1);

    REQUIRE(waitFor([&] { return w.pendingResults() > 0; }));

    auto results = w.drainResults();
    REQUIRE(results.size() == 1);
    REQUIRE(results[0].symIdx == 0);
    REQUIRE(results[0].sym == "BTCUSDT");
    REQUIRE(results[0].requestId == 1);
    REQUIRE(results[0].data.success);
    REQUIRE(results[0].data.lastUpdateId == 1001);

    w.stop();
}

TEST_CASE("SnapshotWorker assigns monotonic request ids", "[worker]") {
    FakeFetcher f;
    f.latency = 1ms;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();

    auto a = w.enqueueRequest(0, "BTCUSDT");
    auto b = w.enqueueRequest(1, "ETHUSDT");
    auto c = w.enqueueRequest(2, "SOLUSDT");
    REQUIRE(a == 1);
    REQUIRE(b == 2);
    REQUIRE(c == 3);

    REQUIRE(waitFor([&] { return f.callCount == 3; }));

    w.stop();
}

TEST_CASE("SnapshotWorker propagates fetch failures", "[worker]") {
    FakeFetcher f;
    f.shouldSucceed = false;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();

    w.enqueueRequest(0, "BTCUSDT");
    REQUIRE(waitFor([&] { return w.pendingResults() > 0; }));

    auto results = w.drainResults();
    REQUIRE(results.size() == 1);
    REQUIRE_FALSE(results[0].data.success);
    REQUIRE(results[0].data.error == "fake failure");

    w.stop();
}

TEST_CASE("SnapshotWorker processes many requests in order", "[worker]") {
    FakeFetcher f;
    f.latency = 0ms;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();

    constexpr int N = 5;  // <= QUEUE_CAPACITY (10)
    for (int i = 0; i < N; ++i) {
        w.enqueueRequest(i, "SYM" + std::to_string(i));
    }

    REQUIRE(waitFor([&] { return f.callCount == N; }));

    auto results = w.drainResults();
    REQUIRE(results.size() == N);
    // Single worker thread => results in submission order
    for (int i = 0; i < N; ++i) {
        REQUIRE(results[i].symIdx == i);
        REQUIRE(results[i].requestId == static_cast<std::uint64_t>(i + 1));
    }

    w.stop();
}

TEST_CASE("SnapshotWorker double-stop is safe", "[worker]") {
    FakeFetcher f;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();
    w.stop();
    w.stop();  // should not deadlock or throw
    REQUIRE(true);
}

TEST_CASE("SnapshotWorker double-start is safe", "[worker]") {
    FakeFetcher f;
    SnapshotWorker<FakeFetcher> w(f);
    w.start();
    w.start();  // idempotent - should not spawn a second thread
    w.enqueueRequest(0, "BTCUSDT");
    REQUIRE(waitFor([&] { return f.callCount == 1; }));
    w.stop();
}
