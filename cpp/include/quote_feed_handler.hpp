/**
 * @file quote_feed_handler.hpp
 * @brief WebSocket depth stream handler with snapshot reconciliation (L5)
 * 
 * Implements the full L5 book lifecycle:
 *   1. Connect to @depth@100ms WebSocket stream
 *   2. Buffer incoming deltas
 *   3. Fetch REST snapshot
 *   4. Apply snapshot + buffered deltas
 *   5. Continue applying live deltas
 *   6. Publish L5 on change/timeout
 * 
 * State machine (per symbol):
 *   INIT → (start buffering) → SYNCING → (snapshot + deltas) → VALID
 *   VALID → (sequence gap) → INVALID → INIT (rebuild)
 * 
 * Uses OrderBookManager for:
 *   - Flat-array storage (cache-friendly for 100+ symbols)
 *   - O(1) symbol lookup
 *   - Integrated publisher state
 * 
 * @see docs/decisions/adr-009-L1-Order-Book-Architecture.md
 */

#ifndef QUOTE_FEED_HANDLER_HPP
#define QUOTE_FEED_HANDLER_HPP

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>

#include <string>
#include <vector>
#include <atomic>
#include <memory>

#include "order_book_manager.hpp"
#include "rest_client.hpp"
#include "snapshot_worker.hpp"
#include "snapshot_scheduler.hpp"
#include "market_config.hpp"
#include "tp_publisher.hpp"
#include "row_clock.hpp"

extern "C" {
#include "k.h"
}

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

/**
 * @class QuoteFeedHandler
 * @brief Handles real-time L5 quote data from Binance depth streams
 * 
 * Key responsibilities:
 *   - WebSocket connection management (TLS) with auto-reconnect
 *   - Order book state management via OrderBookManager
 *   - REST snapshot fetching for initial sync
 *   - Delta buffering and replay
 *   - L5 quote extraction and publication
 *   - Graceful shutdown on signal
 */
class QuoteFeedHandler {
public:
    // ========================================================================
    // CONFIGURATION CONSTANTS
    // ========================================================================
    
    
    
    /// Nanoseconds between Unix epoch (1970) and kdb+ epoch (2000)
    static constexpr long long KDB_EPOCH_OFFSET_NS = 946684800000000000LL;
    
    /// Initial reconnection backoff (milliseconds)
    static constexpr int INITIAL_BACKOFF_MS = 1000;
    
    /// Maximum reconnection backoff (milliseconds)
    static constexpr int MAX_BACKOFF_MS = 8000;
    
    /// Backoff multiplier
    static constexpr int BACKOFF_MULTIPLIER = 2;
    

    // ========================================================================
    // CONSTRUCTION
    // ========================================================================
    
    /**
     * @brief Construct a quote feed handler
     * @param symbols List of symbols to subscribe to (lowercase, e.g., "btcusdt")
     * @param tpHost Tickerplant hostname
     * @param tpPort Tickerplant port
     */
    QuoteFeedHandler(const std::vector<std::string>& symbols,
                     const t2s::QuoteMarketConfig& market = t2s::QuoteMarketConfig{},
                     const std::string& tpHost = "localhost",
                     int tpPort = 5010);
    
    /// Destructor - ensures cleanup
    ~QuoteFeedHandler();
    
    // Non-copyable
    QuoteFeedHandler(const QuoteFeedHandler&) = delete;
    QuoteFeedHandler& operator=(const QuoteFeedHandler&) = delete;

    // ========================================================================
    // PUBLIC INTERFACE
    // ========================================================================
    
    /**
     * @brief Run the feed handler (blocking)
     * 
     * Connects to Binance and TP, then processes messages until stop() is called.
     * Automatically reconnects on disconnection.
     */
    void run();
    
    /**
     * @brief Request graceful shutdown
     * 
     * Thread-safe. Can be called from signal handler.
     */
    void stop();
    
    /**
     * @brief Check if handler is running
     */
    bool isRunning() const { return running_.load(); }
    
    /**
     * @brief Get count of messages processed
     */
    long long messageCount() const { return fhSeqNo_; }

    /// Non-empty if TP rejected this handler's session registration.
    const std::string& fatalError() const { return tp_->fatalError(); }

private:
    // ========================================================================
    // CONFIGURATION
    // ========================================================================
    
    std::vector<std::string> symbolsLower_;    // Lowercase for WebSocket subscription
    std::vector<std::string> symbolsUpper_;    // Uppercase for internal use
    t2s::QuoteMarketConfig cfg_;               // market wiring (hosts, table, sync rule)
    std::string tpHost_;
    int tpPort_;
    
    // ========================================================================
    // STATE
    // ========================================================================
    
    /// Shutdown flag
    std::atomic<bool> running_{true};
    
    /// Order book manager (flat arrays, all symbols)
    std::unique_ptr<OrderBookManager> bookMgr_;
    
    /// Connection to the tickerplant: registration, publishing, resend ring.
    std::unique_ptr<t2s::TpPublisher> tp_;
    
    /// FH sequence number
    long long fhSeqNo_{0};

    /// Session id announced to TP on every connect (process start time, ns).
    long long sessionId_{0};

    
    /// Binance reconnection attempt counter
    int binanceReconnectAttempt_{0};
    
    /// REST client for snapshots
    RestClient restClient_;

    /// Async worker that performs snapshot fetches off the WebSocket thread.
    /// The handler enqueues requests and polls for results at the start of
    /// each WebSocket loop iteration. See snapshot_worker.hpp for design.
    std::unique_ptr<t2s::SnapshotWorker<RestClient>> snapshotWorker_;

    /// Latest request id submitted per symbol. Used to discard stale results
    /// (e.g. if the symbol was reset and re-requested while a previous
    /// snapshot was still in flight).
    std::vector<std::uint64_t> latestRequestId_;

    /// Decides when a snapshot may be requested: per-symbol backoff after
    /// failures, a shared request-weight budget, and pauses on HTTP 429/418.
    /// See snapshot_scheduler.hpp.
    std::unique_ptr<t2s::SnapshotScheduler> snapshotScheduler_;

    /// When the in-flight snapshot for each symbol was requested (steady
    /// ms, 0 = none). A request with no result after SNAPSHOT_TIMEOUT_MS
    /// is treated as failed so the symbol cannot wait forever.
    std::vector<std::int64_t> snapshotRequestedAtMs_;
    static constexpr std::int64_t SNAPSHOT_TIMEOUT_MS = 30000;

    /// Monotonic milliseconds for the scheduler.
    static std::int64_t steadyNowMs();

    // Book-level counters (snapshot requests and rate-limit pauses live in
    // the scheduler; buffer overflows and depth refreshes in the book manager)
    long long ctrSequenceGaps_{0};      ///< gaps in the exchange's update ids while VALID
    long long ctrResyncs_{0};           ///< books rebuilt from scratch (gap or failed sync)
    long long ctrSnapshotFailures_{0};  ///< snapshot fetches that failed or timed out
    long long ctrWsReconnects_{0};      ///< Binance WebSocket connections lost and re-opened

    /// Decides each row's `time`: the receive time, or the exchange event
    /// time while the system clock lags it (after a wake). Counts the
    /// corrected rows, reported to TP as clockLagRows.
    t2s::RowClock rowClock_;

    /// One STATS line with every counter, every STATS_INTERVAL_SEC and at exit.
    void logStats() const;
    static constexpr int STATS_INTERVAL_SEC = 60;
    
    // ========================================================================
    // HEALTH TRACKING
    // ========================================================================
    
    /// Handler start time (for uptime calculation)
    std::chrono::system_clock::time_point startTime_;
    
    /// Total messages received from Binance
    long long msgsReceived_{0};
    
    /// Total messages published to TP
    long long msgsPublished_{0};
    
    /// Time of last message received
    std::chrono::system_clock::time_point lastMsgTime_;
    
    /// Time of last publish to TP
    std::chrono::system_clock::time_point lastPubTime_;
    
    /// Current connection state
    std::string connState_{"disconnected"};
    
    /// Last parse latency in microseconds (parse + order book update)
    long long lastParseUs_{0};
    
    /// Health publish interval in seconds
    static constexpr int HEALTH_INTERVAL_SEC = 5;

    // ========================================================================
    // PRIVATE METHODS
    // ========================================================================
    
    /// Build WebSocket path for depth streams
    std::string buildDepthStreamPath() const;
    

    
    /// Sleep with exponential backoff
    bool sleepWithBackoff(int attempt);
    
    /// Process incoming WebSocket message
    void processMessage(const std::string& msg, long long fhRecvTimeUtcNs);
    
    /// Handle delta based on current book state
    void handleDelta(int symIdx, const BufferedDelta& delta, long long fhRecvTimeUtcNs);
    
    /// Enqueue an async snapshot request via the SnapshotWorker. Returns
    /// immediately; the WebSocket loop continues reading deltas (which the
    /// book manager buffers) until the snapshot result lands.
    void requestSnapshot(int symIdx);

    /// Drain any completed snapshot results from the worker and apply them.
    /// Stale results (where a newer request has been submitted for the same
    /// symbol) are discarded. Called at the start of each WebSocket loop
    /// iteration.
    void applySnapshotResults();
    
    /// Maybe publish L5 for a symbol
    void maybePublish(int symIdx, long long fhRecvTimeUtcNs);
    
    /// Publish invalid state for a symbol
    void publishInvalid(int symIdx, long long fhRecvTimeUtcNs);
    
    /// Publish L5 quote to kdb+
    void publishQuote(const BookQuote& quote);
    
    /// Check publish timeouts for all symbols
    void checkPublishTimeouts(long long fhRecvTimeUtcNs);
    
    /// Run the WebSocket connection loop
    void runWebSocketLoop();
    
    /// Publish health metrics to TP
    void publishHealth();
};

#endif // QUOTE_FEED_HANDLER_HPP
