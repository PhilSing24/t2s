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
    
    /// Binance WebSocket host
    static constexpr const char* BINANCE_HOST = "stream.binance.com";
    
    /// Binance WebSocket port (TLS)
    static constexpr const char* BINANCE_PORT = "9443";
    
    /// Nanoseconds between Unix epoch (1970) and kdb+ epoch (2000)
    static constexpr long long KDB_EPOCH_OFFSET_NS = 946684800000000000LL;
    
    /// Initial reconnection backoff (milliseconds)
    static constexpr int INITIAL_BACKOFF_MS = 1000;
    
    /// Maximum reconnection backoff (milliseconds)
    static constexpr int MAX_BACKOFF_MS = 8000;
    
    /// Backoff multiplier
    static constexpr int BACKOFF_MULTIPLIER = 2;
    
    /// Snapshot depth to request (get more than L5 for safety)
    static constexpr int SNAPSHOT_DEPTH = 50;

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

private:
    // ========================================================================
    // CONFIGURATION
    // ========================================================================
    
    std::vector<std::string> symbolsLower_;    // Lowercase for WebSocket subscription
    std::vector<std::string> symbolsUpper_;    // Uppercase for internal use
    std::string tpHost_;
    int tpPort_;
    
    // ========================================================================
    // STATE
    // ========================================================================
    
    /// Shutdown flag
    std::atomic<bool> running_{true};
    
    /// Order book manager (flat arrays, all symbols)
    std::unique_ptr<OrderBookManager> bookMgr_;
    
    /// Tickerplant connection handle
    int tpHandle_{-1};
    
    /// FH sequence number
    long long fhSeqNo_{0};
    
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
    
    /// Connect to tickerplant with retry
    bool connectToTP();
    
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
    void publishL5(const L5Quote& quote);
    
    /// Check publish timeouts for all symbols
    void checkPublishTimeouts(long long fhRecvTimeUtcNs);
    
    /// Run the WebSocket connection loop
    void runWebSocketLoop();
    
    /// Publish health metrics to TP
    void publishHealth();
};

#endif // QUOTE_FEED_HANDLER_HPP
