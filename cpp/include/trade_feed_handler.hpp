/**
 * @file trade_feed_handler.hpp
 * @brief Real-time Binance trade feed handler with kdb+ IPC publishing
 *
 * This feed handler connects to a Binance WebSocket stream, receives
 * real-time trade events, normalizes them, and publishes to a kdb+
 * tickerplant via IPC.
 *
 * The handler is parameterized over a t2s::MarketConfig that carries
 * the host, port, stream suffix, destination TP table, and schema kind.
 * Defaults select Binance spot; the futures binary supplies a different
 * MarketConfig at construction. See ADR-013.
 *
 * Architecture role:
 *   Binance WebSocket -> [Trade Feed Handler] -> Tickerplant -> RDB/RTE
 *
 * @see docs/decisions/adr-001-Timestamps-and-latency-measurement.md
 * @see docs/decisions/adr-002-Feed-handler-to-kdb-ingestion-path.md
 * @see docs/decisions/adr-008-Error-Handling-Strategy.md
 * @see docs/decisions/adr-013-Multi-Market-Trade-Ingestion.md
 */

#ifndef TRADE_FEED_HANDLER_HPP
#define TRADE_FEED_HANDLER_HPP

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/host_name_verification.hpp>

#include <string>
#include <vector>
#include <unordered_map>
#include <atomic>

#include "market_config.hpp"

// kdb+ C API
extern "C" {
#include "k.h"
}

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

/**
 * @class TradeFeedHandler
 * @brief Handles real-time trade data from a Binance trade-style stream and publishes to kdb+ TP
 *
 * Key responsibilities:
 *   - WebSocket connection management (TLS) with auto-reconnect
 *   - JSON parsing and normalization
 *   - Timestamp capture (wall-clock and monotonic)
 *   - Latency instrumentation (parse time, send time)
 *   - Sequence numbering for gap detection
 *   - IPC publication to tickerplant with reconnect
 *   - Graceful shutdown on signal
 *
 * Design decisions:
 *   - Tick-by-tick publishing (no batching) for latency measurement clarity
 *   - Async IPC (neg handle) to minimize blocking
 *   - Combined stream subscription for multi-symbol support
 *   - Reconnect with exponential backoff on disconnect
 *   - Market-specific wiring (host/port/stream/table/schema) injected via MarketConfig
 */
class TradeFeedHandler {
public:
    // ========================================================================
    // CONFIGURATION CONSTANTS (market-agnostic)
    // ========================================================================

    /// Nanoseconds between Unix epoch (1970) and kdb+ epoch (2000)
    static constexpr long long KDB_EPOCH_OFFSET_NS = 946684800000000000LL;

    /// Initial reconnection backoff (milliseconds)
    static constexpr int INITIAL_BACKOFF_MS = 1000;

    /// Maximum reconnection backoff (milliseconds)
    static constexpr int MAX_BACKOFF_MS = 8000;

    /// Backoff multiplier for exponential backoff
    static constexpr int BACKOFF_MULTIPLIER = 2;

    // ========================================================================
    // CONSTRUCTION
    // ========================================================================

    /**
     * @brief Construct a trade feed handler
     * @param symbols  List of symbols to subscribe to (lowercase, e.g., "btcusdt")
     * @param market   Per-market wiring (host, port, stream suffix, TP table, schema)
     * @param tpHost   Tickerplant hostname
     * @param tpPort   Tickerplant port
     */
    TradeFeedHandler(const std::vector<std::string>& symbols,
                     t2s::MarketConfig market,
                     const std::string& tpHost = "localhost",
                     int tpPort = 5010);

    /// Destructor - ensures cleanup
    ~TradeFeedHandler();

    // Non-copyable
    TradeFeedHandler(const TradeFeedHandler&) = delete;
    TradeFeedHandler& operator=(const TradeFeedHandler&) = delete;

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
     * The run() method will exit after current operation completes.
     */
    void stop();

    /**
     * @brief Check if handler is running
     * @return true if running, false if stopped or stopping
     */
    bool isRunning() const { return running_.load(); }

    /**
     * @brief Get count of messages processed
     * @return Number of trades published to TP
     */
    long long messageCount() const { return fhSeqNo_; }

private:
    // ========================================================================
    // CONFIGURATION
    // ========================================================================

    std::vector<std::string> symbols_;
    t2s::MarketConfig        cfg_;
    std::string              tpHost_;
    int                      tpPort_;

    // ========================================================================
    // STATE
    // ========================================================================

    /// Shutdown flag (atomic for thread-safe signal handling)
    std::atomic<bool> running_{true};

    /// FH sequence number (monotonically increasing per instance)
    long long fhSeqNo_{0};

    /// Last tradeId per symbol (for gap detection)
    std::unordered_map<std::string, long long> lastTradeId_;

    /// Tickerplant connection handle
    int tpHandle_{-1};

    /// Binance reconnection attempt counter
    int binanceReconnectAttempt_{0};

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

    /// Health publish interval in seconds
    static constexpr int HEALTH_INTERVAL_SEC = 5;

    // ========================================================================
    // PRIVATE METHODS
    // ========================================================================

    /**
     * @brief Connect to tickerplant with retry
     * @return true if connected, false if shutdown requested
     */
    bool connectToTP();

    /**
     * @brief Sleep with exponential backoff
     * @param attempt Current attempt number (0-based)
     * @return true if should continue, false if shutdown requested
     */
    bool sleepWithBackoff(int attempt);

    /**
     * @brief Validate tradeId sequence and log anomalies
     * @param sym Symbol name
     * @param tradeId Current trade ID
     */
    void validateTradeId(const std::string& sym, long long tradeId);

    /**
     * @brief Process a single WebSocket message
     * @param msg Raw JSON message from Binance
     */
    void processMessage(const std::string& msg);

    /**
     * @brief Run the WebSocket connection loop
     *
     * Connects to Binance and processes messages until disconnected or stopped.
     * Called by run() inside the reconnection loop.
     */
    void runWebSocketLoop();

    /**
     * @brief Publish health metrics to TP
     *
     * Called periodically to report handler health status.
     */
    void publishHealth();
};

#endif // TRADE_FEED_HANDLER_HPP
