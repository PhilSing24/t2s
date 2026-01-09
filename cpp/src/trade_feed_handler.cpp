/**
 * @file trade_feed_handler.cpp
 * @brief Implementation of TradeFeedHandler class
 */

#include "trade_feed_handler.hpp"

#include <rapidjson/document.h>
#include <spdlog/spdlog.h>

#include <iostream>
#include <chrono>
#include <thread>
#include <csignal>

// ============================================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================================

TradeFeedHandler::TradeFeedHandler(const std::vector<std::string>& symbols,
                                   const std::string& tpHost,
                                   int tpPort)
    : symbols_(symbols)
    , tpHost_(tpHost)
    , tpPort_(tpPort)
    , startTime_(std::chrono::system_clock::now())
{
}

TradeFeedHandler::~TradeFeedHandler() {
    if (tpHandle_ > 0) {
        kclose(tpHandle_);
        spdlog::debug("TP connection closed in destructor");
    }
}

// ============================================================================
// PUBLIC INTERFACE
// ============================================================================

void TradeFeedHandler::run() {
    spdlog::info("Starting...");
    spdlog::info("Symbols: {}", fmt::join(symbols_, " "));
    
    // Connect to tickerplant (retries until success or shutdown)
    if (!connectToTP()) {
        spdlog::warn("Shutdown before TP connection established");
        return;
    }
    
    // Main loop with reconnection
    while (running_) {
        try {
            runWebSocketLoop();
        } catch (const std::exception& e) {
            if (!running_) {
                spdlog::info("Connection closed during shutdown");
            } else {
                spdlog::error("Binance error: {}", e.what());
                spdlog::info("Will reconnect...");
                if (!sleepWithBackoff(binanceReconnectAttempt_++)) {
                    break;  // Shutdown requested during backoff
                }
            }
        }
    }
    
    // Cleanup
    spdlog::info("Cleaning up...");
    if (tpHandle_ > 0) {
        kclose(tpHandle_);
        tpHandle_ = -1;
        spdlog::info("TP connection closed");
    }
    
    spdlog::info("Shutdown complete (processed {} messages)", fhSeqNo_);
}

void TradeFeedHandler::stop() {
    spdlog::info("Stop requested");
    running_ = false;
}

// ============================================================================
// PRIVATE METHODS
// ============================================================================

std::string TradeFeedHandler::buildStreamPath() const {
    std::string path = "/stream?streams=";
    for (size_t i = 0; i < symbols_.size(); ++i) {
        if (i > 0) path += "/";
        path += symbols_[i] + "@trade";
    }
    return path;
}

bool TradeFeedHandler::connectToTP() {
    int attempt = 0;
    while (running_) {
        spdlog::info("Connecting to TP on {}:{}...", tpHost_, tpPort_);
        
        int h = khpu((S)tpHost_.c_str(), tpPort_, (S)"");
        
        if (h > 0) {
            tpHandle_ = h;
            spdlog::info("Connected to TP (handle {})", h);
            return true;
        }
        
        spdlog::error("Failed to connect to TP");
        if (!sleepWithBackoff(attempt++)) {
            return false;  // Shutdown requested
        }
    }
    return false;  // Shutdown requested
}

bool TradeFeedHandler::sleepWithBackoff(int attempt) {
    int delay = INITIAL_BACKOFF_MS;
    for (int i = 0; i < attempt && delay < MAX_BACKOFF_MS; ++i) {
        delay *= BACKOFF_MULTIPLIER;
    }
    delay = std::min(delay, MAX_BACKOFF_MS);
    
    spdlog::info("Waiting {}ms before reconnect...", delay);
    
    // Sleep in small increments to allow quick shutdown response
    const int checkIntervalMs = 100;
    int slept = 0;
    while (slept < delay && running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
        slept += checkIntervalMs;
    }
    
    return running_;
}

void TradeFeedHandler::validateTradeId(const std::string& sym, long long tradeId) {
    auto it = lastTradeId_.find(sym);
    
    if (it != lastTradeId_.end()) {
        long long last = it->second;
        
        if (tradeId < last) {
            spdlog::warn("OUT OF ORDER: {} last={} got={}", sym, last, tradeId);
        } else if (tradeId == last) {
            spdlog::warn("DUPLICATE: {} tradeId={}", sym, tradeId);
        } else if (tradeId > last + 1) {
            long long missed = tradeId - last - 1;
            spdlog::warn("Gap: {} missed={} (last={} got={})", sym, missed, last, tradeId);
        }
    }
    
    lastTradeId_[sym] = tradeId;
}

void TradeFeedHandler::processMessage(const std::string& msg) {
    // Capture wall-clock receive time (for cross-process correlation)
    auto recvWall = std::chrono::system_clock::now();
    long long fhRecvTimeUtcNs =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            recvWall.time_since_epoch()).count();
    
    // Update health: message received
    lastMsgTime_ = recvWall;
    ++msgsReceived_;
    
    // Start monotonic timer for parse latency
    auto parseStart = std::chrono::steady_clock::now();
    
    // Parse JSON
    rapidjson::Document doc;
    doc.Parse(msg.c_str());
    if (!doc.IsObject()) return;
    
    // Combined stream format: {"stream":"btcusdt@trade","data":{...}}
    if (!doc.HasMember("data")) return;
    const rapidjson::Value& d = doc["data"];
    
    if (!d.IsObject()) return;
    if (!d.HasMember("s")) return;
    
    // Extract trade fields
    const char* sym = d["s"].GetString();
    long long tradeId = d["t"].GetInt64();
    double price = std::stod(d["p"].GetString());
    double qty = std::stod(d["q"].GetString());
    bool buyerIsMaker = d["m"].GetBool();
    long long exchEventTimeMs = d["E"].GetInt64();
    long long exchTradeTimeMs = d["T"].GetInt64();
    
    // Validate sequence
    validateTradeId(sym, tradeId);
    
    // End parse timer
    auto parseEnd = std::chrono::steady_clock::now();
    long long fhParseUs = std::chrono::duration_cast<std::chrono::microseconds>(
        parseEnd - parseStart).count();
    
    // Increment sequence number
    ++fhSeqNo_;
    
    // Build kdb+ row
    K row = knk(12,
        ktj(-KP, fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS),
        ks((S)sym),
        kj(tradeId),
        kf(price),
        kf(qty),
        kb(buyerIsMaker),
        kj(exchEventTimeMs),
        kj(exchTradeTimeMs),
        kj(fhRecvTimeUtcNs),
        kj(fhParseUs),
        kj(0LL),  // fhSendUs placeholder
        kj(fhSeqNo_)
    );
    
    // Capture send time
    auto sendEnd = std::chrono::steady_clock::now();
    long long fhSendUs = std::chrono::duration_cast<std::chrono::microseconds>(
        sendEnd - parseEnd).count();
    kK(row)[10]->j = fhSendUs;
    
    // Debug output (only shown at debug level)
    spdlog::debug("Trade: sym={} tradeId={} price={:.2f} qty={:.4f} fhParseUs={} fhSendUs={} fhSeqNo={}",
        sym, tradeId, price, qty, fhParseUs, fhSendUs, fhSeqNo_);
    
    // Publish to TP
    K result = k(-tpHandle_, (S)".u.upd", ks((S)"trade_binance"), row, (K)0);
    
    // Update health: message published
    lastPubTime_ = std::chrono::system_clock::now();
    ++msgsPublished_;
    
    // Check if TP connection died
    if (result == nullptr) {
        spdlog::error("TP connection lost, reconnecting...");
        connState_ = "reconnecting";
        kclose(tpHandle_);
        tpHandle_ = -1;
        if (connectToTP()) {
            // Resend to new connection
            k(-tpHandle_, (S)".u.upd", ks((S)"trade_binance"), row, (K)0);
        }
    }
}

void TradeFeedHandler::runWebSocketLoop() {
    std::string target = buildStreamPath();
    spdlog::info("Connecting to Binance: {}{}", BINANCE_HOST, target);
    
    connState_ = "connecting";
    
    // Initialize ASIO and SSL
    net::io_context ioc;
    ssl::context ctx{ssl::context::tlsv12_client};
    ctx.set_default_verify_paths();
    
    // Resolve and connect
    tcp::resolver resolver{ioc};
    websocket::stream<beast::ssl_stream<tcp::socket>> ws{ioc, ctx};
    
    auto const results = resolver.resolve(BINANCE_HOST, BINANCE_PORT);
    net::connect(ws.next_layer().next_layer(), results.begin(), results.end());
    
    // TLS handshake
    ws.next_layer().handshake(ssl::stream_base::client);
    
    // WebSocket handshake
    ws.handshake(BINANCE_HOST, target);
    
    spdlog::info("Connected to Binance ({} symbols)", symbols_.size());
    connState_ = "connected";
    
    // Reset backoff on successful connection
    binanceReconnectAttempt_ = 0;
    
    // Health publish timer
    auto lastHealthPub = std::chrono::steady_clock::now();
    
    // Message loop
    while (running_) {
        beast::flat_buffer buffer;
        ws.read(buffer);
        
        if (!running_) break;
        
        std::string msg = beast::buffers_to_string(buffer.data());
        processMessage(msg);
        
        // Publish health every HEALTH_INTERVAL_SEC seconds
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - lastHealthPub).count() >= HEALTH_INTERVAL_SEC) {
            publishHealth();
            lastHealthPub = now;
        }
    }
    
    connState_ = "disconnected";
    
    // Graceful close
    if (!running_) {
        try {
            ws.close(websocket::close_code::normal);
            spdlog::info("WebSocket closed gracefully");
        } catch (...) {
            // Ignore errors during shutdown
        }
    }
}

void TradeFeedHandler::publishHealth() {
    if (tpHandle_ <= 0) return;
    
    auto now = std::chrono::system_clock::now();
    
    // Calculate uptime
    long long uptimeSec = std::chrono::duration_cast<std::chrono::seconds>(
        now - startTime_).count();
    
    // Convert timestamps to kdb+ format
    auto toKdbTs = [](std::chrono::system_clock::time_point tp) -> long long {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            tp.time_since_epoch()).count() - KDB_EPOCH_OFFSET_NS;
    };
    
    // Build health row (10 fields)
    K row = knk(10,
        ktj(-KP, toKdbTs(now)),                    // time
        ks((S)"trade_fh"),                          // handler
        ktj(-KP, toKdbTs(startTime_)),             // startTimeUtc
        kj(uptimeSec),                              // uptimeSec
        kj(msgsReceived_),                          // msgsReceived
        kj(msgsPublished_),                         // msgsPublished
        ktj(-KP, toKdbTs(lastMsgTime_)),           // lastMsgTimeUtc
        ktj(-KP, toKdbTs(lastPubTime_)),           // lastPubTimeUtc
        ks((S)connState_.c_str()),                  // connState
        ki(static_cast<int>(symbols_.size()))       // symbolCount
    );
    
    // Publish to TP (fire and forget)
    k(-tpHandle_, (S)".u.upd", ks((S)"health_feed_handler"), row, (K)0);
    
    spdlog::debug("Health published: uptime={}s msgs={}/{} state={}", 
        uptimeSec, msgsReceived_, msgsPublished_, connState_);
}

// ============================================================================
// CONFIGURATION
// ============================================================================

#include "config.hpp"
#include "logger.hpp"

static const std::string DEFAULT_CONFIG_PATH = "config/trade_feed_handler.json";

// ============================================================================
// SIGNAL HANDLING
// ============================================================================

// Global pointer for signal handler access
static TradeFeedHandler* g_handler = nullptr;

static void signalHandler(int signum) {
    const char* sigName = (signum == SIGINT) ? "SIGINT" : 
                          (signum == SIGTERM) ? "SIGTERM" : "UNKNOWN";
    spdlog::info("Received {} ({})", sigName, signum);
    
    if (g_handler) {
        g_handler->stop();
    }
}

// ============================================================================
// MAIN
// ============================================================================

int main(int argc, char* argv[]) {
    std::cout << "=== Binance Trade Feed Handler ===\n";
    
    // Determine config path (from argument or default)
    std::string configPath = DEFAULT_CONFIG_PATH;
    if (argc > 1) {
        configPath = argv[1];
    }
    
    // Load configuration
    FeedHandlerConfig config;
    if (!config.load(configPath)) {
        std::cerr << "Failed to load config, exiting\n";
        return 1;
    }
    
    if (config.symbols.empty()) {
        std::cerr << "No symbols configured, exiting\n";
        return 1;
    }
    
    // Initialize logger
    initLogger("Trade FH", config.logLevel, config.logFile);
    
    // Install signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    spdlog::info("Signal handlers installed (Ctrl+C to shutdown)");
    
    // Create and run handler
    TradeFeedHandler handler(config.symbols, config.tpHost, config.tpPort);
    g_handler = &handler;
    
    handler.run();
    
    g_handler = nullptr;
    spdlog::info("Exiting");
    shutdownLogger();
    return 0;
}
