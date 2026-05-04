/**
 * @file quote_feed_handler.cpp
 * @brief Implementation of QuoteFeedHandler class (L5 version)
 * 
 * Uses OrderBookManager for efficient multi-symbol book management.
 * Publishes L5 quotes (22 price/qty fields) to kdb+.
 */

#include "quote_feed_handler.hpp"

#include <rapidjson/document.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <thread>
#include <algorithm>
#include <cctype>

// ============================================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================================

QuoteFeedHandler::QuoteFeedHandler(const std::vector<std::string>& symbols,
                                   const std::string& tpHost,
                                   int tpPort)
    : tpHost_(tpHost)
    , tpPort_(tpPort)
    , startTime_(std::chrono::system_clock::now())
{
    // Store lowercase (for WebSocket) and uppercase (for internal use)
    for (const auto& sym : symbols) {
        symbolsLower_.push_back(sym);
        
        std::string upper = sym;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        symbolsUpper_.push_back(upper);
    }
    
    // Create book manager with uppercase symbols
    bookMgr_ = std::make_unique<OrderBookManager>(symbolsUpper_);
}

QuoteFeedHandler::~QuoteFeedHandler() {
    if (tpHandle_ > 0) {
        kclose(tpHandle_);
        spdlog::debug("TP connection closed in destructor");
    }
}

// ============================================================================
// PUBLIC INTERFACE
// ============================================================================

void QuoteFeedHandler::run() {
    spdlog::info("Starting L5 Quote Feed Handler...");
    spdlog::info("Symbols: {}", fmt::join(symbolsLower_, " "));
    
    // Connect to tickerplant
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
                    break;
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

void QuoteFeedHandler::stop() {
    spdlog::info("Stop requested");
    running_ = false;
}

// ============================================================================
// CONNECTION MANAGEMENT
// ============================================================================

std::string QuoteFeedHandler::buildDepthStreamPath() const {
    // Use @depth@100ms - updates pushed every 100ms (10/sec per symbol)
    std::string path = "/stream?streams=";
    for (size_t i = 0; i < symbolsLower_.size(); ++i) {
        if (i > 0) path += "/";
        path += symbolsLower_[i] + "@depth@100ms";
    }
    return path;
}

bool QuoteFeedHandler::connectToTP() {
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
            return false;
        }
    }
    return false;
}

bool QuoteFeedHandler::sleepWithBackoff(int attempt) {
    int delay = INITIAL_BACKOFF_MS;
    for (int i = 0; i < attempt && delay < MAX_BACKOFF_MS; ++i) {
        delay *= BACKOFF_MULTIPLIER;
    }
    delay = std::min(delay, MAX_BACKOFF_MS);
    
    spdlog::info("Waiting {}ms before reconnect...", delay);
    
    const int checkIntervalMs = 100;
    int slept = 0;
    while (slept < delay && running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(checkIntervalMs));
        slept += checkIntervalMs;
    }
    
    return running_;
}

// ============================================================================
// WEBSOCKET LOOP
// ============================================================================

void QuoteFeedHandler::runWebSocketLoop() {
    std::string target = buildDepthStreamPath();
    spdlog::info("Connecting to Binance: {}{}", BINANCE_HOST, target);
    
    connState_ = "connecting";
    
    // Reset all books on reconnect
    bookMgr_->resetAll();
    
    // Initialize ASIO and SSL
    net::io_context ioc;
    ssl::context ctx{ssl::context::tlsv12_client};
    ctx.set_default_verify_paths();
    ctx.set_verify_mode(ssl::verify_peer);
    
    // Resolve and connect
    tcp::resolver resolver{ioc};
    websocket::stream<beast::ssl_stream<tcp::socket>> ws{ioc, ctx};
    
    // Set SNI hostname so Binance serves the right cert and so we can
    // verify the cert's CN/SAN matches what we asked to connect to.
    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), BINANCE_HOST)) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              net::error::get_ssl_category()),
            "Failed to set SNI hostname");
    }
    ws.next_layer().set_verify_callback(ssl::host_name_verification(BINANCE_HOST));
    
    auto const results = resolver.resolve(BINANCE_HOST, BINANCE_PORT);
    net::connect(ws.next_layer().next_layer(), results.begin(), results.end());
    
    // TLS handshake
    ws.next_layer().handshake(ssl::stream_base::client);
    
    // WebSocket handshake
    ws.handshake(BINANCE_HOST, target);
    
    spdlog::info("Connected to Binance ({} symbols)", symbolsLower_.size());
    connState_ = "connected";
    
    // Reset backoff
    binanceReconnectAttempt_ = 0;
    
    // Health publish timer
    auto lastHealthPub = std::chrono::steady_clock::now();
    
    // Message loop
    while (running_) {
        beast::flat_buffer buffer;
        ws.read(buffer);
        
        if (!running_) break;
        
        auto recvTime = std::chrono::system_clock::now();
        long long fhRecvTimeUtcNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
            recvTime.time_since_epoch()).count();
        
        // Update health: message received
        lastMsgTime_ = recvTime;
        ++msgsReceived_;
        
        // Start monotonic timer for parse latency
        auto parseStart = std::chrono::steady_clock::now();
        
        std::string msg = beast::buffers_to_string(buffer.data());
        processMessage(msg, fhRecvTimeUtcNs);
        
        // End parse timer (parse + order book update)
        auto parseEnd = std::chrono::steady_clock::now();
        lastParseUs_ = std::chrono::duration_cast<std::chrono::microseconds>(
            parseEnd - parseStart).count();
        
        // Check publish timeouts
        checkPublishTimeouts(fhRecvTimeUtcNs);
        
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

// ============================================================================
// MESSAGE PROCESSING
// ============================================================================

void QuoteFeedHandler::processMessage(const std::string& msg, long long fhRecvTimeUtcNs) {
    rapidjson::Document doc;
    doc.Parse(msg.c_str());
    if (!doc.IsObject()) return;
    
    // Combined stream format: {"stream":"btcusdt@depth@100ms","data":{...}}
    if (!doc.HasMember("data")) return;
    const auto& d = doc["data"];
    if (!d.IsObject()) return;
    
    // Extract symbol (uppercase)
    if (!d.HasMember("s")) return;
    std::string sym = d["s"].GetString();
    
    int symIdx = bookMgr_->getSymbolIndex(sym);
    if (symIdx < 0) return;  // Unknown symbol
    
    // Parse delta fields
    // U = first update ID, u = final update ID, E = event time
    if (!d.HasMember("U") || !d.HasMember("u")) return;
    
    BufferedDelta delta;
    delta.firstUpdateId = d["U"].GetInt64();
    delta.finalUpdateId = d["u"].GetInt64();
    delta.eventTimeMs = d.HasMember("E") ? d["E"].GetInt64() : 0;
    
    // Parse bid updates
    if (d.HasMember("b") && d["b"].IsArray()) {
        for (const auto& lvl : d["b"].GetArray()) {
            if (lvl.IsArray() && lvl.Size() >= 2) {
                PriceLevel pl;
                pl.price = std::stod(lvl[0].GetString());
                pl.qty = std::stod(lvl[1].GetString());
                delta.bids.push_back(pl);
            }
        }
    }
    
    // Parse ask updates
    if (d.HasMember("a") && d["a"].IsArray()) {
        for (const auto& lvl : d["a"].GetArray()) {
            if (lvl.IsArray() && lvl.Size() >= 2) {
                PriceLevel pl;
                pl.price = std::stod(lvl[0].GetString());
                pl.qty = std::stod(lvl[1].GetString());
                delta.asks.push_back(pl);
            }
        }
    }
    
    // Handle delta based on book state
    handleDelta(symIdx, delta, fhRecvTimeUtcNs);
}

void QuoteFeedHandler::handleDelta(int symIdx, const BufferedDelta& delta, long long fhRecvTimeUtcNs) {
    BookState state = bookMgr_->getState(symIdx);
    
    switch (state) {
        case BookState::INIT:
            // Buffer delta and request snapshot
            bookMgr_->getDeltaBuffer(symIdx).push_back(delta);
            
            if (bookMgr_->needsSnapshot(symIdx)) {
                requestSnapshot(symIdx);
            }
            break;
            
        case BookState::SYNCING:
            // Apply delta (may transition to VALID)
            if (!bookMgr_->applyDelta(symIdx, delta.firstUpdateId, delta.finalUpdateId,
                                      delta.bids, delta.asks, delta.eventTimeMs)) {
                spdlog::warn("{} failed to apply delta in SYNCING state", 
                    bookMgr_->getSymbol(symIdx));
                publishInvalid(symIdx, fhRecvTimeUtcNs);
                bookMgr_->reset(symIdx);
            } else {
                if (bookMgr_->isValid(symIdx)) {
                    maybePublish(symIdx, fhRecvTimeUtcNs);
                }
            }
            break;
            
        case BookState::VALID:
            // Apply delta directly
            if (!bookMgr_->applyDelta(symIdx, delta.firstUpdateId, delta.finalUpdateId,
                                      delta.bids, delta.asks, delta.eventTimeMs)) {
                spdlog::warn("{} sequence gap detected", bookMgr_->getSymbol(symIdx));
                publishInvalid(symIdx, fhRecvTimeUtcNs);
                bookMgr_->reset(symIdx);
            } else {
                maybePublish(symIdx, fhRecvTimeUtcNs);
            }
            break;
            
        case BookState::INVALID:
            // Reset and start over
            bookMgr_->reset(symIdx);
            break;
    }
}

// ============================================================================
// SNAPSHOT HANDLING
// ============================================================================

void QuoteFeedHandler::requestSnapshot(int symIdx) {
    const std::string& sym = bookMgr_->getSymbol(symIdx);
    spdlog::info("Requesting snapshot for {}", sym);
    
    bookMgr_->setSnapshotRequested(symIdx, true);
    
    // Fetch snapshot (blocking)
    SnapshotData snapshot = restClient_.fetchSnapshot(sym, SNAPSHOT_DEPTH);
    
    if (!snapshot.success) {
        spdlog::error("Snapshot failed for {}: {}", sym, snapshot.error);
        bookMgr_->invalidate(symIdx, "Snapshot fetch failed");
        return;
    }
    
    // Apply snapshot
    bookMgr_->applySnapshot(symIdx, snapshot.lastUpdateId, snapshot.bids, snapshot.asks);
    
    spdlog::debug("{} snapshot applied, lastUpdateId={}", sym, snapshot.lastUpdateId);
    
    // Apply buffered deltas
    auto& buffer = bookMgr_->getDeltaBuffer(symIdx);
    spdlog::debug("Applying {} buffered deltas for {}", buffer.size(), sym);
    
    while (!buffer.empty()) {
        const auto& delta = buffer.front();
        if (!bookMgr_->applyDelta(symIdx, delta.firstUpdateId, delta.finalUpdateId,
                                  delta.bids, delta.asks, delta.eventTimeMs)) {
            spdlog::warn("{} failed during buffered delta replay", sym);
            break;
        }
        buffer.pop_front();
    }
    
    // Clear remaining buffer
    buffer.clear();
    
    if (bookMgr_->isValid(symIdx)) {
        spdlog::info("{} is now VALID", sym);
    }
}

// ============================================================================
// PUBLISHING
// ============================================================================

void QuoteFeedHandler::maybePublish(int symIdx, long long fhRecvTimeUtcNs) {
    ++fhSeqNo_;
    L5Quote quote = bookMgr_->getL5(symIdx, fhRecvTimeUtcNs, fhSeqNo_);
    
    if (bookMgr_->shouldPublish(symIdx, quote)) {
        publishL5(quote);
        bookMgr_->recordPublish(symIdx, quote);
    }
}

void QuoteFeedHandler::publishInvalid(int symIdx, long long fhRecvTimeUtcNs) {
    ++fhSeqNo_;
    L5Quote quote;
    quote.sym = bookMgr_->getSymbol(symIdx);
    quote.isValid = false;
    quote.fhRecvTimeUtcNs = fhRecvTimeUtcNs;
    quote.fhSeqNo = fhSeqNo_;
    // All price/qty fields default to 0.0
    
    publishL5(quote);
    bookMgr_->recordPublish(symIdx, quote);
    
    spdlog::warn("Published INVALID for {}", quote.sym);
}

void QuoteFeedHandler::publishL5(const L5Quote& quote) {
    // Start send timer
    auto sendStart = std::chrono::steady_clock::now();
    
    // Build kdb+ row matching quote_binance L5 schema
    // FH sends 28 fields, TP adds tpRecvTimeUtcNs (29th)
    // Schema: time, sym, bidPrice1..5, bidQty1..5, askPrice1..5, askQty1..5, 
    //         isValid, exchEventTimeMs, fhRecvTimeUtcNs, fhParseUs, fhSendUs, fhSeqNo
    
    K row = knk(28,
        // time, sym
        ktj(-KP, quote.fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS),
        ks((S)quote.sym.c_str()),
        // Bid prices (5)
        kf(quote.bidPrice1),
        kf(quote.bidPrice2),
        kf(quote.bidPrice3),
        kf(quote.bidPrice4),
        kf(quote.bidPrice5),
        // Bid quantities (5)
        kf(quote.bidQty1),
        kf(quote.bidQty2),
        kf(quote.bidQty3),
        kf(quote.bidQty4),
        kf(quote.bidQty5),
        // Ask prices (5)
        kf(quote.askPrice1),
        kf(quote.askPrice2),
        kf(quote.askPrice3),
        kf(quote.askPrice4),
        kf(quote.askPrice5),
        // Ask quantities (5)
        kf(quote.askQty1),
        kf(quote.askQty2),
        kf(quote.askQty3),
        kf(quote.askQty4),
        kf(quote.askQty5),
        // Metadata
        kb(quote.isValid),
        kj(quote.exchEventTimeMs),
        kj(quote.fhRecvTimeUtcNs),
        kj(lastParseUs_),                              // fhParseUs
        kj(0LL),                                       // fhSendUs placeholder
        kj(quote.fhSeqNo)
    );
    
    // Capture send time
    auto sendEnd = std::chrono::steady_clock::now();
    long long fhSendUs = std::chrono::duration_cast<std::chrono::microseconds>(
        sendEnd - sendStart).count();
    kK(row)[26]->j = fhSendUs;  // Update fhSendUs field (index 26)
    
    K result = k(-tpHandle_, (S)".u.upd", ks((S)"quote_binance"), row, (K)0);
    
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
            K row2 = knk(28,
                ktj(-KP, quote.fhRecvTimeUtcNs - KDB_EPOCH_OFFSET_NS),
                ks((S)quote.sym.c_str()),
                kf(quote.bidPrice1), kf(quote.bidPrice2), kf(quote.bidPrice3),
                kf(quote.bidPrice4), kf(quote.bidPrice5),
                kf(quote.bidQty1), kf(quote.bidQty2), kf(quote.bidQty3),
                kf(quote.bidQty4), kf(quote.bidQty5),
                kf(quote.askPrice1), kf(quote.askPrice2), kf(quote.askPrice3),
                kf(quote.askPrice4), kf(quote.askPrice5),
                kf(quote.askQty1), kf(quote.askQty2), kf(quote.askQty3),
                kf(quote.askQty4), kf(quote.askQty5),
                kb(quote.isValid),
                kj(quote.exchEventTimeMs),
                kj(quote.fhRecvTimeUtcNs),
                kj(lastParseUs_),
                kj(fhSendUs),
                kj(quote.fhSeqNo)
            );
            k(-tpHandle_, (S)".u.upd", ks((S)"quote_binance"), row2, (K)0);
        }
    }
}

void QuoteFeedHandler::publishHealth() {
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
        ks((S)"quote_fh"),                          // handler
        ktj(-KP, toKdbTs(startTime_)),             // startTimeUtc
        kj(uptimeSec),                              // uptimeSec
        kj(msgsReceived_),                          // msgsReceived
        kj(msgsPublished_),                         // msgsPublished
        ktj(-KP, toKdbTs(lastMsgTime_)),           // lastMsgTimeUtc
        ktj(-KP, toKdbTs(lastPubTime_)),           // lastPubTimeUtc
        ks((S)connState_.c_str()),                  // connState
        ki(static_cast<int>(symbolsLower_.size()))  // symbolCount
    );
    
    // Publish to TP (fire and forget)
    k(-tpHandle_, (S)".u.upd", ks((S)"health_feed_handler"), row, (K)0);
    
    spdlog::debug("Health published: uptime={}s msgs={}/{} state={}", 
        uptimeSec, msgsReceived_, msgsPublished_, connState_);
}

void QuoteFeedHandler::checkPublishTimeouts(long long fhRecvTimeUtcNs) {
    // Get symbols that need timeout publish
    std::vector<int> needsPublish = bookMgr_->getTimeoutPublishNeeded();
    
    for (int symIdx : needsPublish) {
        ++fhSeqNo_;
        L5Quote quote = bookMgr_->getL5(symIdx, fhRecvTimeUtcNs, fhSeqNo_);
        publishL5(quote);
        bookMgr_->recordPublish(symIdx, quote);
    }
}
