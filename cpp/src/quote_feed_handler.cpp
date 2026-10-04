/**
 * @file quote_feed_handler.cpp
 * @brief Implementation of QuoteFeedHandler class (L5 version)
 * 
 * Uses OrderBookManager for efficient multi-symbol book management.
 * Publishes L5 quotes (22 price/qty fields) to kdb+.
 */

#include "quote_feed_handler.hpp"
#include "socket_utils.hpp"
#include "k_object.hpp"
#include "json_reader.hpp"
#include "fh_stats.hpp"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <algorithm>
#include <cctype>

// ============================================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================================

QuoteFeedHandler::QuoteFeedHandler(const std::vector<std::string>& symbols,
                                   const t2s::QuoteMarketConfig& market,
                                   const std::string& tpHost,
                                   int tpPort)
    : cfg_(market)
    , tpHost_(tpHost)
    , tpPort_(tpPort)
    , restClient_(market.restHost, market.restPort, market.restPath)
    , startTime_(std::chrono::system_clock::now())
{
    sessionId_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
        startTime_.time_since_epoch()).count();

    // Store lowercase (for WebSocket) and uppercase (for internal use)
    for (const auto& sym : symbols) {
        symbolsLower_.push_back(sym);
        
        std::string upper = sym;
        std::transform(upper.begin(), upper.end(), upper.begin(), ::toupper);
        symbolsUpper_.push_back(upper);
    }
    
    // Create book manager with uppercase symbols
    BookConfig bookCfg;
    bookCfg.snapshotLimit = static_cast<std::size_t>(cfg_.snapshotLimit);
    bookCfg.sync = cfg_.sync;
    bookMgr_ = std::make_unique<OrderBookManager>(symbolsUpper_, bookCfg);

    // Per-symbol "latest request id" tracking, used to discard stale
    // snapshot results when the symbol gets reset and re-requested.
    latestRequestId_.assign(symbolsUpper_.size(), 0);

    // Async snapshot worker. Starts a background thread that pulls from
    // an internal queue and calls restClient_.fetchSnapshot. Constructed
    // here so it's available immediately; thread is started in run().
    snapshotWorker_ = std::make_unique<t2s::SnapshotWorker<RestClient>>(restClient_, cfg_.snapshotLimit);

    // The scheduler keeps snapshot requests at a tenth of the exchange's
    // weight limit (see snapshot_scheduler.hpp); the numbers are per market.
    t2s::SnapshotSchedulerConfig schedCfg;
    schedCfg.weightPerRequest = cfg_.snapshotWeight;
    schedCfg.weightLimitPerMin = cfg_.weightLimitPerMin;
    snapshotScheduler_ = std::make_unique<t2s::SnapshotScheduler>(
        static_cast<int>(symbolsUpper_.size()), schedCfg);
    snapshotRequestedAtMs_.assign(symbolsUpper_.size(), 0);
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

    // Spin up the async snapshot worker thread. This must be running
    // before runWebSocketLoop() can enqueue requests.
    snapshotWorker_->start();
    spdlog::info("Snapshot worker thread started");

    // Connect to tickerplant and register this session
    if (!connectToTP(fhSeqNo_ + 1)) {
        if (!fatalError_.empty()) {
            spdlog::critical("TP rejected this handler: {} - exiting", fatalError_);
        } else {
            spdlog::warn("Shutdown before TP connection established");
        }
        snapshotWorker_->stop();
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
                ++ctrWsReconnects_;
                spdlog::info("Will reconnect...");
                if (!sleepWithBackoff(binanceReconnectAttempt_++)) {
                    break;
                }
            }
        }
    }
    
    // Cleanup
    logStats();
    spdlog::info("Cleaning up...");
    snapshotWorker_->stop();
    spdlog::info("Snapshot worker stopped");

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
    // e.g. /stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms (spot)
    //      /public/stream?streams=btcusdt@depth@100ms/...          (USD-M futures)
    return cfg_.wsPathPrefix + t2s::buildStreamPath(symbolsLower_, cfg_.streamSuffix);
}

int QuoteFeedHandler::registerSession(int h, long long nextFhSeqNo) {
    const long long width = 28LL;   // feed-handler columns of the quote schema
    K r = k(h, (S)".tp.registerSession",
            ks((S)cfg_.tpTable.c_str()), kj(sessionId_), kj(nextFhSeqNo), kj(width), (K)0);
    if (r == nullptr) {
        spdlog::error("TP connection lost during session registration");
        return 0;
    }
    if (r->t == -128) {
        fatalError_ = "registration rejected for " + cfg_.tpTable + ": " + r->s;
        spdlog::critical("TP REJECTED session registration for {} (sessionId={}, nextFhSeqNo={}, width={}): {}",
                         cfg_.tpTable, sessionId_, nextFhSeqNo, width, r->s);
        r0(r);
        return -1;
    }
    r0(r);
    spdlog::info("Session registered with TP: table={} sessionId={} nextFhSeqNo={} width={}",
                 cfg_.tpTable, sessionId_, nextFhSeqNo, width);
    return 1;
}

bool QuoteFeedHandler::connectToTP(long long nextFhSeqNo) {
    int attempt = 0;
    while (running_) {
        spdlog::info("Connecting to TP on {}:{}...", tpHost_, tpPort_);
        
        int h = khpu((S)tpHost_.c_str(), tpPort_, (S)"");
        
        if (h > 0) {
            int reg = registerSession(h, nextFhSeqNo);
            if (reg == 1) {
                tpHandle_ = h;
                spdlog::info("Connected to TP (handle {})", h);
                return true;
            }
            kclose(h);
            if (reg < 0) {
                running_ = false;   // fatal: exit the handler
                return false;
            }
        } else {
            spdlog::error("Failed to connect to TP");
        }
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
    spdlog::info("Connecting to Binance: {}:{}{}", cfg_.wsHost, cfg_.wsPort, target);
    
    connState_ = "connecting";
    
    // Reset all books on reconnect
    bookMgr_->resetAll();
    // Snapshots still in flight belong to the previous connection's update
    // id sequence: mark them stale so their results are discarded.
    for (std::size_t i = 0; i < latestRequestId_.size(); ++i) {
        if (snapshotRequestedAtMs_[i] != 0) {
            ++latestRequestId_[i];
            snapshotRequestedAtMs_[i] = 0;
        }
    }
    
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
    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), cfg_.wsHost.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              net::error::get_ssl_category()),
            "Failed to set SNI hostname");
    }
    ws.next_layer().set_verify_callback(ssl::host_name_verification(cfg_.wsHost));
    
    auto const results = resolver.resolve(cfg_.wsHost, cfg_.wsPort);
    net::connect(ws.next_layer().next_layer(), results.begin(), results.end());

    // Configure aggressive TCP keepalive so dead connections (e.g. after
    // host suspend or upstream LB drop) are detected within ~90 seconds
    // instead of relying on Linux kernel defaults (2 hours before first probe).
    t2s::applyKeepalive(ws.next_layer().next_layer());

    // TLS handshake
    ws.next_layer().handshake(ssl::stream_base::client);
    
    // WebSocket handshake
    ws.handshake(cfg_.wsHost, target);

    // Configure idle timeout: if no message arrives for 30s, ws.read()
    // throws, which the outer try/catch treats as a disconnect and
    // triggers reconnect. Combined with keep_alive_pings (Beast sends
    // ws ping frames during quiet periods), this catches hung connections
    // that pass TCP keepalive but stop delivering data.
    {
        auto timeout = websocket::stream_base::timeout::suggested(beast::role_type::client);
        timeout.idle_timeout = std::chrono::seconds(30);
        timeout.keep_alive_pings = true;
        ws.set_option(timeout);
    }

    spdlog::info("Connected to Binance ({} symbols)", symbolsLower_.size());
    connState_ = "connected";
    
    // Reset backoff
    binanceReconnectAttempt_ = 0;
    
    // Health publish timer
    auto lastHealthPub = std::chrono::steady_clock::now();
    auto lastStatsLog = lastHealthPub;
    
    // Message loop
    while (running_) {
        // Drain any snapshot results from the async worker and apply them.
        // Done at the top of the loop so the book is fresh before we read
        // the next delta. Cheap when nothing is pending (just a mutex grab
        // on an empty deque).
        applySnapshotResults();

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
        if (std::chrono::duration_cast<std::chrono::seconds>(now - lastStatsLog).count() >= STATS_INTERVAL_SEC) {
            logStats();
            lastStatsLog = now;
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

namespace {

// Process-local counter for messages dropped due to malformed/unexpected JSON.
// Logged with rate limiting (first 10, then every 1000th) to avoid log spam
// on systematic schema breaks.
std::atomic<long long> g_parseFailures{0};

bool shouldLogParseFailure(long long count) noexcept {
    return count <= 10 || (count % 1000) == 0;
}

} // namespace

void QuoteFeedHandler::processMessage(const std::string& msg, long long fhRecvTimeUtcNs) {
    rapidjson::Document doc;
    doc.Parse(msg.c_str());
    if (doc.HasParseError()) {
        long long n = ++g_parseFailures;
        if (shouldLogParseFailure(n)) {
            spdlog::warn("quote JSON parse error [count={}]: {} at offset {} - msg: {}",
                n,
                rapidjson::GetParseError_En(doc.GetParseError()),
                doc.GetErrorOffset(),
                msg.substr(0, 200));
        }
        return;
    }

    // Combined stream format: {"stream":"btcusdt@depth@100ms","data":{...}}
    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");
    auto symField = d.string("s");
    auto Uf       = d.int64("U");
    auto uf       = d.int64("u");
    auto Ef       = d.int64("E");
    const auto* bArr = d.array("b");
    const auto* aArr = d.array("a");
    // USD-M futures depth events also carry pu (the previous event's u,
    // needed by the futures sync rule) and T (transaction time). Both are
    // required there; a payload without them is a schema error.
    const bool futures = (cfg_.sync == t2s::DepthSync::Futures);
    std::optional<std::int64_t> puf, Tf;
    if (futures) {
        puf = d.int64("pu");
        Tf  = d.int64("T");
    }

    if (d.hasError()) {
        long long n = ++g_parseFailures;
        if (shouldLogParseFailure(n)) {
            spdlog::warn("quote schema error [count={}]: {} - msg: {}",
                n, d.lastError(), msg.substr(0, 200));
        }
        return;
    }

    // All required fields validated. Map to delta struct.
    std::string sym(*symField);
    int symIdx = bookMgr_->getSymbolIndex(sym);
    if (symIdx < 0) return;  // Unknown symbol (not a parse failure - configured subset)

    BufferedDelta delta;
    delta.firstUpdateId = *Uf;
    delta.finalUpdateId = *uf;
    delta.eventTimeMs   = *Ef;
    if (futures) {
        delta.prevFinalUpdateId = *puf;
        delta.transactTimeMs    = *Tf;
    }

    // Per-level parsing. Malformed levels are silently skipped (per-level
    // resilience) rather than failing the whole message - one bad price
    // tick shouldn't invalidate the rest of the diff.
    for (const auto& lvl : bArr->GetArray()) {
        if (auto p = t2s::parseLevelPair(lvl)) {
            PriceLevel pl;
            pl.price = p->first;
            pl.qty   = p->second;
            delta.bids.push_back(pl);
        }
    }
    for (const auto& lvl : aArr->GetArray()) {
        if (auto p = t2s::parseLevelPair(lvl)) {
            PriceLevel pl;
            pl.price = p->first;
            pl.qty   = p->second;
            delta.asks.push_back(pl);
        }
    }

    // Handle delta based on book state
    handleDelta(symIdx, delta, fhRecvTimeUtcNs);
}

void QuoteFeedHandler::handleDelta(int symIdx, const BufferedDelta& delta, long long fhRecvTimeUtcNs) {
    BookState state = bookMgr_->getState(symIdx);

    switch (state) {
        case BookState::INIT:
            // Buffer the delta (capped) until the snapshot arrives
            if (!bookMgr_->bufferDelta(symIdx, delta)) {
                long long n = bookMgr_->bufferOverflows();
                if (n == 1 || n % 1000 == 0) {
                    spdlog::warn("{} delta buffer full ({} deltas): oldest dropped [total dropped={}]",
                                 bookMgr_->getSymbol(symIdx), MAX_DELTA_BUFFER_SIZE, n);
                }
            }

            // Ask the scheduler first: after a failure it makes us wait
            // (exponential backoff), and it enforces the weight budget.
            // Deltas arrive every 100 ms, so this is retried soon enough.
            if (bookMgr_->needsSnapshot(symIdx) &&
                snapshotScheduler_->tryAcquire(symIdx, steadyNowMs())) {
                requestSnapshot(symIdx);
            }
            break;

        case BookState::SYNCING:
        case BookState::VALID:
            if (!bookMgr_->applyDelta(symIdx, delta)) {
                // Sequence gap (or a snapshot too old to bridge): the book
                // can no longer be trusted. Publish one invalid row so the
                // hole is visible downstream, then rebuild from a snapshot.
                if (state == BookState::VALID) {
                    ++ctrSequenceGaps_;
                    spdlog::warn("{} sequence gap detected (U={} u={})", bookMgr_->getSymbol(symIdx),
                                 delta.firstUpdateId, delta.finalUpdateId);
                } else {
                    spdlog::warn("{} snapshot does not bridge to the stream (U={} u={})",
                                 bookMgr_->getSymbol(symIdx), delta.firstUpdateId, delta.finalUpdateId);
                    snapshotScheduler_->onFailure(symIdx, steadyNowMs());
                }
                ++ctrResyncs_;
                publishInvalid(symIdx, fhRecvTimeUtcNs);
                bookMgr_->reset(symIdx);
                break;
            }
            if (!bookMgr_->isValid(symIdx)) break;       // still waiting for the bridging delta

            if (state == BookState::SYNCING) {
                snapshotScheduler_->onSynced(symIdx);
                spdlog::info("{} is now VALID", bookMgr_->getSymbol(symIdx));
            }
            maybePublish(symIdx, fhRecvTimeUtcNs);

            // Known depth running low (the market moved toward the snapshot
            // horizon): fetch a fresh snapshot in the background. The book
            // stays VALID and keeps publishing meanwhile.
            if (bookMgr_->wantsRefresh(symIdx) &&
                snapshotScheduler_->tryAcquire(symIdx, steadyNowMs())) {
                bookMgr_->beginRefresh(symIdx);
                requestSnapshot(symIdx);
            }
            break;

        case BookState::INVALID:
            bookMgr_->reset(symIdx);
            break;
    }
}

void QuoteFeedHandler::requestSnapshot(int symIdx) {
    const std::string& sym = bookMgr_->getSymbol(symIdx);

    // Mark requested at the book level so the existing INIT-state guard
    // (needsSnapshot returns false while one is in flight) prevents us
    // from spamming requests for the same symbol.
    bookMgr_->setSnapshotRequested(symIdx, true);
    snapshotRequestedAtMs_[symIdx] = steadyNowMs();

    // Enqueue and remember the request id. When the result eventually
    // arrives, applySnapshotResults compares its id against this one and
    // discards anything older - this happens if we reset and re-requested
    // while a previous fetch was still in flight.
    std::uint64_t id = snapshotWorker_->enqueueRequest(symIdx, sym);
    latestRequestId_[symIdx] = id;

    spdlog::info("Snapshot enqueued for {} (id {}, queue depth: {})",
                 sym, id, snapshotWorker_->pendingRequests());
}

std::int64_t QuoteFeedHandler::steadyNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

void QuoteFeedHandler::applySnapshotResults() {
    const std::int64_t nowMs = steadyNowMs();

    // A request whose result never came back (dropped from the worker's
    // queue, or a REST call stuck in connect) must not block the symbol.
    for (std::size_t i = 0; i < snapshotRequestedAtMs_.size(); ++i) {
        if (snapshotRequestedAtMs_[i] != 0 &&
            nowMs - snapshotRequestedAtMs_[i] > SNAPSHOT_TIMEOUT_MS) {
            int symIdx = static_cast<int>(i);
            spdlog::error("Snapshot for {} timed out after {} ms",
                          bookMgr_->getSymbol(symIdx), SNAPSHOT_TIMEOUT_MS);
            ++latestRequestId_[i];              // a late result is now stale
            snapshotRequestedAtMs_[i] = 0;
            bookMgr_->cancelRefresh(symIdx);
            bookMgr_->setSnapshotRequested(symIdx, false);
            snapshotScheduler_->onFailure(symIdx, nowMs);
            ++ctrSnapshotFailures_;
        }
    }

    auto results = snapshotWorker_->drainResults();
    for (auto& r : results) {
        const std::string& sym = r.sym;
        int symIdx = r.symIdx;

        // Discard stale results - a newer request was submitted for this symbol
        if (r.requestId < latestRequestId_[symIdx]) {
            spdlog::debug("Discarding stale snapshot result for {} (id {} < {})",
                          sym, r.requestId, latestRequestId_[symIdx]);
            continue;
        }
        snapshotRequestedAtMs_[symIdx] = 0;

        if (!r.data.success) {
            // Initial sync: stay in INIT and keep buffering. Refresh: the
            // live book carries on. Either way the scheduler decides when
            // the next attempt may go out; nothing is re-requested here.
            ++ctrSnapshotFailures_;
            snapshotScheduler_->onFailure(symIdx, nowMs, r.data.httpStatus,
                                          r.data.retryAfterSec, r.data.usedWeight1m);
            spdlog::error("Snapshot fetch failed for {}: {} (failure #{} for this symbol, next attempt in {} ms)",
                          sym, r.data.error, snapshotScheduler_->consecutiveFailures(symIdx),
                          snapshotScheduler_->nextAllowedMs(symIdx) - nowMs);
            if (r.data.httpStatus == 429 || r.data.httpStatus == 418) {
                spdlog::critical("Binance rate limit hit (HTTP {}): all snapshot requests paused for {} ms",
                                 r.data.httpStatus, snapshotScheduler_->pausedUntilMs() - nowMs);
            }
            bookMgr_->cancelRefresh(symIdx);
            bookMgr_->setSnapshotRequested(symIdx, false);
            continue;
        }
        snapshotScheduler_->onFetchOk(symIdx, nowMs, r.data.usedWeight1m);

        SnapshotOutcome outcome = bookMgr_->onSnapshot(symIdx, r.data.lastUpdateId,
                                                       r.data.bids, r.data.asks);
        switch (outcome) {
            case SnapshotOutcome::SYNCED:
                snapshotScheduler_->onSynced(symIdx);
                spdlog::info("{} is now VALID (snapshot lastUpdateId={}, {} bids, {} asks)",
                             sym, r.data.lastUpdateId, r.data.bids.size(), r.data.asks.size());
                break;
            case SnapshotOutcome::AWAITING_BRIDGE:
                spdlog::debug("{} snapshot applied (lastUpdateId={}), waiting for the bridging delta",
                              sym, r.data.lastUpdateId);
                break;
            case SnapshotOutcome::SYNC_FAILED:
                // Book is INVALID; the next delta resets it and a new
                // snapshot is requested under the scheduler's backoff.
                ++ctrResyncs_;
                snapshotScheduler_->onFailure(symIdx, nowMs);
                spdlog::warn("{} snapshot (lastUpdateId={}) is older than the buffered deltas - resync",
                             sym, r.data.lastUpdateId);
                break;
            case SnapshotOutcome::REFRESHED:
                snapshotScheduler_->onSynced(symIdx);
                spdlog::info("{} depth refreshed in the background (lastUpdateId={}, known levels {}/{})",
                             sym, r.data.lastUpdateId,
                             bookMgr_->knownLevels(symIdx, true), bookMgr_->knownLevels(symIdx, false));
                break;
            case SnapshotOutcome::REFRESH_FAILED:
                snapshotScheduler_->onFailure(symIdx, nowMs);
                spdlog::warn("{} depth refresh could not bridge (lastUpdateId={}); live book kept, will retry",
                             sym, r.data.lastUpdateId);
                break;
            case SnapshotOutcome::IGNORED:
                spdlog::debug("{} snapshot result ignored (book state changed meanwhile)", sym);
                break;
        }
    }
}

void QuoteFeedHandler::maybePublish(int symIdx, long long fhRecvTimeUtcNs) {
    // Build a candidate quote with a tentative seq, then only commit
    // (increment fhSeqNo, publish) if shouldPublish accepts it.
    // Bumping fhSeqNo unconditionally caused TP to see "gaps" whenever
    // shouldPublish filtered an unchanged L5.
    L5Quote quote = bookMgr_->getL5(symIdx, fhRecvTimeUtcNs, fhSeqNo_ + 1);

    if (bookMgr_->shouldPublish(symIdx, quote)) {
        ++fhSeqNo_;
        quote.fhSeqNo = fhSeqNo_;
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
    
    t2s::KOwned row(knk(28,
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
    ));
    
    // Capture send time and patch the placeholder via borrowed view.
    auto sendEnd = std::chrono::steady_clock::now();
    long long fhSendUs = std::chrono::duration_cast<std::chrono::microseconds>(
        sendEnd - sendStart).count();
    t2s::KBorrowed sendField(kK(row.get())[26]);
    sendField.get()->j = fhSendUs;
    
    K result = k(-tpHandle_, (S)".u.upd", ks((S)cfg_.tpTable.c_str()), row.release(), (K)0);
    
    // Update health: message published
    lastPubTime_ = std::chrono::system_clock::now();
    ++msgsPublished_;
    
    // Check if TP connection died
    if (result == nullptr) {
        spdlog::error("TP connection lost, reconnecting...");
        connState_ = "reconnecting";
        kclose(tpHandle_);
        tpHandle_ = -1;
        // Re-register announcing the row we are about to resend.
        if (connectToTP(quote.fhSeqNo)) {
            // Build a fresh row for the resend - the original was consumed
            // by the failed k() above.
            t2s::KOwned row2(knk(28,
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
            ));
            k(-tpHandle_, (S)".u.upd", ks((S)cfg_.tpTable.c_str()), row2.release(), (K)0);
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
    t2s::KOwned row(knk(10,
        ktj(-KP, toKdbTs(now)),                    // time
        ks((S)cfg_.healthName.c_str()),                        // handler
        ktj(-KP, toKdbTs(startTime_)),             // startTimeUtc
        kj(uptimeSec),                              // uptimeSec
        kj(msgsReceived_),                          // msgsReceived
        kj(msgsPublished_),                         // msgsPublished
        ktj(-KP, toKdbTs(lastMsgTime_)),           // lastMsgTimeUtc
        ktj(-KP, toKdbTs(lastPubTime_)),           // lastPubTimeUtc
        ks((S)connState_.c_str()),                  // connState
        ki(static_cast<int>(symbolsLower_.size()))  // symbolCount
    ));
    
    // Publish to TP (fire and forget)
    k(-tpHandle_, (S)".u.upd", ks((S)"health_feed_handler"), row.release(), (K)0);

    // Book-level counters, shown per table by TP's .health[] and status.sh
    t2s::sendFhStats(tpHandle_, cfg_.tpTable, {
        {"msgsReceived",     msgsReceived_},
        {"rowsPublished",    msgsPublished_},
        {"wsReconnects",     ctrWsReconnects_},
        {"bookGaps",         ctrSequenceGaps_},
        {"resyncs",          ctrResyncs_},
        {"snapshotRequests", snapshotScheduler_->requests()},
        {"snapshotFailures", ctrSnapshotFailures_},
        {"rateLimitPauses",  snapshotScheduler_->rateLimitPauses()},
        {"bufferOverflows",  bookMgr_->bufferOverflows()},
        {"depthRefreshes",   bookMgr_->depthRefreshes()},
        {"refreshFailures",  bookMgr_->refreshFailures()},
        {"depthExhausted",   bookMgr_->depthExhaustedEvents()},
    });
    
    spdlog::debug("Health published: uptime={}s msgs={}/{} state={}", 
        uptimeSec, msgsReceived_, msgsPublished_, connState_);
}

void QuoteFeedHandler::checkPublishTimeouts(long long fhRecvTimeUtcNs) {
    // Get symbols that need timeout publish
    std::vector<int> needsPublish = bookMgr_->getTimeoutPublishNeeded();
    
    for (int symIdx : needsPublish) {
        // Heartbeats are for valid quotes only. A book whose known depth
        // is exhausted has already published its one invalid row.
        L5Quote quote = bookMgr_->getL5(symIdx, fhRecvTimeUtcNs, fhSeqNo_ + 1);
        if (!quote.isValid) continue;
        ++fhSeqNo_;
        publishL5(quote);
        bookMgr_->recordPublish(symIdx, quote);
    }
}

void QuoteFeedHandler::logStats() const {
    spdlog::info("STATS msgs={} published={} wsReconnects={} gaps={} resyncs={} snapshotRequests={} snapshotFailures={} "
                 "rateLimitPauses={} bufferOverflows={} depthRefreshes={} refreshFailures={} depthExhausted={}",
                 msgsReceived_, msgsPublished_, ctrWsReconnects_, ctrSequenceGaps_, ctrResyncs_,
                 snapshotScheduler_->requests(), ctrSnapshotFailures_,
                 snapshotScheduler_->rateLimitPauses(), bookMgr_->bufferOverflows(),
                 bookMgr_->depthRefreshes(), bookMgr_->refreshFailures(),
                 bookMgr_->depthExhaustedEvents());
}
