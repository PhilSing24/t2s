/**
 * @file trade_feed_handler.cpp
 * @brief Implementation of TradeFeedHandler class
 */

#include "trade_feed_handler.hpp"
#include "socket_utils.hpp"
#include "k_object.hpp"
#include "json_reader.hpp"
#include "trade_row.hpp"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <utility>

// ============================================================================
// CONSTRUCTION / DESTRUCTION
// ============================================================================

TradeFeedHandler::TradeFeedHandler(const std::vector<std::string>& symbols,
                                   t2s::MarketConfig market,
                                   const std::string& tpHost,
                                   int tpPort)
    : symbols_(symbols)
    , cfg_(std::move(market))
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
    spdlog::info("Market: host={} port={} streamSuffix={} tpTable={}",
                 cfg_.host, cfg_.port, cfg_.streamSuffix, cfg_.tpTable);
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

namespace {

// Process-local counter for messages dropped due to malformed/unexpected JSON.
// Logged with rate limiting (first 10, then every 1000th) to avoid log spam
// on systematic schema breaks; not exposed in the health table since that
// would require a kdb-side schema change.
std::atomic<long long> g_parseFailures{0};

// Returns true on the first 10 increments, then every 1000th. Used to
// cap the log volume of repeat parse failures.
bool shouldLogParseFailure(long long count) noexcept {
    return count <= 10 || (count % 1000) == 0;
}

} // namespace

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

    // Parse JSON. rapidjson asserts on type-mismatch in GetX() calls and
    // std::stod throws on garbage, so we never touch the raw API directly -
    // all field access goes through JsonReader, which returns nullopt and
    // accumulates an error string on missing/wrong-type/parse failures.
    rapidjson::Document doc;
    doc.Parse(msg.c_str());
    if (doc.HasParseError()) {
        long long n = ++g_parseFailures;
        if (shouldLogParseFailure(n)) {
            spdlog::warn("trade JSON parse error [count={}]: {} at offset {} - msg: {}",
                n,
                rapidjson::GetParseError_En(doc.GetParseError()),
                doc.GetErrorOffset(),
                msg.substr(0, 200));
        }
        return;
    }

    t2s::JsonReader root(doc);
    t2s::JsonReader d = root.obj("data");

    // Common fields - same names in both spot @trade and futures @aggTrade.
    auto sym      = d.string("s");
    auto price    = d.priceString("p");
    auto qty      = d.priceString("q");
    auto buyerMkr = d.boolean("m");
    auto evtTime  = d.int64("E");
    auto trdTime  = d.int64("T");

    // Schema-specific id extraction. Spot has just `t` (tradeId); futures
    // aggTrade has `a` (aggTradeId) plus `f`/`l` (first/last constituent
    // trade ids). The sequence-validation contract is identical: the
    // primary id (tradeId or aggTradeId) must be monotonically increasing
    // per symbol; the validator log message is generic.
    long long primaryId  = 0;  // tradeId (spot) or aggTradeId (futures)
    long long firstAggId = 0;  // futures only; 0 for spot
    long long lastAggId  = 0;  // futures only; 0 for spot

    if (cfg_.schema == t2s::TradeSchema::SpotTrade) {
        auto t = d.int64("t");
        if (d.hasError()) {
            long long n = ++g_parseFailures;
            if (shouldLogParseFailure(n)) {
                spdlog::warn("trade schema error [count={}, schema=spot]: {} - msg: {}",
                    n, d.lastError(), msg.substr(0, 200));
            }
            return;
        }
        primaryId = *t;
    } else {  // FuturesAggTrade
        auto a = d.int64("a");
        auto f = d.int64("f");
        auto l = d.int64("l");
        if (d.hasError()) {
            long long n = ++g_parseFailures;
            if (shouldLogParseFailure(n)) {
                spdlog::warn("trade schema error [count={}, schema=futures]: {} - msg: {}",
                    n, d.lastError(), msg.substr(0, 200));
            }
            return;
        }
        primaryId  = *a;
        firstAggId = *f;
        lastAggId  = *l;
    }

    // All fields validated. string_view points into doc (alive for this
    // function's scope); copy to std::string for stable lifetime through
    // the kdb row construction below.
    std::string symStr(*sym);
    double priceV            = *price;
    double qtyV              = *qty;
    bool buyerIsMaker        = *buyerMkr;
    long long exchEventTimeMs = *evtTime;
    long long exchTradeTimeMs = *trdTime;

    // Validate sequence
    validateTradeId(symStr, primaryId);

    // End parse timer
    auto parseEnd = std::chrono::steady_clock::now();
    long long fhParseUs = std::chrono::duration_cast<std::chrono::microseconds>(
        parseEnd - parseStart).count();

    // Increment sequence number
    ++fhSeqNo_;

    // Build kdb+ row using the schema-appropriate helper. fhSendUs starts
    // as a placeholder 0; we patch the slot below after measuring send
    // latency. Slot index for fhSendUs is 10 (spot) or 12 (futures).
    t2s::KOwned row;
    int fhSendSlotIdx;
    if (cfg_.schema == t2s::TradeSchema::SpotTrade) {
        row = t2s::buildTradeRow(
            fhRecvTimeUtcNs, symStr, primaryId, priceV, qtyV, buyerIsMaker,
            exchEventTimeMs, exchTradeTimeMs, fhParseUs, /*fhSendUs=*/0LL, fhSeqNo_,
            KDB_EPOCH_OFFSET_NS);
        fhSendSlotIdx = 10;
    } else {  // FuturesAggTrade
        row = t2s::buildAggTradeRow(
            fhRecvTimeUtcNs, symStr, primaryId, firstAggId, lastAggId,
            priceV, qtyV, buyerIsMaker,
            exchEventTimeMs, exchTradeTimeMs, fhParseUs, /*fhSendUs=*/0LL, fhSeqNo_,
            KDB_EPOCH_OFFSET_NS);
        fhSendSlotIdx = 12;
    }

    // Capture send time and patch the placeholder. kK(...)[i] returns a
    // borrowed ref - we mutate it but do NOT release it; the parent list
    // owns it.
    auto sendEnd = std::chrono::steady_clock::now();
    long long fhSendUs = std::chrono::duration_cast<std::chrono::microseconds>(
        sendEnd - parseEnd).count();
    t2s::KBorrowed sendField(kK(row.get())[fhSendSlotIdx]);
    sendField.get()->j = fhSendUs;

    // Debug output (only shown at debug level)
    spdlog::debug("Trade: sym={} primaryId={} price={:.2f} qty={:.4f} fhParseUs={} fhSendUs={} fhSeqNo={}",
        symStr, primaryId, priceV, qtyV, fhParseUs, fhSendUs, fhSeqNo_);

    // Publish to TP. Async k() consumes its K args, so we release ownership
    // out of the wrapper. After this, `row` is empty - any attempt to reuse
    // it on the reconnect path below would just pass NULL (compile-time safe).
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
        if (connectToTP()) {
            // Build a fresh row for the resend - the original was consumed
            // by the failed k() above. (Pre-RAII this code reused the freed
            // row pointer, a use-after-free.) Schema branch the same way.
            t2s::KOwned row2;
            if (cfg_.schema == t2s::TradeSchema::SpotTrade) {
                row2 = t2s::buildTradeRow(
                    fhRecvTimeUtcNs, symStr, primaryId, priceV, qtyV, buyerIsMaker,
                    exchEventTimeMs, exchTradeTimeMs, fhParseUs, fhSendUs, fhSeqNo_,
                    KDB_EPOCH_OFFSET_NS);
            } else {  // FuturesAggTrade
                row2 = t2s::buildAggTradeRow(
                    fhRecvTimeUtcNs, symStr, primaryId, firstAggId, lastAggId,
                    priceV, qtyV, buyerIsMaker,
                    exchEventTimeMs, exchTradeTimeMs, fhParseUs, fhSendUs, fhSeqNo_,
                    KDB_EPOCH_OFFSET_NS);
            }
            k(-tpHandle_, (S)".u.upd", ks((S)cfg_.tpTable.c_str()), row2.release(), (K)0);
        }
    }
}

void TradeFeedHandler::runWebSocketLoop() {
    // Binance USDT-M futures requires a routed path prefix as of the
    // 2026-04-23 URL migration: streams on /market (which includes
    // @aggTrade) won't deliver data on unrouted connections - the
    // WebSocket handshake succeeds but no messages arrive. See
    //   https://developers.binance.com/docs/derivatives/usds-margined-futures
    //         /websocket-market-streams/Important-WebSocket-Change-Notice
    // Spot (stream.binance.com:9443) is unaffected - it uses a different
    // host and routing isn't required there.
    std::string pathPrefix = (cfg_.schema == t2s::TradeSchema::FuturesAggTrade)
                             ? "/market"
                             : "";
    std::string target = pathPrefix + t2s::buildStreamPath(symbols_, cfg_.streamSuffix);
    spdlog::info("Connecting to Binance: {}{}", cfg_.host, target);

    connState_ = "connecting";

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
    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), cfg_.host.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              net::error::get_ssl_category()),
            "Failed to set SNI hostname");
    }
    ws.next_layer().set_verify_callback(ssl::host_name_verification(cfg_.host));

    auto const results = resolver.resolve(cfg_.host, cfg_.port);
    net::connect(ws.next_layer().next_layer(), results.begin(), results.end());

    // Configure aggressive TCP keepalive so dead connections (e.g. after
    // host suspend or upstream LB drop) are detected within ~90 seconds
    // instead of relying on Linux kernel defaults (2 hours before first probe).
    t2s::applyKeepalive(ws.next_layer().next_layer());

    // TLS handshake
    ws.next_layer().handshake(ssl::stream_base::client);

    // WebSocket handshake
    ws.handshake(cfg_.host, target);

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

    // Handler name distinguishes spot vs futures FH in the shared
    // health_feed_handler table. Without this, both binaries publish
    // under "trade_fh" and operators can't tell which one is dead.
    const char* handlerName = (cfg_.schema == t2s::TradeSchema::FuturesAggTrade)
                              ? "trade_fh_fut"
                              : "trade_fh";

    // Build health row (10 fields)
    t2s::KOwned row(knk(10,
        ktj(-KP, toKdbTs(now)),                    // time
        ks((S)handlerName),                         // handler
        ktj(-KP, toKdbTs(startTime_)),             // startTimeUtc
        kj(uptimeSec),                              // uptimeSec
        kj(msgsReceived_),                          // msgsReceived
        kj(msgsPublished_),                         // msgsPublished
        ktj(-KP, toKdbTs(lastMsgTime_)),           // lastMsgTimeUtc
        ktj(-KP, toKdbTs(lastPubTime_)),           // lastPubTimeUtc
        ks((S)connState_.c_str()),                  // connState
        ki(static_cast<int>(symbols_.size()))       // symbolCount
    ));

    // Publish to TP (fire and forget)
    k(-tpHandle_, (S)".u.upd", ks((S)"health_feed_handler"), row.release(), (K)0);

    spdlog::debug("Health published: uptime={}s msgs={}/{} state={}",
        uptimeSec, msgsReceived_, msgsPublished_, connState_);
}
