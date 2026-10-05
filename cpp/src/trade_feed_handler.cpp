/**
 * @file trade_feed_handler.cpp
 * @brief Implementation of TradeFeedHandler class
 */

#include "trade_feed_handler.hpp"
#include <limits>
#include <cstdlib>
#include "fh_stats.hpp"
#include "socket_utils.hpp"
#include "k_object.hpp"
#include "json_reader.hpp"
#include "trade_row.hpp"
#include "row_clock_log.hpp"

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
    // cfg_, not `market`: the parameter was moved into cfg_ two lines up
    , backfillHttp_(cfg_.backfillRestHost, cfg_.backfillRestPort, cfg_.backfillRestPath)
    , startTime_(std::chrono::system_clock::now())
    , rowClock_(cfg_.clockLagMs)
{
    sessionId_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
        startTime_.time_since_epoch()).count();

    t2s::TpPublisherConfig tpCfg;
    tpCfg.host = tpHost_;
    tpCfg.port = tpPort_;
    tpCfg.table = cfg_.tpTable;
    tpCfg.width = (cfg_.schema == t2s::TradeSchema::SpotTrade)
                  ? t2s::TRADE_ROW_WIDTH : t2s::AGG_TRADE_ROW_WIDTH;
    tpCfg.sessionId = sessionId_;
    tp_ = std::make_unique<t2s::TpPublisher>(tpCfg, running_);

    // Backfill of trade-id gaps (trade_backfill.hpp). Rate limits: a tenth
    // of the exchange's weight limit, like the quote handlers' snapshots.
    t2s::TradeBackfillConfig bfCfg;
    bfCfg.enabled = cfg_.backfillEnabled;
    bfCfg.maxGapIds = cfg_.backfillMaxGapIds;
    bfCfg.sched.weightPerRequest = cfg_.backfillWeight;
    bfCfg.sched.weightLimitPerMin = cfg_.backfillWeightLimit;
    backfillFetcher_ = std::make_unique<BackfillFetcher>(backfillHttp_, cfg_.backfillRestPath, cfg_.schema);
    backfill_ = std::make_unique<t2s::TradeBackfill<BackfillFetcher>>(*backfillFetcher_, bfCfg);
}

TradeFeedHandler::~TradeFeedHandler() {
    tp_->close();
}

// ============================================================================
// PUBLIC INTERFACE
// ============================================================================

void TradeFeedHandler::run() {
    spdlog::info("Starting...");
    spdlog::info("Market: host={} port={} streamSuffix={} tpTable={}",
                 cfg_.host, cfg_.port, cfg_.streamSuffix, cfg_.tpTable);
    spdlog::info("Symbols: {}", fmt::join(symbols_, " "));

    // Connect to tickerplant (retries until success or shutdown) and
    // register this session. First row will carry fhSeqNo 1.
    if (!tp_->connect(fhSeqNo_ + 1)) {
        if (!tp_->fatalError().empty()) {
            spdlog::critical("TP rejected this handler: {} - exiting", tp_->fatalError());
        } else {
            spdlog::warn("Shutdown before TP connection established");
        }
        return;
    }

    // What did TP already log? Seeding the id tracker with the last trade id
    // per symbol makes the first live trade comparable: whatever was traded
    // while this handler was not running shows up as a gap, is recorded and
    // backfilled. Open gaps from a previous run are picked up again.
    {
        t2s::TpPublisher::TradeState st = tp_->tradeState();
        if (st.ok) {
            for (const auto& kv : st.lastIds) {
                idTracker_.seed(kv.first, kv.second);
                seededSyms_.insert(kv.first);
            }
            for (const auto& g : st.openGaps) {
                t2s::TradeGap gap;
                gap.sym = g.sym; gap.firstId = g.firstId; gap.lastId = g.lastId;
                gap.recovered = g.recovered; gap.recoveredThroughId = g.recoveredThroughId;
                spdlog::warn("Resuming open gap {} ids {}..{} (recovered through {})",
                             gap.sym, gap.firstId, gap.lastId, gap.recoveredThroughId);
                backfill_->addGap(gap);
            }
            spdlog::info("TP trade state for {}: last logged id known for {} symbol(s), {} open gap(s) to resume",
                         cfg_.tpTable, st.lastIds.size(), st.openGaps.size());
        } else {
            spdlog::warn("No trade state from TP for {}: a gap left by this handler's downtime cannot be detected", cfg_.tpTable);
        }
    }
    backfill_->start();
    spdlog::info("Gap backfill {}: {}{} weight {}/{} per min, max gap {} ids",
                 cfg_.backfillEnabled ? "enabled" : "DISABLED (gaps are recorded as unrecoverable)",
                 cfg_.backfillRestHost, cfg_.backfillRestPath, cfg_.backfillWeight,
                 cfg_.backfillWeightLimit, cfg_.backfillMaxGapIds);

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
                    break;  // Shutdown requested during backoff
                }
            }
        }
    }

    // Cleanup
    spdlog::info("Cleaning up...");
    backfill_->stop();
    tp_->close();
    spdlog::info("TP connection closed");

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
    using Kind = t2s::TradeIdTracker::Kind;
    t2s::TradeIdTracker::Result r = idTracker_.onId(sym, tradeId);
    switch (r.kind) {
        case Kind::First:
        case Kind::InOrder:
            break;
        case Kind::OutOfOrder:
            ++ctrExchOutOfOrder_;
            spdlog::warn("OUT OF ORDER: {} last={} got={}", sym, r.previous, tradeId);
            break;
        case Kind::Duplicate:
            ++ctrExchDuplicates_;
            spdlog::warn("DUPLICATE: {} tradeId={}", sym, tradeId);
            break;
        case Kind::Gap: {
            t2s::TradeGap gap;
            gap.sym = sym;
            gap.firstId = r.firstMissing;
            gap.lastId = r.lastMissing;
            ++ctrExchGaps_;
            ctrExchMissed_ += gap.missing();
            // The first live id after a seed from TP: the gap is what this
            // handler's downtime left, not something lost while it ran.
            const bool afterRestart = seededSyms_.count(sym) > 0;
            spdlog::warn("Gap: {} missed={} (last={} got={}){}", sym, gap.missing(), r.previous, tradeId,
                         afterRestart ? " - left by this handler's downtime" : "");
            recordGap(gap, t2s::GapStatus::Detected, afterRestart ? "handlerRestart" : "");
            backfill_->addGap(gap);
            break;
        }
    }
    seededSyms_.erase(sym);
}

void TradeFeedHandler::recordGap(const t2s::TradeGap& gap, t2s::GapStatus status, const std::string& reason) {
    long long nowNs = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    // Queued until TP acknowledges it: a gap must never go unrecorded.
    gapEvents_.push(t2s::buildGapRow(nowNs, gap, cfg_.tpTable, status, reason));
    gapEvents_.flush(*tp_);
}

void TradeFeedHandler::pumpBackfill() {
    const std::int64_t nowMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    backfill_->pump(nowMs,
        [this](const std::string& sym, const t2s::BackfillTrade& t, long long recvNs) {
            publishBackfilled(sym, t, recvNs);
        },
        [this](const t2s::TradeGap& g, t2s::GapStatus st, const std::string& reason) {
            if (st == t2s::GapStatus::Recovered) {
                spdlog::warn("Gap RECOVERED: {} ids {}..{} ({} trades backfilled)", g.sym, g.firstId, g.lastId, g.recovered);
            } else if (st == t2s::GapStatus::Unrecoverable) {
                spdlog::error("Gap UNRECOVERABLE: {} ids {}..{} reason={} (recovered {} of {})",
                              g.sym, g.firstId, g.lastId, reason, g.recovered, g.missing());
            }
            recordGap(g, st, reason);
        });
}

void TradeFeedHandler::publishBackfilled(const std::string& sym, const t2s::BackfillTrade& t, long long recvTimeUtcNs) {
    // A backfilled row is a normal row of this session (next fhSeqNo, the
    // resend ring covers it) with one difference: REST does not return the
    // event time, so exchEventTimeMs is null. That null is what marks the row
    // as backfilled. `time` is when the REST reply arrived; the trade's own
    // time is exchTradeTimeMs.
    // No event time of its own: while the clock lags, `time` comes from the
    // most recent live event (row_clock.hpp).
    const t2s::RowStamp stamp = rowClock_.stamp(recvTimeUtcNs, 0);
    t2s::logClockEdge(stamp, rowClock_);
    ++fhSeqNo_;
    t2s::KOwned row;
    if (cfg_.schema == t2s::TradeSchema::SpotTrade) {
        row = t2s::buildTradeRow(stamp.timeUtcNs, recvTimeUtcNs, sym, t.id, t.price, t.qty, t.buyerIsMaker,
                                 t2s::GAP_NULL_LONG, t.tradeTimeMs, 0LL, 0LL, fhSeqNo_, KDB_EPOCH_OFFSET_NS);
    } else {
        row = t2s::buildAggTradeRow(stamp.timeUtcNs, recvTimeUtcNs, sym, t.id, t.firstTradeId, t.lastTradeId,
                                    t.price, t.qty, t.qtyExRpi, t.buyerIsMaker,
                                    t2s::GAP_NULL_LONG, t.tradeTimeMs, 0LL, 0LL, fhSeqNo_, KDB_EPOCH_OFFSET_NS);
    }
    tp_->publish(row.release(), fhSeqNo_);
    lastPubTime_ = std::chrono::system_clock::now();
    ++msgsPublished_;
    ++ctrBackfilled_;
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
    double qtyExRpi = std::numeric_limits<double>::quiet_NaN();   // futures `nq`; NaN = kdb+ null float
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

        // `nq`: quantity without the trades involving RPI orders (see
        // trade_row.hpp). Read it without failing the row if it is absent:
        // losing a trade over an optional field would be worse. A missing
        // or malformed nq is stored as null and counted (nqMissing).
        const auto& dv = doc["data"];
        bool nqOk = false;
        if (dv.HasMember("nq") && dv["nq"].IsString()) {
            const char* sNq = dv["nq"].GetString();
            char* end = nullptr;
            double v = std::strtod(sNq, &end);
            if (end != sNq && *end == '\0') { qtyExRpi = v; nqOk = true; }
        }
        if (!nqOk) ++ctrNqMissing_;
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

    // `time` is the receive time, unless the system clock is behind the
    // exchange (the first seconds after a wake): then it is the exchange
    // event time. fhRecvTimeUtcNs keeps the raw clock reading either way.
    const t2s::RowStamp stamp = rowClock_.stamp(fhRecvTimeUtcNs, exchEventTimeMs);
    t2s::logClockEdge(stamp, rowClock_);

    // Increment sequence number
    ++fhSeqNo_;

    // Build kdb+ row using the schema-appropriate helper. fhSendUs starts
    // as a placeholder 0; we patch the slot below after measuring send
    // latency. Slot index for fhSendUs is 10 (spot) or 12 (futures).
    t2s::KOwned row;
    int fhSendSlotIdx;
    if (cfg_.schema == t2s::TradeSchema::SpotTrade) {
        row = t2s::buildTradeRow(
            stamp.timeUtcNs, fhRecvTimeUtcNs, symStr, primaryId, priceV, qtyV, buyerIsMaker,
            exchEventTimeMs, exchTradeTimeMs, fhParseUs, /*fhSendUs=*/0LL, fhSeqNo_,
            KDB_EPOCH_OFFSET_NS);
        fhSendSlotIdx = t2s::TRADE_ROW_SEND_US_IDX;
    } else {  // FuturesAggTrade
        row = t2s::buildAggTradeRow(
            stamp.timeUtcNs, fhRecvTimeUtcNs, symStr, primaryId, firstAggId, lastAggId,
            priceV, qtyV, qtyExRpi, buyerIsMaker,
            exchEventTimeMs, exchTradeTimeMs, fhParseUs, /*fhSendUs=*/0LL, fhSeqNo_,
            KDB_EPOCH_OFFSET_NS);
        fhSendSlotIdx = t2s::AGG_TRADE_ROW_SEND_US_IDX;
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

    // Publish to TP. The publisher keeps the row in its resend ring; if
    // the connection is found dead it reconnects and resends whatever TP
    // has not logged, this row included.
    if (!tp_->connected()) connState_ = "reconnecting";
    tp_->publish(row.release(), fhSeqNo_);
    connState_ = tp_->connected() ? "connected" : "disconnected";

    // Update health: message published
    lastPubTime_ = std::chrono::system_clock::now();
    ++msgsPublished_;

    // Gap events that could not be acknowledged earlier (TP was away)
    if (gapEvents_.size() > 0) gapEvents_.flush(*tp_);
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
        pumpBackfill();

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
    if (gapEvents_.size() > 0) gapEvents_.flush(*tp_);
    if (!tp_->connected()) return;

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
    k(-tp_->handle(), (S)".u.upd", ks((S)"health_feed_handler"), row.release(), (K)0);

    // Exchange-hop counters, shown per table by TP's .health[] and status.sh.
    // exchGaps/exchMissed count jumps in the exchange's own trade ids, i.e.
    // trades Binance sent (or we failed to receive) between two messages.
    t2s::sendFhStats(tp_->handle(), cfg_.tpTable, {
        {"msgsReceived",   msgsReceived_},
        {"rowsPublished",  msgsPublished_},
        {"wsReconnects",   ctrWsReconnects_},
        {"exchGaps",       ctrExchGaps_},
        {"exchMissed",     ctrExchMissed_},
        {"exchOutOfOrder", ctrExchOutOfOrder_},
        {"exchDuplicates", ctrExchDuplicates_},
        {"nqMissing",      ctrNqMissing_},      // futures only: aggTrade events without `nq`
        {"gapEventsPending", static_cast<long long>(gapEvents_.size())},
        {"gapsOpen",         static_cast<long long>(backfill_->openGaps())},
        {"gapsRecovered",    backfill_->gapsRecovered()},
        {"gapsUnrecoverable", backfill_->gapsUnrecoverable()},
        {"tradesBackfilled", ctrBackfilled_},
        {"backfillRequests", backfill_->pagesRequested()},
        {"backfillFailures", backfill_->pagesFailed()},
        {"rateLimitPauses",  backfill_->scheduler().rateLimitPauses()},
        {"tpReconnects",   tp_->reconnects()},
        {"rowsResent",     tp_->rowsResent()},
        {"rowsUnresendable", tp_->rowsUnresendable()},
        {"clockLagRows",   rowClock_.correctedRows()},   // rows whose time came from the exchange
    });

    spdlog::debug("Health published: uptime={}s msgs={}/{} state={}",
        uptimeSec, msgsReceived_, msgsPublished_, connState_);
}
