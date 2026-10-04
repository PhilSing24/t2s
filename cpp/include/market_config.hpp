/**
 * @file market_config.hpp
 * @brief Per-market wiring for the trade feed handler.
 *
 * Holds the four pieces of information that differ between Binance spot
 * and Binance USDT-M futures (and that could differ between other
 * Binance products or venues added later):
 *
 *   host         - WebSocket host (stream.binance.com vs fstream.binance.com)
 *   port         - WebSocket port (9443 vs 443)
 *   streamSuffix - per-symbol stream qualifier (@trade vs @aggTrade)
 *   tpTable      - destination TP table name (trade_binance vs trade_binance_fut)
 *   schema       - controls how processMessage extracts fields from the
 *                  JSON payload (SpotTrade has field `t`; FuturesAggTrade
 *                  has `a`/`f`/`l`)
 *
 * Defaults preserve the existing spot configuration, so adding a
 * MarketConfig to the constructor is a zero-behaviour-change refactor
 * for the spot binary as long as the JSON config file doesn't override
 * any of these fields.
 *
 * See ADR-013 for the rationale and migration plan.
 */

#ifndef T2S_MARKET_CONFIG_HPP
#define T2S_MARKET_CONFIG_HPP

#include <string>
#include <vector>

namespace t2s {

enum class TradeSchema {
    SpotTrade,         ///< Binance spot @trade stream payload shape
    FuturesAggTrade,   ///< Binance USDT-M futures @aggTrade stream payload shape (used from step 5 of ADR-013)
};

struct MarketConfig {
    std::string  host         = "stream.binance.com";
    std::string  port         = "9443";
    std::string  streamSuffix = "@trade";
    std::string  tpTable      = "trade_binance";
    TradeSchema  schema       = TradeSchema::SpotTrade;
};

/// Which exchange rule keeps the local book in step with the diff stream.
enum class DepthSync {
    Spot,      ///< consecutive update ids: U <= lastUpdateId+1 <= u, then U <= last+1
    Futures,   ///< USD-M futures: U <= lastUpdateId <= u, then pu == previous u
};

/**
 * Per-market wiring for the quote (depth) feed handler. Defaults are
 * Binance spot. Everything that differs between spot and USD-M futures is
 * here, so both binaries run the same QuoteFeedHandler class.
 */
struct QuoteMarketConfig {
    // WebSocket
    std::string wsHost       = "stream.binance.com";
    std::string wsPort       = "9443";
    std::string wsPathPrefix = "";                 ///< "/public" for USD-M futures
    std::string streamSuffix = "@depth@100ms";
    // REST snapshot
    std::string restHost     = "api.binance.com";
    std::string restPort     = "443";
    std::string restPath     = "/api/v3/depth";
    int snapshotLimit        = 1000;               ///< levels per side requested
    int snapshotWeight       = 50;                 ///< request weight of one snapshot at that limit
    int weightLimitPerMin    = 6000;               ///< the exchange's IP weight limit per minute
    // Destination and identity
    std::string tpTable      = "quote_binance";
    std::string healthName   = "quote_fh";
    DepthSync   sync         = DepthSync::Spot;
    // Published row layout
    int  depth               = 5;                  ///< levels per side (quote_depth in config/shared.json)
    bool publishTransactTime = false;              ///< futures row layout: adds exchTransactTimeMs (`T`) and exchPrevUpdateId (`pu`)
};

/**
 * Build a Binance combined-stream path from a symbols list and a stream
 * suffix. Pure: no clocks, no I/O, no state.
 *
 *   buildStreamPath({"btcusdt", "ethusdt"}, "@trade")
 *     => "/stream?streams=btcusdt@trade/ethusdt@trade"
 *
 *   buildStreamPath({"btcusdt"}, "@aggTrade")
 *     => "/stream?streams=btcusdt@aggTrade"
 *
 *   buildStreamPath({}, "@trade")
 *     => "/stream?streams="
 *
 * Symbol case is preserved verbatim. Binance expects lowercase symbols;
 * the caller is responsible for satisfying that contract.
 */
inline std::string buildStreamPath(const std::vector<std::string>& symbols,
                                   const std::string& streamSuffix) {
    std::string path = "/stream?streams=";
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) path += "/";
        path += symbols[i] + streamSuffix;
    }
    return path;
}

} // namespace t2s

#endif // T2S_MARKET_CONFIG_HPP
