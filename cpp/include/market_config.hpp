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
