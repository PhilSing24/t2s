/**
 * @file trade_fh_fut_main.cpp
 * @brief Entry point for the futures trade feed handler binary.
 *
 * Owns process-level concerns: argv parsing, config load, logger init,
 * signal installation, and lifecycle of the TradeFeedHandler instance
 * driving Binance USDT-M futures (@aggTrade).
 *
 * Structurally identical to trade_fh_main.cpp; the only difference is
 * the default config path. Both binaries link the same TradeFeedHandler
 * class via the t2s_fh static library, and select market wiring (host,
 * port, stream suffix, TP table, schema) from their JSON config.
 *
 * The naming convention separates spot and futures binaries cleanly:
 *   trade_feed_handler       -> spot,    default config trade_feed_handler.json
 *   trade_feed_handler_fut   -> futures, default config trade_feed_handler_fut.json
 *
 * See ADR-013.
 */

#include "trade_feed_handler.hpp"
#include "market_config.hpp"
#include "config.hpp"
#include "logger.hpp"

#include <spdlog/spdlog.h>

#include <iostream>
#include <csignal>
#include <string>

namespace {

constexpr char DEFAULT_CONFIG_PATH[] = "config/trade_feed_handler_fut.json";

// Global pointer for signal handler access.
TradeFeedHandler* g_handler = nullptr;

void signalHandler(int signum) {
    const char* sigName = (signum == SIGINT) ? "SIGINT"
                        : (signum == SIGTERM) ? "SIGTERM"
                        : "UNKNOWN";
    spdlog::info("Received {} ({})", sigName, signum);

    if (g_handler) {
        g_handler->stop();
    }
}

// Build a MarketConfig from the parsed JSON config fields. Identical to
// the helper in trade_fh_main.cpp; duplicated here rather than shared to
// keep both binaries self-contained at the file level (the bodies will
// always be the same — if a third market is added, factor at that point).
t2s::MarketConfig buildMarketConfig(const FeedHandlerConfig& config) {
    t2s::MarketConfig m;
    m.host         = config.marketHost;
    m.port         = config.marketPort;
    m.streamSuffix = config.marketStreamSuffix;
    m.tpTable      = config.marketTpTable;
    m.schema       = (config.marketSchema == "futures_agg_trade")
                     ? t2s::TradeSchema::FuturesAggTrade
                     : t2s::TradeSchema::SpotTrade;
    return m;
}

} // namespace

int main(int argc, char* argv[]) {
    std::cout << "=== Binance Futures Trade Feed Handler (USDT-M aggTrade) ===\n";

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
    initLogger("Trade FH Fut", config.logLevel, config.logFile);

    // Install signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    spdlog::info("Signal handlers installed (Ctrl+C to shutdown)");

    // Sanity-check that the config actually selected futures. The binary
    // would still run with spot defaults if the JSON omitted the market
    // block - but that's a misconfiguration (a spot FH already exists),
    // so warn loudly rather than silently duplicate it.
    if (config.marketSchema != "futures_agg_trade") {
        spdlog::warn("Config schema '{}' is not 'futures_agg_trade' - "
                     "this binary is intended for futures. Spot mode is "
                     "served by the trade_feed_handler binary instead.",
                     config.marketSchema);
    }

    // Build market config and create handler
    t2s::MarketConfig market = buildMarketConfig(config);
    TradeFeedHandler handler(config.symbols, market, config.tpHost, config.tpPort);
    g_handler = &handler;

    handler.run();

    g_handler = nullptr;
    spdlog::info("Exiting");
    shutdownLogger();
    return 0;
}
