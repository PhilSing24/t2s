/**
 * @file trade_fh_main.cpp
 * @brief Entry point for the trade feed handler binary.
 *
 * Owns process-level concerns: argv parsing, config load, logger init,
 * signal installation, and lifecycle of the TradeFeedHandler instance.
 * The handler itself is in trade_feed_handler.cpp and linked via the
 * t2s_fh shared lib so the same class is available to test binaries.
 *
 * The handler is parameterized over a t2s::MarketConfig built from the
 * JSON config. Defaults preserve spot behaviour (see ADR-013); the
 * futures binary supplies a different config file that overrides
 * host/port/stream_suffix/tp_table/schema.
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

constexpr char DEFAULT_CONFIG_PATH[] = "config/trade_feed_handler.json";

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

// Build a MarketConfig from the parsed JSON config fields. Unknown
// schema strings fall back to spot to keep accidental typos benign;
// the launch log makes the resolved schema visible.
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
