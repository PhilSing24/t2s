/**
 * @file quote_fh_main.cpp
 * @brief Entry point for the L5 quote feed handler binary.
 *
 * Owns process-level concerns: argv parsing, config load, logger init,
 * signal installation, and lifecycle of the QuoteFeedHandler instance.
 * The handler itself is in quote_feed_handler.cpp and linked via the
 * t2s_fh shared lib so the same class is available to test binaries.
 */

#include "quote_feed_handler.hpp"
#include "config.hpp"
#include "market_config.hpp"
#include "logger.hpp"

#include <spdlog/spdlog.h>

#include <iostream>
#include <csignal>
#include <string>

namespace {

constexpr char DEFAULT_CONFIG_PATH[] = "config/quote_feed_handler.json";

// Global pointer for signal handler access.
QuoteFeedHandler* g_handler = nullptr;

void signalHandler(int signum) {
    const char* sigName = (signum == SIGINT) ? "SIGINT"
                        : (signum == SIGTERM) ? "SIGTERM"
                        : "UNKNOWN";
    spdlog::info("Received {} ({})", sigName, signum);

    if (g_handler) {
        g_handler->stop();
    }
}

// Market wiring from the JSON "market" block. Every field is required: a
// quote handler pointed at the wrong REST host or with the wrong weight
// numbers would either not sync or overspend its rate-limit budget.
bool buildMarketConfig(const FeedHandlerConfig& config, t2s::QuoteMarketConfig& m, std::string& err) {
    if (!config.hasMarketBlock) { err = "config has no \"market\" block"; return false; }
    if (config.marketSchema == "spot_depth")         m.sync = t2s::DepthSync::Spot;
    else if (config.marketSchema == "futures_depth") m.sync = t2s::DepthSync::Futures;
    else { err = "market.schema must be \"spot_depth\" or \"futures_depth\", got \"" + config.marketSchema + "\""; return false; }
    if (config.restHost.empty() || config.restPath.empty()) { err = "market.rest_host and market.rest_path are required"; return false; }
    if (config.snapshotLimit <= 0 || config.snapshotWeight <= 0 || config.weightLimitPerMin <= 0) {
        err = "market.snapshot_limit, market.snapshot_weight and market.weight_limit_per_min are required";
        return false;
    }
    m.wsHost            = config.marketHost;
    m.wsPort            = config.marketPort;
    m.wsPathPrefix      = config.marketPathPrefix;
    m.streamSuffix      = config.marketStreamSuffix;
    m.restHost          = config.restHost;
    m.restPort          = config.restPort;
    m.restPath          = config.restPath;
    m.snapshotLimit     = config.snapshotLimit;
    m.snapshotWeight    = config.snapshotWeight;
    m.weightLimitPerMin = config.weightLimitPerMin;
    m.tpTable           = config.marketTpTable;
    m.healthName        = (m.sync == t2s::DepthSync::Futures) ? "quote_fh_fut" : "quote_fh";
    return true;
}

} // namespace

int main(int argc, char* argv[]) {
    std::cout << "=== Binance L5 Quote Feed Handler ===" << std::endl;

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
    initLogger("Quote FH", config.logLevel, config.logFile);

    // Install signal handlers
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    spdlog::info("Signal handlers installed (Ctrl+C to shutdown)");

    t2s::QuoteMarketConfig market;
    std::string marketErr;
    if (!buildMarketConfig(config, market, marketErr)) {
        spdlog::critical("Bad config {}: {}", configPath, marketErr);
        shutdownLogger();
        return 1;
    }
    spdlog::info("Market: ws={}:{}{} suffix={} rest={}{} limit={} weight={}/{} per min table={}",
                 market.wsHost, market.wsPort, market.wsPathPrefix, market.streamSuffix,
                 market.restHost, market.restPath, market.snapshotLimit,
                 market.snapshotWeight, market.weightLimitPerMin, market.tpTable);

    // Create and run handler
    QuoteFeedHandler handler(config.symbols, market, config.tpHost, config.tpPort);
    g_handler = &handler;

    handler.run();

    g_handler = nullptr;
    if (!handler.fatalError().empty()) {
        spdlog::critical("Exiting with error: {}", handler.fatalError());
        shutdownLogger();
        return 2;
    }
    spdlog::info("Exiting");
    shutdownLogger();
    return 0;
}
