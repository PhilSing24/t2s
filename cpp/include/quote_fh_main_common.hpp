/**
 * @file quote_fh_main_common.hpp
 * @brief The main() shared by the spot and futures quote handler binaries.
 *
 * Both binaries run the same QuoteFeedHandler class; they differ only in
 * their default config file and in which market schema they accept:
 *
 *   quote_feed_handler       spot,          config/quote_feed_handler.json      (spot_depth)
 *   quote_feed_handler_fut   USD-M futures, config/quote_feed_handler_fut.json  (futures_depth)
 *
 * A binary given the other market's config refuses to start, so a mix-up
 * cannot send spot rows to the futures table or the other way round.
 * Symbols and the quote depth come from config/shared.json.
 */

#ifndef T2S_QUOTE_FH_MAIN_COMMON_HPP
#define T2S_QUOTE_FH_MAIN_COMMON_HPP

#include "quote_feed_handler.hpp"
#include "config.hpp"
#include "market_config.hpp"
#include "logger.hpp"

#include <spdlog/spdlog.h>

#include <iostream>
#include <csignal>
#include <string>

namespace t2s {
namespace quote_main {

// Global pointer for signal handler access.
inline QuoteFeedHandler* g_handler = nullptr;

inline void signalHandler(int signum) {
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
inline bool buildMarketConfig(const FeedHandlerConfig& config, t2s::QuoteMarketConfig& m, std::string& err) {
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
    m.depth             = config.quoteDepth;
    // Futures depth events carry a transaction time T and a previous
    // update id pu, stored in quote_binance_fut (exchTransactTimeMs,
    // exchPrevUpdateId); spot events have neither.
    m.publishTransactTime = (m.sync == t2s::DepthSync::Futures);
    return true;
}

/**
 * @param defaultConfigPath config used when no argument is given
 * @param banner            first line printed
 * @param loggerName        spdlog logger name
 * @param requiredSchema    "spot_depth" or "futures_depth": what this binary is for
 */
inline int run(int argc, char* argv[], const char* defaultConfigPath, const char* banner,
               const char* loggerName, const char* requiredSchema) {
    std::cout << banner << std::endl;

    std::string configPath = defaultConfigPath;
    if (argc > 1) {
        configPath = argv[1];
    }

    FeedHandlerConfig config;
    if (!config.load(configPath)) {
        std::cerr << "Failed to load config, exiting\n";
        return 1;
    }

    initLogger(loggerName, config.logLevel, config.logFile);

    if (config.marketSchema != requiredSchema) {
        spdlog::critical("Bad config {}: this binary handles market.schema \"{}\" but the config says \"{}\"",
                         configPath, requiredSchema, config.marketSchema);
        shutdownLogger();
        return 1;
    }

    t2s::QuoteMarketConfig market;
    std::string marketErr;
    if (!buildMarketConfig(config, market, marketErr)) {
        spdlog::critical("Bad config {}: {}", configPath, marketErr);
        shutdownLogger();
        return 1;
    }
    spdlog::info("Symbols and depth from {}: depth={} (row width {})", config.sharedConfigPath,
                 market.depth, 4 * market.depth + 10 + (market.publishTransactTime ? 2 : 0));
    spdlog::info("Market: ws={}:{}{} suffix={} rest={}{} limit={} weight={}/{} per min table={}",
                 market.wsHost, market.wsPort, market.wsPathPrefix, market.streamSuffix,
                 market.restHost, market.restPath, market.snapshotLimit,
                 market.snapshotWeight, market.weightLimitPerMin, market.tpTable);

    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);
    spdlog::info("Signal handlers installed (Ctrl+C to shutdown)");

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

} // namespace quote_main
} // namespace t2s

#endif // T2S_QUOTE_FH_MAIN_COMMON_HPP
