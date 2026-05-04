/**
 * @file trade_fh_main.cpp
 * @brief Entry point for the trade feed handler binary.
 *
 * Owns process-level concerns: argv parsing, config load, logger init,
 * signal installation, and lifecycle of the TradeFeedHandler instance.
 * The handler itself is in trade_feed_handler.cpp and linked via the
 * t2s_fh shared lib so the same class is available to test binaries.
 */

#include "trade_feed_handler.hpp"
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

    // Create and run handler
    TradeFeedHandler handler(config.symbols, config.tpHost, config.tpPort);
    g_handler = &handler;

    handler.run();

    g_handler = nullptr;
    spdlog::info("Exiting");
    shutdownLogger();
    return 0;
}
