/**
 * @file logger.hpp
 * @brief Logging wrapper using spdlog
 * 
 * Provides consistent logging across all feed handlers with:
 * - Log levels (TRACE, DEBUG, INFO, WARN, ERROR, CRITICAL)
 * - Timestamps
 * - Component tags
 * - Optional file output
 */

#ifndef LOGGER_HPP
#define LOGGER_HPP

#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <memory>
#include <string>

/**
 * @brief Initialize logging for a component
 * 
 * @param component Component name (e.g., "Trade FH", "Quote FH")
 * @param level Log level: "trace", "debug", "info", "warn", "error"
 * @param logFile Optional file path for file logging (empty = console only)
 */
inline void initLogger(const std::string& component, 
                       const std::string& level = "info",
                       const std::string& logFile = "") {
    
    std::vector<spdlog::sink_ptr> sinks;
    
    // Console sink (with colors)
    auto consoleSink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    consoleSink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%^%l%$] %v");
    sinks.push_back(consoleSink);
    
    // Optional file sink
    if (!logFile.empty()) {
        auto fileSink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(logFile, true);
        fileSink->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] %v");
        sinks.push_back(fileSink);
    }
    
    // Create logger with component name
    auto logger = std::make_shared<spdlog::logger>(component, sinks.begin(), sinks.end());
    
    // Set log level
    if (level == "trace") logger->set_level(spdlog::level::trace);
    else if (level == "debug") logger->set_level(spdlog::level::debug);
    else if (level == "info") logger->set_level(spdlog::level::info);
    else if (level == "warn") logger->set_level(spdlog::level::warn);
    else if (level == "error") logger->set_level(spdlog::level::err);
    else logger->set_level(spdlog::level::info);
    
    // Register as default logger
    spdlog::set_default_logger(logger);
    
    spdlog::info("Logger initialized (level: {})", level);
}

/**
 * @brief Shutdown logging (flush buffers)
 */
inline void shutdownLogger() {
    spdlog::shutdown();
}

#endif // LOGGER_HPP
