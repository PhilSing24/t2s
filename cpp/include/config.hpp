/**
 * @file config.hpp
 * @brief JSON configuration reader for feed handlers
 */

#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <vector>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <iostream>
#include <rapidjson/document.h>

/**
 * @brief Configuration for feed handlers
 *
 * The market block (host/port/streamSuffix/tpTable/schema) carries the
 * per-market wiring that distinguishes spot from futures (see ADR-013).
 * Defaults reproduce the existing spot configuration, so a config file
 * that omits the "market" block continues to work as before.
 */
struct FeedHandlerConfig {
    std::vector<std::string> symbols;
    std::string tpHost = "localhost";
    int tpPort = 5010;
    int initialBackoffMs = 1000;
    int maxBackoffMs = 8000;

    // Per-market wiring. Defaults select Binance spot.
    std::string marketHost         = "stream.binance.com";
    std::string marketPort         = "9443";
    std::string marketStreamSuffix = "@trade";
    std::string marketTpTable      = "trade_binance";
    std::string marketSchema       = "spot_trade";  // or "futures_agg_trade"; quotes: "spot_depth" / "futures_depth"
    bool        hasMarketBlock     = false;

    // Trade handlers only: backfill of trade-id gaps over REST ("backfill" block)
    bool        backfillEnabled     = false;
    std::string backfillRestHost    = "";
    std::string backfillRestPath    = "";
    int         backfillWeight      = 0;
    int         backfillWeightLimit = 0;
    long long   backfillMaxGapIds   = 500000;

    // Quote handlers only (REST snapshot endpoint and its rate-limit numbers)
    std::string marketPathPrefix   = "";
    std::string restHost           = "";
    std::string restPort           = "443";
    std::string restPath           = "";
    int         snapshotLimit      = 0;
    int         snapshotWeight     = 0;
    int         weightLimitPerMin  = 0;

    // From the shared file (symbols above come from it too)
    int         quoteDepth = 5;          // levels per side published by the quote handlers
    std::string sharedConfigPath;        // where symbols and quoteDepth were read from
    /// Optional clock_lag_ms: an exchange event time ahead of the receive time
    /// by more than this means the system clock is behind (row_clock.hpp).
    long long   clockLagMs = 2000;
    static constexpr long long MIN_CLOCK_LAG_MS = 100;       // below this, network jitter would trigger it
    static constexpr long long MAX_CLOCK_LAG_MS = 3600000;

    /// Smallest / largest quote depth accepted. The upper bound keeps the
    /// depth well under the book's refresh low-water mark (100 levels).
    static constexpr int MIN_QUOTE_DEPTH = 1;
    static constexpr int MAX_QUOTE_DEPTH = 50;

    /// shared.json path: T2S_SHARED_CONFIG, else next to the handler's config.
    static std::string sharedPathFor(const std::string& handlerConfigPath) {
        if (const char* env = std::getenv("T2S_SHARED_CONFIG")) {
            if (*env) return env;
        }
        auto slash = handlerConfigPath.find_last_of('/');
        std::string dir = (slash == std::string::npos) ? "." : handlerConfigPath.substr(0, slash);
        return dir + "/shared.json";
    }

    bool loadShared(const std::string& handlerConfigPath) {
        sharedConfigPath = sharedPathFor(handlerConfigPath);
        std::ifstream file(sharedConfigPath);
        if (!file.is_open()) {
            std::cerr << "[Config] Failed to open shared config: " << sharedConfigPath << std::endl;
            return false;
        }
        std::stringstream buffer;
        buffer << file.rdbuf();
        rapidjson::Document doc;
        doc.Parse(buffer.str().c_str());
        if (doc.HasParseError() || !doc.IsObject()) {
            std::cerr << "[Config] JSON parse error in: " << sharedConfigPath << std::endl;
            return false;
        }
        if (!doc.HasMember("symbols") || !doc["symbols"].IsArray() || doc["symbols"].Empty()) {
            std::cerr << "[Config] " << sharedConfigPath << ": \"symbols\" must be a non-empty array" << std::endl;
            return false;
        }
        symbols.clear();
        for (const auto& v : doc["symbols"].GetArray()) {
            if (!v.IsString() || v.GetStringLength() == 0) {
                std::cerr << "[Config] " << sharedConfigPath << ": every symbol must be a non-empty string" << std::endl;
                return false;
            }
            std::string sym = v.GetString();
            for (auto& ch : sym) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
            symbols.push_back(sym);       // Binance stream names are lowercase
        }
        if (!doc.HasMember("quote_depth") || !doc["quote_depth"].IsInt()) {
            std::cerr << "[Config] " << sharedConfigPath << ": \"quote_depth\" (integer) is required" << std::endl;
            return false;
        }
        quoteDepth = doc["quote_depth"].GetInt();
        if (quoteDepth < MIN_QUOTE_DEPTH || quoteDepth > MAX_QUOTE_DEPTH) {
            std::cerr << "[Config] " << sharedConfigPath << ": quote_depth " << quoteDepth
                      << " is outside " << MIN_QUOTE_DEPTH << ".." << MAX_QUOTE_DEPTH << std::endl;
            return false;
        }
        if (doc.HasMember("clock_lag_ms")) {
            if (!doc["clock_lag_ms"].IsInt64()) {
                std::cerr << "[Config] " << sharedConfigPath << ": \"clock_lag_ms\" must be an integer" << std::endl;
                return false;
            }
            clockLagMs = doc["clock_lag_ms"].GetInt64();
            if (clockLagMs < MIN_CLOCK_LAG_MS || clockLagMs > MAX_CLOCK_LAG_MS) {
                std::cerr << "[Config] " << sharedConfigPath << ": clock_lag_ms " << clockLagMs
                          << " is outside " << MIN_CLOCK_LAG_MS << ".." << MAX_CLOCK_LAG_MS << std::endl;
                return false;
            }
        }
        return true;
    }

    // Logging config
    std::string logLevel = "info";
    std::string logFile = "";  // Empty = console only

    /**
     * @brief Load configuration from JSON file
     * @param filepath Path to JSON config file
     * @return true if loaded successfully, false otherwise
     */
    bool load(const std::string& filepath) {
        std::ifstream file(filepath);
        if (!file.is_open()) {
            std::cerr << "[Config] Failed to open: " << filepath << std::endl;
            return false;
        }

        std::stringstream buffer;
        buffer << file.rdbuf();
        std::string json = buffer.str();

        rapidjson::Document doc;
        doc.Parse(json.c_str());

        if (doc.HasParseError()) {
            std::cerr << "[Config] JSON parse error in: " << filepath << std::endl;
            return false;
        }

        // Symbols and the quote depth are shared by all four handlers and
        // by the q schemas: they live in shared.json next to this file (or
        // wherever T2S_SHARED_CONFIG points), never in a handler's own file.
        if (doc.HasMember("symbols")) {
            std::cerr << "[Config] " << filepath << " has a \"symbols\" list. Symbols are now shared by all"
                      << " handlers: remove it and edit shared.json instead." << std::endl;
            return false;
        }
        if (!loadShared(filepath)) {
            return false;
        }

        // Parse tickerplant config
        if (doc.HasMember("tickerplant") && doc["tickerplant"].IsObject()) {
            const auto& tp = doc["tickerplant"];
            if (tp.HasMember("host") && tp["host"].IsString()) {
                tpHost = tp["host"].GetString();
            }
            if (tp.HasMember("port") && tp["port"].IsInt()) {
                tpPort = tp["port"].GetInt();
            }
        }

        // Parse reconnect config
        if (doc.HasMember("reconnect") && doc["reconnect"].IsObject()) {
            const auto& rc = doc["reconnect"];
            if (rc.HasMember("initial_backoff_ms") && rc["initial_backoff_ms"].IsInt()) {
                initialBackoffMs = rc["initial_backoff_ms"].GetInt();
            }
            if (rc.HasMember("max_backoff_ms") && rc["max_backoff_ms"].IsInt()) {
                maxBackoffMs = rc["max_backoff_ms"].GetInt();
            }
        }

        // Parse market config (optional; defaults preserve spot behaviour)
        if (doc.HasMember("market") && doc["market"].IsObject()) {
            const auto& m = doc["market"];
            hasMarketBlock = true;
            if (m.HasMember("path_prefix") && m["path_prefix"].IsString()) {
                marketPathPrefix = m["path_prefix"].GetString();
            }
            if (m.HasMember("rest_host") && m["rest_host"].IsString()) {
                restHost = m["rest_host"].GetString();
            }
            if (m.HasMember("rest_port") && m["rest_port"].IsString()) {
                restPort = m["rest_port"].GetString();
            }
            if (m.HasMember("rest_path") && m["rest_path"].IsString()) {
                restPath = m["rest_path"].GetString();
            }
            if (m.HasMember("snapshot_limit") && m["snapshot_limit"].IsInt()) {
                snapshotLimit = m["snapshot_limit"].GetInt();
            }
            if (m.HasMember("snapshot_weight") && m["snapshot_weight"].IsInt()) {
                snapshotWeight = m["snapshot_weight"].GetInt();
            }
            if (m.HasMember("weight_limit_per_min") && m["weight_limit_per_min"].IsInt()) {
                weightLimitPerMin = m["weight_limit_per_min"].GetInt();
            }
            if (m.HasMember("host") && m["host"].IsString()) {
                marketHost = m["host"].GetString();
            }
            if (m.HasMember("port") && m["port"].IsString()) {
                marketPort = m["port"].GetString();
            }
            if (m.HasMember("stream_suffix") && m["stream_suffix"].IsString()) {
                marketStreamSuffix = m["stream_suffix"].GetString();
            }
            if (m.HasMember("tp_table") && m["tp_table"].IsString()) {
                marketTpTable = m["tp_table"].GetString();
            }
            if (m.HasMember("schema") && m["schema"].IsString()) {
                marketSchema = m["schema"].GetString();
            }
        }

        // Parse backfill config (trade handlers; optional)
        if (doc.HasMember("backfill") && doc["backfill"].IsObject()) {
            const auto& b = doc["backfill"];
            backfillEnabled = !(b.HasMember("enabled") && b["enabled"].IsBool() && !b["enabled"].GetBool());
            if (b.HasMember("rest_host") && b["rest_host"].IsString()) backfillRestHost = b["rest_host"].GetString();
            if (b.HasMember("rest_path") && b["rest_path"].IsString()) backfillRestPath = b["rest_path"].GetString();
            if (b.HasMember("request_weight") && b["request_weight"].IsInt()) backfillWeight = b["request_weight"].GetInt();
            if (b.HasMember("weight_limit_per_min") && b["weight_limit_per_min"].IsInt()) backfillWeightLimit = b["weight_limit_per_min"].GetInt();
            if (b.HasMember("max_gap_ids") && b["max_gap_ids"].IsInt64()) backfillMaxGapIds = b["max_gap_ids"].GetInt64();
            if (backfillEnabled && (backfillRestHost.empty() || backfillRestPath.empty() ||
                                    backfillWeight <= 0 || backfillWeightLimit <= 0 || backfillMaxGapIds <= 0)) {
                std::cerr << "[Config] " << filepath << ": backfill needs rest_host, rest_path, request_weight,"
                          << " weight_limit_per_min (and a positive max_gap_ids)" << std::endl;
                return false;
            }
        }

        // Parse logging config
        if (doc.HasMember("logging") && doc["logging"].IsObject()) {
            const auto& lg = doc["logging"];
            if (lg.HasMember("level") && lg["level"].IsString()) {
                logLevel = lg["level"].GetString();
            }
            if (lg.HasMember("file") && lg["file"].IsString()) {
                logFile = lg["file"].GetString();
            }
        }

        std::cout << "[Config] Loaded from: " << filepath << std::endl;
        std::cout << "[Config] Shared:  " << sharedConfigPath << " (quote_depth=" << quoteDepth << ", clock_lag_ms=" << clockLagMs << ")" << std::endl;
        std::cout << "[Config] Symbols: ";
        for (const auto& s : symbols) std::cout << s << " ";
        std::cout << std::endl;
        std::cout << "[Config] TP: " << tpHost << ":" << tpPort << std::endl;
        std::cout << "[Config] Market: " << marketHost << ":" << marketPort
                  << " streamSuffix=" << marketStreamSuffix
                  << " tpTable=" << marketTpTable
                  << " schema=" << marketSchema << std::endl;
        std::cout << "[Config] Log level: " << logLevel << std::endl;

        return true;
    }
};

#endif // CONFIG_HPP
