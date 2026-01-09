/**
 * @file config.hpp
 * @brief JSON configuration reader for feed handlers
 */

#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>
#include <rapidjson/document.h>

/**
 * @brief Configuration for feed handlers
 */
struct FeedHandlerConfig {
    std::vector<std::string> symbols;
    std::string tpHost = "localhost";
    int tpPort = 5010;
    int initialBackoffMs = 1000;
    int maxBackoffMs = 8000;
    
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
        
        // Parse symbols array
        if (doc.HasMember("symbols") && doc["symbols"].IsArray()) {
            symbols.clear();
            const auto& arr = doc["symbols"];
            for (rapidjson::SizeType i = 0; i < arr.Size(); ++i) {
                if (arr[i].IsString()) {
                    symbols.push_back(arr[i].GetString());
                }
            }
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
        std::cout << "[Config] Symbols: ";
        for (const auto& s : symbols) std::cout << s << " ";
        std::cout << std::endl;
        std::cout << "[Config] TP: " << tpHost << ":" << tpPort << std::endl;
        std::cout << "[Config] Log level: " << logLevel << std::endl;
        
        return true;
    }
};

#endif // CONFIG_HPP
