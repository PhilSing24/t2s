/**
 * @file rest_client.hpp
 * @brief HTTPS REST client for Binance API using Boost.Beast
 * 
 * Used to fetch order book snapshots for reconciliation.
 * Synchronous implementation - blocks until response received.
 * 
 * @see https://binance-docs.github.io/apidocs/spot/en/#order-book
 */

#ifndef REST_CLIENT_HPP
#define REST_CLIENT_HPP

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>

#include <rapidjson/document.h>

#include <string>
#include <vector>
#include <iostream>
#include <stdexcept>

#include "order_book_manager.hpp"

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;

/**
 * @brief Snapshot data returned from REST API
 */
struct SnapshotData {
    long long lastUpdateId = 0;
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
    bool success = false;
    std::string error;
};

/**
 * @brief Synchronous HTTPS REST client for Binance
 */
class RestClient {
public:
    RestClient() : ctx_(ssl::context::tlsv12_client) {
        ctx_.set_default_verify_paths();
    }

    /**
     * @brief Fetch order book snapshot from Binance REST API
     * 
     * GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=N
     * 
     * @param symbol Symbol in uppercase (e.g., "BTCUSDT")
     * @param limit Number of levels (default BOOK_DEPTH)
     * @return SnapshotData with bids, asks, and lastUpdateId
     */
    SnapshotData fetchSnapshot(const std::string& symbol, int limit = BOOK_DEPTH) {
        SnapshotData result;
        
        try {
            const std::string host = "api.binance.com";
            const std::string port = "443";
            const std::string target = "/api/v3/depth?symbol=" + symbol + 
                                       "&limit=" + std::to_string(limit);

            std::cout << "[REST] Fetching snapshot: " << host << target << std::endl;

            // IO context for this request
            net::io_context ioc;

            // Resolver and SSL stream
            tcp::resolver resolver(ioc);
            beast::ssl_stream<beast::tcp_stream> stream(ioc, ctx_);

            // Set SNI hostname
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                throw beast::system_error(
                    beast::error_code(static_cast<int>(::ERR_get_error()),
                                      net::error::get_ssl_category()),
                    "Failed to set SNI hostname");
            }

            // Resolve and connect
            auto const results = resolver.resolve(host, port);
            beast::get_lowest_layer(stream).connect(results);

            // TLS handshake
            stream.handshake(ssl::stream_base::client);

            // Build HTTP request
            http::request<http::string_body> req{http::verb::get, target, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "binance-feed-handler/1.0");

            // Send request
            http::write(stream, req);

            // Receive response
            beast::flat_buffer buffer;
            http::response<http::string_body> res;
            http::read(stream, buffer, res);

            // Check status
            if (res.result() != http::status::ok) {
                result.error = "HTTP " + std::to_string(static_cast<int>(res.result()));
                std::cerr << "[REST] Error: " << result.error << std::endl;
                return result;
            }

            // Parse JSON response
            parseSnapshotResponse(res.body(), result);

            // Graceful shutdown
            beast::error_code ec;
            stream.shutdown(ec);
            // Ignore shutdown errors (common with SSL)

            std::cout << "[REST] Snapshot received: lastUpdateId=" << result.lastUpdateId
                      << " bids=" << result.bids.size()
                      << " asks=" << result.asks.size() << std::endl;

        } catch (const std::exception& e) {
            result.error = e.what();
            std::cerr << "[REST] Exception: " << result.error << std::endl;
        }

        return result;
    }

private:
    ssl::context ctx_;

    /**
     * @brief Parse JSON snapshot response
     * 
     * Response format:
     * {
     *   "lastUpdateId": 1027024,
     *   "bids": [["4.00000000", "431.00000000"], ...],
     *   "asks": [["4.00000200", "12.00000000"], ...]
     * }
     * 
     * Note: Prices and quantities are strings in Binance API
     */
    void parseSnapshotResponse(const std::string& body, SnapshotData& result) {
        rapidjson::Document doc;
        doc.Parse(body.c_str());

        if (!doc.IsObject()) {
            result.error = "Invalid JSON response";
            return;
        }

        // Check for API error
        if (doc.HasMember("code")) {
            result.error = "API error: " + std::to_string(doc["code"].GetInt());
            if (doc.HasMember("msg")) {
                result.error += " - " + std::string(doc["msg"].GetString());
            }
            return;
        }

        // Extract lastUpdateId
        if (!doc.HasMember("lastUpdateId")) {
            result.error = "Missing lastUpdateId";
            return;
        }
        result.lastUpdateId = doc["lastUpdateId"].GetInt64();

        // Parse bids (already sorted high→low by exchange)
        if (doc.HasMember("bids") && doc["bids"].IsArray()) {
            const auto& bids = doc["bids"];
            result.bids.reserve(bids.Size());
            for (rapidjson::SizeType i = 0; i < bids.Size(); ++i) {
                if (bids[i].IsArray() && bids[i].Size() >= 2) {
                    PriceLevel lvl;
                    lvl.price = std::stod(bids[i][0].GetString());
                    lvl.qty = std::stod(bids[i][1].GetString());
                    result.bids.push_back(lvl);
                }
            }
        }

        // Parse asks (already sorted low→high by exchange)
        if (doc.HasMember("asks") && doc["asks"].IsArray()) {
            const auto& asks = doc["asks"];
            result.asks.reserve(asks.Size());
            for (rapidjson::SizeType i = 0; i < asks.Size(); ++i) {
                if (asks[i].IsArray() && asks[i].Size() >= 2) {
                    PriceLevel lvl;
                    lvl.price = std::stod(asks[i][0].GetString());
                    lvl.qty = std::stod(asks[i][1].GetString());
                    result.asks.push_back(lvl);
                }
            }
        }

        result.success = true;
    }
};

#endif // REST_CLIENT_HPP
