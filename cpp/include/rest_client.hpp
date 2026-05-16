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
#include <boost/asio/ssl/host_name_verification.hpp>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <string>
#include <vector>
#include <iostream>
#include <stdexcept>

#include "order_book_manager.hpp"
#include "json_reader.hpp"

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
        // Enable certificate validation. Without set_verify_mode the
        // default (verify_none) accepts any cert, which means encryption
        // works but there's no proof we're talking to Binance.
        ctx_.set_default_verify_paths();
        ctx_.set_verify_mode(ssl::verify_peer);
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

            // Set SNI hostname (required for Binance TLS)
            if (!SSL_set_tlsext_host_name(stream.native_handle(), host.c_str())) {
                throw beast::system_error(
                    beast::error_code(static_cast<int>(::ERR_get_error()),
                                      net::error::get_ssl_category()),
                    "Failed to set SNI hostname");
            }

            // Verify the cert's CN/SAN matches the hostname we asked to connect to.
            // Together with set_verify_mode(verify_peer) in the constructor, this
            // makes a MITM with any valid TLS cert fail the handshake.
            stream.set_verify_callback(ssl::host_name_verification(host));

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
     * Note: Prices and quantities are strings in Binance API.
     * 
     * Uses JsonReader for safe field access (no rapidjson asserts, no
     * std::stod exceptions). Malformed individual levels are silently
     * skipped; top-level schema violations set result.error and abort.
     */
    void parseSnapshotResponse(const std::string& body, SnapshotData& result) {
        rapidjson::Document doc;
        doc.Parse(body.c_str());

        if (doc.HasParseError()) {
            result.error = std::string("JSON parse error: ")
                         + rapidjson::GetParseError_En(doc.GetParseError());
            return;
        }
        if (!doc.IsObject()) {
            result.error = "Response not a JSON object";
            return;
        }

        // Check for API error first (separate response shape from success).
        // We do this with raw rapidjson access since the JsonReader's
        // accessors would set an error if "code" is missing in normal
        // success responses.
        if (doc.HasMember("code")) {
            int code = 0;
            if (doc["code"].IsInt()) code = doc["code"].GetInt();
            std::string apiMsg;
            if (doc.HasMember("msg") && doc["msg"].IsString()) {
                apiMsg = doc["msg"].GetString();
            }
            result.error = "API error " + std::to_string(code) + ": " + apiMsg;
            return;
        }

        t2s::JsonReader r(doc);

        auto lastId = r.int64("lastUpdateId");
        const auto* bidsArr = r.array("bids");
        const auto* asksArr = r.array("asks");

        if (r.hasError()) {
            result.error = "Snapshot schema error: " + r.lastError();
            return;
        }

        result.lastUpdateId = *lastId;

        // Bids: pre-sorted high->low by exchange. Silently skip malformed
        // levels (per-level resilience).
        result.bids.reserve(bidsArr->Size());
        for (const auto& lvl : bidsArr->GetArray()) {
            if (auto p = t2s::parseLevelPair(lvl)) {
                PriceLevel pl;
                pl.price = p->first;
                pl.qty   = p->second;
                result.bids.push_back(pl);
            }
        }

        // Asks: pre-sorted low->high by exchange.
        result.asks.reserve(asksArr->Size());
        for (const auto& lvl : asksArr->GetArray()) {
            if (auto p = t2s::parseLevelPair(lvl)) {
                PriceLevel pl;
                pl.price = p->first;
                pl.qty   = p->second;
                result.asks.push_back(pl);
            }
        }

        result.success = true;
    }
};

#endif // REST_CLIENT_HPP
