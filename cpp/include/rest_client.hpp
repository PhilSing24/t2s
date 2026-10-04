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

#include <sys/socket.h>
#include <sys/time.h>

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
    // Rate-limit inputs for SnapshotScheduler (0 / -1 when not available)
    int httpStatus = 0;        ///< HTTP status, 0 if the request never got a response
    int retryAfterSec = 0;     ///< Retry-After header (sent with 429 and 418)
    int usedWeight1m = -1;     ///< X-MBX-USED-WEIGHT-1M header
};

/**
 * @brief Synchronous HTTPS REST client for Binance
 */
class RestClient {
public:
    /**
     * @param host REST host, e.g. api.binance.com (spot) or fapi.binance.com (USD-M futures)
     * @param port TLS port
     * @param path depth endpoint path, e.g. /api/v3/depth or /fapi/v1/depth
     */
    explicit RestClient(std::string host = "api.binance.com",
                        std::string port = "443",
                        std::string path = "/api/v3/depth")
        : host_(std::move(host)), port_(std::move(port)), path_(std::move(path)),
          ctx_(ssl::context::tlsv12_client) {
        // Enable certificate validation. Without set_verify_mode the
        // default (verify_none) accepts any cert, which means encryption
        // works but there's no proof we're talking to Binance.
        ctx_.set_default_verify_paths();
        ctx_.set_verify_mode(ssl::verify_peer);
    }

    /**
     * @brief Fetch order book snapshot from Binance REST API
     * 
     * GET https://<host><path>?symbol=BTCUSDT&limit=N
     *   spot:    https://api.binance.com/api/v3/depth
     *   futures: https://fapi.binance.com/fapi/v1/depth (same shape plus E and T)
     * 
     * @param symbol Symbol in uppercase (e.g., "BTCUSDT")
     * @param limit Number of levels per side to request
     * @return SnapshotData with bids, asks, and lastUpdateId
     */
    /// Result of one HTTPS GET.
    struct HttpResult {
        int status = 0;            ///< HTTP status, 0 if no response was received
        std::string body;
        int retryAfterSec = 0;     ///< Retry-After header (sent with 429 and 418)
        int usedWeight1m = -1;     ///< X-MBX-USED-WEIGHT-1M header
        std::string error;         ///< transport error text, empty if a response arrived
    };

    /**
     * @brief One synchronous HTTPS GET to this client's host.
     * @param target path and query, e.g. "/api/v3/depth?symbol=BTCUSDT&limit=1000"
     * A new connection per request; send/receive timeouts of IO_TIMEOUT_SEC.
     */
    HttpResult get(const std::string& target) {
        HttpResult out;
        if (host_.empty()) { out.error = "REST client has no host configured"; return out; }
        try {
            const std::string& host = host_;
            const std::string& port = port_;

            net::io_context ioc;
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

            auto const results = resolver.resolve(host, port);
            beast::get_lowest_layer(stream).connect(results);

            // Synchronous I/O has no deadline of its own: without these a
            // stalled server would block the calling worker forever.
            {
                struct timeval tv{IO_TIMEOUT_SEC, 0};
                int fd = beast::get_lowest_layer(stream).socket().native_handle();
                ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
                ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            }

            stream.handshake(ssl::stream_base::client);

            http::request<http::string_body> req{http::verb::get, target, 11};
            req.set(http::field::host, host);
            req.set(http::field::user_agent, "binance-feed-handler/1.0");
            http::write(stream, req);

            beast::flat_buffer buffer;
            http::response_parser<http::string_body> parser;
            parser.body_limit(64 * 1024 * 1024);      // depth 1000 and 1000-trade pages are well under this
            http::read(stream, buffer, parser);
            auto res = parser.release();

            out.status = static_cast<int>(res.result_int());
            out.retryAfterSec = headerInt(res, "Retry-After", 0);
            out.usedWeight1m = headerInt(res, "X-MBX-USED-WEIGHT-1M", -1);
            out.body = std::move(res.body());

            beast::error_code ec;
            stream.shutdown(ec);       // ignore shutdown errors (common with SSL)
        } catch (const std::exception& e) {
            out.error = e.what();
        }
        return out;
    }

    SnapshotData fetchSnapshot(const std::string& symbol, int limit = 1000) {
        SnapshotData result;
        const std::string target = path_ + "?symbol=" + symbol + "&limit=" + std::to_string(limit);
        std::cout << "[REST] Fetching snapshot: " << host_ << target << std::endl;

        HttpResult r = get(target);
        result.httpStatus = r.status;
        result.retryAfterSec = r.retryAfterSec;
        result.usedWeight1m = r.usedWeight1m;
        if (!r.error.empty()) {
            result.error = r.error;
            std::cerr << "[REST] Exception: " << result.error << std::endl;
            return result;
        }
        if (r.status != 200) {
            result.error = "HTTP " + std::to_string(r.status);
            std::cerr << "[REST] Error: " << result.error << std::endl;
            return result;
        }
        parseSnapshotResponse(r.body, result);
        std::cout << "[REST] Snapshot received: lastUpdateId=" << result.lastUpdateId
                  << " bids=" << result.bids.size()
                  << " asks=" << result.asks.size() << std::endl;
        return result;
    }

    const std::string& host() const { return host_; }
    const std::string& path() const { return path_; }

    /// Send/receive timeout on the REST socket, seconds.
    static constexpr int IO_TIMEOUT_SEC = 10;

private:
    std::string host_;
    std::string port_;
    std::string path_;
    ssl::context ctx_;

    /// Integer value of a response header, or dflt if absent / not a number.
    static int headerInt(const http::response<http::string_body>& res,
                         const char* name, int dflt) {
        auto it = res.find(name);
        if (it == res.end()) return dflt;
        try { return std::stoi(std::string(it->value())); } catch (...) { return dflt; }
    }

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
