/**
 * @file quote_fh_fut_main.cpp
 * @brief Entry point for the USD-M FUTURES quote feed handler binary
 *        (quote_feed_handler_fut).
 *
 * Same QuoteFeedHandler class as the spot binary, driven by
 * config/quote_feed_handler_fut.json: routed WebSocket endpoint
 * wss://fstream.binance.com/public, REST snapshots from
 * fapi.binance.com/fapi/v1/depth, the futures sync rule (pu chain), and
 * the quote_binance_fut table. Mirrors trade_fh_fut_main.cpp.
 */

#include "quote_fh_main_common.hpp"

int main(int argc, char* argv[]) {
    return t2s::quote_main::run(argc, argv,
                                "config/quote_feed_handler_fut.json",
                                "=== Binance USD-M Futures Quote Feed Handler ===",
                                "Quote FH Fut",
                                "futures_depth");
}
