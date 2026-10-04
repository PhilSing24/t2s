/**
 * @file quote_fh_main.cpp
 * @brief Entry point for the SPOT quote feed handler binary (quote_feed_handler).
 *
 * Everything is in quote_fh_main_common.hpp; this file only names the
 * default config and the market this binary is for.
 */

#include "quote_fh_main_common.hpp"

int main(int argc, char* argv[]) {
    return t2s::quote_main::run(argc, argv,
                                "config/quote_feed_handler.json",
                                "=== Binance Spot Quote Feed Handler ===",
                                "Quote FH",
                                "spot_depth");
}
