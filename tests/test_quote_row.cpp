/**
 * @file test_quote_row.cpp
 * @brief The quote row sent to TP: width and column positions per depth.
 */

#include "quote_row.hpp"
#include "catch_amalgamated.hpp"

namespace {
BookQuote sample(int depth) {
    BookQuote q(depth);
    q.sym = "BTCUSDT";
    for (int i = 0; i < depth; ++i) {
        q.bidPrices[i] = 100.0 - i; q.bidQtys[i] = 1.0 + i;
        q.askPrices[i] = 101.0 + i; q.askQtys[i] = 11.0 + i;
    }
    q.isValid = true;
    q.exchEventTimeMs = 1700000000123LL;
    q.exchTransactTimeMs = 1700000000111LL;
    q.fhRecvTimeUtcNs = 1700000000200000000LL;
    q.fhSeqNo = 42;
    return q;
}
} // namespace

TEST_CASE("Quote row width follows the depth", "[quoterow]") {
    REQUIRE(t2s::quoteRowWidth(5, false) == 28);     // the layout before depth was configurable
    REQUIRE(t2s::quoteRowWidth(5, true) == 29);
    REQUIRE(t2s::quoteRowWidth(3, false) == 20);
    REQUIRE(t2s::quoteRowWidth(10, true) == 49);
    REQUIRE(t2s::quoteSendUsIndex(5, false) == 26);
}

TEST_CASE("Quote row columns are in schema order", "[quoterow]") {
    for (int depth : {1, 3, 5, 10}) {
        for (bool withT : {false, true}) {
            BookQuote q = sample(depth);
            K row = t2s::buildQuoteRow(q, 7, 9, withT);
            REQUIRE(row->t == 0);
            REQUIRE(row->n == t2s::quoteRowWidth(depth, withT));

            K* f = kK(row);
            REQUIRE(f[0]->t == -KP);
            REQUIRE(f[0]->j == q.fhRecvTimeUtcNs - t2s::QUOTE_KDB_EPOCH_OFFSET_NS);
            REQUIRE(f[1]->t == -KS);
            REQUIRE(std::string(f[1]->s) == "BTCUSDT");
            for (int i = 0; i < depth; ++i) {
                REQUIRE(f[2 + i]->f == 100.0 - i);               // bidPrice
                REQUIRE(f[2 + depth + i]->f == 1.0 + i);         // bidQty
                REQUIRE(f[2 + 2 * depth + i]->f == 101.0 + i);   // askPrice
                REQUIRE(f[2 + 3 * depth + i]->f == 11.0 + i);    // askQty
            }
            int i = 2 + 4 * depth;
            REQUIRE(f[i]->t == -KB);  REQUIRE(f[i]->g == 1);  ++i;          // isValid
            REQUIRE(f[i++]->j == 1700000000123LL);                          // exchEventTimeMs
            if (withT) REQUIRE(f[i++]->j == 1700000000111LL);               // exchTransactTimeMs
            REQUIRE(f[i++]->j == q.fhRecvTimeUtcNs);
            REQUIRE(f[i++]->j == 7);                                        // fhParseUs
            REQUIRE(i == t2s::quoteSendUsIndex(depth, withT));
            REQUIRE(f[i++]->j == 9);                                        // fhSendUs
            REQUIRE(f[i++]->j == 42);                                       // fhSeqNo
            REQUIRE(i == row->n);
            r0(row);
        }
    }
}
