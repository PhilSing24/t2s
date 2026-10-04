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
    q.exchFirstUpdateId = 5001;
    q.exchUpdateId = 5009;
    q.exchPrevUpdateId = 4990;
    q.fhRecvTimeUtcNs = 1700000000200000000LL;
    q.fhSeqNo = 42;
    return q;
}
} // namespace

TEST_CASE("Quote row width follows the depth and the market", "[quoterow]") {
    REQUIRE(t2s::quoteRowWidth(5, false) == 30);     // spot: 8 + U + u + 4*5
    REQUIRE(t2s::quoteRowWidth(5, true) == 32);      // futures: + T + pu
    REQUIRE(t2s::quoteRowWidth(3, false) == 22);
    REQUIRE(t2s::quoteRowWidth(10, true) == 52);
    REQUIRE(t2s::quoteSendUsIndex(5, false) == 28);
}

TEST_CASE("Quote row columns are in schema order", "[quoterow]") {
    for (int depth : {1, 3, 5, 10}) {
        for (bool fut : {false, true}) {
            BookQuote q = sample(depth);
            K row = t2s::buildQuoteRow(q, 7, 9, fut);
            REQUIRE(row->t == 0);
            REQUIRE(row->n == t2s::quoteRowWidth(depth, fut));

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
            if (fut) REQUIRE(f[i++]->j == 1700000000111LL);                 // exchTransactTimeMs
            REQUIRE(f[i++]->j == 5001);                                     // exchFirstUpdateId
            REQUIRE(f[i++]->j == 5009);                                     // exchUpdateId
            if (fut) REQUIRE(f[i++]->j == 4990);                            // exchPrevUpdateId
            REQUIRE(f[i++]->j == q.fhRecvTimeUtcNs);
            REQUIRE(f[i++]->j == 7);                                        // fhParseUs
            REQUIRE(i == t2s::quoteSendUsIndex(depth, fut));
            REQUIRE(f[i++]->j == 9);                                        // fhSendUs
            REQUIRE(f[i++]->j == 42);                                       // fhSeqNo
            REQUIRE(i == row->n);
            r0(row);
        }
    }
}

TEST_CASE("An invalid quote row has null update ids", "[quoterow]") {
    for (bool fut : {false, true}) {
        BookQuote q(5);
        q.sym = "BTCUSDT";
        q.isValid = false;
        q.exchFirstUpdateId = 7; q.exchUpdateId = 8; q.exchPrevUpdateId = 6;   // must not leak out
        K row = t2s::buildQuoteRow(q, 0, 0, fut);
        K* f = kK(row);
        int i = 2 + 4 * 5 + 2 + (fut ? 1 : 0);
        REQUIRE(f[i++]->j == t2s::QUOTE_NULL_LONG);
        REQUIRE(f[i++]->j == t2s::QUOTE_NULL_LONG);
        if (fut) REQUIRE(f[i++]->j == t2s::QUOTE_NULL_LONG);
        r0(row);
    }
}
