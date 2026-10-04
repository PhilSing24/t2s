/**
 * @file quote_row.hpp
 * @brief Build the kdb+ row a quote handler publishes to the tickerplant.
 *
 * Column order must match .schema.quote / .schema.quoteFut in
 * kdb/schemas.q, which are generated from the same depth (quote_depth in
 * config/shared.json):
 *
 *   time, sym,
 *   bidPrice1..N, bidQty1..N, askPrice1..N, askQty1..N,
 *   isValid, exchEventTimeMs,
 *   [exchTransactTimeMs,]                    <- futures only
 *   exchFirstUpdateId, exchUpdateId,
 *   [exchPrevUpdateId,]                      <- futures only
 *   fhRecvTimeUtcNs, fhParseUs, fhSendUs, fhSeqNo
 *
 * The three update-id columns are null on an invalid row.
 *
 * The width is what the handler announces to TP at registration; TP
 * refuses a handler whose width differs from its schema, so a handler and
 * a TP configured with different depths cannot exchange a single row.
 */

#ifndef T2S_QUOTE_ROW_HPP
#define T2S_QUOTE_ROW_HPP

#include "order_book_manager.hpp"

#include "k_object.hpp"   // includes k.h and undefines its one-letter macros

namespace t2s {

/// Nanoseconds between the Unix epoch (1970) and the kdb+ epoch (2000)
constexpr long long QUOTE_KDB_EPOCH_OFFSET_NS = 946684800000000000LL;

/// kdb+ null long
constexpr long long QUOTE_NULL_LONG = static_cast<long long>(0x8000000000000000ULL);

/// Number of feed-handler columns in a quote row. futuresLayout adds
/// exchTransactTimeMs and exchPrevUpdateId.
inline int quoteRowWidth(int depth, bool futuresLayout) noexcept {
    return 4 * depth + 10 + (futuresLayout ? 2 : 0);
}

/// Position of fhSendUs in the row (second to last)
inline int quoteSendUsIndex(int depth, bool futuresLayout) noexcept {
    return quoteRowWidth(depth, futuresLayout) - 2;
}

/// Build the row. Caller owns the returned K (or passes it to k()).
inline K buildQuoteRow(const BookQuote& q, long long fhParseUs, long long fhSendUs,
                       bool futuresLayout) {
    const int depth = q.depth();
    auto id = [&q](long long v) { return q.isValid ? v : QUOTE_NULL_LONG; };
    K row = ktn(0, quoteRowWidth(depth, futuresLayout));
    int i = 0;
    kK(row)[i++] = ktj(-KP, q.fhRecvTimeUtcNs - QUOTE_KDB_EPOCH_OFFSET_NS);
    kK(row)[i++] = ks(const_cast<S>(q.sym.c_str()));
    for (double v : q.bidPrices) kK(row)[i++] = kf(v);
    for (double v : q.bidQtys)   kK(row)[i++] = kf(v);
    for (double v : q.askPrices) kK(row)[i++] = kf(v);
    for (double v : q.askQtys)   kK(row)[i++] = kf(v);
    kK(row)[i++] = kb(q.isValid);
    kK(row)[i++] = kj(q.exchEventTimeMs);
    if (futuresLayout) kK(row)[i++] = kj(q.exchTransactTimeMs);
    kK(row)[i++] = kj(id(q.exchFirstUpdateId));
    kK(row)[i++] = kj(id(q.exchUpdateId));
    if (futuresLayout) kK(row)[i++] = kj(id(q.exchPrevUpdateId));
    kK(row)[i++] = kj(q.fhRecvTimeUtcNs);
    kK(row)[i++] = kj(fhParseUs);
    kK(row)[i++] = kj(fhSendUs);
    kK(row)[i++] = kj(q.fhSeqNo);
    return row;
}

} // namespace t2s

#endif // T2S_QUOTE_ROW_HPP
