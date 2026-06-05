/**
 * @file trade_row.hpp
 * @brief Pure K-row builder for the trade_binance schema.
 *
 * Extracted from TradeFeedHandler::processMessage as a free function so
 * that row construction can be unit-tested without spinning up the full
 * handler (no WebSocket, no TP connection, no clocks). Production code
 * calls this from processMessage with a placeholder fhSendUs of 0, then
 * patches the slot via KBorrowed after measuring send latency. Tests
 * call it with all fields already known and assert structural and value
 * properties of the resulting K object.
 *
 * The function is intentionally `inline noexcept` and stateless: it
 * takes the KDB epoch offset as a parameter rather than reading it from
 * a class constant, so it can be called from tests that don't link the
 * FH library.
 *
 * Row layout (12 fields) — matches specs/trades-schema.md:
 *   [0]  time             KP timestamp (recvUtcNs - epochOffset)
 *   [1]  sym              symbol
 *   [2]  tradeId          long
 *   [3]  price            float
 *   [4]  qty              float
 *   [5]  buyerIsMaker     bool
 *   [6]  exchEventTimeMs  long
 *   [7]  exchTradeTimeMs  long
 *   [8]  fhRecvTimeUtcNs  long
 *   [9]  fhParseUs        long
 *   [10] fhSendUs         long  (callers may patch this in-place after measuring send latency)
 *   [11] fhSeqNo          long
 *
 * The returned KOwned takes ownership and will release via r0 unless
 * the caller calls release() to transfer ownership to a consuming API
 * (such as the async k(-h, ...) send).
 */

#ifndef T2S_TRADE_ROW_HPP
#define T2S_TRADE_ROW_HPP

#include "k_object.hpp"

#include <string>

extern "C" {
#include "k.h"
}

namespace t2s {

inline KOwned buildTradeRow(
    long long          fhRecvTimeUtcNs,
    const std::string& sym,
    long long          tradeId,
    double             price,
    double             qty,
    bool               buyerIsMaker,
    long long          exchEventTimeMs,
    long long          exchTradeTimeMs,
    long long          fhParseUs,
    long long          fhSendUs,
    long long          fhSeqNo,
    long long          kdbEpochOffsetNs) noexcept
{
    return KOwned(knk(12,
        ktj(-KP, fhRecvTimeUtcNs - kdbEpochOffsetNs),
        ks((S)sym.c_str()),
        kj(tradeId),
        kf(price),
        kf(qty),
        kb(buyerIsMaker),
        kj(exchEventTimeMs),
        kj(exchTradeTimeMs),
        kj(fhRecvTimeUtcNs),
        kj(fhParseUs),
        kj(fhSendUs),
        kj(fhSeqNo)
    ));
}

} // namespace t2s

#endif // T2S_TRADE_ROW_HPP
