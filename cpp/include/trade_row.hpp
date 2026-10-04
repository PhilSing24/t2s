/**
 * @file trade_row.hpp
 * @brief Pure K-row builders for spot trade_binance and futures trade_binance_fut.
 *
 * Two free functions, both inline noexcept and stateless:
 *
 *   buildTradeRow      - 12-field row for the spot @trade stream payload.
 *                        Carries `tradeId` as the per-symbol sequence id.
 *
 *   buildAggTradeRow   - 15-field row for the futures @aggTrade stream payload.
 *                        Carries `aggTradeId` as the sequence id, plus
 *                        `firstTradeId` and `lastTradeId` describing the
 *                        constituent fills aggregated into this event.
 *
 * Both functions take the KDB epoch offset as a parameter rather than
 * reading from a class constant, so tests can call them without linking
 * the FH library. Both return KOwned so the caller can either release()
 * into a consuming async k() send or let RAII clean up on early return.
 *
 * The slot for fhSendUs is initialised to a placeholder by the production
 * caller and patched in-place via KBorrowed after the send-latency
 * measurement. The slot index DIFFERS between the two row builders
 * (10 for spot, 12 for futures). Callers in processMessage select the
 * correct index based on cfg_.schema. Tests below lock both layouts.
 *
 * Spot row layout (12 fields) - matches specs/trades-schema.md:
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
 *   [10] fhSendUs         long  (patchable in-place)
 *   [11] fhSeqNo          long
 *
 * Futures aggTrade row layout (15 fields) - matches .schema.aggTrade:
 *   [0]  time             KP timestamp
 *   [1]  sym              symbol
 *   [2]  aggTradeId       long
 *   [3]  firstTradeId     long  (new vs spot)
 *   [4]  lastTradeId      long  (new vs spot)
 *   [5]  price            float
 *   [6]  qty              float  `q`: quantity with all the market trades
 *   [7]  qtyExRpi         float  `nq`: "normal quantity without the trades
 *                                involving RPI orders" (Retail Price
 *                                Improvement). In the stream since 2025-12-31:
 *                                https://developers.binance.com/docs/derivatives/change-log
 *                                https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Aggregate-Trade-Streams
 *                                Null if an event arrives without it.
 *   [8]  buyerIsMaker     bool
 *   [9]  exchEventTimeMs  long
 *   [10] exchTradeTimeMs  long
 *   [11] fhRecvTimeUtcNs  long
 *   [12] fhParseUs        long
 *   [13] fhSendUs         long  (patchable in-place)
 *   [14] fhSeqNo          long
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

inline KOwned buildAggTradeRow(
    long long          fhRecvTimeUtcNs,
    const std::string& sym,
    long long          aggTradeId,
    long long          firstTradeId,
    long long          lastTradeId,
    double             price,
    double             qty,
    double             qtyExRpi,
    bool               buyerIsMaker,
    long long          exchEventTimeMs,
    long long          exchTradeTimeMs,
    long long          fhParseUs,
    long long          fhSendUs,
    long long          fhSeqNo,
    long long          kdbEpochOffsetNs) noexcept
{
    return KOwned(knk(15,
        ktj(-KP, fhRecvTimeUtcNs - kdbEpochOffsetNs),
        ks((S)sym.c_str()),
        kj(aggTradeId),
        kj(firstTradeId),
        kj(lastTradeId),
        kf(price),
        kf(qty),
        kf(qtyExRpi),
        kb(buyerIsMaker),
        kj(exchEventTimeMs),
        kj(exchTradeTimeMs),
        kj(fhRecvTimeUtcNs),
        kj(fhParseUs),
        kj(fhSendUs),
        kj(fhSeqNo)
    ));
}

/// Width and fhSendUs position of the two trade layouts
constexpr int TRADE_ROW_WIDTH = 12;
constexpr int TRADE_ROW_SEND_US_IDX = 10;
constexpr int AGG_TRADE_ROW_WIDTH = 15;
constexpr int AGG_TRADE_ROW_SEND_US_IDX = 13;

} // namespace t2s

#endif // T2S_TRADE_ROW_HPP
