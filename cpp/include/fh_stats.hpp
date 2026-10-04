/**
 * @file fh_stats.hpp
 * @brief Send a feed handler's counters to the tickerplant.
 *
 * Calls .tp.fhStats[table; names; values] asynchronously. TP keeps the
 * latest report per table and shows it in .health[] and status.sh, so
 * exchange-side events (book gaps, resyncs, snapshot failures, missed
 * trade ids) are visible without reading the handler's log.
 * Counters are cumulative since the handler process started.
 */

#ifndef T2S_FH_STATS_HPP
#define T2S_FH_STATS_HPP

#include <string>
#include <utility>
#include <vector>

extern "C" {
#include "k.h"
}

namespace t2s {

using FhStats = std::vector<std::pair<const char*, long long>>;

/// Fire-and-forget; a lost TP connection is detected by the next row publish.
inline void sendFhStats(int tpHandle, const std::string& table, const FhStats& stats) {
    if (tpHandle <= 0) return;
    K names = ktn(KS, static_cast<J>(stats.size()));
    K vals  = ktn(KJ, static_cast<J>(stats.size()));
    for (std::size_t i = 0; i < stats.size(); ++i) {
        kS(names)[i] = ss(const_cast<S>(stats[i].first));
        kJ(vals)[i]  = stats[i].second;
    }
    k(-tpHandle, (S)".tp.fhStats", ks(const_cast<S>(table.c_str())), names, vals, (K)0);
}

} // namespace t2s

#endif // T2S_FH_STATS_HPP
