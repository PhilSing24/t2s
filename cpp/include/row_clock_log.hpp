/**
 * @file row_clock_log.hpp
 * @brief Log the start and the end of a clock lag (see row_clock.hpp).
 *
 * Kept apart from row_clock.hpp so the rule itself has no dependency.
 */

#ifndef T2S_ROW_CLOCK_LOG_HPP
#define T2S_ROW_CLOCK_LOG_HPP

#include "row_clock.hpp"

#include <spdlog/spdlog.h>

namespace t2s {

inline void logClockEdge(const RowStamp& stamp, const RowClock& clock) {
    if (stamp.edge == ClockEdge::LagStarted) {
        spdlog::warn("CLOCK LAG: the system clock is {} ms behind the exchange (threshold {} ms). "
                     "Rows take `time` from the exchange event time until it is back in step; "
                     "fhRecvTimeUtcNs keeps the clock reading.",
                     stamp.lagMs, clock.thresholdMs());
    } else if (stamp.edge == ClockEdge::LagEnded) {
        spdlog::warn("CLOCK LAG over: the system clock is back in step. {} rows were stamped from the "
                     "exchange time, largest lag {} ms ({} corrected rows since start).",
                     clock.lastEpisodeRows(), clock.lastEpisodeMaxLagMs(), clock.correctedRows());
    }
}

} // namespace t2s

#endif // T2S_ROW_CLOCK_LOG_HPP
