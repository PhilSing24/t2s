/**
 * @file row_clock.hpp
 * @brief Decide a row's `time` when the local clock lags the exchange.
 *
 * A row's `time` is normally the handler's receive time, the same instant
 * as fhRecvTimeUtcNs. After a wake from sleep the system clock can be
 * minutes behind for a few seconds; rows stamped with it would carry a
 * wrong time and, around midnight, land in the wrong partition.
 *
 * Rule, per row: when the exchange event time is AHEAD of the local
 * receive time by more than the threshold (clock_lag_ms in
 * config/shared.json), `time` is taken from the exchange event time.
 * fhRecvTimeUtcNs always keeps the raw clock reading, so
 *
 *     a corrected row is a row whose `time` differs from fhRecvTimeUtcNs
 *
 * and the original receive time is never lost. No schema change.
 *
 * In normal running the receive time is some hundreds of milliseconds
 * AFTER the event time, so an event time seconds ahead can only be a clock
 * that is behind. The opposite case (receive time well after the event
 * time) is indistinguishable from late delivery and is left alone; TP's
 * skew check reports it.
 *
 * Which exchange time:
 *   - a row with its own event time uses it (millisecond resolution);
 *   - a row without one (invalid quote row, backfilled trade) uses the
 *     event time of the most recent event the handler saw.
 * There is no running maximum: one bad event time from the exchange
 * affects that row only, not the rows after it.
 *
 * Pure: no clock is read here, so the tests drive it with any times.
 */

#ifndef T2S_ROW_CLOCK_HPP
#define T2S_ROW_CLOCK_HPP

namespace t2s {

/// Default threshold when config/shared.json has no clock_lag_ms
constexpr long long DEFAULT_CLOCK_LAG_MS = 2000;

/// What happened to the lag state with this row (for the handler's log)
enum class ClockEdge { None, LagStarted, LagEnded };

struct RowStamp {
    long long timeUtcNs = 0;     ///< the row's `time` (Unix epoch ns)
    bool      corrected = false; ///< true: taken from the exchange, differs from the receive time
    long long lagMs     = 0;     ///< exchange time minus receive time (0 when there is no exchange time)
    ClockEdge edge      = ClockEdge::None;
};

/// An event time is usable when positive (0 = unset, kdb+ null long is negative)
inline bool hasEventTime(long long eventTimeMs) noexcept { return eventTimeMs > 0; }

/// The rule itself. eventTimeMs is the exchange time to compare with.
inline RowStamp stampRowTime(long long recvTimeUtcNs, long long eventTimeMs, long long thresholdMs) noexcept {
    RowStamp s;
    s.timeUtcNs = recvTimeUtcNs;
    if (!hasEventTime(eventTimeMs) || thresholdMs <= 0) return s;
    const long long eventNs = eventTimeMs * 1000000LL;
    s.lagMs = (eventNs - recvTimeUtcNs) / 1000000LL;
    if (eventNs - recvTimeUtcNs > thresholdMs * 1000000LL) {
        s.timeUtcNs = eventNs;
        s.corrected = true;
    }
    return s;
}

/**
 * One per handler: remembers the most recent event time, counts the
 * corrected rows and tracks whether the clock is currently lagging.
 * Single-threaded, like the handler's publish path.
 */
class RowClock {
public:
    explicit RowClock(long long thresholdMs = DEFAULT_CLOCK_LAG_MS) noexcept : thresholdMs_(thresholdMs) {}

    /// An event was received (whether or not a row is published for it)
    void noteEvent(long long eventTimeMs) noexcept {
        if (hasEventTime(eventTimeMs)) lastEventMs_ = eventTimeMs;
    }

    /// Stamp a row. ownEventTimeMs: the row's own event time, or 0/null
    /// when it has none (then the most recent event time is used).
    RowStamp stamp(long long recvTimeUtcNs, long long ownEventTimeMs) noexcept {
        noteEvent(ownEventTimeMs);
        const long long ref = hasEventTime(ownEventTimeMs) ? ownEventTimeMs : lastEventMs_;
        RowStamp s = stampRowTime(recvTimeUtcNs, ref, thresholdMs_);
        if (s.corrected) {
            ++correctedRows_;
            ++episodeRows_;
            if (s.lagMs > episodeMaxLagMs_) episodeMaxLagMs_ = s.lagMs;
            if (!lagging_) { lagging_ = true; s.edge = ClockEdge::LagStarted; }
        } else if (lagging_ && hasEventTime(ref) && s.lagMs <= thresholdMs_ / 2) {
            // Back in step. Half the threshold, so a lag hovering at the
            // threshold does not start and end an episode on every row.
            lagging_ = false;
            s.edge = ClockEdge::LagEnded;
            lastEpisodeRows_ = episodeRows_;
            lastEpisodeMaxLagMs_ = episodeMaxLagMs_;
            episodeRows_ = 0;
            episodeMaxLagMs_ = 0;
        }
        return s;
    }

    long long thresholdMs() const noexcept { return thresholdMs_; }
    long long correctedRows() const noexcept { return correctedRows_; }   ///< since the handler started
    bool lagging() const noexcept { return lagging_; }
    long long lastEventMs() const noexcept { return lastEventMs_; }
    /// The episode that just ended (valid on a LagEnded edge)
    long long lastEpisodeRows() const noexcept { return lastEpisodeRows_; }
    long long lastEpisodeMaxLagMs() const noexcept { return lastEpisodeMaxLagMs_; }

private:
    long long thresholdMs_;
    long long lastEventMs_ = 0;
    long long correctedRows_ = 0;
    bool      lagging_ = false;
    long long episodeRows_ = 0, episodeMaxLagMs_ = 0;
    long long lastEpisodeRows_ = 0, lastEpisodeMaxLagMs_ = 0;
};

} // namespace t2s

#endif // T2S_ROW_CLOCK_HPP
