/**
 * @file order_book_manager.hpp
 * @brief Full-depth local order books with snapshot reconciliation; publishes L5
 *
 * Each symbol keeps EVERY price level it knows, not just the published top
 * five, so deleting a top-of-book level promotes the next real level
 * instead of leaving an empty slot (review pass 2 item 9).
 *
 * What "every level it knows" means - the snapshot horizon:
 *   A REST snapshot returns at most `snapshotLimit` levels per side. The
 *   diff stream only reports levels that CHANGE, so a level that was
 *   beyond the snapshot's last price and has not changed since is unknown
 *   to us. The worst price of a truncated snapshot is the side's horizon:
 *   levels at or inside it are exact, levels beyond it are not stored. If
 *   the snapshot returned fewer levels than the limit, the whole side is
 *   known and there is no horizon.
 *
 *   When the market moves toward the horizon the known levels run down.
 *   Below `refreshLowWater` known levels the book asks for a background
 *   refresh (wantsRefresh): the live book keeps applying deltas and keeps
 *   publishing, the deltas are also buffered, and when the new snapshot
 *   arrives a shadow book is built from it with the normal sync rule and
 *   swapped in. No invalid row is published. If a side ever has fewer
 *   known levels than the published depth before the refresh lands, the
 *   quote is published invalid (and counted) rather than with a slot that
 *   might be wrong.
 *
 * Storage: one sorted vector of levels per side per symbol, worst price
 * first so the frequent top-of-book inserts and deletes touch the end.
 * Capped at `maxLevels`; trimming the worst level moves the horizon in.
 *
 * State machine per symbol: INIT -> SYNCING -> VALID -> INVALID.
 */

#ifndef ORDER_BOOK_MANAGER_HPP
#define ORDER_BOOK_MANAGER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <deque>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <stdexcept>

// ============================================================================
// CONFIGURATION
// ============================================================================

/// Number of price levels to maintain per side (L5)
constexpr int BOOK_DEPTH = 5;

/// Publish timeout in milliseconds (publish even if no change)
constexpr int PUBLISH_TIMEOUT_MS = 50;

/// Maximum number of deltas buffered per symbol while waiting for a
/// snapshot (100 s of a 100 ms stream). Enforced by bufferDelta(): on
/// overflow the OLDEST delta is dropped and the overflow is counted.
constexpr size_t MAX_DELTA_BUFFER_SIZE = 1000;

/// Sizing of the local book (see the file comment for the horizon).
struct BookConfig {
    std::size_t snapshotLimit   = 1000;  ///< levels per side requested from REST
    std::size_t refreshLowWater = 100;   ///< known levels below which a refresh is wanted
    std::size_t maxLevels       = 4000;  ///< cap on stored levels per side
};

/// What OrderBookManager::onSnapshot did with a snapshot.
enum class SnapshotOutcome {
    SYNCED,           ///< initial sync: snapshot + buffered deltas bridged, book is VALID
    AWAITING_BRIDGE,  ///< initial sync: snapshot applied, waiting for the bridging delta
    SYNC_FAILED,      ///< initial sync: buffered deltas do not bridge (snapshot too old)
    REFRESHED,        ///< background refresh swapped in; the book stayed VALID throughout
    REFRESH_FAILED,   ///< background refresh could not bridge; the live book is untouched
    IGNORED           ///< no snapshot was expected in this state
};

// ============================================================================
// DATA STRUCTURES
// ============================================================================

/**
 * @brief Single price level (price + quantity)
 */
struct PriceLevel {
    double price = 0.0;
    double qty = 0.0;
    
    bool operator==(const PriceLevel& other) const {
        return price == other.price && qty == other.qty;
    }
    
    bool operator!=(const PriceLevel& other) const {
        return !(*this == other);
    }
    
    bool isEmpty() const {
        return price == 0.0 && qty == 0.0;
    }
};

/**
 * @brief L5 quote for kdb+ publication (22 price/qty fields + metadata)
 */
struct L5Quote {
    std::string sym;
    
    // Bids (best to worst: index 0 = best bid)
    double bidPrice1 = 0.0, bidQty1 = 0.0;
    double bidPrice2 = 0.0, bidQty2 = 0.0;
    double bidPrice3 = 0.0, bidQty3 = 0.0;
    double bidPrice4 = 0.0, bidQty4 = 0.0;
    double bidPrice5 = 0.0, bidQty5 = 0.0;
    
    // Asks (best to worst: index 0 = best ask)
    double askPrice1 = 0.0, askQty1 = 0.0;
    double askPrice2 = 0.0, askQty2 = 0.0;
    double askPrice3 = 0.0, askQty3 = 0.0;
    double askPrice4 = 0.0, askQty4 = 0.0;
    double askPrice5 = 0.0, askQty5 = 0.0;
    
    bool isValid = false;
    long long exchEventTimeMs = 0;
    long long fhRecvTimeUtcNs = 0;
    long long fhSeqNo = 0;
    
    // Compare L5 for change detection (price and qty only)
    bool samePricesAs(const L5Quote& other) const {
        return bidPrice1 == other.bidPrice1 && bidQty1 == other.bidQty1 &&
               bidPrice2 == other.bidPrice2 && bidQty2 == other.bidQty2 &&
               bidPrice3 == other.bidPrice3 && bidQty3 == other.bidQty3 &&
               bidPrice4 == other.bidPrice4 && bidQty4 == other.bidQty4 &&
               bidPrice5 == other.bidPrice5 && bidQty5 == other.bidQty5 &&
               askPrice1 == other.askPrice1 && askQty1 == other.askQty1 &&
               askPrice2 == other.askPrice2 && askQty2 == other.askQty2 &&
               askPrice3 == other.askPrice3 && askQty3 == other.askQty3 &&
               askPrice4 == other.askPrice4 && askQty4 == other.askQty4 &&
               askPrice5 == other.askPrice5 && askQty5 == other.askQty5;
    }
};

/**
 * @brief Buffered delta for replay after snapshot
 */
struct BufferedDelta {
    long long firstUpdateId;
    long long finalUpdateId;
    long long eventTimeMs;
    std::vector<PriceLevel> bids;   // Level updates (price, qty) - qty=0 means delete
    std::vector<PriceLevel> asks;
};

/**
 * @brief Order book state machine states
 */
enum class BookState {
    INIT,       // Initial state, buffering deltas
    SYNCING,    // Snapshot applied, replaying buffered deltas
    VALID,      // Normal operation, applying live deltas
    INVALID     // Sequence gap detected, needs rebuild
};

// ============================================================================
// ORDER BOOK MANAGER
// ============================================================================

/**
 * @class OrderBookManager
 * @brief Full-depth order books for multiple symbols, L5 extraction, publisher state
 */
class OrderBookManager {
public:
    // ========================================================================
    // CONSTRUCTION
    // ========================================================================

    /**
     * @param symbols List of symbols (uppercase, e.g., "BTCUSDT")
     * @param cfg     Book sizing (snapshot limit, refresh low-water mark, cap)
     */
    explicit OrderBookManager(const std::vector<std::string>& symbols,
                              BookConfig cfg = BookConfig{})
        : cfg_(cfg) {
        numSymbols_ = static_cast<int>(symbols.size());

        for (int i = 0; i < numSymbols_; ++i) {
            symToIdx_[symbols[i]] = i;
            idxToSym_.push_back(symbols[i]);
        }

        books_.resize(numSymbols_);
        seq_.resize(numSymbols_);
        states_.resize(numSymbols_, BookState::INIT);
        exchEventTimeMs_.resize(numSymbols_, 0);
        deltaBuffers_.resize(numSymbols_);
        snapshotRequested_.resize(numSymbols_, false);
        refreshPending_.resize(numSymbols_, false);
        wasExhausted_.resize(numSymbols_, false);

        lastPublished_.resize(numSymbols_);
        lastPublishTimes_.resize(numSymbols_);
        hasPublished_.resize(numSymbols_, false);

        auto now = std::chrono::steady_clock::now();
        for (int i = 0; i < numSymbols_; ++i) {
            lastPublishTimes_[i] = now;
        }
    }

    // ========================================================================
    // SYMBOL LOOKUP
    // ========================================================================

    int getSymbolIndex(const std::string& sym) const {
        auto it = symToIdx_.find(sym);
        return (it != symToIdx_.end()) ? it->second : -1;
    }
    const std::string& getSymbol(int idx) const { return idxToSym_[idx]; }
    int numSymbols() const { return numSymbols_; }

    // ========================================================================
    // STATE ACCESS
    // ========================================================================

    BookState getState(int idx) const { return states_[idx]; }
    /// Sequence-valid: every delta since the snapshot has been applied.
    bool isValid(int idx) const { return states_[idx] == BookState::VALID; }
    bool needsSnapshot(int idx) const { return states_[idx] == BookState::INIT && !snapshotRequested_[idx]; }
    void setSnapshotRequested(int idx, bool val) { snapshotRequested_[idx] = val; }

    /// Known levels on a side (all of them are inside the horizon).
    std::size_t knownLevels(int idx, bool isBid) const {
        return (isBid ? books_[idx].bid : books_[idx].ask).levels.size();
    }
    /// True if the side's snapshot was truncated, i.e. deeper levels are unknown.
    bool hasHorizon(int idx, bool isBid) const {
        return (isBid ? books_[idx].bid : books_[idx].ask).bounded;
    }

    /**
     * @brief A side has fewer known levels than the published depth while
     *        the exchange may have more beyond the horizon: L5 cannot be
     *        guaranteed, so the quote is reported invalid.
     */
    bool depthExhausted(int idx) const {
        return books_[idx].bid.below(BOOK_DEPTH) || books_[idx].ask.below(BOOK_DEPTH);
    }

    // ========================================================================
    // BACKGROUND DEPTH REFRESH
    // ========================================================================

    /// VALID book whose known depth is running low and no refresh is in flight.
    bool wantsRefresh(int idx) const {
        return states_[idx] == BookState::VALID && !refreshPending_[idx] &&
               (books_[idx].bid.below(cfg_.refreshLowWater) ||
                books_[idx].ask.below(cfg_.refreshLowWater));
    }

    /// The caller is about to request a refresh snapshot: start buffering.
    void beginRefresh(int idx) {
        refreshPending_[idx] = true;
        snapshotRequested_[idx] = true;
        deltaBuffers_[idx].clear();
    }

    /// The refresh request failed: stop buffering, the live book carries on.
    void cancelRefresh(int idx) {
        if (!refreshPending_[idx]) return;
        refreshPending_[idx] = false;
        snapshotRequested_[idx] = false;
        deltaBuffers_[idx].clear();
        ++refreshFailures_;
    }

    bool refreshPending(int idx) const { return refreshPending_[idx]; }

    // -- counters (all symbols, since start) --------------------------------
    long long bufferOverflows() const { return bufferOverflows_; }
    long long depthRefreshes() const { return depthRefreshes_; }
    long long refreshFailures() const { return refreshFailures_; }
    long long depthExhaustedEvents() const { return depthExhaustedEvents_; }

    // ========================================================================
    // DELTA BUFFER
    // ========================================================================

    /**
     * @brief Buffer a delta while the book waits for its snapshot.
     *
     * The buffer is capped at MAX_DELTA_BUFFER_SIZE. On overflow the oldest
     * delta is dropped and counted. That is safe: a snapshot fetched later
     * is newer than the dropped deltas, which would have been skipped as
     * stale anyway. If a snapshot older than the oldest remaining delta
     * does arrive, the bridge rule ("Snapshot too old") rejects it and the
     * book resyncs - nothing is applied over a hole.
     *
     * @return false if the buffer overflowed (oldest delta dropped)
     */
    bool bufferDelta(int idx, BufferedDelta delta) {
        auto& buf = deltaBuffers_[idx];
        buf.push_back(std::move(delta));
        if (buf.size() > MAX_DELTA_BUFFER_SIZE) {
            buf.pop_front();
            ++bufferOverflows_;
            return false;
        }
        return true;
    }

    std::deque<BufferedDelta>& getDeltaBuffer(int idx) { return deltaBuffers_[idx]; }

    // ========================================================================
    // BOOK OPERATIONS
    // ========================================================================

    /**
     * @brief Load a REST snapshot into a symbol's book (state -> SYNCING)
     * @param bids Bid levels from the snapshot (best first)
     * @param asks Ask levels from the snapshot (best first)
     *
     * All levels are kept. A side that came back with snapshotLimit levels
     * was truncated by the exchange and gets a horizon at its worst price.
     */
    void applySnapshot(int idx, long long lastUpdateId,
                       const std::vector<PriceLevel>& bids,
                       const std::vector<PriceLevel>& asks) {
        books_[idx].bid.load(true, bids, cfg_.snapshotLimit, cfg_.maxLevels);
        books_[idx].ask.load(false, asks, cfg_.snapshotLimit, cfg_.maxLevels);
        seq_[idx] = Seq{true, lastUpdateId, lastUpdateId};
        states_[idx] = BookState::SYNCING;
        wasExhausted_[idx] = false;
    }

    /**
     * @brief A snapshot result arrived: initial sync or background refresh.
     *
     * Initial sync (state INIT): load the snapshot and replay the buffered
     * deltas through the sync rule.
     *
     * Refresh (state VALID with a refresh pending): build a shadow book
     * from the snapshot, replay the deltas buffered since the request, and
     * swap it in. The live book is untouched if the deltas do not bridge.
     */
    SnapshotOutcome onSnapshot(int idx, long long lastUpdateId,
                               const std::vector<PriceLevel>& bids,
                               const std::vector<PriceLevel>& asks) {
        snapshotRequested_[idx] = false;

        if (states_[idx] == BookState::VALID && refreshPending_[idx]) {
            return finishRefresh(idx, lastUpdateId, bids, asks);
        }
        refreshPending_[idx] = false;
        if (states_[idx] != BookState::INIT) {
            return SnapshotOutcome::IGNORED;
        }

        applySnapshot(idx, lastUpdateId, bids, asks);
        auto& buf = deltaBuffers_[idx];
        bool failed = false;
        for (const auto& d : buf) {
            if (!applyDelta(idx, d.firstUpdateId, d.finalUpdateId, d.bids, d.asks, d.eventTimeMs)) {
                failed = true;
                break;
            }
        }
        buf.clear();
        if (failed) return SnapshotOutcome::SYNC_FAILED;
        return states_[idx] == BookState::VALID ? SnapshotOutcome::SYNCED
                                                : SnapshotOutcome::AWAITING_BRIDGE;
    }

    /**
     * @brief Apply delta update to a symbol's book
     * @param firstUpdateId Delta's first update ID (U)
     * @param finalUpdateId Delta's final update ID (u)
     * @param bidUpdates Bid level updates (qty=0 means delete)
     * @return true if applied or skipped as stale, false on a sequence gap
     *         (the book is then INVALID and must be rebuilt)
     */
    bool applyDelta(int idx, long long firstUpdateId, long long finalUpdateId,
                    const std::vector<PriceLevel>& bidUpdates,
                    const std::vector<PriceLevel>& askUpdates,
                    long long eventTimeMs) {
        BookState state = states_[idx];
        if (state != BookState::SYNCING && state != BookState::VALID) {
            return false;   // INIT or INVALID - shouldn't be applying deltas
        }

        Verdict v = checkSequence(seq_[idx], firstUpdateId, finalUpdateId);
        if (v == Verdict::FAIL) {
            invalidate(idx, "Sequence gap");
            return false;
        }
        if (v == Verdict::APPLY) {
            applyLevels(books_[idx], bidUpdates, askUpdates);
            seq_[idx].lastId = finalUpdateId;
            exchEventTimeMs_[idx] = eventTimeMs;
            states_[idx] = BookState::VALID;

            bool exhausted = depthExhausted(idx);
            if (exhausted && !wasExhausted_[idx]) ++depthExhaustedEvents_;
            wasExhausted_[idx] = exhausted;
        }

        // A refresh is in flight: keep a copy for the shadow book.
        if (refreshPending_[idx]) {
            bufferDelta(idx, BufferedDelta{firstUpdateId, finalUpdateId, eventTimeMs,
                                           bidUpdates, askUpdates});
        }
        return true;
    }

    /// Reset a symbol's book to INIT state
    void reset(int idx) {
        books_[idx] = Book{};
        states_[idx] = BookState::INIT;
        seq_[idx] = Seq{};
        exchEventTimeMs_[idx] = 0;
        deltaBuffers_[idx].clear();
        snapshotRequested_[idx] = false;
        refreshPending_[idx] = false;
        wasExhausted_[idx] = false;
    }

    /// Reset all books (on reconnect)
    void resetAll() {
        for (int i = 0; i < numSymbols_; ++i) reset(i);
    }

    /// Mark book as invalid (caller logs the reason)
    void invalidate(int idx, const char* /*reason*/) {
        states_[idx] = BookState::INVALID;
    }

    // ========================================================================
    // L5 EXTRACTION
    // ========================================================================

    /**
     * @brief Extract L5 quote for publication
     *
     * isValid is true only when the book is sequence-valid AND both sides
     * can guarantee their top BOOK_DEPTH levels (see depthExhausted). An
     * invalid quote carries no levels. Slots beyond the exchange's real
     * depth (a genuinely thin book) are zero.
     */
    L5Quote getL5(int idx, long long fhRecvTimeUtcNs, long long fhSeqNo) const {
        L5Quote q;
        q.sym = idxToSym_[idx];
        q.isValid = (states_[idx] == BookState::VALID) && !depthExhausted(idx);
        q.exchEventTimeMs = exchEventTimeMs_[idx];
        q.fhRecvTimeUtcNs = fhRecvTimeUtcNs;
        q.fhSeqNo = fhSeqNo;

        if (states_[idx] != BookState::VALID && states_[idx] != BookState::SYNCING) return q;
        if (states_[idx] == BookState::VALID && !q.isValid) return q;

        double* bp[BOOK_DEPTH] = {&q.bidPrice1, &q.bidPrice2, &q.bidPrice3, &q.bidPrice4, &q.bidPrice5};
        double* bq[BOOK_DEPTH] = {&q.bidQty1, &q.bidQty2, &q.bidQty3, &q.bidQty4, &q.bidQty5};
        double* ap[BOOK_DEPTH] = {&q.askPrice1, &q.askPrice2, &q.askPrice3, &q.askPrice4, &q.askPrice5};
        double* aq[BOOK_DEPTH] = {&q.askQty1, &q.askQty2, &q.askQty3, &q.askQty4, &q.askQty5};

        const auto& bl = books_[idx].bid.levels;
        const auto& al = books_[idx].ask.levels;
        for (int i = 0; i < BOOK_DEPTH; ++i) {
            if (static_cast<std::size_t>(i) < bl.size()) {
                const PriceLevel& l = bl[bl.size() - 1 - i];   // best is at the back
                *bp[i] = l.price; *bq[i] = l.qty;
            }
            if (static_cast<std::size_t>(i) < al.size()) {
                const PriceLevel& l = al[al.size() - 1 - i];
                *ap[i] = l.price; *aq[i] = l.qty;
            }
        }
        return q;
    }

    // ========================================================================
    // PUBLISHER LOGIC
    // ========================================================================

    /// Should the current quote be published? (first, validity change, change, heartbeat)
    bool shouldPublish(int idx, const L5Quote& current) {
        auto now = std::chrono::steady_clock::now();

        if (!hasPublished_[idx]) return true;
        if (current.isValid != lastPublished_[idx].isValid) return true;
        if (!current.isValid) return false;                         // don't spam invalid rows
        if (!current.samePricesAs(lastPublished_[idx])) return true;

        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - lastPublishTimes_[idx]).count();
        return elapsed >= PUBLISH_TIMEOUT_MS;
    }

    void recordPublish(int idx, const L5Quote& quote) {
        lastPublished_[idx] = quote;
        lastPublishTimes_[idx] = std::chrono::steady_clock::now();
        hasPublished_[idx] = true;
    }

    /// Symbols whose last publish is older than the heartbeat timeout
    std::vector<int> getTimeoutPublishNeeded() {
        std::vector<int> result;
        auto now = std::chrono::steady_clock::now();
        for (int i = 0; i < numSymbols_; ++i) {
            if (states_[i] == BookState::VALID && hasPublished_[i]) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - lastPublishTimes_[i]).count();
                if (elapsed >= PUBLISH_TIMEOUT_MS) result.push_back(i);
            }
        }
        return result;
    }

private:
    // ========================================================================
    // ONE SIDE OF ONE BOOK
    // ========================================================================

    /// true if price a is strictly better than price b on this side
    static bool better(bool isBid, double a, double b) { return isBid ? a > b : a < b; }

    struct Side {
        std::vector<PriceLevel> levels;   ///< sorted worst -> best (best at the back)
        bool bounded = false;             ///< deeper levels exist that we do not know
        double horizon = 0.0;             ///< worst known price (meaningful if bounded)

        /// fewer than n known levels while more may exist beyond the horizon
        bool below(std::size_t n) const { return bounded && levels.size() < n; }

        bool inHorizon(bool isBid, double price) const {
            return !bounded || !better(isBid, horizon, price);
        }

        void trim(std::size_t maxLevels) {
            if (levels.size() <= maxLevels) return;
            levels.erase(levels.begin(), levels.begin() + (levels.size() - maxLevels));
            bounded = true;
            horizon = levels.front().price;
        }

        void load(bool isBid, const std::vector<PriceLevel>& snapshot,
                  std::size_t snapshotLimit, std::size_t maxLevels) {
            levels.clear();
            levels.reserve(snapshot.size() + 64);
            for (const auto& l : snapshot) {
                if (l.qty > 0.0) levels.push_back(l);
            }
            std::sort(levels.begin(), levels.end(),
                      [isBid](const PriceLevel& a, const PriceLevel& b) {
                          return better(isBid, b.price, a.price);
                      });
            bounded = snapshot.size() >= snapshotLimit;
            horizon = (bounded && !levels.empty()) ? levels.front().price : 0.0;
            if (levels.empty()) bounded = false;
            trim(maxLevels);
        }

        /// Binance semantics: qty is absolute; qty 0 removes the level.
        void update(bool isBid, const PriceLevel& u, std::size_t maxLevels) {
            auto it = std::lower_bound(levels.begin(), levels.end(), u.price,
                [isBid](const PriceLevel& l, double p) { return better(isBid, p, l.price); });
            bool found = (it != levels.end() && it->price == u.price);

            if (u.qty == 0.0) {
                if (found) levels.erase(it);     // deleting an unknown level is normal
                return;
            }
            if (found) { it->qty = u.qty; return; }
            if (!inHorizon(isBid, u.price)) return;   // beyond the horizon: not tracked
            levels.insert(it, u);
            trim(maxLevels);
        }
    };

    struct Book { Side bid; Side ask; };

    void applyLevels(Book& b, const std::vector<PriceLevel>& bidUpdates,
                     const std::vector<PriceLevel>& askUpdates) const {
        for (const auto& u : bidUpdates) b.bid.update(true, u, cfg_.maxLevels);
        for (const auto& u : askUpdates) b.ask.update(false, u, cfg_.maxLevels);
    }

    // ========================================================================
    // SEQUENCING
    // ========================================================================

    /// Update-id bookkeeping of one book (live or shadow).
    struct Seq {
        bool needBridge = false;      ///< snapshot loaded, first event not yet seen
        long long snapshotId = 0;     ///< lastUpdateId of that snapshot
        long long lastId = 0;         ///< update id the book is at
    };

    enum class Verdict { SKIP, APPLY, FAIL };

    /**
     * Binance spot rule ("How to manage a local order book correctly"):
     *   first event after a snapshot:  U <= lastUpdateId+1 <= u
     *   afterwards:  u < lastId -> stale, skip;  U > lastId+1 -> gap;
     *                otherwise apply (payload quantities are absolute, so
     *                an overlapping event is safe to re-apply).
     */
    static Verdict checkSequence(Seq& s, long long U, long long u) {
        if (s.needBridge) {
            if (u < s.snapshotId + 1) return Verdict::SKIP;      // older than the snapshot
            if (U > s.snapshotId + 1) return Verdict::FAIL;      // snapshot too old
            s.needBridge = false;
            return Verdict::APPLY;
        }
        if (u < s.lastId) return Verdict::SKIP;
        if (U > s.lastId + 1) return Verdict::FAIL;
        return Verdict::APPLY;
    }

    SnapshotOutcome finishRefresh(int idx, long long lastUpdateId,
                                  const std::vector<PriceLevel>& bids,
                                  const std::vector<PriceLevel>& asks) {
        refreshPending_[idx] = false;

        Book shadow;
        shadow.bid.load(true, bids, cfg_.snapshotLimit, cfg_.maxLevels);
        shadow.ask.load(false, asks, cfg_.snapshotLimit, cfg_.maxLevels);
        Seq sseq{true, lastUpdateId, lastUpdateId};

        auto& buf = deltaBuffers_[idx];
        bool failed = false;
        for (const auto& d : buf) {
            Verdict v = checkSequence(sseq, d.firstUpdateId, d.finalUpdateId);
            if (v == Verdict::FAIL) { failed = true; break; }
            if (v == Verdict::APPLY) {
                applyLevels(shadow, d.bids, d.asks);
                sseq.lastId = d.finalUpdateId;
            }
        }
        buf.clear();

        if (failed) {                 // hole between snapshot and buffered deltas
            ++refreshFailures_;
            return SnapshotOutcome::REFRESH_FAILED;
        }

        // If no buffered delta bridged, the snapshot is ahead of the stream:
        // sseq.needBridge stays set and the next live delta must bridge it.
        books_[idx] = std::move(shadow);
        seq_[idx] = sseq;
        wasExhausted_[idx] = depthExhausted(idx);
        ++depthRefreshes_;
        return SnapshotOutcome::REFRESHED;
    }

    // ========================================================================
    // MEMBERS
    // ========================================================================

    BookConfig cfg_;
    int numSymbols_;
    std::unordered_map<std::string, int> symToIdx_;
    std::vector<std::string> idxToSym_;

    std::vector<Book> books_;
    std::vector<Seq> seq_;
    std::vector<BookState> states_;
    std::vector<long long> exchEventTimeMs_;
    std::vector<std::deque<BufferedDelta>> deltaBuffers_;
    std::vector<bool> snapshotRequested_;
    std::vector<bool> refreshPending_;
    std::vector<bool> wasExhausted_;

    long long bufferOverflows_ = 0;
    long long depthRefreshes_ = 0;
    long long refreshFailures_ = 0;
    long long depthExhaustedEvents_ = 0;

    std::vector<L5Quote> lastPublished_;
    std::vector<std::chrono::steady_clock::time_point> lastPublishTimes_;
    std::vector<bool> hasPublished_;
};

#endif // ORDER_BOOK_MANAGER_HPP
