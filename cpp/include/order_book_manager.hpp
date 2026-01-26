/**
 * @file order_book_manager.hpp
 * @brief Flat-array order book manager for L5 depth with snapshot reconciliation
 * 
 * Optimized for 100+ symbols with:
 *   - O(1) symbol lookup via index mapping
 *   - Contiguous memory for cache efficiency
 *   - All book state in flat arrays
 *   - Publisher state integrated
 * 
 * Architecture:
 *   - Symbol string → index mapping (one-time lookup)
 *   - All price/qty data in flat arrays [numSymbols * DEPTH]
 *   - State machine per symbol (INIT → SYNCING → VALID)
 *   - L5 quote extraction for kdb+ publication
 * 
 * Memory layout for 100 symbols:
 *   bidPrices_:  100 * 5 * 8 bytes = 4,000 bytes
 *   bidQtys_:    100 * 5 * 8 bytes = 4,000 bytes
 *   askPrices_:  100 * 5 * 8 bytes = 4,000 bytes
 *   askQtys_:    100 * 5 * 8 bytes = 4,000 bytes
 *   Total book data: ~16 KB (fits in L1 cache)
 * 
 * @see docs/decisions/adr-009-L1-Order-Book-Architecture.md
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

/// Maximum delta buffer size before forced snapshot
constexpr size_t MAX_DELTA_BUFFER_SIZE = 1000;

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
 * @brief Manages L5 order books for multiple symbols with flat-array storage
 * 
 * Key design choices:
 *   - Symbol → index mapping for O(1) access
 *   - Flat arrays for all book data (cache-friendly)
 *   - Per-symbol state machine
 *   - Integrated publisher state (last published, timeout)
 */
class OrderBookManager {
public:
    // ========================================================================
    // CONSTRUCTION
    // ========================================================================
    
    /**
     * @brief Initialize manager with symbol list
     * @param symbols List of symbols (uppercase, e.g., "BTCUSDT")
     */
    explicit OrderBookManager(const std::vector<std::string>& symbols) {
        numSymbols_ = static_cast<int>(symbols.size());
        
        // Build symbol ↔ index mapping
        for (int i = 0; i < numSymbols_; ++i) {
            symToIdx_[symbols[i]] = i;
            idxToSym_.push_back(symbols[i]);
        }
        
        // Allocate flat arrays
        const int totalLevels = numSymbols_ * BOOK_DEPTH;
        bidPrices_.resize(totalLevels, 0.0);
        bidQtys_.resize(totalLevels, 0.0);
        askPrices_.resize(totalLevels, 0.0);
        askQtys_.resize(totalLevels, 0.0);
        
        // Per-symbol state
        states_.resize(numSymbols_, BookState::INIT);
        lastUpdateIds_.resize(numSymbols_, 0);
        snapshotUpdateIds_.resize(numSymbols_, 0);
        exchEventTimeMs_.resize(numSymbols_, 0);
        deltaBuffers_.resize(numSymbols_);
        snapshotRequested_.resize(numSymbols_, false);
        
        // Publisher state
        lastPublished_.resize(numSymbols_);
        lastPublishTimes_.resize(numSymbols_);
        hasPublished_.resize(numSymbols_, false);
        
        // Initialize lastPublishTimes to now
        auto now = std::chrono::steady_clock::now();
        for (int i = 0; i < numSymbols_; ++i) {
            lastPublishTimes_[i] = now;
        }
    }
    
    // ========================================================================
    // SYMBOL LOOKUP
    // ========================================================================
    
    /**
     * @brief Get symbol index (returns -1 if not found)
     */
    int getSymbolIndex(const std::string& sym) const {
        auto it = symToIdx_.find(sym);
        return (it != symToIdx_.end()) ? it->second : -1;
    }
    
    /**
     * @brief Get symbol name by index
     */
    const std::string& getSymbol(int idx) const {
        return idxToSym_[idx];
    }
    
    /**
     * @brief Get number of symbols
     */
    int numSymbols() const { return numSymbols_; }
    
    // ========================================================================
    // STATE ACCESS
    // ========================================================================
    
    BookState getState(int idx) const { return states_[idx]; }
    bool isValid(int idx) const { return states_[idx] == BookState::VALID; }
    bool needsSnapshot(int idx) const { return states_[idx] == BookState::INIT && !snapshotRequested_[idx]; }
    void setSnapshotRequested(int idx, bool val) { snapshotRequested_[idx] = val; }
    
    /**
     * @brief Get delta buffer for a symbol (for adding incoming deltas)
     */
    std::deque<BufferedDelta>& getDeltaBuffer(int idx) {
        return deltaBuffers_[idx];
    }
    
    // ========================================================================
    // BOOK OPERATIONS
    // ========================================================================
    
    /**
     * @brief Apply REST snapshot to a symbol's book
     * @param idx Symbol index
     * @param lastUpdateId Snapshot's lastUpdateId from REST API
     * @param bids Bid levels from snapshot (sorted high→low)
     * @param asks Ask levels from snapshot (sorted low→high)
     */
    void applySnapshot(int idx, long long lastUpdateId,
                       const std::vector<PriceLevel>& bids,
                       const std::vector<PriceLevel>& asks) {
        // Clear existing book
        clearBook(idx);
        
        // Copy top BOOK_DEPTH levels
        const int offset = idx * BOOK_DEPTH;
        
        for (size_t i = 0; i < bids.size() && i < BOOK_DEPTH; ++i) {
            bidPrices_[offset + i] = bids[i].price;
            bidQtys_[offset + i] = bids[i].qty;
        }
        
        for (size_t i = 0; i < asks.size() && i < BOOK_DEPTH; ++i) {
            askPrices_[offset + i] = asks[i].price;
            askQtys_[offset + i] = asks[i].qty;
        }
        
        snapshotUpdateIds_[idx] = lastUpdateId;
        lastUpdateIds_[idx] = lastUpdateId;
        states_[idx] = BookState::SYNCING;
    }
    
    /**
     * @brief Apply delta update to a symbol's book
     * @param idx Symbol index
     * @param firstUpdateId Delta's first update ID (U)
     * @param finalUpdateId Delta's final update ID (u)
     * @param bidUpdates Bid level updates (qty=0 means delete)
     * @param askUpdates Ask level updates
     * @param eventTimeMs Exchange event time
     * @return true if applied successfully, false if sequence gap
     */
    bool applyDelta(int idx, long long firstUpdateId, long long finalUpdateId,
                    const std::vector<PriceLevel>& bidUpdates,
                    const std::vector<PriceLevel>& askUpdates,
                    long long eventTimeMs) {
        
        BookState state = states_[idx];
        
        if (state == BookState::SYNCING) {
            // First delta after snapshot
            // Must satisfy: U <= snapshotUpdateId+1 <= u
            if (firstUpdateId > snapshotUpdateIds_[idx] + 1) {
                // Snapshot is too old, need new snapshot
                invalidate(idx, "Snapshot too old");
                return false;
            }
            if (finalUpdateId < snapshotUpdateIds_[idx] + 1) {
                // Delta is stale, skip it
                return true;
            }
            // Transition to VALID
            states_[idx] = BookState::VALID;
        }
        else if (state == BookState::VALID) {
            // Normal operation: expect consecutive sequence
            if (firstUpdateId != lastUpdateIds_[idx] + 1) {
                invalidate(idx, "Sequence gap");
                return false;
            }
        }
        else {
            // INIT or INVALID - shouldn't be applying deltas
            return false;
        }
        
        // Apply bid updates
        for (const auto& upd : bidUpdates) {
            applyLevelUpdate(idx, true, upd);
        }
        
        // Apply ask updates
        for (const auto& upd : askUpdates) {
            applyLevelUpdate(idx, false, upd);
        }
        
        lastUpdateIds_[idx] = finalUpdateId;
        exchEventTimeMs_[idx] = eventTimeMs;
        return true;
    }
    
    /**
     * @brief Reset a symbol's book to INIT state
     */
    void reset(int idx) {
        clearBook(idx);
        states_[idx] = BookState::INIT;
        lastUpdateIds_[idx] = 0;
        snapshotUpdateIds_[idx] = 0;
        exchEventTimeMs_[idx] = 0;
        deltaBuffers_[idx].clear();
        snapshotRequested_[idx] = false;
    }
    
    /**
     * @brief Reset all books (on reconnect)
     */
    void resetAll() {
        for (int i = 0; i < numSymbols_; ++i) {
            reset(i);
        }
    }
    
    /**
     * @brief Mark book as invalid
     */
    void invalidate(int idx, const char* reason) {
        states_[idx] = BookState::INVALID;
        // Caller should log the reason
    }
    
    // ========================================================================
    // L5 EXTRACTION
    // ========================================================================
    
    /**
     * @brief Extract L5 quote for publication
     * @param idx Symbol index
     * @param fhRecvTimeUtcNs Feed handler receive timestamp
     * @param fhSeqNo Feed handler sequence number
     * @return L5Quote ready for kdb+ publication
     */
    L5Quote getL5(int idx, long long fhRecvTimeUtcNs, long long fhSeqNo) const {
        L5Quote q;
        q.sym = idxToSym_[idx];
        
        const int offset = idx * BOOK_DEPTH;
        
        // Copy L5 bid levels
        q.bidPrice1 = bidPrices_[offset + 0]; q.bidQty1 = bidQtys_[offset + 0];
        q.bidPrice2 = bidPrices_[offset + 1]; q.bidQty2 = bidQtys_[offset + 1];
        q.bidPrice3 = bidPrices_[offset + 2]; q.bidQty3 = bidQtys_[offset + 2];
        q.bidPrice4 = bidPrices_[offset + 3]; q.bidQty4 = bidQtys_[offset + 3];
        q.bidPrice5 = bidPrices_[offset + 4]; q.bidQty5 = bidQtys_[offset + 4];
        
        // Copy L5 ask levels
        q.askPrice1 = askPrices_[offset + 0]; q.askQty1 = askQtys_[offset + 0];
        q.askPrice2 = askPrices_[offset + 1]; q.askQty2 = askQtys_[offset + 1];
        q.askPrice3 = askPrices_[offset + 2]; q.askQty3 = askQtys_[offset + 2];
        q.askPrice4 = askPrices_[offset + 3]; q.askQty4 = askQtys_[offset + 3];
        q.askPrice5 = askPrices_[offset + 4]; q.askQty5 = askQtys_[offset + 4];
        
        q.isValid = (states_[idx] == BookState::VALID);
        q.exchEventTimeMs = exchEventTimeMs_[idx];
        q.fhRecvTimeUtcNs = fhRecvTimeUtcNs;
        q.fhSeqNo = fhSeqNo;
        
        return q;
    }
    
    // ========================================================================
    // PUBLISHER LOGIC
    // ========================================================================
    
    /**
     * @brief Check if should publish L5 for a symbol
     * @param idx Symbol index
     * @param current Current L5 quote
     * @return true if should publish
     */
    bool shouldPublish(int idx, const L5Quote& current) {
        auto now = std::chrono::steady_clock::now();
        
        // First publish ever
        if (!hasPublished_[idx]) {
            return true;
        }
        
        // Validity changed
        if (current.isValid != lastPublished_[idx].isValid) {
            return true;
        }
        
        // If invalid, don't spam
        if (!current.isValid) {
            return false;
        }
        
        // Price/qty changed
        if (!current.samePricesAs(lastPublished_[idx])) {
            return true;
        }
        
        // Timeout (publish heartbeat even if unchanged)
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - lastPublishTimes_[idx]).count();
        if (elapsed >= PUBLISH_TIMEOUT_MS) {
            return true;
        }
        
        return false;
    }
    
    /**
     * @brief Record that a quote was published
     */
    void recordPublish(int idx, const L5Quote& quote) {
        lastPublished_[idx] = quote;
        lastPublishTimes_[idx] = std::chrono::steady_clock::now();
        hasPublished_[idx] = true;
    }
    
    /**
     * @brief Check if any symbol needs timeout publish
     * @return Vector of symbol indices that need timeout publish
     */
    std::vector<int> getTimeoutPublishNeeded() {
        std::vector<int> result;
        auto now = std::chrono::steady_clock::now();
        
        for (int i = 0; i < numSymbols_; ++i) {
            if (states_[i] == BookState::VALID && hasPublished_[i]) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - lastPublishTimes_[i]).count();
                if (elapsed >= PUBLISH_TIMEOUT_MS) {
                    result.push_back(i);
                }
            }
        }
        
        return result;
    }

private:
    // ========================================================================
    // SYMBOL MAPPING
    // ========================================================================
    
    int numSymbols_;
    std::unordered_map<std::string, int> symToIdx_;
    std::vector<std::string> idxToSym_;
    
    // ========================================================================
    // BOOK DATA (flat arrays)
    // ========================================================================
    
    // Price and qty arrays: [numSymbols * BOOK_DEPTH]
    // Access: bidPrices_[symIdx * BOOK_DEPTH + level]
    std::vector<double> bidPrices_;     // Bids sorted high→low (index 0 = best bid)
    std::vector<double> bidQtys_;
    std::vector<double> askPrices_;     // Asks sorted low→high (index 0 = best ask)
    std::vector<double> askQtys_;
    
    // ========================================================================
    // STATE (per symbol)
    // ========================================================================
    
    std::vector<BookState> states_;
    std::vector<long long> lastUpdateIds_;
    std::vector<long long> snapshotUpdateIds_;
    std::vector<long long> exchEventTimeMs_;
    std::vector<std::deque<BufferedDelta>> deltaBuffers_;
    std::vector<bool> snapshotRequested_;
    
    // ========================================================================
    // PUBLISHER STATE (per symbol)
    // ========================================================================
    
    std::vector<L5Quote> lastPublished_;
    std::vector<std::chrono::steady_clock::time_point> lastPublishTimes_;
    std::vector<bool> hasPublished_;
    
    // ========================================================================
    // PRIVATE HELPERS
    // ========================================================================
    
    /**
     * @brief Clear a symbol's book to zeros
     */
    void clearBook(int idx) {
        const int offset = idx * BOOK_DEPTH;
        for (int i = 0; i < BOOK_DEPTH; ++i) {
            bidPrices_[offset + i] = 0.0;
            bidQtys_[offset + i] = 0.0;
            askPrices_[offset + i] = 0.0;
            askQtys_[offset + i] = 0.0;
        }
    }
    
    /**
     * @brief Apply a single level update to the book
     * 
     * Binance delta semantics:
     *   - qty > 0: update or insert at this price
     *   - qty = 0: delete this price level
     * 
     * @param idx Symbol index
     * @param isBid true for bid side, false for ask side
     * @param update Price level update
     */
    void applyLevelUpdate(int idx, bool isBid, const PriceLevel& update) {
        const int offset = idx * BOOK_DEPTH;
        double* prices = isBid ? &bidPrices_[offset] : &askPrices_[offset];
        double* qtys = isBid ? &bidQtys_[offset] : &askQtys_[offset];
        
        // Find existing price or insertion point
        int existingIdx = -1;
        int insertIdx = BOOK_DEPTH;  // Default: beyond end
        
        for (int i = 0; i < BOOK_DEPTH; ++i) {
            if (prices[i] == update.price && qtys[i] > 0.0) {
                existingIdx = i;
                break;
            }
            
            // Find insertion point (maintain sort order)
            if (prices[i] == 0.0 || (isBid ? update.price > prices[i] : update.price < prices[i])) {
                if (insertIdx == BOOK_DEPTH) {
                    insertIdx = i;
                }
            }
        }
        
        if (update.qty == 0.0) {
            // DELETE: remove this price level
            if (existingIdx >= 0) {
                // Shift remaining levels up
                for (int i = existingIdx; i < BOOK_DEPTH - 1; ++i) {
                    prices[i] = prices[i + 1];
                    qtys[i] = qtys[i + 1];
                }
                // Clear last level
                prices[BOOK_DEPTH - 1] = 0.0;
                qtys[BOOK_DEPTH - 1] = 0.0;
            }
        } else {
            // UPDATE or INSERT
            if (existingIdx >= 0) {
                // Update existing level
                qtys[existingIdx] = update.qty;
            } else if (insertIdx < BOOK_DEPTH) {
                // Insert new level: shift levels down
                for (int i = BOOK_DEPTH - 1; i > insertIdx; --i) {
                    prices[i] = prices[i - 1];
                    qtys[i] = qtys[i - 1];
                }
                prices[insertIdx] = update.price;
                qtys[insertIdx] = update.qty;
            }
            // else: price would be beyond L5, ignore
        }
    }
};

#endif // ORDER_BOOK_MANAGER_HPP
