/**
 * @file k_object.hpp
 * @brief RAII wrappers for the kdb+ C API K type.
 *
 * Provides two distinct types that make K-object ownership explicit at
 * every callsite:
 *
 *   KOwned    - We own this K and must release it (via r0) when done.
 *               Move-only. Releases on destruction unless ownership is
 *               transferred out via release().
 *
 *   KBorrowed - A view of a K we DO NOT own (e.g. kK(list)[i]).
 *               No destructor; just a typed pointer that prevents us
 *               from accidentally calling r0 on a borrowed reference.
 *
 * Why two types instead of one std::unique_ptr<...>: a single owning type
 * over-releases on borrowed refs from kK(list)[i] (the list owns those,
 * not us). The two-type split lets the type system enforce the borrowed
 * vs owned distinction at compile time.
 *
 * Common patterns:
 *
 *   // Build an owned row. knk consumes its varargs, so the inline
 *   // kj/ks/kf constructions don't need wrapping.
 *   t2s::KOwned row(knk(3, kj(1), ks((S)"sym"), kf(3.14)));
 *
 *   // Borrowed mutation of a list element.
 *   t2s::KBorrowed elem(kK(row.get())[0]);
 *   elem.get()->j = 42;
 *
 *   // Async send (negative handle) consumes its K args. Use release()
 *   // to transfer ownership out of the wrapper.
 *   k(-handle, (S)".u.upd", ks((S)"table"), row.release(), (K)0);
 *
 *   // Sync call (positive handle) does NOT consume args. Use get().
 *   t2s::KOwned result(k(handle, (S)"f", row.get(), (K)0));
 */

#ifndef T2S_K_OBJECT_HPP
#define T2S_K_OBJECT_HPP

extern "C" {
#include "k.h"
}

namespace t2s {

/**
 * @class KOwned
 * @brief RAII handle for an owned K. Releases via r0 on destruction.
 *
 * Move-only. Copying would require r1-based sharing, which we make
 * explicit through the share() free function rather than implicit
 * via copy semantics.
 */
class KOwned {
public:
    KOwned() noexcept = default;

    /// Take ownership of an existing K. The caller must have a reference
    /// to release (e.g. fresh from knk/kj/ks/r1/sync k()).
    explicit KOwned(K k) noexcept : k_(k) {}

    ~KOwned() noexcept {
        if (k_) r0(k_);
    }

    KOwned(KOwned&& o) noexcept : k_(o.k_) { o.k_ = nullptr; }
    KOwned& operator=(KOwned&& o) noexcept {
        if (this != &o) {
            if (k_) r0(k_);
            k_ = o.k_;
            o.k_ = nullptr;
        }
        return *this;
    }

    KOwned(const KOwned&) = delete;
    KOwned& operator=(const KOwned&) = delete;

    /// Borrowed view of the underlying pointer. Do NOT call r0 on this.
    K get() const noexcept { return k_; }

    /// Transfer ownership out. Wrapper becomes empty.
    /// Use when passing to APIs that consume their arguments:
    ///   - knk(n, ...) consumes its varargs
    ///   - k(-handle, ...) (async send) consumes its K args
    K release() noexcept {
        K r = k_;
        k_ = nullptr;
        return r;
    }

    explicit operator bool() const noexcept { return k_ != nullptr; }

private:
    K k_ = nullptr;
};

/**
 * @class KBorrowed
 * @brief Typed view of a K we do not own.
 *
 * No destructor. Used for kK(list)[i] (element borrowed from a parent
 * list) and similar borrowed refs. If you need to extend the lifetime
 * of a borrowed ref, promote it via share().
 */
class KBorrowed {
public:
    KBorrowed() noexcept = default;
    explicit KBorrowed(K k) noexcept : k_(k) {}

    K get() const noexcept { return k_; }
    explicit operator bool() const noexcept { return k_ != nullptr; }

private:
    K k_ = nullptr;
};

/**
 * @brief Promote a borrowed ref to an owned one via r1.
 *
 * Rare in t2s code today; included for completeness.
 */
inline KOwned share(KBorrowed b) noexcept {
    return b ? KOwned(r1(b.get())) : KOwned();
}

} // namespace t2s

#endif // T2S_K_OBJECT_HPP
