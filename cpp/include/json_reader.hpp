/**
 * @file json_reader.hpp
 * @brief Safe-by-default accessor wrapper around rapidjson::Value.
 *
 * Replaces the unchecked rapidjson pattern:
 *
 *     if (!d.HasMember("t")) return;
 *     long long t = d["t"].GetInt64();      // UB if wrong type
 *     double p = std::stod(d["p"].GetString()); // throws on garbage
 *
 * With:
 *
 *     t2s::JsonReader d(value);
 *     auto t = d.int64("t");
 *     auto p = d.priceString("p");
 *     if (d.hasError()) { ++failures; return; }
 *     // safe to unwrap: *t, *p
 *
 * Accessors return std::optional<T>; nullopt on missing key, wrong type,
 * or parse failure. Errors are recorded in the reader on a first-wins
 * basis so log messages point at the root cause. No exceptions in the
 * hot path - priceString catches strtod failures internally via errno.
 *
 * Lifetime: the reader holds a borrowed pointer to the underlying Value.
 * The caller owns the Document and is responsible for ensuring it
 * outlives the reader. String-typed accessors return string_view into
 * the underlying buffer.
 *
 * Performance: each accessor is one branch + optional construction.
 * At t2s scale (10-30 msg/sec) the overhead is invisible.
 */

#ifndef T2S_JSON_READER_HPP
#define T2S_JSON_READER_HPP

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

namespace t2s {

class JsonReader {
public:
    /// Poisoned (empty) reader. All accessors return nullopt.
    JsonReader() noexcept = default;

    /// Bind to a JSON value. If not an object, the reader is poisoned
    /// (hasError() returns true; all accessors return nullopt).
    explicit JsonReader(const rapidjson::Value& v) noexcept {
        if (v.IsObject()) {
            v_ = &v;
        } else {
            err_ = "expected JSON object";
        }
    }

    /// Drill into a nested object. Errors propagate to the returned reader.
    JsonReader obj(const char* key) noexcept {
        JsonReader r;
        if (!v_) { r.err_ = err_; return r; }
        if (!v_->HasMember(key)) {
            setError(std::string("missing object: ") + key);
            r.err_ = err_;
            return r;
        }
        const auto& inner = (*v_)[key];
        if (!inner.IsObject()) {
            setError(std::string("not an object: ") + key);
            r.err_ = err_;
            return r;
        }
        return JsonReader(inner);
    }

    /// String accessor. Returns string_view into the underlying JSON buffer
    /// (zero-copy); valid for the lifetime of the parent Document.
    std::optional<std::string_view> string(const char* key) noexcept {
        if (!v_) return std::nullopt;
        if (!v_->HasMember(key)) {
            setError(std::string("missing string: ") + key);
            return std::nullopt;
        }
        const auto& f = (*v_)[key];
        if (!f.IsString()) {
            setError(std::string("not a string: ") + key);
            return std::nullopt;
        }
        return std::string_view(f.GetString(), f.GetStringLength());
    }

    /// Int64 accessor. rapidjson::IsInt64 accepts both int and int64 values.
    std::optional<std::int64_t> int64(const char* key) noexcept {
        if (!v_) return std::nullopt;
        if (!v_->HasMember(key)) {
            setError(std::string("missing int64: ") + key);
            return std::nullopt;
        }
        const auto& f = (*v_)[key];
        if (!f.IsInt64()) {
            setError(std::string("not an int64: ") + key);
            return std::nullopt;
        }
        return f.GetInt64();
    }

    /// Int accessor (32-bit).
    std::optional<int> integer(const char* key) noexcept {
        if (!v_) return std::nullopt;
        if (!v_->HasMember(key)) {
            setError(std::string("missing int: ") + key);
            return std::nullopt;
        }
        const auto& f = (*v_)[key];
        if (!f.IsInt()) {
            setError(std::string("not an int: ") + key);
            return std::nullopt;
        }
        return f.GetInt();
    }

    /// Bool accessor.
    std::optional<bool> boolean(const char* key) noexcept {
        if (!v_) return std::nullopt;
        if (!v_->HasMember(key)) {
            setError(std::string("missing bool: ") + key);
            return std::nullopt;
        }
        const auto& f = (*v_)[key];
        if (!f.IsBool()) {
            setError(std::string("not a bool: ") + key);
            return std::nullopt;
        }
        return f.GetBool();
    }

    /// Parse a numeric string (e.g. Binance's "p": "50000.12") into a double.
    /// Uses strtod (no exceptions). Returns nullopt and sets error on missing,
    /// non-string, or unparseable content.
    std::optional<double> priceString(const char* key) noexcept {
        if (!v_) return std::nullopt;
        if (!v_->HasMember(key)) {
            setError(std::string("missing string: ") + key);
            return std::nullopt;
        }
        const auto& f = (*v_)[key];
        if (!f.IsString()) {
            setError(std::string("not a string: ") + key);
            return std::nullopt;
        }
        const char* s = f.GetString();
        char* end = nullptr;
        errno = 0;
        double d = std::strtod(s, &end);
        if (end == s || errno == ERANGE) {
            setError(std::string("strtod failed for ") + key + ": '" + s + "'");
            return std::nullopt;
        }
        return d;
    }

    /// Array accessor. Returns a borrowed pointer (lifetime = parent value)
    /// or nullptr on missing/wrong-type.
    const rapidjson::Value* array(const char* key) noexcept {
        if (!v_) return nullptr;
        if (!v_->HasMember(key)) {
            setError(std::string("missing array: ") + key);
            return nullptr;
        }
        const auto& f = (*v_)[key];
        if (!f.IsArray()) {
            setError(std::string("not an array: ") + key);
            return nullptr;
        }
        return &f;
    }

    bool hasError() const noexcept { return !err_.empty(); }
    const std::string& lastError() const noexcept { return err_; }

private:
    const rapidjson::Value* v_ = nullptr;
    std::string err_;

    void setError(std::string msg) noexcept {
        if (err_.empty()) err_ = std::move(msg);
    }
};

/**
 * @brief Parse a [price-string, qty-string] level array into (price, qty).
 *
 * Used for Binance's bid/ask level format: `["50000.12", "0.5"]`.
 * Returns nullopt on any malformation (wrong shape, non-string elements,
 * non-numeric content). Callers typically silently skip malformed levels.
 *
 * Free function rather than a JsonReader method to avoid a circular
 * dependency between json_reader.hpp and order_book_manager.hpp
 * (where PriceLevel lives).
 */
inline std::optional<std::pair<double, double>>
parseLevelPair(const rapidjson::Value& lvl) noexcept {
    if (!lvl.IsArray() || lvl.Size() < 2) return std::nullopt;
    if (!lvl[0].IsString() || !lvl[1].IsString()) return std::nullopt;

    const char* p = lvl[0].GetString();
    const char* q = lvl[1].GetString();
    char* end = nullptr;

    errno = 0;
    double price = std::strtod(p, &end);
    if (end == p || errno == ERANGE) return std::nullopt;

    errno = 0;
    double qty = std::strtod(q, &end);
    if (end == q || errno == ERANGE) return std::nullopt;

    return std::make_pair(price, qty);
}

} // namespace t2s

#endif // T2S_JSON_READER_HPP
