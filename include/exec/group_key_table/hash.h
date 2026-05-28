#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace columnar::exec::group_key {
inline void HashCombine(size_t& seed, size_t value) noexcept {
    seed ^= value + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
}

inline size_t HashIntString(int64_t first, size_t second_hash) noexcept {
    size_t seed = 2;
    HashCombine(seed, std::hash<int64_t>{}(first));
    HashCombine(seed, second_hash);
    return seed;
}

inline size_t HashIntIntString(int64_t first, int64_t second, size_t third_hash) noexcept {
    size_t seed = 3;
    HashCombine(seed, std::hash<int64_t>{}(first));
    HashCombine(seed, std::hash<int64_t>{}(second));
    HashCombine(seed, third_hash);
    return seed;
}
}  // namespace columnar::exec::group_key
