#pragma once

#include <util/bit_vector.h>
#include <util/macro.h>

#include <algorithm>
#include <array>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

namespace columnar::core::encoding {
constexpr uint8_t kBitPackingMaxWidth = 56;

inline size_t BitPackedSize(size_t n, uint8_t bit_width) {
    return (n * static_cast<size_t>(bit_width) + 7) / 8;
}

template <std::unsigned_integral T>
inline uint8_t BitWidth(T value) {
    uint8_t w = 0;
    while (value > 0) {
        ++w;
        value >>= 1;
    }
    return w;
}

template <size_t N>
constexpr std::array<std::array<uint8_t, N>, 256> MakeUnpackTable(uint8_t bit_width) {
    std::array<std::array<uint8_t, N>, 256> table = {};
    uint8_t mask = static_cast<uint8_t>((static_cast<uint16_t>(1) << bit_width) - 1);
    for (size_t byte = 0; byte < table.size(); ++byte) {
        for (size_t i = 0; i < N; ++i) {
            table[byte][i] = static_cast<uint8_t>((byte >> (i * bit_width)) & mask);
        }
    }
    return table;
}

inline constexpr auto kUnpackTable1 = MakeUnpackTable<8>(1);
inline constexpr auto kUnpackTable2 = MakeUnpackTable<4>(2);
inline constexpr auto kUnpackTable4 = MakeUnpackTable<2>(4);

template <std::integral T, size_t N>
void BitUnpackWithTable(const uint8_t* src, size_t n, T offset, T* dst,
                        const std::array<std::array<uint8_t, N>, 256>& table) {
    size_t i = 0;
    size_t j = 0;
    uint64_t base = static_cast<uint64_t>(offset);
    while (i + N <= n) {
        const std::array<uint8_t, N>& row = table[src[j++]];
        for (size_t k = 0; k < N; ++k) {
            dst[i + k] = static_cast<T>(base + row[k]);
        }
        i += N;
    }
    if (i < n) {
        const std::array<uint8_t, N>& row = table[src[j]];
        for (size_t k = 0; i < n; ++i, ++k) {
            dst[i] = static_cast<T>(base + row[k]);
        }
    }
}

template <std::integral T>
void BitPackWithOffset(const T* src, size_t n, T offset, uint8_t bit_width, uint8_t* out) {
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    if (bit_width == 0 || n == 0) {
        return;
    }

    if (bit_width % 8 == 0) {
        size_t bytes = bit_width / 8;
        for (size_t i = 0; i < n; ++i) {
            uint64_t v = static_cast<uint64_t>(src[i]) - static_cast<uint64_t>(offset);
            std::memcpy(out + i * bytes, &v, bytes);
        }
        return;
    }

    uint64_t mask = (static_cast<uint64_t>(1) << bit_width) - 1;
    uint64_t res = 0;
    uint32_t res_bits = 0;
    size_t ind = 0;
    for (size_t i = 0; i < n; ++i) {
        uint64_t v = static_cast<uint64_t>(src[i]) - static_cast<uint64_t>(offset);
        res |= (v & mask) << res_bits;
        res_bits += bit_width;
        while (res_bits >= 8) {
            out[ind++] = static_cast<uint8_t>(res);
            res >>= 8;
            res_bits -= 8;
        }
    }
    if (res_bits > 0) {
        out[ind] = static_cast<uint8_t>(res);
    }
}

template <std::integral T>
void BitUnpackWithOffset(const uint8_t* src, size_t src_size, size_t n, uint8_t bit_width, T offset,
                         T* dst) {
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    if (src_size < BitPackedSize(n, bit_width)) {
        THROW_RUNTIME_ERROR("Input is too small");
    }
    if (n == 0) {
        return;
    }
    if (bit_width == 0) {
        std::fill(dst, dst + n, offset);
        return;
    }

    if (bit_width == 1) {
        BitUnpackWithTable(src, n, offset, dst, kUnpackTable1);
        return;
    }
    if (bit_width == 2) {
        BitUnpackWithTable(src, n, offset, dst, kUnpackTable2);
        return;
    }
    if (bit_width == 4) {
        BitUnpackWithTable(src, n, offset, dst, kUnpackTable4);
        return;
    }

    if (bit_width % 8 == 0) {
        size_t bytes = bit_width / 8;
        for (size_t i = 0; i < n; ++i) {
            uint64_t v = 0;
            std::memcpy(&v, src + i * bytes, bytes);
            dst[i] = static_cast<T>(static_cast<uint64_t>(offset) + v);
        }
        return;
    }

    uint64_t mask = (static_cast<uint64_t>(1) << bit_width) - 1;
    uint64_t res = 0;
    uint32_t res_bits = 0;
    size_t ind = 0;
    for (size_t i = 0; i < n; ++i) {
        while (res_bits < bit_width) {
            res |= static_cast<uint64_t>(src[ind++]) << res_bits;
            res_bits += 8;
        }
        dst[i] = static_cast<T>(static_cast<uint64_t>(offset) + (res & mask));
        res >>= bit_width;
        res_bits -= bit_width;
    }
}

inline void PackBitVector(const util::BitVector& bits, uint8_t* out) {
    size_t packed_size = BitPackedSize(bits.Size(), 1);
    if (packed_size > 0) {
        std::memcpy(out, bits.GetData().data(), packed_size);
    }
}

inline util::BitVector UnpackBitVector(const uint8_t* src, size_t src_size, size_t n) {
    size_t packed = BitPackedSize(n, 1);
    if (src_size < packed) {
        THROW_RUNTIME_ERROR("Input is too small");
    }
    std::vector<uint64_t> words((n + 63) / 64, 0);
    if (packed > 0) {
        std::memcpy(words.data(), src, packed);
    }
    return util::BitVector(std::move(words), n);
}
}  // namespace columnar::core::encoding
