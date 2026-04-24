#pragma once

#include <util/bit_vector.h>
#include <util/macro.h>

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

void BitPack(const uint64_t* src, size_t n, uint8_t bit_width, uint8_t* out);

void BitUnpack(const uint8_t* src, size_t src_size, size_t n, uint8_t bit_width, uint64_t* dst);

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
