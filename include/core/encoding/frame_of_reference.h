#pragma once

#include <core/encoding/bit_packing.h>
#include <util/macro.h>
#include <util/stream_helper.h>

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <istream>
#include <ostream>
#include <vector>

namespace columnar::core::encoding {
template <std::integral T>
void EncodeFOR(std::ostream& os, const T* src, size_t n) {
    T mn = n > 0 ? src[0] : T(0);
    T mx = mn;
    for (size_t i = 1; i < n; ++i) {
        if (src[i] < mn) {
            mn = src[i];
        }
        if (src[i] > mx) {
            mx = src[i];
        }
    }

    uint8_t bit_width = BitWidth(static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn));
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    util::Write<T>(os, mn);
    util::Write<uint8_t>(os, bit_width);
    if (bit_width == 0 || n == 0) {
        return;
    }
    std::vector<uint64_t> remain(n);
    for (size_t i = 0; i < n; ++i) {
        remain[i] = static_cast<uint64_t>(src[i]) - static_cast<uint64_t>(mn);
    }
    std::vector<uint8_t> packed(BitPackedSize(n, bit_width));
    BitPack(remain.data(), n, bit_width, packed.data());
    util::WriteRaw(os, packed.data(), packed.size());
}

template <std::integral T>
std::vector<T> DecodeFOR(std::istream& is, size_t n) {
    auto mn = util::Read<T>(is);
    auto bit_width = util::Read<uint8_t>(is);
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    std::vector<T> out(n);
    if (bit_width == 0) {
        std::fill(out.begin(), out.end(), mn);
        return out;
    }
    size_t packed_size = BitPackedSize(n, bit_width);
    std::vector<uint8_t> packed(packed_size);
    util::ReadRaw(is, packed.data(), packed_size);
    std::vector<uint64_t> remains(n);
    BitUnpack(packed.data(), packed_size, n, bit_width, remains.data());
    for (size_t i = 0; i < n; ++i) {
        out[i] = static_cast<T>(static_cast<uint64_t>(mn) + remains[i]);
    }
    return out;
}
}  // namespace columnar::core::encoding
