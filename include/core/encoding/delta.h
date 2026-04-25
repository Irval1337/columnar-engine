#pragma once

#include <core/encoding/frame_of_reference.h>
#include <util/stream_helper.h>

#include <concepts>
#include <cstddef>
#include <istream>
#include <ostream>
#include <vector>

namespace columnar::core::encoding {
template <std::integral T>
void EncodeDelta(std::ostream& os, const T* src, size_t n, T min_delta, T max_delta) {
    util::Write<T>(os, n > 0 ? src[0] : T(0));
    if (n == 0) {
        return;
    }
    size_t m = n - 1;
    if (m == 0 || min_delta == max_delta) {
        util::Write<T>(os, min_delta);
        util::Write<uint8_t>(os, 0);
        return;
    }
    std::vector<T> deltas(m);
    for (size_t i = 0; i < m; ++i) {
        deltas[i] =
            static_cast<T>(static_cast<uint64_t>(src[i + 1]) - static_cast<uint64_t>(src[i]));
    }
    EncodeFOR<T>(os, deltas.data(), m, min_delta, max_delta);
}

template <std::integral T>
void EncodeDelta(std::ostream& os, const T* src, size_t n) {
    util::Write<T>(os, n > 0 ? src[0] : T(0));
    if (n == 0) {
        return;
    }
    size_t m = n - 1;
    std::vector<T> deltas(m);
    for (size_t i = 0; i < m; ++i) {
        deltas[i] =
            static_cast<T>(static_cast<uint64_t>(src[i + 1]) - static_cast<uint64_t>(src[i]));
    }
    EncodeFOR<T>(os, deltas.data(), m);
}

template <std::integral T>
std::vector<T> DecodeDelta(std::istream& is, size_t n, std::vector<uint8_t>& packed) {
    auto first = util::Read<T>(is);
    size_t m = n > 0 ? n - 1 : 0;
    std::vector<T> out(n);
    if (n == 0) {
        return out;
    }
    auto min_delta = util::Read<T>(is);
    auto bit_width = util::Read<uint8_t>(is);
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    out[0] = first;
    if (bit_width == 0) {
        for (size_t i = 0; i < m; ++i) {
            out[i + 1] =
                static_cast<T>(static_cast<uint64_t>(out[i]) + static_cast<uint64_t>(min_delta));
        }
        return out;
    }
    size_t packed_size = BitPackedSize(m, bit_width);
    packed.resize(packed_size);
    util::ReadRaw(is, packed.data(), packed_size);
    std::vector<T> deltas(m);
    BitUnpackWithOffset(packed.data(), packed_size, m, bit_width, min_delta, deltas.data());
    for (size_t i = 0; i < m; ++i) {
        out[i + 1] =
            static_cast<T>(static_cast<uint64_t>(out[i]) + static_cast<uint64_t>(deltas[i]));
    }
    return out;
}

template <std::integral T>
std::vector<T> DecodeDelta(std::istream& is, size_t n) {
    std::vector<uint8_t> packed;
    return DecodeDelta<T>(is, n, packed);
}
}  // namespace columnar::core::encoding
