#pragma once

#include <core/encoding/bit_packing.h>
#include <util/byte_buffer.h>
#include <util/macro.h>

#include <algorithm>
#include <concepts>
#include <cstdint>
#include <vector>

namespace columnar::core::encoding {
template <std::integral T>
void EncodeFOR(util::BufWriter& w, const T* src, size_t n, T mn, T mx,
               std::vector<uint8_t>& packed_buf) {
    uint8_t bit_width = BitWidth(static_cast<uint64_t>(mx) - static_cast<uint64_t>(mn));
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    w.Write<T>(mn);
    w.Write<uint8_t>(bit_width);
    if (bit_width == 0 || n == 0) {
        return;
    }
    size_t packed_size = BitPackedSize(n, bit_width);
    packed_buf.resize(packed_size);
    BitPackWithOffset(src, n, mn, bit_width, packed_buf.data());
    w.WriteRaw(packed_buf.data(), packed_size);
}

template <std::integral T>
void EncodeFOR(util::BufWriter& w, const T* src, size_t n, T mn, T mx) {
    std::vector<uint8_t> packed_buf;
    EncodeFOR(w, src, n, mn, mx, packed_buf);
}

template <std::integral T>
void EncodeFOR(util::BufWriter& w, const T* src, size_t n) {
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
    EncodeFOR(w, src, n, mn, mx);
}

template <std::integral T>
std::vector<T> DecodeFOR(util::BufReader& r, size_t n) {
    auto mn = r.Read<T>();
    auto bit_width = r.Read<uint8_t>();
    if (bit_width > kBitPackingMaxWidth) {
        THROW_RUNTIME_ERROR("bit_width is too large");
    }
    std::vector<T> out(n);
    if (bit_width == 0) {
        std::fill(out.begin(), out.end(), mn);
        return out;
    }
    size_t packed_size = BitPackedSize(n, bit_width);
    BitUnpackWithOffset(r.Take(packed_size), packed_size, n, bit_width, mn, out.data());
    return out;
}
}  // namespace columnar::core::encoding
