#pragma once

#include <core/encoding/frame_of_reference.h>
#include <util/byte_buffer.h>

#include <concepts>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace columnar::core::encoding {
template <std::integral T>
void EncodeDelta(util::BufWriter& w, const T* src, size_t n, T min_delta, T max_delta,
                 std::vector<uint8_t>& packed_buf) {
    w.Write<T>(n > 0 ? src[0] : T(0));
    if (n == 0) {
        return;
    }
    size_t m = n - 1;
    if (m == 0 || min_delta == max_delta) {
        w.Write<T>(min_delta);
        w.Write<uint8_t>(0);
        return;
    }
    std::vector<T> deltas(m);
    for (size_t i = 0; i < m; ++i) {
        deltas[i] =
            static_cast<T>(static_cast<uint64_t>(src[i + 1]) - static_cast<uint64_t>(src[i]));
    }
    EncodeFOR<T>(w, deltas.data(), m, min_delta, max_delta, packed_buf);
}

template <std::integral T>
void EncodeDelta(util::BufWriter& w, const T* src, size_t n, T min_delta, T max_delta) {
    std::vector<uint8_t> packed_buf;
    EncodeDelta(w, src, n, min_delta, max_delta, packed_buf);
}

template <std::integral T>
void EncodeDelta(util::BufWriter& w, const T* src, size_t n) {
    w.Write<T>(n > 0 ? src[0] : T(0));
    if (n == 0) {
        return;
    }
    size_t m = n - 1;
    std::vector<T> deltas(m);
    for (size_t i = 0; i < m; ++i) {
        deltas[i] =
            static_cast<T>(static_cast<uint64_t>(src[i + 1]) - static_cast<uint64_t>(src[i]));
    }
    EncodeFOR<T>(w, deltas.data(), m);
}

template <std::integral T>
std::vector<T> DecodeDelta(util::BufReader& r, size_t n) {
    auto first = r.Read<T>();
    size_t m = n > 0 ? n - 1 : 0;
    std::vector<T> out(n);
    if (n == 0) {
        return out;
    }
    auto min_delta = r.Read<T>();
    auto bit_width = r.Read<uint8_t>();
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
    BitUnpackWithOffset(r.Take(packed_size), packed_size, m, bit_width, min_delta, out.data() + 1);
    for (size_t i = 0; i < m; ++i) {
        out[i + 1] =
            static_cast<T>(static_cast<uint64_t>(out[i]) + static_cast<uint64_t>(out[i + 1]));
    }
    return out;
}
}  // namespace columnar::core::encoding
