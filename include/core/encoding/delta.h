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
void EncodeDelta(std::ostream& os, const T* src, size_t n) {
    util::Write<T>(os, n > 0 ? src[0] : T(0));
    size_t m = n > 0 ? n - 1 : 0;
    std::vector<T> deltas(m);
    for (size_t i = 0; i < m; ++i) {
        deltas[i] =
            static_cast<T>(static_cast<uint64_t>(src[i + 1]) - static_cast<uint64_t>(src[i]));
    }
    EncodeFOR<T>(os, deltas.data(), m);
}

template <std::integral T>
std::vector<T> DecodeDelta(std::istream& is, size_t n) {
    auto first = util::Read<T>(is);
    size_t m = n > 0 ? n - 1 : 0;
    auto deltas = DecodeFOR<T>(is, m);
    std::vector<T> out(n);
    if (n == 0) {
        return out;
    }
    out[0] = first;
    for (size_t i = 0; i < m; ++i) {
        out[i + 1] =
            static_cast<T>(static_cast<uint64_t>(out[i]) + static_cast<uint64_t>(deltas[i]));
    }
    return out;
}
}  // namespace columnar::core::encoding
