#pragma once

#include <util/bit_vector.h>
#include <util/byte_buffer.h>
#include <util/macro.h>

#include <algorithm>
#include <cstdint>
#include <vector>

namespace columnar::core::encoding {
template <typename T>
struct Run {
    uint32_t len;
    T value;
};

template <util::BinaryTrivial T>
void EncodeRLE(util::BufWriter& w, const T* data, size_t n) {
    std::vector<Run<T>> runs;
    for (size_t i = 0; i < n;) {
        size_t j = i + 1;
        while (j < n && data[j] == data[i]) {
            ++j;
        }
        runs.push_back({static_cast<uint32_t>(j - i), data[i]});
        i = j;
    }
    w.Write<uint32_t>(runs.size());
    for (auto& run : runs) {
        w.Write<uint32_t>(run.len);
        w.Write<T>(run.value);
    }
}

template <util::BinaryTrivial T>
std::vector<T> DecodeRLE(util::BufReader& r, size_t n) {
    auto cnt = r.Read<uint32_t>();
    std::vector<T> out(n);
    size_t pos = 0;
    for (uint32_t i = 0; i < cnt; ++i) {
        auto len = r.Read<uint32_t>();
        auto val = r.Read<T>();
        if (pos + len > n) {
            THROW_RUNTIME_ERROR("RLE length invalid");
        }
        std::fill(out.data() + pos, out.data() + pos + len, val);
        pos += len;
    }
    if (pos != n) {
        THROW_RUNTIME_ERROR("RLE length mismatch");
    }
    return out;
}

void EncodeBoolRLE(util::BufWriter& w, const util::BitVector& bits, size_t n);

util::BitVector DecodeBoolRLE(util::BufReader& r, size_t n);
}  // namespace columnar::core::encoding
